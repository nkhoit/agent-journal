use journal_protocol::{domain::*, *};
use journal_service::{BootstrapError, BootstrapService};
use journal_storage_sqlite::Database;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture {
    db: Database,
    service: BootstrapService,
    token: String,
    delivery: String,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/record-tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "{}-{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let db = Database::open(&path).unwrap();
        let service = BootstrapService::new(db.clone());
        for id in ["writer", "reader", "outsider"] {
            service
                .create_principal(&PrincipalCreateRequest {
                    id: id.into(),
                    display_name: id.into(),
                })
                .unwrap();
        }
        for id in ["space", "other"] {
            service
                .create_space(&SpaceCreateRequest {
                    id: id.into(),
                    name: id.into(),
                })
                .unwrap();
        }
        for principal in ["writer", "reader"] {
            service
                .set_membership(&MembershipRequest {
                    space_id: "space".into(),
                    principal_id: principal.into(),
                    can_read: true,
                    can_append: principal == "writer",
                    can_admin: false,
                })
                .unwrap();
        }
        service
            .provision_adapter(&AdapterProvisionRequest {
                principal_id: "writer".into(),
                adapter_id: "adapter".into(),
            })
            .unwrap();
        let ticket = service
            .create_ticket(&EnrollmentTicketCreateRequest {
                principal_id: "writer".into(),
                adapter_id: "adapter".into(),
                ttl_seconds: 900,
            })
            .unwrap();
        let enrollment = service
            .exchange(
                &ticket.enrollment_ticket.ticket,
                &EnrollmentExchangeRequest {
                    instance_id: "installation".into(),
                },
            )
            .unwrap();
        Self {
            db,
            service,
            token: enrollment.principal_client_secret.secret,
            delivery: enrollment.delivery_adapter_secret.secret,
            path,
        }
    }
    fn input(&self) -> RecordInput {
        RecordInput {
            kind: "note".into(),
            content: "hello".into(),
            run_id: None,
            attention: vec!["reader".into()],
            routing_key: None,
            relations: vec![],
        }
    }
    fn count(&self, table: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

fn search_query(q: &str) -> SearchRecordsQuery {
    SearchRecordsQuery {
        q: q.into(),
        page: PageQuery {
            limit: Some(1),
            cursor: None,
        },
        author: None,
        attention: None,
        since: None,
        order: SearchOrder::Rank,
    }
}

#[test]
fn search_reads_do_not_acquire_the_writer_lock() {
    let f = Fixture::new();
    f.service
        .append_record(&f.token, "space", "search-lock", &f.input())
        .unwrap();
    // Initialize the persisted cursor key before holding the competing writer.
    f.service
        .search_records(&f.token, "space", &search_query("hello"))
        .unwrap();
    let mut connection = f.db.connect().unwrap();
    let writer = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    writer
        .execute(
            "INSERT INTO spaces(id,name,created_at) VALUES ('uncommitted','Uncommitted','2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
    for order in [SearchOrder::Seq, SearchOrder::Rank] {
        let mut query = search_query("hello");
        query.order = order;
        assert_eq!(
            f.service
                .search_records(&f.token, "space", &query)
                .unwrap()
                .items
                .len(),
            1
        );
    }
    writer.commit().unwrap();
}

#[test]
fn sequence_search_only_renders_the_selected_page() {
    let f = Fixture::new();
    let connection = f.db.connect().unwrap();
    connection.execute(
        "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<1000)
         INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
         SELECT printf('record-%05d',n),'space',n,'writer','note',?1,'2026-01-01T00:00:00Z' FROM seq",
        ["hello world ".repeat(1000)],
    ).unwrap();
    let mut query = search_query("hello");
    query.order = SearchOrder::Seq;
    query.page.limit = Some(2);
    let started = std::time::Instant::now();
    let first = f.service.search_records(&f.token, "space", &query).unwrap();
    assert_eq!(first.items[0].record.seq, 1);
    let secret: Vec<u8> = connection
        .query_row(
            "SELECT secret FROM journal_secrets WHERE name='cursor'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let filters = serde_json::to_vec(&(
        "writer",
        "space",
        &query.q,
        &query.author,
        &query.attention,
        &query.since,
    ))
    .unwrap();
    let scope = CursorScope::new(CursorRoute::Search, &filters, CursorOrder::Sequence);
    query.page.cursor = Some(
        CursorCodec::new(&secret)
            .unwrap()
            .encode(
                &scope,
                &CursorPosition::Sequence {
                    sequence: 990,
                    id: "record-00990".into(),
                },
            )
            .unwrap(),
    );
    let second = f.service.search_records(&f.token, "space", &query).unwrap();
    assert_eq!(second.items[0].record.seq, 991);
    assert_eq!(second.items[0].score, 1000.0);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "two small pages rendered the full matching corpus: {:?}",
        started.elapsed()
    );
}

#[test]
fn search_is_authorized_before_scoring_and_cursors_are_scoped() {
    let f = Fixture::new();
    let mut input = f.input();
    input.content = "hello hello hello".into();
    let first = f
        .service
        .append_record(&f.token, "space", "first", &input)
        .unwrap()
        .record;
    input.content = "hello world".into();
    f.service
        .append_record(&f.token, "space", "second", &input)
        .unwrap();
    let mut query = search_query("hello");
    let before = f.service.search_records(&f.token, "space", &query).unwrap();
    assert_eq!(before.items[0].record.id, first.id);
    assert_eq!(before.consistency, Some(SearchConsistency::BestEffort));
    f.db.connect().unwrap().execute_batch(
        "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
         VALUES ('hidden','other',1,'outsider','note','secretword hello hello hello hello','2026-01-01T00:00:00Z');"
    ).unwrap();
    assert_eq!(
        before,
        f.service.search_records(&f.token, "space", &query).unwrap()
    );
    assert!(
        f.service
            .search_records(&f.token, "space", &search_query("secretword"))
            .unwrap()
            .items
            .is_empty()
    );
    assert!(matches!(
        f.service.search_records(&f.token, "other", &query),
        Err(BootstrapError::NotFound)
    ));
    assert!(matches!(
        f.service.search_records(&f.delivery, "space", &query),
        Err(BootstrapError::Unauthorized)
    ));
    query.page.cursor = before.next_cursor;
    assert_eq!(
        f.service
            .search_records(&f.token, "space", &query)
            .unwrap()
            .items
            .len(),
        1
    );
    query.order = SearchOrder::Seq;
    assert!(matches!(
        f.service.search_records(&f.token, "space", &query),
        Err(BootstrapError::InvalidJournal)
    ));
    query.page.cursor = None;
    let page = f.service.search_records(&f.token, "space", &query).unwrap();
    assert_eq!(page.consistency, Some(SearchConsistency::Deterministic));
    input.content = "hello new".into();
    f.service
        .append_record(&f.token, "space", "third", &input)
        .unwrap();
    query.page.cursor = page.next_cursor;
    assert_eq!(
        f.service
            .search_records(&f.token, "space", &query)
            .unwrap()
            .items[0]
            .record
            .seq,
        2
    );
    query.author = Some("reader".into());
    assert!(matches!(
        f.service.search_records(&f.token, "space", &query),
        Err(BootstrapError::InvalidJournal)
    ));
    for q in ["\"", "hello OR", "", &"a".repeat(513)] {
        assert!(
            matches!(
                f.service
                    .search_records(&f.token, "space", &search_query(q)),
                Err(BootstrapError::InvalidJournal) | Err(BootstrapError::Invalid(_))
            ),
            "{q}"
        );
    }
}

#[test]
fn thread_projects_only_replies_and_binds_cursor_to_anchor() {
    let f = Fixture::new();
    let mut input = f.input();
    let root = f
        .service
        .append_record(&f.token, "space", "root", &input)
        .unwrap()
        .record;
    input.relations = vec![Relation {
        relation_type: RelationType::ReplyTo,
        record_id: root.id.clone(),
    }];
    let child = f
        .service
        .append_record(&f.token, "space", "child", &input)
        .unwrap()
        .record;
    input.relations[0].relation_type = RelationType::RefersTo;
    f.service
        .append_record(&f.token, "space", "reference", &input)
        .unwrap();
    let mut query = PageQuery {
        limit: Some(1),
        cursor: None,
    };
    let page = f.service.get_thread(&f.token, &child.id, &query).unwrap();
    assert_eq!(page.items, vec![root.clone()]);
    query.cursor = page.next_cursor;
    assert_eq!(
        f.service
            .get_thread(&f.token, &child.id, &query)
            .unwrap()
            .items,
        vec![child.clone()]
    );
    assert!(matches!(
        f.service.get_thread(&f.token, &root.id, &query),
        Err(BootstrapError::InvalidJournal)
    ));
    assert!(matches!(
        f.service
            .get_thread(&f.delivery, &root.id, &PageQuery::default()),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service
            .get_thread(&f.token, "missing", &PageQuery::default()),
        Err(BootstrapError::NotFound)
    ));
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "writer".into(),
            can_read: false,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    assert!(matches!(
        f.service.get_thread(&f.token, &child.id, &query),
        Err(BootstrapError::NotFound)
    ));
}

#[test]
fn search_filters_fts_syntax_and_unicode_bounds() {
    let f = Fixture::new();
    assert!(matches!(
        f.service
            .search_records(&f.token, "space", &search_query("\"")),
        Err(BootstrapError::InvalidJournal)
    ));
    let mut input = f.input();
    input.content = "Hello 界 hello world\0 hello".into();
    let record = f
        .service
        .append_record(&f.token, "space", "one", &input)
        .unwrap()
        .record;
    let mut query = search_query("\"hello world\" OR 界");
    query.author = Some("writer".into());
    query.attention = Some("reader".into());
    query.since = Some(record.created_at.clone());
    assert_eq!(
        f.service
            .search_records(&f.token, "space", &query)
            .unwrap()
            .items[0]
            .record,
        record
    );
    let instant: jiff::Timestamp = record.created_at.parse().unwrap();
    query.since = Some(
        jiff::Timestamp::from_nanosecond(instant.as_nanosecond() + 1)
            .unwrap()
            .to_string(),
    );
    assert!(
        f.service
            .search_records(&f.token, "space", &query)
            .unwrap()
            .items
            .is_empty()
    );
    query.since = Some(record.created_at.replace('Z', "+00:00"));
    assert_eq!(
        f.service
            .search_records(&f.token, "space", &query)
            .unwrap()
            .items
            .len(),
        1
    );
    query.author = Some("reader".into());
    assert!(
        f.service
            .search_records(&f.token, "space", &query)
            .unwrap()
            .items
            .is_empty()
    );
    query.author = None;
    query.attention = Some("writer".into());
    assert!(
        f.service
            .search_records(&f.token, "space", &query)
            .unwrap()
            .items
            .is_empty()
    );
    input.content = format!("hello {}", "界".repeat(2000));
    f.service
        .append_record(&f.token, "space", "long", &input)
        .unwrap();
    let mut query = search_query("hel*");
    query.page.limit = Some(100);
    let page = f.service.search_records(&f.token, "space", &query).unwrap();
    assert_eq!(page.items.len(), 2);
    assert!(
        page.items
            .iter()
            .all(|r| r.snippet.as_ref().unwrap().chars().count() <= 1024)
    );
    assert_eq!(
        page.items
            .iter()
            .find(|r| r.record.id == record.id)
            .unwrap()
            .score,
        3.0
    );
}

#[test]
fn thread_depth_limit_is_not_response_limit() {
    let f = Fixture::new();
    let mut input = f.input();
    let root = f
        .service
        .append_record(&f.token, "space", "root", &input)
        .unwrap()
        .record;
    let mut parent = root.id.clone();
    for depth in 1..=64 {
        input.relations = vec![Relation {
            relation_type: RelationType::ReplyTo,
            record_id: parent,
        }];
        parent = f
            .service
            .append_record(&f.token, "space", &format!("depth-{depth}"), &input)
            .unwrap()
            .record
            .id;
    }
    let query = PageQuery {
        limit: Some(1),
        cursor: None,
    };
    assert_eq!(
        f.service
            .get_thread(&f.token, &parent, &query)
            .unwrap()
            .items[0]
            .id,
        root.id
    );
    input.relations[0].record_id = parent;
    let leaf = f
        .service
        .append_record(&f.token, "space", "too-deep", &input)
        .unwrap()
        .record
        .id;
    for anchor in [root.id, leaf] {
        assert!(matches!(
            f.service.get_thread(&f.token, &anchor, &query),
            Err(BootstrapError::InvalidJournal)
        ));
    }
}

#[test]
fn thread_node_budget_accepts_exact_limit_and_rejects_one_over() {
    let f = Fixture::new();
    let connection = f.db.connect().unwrap();
    connection.execute_batch(
        "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<4096)
         INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
         SELECT printf('record-%05d',n),'space',n,'writer','note','hello','2026-01-01T00:00:00Z' FROM seq;
         INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at)
         SELECT id,'reply-to','record-00001',created_at FROM records WHERE space_seq>1;"
    ).unwrap();
    let query = PageQuery {
        limit: Some(1),
        cursor: None,
    };
    assert_eq!(
        f.service
            .get_thread(&f.token, "record-00001", &query)
            .unwrap()
            .items[0]
            .seq,
        1
    );
    connection
        .execute_batch(
            "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
         VALUES ('extra','space',4097,'writer','note','hello','2026-01-01T00:00:00Z');
         INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at)
         VALUES ('extra','reply-to','record-00001','2026-01-01T00:00:00Z');",
        )
        .unwrap();
    assert!(matches!(
        f.service.get_thread(&f.token, "record-00001", &query),
        Err(BootstrapError::InvalidJournal)
    ));
}

#[test]
fn append_replay_read_and_mailbox_are_durable() {
    let f = Fixture::new();
    let input = f.input();
    let first = f
        .service
        .append_record(&f.token, "space", "key", &input)
        .unwrap();
    assert_eq!(first.record.author, "writer");
    assert_eq!(first.record.seq, 1);
    assert_eq!(&first.record.id[14..15], "7");
    assert!("89ab".contains(&first.record.id[19..20]));
    assert_eq!(first.mailbox_created, 1);
    assert!(!first.replayed);
    let restarted = BootstrapService::new(Database::open(&f.path).unwrap());
    let replay = restarted
        .append_record(&f.token, "space", "key", &input)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(first.record, replay.record);
    assert_eq!(
        restarted.get_record(&f.token, &first.record.id).unwrap(),
        first.record
    );
    for table in [
        "records",
        "attention",
        "mailbox_items",
        "delivery_attempts",
        "idempotency_keys",
    ] {
        assert_eq!(f.count(table), 1, "{table}");
    }
    let mut changed = input;
    changed.content.push('!');
    assert!(matches!(
        restarted.append_record(&f.token, "space", "key", &changed),
        Err(BootstrapError::IdempotencyConflict)
    ));
}

#[test]
fn record_cursor_preserves_after_seq_filters_and_page_boundaries() {
    let f = Fixture::new();
    for n in 1..=9 {
        let mut input = f.input();
        input.kind = if n % 2 == 0 { "even" } else { "odd" }.into();
        f.service
            .append_record(&f.token, "space", &format!("key-{n}"), &input)
            .unwrap();
    }
    let mut query = ListRecordsQuery {
        after_seq: Some(2),
        kind: Some("even".into()),
        page: PageQuery::new(None, Some(1)),
        ..Default::default()
    };
    let first = f.service.list_records(&f.token, "space", &query).unwrap();
    assert_eq!(first.items.iter().map(|r| r.seq).collect::<Vec<_>>(), [4]);
    query.page.cursor = Some(first.next_cursor.unwrap());
    let mut changed = query.clone();
    changed.after_seq = Some(3);
    assert!(matches!(
        f.service.list_records(&f.token, "space", &changed),
        Err(BootstrapError::InvalidJournal)
    ));
    query.page.limit = Some(2);
    let last = f.service.list_records(&f.token, "space", &query).unwrap();
    assert_eq!(last.items.iter().map(|r| r.seq).collect::<Vec<_>>(), [6, 8]);
    assert!(last.next_cursor.is_none());
}

#[test]
fn rollback_at_every_append_boundary() {
    for boundary in [
        "append-sequence",
        "append-record",
        "append-relations",
        "append-attention",
        "append-mailbox",
        "append-idempotency",
    ] {
        let f = Fixture::new();
        let seed = f
            .service
            .append_record(&f.token, "space", "seed", &f.input())
            .unwrap();
        let mut input = f.input();
        input.relations.push(Relation {
            relation_type: RelationType::ReplyTo,
            record_id: seed.record.id,
        });
        assert!(
            f.service
                .clone()
                .with_failpoint(boundary)
                .append_record(&f.token, "space", "key", &input)
                .is_err()
        );
        for table in [
            "records",
            "record_relations",
            "attention",
            "mailbox_items",
            "delivery_attempts",
            "idempotency_keys",
        ] {
            assert_eq!(
                f.count(table),
                if table == "record_relations" { 0 } else { 1 },
                "{boundary}: {table}"
            );
        }
        assert_eq!(
            f.service
                .append_record(&f.token, "space", "key", &input)
                .unwrap()
                .record
                .seq,
            2
        );
    }
}

#[test]
fn authorization_recipients_relations_and_utf8_limits() {
    let f = Fixture::new();
    assert!(matches!(
        f.service
            .append_record(&f.delivery, "space", "key", &f.input()),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service
            .append_record(&f.token, "other", "key", &f.input()),
        Err(BootstrapError::NotFound)
    ));
    let mut input = f.input();
    input.attention = vec!["outsider".into()];
    assert!(matches!(
        f.service.append_record(&f.token, "space", "key", &input),
        Err(BootstrapError::NotFound)
    ));
    input = f.input();
    input.relations.push(Relation {
        relation_type: RelationType::ReplyTo,
        record_id: "missing".into(),
    });
    assert!(matches!(
        f.service.append_record(&f.token, "space", "key", &input),
        Err(BootstrapError::NotFound)
    ));
    input = f.input();
    input.content = format!("{}a", "界".repeat(21845));
    let first = f
        .service
        .append_record(&f.token, "space", "key", &input)
        .unwrap();
    input.content.push('b');
    assert!(
        f.service
            .append_record(&f.token, "space", "large", &input)
            .is_err()
    );
    let mut reply = f.input();
    reply.relations.push(Relation {
        relation_type: RelationType::ReplyTo,
        record_id: first.record.id.clone(),
    });
    let second = f
        .service
        .append_record(&f.token, "space", "reply", &reply)
        .unwrap();
    assert_eq!(second.record.relations, reply.relations);
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "writer".into(),
            can_read: false,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    assert!(matches!(
        f.service.get_record(&f.token, &first.record.id),
        Err(BootstrapError::NotFound)
    ));
    assert!(matches!(
        f.service
            .list_records(&f.token, "space", &ListRecordsQuery::default()),
        Err(BootstrapError::NotFound)
    ));
    assert!(matches!(
        f.service
            .append_record(&f.token, "space", "key", &f.input()),
        Err(BootstrapError::NotFound)
    ));
}

#[test]
fn concurrent_sequences_and_scoped_resumable_pages() {
    let f = Fixture::new();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..12)
            .map(|i| {
                let f = &f;
                scope.spawn(move || {
                    f.service
                        .append_record(&f.token, "space", &format!("key-{i}"), &f.input())
                        .unwrap()
                        .record
                        .seq
                })
            })
            .collect();
        let mut sequences: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        sequences.sort();
        assert_eq!(sequences, (1..=12).collect::<Vec<_>>());
    });
    let mut query = ListRecordsQuery {
        page: PageQuery::new(None, Some(5)),
        ..Default::default()
    };
    let first = f.service.list_records(&f.token, "space", &query).unwrap();
    assert_eq!(
        first.items.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    query.page.cursor = first.next_cursor;
    let restarted = BootstrapService::new(Database::open(&f.path).unwrap());
    assert_eq!(
        restarted
            .list_records(&f.token, "space", &query)
            .unwrap()
            .items[0]
            .seq,
        6
    );
    query.kind = Some("different".into());
    assert!(restarted.list_records(&f.token, "space", &query).is_err());
    assert_eq!(
        f.service
            .list_spaces(&f.token, &PageQuery::default())
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        f.service
            .list_principals(
                &f.token,
                &ListPrincipalsQuery {
                    space: "space".into(),
                    page: PageQuery::default()
                }
            )
            .unwrap()
            .items
            .len(),
        2
    );
}
#[test]
fn limits_filters_relations_and_attention_set_replay() {
    let f = Fixture::new();
    let mut input = f.input();
    input.attention.clear();
    let mut targets = Vec::new();
    for i in 0..32 {
        targets.push(
            f.service
                .append_record(&f.token, "space", &format!("target-{i}"), &input)
                .unwrap()
                .record
                .id,
        );
    }
    for i in 0..17 {
        let id = format!("recipient-{i:02}");
        f.service
            .create_principal(&PrincipalCreateRequest {
                id: id.clone(),
                display_name: id.clone(),
            })
            .unwrap();
        f.service
            .set_membership(&MembershipRequest {
                space_id: "space".into(),
                principal_id: id,
                can_read: true,
                can_append: false,
                can_admin: false,
            })
            .unwrap();
    }
    input.attention = (0..16).map(|i| format!("recipient-{i:02}")).collect();
    input.relations = targets
        .iter()
        .rev()
        .map(|id| Relation {
            relation_type: RelationType::RefersTo,
            record_id: id.clone(),
        })
        .collect();
    let result = f
        .service
        .append_record(&f.token, "space", "boundary", &input)
        .unwrap();
    assert_eq!(result.mailbox_created, 16);
    assert_eq!(
        f.service.get_record(&f.token, &result.record.id).unwrap(),
        result.record
    );
    input.attention.reverse();
    assert!(
        f.service
            .append_record(&f.token, "space", "boundary", &input)
            .unwrap()
            .replayed
    );
    input.attention.push("recipient-16".into());
    assert!(
        f.service
            .append_record(&f.token, "space", "too-many", &input)
            .is_err()
    );
    input.attention.pop();
    input.relations.push(Relation {
        relation_type: RelationType::Acknowledges,
        record_id: targets[0].clone(),
    });
    assert!(
        f.service
            .append_record(&f.token, "space", "too-many", &input)
            .is_err()
    );
    let query = ListRecordsQuery {
        author: Some("writer".into()),
        kind: Some("note".into()),
        attention: Some("recipient-00".into()),
        relation: Some(RelationType::RefersTo),
        after_seq: Some(32),
        ..Default::default()
    };
    assert_eq!(
        f.service
            .list_records(&f.token, "space", &query)
            .unwrap()
            .items,
        vec![result.record]
    );
    for query in [
        ListRecordsQuery {
            after_seq: Some(u64::MAX),
            ..Default::default()
        },
        ListRecordsQuery {
            kind: Some("absent".into()),
            ..Default::default()
        },
        ListRecordsQuery {
            author: Some("absent".into()),
            ..Default::default()
        },
        ListRecordsQuery {
            attention: Some("outsider".into()),
            ..Default::default()
        },
    ] {
        assert!(
            f.service
                .list_records(&f.token, "space", &query)
                .unwrap()
                .items
                .is_empty()
        );
    }
    let page = f
        .service
        .list_principals(
            &f.token,
            &ListPrincipalsQuery {
                space: "space".into(),
                page: PageQuery::new(None, Some(1)),
            },
        )
        .unwrap();
    assert!(
        f.service
            .list_spaces(&f.token, &PageQuery::new(page.next_cursor, Some(1)))
            .is_err()
    );
    let mut duplicate = f.input();
    duplicate.attention.push("reader".into());
    assert!(
        f.service
            .append_record(&f.token, "space", "duplicate", &duplicate)
            .is_err()
    );
    duplicate = f.input();
    duplicate.relations = targets[..2]
        .iter()
        .map(|id| Relation {
            relation_type: RelationType::ReplyTo,
            record_id: id.clone(),
        })
        .collect();
    assert!(
        f.service
            .append_record(&f.token, "space", "two-replies", &duplicate)
            .is_err()
    );
    f.service
        .set_membership(&MembershipRequest {
            space_id: "other".into(),
            principal_id: "writer".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    let mut cross = f.input();
    cross.attention.clear();
    let other = f
        .service
        .append_record(&f.token, "other", "other", &cross)
        .unwrap();
    cross.relations.push(Relation {
        relation_type: RelationType::RefersTo,
        record_id: other.record.id,
    });
    assert!(matches!(
        f.service.append_record(&f.token, "space", "cross", &cross),
        Err(BootstrapError::NotFound)
    ));
}

#[test]
fn append_crash_child() {
    use journal_service::{OsSecretSource, SecretSource, SystemClock};
    use std::sync::Arc;
    let Ok(path) = std::env::var("AJ_CRASH_DATABASE") else {
        return;
    };
    struct BarrierSource {
        calls: AtomicU64,
        marker: String,
    }
    impl SecretSource for BarrierSource {
        fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
            OsSecretSource.fill(bytes)?;
            if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                std::fs::write(&self.marker, b"uncommitted").unwrap();
                loop {
                    std::thread::park();
                }
            }
            Ok(())
        }
    }
    let marker = std::env::var("AJ_CRASH_MARKER").unwrap();
    let random: Arc<dyn SecretSource> = if std::env::var("AJ_CRASH_MODE").unwrap() == "before" {
        Arc::new(BarrierSource {
            calls: AtomicU64::new(0),
            marker: marker.clone(),
        })
    } else {
        Arc::new(OsSecretSource)
    };
    let service = BootstrapService::with_sources(
        Database::open(path).unwrap(),
        Arc::new(SystemClock),
        random,
    );
    let input = RecordInput {
        kind: "note".into(),
        content: "crash payload".into(),
        attention: vec!["reader".into()],
        run_id: None,
        routing_key: None,
        relations: vec![],
    };
    service
        .append_record(
            &std::env::var("AJ_CRASH_TOKEN").unwrap(),
            "space",
            "crash",
            &input,
        )
        .unwrap();
    std::fs::write(marker, b"committed").unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn process_termination_rolls_back_or_replays_complete_append() {
    for mode in ["before", "after"] {
        let f = Fixture::new();
        let marker = f.path.with_extension("marker");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "append_crash_child"])
            .env("AJ_CRASH_DATABASE", &f.path)
            .env("AJ_CRASH_TOKEN", &f.token)
            .env("AJ_CRASH_MARKER", &marker)
            .env("AJ_CRASH_MODE", mode)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !marker.exists() && std::time::Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let reached = marker.exists();
        let _ = child.kill();
        child.wait().unwrap();
        let _ = std::fs::remove_file(marker);
        assert!(reached, "child failed to reach crash boundary");
        let expected = if mode == "before" { 0 } else { 1 };
        for table in [
            "records",
            "attention",
            "mailbox_items",
            "delivery_attempts",
            "idempotency_keys",
        ] {
            assert_eq!(f.count(table), expected, "{mode}: {table}");
        }
        let mut input = f.input();
        input.content = "crash payload".into();
        let retry = f
            .service
            .append_record(&f.token, "space", "crash", &input)
            .unwrap();
        assert_eq!(retry.replayed, mode == "after");
        assert_eq!(retry.record.seq, 1);
    }
}
