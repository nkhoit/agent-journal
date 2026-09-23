use journal_protocol::{domain::*, *};
use journal_service::{BootstrapError, BootstrapService};
use journal_storage_sqlite::Database;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture {
    db: Database,
    service: BootstrapService,
    token: String,
    unbound_token: String,
    path: std::path::PathBuf,
}

#[test]
fn independently_registered_principals_use_public_spaces_without_memberships() {
    let f = Fixture::new();
    let alpha = "a".repeat(64);
    let beta = "b".repeat(64);
    for (token, handle) in [(&alpha, "alpha"), (&beta, "beta")] {
        f.service
            .register(
                token,
                &RegistrationRequest {
                    handle: handle.into(),
                    display_name: handle.into(),
                },
            )
            .unwrap();
    }
    let memberships = f.count("memberships");
    let first_page = f
        .service
        .list_spaces(&alpha, &PageQuery::new(None, Some(1)))
        .unwrap();
    assert_eq!(first_page.items[0].id, "other");
    let continuation = PageQuery::new(first_page.next_cursor, Some(1));
    let second_page = f.service.list_spaces(&alpha, &continuation).unwrap();
    assert_eq!(second_page.items[0].id, "space");
    assert!(second_page.next_cursor.is_none());
    assert!(f.service.list_spaces(&beta, &continuation).is_err());
    assert_eq!(
        f.service
            .list_spaces(&alpha, &PageQuery::default())
            .unwrap()
            .items
            .len(),
        2
    );
    assert_eq!(f.service.get_space(&beta, "space").unwrap().id, "space");
    let posted = f
        .service
        .append_record(
            &alpha,
            "space",
            "public",
            &RecordInput {
                attention: vec!["beta".into()],
                ..f.input()
            },
        )
        .unwrap();
    assert_eq!(
        f.service.get_record(&beta, &posted.record.id).unwrap(),
        posted.record
    );
    assert_eq!(posted.mailbox_created, 1);
    assert_eq!(
        f.service
            .delivery_status(&beta, &posted.record.id, &PageQuery::default())
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(f.count("memberships"), memberships);
    f.db.connect()
        .unwrap()
        .execute(
            "UPDATE spaces SET archived_at='2026-01-01T00:00:00Z' WHERE id='space'",
            [],
        )
        .unwrap();
    assert!(
        f.service
            .append_record(&alpha, "space", "new", &f.input())
            .is_err()
    );
    assert_eq!(
        f.service.get_record(&beta, &posted.record.id).unwrap(),
        posted.record
    );
    let replay = f
        .service
        .append_record(
            &alpha,
            "space",
            "public",
            &RecordInput {
                attention: vec!["beta".into()],
                ..f.input()
            },
        )
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.record, posted.record);
    assert_eq!(replay.mailbox_created, posted.mailbox_created);
    let inbox = f.service.inbox(&beta, &InboxQuery::default()).unwrap();
    assert_eq!(inbox.items.len(), 1);
    assert_eq!(inbox.items[0].record, posted.record);
    f.service
        .acknowledge_inbox_item(&beta, &inbox.items[0].inbox_item_id)
        .unwrap();
    assert_eq!(f.count("memberships"), memberships);
    f.db.connect()
        .unwrap()
        .execute(
            "UPDATE principals SET disabled_at='2026-01-01T00:00:00Z' WHERE id=?",
            [f.principal_id("beta")],
        )
        .unwrap();
    let records = f.count("records");
    assert!(matches!(
        f.service.append_record(
            &alpha,
            "other",
            "disabled-recipient",
            &RecordInput {
                attention: vec!["beta".into()],
                ..f.input()
            }
        ),
        Err(BootstrapError::NotFound)
    ));
    assert_eq!(f.count("records"), records);
    assert_eq!(f.count("mailbox_items"), 1);
    assert!(matches!(
        f.service.get_record(&beta, &posted.record.id),
        Err(BootstrapError::Unauthorized)
    ));
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
        for (index, id) in ["writer", "reader", "outsider"].into_iter().enumerate() {
            service
                .register(
                    &format!("{:064x}", index + 1),
                    &RegistrationRequest {
                        handle: id.into(),
                        display_name: id.into(),
                    },
                )
                .unwrap();
        }
        for id in ["space", "other"] {
            service
                .create_space(&SpaceCreateRequest {
                    access: journal_protocol::domain::SpaceAccess::Public,
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
        Self {
            db,
            service,
            token: format!("{:064x}", 1),
            unbound_token: "f".repeat(64),
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
            title: None,
        }
    }
    fn count(&self, table: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
    fn principal_id(&self, handle: &str) -> String {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT principal_id FROM principal_names WHERE name=?",
                [handle],
                |r| r.get(0),
            )
            .unwrap()
    }

    fn reader_token(&self) -> String {
        format!("{:064x}", 2)
    }

    fn stored_response(&self, key: &str) -> String {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT response_json FROM idempotency_keys
                 WHERE principal_id=? AND method='POST' AND path='/v1/spaces/space/records'
                 AND idempotency_key=?",
                rusqlite::params![self.principal_id("writer"), key],
                |row| row.get(0),
            )
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
            "INSERT INTO spaces(id,name,access,created_at) VALUES ('uncommitted','Uncommitted','public','2026-01-01T00:00:00Z')",
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
fn ordinary_reads_do_not_acquire_the_writer_lock() {
    let f = Fixture::new();
    let root = f
        .service
        .append_record(&f.token, "space", "read-lock-root", &f.input())
        .unwrap()
        .record;
    let reply = f
        .service
        .append_record(
            &f.token,
            "space",
            "read-lock-reply",
            &RecordInput {
                relations: vec![Relation {
                    relation_type: RelationType::ReplyTo,
                    record_id: root.id.clone(),
                }],
                ..f.input()
            },
        )
        .unwrap()
        .record;
    let actor = f
        .service
        .authenticate(&f.token, CredentialClass::PrincipalClient)
        .unwrap();
    let mut connection = f.db.connect().unwrap();
    let writer = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    writer
        .execute(
            "INSERT INTO spaces(id,name,access,created_at) VALUES ('uncommitted','Uncommitted','public','2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
    let page = PageQuery::new(None, Some(1));
    let s = &f.service;
    assert_eq!(s.get_record(&f.token, &reply.id).unwrap(), reply);
    assert_eq!(s.get_space(&f.token, "space").unwrap().id, "space");
    assert!(
        s.list_spaces(&f.token, &page)
            .unwrap()
            .next_cursor
            .is_some()
    );
    let principals = ListPrincipalsQuery {
        space: "space".into(),
        page: page.clone(),
    };
    assert!(
        s.list_principals(&f.token, &principals)
            .unwrap()
            .next_cursor
            .is_some()
    );
    assert_eq!(
        s.list_records(&f.token, "space", &ListRecordsQuery::default())
            .unwrap()
            .items,
        vec![root.clone(), reply.clone()]
    );
    assert_eq!(
        s.get_thread(&f.token, &reply.id, &PageQuery::default())
            .unwrap()
            .items,
        vec![root.clone(), reply.clone()]
    );
    assert_eq!(s.me(&actor).unwrap().principal.handle, "writer");
    let viewer = s.shared_viewer("reader");
    assert_eq!(viewer.record(&reply.id).unwrap(), reply);
    assert!(viewer.spaces(&page).unwrap().next_cursor.is_some());
    assert_eq!(
        viewer
            .records("space", &ListRecordsQuery::default())
            .unwrap()
            .items
            .len(),
        2
    );
    assert_eq!(
        viewer
            .thread(&reply.id, &PageQuery::default())
            .unwrap()
            .items
            .len(),
        2
    );
    assert_eq!(viewer.thread_root(&reply.id).unwrap(), (root.clone(), true));
    assert_eq!(
        viewer
            .thread_roots(std::slice::from_ref(&reply.id))
            .unwrap()[&reply.id]
            .0,
        root.id
    );
    writer.commit().unwrap();
}

#[test]
fn shared_viewer_verification_requires_an_existing_active_principal() {
    let f = Fixture::new();
    f.service.shared_viewer("reader").verify().unwrap();
    f.service
        .shared_viewer(&f.principal_id("reader"))
        .verify()
        .unwrap();
    for viewer in ["missing", ""] {
        assert!(
            f.service.shared_viewer(viewer).verify().is_err(),
            "{viewer}"
        );
    }
    f.db.connect()
        .unwrap()
        .execute(
            "UPDATE principals SET disabled_at='2026-01-01T00:00:00Z' WHERE id=?",
            [f.principal_id("reader")],
        )
        .unwrap();
    assert!(f.service.shared_viewer("reader").verify().is_err());
    assert_eq!(f.count("journal_secrets"), 0);
}

#[test]
fn paged_reads_initialize_the_cursor_key_once_for_authenticated_callers() {
    let f = Fixture::new();
    assert_eq!(f.count("journal_secrets"), 0);
    // Rollback-journal mode makes any reader snapshot block the key's writer.
    f.db.connect()
        .unwrap()
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    let page = PageQuery::new(None, Some(1));
    assert!(matches!(
        f.service.list_spaces(&f.unbound_token, &page),
        Err(BootstrapError::Unauthorized)
    ));
    assert_eq!(f.count("journal_secrets"), 0);
    let first = f.service.list_spaces(&f.token, &page).unwrap();
    assert_eq!(f.count("journal_secrets"), 1);
    let second = f
        .service
        .list_spaces(&f.token, &PageQuery::new(first.next_cursor, Some(1)))
        .unwrap();
    assert_eq!(
        [first.items[0].id.as_str(), second.items[0].id.as_str()],
        ["other", "space"]
    );
    assert_eq!(f.count("journal_secrets"), 1);
}

#[test]
fn sequence_search_only_renders_the_selected_page() {
    let f = Fixture::new();
    let connection = f.db.connect().unwrap();
    connection.execute(
        "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<1000)
         INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
         SELECT printf('record-%05d',n),'space',n,(SELECT principal_id FROM principal_names WHERE name='writer'),'note',?1,'2026-01-01T00:00:00Z' FROM seq",
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
        f.principal_id("writer"),
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
    f.db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
             VALUES ('hidden','other',1,?,'note','secretword hello hello hello hello','2026-01-01T00:00:00Z')",
            [f.principal_id("outsider")],
        )
        .unwrap();
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
        f.service.search_records(&f.token, "missing", &query),
        Err(BootstrapError::NotFound)
    ));
    assert!(matches!(
        f.service.search_records(&f.unbound_token, "space", &query),
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
            .get_thread(&f.unbound_token, &root.id, &PageQuery::default()),
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
    assert_eq!(
        f.service
            .get_thread(&f.token, &child.id, &query)
            .unwrap()
            .items,
        vec![child]
    );
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
         SELECT printf('record-%05d',n),'space',n,(SELECT principal_id FROM principal_names WHERE name='writer'),'note','hello','2026-01-01T00:00:00Z' FROM seq;
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
         VALUES ('extra','space',4097,(SELECT principal_id FROM principal_names WHERE name='writer'),'note','hello','2026-01-01T00:00:00Z');
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
    assert_eq!(first.record.author, f.principal_id("writer"));
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
    assert_eq!(replay.record, first.record);
    assert_eq!(replay.mailbox_created, first.mailbox_created);
    assert_eq!(
        restarted.get_record(&f.token, &first.record.id).unwrap(),
        first.record
    );
    for table in [
        "records",
        "attention",
        "mailbox_items",
        "inbox_sequences",
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
fn append_replay_survives_post_commit_acl_and_disable_changes() {
    for mutation in ["revoke-recipient", "disable-author"] {
        let f = Fixture::new();
        let input = f.input();
        let first = f
            .service
            .append_record(&f.token, "space", mutation, &input)
            .unwrap();
        f.db
            .with_transaction(|tx| {
                match mutation {
                    "revoke-recipient" => {
                        tx.execute(
                            "UPDATE memberships SET can_read=0 WHERE space_id='space' AND principal_id=?",
                            [f.principal_id("reader")],
                        )?;
                    }
                    "disable-author" => {
                        tx.execute(
                            "UPDATE principals SET disabled_at='2026-01-01T00:00:00Z' WHERE id=?",
                            [f.principal_id("writer")],
                        )?;
                    }
                    _ => unreachable!(),
                }
                Ok(())
            })
            .unwrap();
        let replay = f
            .service
            .append_record(&f.token, "space", mutation, &input)
            .unwrap();
        assert!(replay.replayed, "{mutation}");
        assert_eq!(replay.record, first.record, "{mutation}");
        assert_eq!(replay.mailbox_created, first.mailbox_created, "{mutation}");
        assert_eq!(f.count("records"), 1);
        assert_eq!(f.count("mailbox_items"), 1);
    }
}

#[test]
fn append_replay_is_rebuilt_from_the_immutable_record() {
    let f = Fixture::new();
    let root = f
        .service
        .append_record(&f.token, "space", "root", &f.input())
        .unwrap()
        .record;
    let other = f
        .service
        .append_record(&f.token, "space", "other", &f.input())
        .unwrap()
        .record;
    let input = RecordInput {
        attention: vec!["reader".into(), "writer".into()],
        relations: vec![
            Relation {
                relation_type: RelationType::RefersTo,
                record_id: other.id.clone(),
            },
            Relation {
                relation_type: RelationType::ReplyTo,
                record_id: root.id.clone(),
            },
        ],
        run_id: Some("run-1".into()),
        routing_key: Some("route".into()),
        title: Some("  Spaced title  ".into()),
        ..f.input()
    };
    let first = f
        .service
        .append_record(&f.token, "space", "rebuilt", &input)
        .unwrap();
    assert_eq!(
        f.stored_response("rebuilt"),
        "{}",
        "no second copy of the record"
    );
    let replay = f
        .service
        .append_record(&f.token, "space", "rebuilt", &input)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.record, first.record);
    assert_eq!(replay.mailbox_created, 2);

    // Rows written before this change keep serving their stored copy exactly.
    let mut legacy = f
        .service
        .append_record(&f.token, "space", "legacy", &f.input())
        .unwrap();
    legacy.mailbox_created = 7;
    f.db.connect()
        .unwrap()
        .execute(
            "UPDATE idempotency_keys SET response_json=? WHERE idempotency_key='legacy'",
            [serde_json::to_string(&legacy).unwrap()],
        )
        .unwrap();
    let replay = f
        .service
        .append_record(&f.token, "space", "legacy", &f.input())
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.mailbox_created, 7);
    assert_eq!(replay.record, legacy.record);
}

#[test]
fn recipients_resolving_to_one_principal_are_invalid_not_conflicts() {
    let f = Fixture::new();
    let reader_id = f.principal_id("reader");
    let reader = f
        .service
        .authenticate(&f.reader_token(), CredentialClass::PrincipalClient)
        .unwrap();
    f.service
        .update_own_profile(
            &reader,
            "rename-reader",
            &ProfileUpdateRequest {
                handle: "reader-renamed".into(),
                display_name: "Reader".into(),
                description: None,
                expected_profile_revision: 1,
            },
        )
        .unwrap();
    for (key, attention) in [
        (
            "handle-and-id",
            vec!["reader-renamed".to_owned(), reader_id.clone()],
        ),
        (
            "handle-and-alias",
            vec!["reader-renamed".to_owned(), "reader".to_owned()],
        ),
    ] {
        let input = RecordInput {
            attention,
            ..f.input()
        };
        assert!(
            matches!(
                f.service.append_record(&f.token, "space", key, &input),
                Err(BootstrapError::InvalidJournal)
            ),
            "{key}"
        );
    }
    for table in ["records", "attention", "mailbox_items", "idempotency_keys"] {
        assert_eq!(f.count(table), 0, "{table}");
    }
}

#[test]
fn only_state_collisions_map_to_conflict() {
    let f = Fixture::new();
    let record = f
        .service
        .append_record(&f.token, "space", "immutable", &f.input())
        .unwrap()
        .record;
    let connection = f.db.connect().unwrap();
    let failure = |sql: &str, params: &[&dyn rusqlite::ToSql]| {
        BootstrapError::from(connection.execute(sql, params).unwrap_err())
    };
    // Invariant guards mean server code or persisted data is wrong.
    assert!(matches!(
        failure(
            "UPDATE records SET content='changed' WHERE id=?",
            &[&record.id]
        ),
        BootstrapError::Sqlite(_)
    ));
    assert!(matches!(
        failure("DELETE FROM mailbox_items", &[]),
        BootstrapError::Sqlite(_)
    ));
    // A taken name, and a name that shadows a principal ID, collide with state.
    let writer = f.principal_id("writer");
    let reader = f.principal_id("reader");
    let name = "INSERT INTO principal_names(name,principal_id,kind,created_at)
                VALUES (?,?,'alias','2026-01-01T00:00:00Z')";
    assert!(matches!(
        failure(name, &[&"reader", &writer]),
        BootstrapError::Conflict
    ));
    assert!(matches!(
        failure(name, &[&reader, &writer]),
        BootstrapError::Conflict
    ));
}

#[test]
fn append_replay_uses_raw_handles_after_profile_rename_and_alias() {
    let f = Fixture::new();
    let input = f.input();
    let first = f
        .service
        .append_record(&f.token, "space", "rename-replay", &input)
        .unwrap();
    let reader_token = f.reader_token();
    let reader = f
        .service
        .authenticate(&reader_token, CredentialClass::PrincipalClient)
        .unwrap();
    f.service
        .update_own_profile(
            &reader,
            "rename-reader",
            &ProfileUpdateRequest {
                handle: "reader-renamed".into(),
                display_name: "Reader Renamed".into(),
                description: None,
                expected_profile_revision: 1,
            },
        )
        .unwrap();
    let replay = f
        .service
        .append_record(&f.token, "space", "rename-replay", &input)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.record, first.record);

    let mut renamed_handle = input;
    renamed_handle.attention = vec!["reader-renamed".into()];
    assert!(matches!(
        f.service
            .append_record(&f.token, "space", "rename-replay", &renamed_handle),
        Err(BootstrapError::IdempotencyConflict)
    ));
    assert_eq!(f.count("records"), 1);
    assert_eq!(f.count("mailbox_items"), 1);
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
        "append-inbox-sequence",
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
            .append_record(&f.unbound_token, "space", "key", &f.input()),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service
            .append_record(&f.token, "missing", "key", &f.input()),
        Err(BootstrapError::NotFound)
    ));
    let mut input = f.input();
    input.attention = vec!["missing".into()];
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
    assert_eq!(
        f.service.get_record(&f.token, &first.record.id).unwrap(),
        first.record
    );
    assert_eq!(
        f.service
            .list_records(&f.token, "space", &ListRecordsQuery::default())
            .unwrap()
            .items
            .len(),
        2
    );
    assert!(matches!(
        f.service
            .append_record(&f.token, "space", "key", &f.input()),
        Err(BootstrapError::IdempotencyConflict)
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
    // Cold admission refuses live WAL sidecars; hand the file over as a
    // restarting daemon would.
    f.db.normalize_for_clean_shutdown().unwrap();
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
        2
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
        3
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
                handle: id.clone(),
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
    assert!(matches!(
        f.service
            .append_record(&f.token, "space", "boundary", &input),
        Err(BootstrapError::IdempotencyConflict)
    ));
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
        title: None,
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
        for table in ["records", "attention", "mailbox_items", "idempotency_keys"] {
            assert_eq!(f.count(table), expected, "{mode}: {table}");
        }
        let mut input = f.input();
        input.content = "crash payload".into();
        let retry = f
            .service
            .append_record(&f.token, "space", "crash", &input)
            .unwrap();
        // "before" crashed before commit: fresh append. "after" completed the
        // append: the retry replays it instead of duplicating.
        assert_eq!(retry.replayed, mode == "after");
        assert_eq!(retry.record.seq, 1);
    }
}

#[test]
fn title_normalization_and_idempotency_hash_before_normalization() {
    let f = Fixture::new();
    // Title is trimmed and blank becomes None on append.
    let mut input = f.input();
    input.title = Some("  Project Alpha  ".into());
    let first = f
        .service
        .append_record(&f.token, "space", "titled", &input)
        .unwrap();
    assert_eq!(first.record.title.as_deref(), Some("Project Alpha"));
    assert!(!first.replayed);

    // Blank title normalizes to None.
    let mut blank = f.input();
    blank.title = Some("   ".into());
    let blank_result = f
        .service
        .append_record(&f.token, "space", "blank", &blank)
        .unwrap();
    assert_eq!(blank_result.record.title, None);

    // Idempotency hashes the submitted input BEFORE normalization, so
    // " Topic " and "Topic" conflict under one reused key.
    let mut spaced = f.input();
    spaced.title = Some(" Topic ".into());
    let mut trimmed = f.input();
    trimmed.title = Some("Topic".into());
    f.service
        .append_record(&f.token, "space", "ws-key", &spaced)
        .unwrap();
    assert!(matches!(
        f.service
            .append_record(&f.token, "space", "ws-key", &trimmed),
        Err(BootstrapError::IdempotencyConflict)
    ));

    // Replay of the identical input returns the normalized title.
    let replay = f
        .service
        .append_record(&f.token, "space", "titled", &input)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.record.title.as_deref(), Some("Project Alpha"));

    // Reply titles are message-level: stored on the reply, retrievable
    // independently of the root's title.
    let mut reply_input = f.input();
    reply_input.title = Some("Reply subject".into());
    reply_input.relations = vec![domain::Relation {
        relation_type: domain::RelationType::ReplyTo,
        record_id: first.record.id.clone(),
    }];
    let reply = f
        .service
        .append_record(&f.token, "space", "reply", &reply_input)
        .unwrap();
    assert_eq!(reply.record.title.as_deref(), Some("Reply subject"));
    // The root's title is unchanged by the reply's title.
    let root_again = f.service.get_record(&f.token, &first.record.id).unwrap();
    assert_eq!(root_again.title.as_deref(), Some("Project Alpha"));
}
