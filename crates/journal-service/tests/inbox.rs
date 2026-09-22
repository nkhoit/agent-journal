use journal_protocol::{domain::*, *};
use journal_service::{BootstrapError, BootstrapService, Clock, SecretSource};
use journal_storage_sqlite::Database;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime};

struct Sources(AtomicU64);
impl Clock for Sources {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000 + self.0.load(Ordering::SeqCst))
    }
}
impl SecretSource for Sources {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        for chunk in bytes.chunks_exact_mut(8) {
            chunk.copy_from_slice(&n.to_be_bytes());
        }
        Ok(())
    }
}
struct Fixture {
    db: Database,
    service: BootstrapService,
    alpha: String,
    beta: String,
    gamma: String,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "inbox-{}-{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let db = Database::open(&path).unwrap();
        let sources = Arc::new(Sources(AtomicU64::new(1)));
        let service = BootstrapService::with_sources(db.clone(), sources.clone(), sources);
        let f = Self {
            db,
            service,
            alpha: "a".repeat(64),
            beta: "b".repeat(64),
            gamma: "c".repeat(64),
            path,
        };
        for (token, handle) in [(&f.alpha, "alpha"), (&f.beta, "beta"), (&f.gamma, "gamma")] {
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
        f.service
            .create_space(&SpaceCreateRequest {
                id: "public".into(),
                name: "Public".into(),
                access: SpaceAccess::Public,
            })
            .unwrap();
        f
    }
    fn input(&self) -> RecordInput {
        RecordInput {
            kind: "message".into(),
            content: "Untrusted message".into(),
            run_id: None,
            attention: vec!["beta".into()],
            routing_key: None,
            relations: vec![],
            title: None,
        }
    }
    fn post(&self, key: &str) -> AppendResult {
        self.service
            .append_record(&self.alpha, "public", key, &self.input())
            .unwrap()
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

#[test]
fn independent_principals_replay_ack_and_archive_without_adapters() {
    let f = Fixture::new();
    let posted = f.post("once");
    let replay = f.post("once");
    assert!(replay.replayed);
    assert_eq!(replay.record, posted.record);
    assert_eq!(replay.mailbox_created, posted.mailbox_created);
    for table in ["records", "mailbox_items", "inbox_sequences"] {
        assert_eq!(f.count(table), 1);
    }
    assert_eq!(f.count("memberships"), 0);
    let page = f.service.inbox(&f.beta, &InboxQuery::default()).unwrap();
    let item = &page.items[0];
    assert_eq!(item.seq, 1);
    assert_eq!(item.record, posted.record);
    assert!(item.acknowledged_at.is_none());
    assert!(page.next_cursor.is_none());
    assert!(matches!(
        f.service
            .acknowledge_inbox_item(&f.alpha, &item.inbox_item_id),
        Err(BootstrapError::NotFound)
    ));
    assert!(matches!(
        f.service
            .delivery_status(&f.gamma, &posted.record.id, &PageQuery::default()),
        Err(BootstrapError::NotFound)
    ));
    f.db.connect()
        .unwrap()
        .execute("UPDATE spaces SET archived_at='2027-01-01T00:00:00Z'", [])
        .unwrap();
    f.service
        .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
        .unwrap();
    let all = InboxQuery::from_query("state=all").unwrap();
    let acked = f.service.inbox(&f.beta, &all).unwrap();
    assert!(acked.items[0].acknowledged_at.is_some());
    f.service
        .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
        .unwrap();
    let restarted = BootstrapService::new(Database::open(&f.path).unwrap());
    assert_eq!(restarted.inbox(&f.beta, &all).unwrap(), acked);
    assert!(
        restarted
            .inbox(&f.beta, &InboxQuery::default())
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(f.count("records"), 1);
    for token in [&f.alpha, &f.beta] {
        let status = f
            .service
            .delivery_status(token, &posted.record.id, &PageQuery::default())
            .unwrap();
        assert_eq!(status.items[0].state, ReceiptState::Acknowledged);
        assert_eq!(
            status.items[0].acknowledged_at,
            acked.items[0].acknowledged_at
        );
    }
}

#[test]
fn bounded_pass_finishes_despite_arrivals_and_retries_early_failure() {
    let f = Fixture::new();
    for n in 0..121 {
        f.post(&format!("initial-{n}"));
    }
    let mut query = InboxQuery::from_query("limit=50").unwrap();
    let first = f.service.inbox(&f.beta, &query).unwrap();
    assert_eq!(first.items.len(), 50);
    let failed = first.items[0].inbox_item_id.clone();
    for item in first.items.iter().skip(1) {
        f.service
            .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
            .unwrap();
    }
    query.page.cursor = first.next_cursor;
    let mut seen = 50;
    while query.page.cursor.is_some() {
        f.post(&format!("arrival-{seen}"));
        let page = f.service.inbox(&f.beta, &query).unwrap();
        assert!(!page.items.is_empty());
        for item in &page.items {
            assert!(item.seq <= 121);
            f.service
                .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
                .unwrap();
        }
        seen += page.items.len();
        assert!(seen <= 121);
        query.page.cursor = page.next_cursor;
    }
    assert_eq!(seen, 121);
    let next_pass = f.service.inbox(&f.beta, &InboxQuery::default()).unwrap();
    assert_eq!(next_pass.items[0].inbox_item_id, failed);
    assert!(next_pass.items.iter().skip(1).all(|item| item.seq > 121));
}

#[test]
fn cursor_scope_and_current_authority_are_checked() {
    let f = Fixture::new();
    f.post("one");
    f.post("two");
    let first = f
        .service
        .inbox(&f.beta, &InboxQuery::from_query("limit=1").unwrap())
        .unwrap();
    let mut query = InboxQuery::default();
    query.page.cursor = first.next_cursor;
    assert!(f.service.inbox(&f.gamma, &query).is_err());
    let mut changed = query.clone();
    changed.state = InboxState::All;
    assert!(f.service.inbox(&f.beta, &changed).is_err());
    let mut tampered = query.clone();
    tampered.page.cursor.as_mut().unwrap().push('x');
    assert!(f.service.inbox(&f.beta, &tampered).is_err());
    f.db.connect()
        .unwrap()
        .execute(
            "UPDATE principals SET disabled_at='2027-01-01T00:00:00Z' WHERE id=?",
            [&first.items[0].recipient],
        )
        .unwrap();
    assert!(matches!(
        f.service.inbox(&f.beta, &query),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service
            .acknowledge_inbox_item(&f.beta, &first.items[0].inbox_item_id),
        Err(BootstrapError::Unauthorized)
    ));
}

#[test]
fn multi_recipient_append_and_ack_failures_are_atomic() {
    let f = Fixture::new();
    let mut input = f.input();
    input.attention.push("gamma".into());
    f.db.connect().unwrap().execute_batch("CREATE TRIGGER fail_second_inbox BEFORE INSERT ON mailbox_items WHEN (SELECT count(*) FROM mailbox_items)=1 BEGIN SELECT RAISE(ABORT,'injected second recipient failure'); END;").unwrap();
    assert!(
        f.service
            .append_record(&f.alpha, "public", "rollback", &input)
            .is_err()
    );
    for table in [
        "records",
        "attention",
        "mailbox_items",
        "inbox_sequences",
        "idempotency_keys",
    ] {
        assert_eq!(f.count(table), 0, "{table}");
    }
    f.db.connect()
        .unwrap()
        .execute_batch("DROP TRIGGER fail_second_inbox")
        .unwrap();
    f.post("ack");
    let item = f
        .service
        .inbox(&f.beta, &InboxQuery::default())
        .unwrap()
        .items
        .remove(0);
    assert!(
        f.service
            .clone()
            .with_failpoint("inbox-acknowledged")
            .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
            .is_err()
    );
    assert!(
        f.service
            .inbox(&f.beta, &InboxQuery::default())
            .unwrap()
            .items[0]
            .acknowledged_at
            .is_none()
    );
}

#[test]
fn concurrent_append_and_ack_keep_unique_sequences_and_first_timestamp() {
    let f = Fixture::new();
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|n| {
                let f = &f;
                scope.spawn(move || f.post(&format!("parallel-{n}")))
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let items = f
        .service
        .inbox(&f.beta, &InboxQuery::default())
        .unwrap()
        .items;
    assert_eq!(
        items.iter().map(|item| item.seq).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let f = &f;
                let id = &items[0].inbox_item_id;
                scope.spawn(move || f.service.acknowledge_inbox_item(&f.beta, id).unwrap())
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let all = InboxQuery::from_query("state=all").unwrap();
    let before = f.service.inbox(&f.beta, &all).unwrap();
    f.service
        .acknowledge_inbox_item(&f.beta, &items[0].inbox_item_id)
        .unwrap();
    assert_eq!(f.service.inbox(&f.beta, &all).unwrap(), before);
}

#[test]
fn inbox_reads_do_not_take_writer_lock_and_recovery_epoch_invalidates_cursors() {
    let f = Fixture::new();
    f.post("first");
    f.post("second");
    let first = f
        .service
        .inbox(&f.beta, &InboxQuery::from_query("limit=1").unwrap())
        .unwrap();
    let mut connection = f.db.connect().unwrap();
    let writer = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    writer
        .execute(
            "UPDATE mailbox_items SET acknowledged_at='2027-01-01T00:00:00Z'",
            [],
        )
        .unwrap();
    assert_eq!(
        f.service
            .inbox(&f.beta, &InboxQuery::default())
            .unwrap()
            .items
            .len(),
        2
    );
    drop(writer);
    connection
        .execute("UPDATE recovery_anchor SET inbox_epoch=10", [])
        .unwrap();
    let mut query = InboxQuery::default();
    query.page.cursor = first.next_cursor;
    assert!(f.service.inbox(&f.beta, &query).is_err());
    assert_eq!(
        f.service
            .inbox(&f.beta, &InboxQuery::default())
            .unwrap()
            .items
            .len(),
        2
    );
}

#[test]
fn receipt_authentication_is_current_even_after_acknowledgment() {
    for mutation in [
        "UPDATE credentials SET revoked_at='2000-01-01T00:00:00Z' WHERE principal_id=?",
        "UPDATE credentials SET expires_at='2000-01-01T00:00:00Z' WHERE principal_id=?",
    ] {
        let f = Fixture::new();
        f.post("auth");
        let item = f
            .service
            .inbox(&f.beta, &InboxQuery::default())
            .unwrap()
            .items
            .remove(0);
        f.service
            .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
            .unwrap();
        f.db.connect()
            .unwrap()
            .execute(mutation, [&item.recipient])
            .unwrap();
        assert!(matches!(
            f.service
                .inbox(&f.beta, &InboxQuery::from_query("state=all").unwrap()),
            Err(BootstrapError::Unauthorized)
        ));
        assert!(matches!(
            f.service
                .acknowledge_inbox_item(&f.beta, &item.inbox_item_id),
            Err(BootstrapError::Unauthorized)
        ));
        assert_eq!(f.count("records"), 1);
    }
}

#[test]
fn legacy_state_cannot_mutate_inbox_receipts() {
    let f = Fixture::new();
    f.post("legacy");
    let original = f.service.inbox(&f.beta, &InboxQuery::default()).unwrap();
    for state in [
        "pending",
        "claimed",
        "host-accepted",
        "adapter-reported-runtime-accepted",
        "adapter-reported-retryable-failure",
        "route-unavailable",
        "adapter-reported-terminal-failure",
        "suppressed-revoked",
    ] {
        let connection = f.db.connect().unwrap();
        assert!(
            connection
                .execute("UPDATE mailbox_items SET state=?", [state])
                .is_err()
        );
        assert!(
            connection
                .execute("UPDATE delivery_attempts SET state=?", [state])
                .is_err()
        );
        assert_eq!(
            f.service.inbox(&f.beta, &InboxQuery::default()).unwrap(),
            original
        );
    }
    f.service
        .acknowledge_inbox_item(&f.beta, &original.items[0].inbox_item_id)
        .unwrap();
    let acked = f
        .service
        .inbox(&f.beta, &InboxQuery::from_query("state=all").unwrap())
        .unwrap();
    assert!(
        f.db.connect()
            .unwrap()
            .execute("UPDATE mailbox_items SET state='pending'", [])
            .is_err()
    );
    assert_eq!(
        f.service
            .inbox(&f.beta, &InboxQuery::from_query("state=all").unwrap())
            .unwrap(),
        acked
    );
}

#[test]
fn no_attention_and_exhausted_allocation_do_not_create_partial_inbox_items() {
    let f = Fixture::new();
    let mut input = f.input();
    input.attention.clear();
    f.service
        .append_record(&f.alpha, "public", "no-attention", &input)
        .unwrap();
    assert_eq!(f.count("inbox_sequences"), 0);
    assert_eq!(f.count("mailbox_items"), 0);
    f.post("one");
    f.db.connect()
        .unwrap()
        .execute(
            "UPDATE inbox_sequences SET last_seq=9223372036854775807",
            [],
        )
        .unwrap();
    assert!(
        f.service
            .append_record(&f.alpha, "public", "overflow", &f.input())
            .is_err()
    );
    assert_eq!(f.count("records"), 2);
    assert_eq!(f.count("mailbox_items"), 1);
    assert_eq!(f.count("idempotency_keys"), 2);
}

#[cfg(unix)]
#[test]
fn fetch_status_and_repeated_ack_do_not_advance_external_audit() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new();
    let posted = f.post("protected");
    let directory = f.path.with_extension("protected");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let central = directory.join("central.db");
    f.db.backup_to(&central).unwrap();
    let db = Database::open_protected(&central, directory.join("audit.db")).unwrap();
    let service = BootstrapService::new(db.clone());
    let revision = || {
        db.connect_read_only()
            .unwrap()
            .query_row("SELECT revision FROM recovery_anchor", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
    };
    let before = revision();
    let item = service
        .inbox(&f.beta, &InboxQuery::default())
        .unwrap()
        .items
        .remove(0);
    service
        .delivery_status(&f.alpha, &posted.record.id, &PageQuery::default())
        .unwrap();
    assert_eq!(revision(), before);
    service
        .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
        .unwrap();
    let committed = revision();
    assert_eq!(committed, before + 1);
    service
        .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
        .unwrap();
    service.inbox(&f.beta, &InboxQuery::default()).unwrap();
    assert_eq!(revision(), committed);
    drop(service);
    drop(db);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn ack_crash_child() {
    let Some(path) = std::env::var_os("JOURNAL_INBOX_CRASH_PATH") else {
        return;
    };
    let stage = std::env::var("JOURNAL_INBOX_CRASH_STAGE").unwrap();
    let marker = std::path::PathBuf::from(std::env::var_os("JOURNAL_INBOX_CRASH_MARKER").unwrap());
    let db = Database::open(path).unwrap();
    let token = "b".repeat(64);
    let service = BootstrapService::new(db.clone());
    let item = service
        .inbox(&token, &InboxQuery::default())
        .unwrap()
        .items
        .remove(0);
    if stage == "before" {
        let mut connection = db.connect().unwrap();
        let tx = connection.transaction().unwrap();
        tx.execute(
            "UPDATE mailbox_items SET acknowledged_at='2027-01-01T00:00:00Z' WHERE id=?",
            [&item.inbox_item_id],
        )
        .unwrap();
        std::fs::write(marker, b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
    service
        .acknowledge_inbox_item(&token, &item.inbox_item_id)
        .unwrap();
    let ack = service
        .inbox(&token, &InboxQuery::from_query("state=all").unwrap())
        .unwrap()
        .items
        .remove(0)
        .acknowledged_at
        .unwrap();
    std::fs::write(marker, ack).unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn process_kill_preserves_ack_commit_boundary_and_original_timestamp() {
    for stage in ["before", "after"] {
        let f = Fixture::new();
        f.post("crash");
        let marker = f.path.with_extension("ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "ack_crash_child", "--nocapture"])
            .env("JOURNAL_INBOX_CRASH_PATH", &f.path)
            .env("JOURNAL_INBOX_CRASH_STAGE", stage)
            .env("JOURNAL_INBOX_CRASH_MARKER", &marker)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !marker.exists() && std::time::Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let reached = marker.exists();
        let _ = child.kill();
        child.wait().unwrap();
        assert!(reached, "child failed to reach {stage}");
        let item = f
            .service
            .inbox(&f.beta, &InboxQuery::from_query("state=all").unwrap())
            .unwrap()
            .items
            .remove(0);
        if stage == "before" {
            assert!(item.acknowledged_at.is_none());
        } else {
            assert_eq!(
                item.acknowledged_at.as_deref(),
                Some(std::fs::read_to_string(&marker).unwrap().as_str())
            );
            f.service
                .acknowledge_inbox_item(&f.beta, &item.inbox_item_id)
                .unwrap();
            assert_eq!(
                f.service
                    .inbox(&f.beta, &InboxQuery::from_query("state=all").unwrap())
                    .unwrap()
                    .items[0],
                item
            );
        }
        std::fs::remove_file(marker).unwrap();
    }
}
