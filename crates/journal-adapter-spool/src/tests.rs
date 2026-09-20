use super::*;
use journal_adapter_core::Envelope;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{process::Command, time::Duration};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!(
                "spool-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn open(&self) -> SqliteStore {
        SqliteStore::open(self.0.join("spool.db"), Limits::default()).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn item() -> SpoolItem {
    SpoolItem {
        record: None,
        pending_event: None,
        event_sequence: 0,
        runtime_failures: 0,
        mailbox_item_id: "item-1".into(),
        attempt_id: "attempt-1".into(),
        claim_id: "claim-1".into(),
        instance_id: "instance-1".into(),
        generation: 1,
        record_id: "record-1".into(),
        space_id: "space".into(),
        routing_key: None,
        envelope: Envelope {
            record_id: "record-1".into(),
            mailbox_item_id: "item-1".into(),
            attempt_id: "attempt-1".into(),
            space_id: "space".into(),
            from_principal: "source".into(),
            source_run: None,
            reply_to: None,
            addressed_to: "destination".into(),
            routing_key: None,
            body: "complete untrusted body".into(),
        },
        custody_confirmed: false,
        injection_state: InjectionState::Pending,
        runtime_receipt: String::new(),
        failure_detail: String::new(),
        next_runtime_try_at: None,
    }
}

#[test]
fn pressure_snapshot_matches_exact_admission_boundaries() {
    let limits = Limits {
        max_items: 2,
        max_bytes: 100,
        min_free_bytes: 50,
    };
    let required = 50 + 4 * 40 + 16384;
    let snapshot = PressureSnapshot::new(limits, 1, 60, required, 1, 40).unwrap();
    assert!(!snapshot.claiming_paused);
    assert!(
        PressureSnapshot::new(limits, 1, 60, required - 1, 1, 40)
            .unwrap()
            .claiming_paused
    );
    assert!(
        PressureSnapshot::new(limits, 2, 60, required, 1, 40)
            .unwrap()
            .claiming_paused
    );
    assert!(
        PressureSnapshot::new(limits, 1, 61, required, 1, 40)
            .unwrap()
            .claiming_paused
    );
    assert!(PressureSnapshot::new(limits, 0, 0, u64::MAX, 1, u64::MAX).is_err());
}

#[test]
fn pressure_snapshot_reports_persisted_rows_and_real_free_space() {
    let fixture = Fixture::new();
    let store = fixture.open();
    let value = item();
    store.put(&value).unwrap();
    let snapshot = store.pressure_snapshot(0, 0).unwrap();
    assert_eq!(snapshot.retained_items, 1);
    assert_eq!(
        snapshot.retained_bytes,
        serde_json::to_vec(&value).unwrap().len() as u64
    );
    assert!(snapshot.available_bytes > 0);
    assert!(!snapshot.claiming_paused);
    store.close().unwrap();
    assert!(store.pressure_snapshot(0, 0).is_err());
    let store = fixture.open();
    assert_eq!(store.pressure_snapshot(0, 0).unwrap().retained_items, 1);
}
fn confirm(store: &SqliteStore) {
    store
        .confirm_custody("attempt-1", "claim-1", "instance-1", 1)
        .unwrap();
}

#[cfg(unix)]
#[test]
fn inherited_lock_descriptor_does_not_keep_closed_spool_locked() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;

    for explicit in [false, true] {
        let fixture = Fixture::new();
        let store = fixture.open();
        let inherited = store
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .lock
            .file
            .try_clone()
            .unwrap();
        // Stdio safely duplicates the same open file description into a child,
        // without running Rust after fork in a multithreaded test process.
        let mut child = Command::new("python3")
            .args([
                "-c",
                "import time; print('ready', flush=True); time.sleep(60)",
            ])
            .stdin(Stdio::from(inherited))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready.trim(), "ready");
        if explicit {
            store.close().unwrap();
        }
        drop(store);
        let reopened = SqliteStore::open(fixture.0.join("spool.db"), Limits::default());
        let still_alive = child.try_wait().unwrap().is_none();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(still_alive);
        reopened.unwrap().close().unwrap();
    }
}

#[test]
fn current_spool_sidecars_are_refused_without_mutation() {
    for suffix in ["-wal", "-shm", "-journal"] {
        for contents in [b"".as_slice(), b"retained hot evidence".as_slice()] {
            let fixture = Fixture::new();
            let store = fixture.open();
            store.put(&item()).unwrap();
            drop(store);

            let spool = fixture.0.join("spool.db");
            let lock = fixture.0.join("spool.db.lock");
            let sidecar = PathBuf::from(format!("{}{suffix}", spool.display()));
            let _ = std::fs::remove_file(&sidecar);
            std::fs::write(&sidecar, contents).unwrap();
            let before = [spool.clone(), lock.clone(), sidecar.clone()]
                .into_iter()
                .map(|path| {
                    let metadata = std::fs::metadata(&path).unwrap();
                    let bytes = std::fs::read(&path).unwrap();
                    (path, bytes, metadata)
                })
                .collect::<Vec<_>>();

            assert!(SqliteStore::open(&spool, Limits::default()).is_err());
            for (path, bytes, metadata) in before {
                assert_eq!(std::fs::read(&path).unwrap(), bytes, "{suffix}: {path:?}");
                #[cfg(unix)]
                assert_eq!(std::fs::metadata(&path).unwrap().ino(), metadata.ino());
            }
        }
    }
}

#[cfg(unix)]
#[test]
fn dangling_spool_sidecar_symlinks_are_refused_without_mutation() {
    use std::os::unix::fs::symlink;

    for suffix in ["-wal", "-shm", "-journal"] {
        let fixture = Fixture::new();
        let store = fixture.open();
        store.put(&item()).unwrap();
        drop(store);

        let spool = fixture.0.join("spool.db");
        let lock = fixture.0.join("spool.db.lock");
        let sidecar = PathBuf::from(format!("{}{suffix}", spool.display()));
        let _ = std::fs::remove_file(&sidecar);
        let dangling_target = fixture.0.join(format!("missing-{suffix}"));
        symlink(&dangling_target, &sidecar).unwrap();
        let spool_bytes = std::fs::read(&spool).unwrap();
        let spool_inode = std::fs::metadata(&spool).unwrap().ino();
        let lock_inode = std::fs::metadata(&lock).unwrap().ino();
        let sidecar_inode = std::fs::symlink_metadata(&sidecar).unwrap().ino();

        assert!(matches!(
            SqliteStore::open(&spool, Limits::default()),
            Err(CoreError::SpoolUnavailable(message)) if message.contains("reset required")
        ));
        assert_eq!(std::fs::read(&spool).unwrap(), spool_bytes);
        assert_eq!(std::fs::metadata(&spool).unwrap().ino(), spool_inode);
        assert_eq!(std::fs::metadata(&lock).unwrap().ino(), lock_inode);
        assert_eq!(std::fs::read_link(&sidecar).unwrap(), dangling_target);
        assert_eq!(
            std::fs::symlink_metadata(&sidecar).unwrap().ino(),
            sidecar_inode
        );
        assert!(!dangling_target.exists());
    }
}

#[test]
fn current_spool_rejects_recreated_tables_and_indexes_with_weakened_definitions() {
    let mutations = [
        (
            "attempts-table",
            "DROP INDEX recovery;
             DROP INDEX outbox;
             ALTER TABLE attempts RENAME TO old_attempts;
             CREATE TABLE attempts (
                 attempt_id TEXT PRIMARY KEY NOT NULL,
                 fingerprint BLOB NOT NULL,
                 item TEXT NOT NULL,
                 bytes INTEGER NOT NULL,
                 terminal INTEGER NOT NULL,
                 retry_at INTEGER,
                 event_pending INTEGER NOT NULL DEFAULT 0
             ) STRICT;
             INSERT INTO attempts SELECT * FROM old_attempts;
             DROP TABLE old_attempts;
             CREATE INDEX recovery ON attempts(terminal, attempt_id);
             CREATE INDEX outbox ON attempts(event_pending, attempt_id);",
        ),
        (
            "recovery-index",
            "DROP INDEX recovery;
             CREATE INDEX recovery ON attempts(attempt_id);",
        ),
    ];
    for (label, mutation) in mutations {
        let fixture = Fixture::new();
        let store = fixture.open();
        store.close().unwrap();
        let spool = fixture.0.join("spool.db");
        let lock = fixture.0.join("spool.db.lock");
        let connection = Connection::open(&spool).unwrap();
        connection.execute_batch(mutation).unwrap();
        drop(connection);
        let spool_bytes = std::fs::read(&spool).unwrap();
        #[cfg(unix)]
        let spool_inode = std::fs::metadata(&spool).unwrap().ino();
        #[cfg(unix)]
        let lock_inode = std::fs::metadata(&lock).unwrap().ino();

        assert!(matches!(
            SqliteStore::open(&spool, Limits::default()),
            Err(CoreError::SpoolUnavailable(message)) if message.contains("reset required")
        ));
        assert_eq!(std::fs::read(&spool).unwrap(), spool_bytes, "{label}");
        #[cfg(unix)]
        {
            assert_eq!(
                std::fs::metadata(&spool).unwrap().ino(),
                spool_inode,
                "{label}"
            );
            assert_eq!(
                std::fs::metadata(&lock).unwrap().ino(),
                lock_inode,
                "{label}"
            );
        }
    }
}

#[test]
fn existing_spool_substitution_after_read_only_preflight_is_refused() {
    let fixture = Fixture::new();
    let store = fixture.open();
    store.put(&item()).unwrap();
    drop(store);

    let spool = fixture.0.join("spool.db");
    let lock = fixture.0.join("spool.db.lock");
    let replacement = fixture.0.join("replacement.db");
    std::fs::copy(&spool, &replacement).unwrap();
    let lock_metadata = std::fs::metadata(&lock).unwrap();
    let replacement_bytes = std::fs::read(&replacement).unwrap();
    *TEST_PRE_OPEN_SUBSTITUTION.lock().unwrap() = Some((spool.clone(), replacement));

    let result = SqliteStore::open(&spool, Limits::default());
    match result {
        Err(CoreError::SpoolUnavailable(message)) => {
            assert!(message.contains("changed during admission"), "{message}");
        }
        Err(error) => panic!("unexpected error: {error:?}"),
        Ok(_) => panic!("substituted spool opened"),
    }
    assert_eq!(std::fs::read(&spool).unwrap(), replacement_bytes);
    #[cfg(unix)]
    assert_eq!(std::fs::metadata(&lock).unwrap().ino(), lock_metadata.ino());
    for suffix in ["-journal", "-wal", "-shm"] {
        assert!(!PathBuf::from(format!("{}{suffix}", spool.display())).exists());
    }
}

fn start(store: &SqliteStore) {
    store
        .mark_injection_started("attempt-1", "instance-1", 1)
        .unwrap();
}

fn expired() -> journal_adapter_core::CustodyResult {
    use journal_adapter_core::{CustodyItemResult, CustodyResult, CustodyResultState};
    CustodyResult {
        claim_id: "claim-1".into(),
        generation: 1,
        items: vec![CustodyItemResult {
            mailbox_item_id: "item-1".into(),
            attempt_id: "attempt-1".into(),
            result: CustodyResultState::LeaseExpired,
        }],
    }
}

fn reclaimed() -> SpoolItem {
    let mut value = item();
    value.claim_id = "claim-2".into();
    value
}

#[test]
fn expired_claim_reconciles_after_restart_before_custody() {
    let fixture = Fixture::new();
    let store = fixture.open();
    store.put(&item()).unwrap();
    drop(store);
    let store = fixture.open();
    assert!(store.put(&reclaimed()).is_err());
    store
        .reconcile_expired_claim(&expired(), &reclaimed())
        .unwrap();
    store
        .reconcile_expired_claim(&expired(), &reclaimed())
        .unwrap();
    store.put(&reclaimed()).unwrap();
    assert!(store.put(&item()).is_err());
    assert!(!store.get("attempt-1").unwrap().ready_for_injection());
    assert!(
        store
            .confirm_custody("attempt-1", "claim-1", "instance-1", 1)
            .is_err()
    );
    drop(store);
    let store = fixture.open();
    assert_eq!(store.get("attempt-1").unwrap(), reclaimed());
    store
        .confirm_custody("attempt-1", "claim-2", "instance-1", 1)
        .unwrap();
    assert!(store.get("attempt-1").unwrap().ready_for_injection());
    assert!(
        store
            .reconcile_expired_claim(&expired(), &reclaimed())
            .is_err()
    );
}

#[test]
fn reconciliation_rejects_changed_bindings_and_uncertain_custody() {
    use journal_adapter_core::CustodyResultState;
    let fixture = Fixture::new();
    let store = fixture.open();
    store.put(&item()).unwrap();
    for field in [
        "instance",
        "generation",
        "body",
        "recipient",
        "mailbox",
        "claim",
    ] {
        let mut replacement = reclaimed();
        match field {
            "instance" => replacement.instance_id.push('x'),
            "generation" => replacement.generation += 1,
            "body" => replacement.envelope.body.push('x'),
            "recipient" => replacement.envelope.addressed_to.push('x'),
            "mailbox" => {
                replacement.mailbox_item_id.push('x');
                replacement.envelope.mailbox_item_id.push('x');
            }
            _ => replacement.claim_id.clear(),
        }
        assert!(
            store
                .reconcile_expired_claim(&expired(), &replacement)
                .is_err(),
            "{field}"
        );
    }
    for state in [
        CustodyResultState::Committed,
        CustodyResultState::AlreadyCommitted,
        CustodyResultState::ClaimNotFound,
        CustodyResultState::StaleGeneration,
        CustodyResultState::AttemptMismatch,
        CustodyResultState::SuppressedRevoked,
    ] {
        let mut result = expired();
        result.items[0].result = state;
        assert!(
            store
                .reconcile_expired_claim(&result, &reclaimed())
                .is_err()
        );
    }
    for field in ["claim", "generation", "attempt", "mailbox", "duplicate"] {
        let mut result = expired();
        match field {
            "claim" => result.claim_id.push('x'),
            "generation" => result.generation += 1,
            "attempt" => result.items[0].attempt_id.push('x'),
            "mailbox" => result.items[0].mailbox_item_id.push('x'),
            _ => result.items.push(result.items[0].clone()),
        }
        assert!(
            store
                .reconcile_expired_claim(&result, &reclaimed())
                .is_err(),
            "{field}"
        );
    }
    assert_eq!(store.get("attempt-1").unwrap(), item());
    confirm(&store);
    assert!(
        store
            .reconcile_expired_claim(&expired(), &reclaimed())
            .is_err()
    );
    start(&store);
    store
        .mark_injected("attempt-1", "instance-1", 1, "receipt")
        .unwrap();
    assert!(
        store
            .reconcile_expired_claim(&expired(), &reclaimed())
            .is_err()
    );
    store.compact("attempt-1").unwrap();
    assert!(
        store
            .reconcile_expired_claim(&expired(), &reclaimed())
            .is_err()
    );
}

#[test]
fn reconciliation_growth_is_bounded_and_sqlite_failure_rolls_back() {
    let fixture = Fixture::new();
    let store = fixture.open();
    store.put(&item()).unwrap();
    drop(store);
    let store = SqliteStore::open(
        fixture.0.join("spool.db"),
        Limits {
            max_bytes: serde_json::to_vec(&item()).unwrap().len() as u64,
            ..Limits::default()
        },
    )
    .unwrap();
    let mut larger = reclaimed();
    larger.claim_id.push('x');
    assert!(store.reconcile_expired_claim(&expired(), &larger).is_err());
    assert_eq!(store.get("attempt-1").unwrap(), item());
    drop(store);
    let store = fixture.open();
    store
        .with_connection(|connection| {
            connection
                .execute_batch("PRAGMA max_page_count=4")
                .map_err(unavailable)
        })
        .unwrap();
    larger.claim_id = "x".repeat(100_000);
    assert!(store.reconcile_expired_claim(&expired(), &larger).is_err());
    assert_eq!(store.get("attempt-1").unwrap(), item());
    store.put(&item()).unwrap();
    store
        .reconcile_expired_claim(&expired(), &reclaimed())
        .unwrap();
}

#[test]
fn complete_item_reopens_and_conflicting_bindings_fail() {
    let fixture = Fixture::new();
    let store = fixture.open();
    store.put(&item()).unwrap();
    store.put(&item()).unwrap();
    assert_eq!(store.get("attempt-1").unwrap(), item());
    assert!(
        store
            .mark_injection_started("attempt-1", "instance-1", 1)
            .is_err()
    );
    for field in ["claim", "instance", "generation", "body"] {
        let mut conflict = item();
        match field {
            "claim" => conflict.claim_id.push('x'),
            "instance" => conflict.instance_id.push('x'),
            "generation" => conflict.generation += 1,
            _ => conflict.envelope.body.push('x'),
        }
        assert!(store.put(&conflict).is_err());
    }
    assert!(
        store
            .confirm_custody("attempt-1", "wrong", "instance-1", 1)
            .is_err()
    );
    assert!(
        store
            .confirm_custody("attempt-1", "claim-1", "instance-1", 2)
            .is_err()
    );
    drop(store);
    let store = fixture.open();
    assert_eq!(store.get("attempt-1").unwrap(), item());
    assert_eq!(
        store.recoverable(SystemTime::now(), 1).unwrap(),
        vec![item()]
    );
}

#[test]
fn lifecycle_retry_and_terminal_tombstones() {
    for outcome in [
        InjectionState::Accepted,
        InjectionState::RouteUnavailable,
        InjectionState::TerminalFailure,
    ] {
        let fixture = Fixture::new();
        let store = fixture.open();
        store.put(&item()).unwrap();
        confirm(&store);
        confirm(&store);
        start(&store);
        start(&store);
        let later = SystemTime::now() + Duration::from_secs(60);
        store
            .mark_retryable("attempt-1", "instance-1", 1, "busy", later)
            .unwrap();
        store
            .mark_retryable("attempt-1", "instance-1", 1, "busy", later)
            .unwrap();
        assert!(store.recoverable(SystemTime::now(), 10).unwrap().is_empty());
        assert_eq!(store.recoverable(later, 10).unwrap().len(), 1);
        start(&store);
        if outcome == InjectionState::Accepted {
            store
                .mark_injected("attempt-1", "instance-1", 1, "receipt")
                .unwrap();
            store
                .mark_injected("attempt-1", "instance-1", 1, "receipt")
                .unwrap();
            assert!(
                store
                    .mark_injected("attempt-1", "instance-1", 1, "different")
                    .is_err()
            );
        } else {
            store
                .mark_injection_failed("attempt-1", "instance-1", 1, outcome, "failure")
                .unwrap();
            store
                .mark_injection_failed("attempt-1", "instance-1", 1, outcome, "failure")
                .unwrap();
        }
        store.compact("attempt-1").unwrap();
        store.compact("attempt-1").unwrap();
        store.put(&item()).unwrap();
        assert!(store.get("attempt-1").unwrap().envelope.body.is_empty());
        assert!(
            store
                .mark_injection_started("attempt-1", "instance-1", 1)
                .is_err()
        );
        assert!(
            store
                .mark_injection_failed(
                    "attempt-1",
                    "instance-1",
                    1,
                    InjectionState::RetryableFailure,
                    "retry"
                )
                .is_err()
        );
        drop(store);
        let store = fixture.open();
        assert!(store.recoverable(SystemTime::now(), 10).unwrap().is_empty());
        assert!(!store.get("attempt-1").unwrap().ready_for_injection());
    }
}

#[test]
fn pressure_and_bounded_recovery() {
    let fixture = Fixture::new();
    let store = SqliteStore::open(
        fixture.0.join("spool.db"),
        Limits {
            max_items: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    store.check_capacity(1, 1000).unwrap();
    store.put(&item()).unwrap();
    assert!(store.check_capacity(1, 1).is_err());
    store.put(&item()).unwrap();
    assert!(store.recoverable(SystemTime::now(), 0).is_err());
    assert!(
        store
            .recoverable(SystemTime::now(), MAX_RECOVERY_BATCH + 1)
            .is_err()
    );
    assert!(
        store
            .recoverable_after(SystemTime::now(), 1, Some("attempt-1"))
            .unwrap()
            .is_empty()
    );
    drop(store);
    for limits in [
        Limits {
            max_bytes: 1,
            ..Limits::default()
        },
        Limits {
            min_free_bytes: fs2::total_space(&fixture.0).unwrap(),
            ..Limits::default()
        },
    ] {
        let store = SqliteStore::open(fixture.0.join("spool.db"), limits).unwrap();
        assert!(store.check_capacity(1, 1000).is_err());
    }
}

#[test]
fn corrupt_database_is_rejected() {
    let fixture = Fixture::new();
    std::fs::write(fixture.0.join("spool.db"), b"not a database").unwrap();
    assert!(SqliteStore::open(fixture.0.join("spool.db"), Limits::default()).is_err());
}

#[test]
fn exact_byte_limits_and_invalid_inputs() {
    let fixture = Fixture::new();
    let mut value = item();
    value.envelope.body = "é".repeat(200);
    let bytes = serde_json::to_vec(&value).unwrap().len() as u64;
    let path = fixture.0.join("spool.db");
    let store = SqliteStore::open(
        &path,
        Limits {
            max_bytes: bytes - 1,
            ..Limits::default()
        },
    )
    .unwrap();
    assert!(store.put(&value).is_err());
    assert!(store.get(&value.attempt_id).is_err());
    drop(store);
    let store = SqliteStore::open(
        &path,
        Limits {
            max_bytes: bytes,
            ..Limits::default()
        },
    )
    .unwrap();
    store.put(&value).unwrap();
    let mut invalid = item();
    invalid.envelope.attempt_id = "other".into();
    assert!(store.put(&invalid).is_err());
    invalid = item();
    invalid.custody_confirmed = true;
    assert!(store.put(&invalid).is_err());
    confirm(&store);
    assert!(store.compact("attempt-1").is_err());
    assert!(
        store
            .mark_injected("attempt-1", "instance-1", 1, "receipt")
            .is_err()
    );
    start(&store);
    assert!(
        store
            .mark_injected("attempt-1", "other", 1, "receipt")
            .is_err()
    );
    assert!(
        store
            .mark_injection_failed(
                "attempt-1",
                "instance-1",
                1,
                InjectionState::Accepted,
                "bad"
            )
            .is_err()
    );
    assert!(
        store
            .mark_injection_failed(
                "attempt-1",
                "instance-1",
                1,
                InjectionState::RetryableFailure,
                &"é".repeat(2049)
            )
            .is_err()
    );
    store
        .mark_injection_failed(
            "attempt-1",
            "instance-1",
            1,
            InjectionState::RetryableFailure,
            &"é".repeat(2048),
        )
        .unwrap();
    store.close().unwrap();
    store.close().unwrap();
    assert!(store.get("attempt-1").is_err());
    drop(store);
    fixture.open();
}

#[test]
fn sqlite_full_rolls_back_put_and_transition() {
    let fixture = Fixture::new();
    let store = fixture.open();
    store
        .with_connection(|connection| {
            connection
                .execute_batch("PRAGMA max_page_count=4")
                .map_err(unavailable)
        })
        .unwrap();
    let mut huge = item();
    huge.envelope.body = "x".repeat(100_000);
    assert!(store.put(&huge).is_err());
    assert!(store.get("attempt-1").is_err());
    store.put(&item()).unwrap();
    confirm(&store);
    start(&store);
    let before = store.get("attempt-1").unwrap();
    assert!(
        store
            .mark_injected("attempt-1", "instance-1", 1, &"r".repeat(4096))
            .is_err()
    );
    assert_eq!(store.get("attempt-1").unwrap(), before);
    drop(store);
    assert_eq!(fixture.open().get("attempt-1").unwrap(), before);
}

#[test]
fn recovery_keyset_does_not_starve_unconfirmed_rows() {
    let fixture = Fixture::new();
    let store = fixture.open();
    for number in 0..5 {
        let mut value = item();
        value.attempt_id = format!("attempt-{number}");
        value.envelope.attempt_id = value.attempt_id.clone();
        store.put(&value).unwrap();
    }
    let first = store.recoverable(SystemTime::now(), 2).unwrap();
    let second = store
        .recoverable_after(SystemTime::now(), 2, Some(&first[1].attempt_id))
        .unwrap();
    let last = store
        .recoverable_after(SystemTime::now(), 2, Some(&second[1].attempt_id))
        .unwrap();
    assert_eq!(first[0].attempt_id, "attempt-0");
    assert_eq!(second[0].attempt_id, "attempt-2");
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].attempt_id, "attempt-4");
    assert!(first.iter().all(|row| !row.ready_for_injection()));
}

#[test]
fn malformed_rows_and_foreign_schema_fail_closed() {
    let fixture = Fixture::new();
    let store = fixture.open();
    store.put(&item()).unwrap();
    store
        .with_connection(|connection| {
            connection
                .execute("UPDATE attempts SET item='{}'", [])
                .map_err(unavailable)?;
            Ok(())
        })
        .unwrap();
    assert!(store.get("attempt-1").is_err());
    assert!(store.recoverable(SystemTime::now(), 1).is_err());
    drop(store);
    let connection = Connection::open(fixture.0.join("spool.db")).unwrap();
    connection.execute_batch("PRAGMA user_version=4").unwrap();
    drop(connection);
    assert!(SqliteStore::open(fixture.0.join("spool.db"), Limits::default()).is_err());
}

#[test]
fn crash_child() {
    let Ok(path) = std::env::var("SPOOL_CHILD_PATH") else {
        return;
    };
    let operation = std::env::var("SPOOL_CHILD_OPERATION").unwrap();
    if operation == "lock" {
        assert!(SqliteStore::open(Path::new(&path).join("spool.db"), Limits::default()).is_err());
        return;
    }
    let store = SqliteStore::open(Path::new(&path).join("spool.db"), Limits::default()).unwrap();
    match operation.as_str() {
        "reconcile" => store
            .reconcile_expired_claim(&expired(), &reclaimed())
            .unwrap(),
        "put" => store.put(&item()).unwrap(),
        "custody" => confirm(&store),
        "start" => start(&store),
        "accepted" => store
            .mark_injected("attempt-1", "instance-1", 1, "receipt")
            .unwrap(),
        "retry" => store
            .mark_retryable(
                "attempt-1",
                "instance-1",
                1,
                "busy",
                SystemTime::UNIX_EPOCH + Duration::from_secs(42),
            )
            .unwrap(),
        "route" => store
            .mark_injection_failed(
                "attempt-1",
                "instance-1",
                1,
                InjectionState::RouteUnavailable,
                "missing",
            )
            .unwrap(),
        "terminal" => store
            .mark_injection_failed(
                "attempt-1",
                "instance-1",
                1,
                InjectionState::TerminalFailure,
                "failed",
            )
            .unwrap(),
        "compact" => store.compact("attempt-1").unwrap(),
        "outcome" => outcome(&store),
        "ack-event" => store
            .acknowledge_event(
                "attempt-1",
                &store.get("attempt-1").unwrap().pending_event.unwrap(),
            )
            .unwrap(),
        _ => panic!("unknown operation"),
    }
}

fn child(fixture: &Fixture, operation: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "tests::crash_child", "--nocapture"])
        .env("SPOOL_CHILD_PATH", &fixture.0)
        .env("SPOOL_CHILD_OPERATION", operation);
    command
}

fn outcome(store: &SqliteStore) {
    let before = store.get("attempt-1").unwrap();
    let mut after = before.clone();
    after.injection_state = InjectionState::Accepted;
    after.runtime_receipt = "receipt".into();
    after.event_sequence = 1;
    after.pending_event = Some(journal_adapter_core::EventRequest {
        attempt_id: before.attempt_id.clone(),
        event_id: "event-1".into(),
        generation: 1,
        occurred_at: "2023-11-14T22:13:20Z".into(),
        state: journal_adapter_core::OutcomeState::AdapterReportedRuntimeAccepted,
        detail: Default::default(),
    });
    store.finish(&before, &after).unwrap();
}

#[test]
fn retained_legacy_spool_requires_archive_or_reset_without_lock_replacement() {
    let fixture = Fixture::new();
    let path = fixture.0.join("spool.db");
    let store = fixture.open();
    store.put(&item()).unwrap();
    drop(store);
    let lock = fixture.0.join("spool.db.lock");
    let lock_id = std::fs::metadata(&lock).unwrap().ino();
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("PRAGMA user_version=1").unwrap();
    drop(connection);
    let before = std::fs::read(&path).unwrap();
    assert!(SqliteStore::open(&path, Limits::default()).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(std::fs::metadata(lock).unwrap().ino(), lock_id);
}

#[test]
fn process_lock_is_exclusive_and_released() {
    let fixture = Fixture::new();
    let store = fixture.open();
    assert!(child(&fixture, "lock").status().unwrap().success());
    drop(store);
    assert!(child(&fixture, "put").status().unwrap().success());
}

#[test]
fn kill_before_and_after_every_commit() {
    for operation in [
        "put",
        "reconcile",
        "custody",
        "start",
        "accepted",
        "retry",
        "route",
        "terminal",
        "compact",
        "outcome",
        "ack-event",
    ] {
        for phase in ["before", "after"] {
            let fixture = Fixture::new();
            let store = fixture.open();
            if operation != "put" {
                store.put(&item()).unwrap();
            }
            if !matches!(operation, "put" | "reconcile" | "custody") {
                confirm(&store);
            }
            if !matches!(operation, "put" | "reconcile" | "custody" | "start") {
                start(&store);
            }
            if operation == "compact" {
                store
                    .mark_injected("attempt-1", "instance-1", 1, "receipt")
                    .unwrap();
            }
            if operation == "ack-event" {
                outcome(&store);
            }
            drop(store);
            let signal = fixture.0.join("checkpoint");
            let mut process = child(&fixture, operation)
                .env("SPOOL_CRASH_PHASE", phase)
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while !signal.exists() {
                assert!(
                    process.try_wait().unwrap().is_none(),
                    "child exited before checkpoint"
                );
                if std::time::Instant::now() > deadline {
                    process.kill().unwrap();
                    process.wait().unwrap();
                    panic!("checkpoint timed out");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            process.kill().unwrap();
            process.wait().unwrap();
            if phase == "before" {
                let artifacts = [
                    fixture.0.join("spool.db"),
                    fixture.0.join("spool.db.lock"),
                    fixture.0.join("spool.db-journal"),
                    fixture.0.join("spool.db-wal"),
                    fixture.0.join("spool.db-shm"),
                ]
                .into_iter()
                .filter(|path| path.exists())
                .map(|path| {
                    let metadata = std::fs::metadata(&path).unwrap();
                    let bytes = std::fs::read(&path).unwrap();
                    (path, bytes, metadata)
                })
                .collect::<Vec<_>>();
                assert!(artifacts.iter().any(|(path, _, _)| {
                    path.file_name()
                        .is_some_and(|name| name != "spool.db" && name != "spool.db.lock")
                }));
                assert!(SqliteStore::open(fixture.0.join("spool.db"), Limits::default()).is_err());
                for (path, bytes, metadata) in artifacts {
                    assert_eq!(std::fs::read(&path).unwrap(), bytes, "{operation}");
                    #[cfg(unix)]
                    assert_eq!(std::fs::metadata(&path).unwrap().ino(), metadata.ino());
                }
                continue;
            }
            let store = fixture.open();
            let after = store.get("attempt-1").ok();
            let saved = after.unwrap();
            match operation {
                "reconcile" => {
                    assert_eq!(saved, reclaimed());
                    store.put(&reclaimed()).unwrap();
                    assert!(store.put(&item()).is_err());
                }
                "put" => assert_eq!(saved, item()),
                "custody" => assert!(saved.custody_confirmed),
                "start" => assert_eq!(saved.injection_state, InjectionState::InFlight),
                "accepted" => assert_eq!(saved.runtime_receipt, "receipt"),
                "retry" => assert_eq!(
                    saved.next_runtime_try_at,
                    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(42))
                ),
                "route" => assert_eq!(saved.injection_state, InjectionState::RouteUnavailable),
                "terminal" => {
                    assert_eq!(saved.injection_state, InjectionState::TerminalFailure)
                }
                "compact" => assert!(saved.envelope.body.is_empty()),
                "outcome" => {
                    assert_eq!(saved.injection_state, InjectionState::Accepted);
                    assert!(saved.pending_event.is_some());
                    assert_eq!(
                        store.work_after(SystemTime::now(), None).unwrap(),
                        Some(saved)
                    );
                }
                "ack-event" => {
                    assert_eq!(saved.injection_state, InjectionState::Accepted);
                    assert!(saved.pending_event.is_none());
                }
                _ => unreachable!(),
            }
        }
    }
}
