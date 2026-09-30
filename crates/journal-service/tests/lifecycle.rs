#![cfg(unix)]

use journal_protocol::*;
use journal_service::{BootstrapError, BootstrapService, Clock};
use journal_storage_sqlite::Database;
use std::{
    os::unix::fs::PermissionsExt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
}
struct Directory(std::path::PathBuf);
impl std::ops::Deref for Directory {
    type Target = std::path::Path;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Fixture {
    directory: Directory,
    database: Database,
    service: BootstrapService,
    clock: Arc<TestClock>,
    principal: String,
    token: String,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "lifecycle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let database =
            Database::open_protected(directory.join("central.db"), directory.join("audit.db"))
                .unwrap();
        let clock = Arc::new(TestClock(AtomicU64::new(1_800_000_000)));
        let service = BootstrapService::with_sources(
            database.clone(),
            clock.clone(),
            Arc::new(journal_service::OsSecretSource),
        );
        let token = "a".repeat(64);
        let principal = service
            .register(
                &token,
                &RegistrationRequest {
                    handle: "alpha".into(),
                    display_name: "Alpha".into(),
                },
            )
            .unwrap()
            .receipt
            .principal
            .id;
        service
            .create_space(&SpaceCreateRequest {
                id: "space".into(),
                name: "Space".into(),
                access: domain::SpaceAccess::Public,
            })
            .unwrap();
        Self {
            directory: Directory(directory),
            database,
            service,
            clock,
            principal,
            token,
        }
    }
    fn state(&self, disabled: bool, reason: &str) -> PrincipalStateRequest {
        PrincipalStateRequest {
            principal_id: self.principal.clone(),
            disabled,
            reason: Some(reason.into()),
        }
    }
    fn archive(&self, archived: bool, reason: &str) -> SpaceArchiveRequest {
        SpaceArchiveRequest {
            space_id: "space".into(),
            archived,
            reason: Some(reason.into()),
        }
    }
    fn history(&self) -> Vec<(String, String, String, String)> {
        self.database
            .connect_read_only()
            .unwrap()
            .prepare("SELECT id,event_type,detail_json,created_at FROM audit_events ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
    fn revision(&self) -> i64 {
        self.database
            .connect_read_only()
            .unwrap()
            .query_row("SELECT revision FROM recovery_anchor", [], |r| r.get(0))
            .unwrap()
    }
    fn tick(&self) {
        self.clock.0.fetch_add(60, Ordering::SeqCst);
    }
}

fn input(attention: Vec<String>) -> AppendRecordRequest {
    decode_json(
        serde_json::json!({"kind":"note","content":"lifecycle history","attention":attention})
            .to_string()
            .as_bytes(),
    )
    .unwrap()
}

#[test]
fn lifecycle_retries_preserve_timestamps_reasons_and_revocations() {
    let f = Fixture::new();
    let committed = f
        .service
        .append_record(&f.token, "space", "committed", &input(vec![]))
        .unwrap();
    let disabled = f
        .service
        .set_principal_state(&f.state(true, "suspended"))
        .unwrap();
    assert!(disabled.principal.disabled);
    assert!(disabled.disabled_at.is_some());
    let history = f.history();
    let revision = f.revision();
    f.tick();
    assert_eq!(
        f.service
            .set_principal_state(&f.state(true, "retry reason"))
            .unwrap(),
        disabled
    );
    assert_eq!(f.history(), history);
    assert_eq!(f.revision(), revision);
    let connection = f.database.connect_read_only().unwrap();
    let revocation: (String, String) = connection
        .query_row(
            "SELECT revoked_at,revocation_reason FROM credentials WHERE principal_id=?",
            [&f.principal],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(Some(revocation.0.clone()), disabled.disabled_at);
    assert_eq!(revocation.1, "suspended");
    assert!(matches!(
        f.service
            .append_record(&f.token, "space", "committed", &input(vec![])),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service.get_record(&f.token, &committed.record.id),
        Err(BootstrapError::Unauthorized)
    ));
    let enabled = f
        .service
        .set_principal_state(&f.state(false, "resumed"))
        .unwrap();
    assert!(!enabled.principal.disabled);
    assert!(enabled.disabled_at.is_none());
    let history = f.history();
    let revision = f.revision();
    f.tick();
    assert_eq!(
        f.service
            .set_principal_state(&f.state(false, "retry"))
            .unwrap(),
        enabled
    );
    assert_eq!(f.history(), history);
    assert_eq!(f.revision(), revision);
    assert!(matches!(
        f.service
            .authenticate(&f.token, CredentialClass::PrincipalClient),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service.register(
            &f.token,
            &RegistrationRequest {
                handle: "alpha".into(),
                display_name: "Alpha".into()
            }
        ),
        Err(BootstrapError::Unauthorized)
    ));
    let replacement = f
        .service
        .recover_principal(&PrincipalRecoveryRequest {
            principal_id: f.principal.clone(),
            reason: Some("explicit recovery".into()),
        })
        .unwrap()
        .replacement_secret;
    f.service
        .authenticate(&replacement.secret, CredentialClass::PrincipalClient)
        .unwrap();
    let second = f
        .service
        .set_principal_state(&f.state(true, "second suspension"))
        .unwrap();
    assert_ne!(second.disabled_at, disabled.disabled_at);
    assert!(matches!(
        f.service
            .authenticate(&replacement.secret, CredentialClass::PrincipalClient),
        Err(BootstrapError::Unauthorized)
    ));
    let count: i64 = connection
        .query_row(
            "SELECT count(*) FROM credential_audit WHERE operation='revoked'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn archive_is_repeat_safe_and_preserves_exact_replay_and_history() {
    let f = Fixture::new();
    let body = input(vec![f.principal.clone()]);
    let committed = f
        .service
        .append_record(&f.token, "space", "committed", &body)
        .unwrap();
    let archived = f
        .service
        .set_space_archive(&f.archive(true, "finished"))
        .unwrap();
    assert!(archived.archived_at.is_some());
    let history = f.history();
    let revision = f.revision();
    f.tick();
    assert_eq!(
        f.service
            .set_space_archive(&f.archive(true, "retry"))
            .unwrap(),
        archived
    );
    assert_eq!(f.history(), history);
    assert_eq!(f.revision(), revision);
    let replay = f
        .service
        .append_record(&f.token, "space", "committed", &body)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.record, committed.record);
    assert_eq!(replay.mailbox_created, committed.mailbox_created);
    assert!(
        f.service
            .append_record(&f.token, "space", "new", &body)
            .is_err()
    );
    assert_eq!(
        f.service
            .get_record(&f.token, &committed.record.id)
            .unwrap(),
        committed.record
    );
    let inbox = f
        .service
        .inbox(&f.token, &InboxQuery::from_query("").unwrap())
        .unwrap();
    f.service
        .acknowledge_inbox_item(&f.token, &inbox.items[0].inbox_item_id)
        .unwrap();
    f.service
        .set_space_archive(&f.archive(false, "reopened"))
        .unwrap();
    f.service
        .append_record(&f.token, "space", "new", &body)
        .unwrap();
    let history = f.history();
    let revision = f.revision();
    f.service
        .set_space_archive(&f.archive(false, "retry"))
        .unwrap();
    assert_eq!(f.history(), history);
    assert_eq!(f.revision(), revision);
    let second = f
        .service
        .set_space_archive(&f.archive(true, "finished again"))
        .unwrap();
    assert_ne!(archived.archived_at, second.archived_at);
}

#[test]
fn lifecycle_exact_uuid_errors_and_atomic_rollback() {
    let f = Fixture::new();
    // A UUID-shaped reserved handle must never resolve as an admin subject ID.
    let missing = "018f1f59-6e90-7000-8000-000000000009";
    f.service
        .create_principal(&PrincipalCreateRequest {
            handle: missing.into(),
            display_name: "UUID handle".into(),
        })
        .unwrap();
    assert!(matches!(
        f.service.set_principal_state(&PrincipalStateRequest {
            principal_id: missing.into(),
            disabled: true,
            reason: None
        }),
        Err(BootstrapError::NotFound)
    ));
    assert!(matches!(
        f.service.set_principal_state(&PrincipalStateRequest {
            principal_id: "alpha".into(),
            disabled: true,
            reason: None
        }),
        Err(BootstrapError::Invalid(_))
    ));
    assert!(matches!(
        f.service.set_space_archive(&SpaceArchiveRequest {
            space_id: "missing".into(),
            archived: true,
            reason: None
        }),
        Err(BootstrapError::NotFound)
    ));
    for boundary in [
        "principal-state-updated",
        "principal-state-revoked",
        "principal-state-audited",
    ] {
        let history = f.history();
        let revision = f.revision();
        assert!(matches!(
            f.service
                .clone()
                .with_failpoint(boundary)
                .set_principal_state(&f.state(true, "fail")),
            Err(BootstrapError::Injected)
        ));
        f.service
            .authenticate(&f.token, CredentialClass::PrincipalClient)
            .unwrap();
        assert_eq!(f.history(), history);
        assert_eq!(f.revision(), revision);
    }
    for boundary in ["space-archive-updated", "space-archive-audited"] {
        assert!(matches!(
            f.service
                .clone()
                .with_failpoint(boundary)
                .set_space_archive(&f.archive(true, "fail")),
            Err(BootstrapError::Injected)
        ));
        assert!(
            f.service
                .get_space(&f.token, "space")
                .unwrap()
                .archived_at
                .is_none()
        );
    }
}

#[test]
fn older_backup_restore_reopen_retains_latest_lifecycle_and_security_history() {
    for resumed in [false, true] {
        let f = Fixture::new();
        let backup = f.directory.join("backup.db");
        let restored = f.directory.join("restored.db");
        let audit = f.database.recovery_audit().unwrap();
        audit.backup(&f.database, &backup).unwrap();
        let disabled = f
            .service
            .set_principal_state(&f.state(true, "after backup"))
            .unwrap();
        let archived = f
            .service
            .set_space_archive(&f.archive(true, "after backup"))
            .unwrap();
        if resumed {
            f.tick();
            f.service
                .set_principal_state(&f.state(false, "resume"))
                .unwrap();
            f.service
                .set_space_archive(&f.archive(false, "resume"))
                .unwrap();
        }
        let history = f.history();
        let mut approval = audit.restore(&backup, &restored, true).unwrap();
        let restored_database = Database::open(&restored).unwrap();
        approval.inventory_complete = true;
        approval.accepted_record_loss = true;
        audit.reopen(&restored_database, &approval).unwrap();
        drop(restored_database);

        // Release all protected handles before admitting the restored file.
        let Fixture {
            directory,
            database,
            service,
            clock,
            principal,
            token,
        } = f;
        drop(service);
        drop(database);
        let database = Database::open_protected(&restored, directory.join("audit.db")).unwrap();
        let service = BootstrapService::with_sources(
            database.clone(),
            clock,
            Arc::new(journal_service::OsSecretSource),
        );
        let connection = database.connect_read_only().unwrap();
        let timestamp: Option<String> = connection
            .query_row(
                "SELECT disabled_at FROM principals WHERE id=?",
                [&principal],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(timestamp, if resumed { None } else { disabled.disabled_at });
        let timestamp: Option<String> = connection
            .query_row("SELECT archived_at FROM spaces WHERE id='space'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(timestamp, if resumed { None } else { archived.archived_at });
        let retained: Vec<(String, String, String, String)> = connection
            .prepare("SELECT id,event_type,detail_json,created_at FROM audit_events ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(retained, history);
        assert!(matches!(
            service.authenticate(&token, CredentialClass::PrincipalClient),
            Err(BootstrapError::Unauthorized)
        ));
        assert!(matches!(
            service.register(
                &token,
                &RegistrationRequest {
                    handle: "unused".into(),
                    display_name: "Unused".into()
                }
            ),
            Err(BootstrapError::Unauthorized)
        ));
        drop(connection);
        drop(service);
        drop(database);
        drop(directory);
    }
}

#[test]
fn concurrent_appends_and_lifecycle_transitions_serialize_without_partial_records() {
    for transition in ["author-disable", "recipient-disable", "archive"] {
        let f = Fixture::new();
        let recipient = f
            .service
            .register(
                &"b".repeat(64),
                &RegistrationRequest {
                    handle: "recipient".into(),
                    display_name: "Recipient".into(),
                },
            )
            .unwrap()
            .receipt
            .principal
            .id;
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let body = input(vec![recipient.clone()]);
        let mut appenders = Vec::new();
        for i in 0..8 {
            let service = f.service.clone();
            let token = f.token.clone();
            let body = body.clone();
            let barrier = barrier.clone();
            appenders.push(std::thread::spawn(move || {
                barrier.wait();
                service.append_record(&token, "space", &format!("race-{i}"), &body)
            }));
        }
        barrier.wait();
        match transition {
            "archive" => {
                f.service
                    .set_space_archive(&f.archive(true, "race"))
                    .unwrap();
            }
            _ => {
                f.service
                    .set_principal_state(&PrincipalStateRequest {
                        principal_id: if transition == "author-disable" {
                            f.principal.clone()
                        } else {
                            recipient
                        },
                        disabled: true,
                        reason: Some("race".into()),
                    })
                    .unwrap();
            }
        }
        let results: Vec<_> = appenders
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        let successes = results.iter().filter(|result| result.is_ok()).count() as i64;
        for result in &results {
            if let Err(error) = result {
                assert!(
                    matches!(
                        error,
                        BootstrapError::Unauthorized
                            | BootstrapError::NotFound
                            | BootstrapError::InvalidJournal
                    ),
                    "{error:?}"
                );
            }
        }
        assert!(
            f.service
                .append_record(&f.token, "space", "after-transition", &body)
                .is_err()
        );
        let connection = f.database.connect_read_only().unwrap();
        for table in ["records", "attention", "mailbox_items", "idempotency_keys"] {
            let count: i64 = connection
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, successes, "{transition}: {table}");
        }
    }
}

#[test]
fn lifecycle_audit_prepare_and_completion_failures_preserve_the_security_boundary() {
    for operation in ["disable", "archive"] {
        for boundary in ["prepare", "completion"] {
            let f = Fixture::new();
            let audit_path = f.directory.join("audit.db");
            let connection = rusqlite::Connection::open(&audit_path).unwrap();
            let trigger = if boundary == "prepare" {
                "CREATE TRIGGER injected_audit_failure BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'injected prepare failure'); END"
            } else {
                "CREATE TRIGGER injected_audit_failure BEFORE UPDATE OF outcome ON events BEGIN SELECT RAISE(ABORT,'injected completion failure'); END"
            };
            connection.execute_batch(trigger).unwrap();
            let result = if operation == "disable" {
                f.service
                    .set_principal_state(&f.state(true, "audit failure"))
                    .map(|_| ())
            } else {
                f.service
                    .set_space_archive(&f.archive(true, "audit failure"))
                    .map(|_| ())
            };
            assert!(result.is_err());
            // Inspect durable central state without going through the closed gate.
            let central = rusqlite::Connection::open(f.directory.join("central.db")).unwrap();
            let (table, column, id) = if operation == "disable" {
                ("principals", "disabled_at", f.principal.as_str())
            } else {
                ("spaces", "archived_at", "space")
            };
            let state: bool = central
                .query_row(
                    &format!("SELECT {column} IS NOT NULL FROM {table} WHERE id=?"),
                    [id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(state, boundary == "completion");
            if operation == "disable" {
                let revoked: bool = central
                    .query_row(
                        "SELECT revoked_at IS NOT NULL FROM credentials WHERE principal_id=?",
                        [id],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(revoked, state);
            }
            let head: (i64, String) = connection
                .query_row(
                    "SELECT revision,outcome FROM events ORDER BY revision DESC LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            connection
                .execute_batch("DROP TRIGGER injected_audit_failure")
                .unwrap();
            if boundary == "prepare" {
                assert_eq!(head.1, "committed");
                f.service
                    .authenticate(&f.token, CredentialClass::PrincipalClient)
                    .unwrap();
            } else {
                assert_eq!(head.1, "prepared");
                assert!(
                    f.service
                        .authenticate(&f.token, CredentialClass::PrincipalClient)
                        .is_err()
                );
                assert!(
                    f.service
                        .set_principal_state(&f.state(false, "bypass"))
                        .is_err()
                );
                assert!(
                    f.service
                        .set_space_archive(&f.archive(false, "bypass"))
                        .is_err()
                );
                let audit = f.database.recovery_audit().unwrap();
                assert!(
                    audit
                        .reconcile(&f.database, true)
                        .unwrap_err()
                        .to_string()
                        .contains("archive/reset required")
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT revision,outcome FROM events ORDER BY revision DESC LIMIT 1",
                            [],
                            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                        )
                        .unwrap(),
                    head
                );
            }
        }
    }
}

struct PausedRandom {
    entered: std::sync::mpsc::Sender<()>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl journal_service::SecretSource for PausedRandom {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
        self.entered.send(()).unwrap();
        self.resume.lock().unwrap().recv().unwrap();
        bytes.fill(0x5a);
        Ok(())
    }
}

#[test]
fn held_ordinary_and_shared_viewer_reads_reauthorize_after_lifecycle_commit() {
    let f = Fixture::new();
    let record = f
        .service
        .append_record(&f.token, "space", "visible", &input(vec![]))
        .unwrap()
        .record;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let writer_service = BootstrapService::with_sources(
        f.database.clone(),
        f.clock.clone(),
        Arc::new(PausedRandom {
            entered: entered_tx,
            resume: std::sync::Mutex::new(resume_rx),
        }),
    );
    let request = f.state(true, "suspend during held reads");
    let writer = std::thread::spawn(move || writer_service.set_principal_state(&request));
    // Pause inside the lifecycle transaction after state and revocations, before
    // publishing audit intent. Reads must wait for the full protected commit.
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let readers: Vec<_> = [false, true]
        .into_iter()
        .map(|viewer| {
            let service = f.service.clone();
            let token = f.token.clone();
            let principal = f.principal.clone();
            let id = record.id.clone();
            let result = result_tx.clone();
            std::thread::spawn(move || {
                result
                    .send(if viewer {
                        service.shared_viewer(&principal).record(&id)
                    } else {
                        service.get_record(&token, &id)
                    })
                    .unwrap();
            })
        })
        .collect();
    assert!(matches!(
        result_rx.recv_timeout(Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    resume_tx.send(()).unwrap();
    writer.join().unwrap().unwrap();
    for _ in 0..2 {
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Err(BootstrapError::Unauthorized | BootstrapError::NotFound)
        ));
    }
    for reader in readers {
        reader.join().unwrap();
    }
}
