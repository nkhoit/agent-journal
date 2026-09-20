use std::fs;
#[cfg(unix)]
use std::hash::{DefaultHasher, Hash, Hasher};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use journal_storage_sqlite::{ConnectionFactory, Database, StorageError};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "journal-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }

    fn database(&self) -> Database {
        let database = Database::open(self.0.join("journal.db")).unwrap();
        database.connect().unwrap().execute_batch(
            "INSERT INTO principals(id,display_name,description,profile_revision,created_at,disabled_at)
             VALUES ('018f1f59-6e90-7000-8000-000000000001','Principal',NULL,1,'2026-01-01T00:00:00Z',NULL);
             INSERT INTO principal_names(name,principal_id,kind,created_at)
             VALUES ('principal','018f1f59-6e90-7000-8000-000000000001','current','2026-01-01T00:00:00Z');
             INSERT INTO spaces VALUES ('s','Space','2026-01-01T00:00:00Z',NULL);
             INSERT INTO memberships VALUES ('s','018f1f59-6e90-7000-8000-000000000001',1,1,0,'2026-01-01T00:00:00Z');
             INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
             VALUES ('r','s',1,'018f1f59-6e90-7000-8000-000000000001','note','recovery probe','2026-01-01T00:00:00Z');
             INSERT INTO attention VALUES ('r','018f1f59-6e90-7000-8000-000000000001','2026-01-01T00:00:00Z');
             INSERT INTO mailbox_items VALUES ('m','r','018f1f59-6e90-7000-8000-000000000001','pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');"
        ).unwrap();
        database
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[cfg(unix)]
#[derive(Debug, PartialEq, Eq)]
struct ArtifactState {
    present: bool,
    bytes: Option<Vec<u8>>,
    hash: Option<u64>,
    device: Option<u64>,
    inode: Option<u64>,
    links: Option<u64>,
    modified_seconds: Option<i64>,
    modified_nanoseconds: Option<i64>,
}

#[cfg(unix)]
fn artifact_state(path: &Path) -> ArtifactState {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let bytes = metadata
                .file_type()
                .is_file()
                .then(|| fs::read(path).unwrap());
            let hash = bytes.as_ref().map(|bytes| {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                hasher.finish()
            });
            ArtifactState {
                present: true,
                bytes,
                hash,
                device: Some(metadata.dev()),
                inode: Some(metadata.ino()),
                links: Some(metadata.nlink()),
                modified_seconds: Some(metadata.mtime()),
                modified_nanoseconds: Some(metadata.mtime_nsec()),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ArtifactState {
            present: false,
            bytes: None,
            hash: None,
            device: None,
            inode: None,
            links: None,
            modified_seconds: None,
            modified_nanoseconds: None,
        },
        Err(error) => panic!("inspect {path:?}: {error}"),
    }
}

#[cfg(unix)]
fn protected_artifacts(central: &Path, audit: &Path) -> Vec<(PathBuf, ArtifactState)> {
    [
        central.to_owned(),
        PathBuf::from(format!("{}-wal", central.display())),
        PathBuf::from(format!("{}-shm", central.display())),
        PathBuf::from(format!("{}-journal", central.display())),
        audit.to_owned(),
        PathBuf::from(format!("{}-wal", audit.display())),
        PathBuf::from(format!("{}-shm", audit.display())),
        PathBuf::from(format!("{}-journal", audit.display())),
        audit.with_extension("recovery-lock"),
    ]
    .into_iter()
    .map(|path| {
        let state = artifact_state(&path);
        (path, state)
    })
    .collect()
}

#[cfg(unix)]
#[test]
fn protected_recovery_admission_refusals_preserve_every_artifact_before_rw_open() {
    for case in ["missing", "malformed", "foreign", "closed"] {
        let fixture = Fixture::new();
        let central = fixture.0.join("journal.db");
        let audit = fixture.0.join("audit.db");
        let database =
            Database::open_protected(&central, &audit).expect("initialize protected database");
        if case == "closed" {
            database
                .recovery_audit()
                .expect("recovery audit")
                .close()
                .expect("close recovery audit");
        }
        drop(database);

        match case {
            "missing" => fs::remove_file(&audit).expect("remove required audit"),
            "malformed" => {
                fs::write(&audit, b"not a SQLite recovery audit").expect("corrupt audit");
                fs::set_permissions(&audit, fs::Permissions::from_mode(0o600))
                    .expect("protect malformed audit");
            }
            "foreign" => {
                let donor = Fixture::new();
                let donor_central = donor.0.join("journal.db");
                let donor_audit = donor.0.join("audit.db");
                drop(
                    Database::open_protected(&donor_central, &donor_audit)
                        .expect("initialize foreign audit"),
                );
                fs::copy(&donor_audit, &audit).expect("replace with foreign audit");
                fs::set_permissions(&audit, fs::Permissions::from_mode(0o600))
                    .expect("protect foreign audit");
            }
            "closed" => {}
            _ => unreachable!(),
        }

        let before = protected_artifacts(&central, &audit);
        assert!(
            Database::open_protected(&central, &audit).is_err(),
            "{case} audit must refuse protected startup"
        );
        assert_eq!(
            protected_artifacts(&central, &audit),
            before,
            "{case} recovery refusal changed protected evidence"
        );
    }
}

#[test]
fn restore_probes_cover_hashes_acls_heads_and_mailbox_history() {
    let fixture = Fixture::new();
    let database = fixture.database();
    let expected = database.recovery_verification().unwrap();
    database.backup_to(fixture.0.join("backup.db")).unwrap();
    Database::restore_backup(fixture.0.join("backup.db"), fixture.0.join("restored.db")).unwrap();
    let restored = Database::open(fixture.0.join("restored.db")).unwrap();
    assert_eq!(restored.recovery_verification().unwrap(), expected);
    restored
        .connect()
        .unwrap()
        .execute(
            "UPDATE memberships SET can_read=0 WHERE space_id='s' AND principal_id='018f1f59-6e90-7000-8000-000000000001'",
            [],
        )
        .unwrap();
    assert_ne!(restored.recovery_verification().unwrap(), expected);
}

#[test]
fn syntactically_valid_but_incomplete_fts_is_not_a_verified_restore() {
    let fixture = Fixture::new();
    let database = fixture.database();
    database
        .connect()
        .unwrap()
        .execute("DELETE FROM records_fts", [])
        .unwrap();
    assert!(database.recovery_verification().is_err());
}

#[test]
fn corrupted_fts_postings_fail_even_when_stored_content_remains() {
    let fixture = Fixture::new();
    let database = fixture.database();
    let connection = database.connect().unwrap();
    connection
        .execute("DELETE FROM records_fts_data WHERE id>10", [])
        .unwrap();
    let content: String = connection
        .query_row(
            "SELECT content FROM records_fts WHERE record_id='r'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(content, "recovery probe");
    assert!(database.recovery_verification().is_err());
}

#[test]
fn missing_attempt_history_is_not_a_verified_restore() {
    let fixture = Fixture::new();
    let database = fixture.database();
    database
        .connect()
        .unwrap()
        .execute("DELETE FROM delivery_attempts", [])
        .unwrap();
    assert!(database.recovery_verification().is_err());
}

#[cfg(not(unix))]
#[test]
fn protected_recovery_has_no_non_unix_permission_fallback() {
    let fixture = Fixture::new();
    assert!(
        Database::open_protected(fixture.0.join("central.db"), fixture.0.join("audit.db")).is_err()
    );
    assert!(!fixture.0.join("central.db").exists());
    assert!(!fixture.0.join("audit.db").exists());
}

#[test]
fn missing_attention_obligation_is_not_a_verified_restore() {
    let fixture = Fixture::new();
    let database = fixture.database();
    database
        .connect()
        .unwrap()
        .execute("DELETE FROM attention", [])
        .unwrap();
    assert!(database.recovery_verification().is_err());
}

#[test]
fn backup_under_writes_has_consistent_heads_and_hashes() {
    let fixture = Fixture::new();
    let database = fixture.database();
    let writer = database.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let mut connection = writer.connect().unwrap();
        for sequence in 2..=100 {
            let transaction = connection.transaction().unwrap();
            transaction.execute(
                "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
                 VALUES (?,'s',?,'018f1f59-6e90-7000-8000-000000000001','note','concurrent recovery token','2026-01-01T00:00:00Z')",
                rusqlite::params![format!("r{sequence}"), sequence],
            ).unwrap();
            if sequence == 100 {
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
            }
            transaction.commit().unwrap();
        }
    });
    started_rx.recv().unwrap();
    database.backup_to(fixture.0.join("backup.db")).unwrap();
    finish_tx.send(()).unwrap();
    thread.join().unwrap();
    let backup = Database::open(fixture.0.join("backup.db")).unwrap();
    let expected = backup.recovery_verification().unwrap();
    assert_eq!(expected.space_heads, vec![("s".to_owned(), 99)]);
    Database::restore_backup(fixture.0.join("backup.db"), fixture.0.join("restored.db")).unwrap();
    assert_eq!(
        Database::open(fixture.0.join("restored.db"))
            .unwrap()
            .recovery_verification()
            .unwrap(),
        expected
    );
}

#[test]
fn closed_recovery_rejects_every_exported_writer_route_without_mutation() {
    fn assert_closed(
        label: &str,
        attempt: impl FnOnce(&Database, &std::path::Path) -> Result<(), StorageError>,
    ) {
        let fixture = Fixture::new();
        let central = fixture.0.join("central.db");
        let audit = fixture.0.join("audit.db");
        let database = Database::open_protected(&central, &audit).unwrap();
        database
            .with_transaction(|transaction| {
                transaction.execute(
                    "INSERT INTO principals(id,display_name,created_at) VALUES ('018f1f59-6e90-7000-8000-000000000002','Before','2026-01-01T00:00:00Z')",
                    [],
                )?;
                Ok::<(), StorageError>(())
            })
            .unwrap();
        let before = database
            .connect_read_only()
            .unwrap()
            .query_row("SELECT count(*) FROM principals", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        database.recovery_audit().unwrap().close().unwrap();

        assert!(
            attempt(&database, &central).is_err(),
            "{label} must fail closed"
        );

        let after = ConnectionFactory::new(&central)
            .connect_read_only()
            .unwrap()
            .query_row("SELECT count(*) FROM principals", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(after, before, "{label} changed protected data");
    }

    assert_closed("Database::connect", |database, _| {
        let connection = database.connect()?;
        connection.execute(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('connect','Connect','2026-01-01T00:00:00Z')",
            [],
        )?;
        Ok(())
    });
    assert_closed("Database::connection_factory", |database, _| {
        let connection = database.connection_factory().connect()?;
        connection.execute(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('factory','Factory','2026-01-01T00:00:00Z')",
            [],
        )?;
        Ok(())
    });
    assert_closed("ConnectionFactory::new", |_, central| {
        let connection = ConnectionFactory::new(central).connect()?;
        connection.execute(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('new-factory','New Factory','2026-01-01T00:00:00Z')",
            [],
        )?;
        Ok(())
    });
    assert_closed("Database::with_transaction", |database, _| {
        database.with_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id,display_name,created_at) VALUES ('transaction','Transaction','2026-01-01T00:00:00Z')",
                [],
            )?;
            Ok(())
        })
    });
    assert_closed("Database::with_transaction_for", |database, _| {
        database.with_transaction_for(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id,display_name,created_at) VALUES ('transaction-for','Transaction For','2026-01-01T00:00:00Z')",
                [],
            )?;
            Ok(())
        })
    });
    assert_closed("Database::open handle", |_, central| {
        let reopened = Database::open(central)?;
        reopened.with_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id,display_name,created_at) VALUES ('reopened','Reopened','2026-01-01T00:00:00Z')",
                [],
            )?;
            Ok(())
        })
    });
}

#[test]
fn protected_recovery_rejects_unguarded_writer_routes_while_open() {
    fn assert_rejected(
        label: &str,
        attempt: impl FnOnce(&Database, &std::path::Path) -> Result<(), StorageError>,
    ) {
        let fixture = Fixture::new();
        let central = fixture.0.join("central.db");
        let audit = fixture.0.join("audit.db");
        let database = Database::open_protected(&central, &audit).unwrap();
        database
            .with_transaction(|transaction| {
                transaction.execute(
                    "INSERT INTO principals(id,display_name,created_at) VALUES ('018f1f59-6e90-7000-8000-000000000002','Before','2026-01-01T00:00:00Z')",
                    [],
                )?;
                Ok::<(), StorageError>(())
            })
            .unwrap();

        assert!(
            attempt(&database, &central).is_err(),
            "{label} must require the audited transaction wrapper"
        );
        let count: i64 = database
            .connect_read_only()
            .unwrap()
            .query_row("SELECT count(*) FROM principals", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1, "{label} changed protected data");
    }

    assert_rejected("Database::connect", |database, _| {
        let connection = database.connect()?;
        connection.execute(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('connect','Connect','2026-01-01T00:00:00Z')",
            [],
        )?;
        Ok(())
    });
    assert_rejected("Database::connection_factory", |database, _| {
        let connection = database.connection_factory().connect()?;
        connection.execute(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('factory','Factory','2026-01-01T00:00:00Z')",
            [],
        )?;
        Ok(())
    });
    assert_rejected("ConnectionFactory::new", |_, central| {
        let connection = ConnectionFactory::new(central).connect()?;
        connection.execute(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('new-factory','New Factory','2026-01-01T00:00:00Z')",
            [],
        )?;
        Ok(())
    });
    assert_rejected("Database::open handle", |_, central| {
        let reopened = Database::open(central)?;
        reopened.with_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id,display_name,created_at) VALUES ('reopened','Reopened','2026-01-01T00:00:00Z')",
                [],
            )?;
            Ok(())
        })
    });
}
