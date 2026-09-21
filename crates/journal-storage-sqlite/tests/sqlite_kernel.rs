use std::fs;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use journal_storage_sqlite::{
    BUSY_TIMEOUT, CURRENT_SCHEMA_VERSION, ConnectionFactory, Database, StorageError,
};
use rusqlite::{Connection, ErrorCode, params};

const NOW: &str = "2026-01-01T00:00:00Z";
const PRINCIPAL_ID: &str = "018f1f59-6e90-7000-8000-000000000001";
static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-journal-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create temporary directory");
        Self { path }
    }

    fn database(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("remove temporary directory");
    }
}

fn pragma_i64(connection: &Connection, name: &str) -> i64 {
    connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
        .expect("read integer pragma")
}

fn pragma_string(connection: &Connection, name: &str) -> String {
    connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
        .expect("read string pragma")
}

fn seed_space(connection: &Connection) {
    connection
        .execute(
            "INSERT OR IGNORE INTO principals(id, display_name, created_at) VALUES (?1, 'P1', ?2)",
            [PRINCIPAL_ID, NOW],
        )
        .expect("insert principal");
    connection
        .execute(
            "INSERT OR IGNORE INTO principal_names(name, principal_id, kind, created_at) VALUES ('p1', ?1, 'current', ?2)",
            [PRINCIPAL_ID, NOW],
        )
        .expect("insert principal name");
    connection
        .execute(
            "INSERT OR IGNORE INTO spaces(id, name, access, created_at) VALUES ('s1', 'Space 1', 'public', ?1)",
            [NOW],
        )
        .expect("insert space");
}

fn insert_record(connection: &Connection, id: &str, sequence: i64, content: &str) {
    connection
        .execute(
            "INSERT INTO records(
                id, space_id, space_seq, author_principal_id, kind, content, created_at
             ) VALUES (?1, 's1', ?2, ?3, 'message', ?4, ?5)",
            params![id, sequence, PRINCIPAL_ID, content, NOW],
        )
        .expect("insert record");
}

fn schema_object_names(connection: &Connection, object_type: &str) -> Vec<String> {
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = ?1 ORDER BY name")
        .expect("prepare schema query");
    statement
        .query_map([object_type], |row| row.get(0))
        .expect("query schema objects")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect schema objects")
}

#[test]
fn operational_snapshot_tracks_pending_rows_and_wal_without_checkpointing() {
    let temporary = TempDir::new("metrics");
    let database = Database::open(temporary.database("journal.db")).unwrap();
    let writer = database.connect().unwrap();
    writer.execute_batch("PRAGMA wal_autocheckpoint=0").unwrap();
    seed_space(&writer);
    let reader = database.connect_read_only().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM records;")
        .unwrap();
    let before = database.operational_snapshot(NOW).unwrap();
    assert_eq!(before.pending_mailbox_count, 0);
    assert_eq!(before.oldest_pending_at, None);
    for sequence in 1..=20 {
        insert_record(
            &writer,
            &format!("r{sequence}"),
            sequence,
            &"x".repeat(4096),
        );
    }
    writer
        .execute(
            "INSERT INTO inbox_sequences VALUES ('018f1f59-6e90-7000-8000-000000000001',1)",
            [],
        )
        .unwrap();
    writer.execute("INSERT INTO mailbox_items(id,record_id,recipient_principal_id,state,created_at,updated_at,recipient_seq) VALUES ('m1','r1','018f1f59-6e90-7000-8000-000000000001','pending',?1,?1,1)", [NOW]).unwrap();
    let after = database.operational_snapshot(NOW).unwrap();
    assert_eq!(after.pending_mailbox_count, 1);
    assert_eq!(after.oldest_pending_at.as_deref(), Some(NOW));
    assert!(after.database_bytes > 0);
    assert!(after.wal_bytes > before.wal_bytes);
    assert_eq!(after.runtime_failure_events, 0);
    assert_eq!(after.oldest_active_heartbeat_at, None);
    reader.execute_batch("ROLLBACK").unwrap();
    writer
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    assert_eq!(database.operational_snapshot(NOW).unwrap().wal_bytes, 0);
}

#[test]
fn page_capacity_exhaustion_rolls_back_without_losing_committed_records() {
    let temporary = TempDir::new("capacity");
    let database = Database::open(temporary.database("journal.db")).unwrap();
    let connection = database.connect().unwrap();
    seed_space(&connection);
    insert_record(&connection, "retained", 1, "retained");
    let pages = pragma_i64(&connection, "page_count");
    connection
        .execute_batch(&format!("PRAGMA max_page_count={pages}; BEGIN IMMEDIATE;"))
        .unwrap();
    let result = connection.execute("INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at) VALUES ('full','s1',2,'018f1f59-6e90-7000-8000-000000000001','message',?1,?2)", params!["x".repeat(65536), NOW]);
    assert!(
        matches!(result, Err(rusqlite::Error::SqliteFailure(error, _)) if error.code == ErrorCode::DiskFull)
    );
    if !connection.is_autocommit() {
        connection.execute_batch("ROLLBACK").unwrap();
    }
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM records", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(pragma_string(&connection, "integrity_check"), "ok");
}

#[test]
fn empty_path_initializes_directly_to_the_uuid_native_baseline() {
    let temporary = TempDir::new("open");
    let database = Database::open(temporary.database("journal.db")).expect("open database");
    let connection = database.connect().expect("connect");

    assert_eq!(
        database.schema_version().expect("schema version"),
        CURRENT_SCHEMA_VERSION
    );
    assert_eq!(pragma_i64(&connection, "foreign_keys"), 1);
    assert_eq!(pragma_string(&connection, "journal_mode"), "wal");
    assert_eq!(pragma_i64(&connection, "synchronous"), 2);
    assert_eq!(
        pragma_i64(&connection, "busy_timeout"),
        BUSY_TIMEOUT.as_millis() as i64
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT sqlite_compileoption_used('ENABLE_FTS5')",
                [],
                |row| { row.get::<_, i64>(0) }
            )
            .expect("probe FTS5"),
        1
    );

    let tables = schema_object_names(&connection, "table");
    for expected in ["principals", "records", "records_fts", "schema_contract"] {
        assert!(
            tables.iter().any(|table| table == expected),
            "missing {expected}"
        );
    }

    let indexes = schema_object_names(&connection, "index");
    assert!(
        indexes
            .iter()
            .any(|index| index == "mailbox_pending_by_recipient")
    );
    let triggers = schema_object_names(&connection, "trigger");
    for expected in [
        "records_are_immutable",
        "records_cannot_be_deleted",
        "records_fts_insert",
    ] {
        assert!(
            triggers.iter().any(|trigger| trigger == expected),
            "missing {expected}"
        );
    }

    drop(connection);
    let reopened = Database::open(temporary.database("journal.db")).expect("reopen database");
    assert_eq!(
        reopened.schema_version().expect("schema version"),
        CURRENT_SCHEMA_VERSION
    );
}

#[test]
fn membership_only_schema_is_rejected_without_exposing_or_rewriting_spaces() {
    let temporary = TempDir::new("pre-public");
    let path = temporary.database("journal.db");
    let connection = Connection::open(&path).unwrap();
    let old = include_str!("../../../migrations/0001_uuid_native.sql")
        .replace("\r\n", "\n")
        .replace("version = 11", "version = 10")
        .replace("(1, 11, 'uuid-native-v1')", "(1, 10, 'uuid-native-v1')")
        .replace("    access TEXT NOT NULL CHECK (access = 'public'),\n", "");
    connection.execute_batch(&old).unwrap();
    connection
        .execute(
            "INSERT INTO spaces(id,name,created_at) VALUES('old','Old',?)",
            [NOW],
        )
        .unwrap();
    drop(connection);
    let before = std::fs::read(&path).unwrap();
    assert!(Database::open(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn transaction_helper_commits_or_rolls_back_atomically() {
    let temporary = TempDir::new("transactions");
    let database = Database::open(temporary.database("journal.db")).expect("open database");

    let error = database
        .with_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id, display_name, created_at) VALUES ('018f1f59-6e90-7000-8000-000000000002', 'P', ?1)",
                [NOW],
            )?;
            Err::<(), _>(rusqlite::Error::InvalidQuery.into())
        })
        .expect_err("operation must roll back");
    assert!(matches!(error, StorageError::Sqlite(_)));

    database
        .with_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id, display_name, created_at) VALUES ('018f1f59-6e90-7000-8000-000000000003', 'P', ?1)",
                [NOW],
            )?;
            Ok(())
        })
        .expect("commit transaction");

    let connection = database.connect().expect("connect");
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM principals WHERE id IN ('018f1f59-6e90-7000-8000-000000000002', '018f1f59-6e90-7000-8000-000000000003')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count principals"),
        1
    );
}

#[test]
fn pre_uuid_database_is_rejected_without_mutation() {
    let temporary = TempDir::new("migration-rollback");
    let path = temporary.database("journal.db");
    let connection = Connection::open(&path).expect("open conflicting database");
    connection
        .execute("CREATE TABLE principals(id TEXT PRIMARY KEY)", [])
        .expect("create conflicting table");
    drop(connection);

    let before = fs::read(&path).expect("read legacy database");
    assert!(matches!(
        Database::open(&path),
        Err(StorageError::ResetRequired {
            kind: "central database"
        })
    ));
    assert_eq!(fs::read(&path).expect("read refused database"), before);

    let connection = Connection::open(&path).expect("reopen failed migration");
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'schema_contract'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("probe contract table"),
        0,
        "refusal must not create the current contract"
    );
    drop(connection);
    fs::remove_file(&path).expect("archive incompatible database before reset");

    let database = Database::open(&path).expect("operator reset creates current database");
    assert_eq!(
        database.schema_version().expect("schema version"),
        CURRENT_SCHEMA_VERSION
    );
}

#[test]
fn protected_legacy_refusal_does_not_touch_recovery_or_sidecar_evidence() {
    let temporary = TempDir::new("protected-legacy-refusal");
    let path = temporary.database("journal.db");
    Connection::open(&path)
        .expect("create legacy database")
        .execute_batch("CREATE TABLE legacy(value TEXT)")
        .expect("create legacy schema");
    let audit = temporary.database("journal.recovery.db");
    let lock = audit.with_extension("recovery-lock");
    let artifacts = [
        path.clone(),
        PathBuf::from(format!("{}-journal", path.display())),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
        audit.clone(),
        lock,
    ];
    for artifact in artifacts.iter().skip(1) {
        fs::write(artifact, b"preserve").expect("write evidence");
    }
    let before: Vec<_> = artifacts
        .iter()
        .map(|artifact| (artifact.clone(), fs::read(artifact).expect("read evidence")))
        .collect();

    assert!(matches!(
        Database::open_protected(&path, &audit),
        Err(StorageError::ResetRequired { .. })
    ));
    for (artifact, bytes) in before {
        assert_eq!(fs::read(artifact).expect("read preserved evidence"), bytes);
    }
}

#[test]
fn concurrent_database_open_serializes_initial_migration() {
    const OPENERS: usize = 8;
    const ROUNDS: usize = 8;

    let temporary = TempDir::new("concurrent-migration");
    for round in 0..ROUNDS {
        let path = Arc::new(temporary.database(&format!("journal-{round}.db")));
        let barrier = Arc::new(Barrier::new(OPENERS));
        let mut threads = Vec::new();
        for _ in 0..OPENERS {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                Database::open(path.as_path())
            }));
        }
        for thread in threads {
            thread
                .join()
                .expect("database opener thread")
                .expect("concurrent database open");
        }
    }
}
#[test]
fn malformed_current_schema_is_rejected_without_mutation() {
    let temporary = TempDir::new("schema-mismatch");
    let path = temporary.database("journal.db");
    let database = Database::open(&path).expect("open database");
    let connection = database.connect().expect("connect");
    connection
        .execute_batch("DROP TABLE schema_contract")
        .expect("corrupt schema contract");
    drop(connection);
    drop(database);
    let before = fs::read(&path).expect("read malformed database");

    assert!(matches!(
        Database::open(&path),
        Err(StorageError::ResetRequired {
            kind: "central database"
        })
    ));
    assert_eq!(fs::read(&path).expect("read refused database"), before);
}

#[test]
fn current_schema_rejects_missing_or_recreated_load_bearing_objects() {
    let mutations = [
        (
            "principal_ids_are_immutable",
            "DROP TRIGGER principal_ids_are_immutable",
        ),
        (
            "principal_names_cannot_be_deleted",
            "DROP TRIGGER principal_names_cannot_be_deleted",
        ),
        (
            "principal_alias_is_immutable",
            "DROP TRIGGER principal_alias_is_immutable",
        ),
        (
            "principal_current_name_transition",
            "DROP TRIGGER principal_current_name_transition",
        ),
        (
            "principal_names_do_not_shadow_ids",
            "DROP TRIGGER principal_names_do_not_shadow_ids",
        ),
        (
            "principal_ids_do_not_shadow_names",
            "DROP TRIGGER principal_ids_do_not_shadow_names",
        ),
        (
            "principal_names_one_current_per_principal",
            "DROP INDEX principal_names_one_current_per_principal",
        ),
        (
            "profile_idempotency_keys",
            "DROP TABLE profile_idempotency_keys;
             CREATE TABLE profile_idempotency_keys(principal_id TEXT, idempotency_key TEXT)",
        ),
    ];
    for (label, mutation) in mutations {
        let temporary = TempDir::new(label);
        let path = temporary.database("journal.db");
        let database = Database::open(&path).expect("open database");
        database
            .connect()
            .expect("connect")
            .execute_batch(mutation)
            .expect("corrupt exact schema");
        drop(database);
        let before = fs::read(&path).expect("read corrupted current database");

        assert!(matches!(
            Database::open(&path),
            Err(StorageError::ResetRequired {
                kind: "central database"
            })
        ));
        assert_eq!(
            fs::read(&path).expect("read refused database"),
            before,
            "{label}"
        );
    }
}

#[test]
fn current_central_sidecars_are_reset_required_without_mutation() {
    for suffix in ["-wal", "-shm", "-journal"] {
        let temporary = TempDir::new(&format!("central-sidecar-{suffix}"));
        let path = temporary.database("journal.db");
        let database = Database::open(&path).expect("open current database");
        drop(database);
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        let _ = fs::remove_file(&sidecar);
        fs::write(&sidecar, format!("retained current central {suffix}")).unwrap();
        let audit = temporary.database("recovery.db");
        let lock = audit.with_extension("recovery-lock");
        let before = [path.clone(), sidecar.clone()]
            .into_iter()
            .map(|artifact| {
                let metadata = fs::metadata(&artifact).unwrap();
                let bytes = fs::read(&artifact).unwrap();
                (artifact, bytes, metadata)
            })
            .collect::<Vec<_>>();

        for result in [
            Database::open(&path).map(|_| ()),
            Database::open_protected(&path, &audit).map(|_| ()),
        ] {
            assert!(matches!(
                result,
                Err(StorageError::ResetRequired {
                    kind: "central database"
                })
            ));
        }
        assert!(!audit.exists());
        assert!(!lock.exists());
        for (artifact, bytes, metadata) in before {
            assert_eq!(
                fs::read(&artifact).unwrap(),
                bytes,
                "{suffix}: {artifact:?}"
            );
            #[cfg(unix)]
            assert_eq!(fs::metadata(&artifact).unwrap().ino(), metadata.ino());
        }
    }
}

#[cfg(unix)]
#[test]
fn dangling_central_sidecar_symlinks_are_reset_required_without_mutation() {
    use std::os::unix::fs::symlink;

    for suffix in ["-wal", "-shm", "-journal"] {
        let temporary = TempDir::new(&format!("central-dangling-sidecar-{suffix}"));
        let path = temporary.database("journal.db");
        let database = Database::open(&path).expect("open current database");
        drop(database);
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        let _ = fs::remove_file(&sidecar);
        let dangling_target = temporary.database(&format!("missing-{suffix}"));
        symlink(&dangling_target, &sidecar).expect("create dangling sidecar symlink");
        let main_before = fs::read(&path).expect("read central database");
        let main_inode = fs::metadata(&path).expect("stat central database").ino();
        let sidecar_inode = fs::symlink_metadata(&sidecar)
            .expect("lstat dangling sidecar")
            .ino();

        assert!(matches!(
            Database::open(&path),
            Err(StorageError::ResetRequired {
                kind: "central database"
            })
        ));
        assert_eq!(fs::read(&path).expect("read refused central"), main_before);
        assert_eq!(
            fs::metadata(&path).expect("stat refused central").ino(),
            main_inode
        );
        assert_eq!(
            fs::read_link(&sidecar).expect("read sidecar link"),
            dangling_target
        );
        assert_eq!(
            fs::symlink_metadata(&sidecar)
                .expect("lstat refused sidecar")
                .ino(),
            sidecar_inode
        );
    }
}

#[cfg(unix)]
#[test]
fn protected_central_hardlink_alias_is_reset_required_without_mutation() {
    use std::os::unix::fs::PermissionsExt;

    let temporary = TempDir::new("central-hardlink-alias");
    fs::set_permissions(&temporary.path, fs::Permissions::from_mode(0o700))
        .expect("protect central hardlink fixture directory");
    let central = temporary.database("journal.db");
    let audit = temporary.database("audit.db");
    drop(Database::open_protected(&central, &audit).expect("initialize protected central"));

    let alias = temporary.database("alias.db");
    let alias_audit = temporary.database("alias-audit.db");
    fs::hard_link(&central, &alias).expect("create central hardlink alias");
    fs::copy(&audit, &alias_audit).expect("copy audit for alias fixture");
    fs::set_permissions(&alias_audit, fs::Permissions::from_mode(0o600))
        .expect("protect copied audit");

    let artifacts = [
        central.clone(),
        alias.clone(),
        audit.clone(),
        alias_audit.clone(),
    ];
    let before = artifacts
        .iter()
        .map(|path| {
            let metadata = fs::metadata(path).expect("stat fixture artifact");
            (
                path.clone(),
                fs::read(path).expect("read fixture artifact"),
                metadata,
            )
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        Database::open_protected(&alias, &alias_audit),
        Err(StorageError::ResetRequired {
            kind: "central database"
        })
    ));
    for (path, bytes, metadata) in before {
        assert_eq!(fs::read(&path).expect("read refused artifact"), bytes);
        let after = fs::metadata(&path).expect("stat refused artifact");
        assert_eq!(after.dev(), metadata.dev(), "device changed for {path:?}");
        assert_eq!(after.ino(), metadata.ino(), "inode changed for {path:?}");
        assert_eq!(
            after.mtime(),
            metadata.mtime(),
            "mtime changed for {path:?}"
        );
        assert_eq!(
            after.mtime_nsec(),
            metadata.mtime_nsec(),
            "mtime nanos changed for {path:?}"
        );
    }
    assert_eq!(fs::metadata(&central).unwrap().nlink(), 2);
    assert_eq!(fs::metadata(&alias).unwrap().nlink(), 2);
    assert!(
        !alias_audit.with_extension("recovery-lock").exists(),
        "hardlink refusal must not create an alias audit lock"
    );
}

#[cfg(unix)]
#[test]
fn admitted_central_rejects_post_admission_hardlink_and_path_replacement_before_writes() {
    use std::os::unix::fs::PermissionsExt;

    #[derive(Debug, PartialEq, Eq)]
    struct ArtifactState {
        bytes: Vec<u8>,
        device: u64,
        inode: u64,
        links: u64,
        modified_seconds: i64,
        modified_nanoseconds: i64,
    }

    fn artifact_state(path: &Path) -> ArtifactState {
        let metadata = fs::metadata(path).expect("stat custody artifact");
        ArtifactState {
            bytes: fs::read(path).expect("read custody artifact"),
            device: metadata.dev(),
            inode: metadata.ino(),
            links: metadata.nlink(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
        }
    }

    fn assert_refused(database: &Database, raw_factory_must_refuse: bool) {
        // `connect` reaches the lowest writable-factory seam directly. The
        // transaction wrapper must independently refuse too, even though a
        // protected audit may reject during its preceding read validation.
        assert!(matches!(
            database.connect(),
            Err(StorageError::ResetRequired {
                kind: "central database"
            })
        ));
        if raw_factory_must_refuse {
            assert!(matches!(
                ConnectionFactory::new(database.path()).connect(),
                Err(StorageError::ResetRequired {
                    kind: "central database"
                })
            ));
        }
        assert!(matches!(
            database.with_transaction(|transaction| {
                transaction.execute(
                    "INSERT INTO principals(id,display_name,created_at) VALUES ('018f1f59-6e90-7000-8000-000000000004','Custody','2026-01-01T00:00:00Z')",
                    [],
                )?;
                Ok(())
            }),
            Err(StorageError::ResetRequired {
                kind: "central database"
            })
        ));
    }

    let temporary = TempDir::new("admitted-central-custody");
    fs::set_permissions(&temporary.path, fs::Permissions::from_mode(0o700))
        .expect("protect custody fixture directory");

    let central = temporary.database("hardlink.db");
    let audit = temporary.database("hardlink-audit.db");
    let database = Database::open_protected(&central, &audit).expect("open protected central");
    let alias = temporary.database("post-admission-alias.db");
    fs::hard_link(&central, &alias).expect("add post-admission hardlink");
    let before = [&central, &audit, &alias].map(|path| artifact_state(path));
    assert_refused(&database, true);
    for (path, expected) in [&central, &audit, &alias].into_iter().zip(before) {
        assert_eq!(
            artifact_state(path),
            expected,
            "post-hardlink write changed {path:?}"
        );
    }
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(
            !PathBuf::from(format!("{}{suffix}", central.display())).exists(),
            "post-hardlink refusal created {suffix}"
        );
    }

    let central = temporary.database("replacement.db");
    let audit = temporary.database("replacement-audit.db");
    let database = Database::open_protected(&central, &audit).expect("open replacement fixture");
    let staged = temporary.database("replacement-staged.db");
    fs::copy(&central, &staged).expect("copy replacement central");
    fs::rename(&staged, &central).expect("replace admitted central path");
    let before = [&central, &audit].map(|path| artifact_state(path));
    assert_refused(&database, false);
    for (path, expected) in [&central, &audit].into_iter().zip(before) {
        assert_eq!(
            artifact_state(path),
            expected,
            "post-replacement write changed {path:?}"
        );
    }
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(
            !PathBuf::from(format!("{}{suffix}", central.display())).exists(),
            "post-replacement refusal created {suffix}"
        );
    }
}

#[test]
fn supported_backup_destination_has_no_sidecars_before_current_admission() {
    let temporary = TempDir::new("backup-sidecars");
    let source = temporary.database("source.db");
    let destination = temporary.database("backup.db");
    let database = Database::open(&source).expect("open source");
    database.backup_to(&destination).expect("backup source");
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(
            !PathBuf::from(format!("{}{suffix}", destination.display())).exists(),
            "backup left {suffix}"
        );
    }
    Database::open(&destination).expect("open standalone backup");
}

#[test]
fn read_only_connections_query_but_cannot_write() {
    let temporary = TempDir::new("read-only");
    let database = Database::open(temporary.database("journal.db")).expect("open database");
    let connection = database.connect_read_only().expect("read-only connection");

    assert_eq!(pragma_i64(&connection, "query_only"), 1);
    assert_eq!(pragma_i64(&connection, "foreign_keys"), 1);
    assert_eq!(pragma_string(&connection, "journal_mode"), "wal");
    assert!(
        connection
            .execute(
                "INSERT INTO principals(id, display_name, created_at) VALUES ('p', 'P', 'now')",
                []
            )
            .is_err()
    );
}

#[test]
fn busy_timeout_returns_a_bounded_busy_error() {
    let temporary = TempDir::new("busy-timeout");
    let database = Database::open(temporary.database("journal.db")).expect("open database");
    let first = database.connect().expect("first connection");
    let second = database.connect().expect("second connection");

    first
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold writer lock");
    let error = second
        .execute(
            "INSERT INTO principals(id, display_name, created_at) VALUES ('blocked', 'P', 'now')",
            [],
        )
        .expect_err("second writer must time out");
    first
        .execute_batch("ROLLBACK")
        .expect("release writer lock");

    assert!(matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ));
}

#[test]
fn fts_search_and_immutable_record_guards_are_live() {
    let temporary = TempDir::new("fts");
    let database = Database::open(temporary.database("journal.db")).expect("open database");
    let connection = database.connect().expect("connect");
    seed_space(&connection);
    insert_record(&connection, "r1", 1, "durable coordination journal");

    assert_eq!(
        connection
            .query_row(
                "SELECT record_id FROM records_fts WHERE records_fts MATCH 'coordination'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("search FTS"),
        "r1"
    );
    assert!(
        connection
            .execute("UPDATE records SET content = 'changed' WHERE id = 'r1'", [])
            .is_err()
    );
    assert!(
        connection
            .execute("DELETE FROM records WHERE id = 'r1'", [])
            .is_err()
    );
}

#[test]
fn backup_is_consistent_and_verified_in_isolation() {
    let temporary = TempDir::new("backup");
    let database = Database::open(temporary.database("journal.db")).expect("open database");
    let connection = database.connect().expect("connect");
    seed_space(&connection);
    insert_record(&connection, "r1", 1, "backup searchable content");
    drop(connection);

    let backup_path = temporary.database("backup.db");
    let verification = database.backup_to(&backup_path).expect("create backup");
    assert_eq!(verification.schema_version, CURRENT_SCHEMA_VERSION);
    assert_eq!(verification.record_count, 1);
    assert_eq!(verification.mailbox_item_count, 0);

    let backup = ConnectionFactory::new(&backup_path)
        .connect_read_only()
        .expect("open backup read-only");
    assert_eq!(
        backup
            .query_row(
                "SELECT count(*) FROM records_fts WHERE records_fts MATCH 'searchable'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("search backup"),
        1
    );

    let restored_path = temporary.database("restored.db");
    let restored = Database::restore_backup(&backup_path, &restored_path).expect("restore backup");
    assert_eq!(restored.record_count, 1);
    let restored_database = Database::open(&restored_path).expect("open restored database");
    assert_eq!(
        restored_database.schema_version().expect("restored schema"),
        CURRENT_SCHEMA_VERSION
    );
    assert!(matches!(
        database.backup_to(&backup_path),
        Err(StorageError::BackupDestinationExists(path)) if path == backup_path
    ));
    assert!(matches!(
        Database::restore_backup(&backup_path, &restored_path),
        Err(StorageError::BackupDestinationExists(path)) if path == restored_path
    ));
    assert!(matches!(
        database.backup_to(database.path()),
        Err(StorageError::BackupDestinationIsSource)
    ));
    assert!(matches!(
        Database::restore_backup(&backup_path, &backup_path),
        Err(StorageError::BackupDestinationIsSource)
    ));
}

#[test]
fn backup_destination_is_literal_and_cannot_overwrite_a_file_uri_target() {
    let temporary = TempDir::new("backup-uri");
    let source = Database::open(temporary.database("source.db")).expect("open source");
    let target_path = temporary.database("existing.db");
    let target = Database::open(&target_path).expect("open target");
    target
        .connect()
        .expect("connect target")
        .execute(
            "INSERT INTO principals(id, display_name, created_at) VALUES ('018f1f59-6e90-7000-8000-000000000004', 'Sentinel', ?1)",
            [NOW],
        )
        .expect("insert target 018f1f59-6e90-7000-8000-000000000004");
    drop(target);

    let uri = PathBuf::from(format!("file:{}", target_path.display()));
    let _ = source.backup_to(uri);

    let target = Database::open(&target_path).expect("reopen target");
    assert_eq!(
        target
            .connect_read_only()
            .expect("read target")
            .query_row(
                "SELECT count(*) FROM principals WHERE id = '018f1f59-6e90-7000-8000-000000000004'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("query target 018f1f59-6e90-7000-8000-000000000004"),
        1,
        "URI-shaped destinations must not address another database"
    );
}

#[test]
fn concurrent_backups_atomically_claim_one_destination() {
    let temporary = TempDir::new("backup-reservation");
    let source =
        Arc::new(Database::open(temporary.database("source.db")).expect("open source database"));
    let destination = Arc::new(temporary.database("backup.db"));
    let barrier = Arc::new(Barrier::new(2));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let source = Arc::clone(&source);
        let destination = Arc::clone(&destination);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            source.backup_to(destination.as_path())
        }));
    }

    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().expect("backup thread"))
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(StorageError::BackupDestinationExists(_))))
            .count(),
        1
    );
    Database::verify_backup(destination.as_path()).expect("verify reserved destination");
}

#[test]
fn backup_failures_and_truncated_files_are_rejected() {
    let temporary = TempDir::new("backup-failures");
    let database = Database::open(temporary.database("journal.db")).expect("open database");

    let missing_parent = temporary.path.join("missing").join("backup.db");
    assert!(database.backup_to(&missing_parent).is_err());
    assert!(!missing_parent.exists());

    let truncated = temporary.database("truncated.db");
    fs::write(&truncated, b"not a sqlite database").expect("write truncated backup");
    assert!(Database::verify_backup(&truncated).is_err());
}

#[test]
fn failed_destination_verification_removes_the_owned_backup() {
    let temporary = TempDir::new("backup-verification-cleanup");
    let database = Database::open(temporary.database("journal.db")).expect("open database");
    database
        .connect()
        .expect("connect")
        .execute("DROP TABLE records_fts", [])
        .expect("remove FTS table");

    let destination = temporary.database("backup.db");
    assert!(database.backup_to(&destination).is_err());
    assert!(
        !destination.exists(),
        "an unverified backup must not remain at its final destination"
    );

    database
        .connect()
        .expect("connect")
        .execute_batch(
            "CREATE VIRTUAL TABLE records_fts USING fts5(
                record_id UNINDEXED,
                space_id UNINDEXED,
                content,
                author_principal_id UNINDEXED,
                kind UNINDEXED,
                run_id UNINDEXED,
                tokenize = 'unicode61'
            );",
        )
        .expect("restore FTS table");
    database
        .backup_to(&destination)
        .expect("retry verified backup at the same destination");
}

#[test]
fn connection_factory_supports_concurrent_readers_and_writers() {
    const WRITERS: usize = 4;
    const RECORDS_PER_WRITER: usize = 20;

    let temporary = TempDir::new("concurrent");
    let database =
        Arc::new(Database::open(temporary.database("journal.db")).expect("open database"));
    let connection = database.connect().expect("connect");
    seed_space(&connection);
    drop(connection);

    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let mut threads = Vec::new();
    for writer in 0..WRITERS {
        let database = Arc::clone(&database);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            for offset in 0..RECORDS_PER_WRITER {
                let sequence = (writer * RECORDS_PER_WRITER + offset + 1) as i64;
                let id = format!("r-{sequence}");
                database
                    .with_transaction(|transaction| {
                        transaction.execute(
                            "INSERT INTO records(
                                id, space_id, space_seq, author_principal_id, kind, content, created_at
                             ) VALUES (?1, 's1', ?2, '018f1f59-6e90-7000-8000-000000000001', 'message', 'concurrent', ?3)",
                            params![id, sequence, NOW],
                        )?;
                        Ok(())
                    })
                    .expect("concurrent write");
            }
        }));
    }

    barrier.wait();
    let reader = database.connect_read_only().expect("reader connection");
    for _ in 0..RECORDS_PER_WRITER {
        reader
            .query_row("SELECT count(*) FROM records", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("concurrent read");
        std::thread::yield_now();
    }
    for thread in threads {
        thread.join().expect("writer thread");
    }

    assert_eq!(
        reader
            .query_row("SELECT count(*) FROM records", [], |row| row
                .get::<_, i64>(0))
            .expect("final record count"),
        (WRITERS * RECORDS_PER_WRITER) as i64
    );
}

#[test]
fn factory_path_is_stable_for_blocking_task_handoffs() {
    let temporary = TempDir::new("factory");
    let path = temporary.database("journal.db");
    let factory = ConnectionFactory::new(&path);
    assert_eq!(factory.path(), Path::new(&path));
    assert_eq!(factory.busy_timeout(), Duration::from_secs(5));
}
