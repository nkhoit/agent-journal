use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use journal_storage_sqlite::{
    BUSY_TIMEOUT, ConnectionFactory, Database, MIGRATION_VERSION, StorageError,
};
use rusqlite::{Connection, ErrorCode, params};

const NOW: &str = "2026-01-01T00:00:00Z";
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
            "INSERT OR IGNORE INTO principals(id, display_name, created_at) VALUES ('p1', 'P1', ?1)",
            [NOW],
        )
        .expect("insert principal");
    connection
        .execute(
            "INSERT OR IGNORE INTO spaces(id, name, created_at) VALUES ('s1', 'Space 1', ?1)",
            [NOW],
        )
        .expect("insert space");
}

fn insert_record(connection: &Connection, id: &str, sequence: i64, content: &str) {
    connection
        .execute(
            "INSERT INTO records(
                id, space_id, space_seq, author_principal_id, kind, content, created_at
             ) VALUES (?1, 's1', ?2, 'p1', 'message', ?3, ?4)",
            params![id, sequence, content, NOW],
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
fn open_applies_migrations_and_connection_policy() {
    let temporary = TempDir::new("open");
    let database = Database::open(temporary.database("journal.db")).expect("open database");
    let connection = database.connect().expect("connect");

    assert_eq!(
        database.schema_version().expect("schema version"),
        MIGRATION_VERSION
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
    for expected in ["principals", "records", "records_fts", "schema_migrations"] {
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
        MIGRATION_VERSION
    );
}

#[test]
fn transaction_helper_commits_or_rolls_back_atomically() {
    let temporary = TempDir::new("transactions");
    let database = Database::open(temporary.database("journal.db")).expect("open database");

    let error = database
        .with_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id, display_name, created_at) VALUES ('rolled-back', 'P', ?1)",
                [NOW],
            )?;
            Err::<(), _>(rusqlite::Error::InvalidQuery.into())
        })
        .expect_err("operation must roll back");
    assert!(matches!(error, StorageError::Sqlite(_)));

    database
        .with_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO principals(id, display_name, created_at) VALUES ('committed', 'P', ?1)",
                [NOW],
            )?;
            Ok(())
        })
        .expect("commit transaction");

    let connection = database.connect().expect("connect");
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM principals WHERE id IN ('rolled-back', 'committed')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count principals"),
        1
    );
}

#[test]
fn failed_migration_rolls_back_and_can_be_retried() {
    let temporary = TempDir::new("migration-rollback");
    let path = temporary.database("journal.db");
    let connection = Connection::open(&path).expect("open conflicting database");
    connection
        .execute("CREATE TABLE principals(id TEXT PRIMARY KEY)", [])
        .expect("create conflicting table");
    drop(connection);

    assert!(matches!(
        Database::open(&path),
        Err(StorageError::Migration { version: 1, .. })
    ));

    let connection = Connection::open(&path).expect("reopen failed migration");
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'schema_migrations'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("probe migration table"),
        0,
        "failed migration must not leave its version table"
    );
    connection
        .execute("DROP TABLE principals", [])
        .expect("remove injected conflict");
    drop(connection);

    let database = Database::open(&path).expect("retry migration");
    assert_eq!(database.schema_version().expect("schema version"), 1);
}

#[test]
fn newer_schema_is_rejected_without_mutation() {
    let temporary = TempDir::new("schema-mismatch");
    let path = temporary.database("journal.db");
    let database = Database::open(&path).expect("open database");
    let connection = database.connect().expect("connect");
    connection
        .execute(
            "UPDATE schema_migrations SET version = 2 WHERE version = 1",
            [],
        )
        .expect("advance schema artificially");
    drop(connection);
    drop(database);

    assert!(matches!(
        Database::open(&path),
        Err(StorageError::IncompatibleSchema {
            found: 2,
            supported: 1
        })
    ));
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
    assert_eq!(verification.schema_version, MIGRATION_VERSION);
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
        1
    );
    assert!(matches!(
        database.backup_to(&backup_path),
        Err(StorageError::BackupDestinationExists(path)) if path == backup_path
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
                             ) VALUES (?1, 's1', ?2, 'p1', 'message', 'concurrent', ?3)",
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
