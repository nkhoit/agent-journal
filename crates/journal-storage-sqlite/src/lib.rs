//! Synchronous SQLite connection, migration, transaction, and backup primitives.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::backup::Backup;
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use thiserror::Error;

pub const MIGRATION_VERSION: i64 = 1;
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const BACKUP_PAGES_PER_STEP: i32 = 128;
const BACKUP_STEP_PAUSE: Duration = Duration::from_millis(5);
const INITIAL_MIGRATION: &str = include_str!("../../../migrations/0001_initial.sql");

struct Migration {
    version: i64,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: INITIAL_MIGRATION,
}];

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("migration {version} failed: {source}")]
    Migration {
        version: i64,
        #[source]
        source: rusqlite::Error,
    },
    #[error("schema version {found} is newer than supported version {supported}")]
    IncompatibleSchema { found: i64, supported: i64 },
    #[error("schema migration history is missing, duplicated, or non-contiguous")]
    InvalidSchemaHistory,
    #[error("connection policy mismatch for {pragma}: expected {expected}, found {actual}")]
    ConnectionPolicy {
        pragma: &'static str,
        expected: String,
        actual: String,
    },
    #[error("table {table} returned invalid row count {count}")]
    InvalidRowCount { table: &'static str, count: i64 },
    #[error("backup destination already exists: {0}")]
    BackupDestinationExists(PathBuf),
    #[error("backup destination must differ from the source database")]
    BackupDestinationIsSource,
    #[error("backup integrity check failed: {0}")]
    BackupIntegrity(String),
}

#[derive(Debug, Clone)]
pub struct ConnectionFactory {
    path: PathBuf,
}

impl ConnectionFactory {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn busy_timeout(&self) -> Duration {
        BUSY_TIMEOUT
    }

    pub fn connect(&self) -> Result<Connection, StorageError> {
        let connection = Connection::open(&self.path)?;
        configure_connection(&connection, false)?;
        Ok(connection)
    }

    pub fn connect_read_only(&self) -> Result<Connection, StorageError> {
        let connection = Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        configure_connection(&connection, true)?;
        Ok(connection)
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    factory: ConnectionFactory,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let factory = ConnectionFactory::new(path);
        let mut connection = factory.connect()?;
        apply_migrations(&mut connection)?;
        verify_schema(&connection)?;
        Ok(Self { factory })
    }

    pub fn path(&self) -> &Path {
        self.factory.path()
    }

    pub fn connection_factory(&self) -> ConnectionFactory {
        self.factory.clone()
    }

    pub fn connect(&self) -> Result<Connection, StorageError> {
        self.factory.connect()
    }

    pub fn connect_read_only(&self) -> Result<Connection, StorageError> {
        let connection = self.factory.connect_read_only()?;
        verify_schema(&connection)?;
        Ok(connection)
    }

    pub fn schema_version(&self) -> Result<i64, StorageError> {
        let connection = self.connect_read_only()?;
        read_schema_version(&connection)
    }

    pub fn with_transaction<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, StorageError>,
    {
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        match operation(&transaction) {
            Ok(value) => {
                transaction.commit()?;
                Ok(value)
            }
            Err(operation_error) => {
                transaction.rollback()?;
                Err(operation_error)
            }
        }
    }

    pub fn backup_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<BackupVerification, StorageError> {
        let destination = destination.as_ref();
        if destination == self.path() {
            return Err(StorageError::BackupDestinationIsSource);
        }
        if destination.exists() {
            return Err(StorageError::BackupDestinationExists(
                destination.to_owned(),
            ));
        }

        let result = self.create_backup(destination);
        if result.is_err() {
            remove_sqlite_files(destination);
        }
        result?;
        Self::verify_backup(destination)
    }

    fn create_backup(&self, destination: &Path) -> Result<(), StorageError> {
        let source = self.connect_read_only()?;
        copy_database(&source, destination)
    }

    pub fn verify_backup(path: impl AsRef<Path>) -> Result<BackupVerification, StorageError> {
        let factory = ConnectionFactory::new(path);
        let connection = factory.connect_read_only()?;
        verify_schema(&connection)?;

        let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(StorageError::BackupIntegrity(integrity));
        }

        connection.query_row(
            "SELECT count(*) FROM records_fts WHERE records_fts MATCH 'agentjournalverificationtoken'",
            [],
            |row| row.get::<_, i64>(0),
        )?;

        Ok(BackupVerification {
            schema_version: read_schema_version(&connection)?,
            record_count: table_count(&connection, "records")?,
            mailbox_item_count: table_count(&connection, "mailbox_items")?,
        })
    }

    pub fn restore_backup(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<BackupVerification, StorageError> {
        let backup = backup.as_ref();
        let destination = destination.as_ref();
        if backup == destination {
            return Err(StorageError::BackupDestinationIsSource);
        }
        if destination.exists() {
            return Err(StorageError::BackupDestinationExists(
                destination.to_owned(),
            ));
        }

        Self::verify_backup(backup)?;
        let source = ConnectionFactory::new(backup).connect_read_only()?;
        let result = copy_database(&source, destination);
        if result.is_err() {
            remove_sqlite_files(destination);
        }
        result?;
        Self::verify_backup(destination)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupVerification {
    pub schema_version: i64,
    pub record_count: u64,
    pub mailbox_item_count: u64,
}

fn copy_database(source: &Connection, destination: &Path) -> Result<(), StorageError> {
    let mut destination_connection = Connection::open(destination)?;
    let backup = Backup::new(source, &mut destination_connection)?;
    backup.run_to_completion(BACKUP_PAGES_PER_STEP, BACKUP_STEP_PAUSE, None)?;
    Ok(())
}

fn configure_connection(connection: &Connection, read_only: bool) -> Result<(), StorageError> {
    connection.busy_timeout(BUSY_TIMEOUT)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    if read_only {
        connection.pragma_update(None, "query_only", "ON")?;
    } else {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
    }
    verify_connection_policy(connection, read_only)
}

fn verify_connection_policy(connection: &Connection, read_only: bool) -> Result<(), StorageError> {
    verify_pragma_i64(connection, "foreign_keys", 1)?;
    verify_pragma_string(connection, "journal_mode", "wal")?;
    verify_pragma_i64(connection, "synchronous", 2)?;
    verify_pragma_i64(connection, "busy_timeout", BUSY_TIMEOUT.as_millis() as i64)?;
    verify_pragma_i64(connection, "query_only", i64::from(read_only))?;
    Ok(())
}

fn verify_pragma_i64(
    connection: &Connection,
    pragma: &'static str,
    expected: i64,
) -> Result<(), StorageError> {
    let actual =
        connection.query_row(&format!("PRAGMA {pragma}"), [], |row| row.get::<_, i64>(0))?;
    if actual != expected {
        return Err(StorageError::ConnectionPolicy {
            pragma,
            expected: expected.to_string(),
            actual: actual.to_string(),
        });
    }
    Ok(())
}

fn verify_pragma_string(
    connection: &Connection,
    pragma: &'static str,
    expected: &str,
) -> Result<(), StorageError> {
    let actual = connection.query_row(&format!("PRAGMA {pragma}"), [], |row| {
        row.get::<_, String>(0)
    })?;
    if !actual.eq_ignore_ascii_case(expected) {
        return Err(StorageError::ConnectionPolicy {
            pragma,
            expected: expected.to_owned(),
            actual,
        });
    }
    Ok(())
}

fn apply_migrations(connection: &mut Connection) -> Result<(), StorageError> {
    let versions = read_schema_versions(connection)?;
    validate_schema_versions(&versions)?;
    let current = versions.last().copied().unwrap_or(0);

    for migration in MIGRATIONS
        .iter()
        .filter(|migration| migration.version > current)
    {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| StorageError::Migration {
                version: migration.version,
                source,
            })?;
        if let Err(source) = transaction.execute_batch(migration.sql) {
            return Err(StorageError::Migration {
                version: migration.version,
                source,
            });
        }
        transaction
            .commit()
            .map_err(|source| StorageError::Migration {
                version: migration.version,
                source,
            })?;
    }
    Ok(())
}

fn verify_schema(connection: &Connection) -> Result<(), StorageError> {
    let versions = read_schema_versions(connection)?;
    validate_schema_versions(&versions)?;
    let found = versions.last().copied().unwrap_or(0);
    if found != MIGRATION_VERSION {
        return Err(StorageError::IncompatibleSchema {
            found,
            supported: MIGRATION_VERSION,
        });
    }
    Ok(())
}

fn read_schema_version(connection: &Connection) -> Result<i64, StorageError> {
    let versions = read_schema_versions(connection)?;
    validate_schema_versions(&versions)?;
    versions
        .last()
        .copied()
        .ok_or(StorageError::InvalidSchemaHistory)
}

fn read_schema_versions(connection: &Connection) -> Result<Vec<i64>, StorageError> {
    let exists = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'schema_migrations'
         )",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !exists {
        return Ok(Vec::new());
    }

    let mut statement =
        connection.prepare("SELECT version FROM schema_migrations ORDER BY version")?;
    statement
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(StorageError::from)
}

fn validate_schema_versions(versions: &[i64]) -> Result<(), StorageError> {
    if let Some(found) = versions
        .last()
        .copied()
        .filter(|version| *version > MIGRATION_VERSION)
    {
        return Err(StorageError::IncompatibleSchema {
            found,
            supported: MIGRATION_VERSION,
        });
    }
    if versions
        .iter()
        .copied()
        .ne(1..=i64::try_from(versions.len()).expect("migration count fits i64"))
    {
        return Err(StorageError::InvalidSchemaHistory);
    }
    Ok(())
}

fn table_count(connection: &Connection, table: &'static str) -> Result<u64, StorageError> {
    debug_assert!(matches!(table, "records" | "mailbox_items"));
    let count = connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get::<_, i64>(0)
    })?;
    u64::try_from(count).map_err(|_| StorageError::InvalidRowCount { table, count })
}

fn remove_sqlite_files(path: &Path) {
    let _ = fs::remove_file(path);
    for suffix in ["-wal", "-shm"] {
        let mut companion = path.as_os_str().to_owned();
        companion.push(suffix);
        let _ = fs::remove_file(PathBuf::from(companion));
    }
}
