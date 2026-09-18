//! Synchronous SQLite connection, migration, transaction, and backup primitives.

mod metrics;
pub use metrics::OperationalSnapshot;

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::backup::Backup;
use rusqlite::{Connection, ErrorCode, OpenFlags, Transaction, TransactionBehavior};
use thiserror::Error;

mod recovery;
pub use recovery::{RecoveryVerification, TableVerification};
mod recovery_audit;
pub use recovery_audit::{RecoveryApproval, RecoveryAudit, RecoveryStatus};

pub const MIGRATION_VERSION: i64 = 7;
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const BACKUP_PAGES_PER_STEP: i32 = 128;
const BACKUP_STEP_PAUSE: Duration = Duration::from_millis(5);
const CONNECTION_RETRY_PAUSE: Duration = Duration::from_millis(5);
const INITIAL_MIGRATION: &str = include_str!("../../../migrations/0001_initial.sql");

struct Migration {
    version: i64,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: INITIAL_MIGRATION,
    },
    Migration {
        version: 2,
        sql: include_str!("../../../migrations/0002_enrollment_recovery.sql"),
    },
    Migration {
        version: 3,
        sql: include_str!("../../../migrations/0003_records.sql"),
    },
    Migration {
        version: 4,
        sql: include_str!("../../../migrations/0004_thread_index.sql"),
    },
    Migration {
        version: 5,
        sql: include_str!("../../../migrations/0005_claim_credentials.sql"),
    },
    Migration {
        version: 6,
        sql: include_str!("../../../migrations/0006_host_custody.sql"),
    },
    Migration {
        version: 7,
        sql: include_str!("../../../migrations/0007_recovery_anchor.sql"),
    },
];

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
    #[error("cannot {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("table {table} returned invalid row count {count}")]
    InvalidRowCount { table: &'static str, count: i64 },
    #[error("backup destination already exists: {0}")]
    BackupDestinationExists(PathBuf),
    #[error("backup destination must differ from the source database")]
    BackupDestinationIsSource,
    #[error("backup integrity check failed: {0}")]
    BackupIntegrity(String),
    #[error("recovery is closed: {0}")]
    RecoveryClosed(&'static str),
    #[error("invalid recovery evidence: {0}")]
    RecoveryEvidence(#[from] serde_json::Error),
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
        let path = absolute_path(&self.path)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection = Connection::open_with_flags(path, flags)?;
        configure_connection(&connection, false)?;
        Ok(connection)
    }

    pub fn connect_read_only(&self) -> Result<Connection, StorageError> {
        let path = absolute_path(&self.path)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection = Connection::open_with_flags(path, flags)?;
        configure_connection(&connection, true)?;
        Ok(connection)
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    factory: ConnectionFactory,
    audit: Option<std::sync::Arc<RecoveryAudit>>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let factory = ConnectionFactory::new(path);
        let mut connection = factory.connect()?;
        apply_migrations(&mut connection)?;
        verify_schema(&connection)?;
        Ok(Self {
            factory,
            audit: None,
        })
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
        let _guard = self.audit.as_ref().map(|audit| audit.lock()).transpose()?;
        if let Some(audit) = &self.audit {
            audit.ensure_open(self)?;
        }
        let connection = self.factory.connect_read_only()?;
        verify_schema(&connection)?;
        Ok(connection)
    }

    pub fn recovery_audit(&self) -> Option<&RecoveryAudit> {
        self.audit.as_deref()
    }

    pub fn recovery_status(&self) -> Result<RecoveryStatus, StorageError> {
        match &self.audit {
            Some(audit) => {
                let _guard = audit.lock()?;
                audit.ensure_open(self)?;
                audit.status()
            }
            None => Ok(RecoveryStatus::default()),
        }
    }

    pub fn open_protected(
        path: impl AsRef<Path>,
        audit_path: impl AsRef<Path>,
    ) -> Result<Self, StorageError> {
        let lock = RecoveryAudit::acquire_lock(audit_path.as_ref())?;
        let mut database = Self::open(path)?;
        let audit = RecoveryAudit::open_with_lock(&database, audit_path.as_ref(), lock)?;
        audit.ensure_open(&database)?;
        database.audit = Some(std::sync::Arc::new(audit));
        Ok(database)
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let factory = ConnectionFactory::new(path);
        verify_schema(&factory.connect_read_only()?)?;
        Ok(Self {
            factory,
            audit: None,
        })
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
        let _guard = self.audit.as_ref().map(|audit| audit.lock()).transpose()?;
        if let Some(audit) = &self.audit {
            audit.ensure_open(self)?;
        }
        self.backup_to_unguarded(destination)
    }

    // The audit backup path already holds the non-reentrant writer guard.
    pub(crate) fn backup_to_unguarded(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<BackupVerification, StorageError> {
        let destination = ReservedDestination::new(destination.as_ref(), self.path())?;
        let source = self.factory.connect_read_only()?;
        verify_schema(&source)?;
        copy_database(&source, destination.path())?;
        let verification = Self::verify_backup(destination.path())?;
        destination.commit();
        Ok(verification)
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
        let backup = absolute_path(backup.as_ref())?;
        Self::verify_backup(&backup)?;
        let destination = ReservedDestination::new(destination.as_ref(), &backup)?;
        let source = ConnectionFactory::new(&backup).connect_read_only()?;
        copy_database(&source, destination.path())?;
        let verification = Self::verify_backup(destination.path())?;
        destination.commit();
        Ok(verification)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupVerification {
    pub schema_version: i64,
    pub record_count: u64,
    pub mailbox_item_count: u64,
}

fn copy_database(source: &Connection, destination: &Path) -> Result<(), StorageError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let mut destination_connection = Connection::open_with_flags(destination, flags)?;
    let backup = Backup::new(source, &mut destination_connection)?;
    backup.run_to_completion(BACKUP_PAGES_PER_STEP, BACKUP_STEP_PAUSE, None)?;
    Ok(())
}

struct ReservedDestination {
    path: PathBuf,
    committed: bool,
}

impl ReservedDestination {
    fn new(destination: &Path, source: &Path) -> Result<Self, StorageError> {
        let destination = absolute_path(destination)?;
        let source = absolute_path(source)?;
        if destination == source {
            return Err(StorageError::BackupDestinationIsSource);
        }

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
        {
            Ok(file) => drop(file),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(StorageError::BackupDestinationExists(destination));
            }
            Err(source) => {
                return Err(StorageError::Io {
                    operation: "reserve backup destination",
                    path: destination,
                    source,
                });
            }
        }
        Ok(Self {
            path: destination,
            committed: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ReservedDestination {
    fn drop(&mut self) {
        if !self.committed {
            remove_sqlite_files(&self.path);
        }
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf, StorageError> {
    std::path::absolute(path).map_err(|source| StorageError::Io {
        operation: "resolve database path",
        path: path.to_owned(),
        source,
    })
}

fn configure_connection(connection: &Connection, read_only: bool) -> Result<(), StorageError> {
    connection.busy_timeout(BUSY_TIMEOUT)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    if read_only {
        connection.pragma_update(None, "query_only", "ON")?;
    } else {
        set_wal_mode(connection)?;
        connection.pragma_update(None, "synchronous", "FULL")?;
    }
    verify_connection_policy(connection, read_only)
}

fn set_wal_mode(connection: &Connection) -> Result<(), StorageError> {
    let deadline = Instant::now() + BUSY_TIMEOUT;
    loop {
        match connection.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => return Ok(()),
            Err(error)
                if matches!(
                    error.sqlite_error_code(),
                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                ) && Instant::now() < deadline =>
            {
                std::thread::sleep(CONNECTION_RETRY_PAUSE);
            }
            Err(error) => return Err(error.into()),
        }
    }
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
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|source| StorageError::Migration {
            version: MIGRATION_VERSION,
            source,
        })?;
    let versions = read_schema_versions(&transaction)?;
    validate_schema_versions(&versions)?;
    let current = versions.last().copied().unwrap_or(0);

    for migration in MIGRATIONS
        .iter()
        .filter(|migration| migration.version > current)
    {
        if let Err(source) = transaction.execute_batch(migration.sql) {
            return Err(StorageError::Migration {
                version: migration.version,
                source,
            });
        }
    }
    verify_schema(&transaction)?;
    transaction
        .commit()
        .map_err(|source| StorageError::Migration {
            version: MIGRATION_VERSION,
            source,
        })
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

#[cfg(test)]
mod recovery_reads;
