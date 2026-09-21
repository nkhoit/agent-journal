//! Synchronous SQLite connection, migration, transaction, and backup primitives.

mod metrics;
pub use metrics::OperationalSnapshot;

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use fs2::FileExt;
use rusqlite::backup::Backup;
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use thiserror::Error;

mod recovery;
pub use recovery::{RecoveryVerification, TableVerification};
mod recovery_audit;
pub use recovery_audit::{RecoveryApproval, RecoveryAudit, RecoveryStatus};

/// The sole supported central on-disk contract. Existing databases are never
/// upgraded in place: archive/reset them and initialize a new UUID-native DB.
pub const CURRENT_SCHEMA_VERSION: i64 = 12;
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

const BACKUP_PAGES_PER_STEP: i32 = 128;
const BACKUP_STEP_PAUSE: Duration = Duration::from_millis(5);
const CONNECTION_RETRY_PAUSE: Duration = Duration::from_millis(5);
const CURRENT_SCHEMA_CONTRACT: &str = "uuid-native-v1";
const UUID_NATIVE_BASELINE: &str = include_str!("../../../migrations/0001_uuid_native.sql");

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaObject {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

static EXPECTED_SCHEMA_OBJECTS: OnceLock<Vec<SchemaObject>> = OnceLock::new();

// Stable file identity and link-count metadata is available in Rust 1.85 on
// Unix. Non-Unix builds still validate regular files but deliberately avoid
// unstable platform metadata APIs.
#[cfg(unix)]
type CentralIdentity = (u64, u64);
#[cfg(not(unix))]
type CentralIdentity = ();

#[derive(Debug, Clone, Copy)]
enum CentralAdmission {
    Unbound,
    Bound(CentralIdentity),
    Refused,
}

struct InitializationLock {
    path: PathBuf,
    _file: std::fs::File,
}

impl Drop for InitializationLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("fresh UUID-native schema initialization failed: {source}")]
    Baseline {
        #[source]
        source: rusqlite::Error,
    },
    #[error(
        "reset required: existing {kind} is not the UUID-native v1 schema; archive it and initialize a fresh path"
    )]
    ResetRequired { kind: &'static str },
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
    admission: Arc<Mutex<CentralAdmission>>,
}

impl ConnectionFactory {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_owned();
        let admission = match fs::symlink_metadata(&path) {
            Ok(_) => current_central_identity(&path)
                .map(CentralAdmission::Bound)
                .unwrap_or(CentralAdmission::Refused),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                CentralAdmission::Unbound
            }
            Err(_) => CentralAdmission::Refused,
        };
        Self {
            path,
            admission: Arc::new(Mutex::new(admission)),
        }
    }

    fn admitted(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let factory = Self::new(path);
        factory.admit_existing()?;
        Ok(factory)
    }

    fn admit_existing(&self) -> Result<(), StorageError> {
        *self.admission()? = CentralAdmission::Bound(current_central_identity(&self.path)?);
        Ok(())
    }

    fn verify_admitted_identity(&self) -> Result<(), StorageError> {
        match *self.admission()? {
            CentralAdmission::Unbound => Ok(()),
            CentralAdmission::Bound(expected)
                if current_central_identity(&self.path)? == expected =>
            {
                Ok(())
            }
            CentralAdmission::Bound(_) | CentralAdmission::Refused => {
                Err(StorageError::ResetRequired {
                    kind: "central database",
                })
            }
        }
    }

    fn bind_after_open(&self) -> Result<(), StorageError> {
        let actual = current_central_identity(&self.path)?;
        let mut admission = self.admission()?;
        match *admission {
            CentralAdmission::Unbound => {
                *admission = CentralAdmission::Bound(actual);
                Ok(())
            }
            CentralAdmission::Bound(expected) if expected == actual => Ok(()),
            CentralAdmission::Bound(_) | CentralAdmission::Refused => {
                Err(StorageError::ResetRequired {
                    kind: "central database",
                })
            }
        }
    }

    fn admission(&self) -> Result<std::sync::MutexGuard<'_, CentralAdmission>, StorageError> {
        self.admission
            .lock()
            .map_err(|_| StorageError::ResetRequired {
                kind: "central database",
            })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn busy_timeout(&self) -> Duration {
        BUSY_TIMEOUT
    }

    pub fn connect(&self) -> Result<Connection, StorageError> {
        let connection = self.open_read_write()?;
        reject_protected_writer(&connection)?;
        configure_connection(&connection, false)?;
        Ok(connection)
    }

    pub(crate) fn connect_unchecked(&self) -> Result<Connection, StorageError> {
        let connection = self.open_read_write()?;
        configure_connection(&connection, false)?;
        Ok(connection)
    }

    fn open_read_write(&self) -> Result<Connection, StorageError> {
        self.verify_admitted_identity()?;
        let path = absolute_path(&self.path)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection = Connection::open_with_flags(path, flags)?;
        // SQLite has not received SQL or pragmas from us yet. Revalidate the
        // path after open so an admitted path replacement or added hardlink
        // cannot reach configuration or a transaction through this factory.
        self.bind_after_open()?;
        Ok(connection)
    }

    pub fn connect_read_only(&self) -> Result<Connection, StorageError> {
        let connection = self.open_read_only_unconfigured()?;
        configure_connection(&connection, true)?;
        Ok(connection)
    }

    fn open_read_only_unconfigured(&self) -> Result<Connection, StorageError> {
        self.verify_admitted_identity()?;
        let path = absolute_path(&self.path)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection = Connection::open_with_flags(path, flags)?;
        self.bind_after_open()?;
        Ok(connection)
    }

    /// Inspect an existing file without participating in SQLite journal or WAL
    /// recovery. Unsupported files are rejected before SQLite can alter their
    /// database, hot journal, WAL, or shared-memory evidence.
    fn open_immutable_read_only(&self) -> Result<Connection, StorageError> {
        if sqlite_sidecar_entries_exist(&self.path)? {
            return Err(StorageError::ResetRequired {
                kind: "central database",
            });
        }
        self.verify_admitted_identity()?;
        let path = absolute_path(&self.path)?;
        let uri = immutable_uri(&path).ok_or(StorageError::ResetRequired {
            kind: "central database",
        })?;
        let connection = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(|_| StorageError::ResetRequired {
            kind: "central database",
        })?;
        self.bind_after_open()?;
        Ok(connection)
    }

    pub(crate) fn connect_recovery_read_only(&self) -> Result<Connection, StorageError> {
        self.open_read_only_unconfigured()
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
        let initialization_path = initialization_lock_path(factory.path());
        let initialization_pending = initialization_path.exists();
        if sqlite_sidecar_entries_exist(factory.path())? {
            // A sidecar is normally fail-closed hot evidence. The sole exception
            // is an active, serialized fresh initialization: wait for that owner
            // to finish instead of misclassifying its transient WAL as cold
            // admission. A stale or inaccessible initializer still times out
            // reset-required without SQLite mutation.
            if initialization_pending {
                wait_for_initialization(factory.path())?;
                return Self::open(factory.path());
            }
            return Err(StorageError::ResetRequired {
                kind: "central database",
            });
        }
        let initialization_lock = if !factory.path().exists() || initialization_pending {
            match try_acquire_initialization_lock(factory.path())? {
                Some(lock) => Some(lock),
                None => {
                    wait_for_initialization(factory.path())?;
                    return Self::open(factory.path());
                }
            }
        } else {
            None
        };
        if factory.path().exists() {
            drop(initialization_lock);
            if sqlite_sidecar_entries_exist(factory.path())? {
                return Err(StorageError::ResetRequired {
                    kind: "central database",
                });
            }
            let existing = Self::open_existing(factory.path());
            if existing.is_ok() || matches!(&existing, Err(StorageError::ResetRequired { .. })) {
                return existing;
            }
            for _ in 0..100 {
                thread::sleep(Duration::from_millis(10));
                let retry = Self::open_existing(factory.path());
                if retry.is_ok() {
                    return retry;
                }
            }
            return existing;
        }
        if sqlite_sidecar_entries_exist(factory.path())? {
            return Err(StorageError::ResetRequired {
                kind: "central database",
            });
        }
        let _initialization_lock =
            initialization_lock.expect("missing database owns initialization lock");
        let mut connection = factory.connect_unchecked()?;
        if let Err(baseline_error) = apply_uuid_native_baseline(&mut connection) {
            drop(connection);
            for _ in 0..100 {
                let current = factory.open_read_only_unconfigured();
                if current
                    .and_then(|connection| verify_schema(&connection))
                    .is_ok()
                {
                    factory.admit_existing()?;
                    return Ok(Self {
                        factory,
                        audit: None,
                    });
                }
                thread::sleep(Duration::from_millis(10));
            }
            return Err(baseline_error);
        }
        verify_schema(&connection)?;
        drop(connection);
        factory.admit_existing()?;
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

    pub(crate) fn connect_unchecked(&self) -> Result<Connection, StorageError> {
        self.factory.connect_unchecked()
    }

    pub(crate) fn connect_recovery_read_only(&self) -> Result<Connection, StorageError> {
        self.factory.connect_recovery_read_only()
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
        let path = path.as_ref();
        let audit_path = audit_path.as_ref();
        // Existing input is admitted read-only before an audit lock, SQLite
        // writer, or recovery artifact can be created. This keeps every legacy
        // database and malformed current lineage byte-for-byte untouched.
        let mut database = if path.exists() {
            let database = Self::open_existing(path)?;
            RecoveryAudit::preflight_admission(&database, audit_path)?;
            database
        } else {
            if RecoveryAudit::entry_exists(audit_path)? {
                return Err(StorageError::RecoveryClosed(
                    "existing recovery audit has no central UUID-native database",
                ));
            }
            Self::open(path)?
        };
        let lock = RecoveryAudit::acquire_lock(audit_path)?;
        let audit = RecoveryAudit::open_with_lock(&database, audit_path, lock, true)?;
        audit.ensure_open(&database)?;
        database.audit = Some(std::sync::Arc::new(audit));
        Ok(database)
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if sqlite_sidecar_entries_exist(path)? {
            return Err(StorageError::ResetRequired {
                kind: "central database",
            });
        }
        let factory = ConnectionFactory::admitted(path)?;
        let connection = factory.open_immutable_read_only()?;
        verify_schema(&connection)?;
        factory.verify_admitted_identity()?;
        Ok(Self {
            factory,
            audit: None,
        })
    }

    pub fn schema_version(&self) -> Result<i64, StorageError> {
        let connection = self.connect_read_only()?;
        read_schema_version(&connection)
    }

    /// Convert this process's live WAL state back into a standalone database
    /// after its servers have stopped accepting work and every request
    /// connection has closed. Cold admission remains intentionally stricter:
    /// it refuses any pre-existing sidecar instead of attempting recovery.
    pub fn normalize_for_clean_shutdown(&self) -> Result<(), StorageError> {
        let audit = self.audit.as_deref();
        let _guard = audit.map(|audit| audit.lock()).transpose()?;
        if let Some(audit) = audit {
            audit.ensure_open(self)?;
        }
        let connection = self.factory.connect_unchecked()?;
        verify_schema(&connection)?;
        connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
        drop(connection);
        if sqlite_sidecar_entries_exist(self.path())? {
            return Err(StorageError::ResetRequired {
                kind: "central database",
            });
        }
        Ok(())
    }

    pub fn with_transaction<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, StorageError>,
    {
        self.with_transaction_for(operation)
    }

    pub fn with_transaction_for<T, E, F>(&self, operation: F) -> Result<T, E>
    where
        E: From<StorageError>,
        F: FnOnce(&Transaction<'_>) -> Result<T, E>,
    {
        let audit = self.audit.as_deref();
        let _guard = audit
            .map(|audit| audit.lock())
            .transpose()
            .map_err(E::from)?;
        if let Some(audit) = audit {
            audit.ensure_open(self).map_err(E::from)?;
        }
        let mut connection = match audit {
            Some(_) => self.factory.connect_unchecked().map_err(E::from)?,
            None => self.factory.connect().map_err(E::from)?,
        };
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StorageError::from)
            .map_err(E::from)?;
        let before = transaction.total_changes();
        match operation(&transaction) {
            Ok(value) => {
                let revision = if transaction.total_changes() != before {
                    audit
                        .map(|audit| audit.prepare(&transaction))
                        .transpose()
                        .map_err(E::from)?
                        .flatten()
                } else {
                    None
                };
                transaction
                    .commit()
                    .map_err(StorageError::from)
                    .map_err(E::from)?;
                if let Some(audit) = audit {
                    audit.committed(revision).map_err(E::from)?;
                }
                Ok(value)
            }
            Err(operation_error) => {
                transaction
                    .rollback()
                    .map_err(StorageError::from)
                    .map_err(E::from)?;
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
    {
        let backup = Backup::new(source, &mut destination_connection)?;
        backup.run_to_completion(BACKUP_PAGES_PER_STEP, BACKUP_STEP_PAUSE, None)?;
    }
    // The reserved destination is new and private. Normalize it to a standalone
    // main database before exposing it to current-schema admission, which
    // intentionally refuses every existing sidecar as untrusted hot evidence.
    destination_connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
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

fn immutable_uri(path: &Path) -> Option<String> {
    let path = path.to_str()?;
    let encoded = path
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b'.' | b'-' | b'_' => {
                vec![byte as char]
            }
            other => format!("%{other:02X}").chars().collect(),
        })
        .collect::<String>();
    Some(format!("file:{encoded}?immutable=1"))
}

/// A separate hardlink can give the same authoritative inode independent
/// path-derived recovery locks. Admission binds a Unix `(dev, ino)` identity
/// and every later factory open rechecks both that identity and a single link.
/// This is accidental/operator alias protection; a hostile same-UID peer can
/// still race these observations and remains outside this boundary.
fn current_central_identity(path: &Path) -> Result<CentralIdentity, StorageError> {
    let reset = || StorageError::ResetRequired {
        kind: "central database",
    };
    let metadata = fs::metadata(path).map_err(|_| reset())?;
    if !metadata.is_file() {
        return Err(reset());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(reset());
        }
        Ok((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Ok(())
    }
}

/// Cold admission uses lstat semantics: an entry is evidence even when it is a
/// dangling symlink. Lookup failures also fail closed rather than treating an
/// unreadable sidecar as absent.
fn sqlite_sidecar_entries_exist(path: &Path) -> Result<bool, StorageError> {
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        let sidecar = PathBuf::from(sidecar);
        match fs::symlink_metadata(&sidecar) {
            Ok(_) => return Ok(true),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(StorageError::ResetRequired {
                    kind: "central database",
                });
            }
        }
    }
    Ok(false)
}

fn initialization_lock_path(path: &Path) -> PathBuf {
    let mut lock = path.as_os_str().to_owned();
    lock.push(".initializing");
    PathBuf::from(lock)
}

fn try_acquire_initialization_lock(
    path: &Path,
) -> Result<Option<InitializationLock>, StorageError> {
    let lock_path = initialization_lock_path(path);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| StorageError::Io {
            operation: "open",
            path: lock_path.clone(),
            source,
        })?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(InitializationLock {
            path: lock_path,
            _file: file,
        })),
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(source) => Err(StorageError::Io {
            operation: "lock",
            path: lock_path,
            source,
        }),
    }
}

fn wait_for_initialization(path: &Path) -> Result<(), StorageError> {
    let lock_path = initialization_lock_path(path);
    for _ in 0..100 {
        thread::sleep(Duration::from_millis(10));
        if !lock_path.exists() {
            return Ok(());
        }
    }
    Err(StorageError::ResetRequired {
        kind: "central database",
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

fn recovery_protection_enabled(connection: &Connection) -> Result<bool, StorageError> {
    let anchor_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='recovery_anchor')",
        [],
        |row| row.get(0),
    )?;
    if !anchor_exists {
        return Ok(false);
    }
    Ok(connection.query_row(
        "SELECT audit_required FROM recovery_anchor WHERE singleton=1",
        [],
        |row| row.get(0),
    )?)
}

fn reject_protected_writer(connection: &Connection) -> Result<(), StorageError> {
    if recovery_protection_enabled(connection)? {
        Err(StorageError::RecoveryClosed(
            "protected writes require the audited transaction wrapper",
        ))
    } else {
        Ok(())
    }
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
    // A newly materialized backup is intentionally a standalone DELETE-mode
    // file until a writable owner adopts it. Read-only callers do not need to
    // impose WAL mode, and must not create a sidecar while inspecting it.
    if !read_only {
        verify_pragma_string(connection, "journal_mode", "wal")?;
    }
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

fn apply_uuid_native_baseline(connection: &mut Connection) -> Result<(), StorageError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|source| StorageError::Baseline { source })?;
    transaction
        .execute_batch(UUID_NATIVE_BASELINE)
        .map_err(|source| StorageError::Baseline { source })?;
    verify_schema(&transaction)?;
    transaction
        .commit()
        .map_err(|source| StorageError::Baseline { source })?;
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(|source| StorageError::Baseline { source })
}

fn verify_schema(connection: &Connection) -> Result<(), StorageError> {
    let reset = || StorageError::ResetRequired {
        kind: "central database",
    };
    let has_contract = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='schema_contract')",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|_| reset())?;
    if !has_contract {
        return Err(reset());
    }
    let contract: Option<(i64, String)> = connection
        .query_row(
            "SELECT version,format FROM schema_contract WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| reset())?;
    if contract != Some((CURRENT_SCHEMA_VERSION, CURRENT_SCHEMA_CONTRACT.to_owned())) {
        return Err(reset());
    }
    let actual = schema_objects(connection).map_err(|_| reset())?;
    if actual != expected_schema_objects() {
        return Err(reset());
    }
    Ok(())
}

fn schema_objects(connection: &Connection) -> rusqlite::Result<Vec<SchemaObject>> {
    let mut statement = connection.prepare(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema
         WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name,tbl_name",
    )?;
    statement
        .query_map([], |row| {
            Ok(SchemaObject {
                object_type: row.get(0)?,
                name: row.get(1)?,
                table_name: row.get(2)?,
                sql: row
                    .get::<_, Option<String>>(3)?
                    .map(|sql| normalize_schema_sql(&sql)),
            })
        })?
        .collect()
}

/// SQLite preserves much of the original DDL spelling in `sqlite_schema`.
/// Compare the direct baseline's semantic token stream instead, so harmless
/// whitespace/comments do not create a second compatibility branch while every
/// table constraint, index, trigger, object name, and operator remains bound.
fn normalize_schema_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut characters = sql.chars().peekable();
    let mut quote = None;
    let mut whitespace = false;
    while let Some(character) = characters.next() {
        if let Some(delimiter) = quote {
            normalized.push(character);
            if character == delimiter {
                if characters.peek() == Some(&delimiter) {
                    normalized.push(characters.next().expect("peeked quote exists"));
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match character {
            '\'' | '"' | '`' => {
                if whitespace && !normalized.is_empty() {
                    normalized.push(' ');
                }
                whitespace = false;
                quote = Some(character);
                normalized.push(character);
            }
            '[' => {
                if whitespace && !normalized.is_empty() {
                    normalized.push(' ');
                }
                whitespace = false;
                quote = Some(']');
                normalized.push(character);
            }
            '/' if characters.peek() == Some(&'*') => {
                characters.next();
                while let Some(comment) = characters.next() {
                    if comment == '*' && characters.peek() == Some(&'/') {
                        characters.next();
                        break;
                    }
                }
                whitespace = true;
            }
            '-' if characters.peek() == Some(&'-') => {
                characters.next();
                for comment in characters.by_ref() {
                    if comment == '\n' {
                        break;
                    }
                }
                whitespace = true;
            }
            character if character.is_whitespace() => whitespace = true,
            _ => {
                if whitespace && !normalized.is_empty() {
                    normalized.push(' ');
                }
                whitespace = false;
                normalized.push(character);
            }
        }
    }
    normalized
}

fn expected_schema_objects() -> &'static [SchemaObject] {
    EXPECTED_SCHEMA_OBJECTS
        .get_or_init(|| {
            let connection = Connection::open_in_memory()
                .expect("UUID-native baseline must initialize in memory");
            connection
                .execute_batch(UUID_NATIVE_BASELINE)
                .expect("UUID-native baseline must define a valid schema");
            schema_objects(&connection).expect("UUID-native baseline schema must be readable")
        })
        .as_slice()
}

fn read_schema_version(connection: &Connection) -> Result<i64, StorageError> {
    verify_schema(connection)?;
    Ok(CURRENT_SCHEMA_VERSION)
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
