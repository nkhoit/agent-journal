use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params, types::Value};
use serde::{Deserialize, Serialize};

use crate::{Database, RecoveryVerification, StorageError};

// Ordered so restored ownership never depends on caller-provided SQL or table names.
const SECURITY_TABLES: &[&str] = &[
    "principals",
    "spaces",
    "adapter_identities",
    "memberships",
    "enrollment_installations",
    "adapter_registrations",
    "credentials",
    "enrollment_tickets",
    "credential_audit",
    "audit_events",
    "record_relations",
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum Cell {
    Null,
    Integer(i64),
    Text(String),
}

impl Cell {
    fn value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Integer(value) => Value::Integer(*value),
            Self::Text(value) => Value::Text(value.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Snapshot {
    tables: Vec<Vec<Vec<Cell>>>,
    space_heads: Vec<(String, i64)>,
}

#[derive(Debug)]
pub struct RecoveryAudit {
    path: PathBuf,
    _lock: File,
    serial: Mutex<()>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryApproval {
    pub verification: RecoveryVerification,
    pub audit_revision: i64,
    pub previous_space_heads: Vec<(String, i64)>,
    pub quiesced_adapters: Vec<(String, String)>,
    pub reconciled_spools: Vec<(String, String)>,
    pub reconciled_clients: Vec<String>,
    pub inventory_complete: bool,
    pub accepted_record_loss: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryStatus {
    pub last_backup_at: Option<String>,
    pub last_verified_restore_at: Option<String>,
}

impl RecoveryAudit {
    /// The directory and log are host-private. Never initialize a replacement
    /// audit when the central anchor says an audit already exists.
    pub fn open(database: &Database, path: &Path) -> Result<Self, StorageError> {
        Self::open_with_lock(database, path, Self::acquire_lock(path)?)
    }

    pub(crate) fn acquire_lock(path: &Path) -> Result<File, StorageError> {
        protect_parent(path)?;
        let lock_path = path.with_extension("recovery-lock");
        let lock = private_file(&lock_path, false)?;
        lock.try_lock_exclusive()
            .map_err(|source| io("lock recovery audit", &lock_path, source))?;
        Ok(lock)
    }

    pub(crate) fn open_with_lock(
        database: &Database,
        path: &Path,
        lock: File,
    ) -> Result<Self, StorageError> {
        if path.exists()
            && std::fs::canonicalize(path).map_err(|source| io("resolve audit", path, source))?
                == std::fs::canonicalize(database.path())
                    .map_err(|source| io("resolve central database", database.path(), source))?
        {
            return Err(StorageError::RecoveryClosed(
                "audit and central database must differ",
            ));
        }
        let mut central_connection = database.connect()?;
        let central = central_connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (journal_id, revision, required): (String, i64, bool) = central.query_row(
            "SELECT journal_id,revision,audit_required FROM recovery_anchor WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if !path.exists() {
            if required || revision != 0 {
                return Err(StorageError::RecoveryClosed(
                    "required external audit is missing",
                ));
            }
            let file = private_file(path, true)?;
            file.sync_all()
                .map_err(|source| io("sync recovery audit", path, source))?;
            sync_parent(path)?;
            let audit = audit_connection(path)?;
            audit.execute_batch(
                "CREATE TABLE control(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                   journal_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('open','closed')));
                 CREATE TABLE events(revision INTEGER PRIMARY KEY, snapshot TEXT NOT NULL,
                   outcome TEXT NOT NULL CHECK(outcome IN ('prepared','committed','reconciled')),
                   occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
                 CREATE TABLE recovery_events(id INTEGER PRIMARY KEY, kind TEXT NOT NULL,
                   detail TEXT NOT NULL,
                   occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));",
            )?;
            let snapshot = serde_json::to_string(&snapshot(&central)?)?;
            audit.execute(
                "INSERT INTO events(revision,snapshot,outcome) VALUES (0,?,'committed')",
                [&snapshot],
            )?;
            audit.execute("INSERT INTO control VALUES(1,?,'open')", [&journal_id])?;
            central.execute(
                "UPDATE recovery_anchor SET audit_required=1 WHERE singleton=1",
                [],
            )?;
        }
        validate_file(path)?;
        let audit = Self {
            path: path.to_owned(),
            _lock: lock,
            serial: Mutex::new(()),
        };
        let stored: String = audit.connection()?.query_row(
            "SELECT journal_id FROM control WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if stored != journal_id {
            return Err(StorageError::RecoveryClosed(
                "external audit belongs to another journal",
            ));
        }
        central.execute(
            "UPDATE recovery_anchor SET audit_required=1 WHERE singleton=1",
            [],
        )?;
        central.commit()?;
        Ok(audit)
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, ()>, StorageError> {
        self.serial
            .lock()
            .map_err(|_| StorageError::RecoveryClosed("audit writer panicked"))
    }

    fn connection(&self) -> Result<Connection, StorageError> {
        validate_file(&self.path)?;
        audit_connection(&self.path)
    }

    pub fn ensure_open(&self, database: &Database) -> Result<(), StorageError> {
        let audit = self.connection()?;
        let state: String =
            audit.query_row("SELECT state FROM control WHERE singleton=1", [], |row| {
                row.get(0)
            })?;
        let (revision, _, outcome) = head(&audit)?;
        let anchor: i64 = database.connect()?.query_row(
            "SELECT revision FROM recovery_anchor WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if state != "open" || outcome == "prepared" || revision != anchor {
            return Err(StorageError::RecoveryClosed(
                "audit intent, restore, or reconciliation is unresolved",
            ));
        }
        Ok(())
    }

    /// Called before the central commit while its writer transaction is held.
    pub fn prepare(&self, transaction: &Transaction<'_>) -> Result<Option<i64>, StorageError> {
        let audit = self.connection()?;
        let (revision, _, outcome) = head(&audit)?;
        if outcome == "prepared" {
            return Err(StorageError::RecoveryClosed(
                "external intent is unresolved",
            ));
        }
        let next = serde_json::to_string(&snapshot(transaction)?)?;
        let revision = revision
            .checked_add(1)
            .ok_or(StorageError::RecoveryClosed("audit revision exhausted"))?;
        audit.execute(
            "INSERT INTO events(revision,snapshot,outcome) VALUES (?,?,'prepared')",
            params![revision, next],
        )?;
        transaction.execute(
            "UPDATE recovery_anchor SET revision=? WHERE singleton=1",
            [revision],
        )?;
        Ok(Some(revision))
    }

    pub fn committed(&self, revision: Option<i64>) -> Result<(), StorageError> {
        if let Some(revision) = revision {
            let changed = self.connection()?.execute(
                "UPDATE events SET outcome='committed' WHERE revision=? AND outcome='prepared'",
                [revision],
            )?;
            if changed != 1 {
                return Err(StorageError::RecoveryClosed(
                    "audit completion did not match intent",
                ));
            }
        }
        Ok(())
    }

    /// Offline only: the exclusive lifetime lock excludes the running daemon.
    /// An uncertain intent is retained and conservatively becomes the security
    /// ceiling; all credentials are revoked before it can be acknowledged.
    pub fn close(&self) -> Result<(), StorageError> {
        self.connection()?.execute_batch(
            "BEGIN IMMEDIATE;
             UPDATE control SET state='closed' WHERE singleton=1;
             INSERT INTO recovery_events(kind,detail) VALUES('closed','offline recovery');
             COMMIT;",
        )?;
        Ok(())
    }

    pub fn backup(
        &self,
        database: &Database,
        destination: &Path,
    ) -> Result<RecoveryVerification, StorageError> {
        let _guard = self.lock()?;
        self.ensure_open(database)?;
        protect_parent(destination)?;
        database.backup_to(destination)?;
        sync_parent(destination)?;
        let verification = Database::open(destination)?.recovery_verification()?;
        self.connection()?.execute(
            "INSERT INTO recovery_events(kind,detail) VALUES('backup',?)",
            [serde_json::to_string(&verification)?],
        )?;
        Ok(verification)
    }

    pub fn restore(
        &self,
        backup: &Path,
        destination: &Path,
        adapters_quiesced: bool,
    ) -> Result<RecoveryApproval, StorageError> {
        let _guard = self.lock()?;
        self.close()?;
        if !adapters_quiesced {
            return Err(StorageError::RecoveryClosed(
                "adapter quiescence was not attested",
            ));
        }
        protect_parent(destination)?;
        Database::verify_backup(backup)?;
        let expected = Database::open(backup)?.recovery_verification()?;
        Database::restore_backup(backup, destination)?;
        sync_parent(destination)?;
        let restored = Database::open(destination)?;
        if restored.recovery_verification()? != expected {
            return Err(StorageError::RecoveryClosed(
                "restored hashes differ from backup",
            ));
        }
        self.reconcile(&restored, adapters_quiesced)
    }

    pub fn write_approval(path: &Path, approval: &RecoveryApproval) -> Result<(), StorageError> {
        use std::io::Write;
        protect_parent(path)?;
        let mut file = private_file(path, true)?;
        file.write_all(&serde_json::to_vec_pretty(approval)?)
            .and_then(|()| file.sync_all())
            .map_err(|source| io("write recovery approval", path, source))?;
        sync_parent(path)
    }

    pub fn read_approval(path: &Path) -> Result<RecoveryApproval, StorageError> {
        protect_parent(path)?;
        validate_file(path)?;
        let bytes =
            std::fs::read(path).map_err(|source| io("read recovery approval", path, source))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Also the explicit recovery path for a crash between either pair of commits.
    pub fn reconcile(
        &self,
        database: &Database,
        adapters_quiesced: bool,
    ) -> Result<RecoveryApproval, StorageError> {
        self.close()?;
        if !adapters_quiesced {
            return Err(StorageError::RecoveryClosed(
                "adapter quiescence was not attested",
            ));
        }
        let audit = self.connection()?;
        let (revision, serialized, outcome) = head(&audit)?;
        let latest: Snapshot = serde_json::from_str(&serialized)?;
        let mut connection = database.connect()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let expected_id: String =
            audit.query_row("SELECT journal_id FROM control", [], |row| row.get(0))?;
        let actual_id: String =
            transaction.query_row("SELECT journal_id FROM recovery_anchor", [], |row| {
                row.get(0)
            })?;
        if expected_id != actual_id {
            return Err(StorageError::RecoveryClosed(
                "restore belongs to another journal",
            ));
        }
        let anchor: i64 =
            transaction.query_row("SELECT revision FROM recovery_anchor", [], |row| row.get(0))?;
        if anchor > revision {
            return Err(StorageError::RecoveryClosed(
                "external audit is older than central state",
            ));
        }
        restore_security(&transaction, &latest)?;
        if outcome == "prepared" {
            transaction.execute(
                "UPDATE memberships SET can_read=0,can_append=0,can_admin=0",
                [],
            )?;
        }
        let revision = revision
            .checked_add(1)
            .ok_or(StorageError::RecoveryClosed("audit revision exhausted"))?;
        audit.execute(
            "INSERT INTO events(revision,snapshot,outcome) VALUES (?,?,'prepared')",
            params![revision, serde_json::to_string(&snapshot(&transaction)?)?],
        )?;
        transaction.execute(
            "UPDATE recovery_anchor SET revision=?,audit_required=1",
            [revision],
        )?;
        transaction.commit()?;
        let verification = database.recovery_verification()?;
        let adapters = installation_inventory(&audit)?;
        let clients = identifiers(&connection, "SELECT id FROM principals ORDER BY id")?;
        let approval = RecoveryApproval {
            verification,
            audit_revision: revision,
            previous_space_heads: latest.space_heads,
            quiesced_adapters: adapters.clone(),
            reconciled_spools: adapters,
            reconciled_clients: clients,
            inventory_complete: false,
            accepted_record_loss: false,
        };
        audit.execute(
            "INSERT INTO recovery_events(kind,detail) VALUES('reconciled',?)",
            [serde_json::to_string(&approval)?],
        )?;
        Ok(approval)
    }

    pub fn status(&self) -> Result<RecoveryStatus, StorageError> {
        let connection = self.connection()?;
        let timestamp =
            |kind: &str| -> Result<Option<String>, StorageError> {
                Ok(connection.query_row(
                "SELECT occurred_at FROM recovery_events WHERE kind=? ORDER BY id DESC LIMIT 1",
                [kind], |row| row.get(0),
            ).optional()?)
            };
        Ok(RecoveryStatus {
            last_backup_at: timestamp("backup")?,
            last_verified_restore_at: timestamp("verified-restore")?,
        })
    }

    /// The approval is an operator attestation, not automated spool reconciliation.
    /// Its complete inventory and exact post-recovery hashes bind that attestation.
    pub fn reopen(
        &self,
        database: &Database,
        approval: &RecoveryApproval,
    ) -> Result<(), StorageError> {
        let _guard = self.lock()?;
        let mut audit = self.connection()?;
        let (revision, _, _) = head(&audit)?;
        let connection = database.connect()?;
        let adapters = installation_inventory(&audit)?;
        let clients = identifiers(&connection, "SELECT id FROM principals ORDER BY id")?;
        let anchor: i64 =
            connection.query_row("SELECT revision FROM recovery_anchor", [], |row| row.get(0))?;
        let reconciled: Option<String> = audit.query_row(
            "SELECT detail FROM recovery_events WHERE kind='reconciled' ORDER BY id DESC LIMIT 1",
            [], |row| row.get(0),
        ).optional()?;
        let verification = database.recovery_verification()?;
        let mut recorded: RecoveryApproval = serde_json::from_str(
            reconciled
                .as_deref()
                .ok_or(StorageError::RecoveryClosed("reconciliation is missing"))?,
        )?;
        recorded.accepted_record_loss = true;
        recorded.inventory_complete = true;
        if !approval.accepted_record_loss
            || !approval.inventory_complete
            || revision != approval.audit_revision
            || revision != anchor
            || approval.verification != verification
            || &recorded != approval
            || approval.quiesced_adapters != adapters
            || approval.reconciled_spools != adapters
            || approval.reconciled_clients != clients
        {
            return Err(StorageError::RecoveryClosed(
                "recovery approval does not match probed state and inventory",
            ));
        }
        let live: i64 = connection.query_row(
            "SELECT (SELECT count(*) FROM credentials WHERE revoked_at IS NULL)
             +(SELECT count(*) FROM enrollment_tickets WHERE invalidated_at IS NULL)
             +(SELECT count(*) FROM claims WHERE state='active')
             +(SELECT count(*) FROM adapter_registrations WHERE status!='revoked')",
            [],
            |row| row.get(0),
        )?;
        if live != 0 {
            return Err(StorageError::RecoveryClosed(
                "restored authority is not fenced",
            ));
        }
        let transaction = audit.transaction()?;
        transaction.execute(
            "UPDATE events SET outcome='reconciled' WHERE revision=?",
            [revision],
        )?;
        transaction.execute("UPDATE control SET state='open' WHERE singleton=1", [])?;
        transaction.execute(
            "INSERT INTO recovery_events(kind,detail) VALUES('verified-restore',?)",
            [serde_json::to_string(approval)?],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

fn head(connection: &Connection) -> Result<(i64, String, String), StorageError> {
    Ok(connection.query_row(
        "SELECT revision,snapshot,outcome FROM events ORDER BY revision DESC LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?)
}

fn snapshot(connection: &Connection) -> Result<Snapshot, StorageError> {
    let mut tables = Vec::new();
    for table in SECURITY_TABLES {
        let count = connection
            .prepare(&format!("SELECT * FROM {table}"))?
            .column_count();
        let order = (1..=count)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut statement =
            connection.prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))?;
        let rows = statement
            .query_map([], |row| {
                (0..count)
                    .map(|index| {
                        Ok(match row.get::<_, Value>(index)? {
                            Value::Null => Cell::Null,
                            Value::Integer(value) => Cell::Integer(value),
                            Value::Text(value) => Cell::Text(value),
                            _ => return Err(rusqlite::Error::InvalidQuery),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?
            .collect::<Result<Vec<_>, _>>()?;
        tables.push(rows);
    }
    let mut statement = connection.prepare(
        "SELECT s.id,coalesce(max(r.space_seq),0) FROM spaces s
         LEFT JOIN records r ON r.space_id=s.id GROUP BY s.id ORDER BY s.id",
    )?;
    let space_heads = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Snapshot {
        tables,
        space_heads,
    })
}

fn restore_security(transaction: &Transaction<'_>, latest: &Snapshot) -> Result<(), StorageError> {
    if latest.tables.len() != SECURITY_TABLES.len() {
        return Err(StorageError::RecoveryClosed(
            "security snapshot shape differs",
        ));
    }
    // Revocations and ticket invalidation are deliberately stronger than replay.
    // Historical credentials, claims, receipts and events are never deleted.
    transaction.execute_batch(
        "UPDATE credentials SET revoked_at=coalesce(revoked_at,strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          revocation_reason='central restore';
         UPDATE enrollment_tickets SET invalidated_at=coalesce(invalidated_at,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
         UPDATE claims SET state='cancelled',closed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE state='active';
         UPDATE delivery_attempts SET state='pending' WHERE state='claimed';
         UPDATE mailbox_items SET state='pending' WHERE state='claimed';
         DELETE FROM memberships;",
    )?;
    for (table, rows) in SECURITY_TABLES.iter().zip(&latest.tables).take(6) {
        let mut statement = transaction.prepare(&format!("PRAGMA table_info({table})"))?;
        let columns = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let names = columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let keys = columns
            .iter()
            .filter(|(_, key)| *key != 0)
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let changes = columns
            .iter()
            .filter(|(_, key)| *key == 0)
            .map(|(name, _)| {
                if *table == "adapter_registrations" && name == "generation" {
                    "generation=max(adapter_registrations.generation,excluded.generation)"
                        .to_owned()
                } else {
                    format!("{name}=excluded.{name}")
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        let placeholders = vec!["?"; columns.len()].join(",");
        for row in rows {
            if row.len() != columns.len() {
                return Err(StorageError::RecoveryClosed(
                    "security snapshot row differs",
                ));
            }
            let values = row.iter().map(Cell::value).collect::<Vec<_>>();
            transaction.execute(
                &format!("INSERT INTO {table}({names}) VALUES({placeholders}) ON CONFLICT({keys}) DO UPDATE SET {changes}"),
                rusqlite::params_from_iter(values),
            )?;
        }
    }
    let exhausted: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM adapter_registrations WHERE generation>=9223372036854775807)",
        [],
        |row| row.get(0),
    )?;
    if exhausted {
        return Err(StorageError::RecoveryClosed(
            "registration generation exhausted",
        ));
    }
    transaction.execute_batch(
        "UPDATE adapter_registrations SET generation=generation+1,status='revoked',
          lease_expires_at='1970-01-01T00:00:00Z';
         UPDATE enrollment_installations SET recovery_authorized=1;",
    )?;
    Ok(())
}

fn identifiers(connection: &Connection, sql: &str) -> Result<Vec<String>, StorageError> {
    Ok(connection
        .prepare(sql)?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?)
}

fn installation_inventory(audit: &Connection) -> Result<Vec<(String, String)>, StorageError> {
    let mut inventory = std::collections::BTreeSet::new();
    let mut statement = audit.prepare("SELECT snapshot FROM events ORDER BY revision")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let snapshot: Snapshot = serde_json::from_str(&row.get::<_, String>(0)?)?;
        let installations = snapshot.tables.get(4).ok_or(StorageError::RecoveryClosed(
            "installation inventory is malformed",
        ))?;
        for installation in installations {
            let [Cell::Text(adapter), Cell::Text(instance), ..] = installation.as_slice() else {
                return Err(StorageError::RecoveryClosed(
                    "installation inventory is malformed",
                ));
            };
            inventory.insert((adapter.clone(), instance.clone()));
        }
    }
    Ok(inventory.into_iter().collect())
}

fn audit_connection(path: &Path) -> Result<Connection, StorageError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    connection.busy_timeout(crate::BUSY_TIMEOUT)?;
    connection.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA;")?;
    Ok(connection)
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> StorageError {
    StorageError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn private_file(path: &Path, exclusive: bool) -> Result<File, StorageError> {
    if path.exists() {
        validate_file(path)?;
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if exclusive {
        options.create_new(true);
    } else {
        options.create(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|source| io("open protected recovery file", path, source))
}

fn validate_file(path: &Path) -> Result<(), StorageError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect recovery file", path, source))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(StorageError::RecoveryClosed(
            "recovery file is not a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err(StorageError::RecoveryClosed("recovery file is not private"));
        }
    }
    Ok(())
}

fn protect_parent(path: &Path) -> Result<(), StorageError> {
    #[cfg(not(unix))]
    if !cfg!(test) {
        return Err(StorageError::RecoveryClosed(
            "protected recovery requires Unix",
        ));
    }
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let metadata = std::fs::symlink_metadata(parent)
        .map_err(|source| io("inspect recovery directory", parent, source))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(StorageError::RecoveryClosed(
            "recovery directory is not regular",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(StorageError::RecoveryClosed(
                "recovery directory is not private",
            ));
        }
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|source| io("sync recovery directory", parent, source))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "journal-audit-{}-{}",
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
            Database::open(self.0.join("central.db")).unwrap()
        }
        fn audit(&self, database: &Database) -> RecoveryAudit {
            RecoveryAudit::open(database, &self.0.join("audit.db")).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn seed(database: &Database) {
        database.connect().unwrap().execute_batch(
                "INSERT INTO principals VALUES('p','Principal','2026-01-01T00:00:00Z',NULL);
                 INSERT INTO spaces VALUES('s','Space','2026-01-01T00:00:00Z',NULL);
                 INSERT INTO memberships VALUES('s','p',1,1,0,'2026-01-01T00:00:00Z');
                 INSERT INTO adapter_identities VALUES('a','p','2026-01-01T00:00:00Z');
                 INSERT INTO enrollment_installations VALUES('a','installation-example',0,'2026-01-01T00:00:00Z');
                 INSERT INTO adapter_registrations VALUES('a','p','installation-example',1,'active',
                     '2026-01-01T00:00:00Z','2026-01-02T00:00:00Z','2026-01-01T00:00:00Z');
                 INSERT INTO credentials(id,principal_id,class,token_hash,created_at)
                     VALUES('c','p','principal-client','fixture-digest','2026-01-01T00:00:00Z');
                 INSERT INTO credentials(id,principal_id,class,token_hash,adapter_id,created_at,instance_id)
                     VALUES('d','p','delivery-adapter','fixture-delivery-digest','a','2026-01-01T00:00:00Z','installation-example');
                 INSERT INTO enrollment_tickets(ticket_hash,principal_id,adapter_id,expires_at)
                     VALUES(lower(hex(zeroblob(32))),'p','a','2026-01-02T00:00:00Z');
                 INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
                     VALUES('r1','s',1,'p','note','pending recovery','2026-01-01T00:00:00Z'),
                           ('r2','s',2,'p','note','retained custody','2026-01-01T00:00:00Z');
                 INSERT INTO attention VALUES('r1','p','2026-01-01T00:00:00Z'),('r2','p','2026-01-01T00:00:00Z');
                 INSERT INTO mailbox_items VALUES('m1','r1','p','pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
                     ('m2','r2','p','pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
                 UPDATE mailbox_items SET state='claimed';
                 UPDATE delivery_attempts SET state='claimed';
                 INSERT INTO claims(id,adapter_id,principal_id,instance_id,generation,state,lease_expires_at,created_at,credential_id)
                     VALUES('claim','a','p','installation-example',1,'active','2026-01-02T00:00:00Z','2026-01-01T00:00:00Z','d');
                 INSERT INTO claim_items VALUES('claim','m1','initial-m1'),('claim','m2','initial-m2');
                 INSERT INTO host_custody VALUES('initial-m2','m2','claim','2026-01-01T00:00:00Z');
                 UPDATE mailbox_items SET state='host-accepted' WHERE id='m2';
                 UPDATE delivery_attempts SET state='host-accepted' WHERE attempt_id='initial-m2';"
            ).unwrap();
    }

    fn mutate(database: &Database, audit: &RecoveryAudit, sql: &str) {
        let _guard = audit.lock().unwrap();
        audit.ensure_open(database).unwrap();
        let mut connection = database.connect().unwrap();
        let transaction = connection.transaction().unwrap();
        transaction.execute_batch(sql).unwrap();
        let revision = audit.prepare(&transaction).unwrap();
        transaction.commit().unwrap();
        audit.committed(revision).unwrap();
    }

    #[test]
    fn missing_audit_is_never_recreated_and_lock_excludes_another_owner() {
        let fixture = Fixture::new();
        let database = fixture.database();
        let audit = fixture.audit(&database);
        assert!(RecoveryAudit::open(&database, &fixture.0.join("audit.db")).is_err());
        drop(audit);
        std::fs::remove_file(fixture.0.join("audit.db")).unwrap();
        assert!(RecoveryAudit::open(&database, &fixture.0.join("audit.db")).is_err());
        assert!(!fixture.0.join("audit.db").exists());
    }

    #[test]
    fn rollback_and_closed_restart_are_refused() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        let audit = fixture.audit(&database);
        audit
            .backup(&database, &fixture.0.join("backup.db"))
            .unwrap();
        mutate(&database, &audit, "UPDATE memberships SET can_read=0");
        let old = Database::open(fixture.0.join("backup.db")).unwrap();
        assert!(audit.ensure_open(&old).is_err());
        audit.close().unwrap();
        drop(audit);
        let reopened = fixture.audit(&database);
        assert!(reopened.ensure_open(&database).is_err());
    }

    #[test]
    fn restore_replays_revocation_fences_authority_and_requires_exact_approval() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        let audit = fixture.audit(&database);
        audit
            .backup(&database, &fixture.0.join("backup.db"))
            .unwrap();
        mutate(
            &database,
            &audit,
            "UPDATE memberships SET can_read=0,can_append=0;
                 UPDATE principals SET disabled_at='2026-01-02T00:00:00Z';
                 UPDATE adapter_registrations SET generation=9,instance_id='replacement-example';
                 UPDATE enrollment_installations SET instance_id='replacement-example';",
        );
        let mut approval = audit
            .restore(
                &fixture.0.join("backup.db"),
                &fixture.0.join("restored.db"),
                true,
            )
            .unwrap();
        let restored = Database::open(fixture.0.join("restored.db")).unwrap();
        assert!(audit.ensure_open(&restored).is_err());
        assert!(audit.reopen(&restored, &approval).is_err());
        let connection = restored.connect().unwrap();
        let state: (i64, i64, String, bool) = connection
            .query_row(
                "SELECT m.can_read,r.generation,r.status,c.revoked_at IS NOT NULL
                 FROM memberships m,adapter_registrations r,credentials c",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, (0, 10, "revoked".to_owned(), true));
        assert_eq!(
            approval.reconciled_spools,
            vec![
                ("a".to_owned(), "installation-example".to_owned()),
                ("a".to_owned(), "replacement-example".to_owned()),
            ]
        );
        let pending: String = connection
            .query_row(
                "SELECT state FROM delivery_attempts WHERE attempt_id='initial-m1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, "pending");
        let retained: i64 = connection
            .query_row(
                "SELECT count(*) FROM host_custody WHERE attempt_id='initial-m2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained, 1);
        assert!(audit.status().unwrap().last_backup_at.is_some());
        assert!(audit.status().unwrap().last_verified_restore_at.is_none());
        approval.accepted_record_loss = true;
        approval.inventory_complete = true;
        let mut incomplete = approval.clone();
        incomplete.reconciled_spools.clear();
        assert!(audit.reopen(&restored, &incomplete).is_err());
        audit.reopen(&restored, &approval).unwrap();
        audit.ensure_open(&restored).unwrap();
        assert!(audit.status().unwrap().last_verified_restore_at.is_some());
        assert!(audit.ensure_open(&database).is_err());
        connection
            .execute("UPDATE memberships SET can_read=1", [])
            .unwrap();
        assert!(audit.reopen(&restored, &approval).is_err());
    }

    #[test]
    fn failed_restore_keeps_gate_closed() {
        let fixture = Fixture::new();
        let database = fixture.database();
        let audit = fixture.audit(&database);
        std::fs::write(fixture.0.join("truncated.db"), b"not sqlite").unwrap();
        assert!(
            audit
                .restore(
                    &fixture.0.join("truncated.db"),
                    &fixture.0.join("restore.db"),
                    true
                )
                .is_err()
        );
        assert!(audit.ensure_open(&database).is_err());
    }

    #[test]
    fn audit_rollback_is_detected() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        let audit = fixture.audit(&database);
        std::fs::copy(fixture.0.join("audit.db"), fixture.0.join("older.db")).unwrap();
        mutate(&database, &audit, "UPDATE memberships SET can_read=0");
        drop(audit);
        std::fs::copy(fixture.0.join("older.db"), fixture.0.join("audit.db")).unwrap();
        let audit = fixture.audit(&database);
        assert!(audit.ensure_open(&database).is_err());
        assert!(audit.reconcile(&database, true).is_err());
    }

    #[test]
    fn crash_child() {
        let Some(root) = std::env::var_os("JOURNAL_AUDIT_CRASH_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let stage = std::env::var("JOURNAL_AUDIT_CRASH_STAGE").unwrap();
        let database = Database::open(root.join("central.db")).unwrap();
        let audit = RecoveryAudit::open(&database, &root.join("audit.db")).unwrap();
        if stage == "closed" {
            audit.close().unwrap();
        } else if stage == "reconciled" || stage == "reopened" {
            let mut approval = audit.reconcile(&database, true).unwrap();
            if stage == "reopened" {
                approval.inventory_complete = true;
                approval.accepted_record_loss = true;
                audit.reopen(&database, &approval).unwrap();
            }
        } else {
            let mut connection = database.connect().unwrap();
            let transaction = connection.transaction().unwrap();
            transaction
                .execute("UPDATE memberships SET can_read=0", [])
                .unwrap();
            let revision = audit.prepare(&transaction).unwrap();
            if stage == "committed" {
                transaction.commit().unwrap();
            } else {
                std::fs::write(root.join("ready"), b"prepared").unwrap();
                loop {
                    std::thread::park();
                }
            }
            assert!(revision.is_some());
        }
        std::fs::write(root.join("ready"), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn killed_writer_and_recovery_processes_remain_closed_until_reconciled() {
        for stage in ["prepared", "committed", "closed", "reconciled", "reopened"] {
            let fixture = Fixture::new();
            let database = fixture.database();
            seed(&database);
            drop(fixture.audit(&database));
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "recovery_audit::tests::crash_child",
                    "--nocapture",
                ])
                .env("JOURNAL_AUDIT_CRASH_ROOT", &fixture.0)
                .env("JOURNAL_AUDIT_CRASH_STAGE", stage)
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while !fixture.0.join("ready").exists() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited before boundary"
                );
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("child did not reach boundary");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert!(RecoveryAudit::open(&database, &fixture.0.join("audit.db")).is_err());
            child.kill().unwrap();
            child.wait().unwrap();
            let audit = fixture.audit(&database);
            if stage == "reopened" {
                audit.ensure_open(&database).unwrap();
                continue;
            }
            assert!(audit.ensure_open(&database).is_err(), "{stage}");
            let mut approval = audit.reconcile(&database, true).unwrap();
            if stage == "prepared" || stage == "committed" {
                let can_read: bool = database
                    .connect()
                    .unwrap()
                    .query_row(
                        "SELECT can_read FROM memberships WHERE space_id='s' AND principal_id='p'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert!(!can_read, "uncertain intent must not restore a grant");
            }
            approval.accepted_record_loss = true;
            approval.inventory_complete = true;
            audit.reopen(&database, &approval).unwrap();
            audit.ensure_open(&database).unwrap();
        }
    }
}
