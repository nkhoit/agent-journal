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
    "principal_names",
    "spaces",
    "memberships",
    "profile_idempotency_keys",
    "credentials",
    "registration_receipts",
    "credential_audit",
    "audit_events",
];

/// Stored as the audit's `user_version`. Only the head revision retains its
/// snapshot body; older formats require operator archive/reset.
const AUDIT_FORMAT: i64 = 1;

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
    schema_version: i64,
    tables: Vec<Vec<Vec<Cell>>>,
    space_heads: Vec<(String, i64)>,
    inbox_heads: Vec<(String, i64)>,
}

#[derive(Debug)]
pub struct RecoveryAudit {
    path: PathBuf,
    _lock: OwnerLock,
    serial: Mutex<()>,
}

#[derive(Debug)]
pub(crate) struct OwnerLock {
    file: File,
    owner_pid: u32,
    released: bool,
}

impl OwnerLock {
    fn release(&mut self) -> std::io::Result<()> {
        if !self.released {
            // flock is attached to the open file description. A fork child
            // inherits that description, so it must only close its copy; an
            // unlock from the child would release the live parent's lock.
            if std::process::id() == self.owner_pid {
                FileExt::unlock(&self.file)?;
            }
            self.released = true;
        }
        Ok(())
    }
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryApproval {
    pub verification: RecoveryVerification,
    pub audit_revision: i64,
    pub previous_space_heads: Vec<(String, i64)>,
    pub reconciled_clients: Vec<String>,
    pub inventory_complete: bool,
    pub accepted_record_loss: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryStatus {
    pub last_backup_at: Option<String>,
    pub last_verified_restore_at: Option<String>,
}

#[derive(Debug, Clone)]
struct AuditLineage {
    journal_id: String,
    revision: i64,
    snapshot: Snapshot,
}

impl RecoveryAudit {
    /// Admit the external lineage before a recovery lock, writable central
    /// connection, WAL pragma, or SQLite sidecar can be created. `true` means
    /// a validated current audit exists; `false` is limited to the original
    /// unadopted revision-zero anchor, which may initialize its first audit.
    pub(crate) fn preflight_admission(
        database: &Database,
        path: &Path,
    ) -> Result<bool, StorageError> {
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                Self::preflight_existing(database, path)?;
                Ok(true)
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                let central = database.connect_recovery_read_only()?;
                let (revision, required): (i64, bool) = central.query_row(
                    "SELECT revision,audit_required FROM recovery_anchor WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                if required || revision != 0 {
                    return Err(StorageError::RecoveryClosed(
                        "required external audit is missing",
                    ));
                }
                Ok(false)
            }
            Err(source) => Err(io("inspect recovery audit", path, source)),
        }
    }

    pub(crate) fn entry_exists(path: &Path) -> Result<bool, StorageError> {
        match std::fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(io("inspect recovery audit", path, source)),
        }
    }

    /// Parse an existing lineage without taking the sidecar lock or opening a
    /// SQLite writer. Protected startup calls this before any operation that
    /// could create, replace, or unlink recovery evidence.
    pub(crate) fn preflight_existing(
        database: &Database,
        path: &Path,
    ) -> Result<i64, StorageError> {
        let lineage = Self::read_open_lineage(path)?;
        let central = database.connect_recovery_read_only()?;
        Self::verify_lineage_matches(&central, &lineage)?;
        Ok(lineage.revision)
    }

    /// Read-only check that the audit is open with a resolved head, before any
    /// lock or copy is made for crash recovery.
    pub(crate) fn preflight_resolved(path: &Path) -> Result<(), StorageError> {
        Self::read_open_lineage(path).map(drop)
    }

    /// Durable evidence that protected startup replayed hot SQLite state left
    /// by an abrupt stop, after proving it against this audit's lineage.
    pub(crate) fn record_crash_recovery(
        &self,
        verified: &crate::crash_recovery::Verified,
    ) -> Result<(), StorageError> {
        let detail = serde_json::to_string(verified)?;
        self.connection()?.execute(
            "INSERT INTO recovery_events(kind,detail) SELECT 'crash-recovered',?1
             WHERE NOT EXISTS(SELECT 1 FROM recovery_events
                              WHERE kind='crash-recovered' AND detail=?1)",
            [detail],
        )?;
        Ok(())
    }

    fn read_open_lineage(path: &Path) -> Result<AuditLineage, StorageError> {
        validate_file(path)?;
        let audit = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        require_format(&audit)?;
        let (journal_id, state): (String, String) = audit.query_row(
            "SELECT journal_id,state FROM control WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let events = audit
            .prepare("SELECT revision,outcome,snapshot IS NOT NULL FROM events ORDER BY revision")?
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if state != "open" || events.is_empty() {
            return Err(StorageError::RecoveryClosed(
                "audit intent, restore, or reconciliation is unresolved",
            ));
        }
        let last_revision = i64::try_from(events.len() - 1).expect("audit revision fits i64");
        for (expected, (revision, outcome, has_body)) in events.into_iter().enumerate() {
            if revision != i64::try_from(expected).expect("audit revision fits i64")
                || !matches!(outcome.as_str(), "committed" | "reconciled")
            {
                return Err(StorageError::RecoveryClosed(
                    "recovery audit lineage is incomplete or unresolved",
                ));
            }
            if has_body != (revision == last_revision) {
                return Err(StorageError::RecoveryClosed(
                    "recovery audit snapshot retention is inconsistent",
                ));
            }
        }
        Ok(AuditLineage {
            journal_id,
            revision: last_revision,
            snapshot: head_snapshot(&audit)?,
        })
    }

    fn verify_lineage_matches(
        central: &Connection,
        lineage: &AuditLineage,
    ) -> Result<(), StorageError> {
        let (central_journal, revision): (String, i64) = central.query_row(
            "SELECT journal_id,revision FROM recovery_anchor WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if lineage.journal_id != central_journal
            || lineage.revision != revision
            || lineage.snapshot != snapshot(central)?
        {
            return Err(StorageError::RecoveryClosed(
                "recovery audit does not match the central UUID-native lineage",
            ));
        }
        Ok(())
    }

    /// With the external audit lock held, atomically bind a durable matching
    /// revision-zero baseline left by a crashed initializer. Existing adopted
    /// lineages avoid a writer entirely; only the exact unadopted baseline can
    /// flip `audit_required`.
    fn adopt_published_baseline(database: &Database, path: &Path) -> Result<bool, StorageError> {
        let central = database.connect_recovery_read_only()?;
        let (revision, required): (i64, bool) = central.query_row(
            "SELECT revision,audit_required FROM recovery_anchor WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if required {
            return Ok(false);
        }
        if revision != 0 {
            return Err(StorageError::RecoveryClosed(
                "required external audit is missing",
            ));
        }
        drop(central);

        // Re-read while stable audit ownership is held, then verify it again
        // inside the central writer transaction before committing adoption.
        let lineage = Self::read_open_lineage(path)?;
        let mut connection = database.connect_unchecked()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        Self::verify_lineage_matches(&transaction, &lineage)?;
        let changed = transaction.execute(
            "UPDATE recovery_anchor SET audit_required=1
             WHERE singleton=1 AND revision=0 AND audit_required=0",
            [],
        )?;
        if changed != 1 {
            return Err(StorageError::RecoveryClosed(
                "recovery baseline adoption did not match intent",
            ));
        }
        transaction.commit()?;
        drop(connection);
        standalone_current_database(database)?;
        Ok(true)
    }

    fn preflight_recovery_operation(path: &Path) -> Result<(), StorageError> {
        validate_file(path)?;
        let audit = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        require_format(&audit)?;
        let _: (String, String) = audit.query_row(
            "SELECT journal_id,state FROM control WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let _: i64 = audit.query_row(
            "SELECT revision FROM events ORDER BY revision DESC LIMIT 1",
            [],
            |row| row.get(0),
        )?;
        Ok(())
    }

    /// The directory and log are host-private. Never initialize a replacement
    /// audit when the central anchor says an audit already exists.
    pub fn open(database: &Database, path: &Path) -> Result<Self, StorageError> {
        Self::open_with_lock(database, path, Self::acquire_lock(path)?, false)
    }

    pub(crate) fn acquire_lock(path: &Path) -> Result<OwnerLock, StorageError> {
        protect_parent(path)?;
        let lock_path = path.with_extension("recovery-lock");
        let lock = private_file(&lock_path, false)?;
        let owner_pid = std::process::id();
        lock.try_lock_exclusive()
            .map_err(|source| io("lock recovery audit", &lock_path, source))?;
        Ok(OwnerLock {
            file: lock,
            owner_pid,
            released: false,
        })
    }

    pub(crate) fn open_with_lock(
        database: &Database,
        path: &Path,
        lock: OwnerLock,
        protected_startup: bool,
    ) -> Result<Self, StorageError> {
        let audit_exists = Self::entry_exists(path)?;
        if audit_exists {
            if protected_startup {
                // The caller performed an initial lock-free refusal check;
                // repeat the full read-only lineage validation while holding
                // stable audit ownership before any adoption write.
                Self::preflight_existing(database, path)?;
                Self::adopt_published_baseline(database, path)?;
            } else {
                Self::preflight_recovery_operation(path)?;
            }
            return Ok(Self {
                path: path.to_owned(),
                _lock: lock,
                serial: Mutex::new(()),
            });
        }

        let central = database.connect_recovery_read_only()?;
        let (revision, required): (i64, bool) = central.query_row(
            "SELECT revision,audit_required FROM recovery_anchor WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if required || revision != 0 {
            return Err(StorageError::RecoveryClosed(
                "required external audit is missing",
            ));
        }
        drop(central);

        // Only a fresh, unadopted revision-zero central baseline may create an
        // audit. Existing required/malformed/foreign/closed audits returned
        // above without touching central SQLite or recovery artifacts.
        let mut central_connection = database.connect_unchecked()?;
        let central = central_connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (journal_id, revision, required): (String, i64, bool) = central.query_row(
            "SELECT journal_id,revision,audit_required FROM recovery_anchor WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if required || revision != 0 {
            return Err(StorageError::RecoveryClosed(
                "required external audit is missing",
            ));
        }
        // Only an unadopted revision-zero anchor may discard interrupted
        // initialization. The final path never contains a partial baseline.
        let mut staging_name = path.as_os_str().to_owned();
        staging_name.push(".initializing");
        let staging = PathBuf::from(staging_name);
        let mut journal_name = staging.as_os_str().to_owned();
        journal_name.push("-journal");
        for leftover in [&PathBuf::from(journal_name), &staging] {
            if leftover
                .try_exists()
                .map_err(|source| io("inspect incomplete recovery audit", leftover, source))?
            {
                validate_file(leftover)?;
                if std::fs::canonicalize(leftover)
                    .map_err(|source| io("resolve incomplete audit", leftover, source))?
                    == std::fs::canonicalize(database.path())
                        .map_err(|source| io("resolve central database", database.path(), source))?
                {
                    return Err(StorageError::RecoveryClosed(
                        "audit staging and central database must differ",
                    ));
                }
                std::fs::remove_file(leftover)
                    .map_err(|source| io("remove incomplete recovery audit", leftover, source))?;
            }
        }
        let file = private_file(&staging, true)?;
        #[cfg(test)]
        initialization_boundary("created");
        let audit_connection = audit_connection(&staging)?;
        audit_connection.execute_batch(&format!(
            "PRAGMA user_version={AUDIT_FORMAT};
             CREATE TABLE control(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
               journal_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('open','closed')));
             CREATE TABLE events(revision INTEGER PRIMARY KEY, snapshot TEXT,
               outcome TEXT NOT NULL CHECK(outcome IN ('prepared','committed','reconciled')),
               occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
             CREATE TABLE recovery_events(id INTEGER PRIMARY KEY, kind TEXT NOT NULL,
               detail TEXT NOT NULL,
               occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));"
        ))?;
        #[cfg(test)]
        initialization_boundary("schema");
        let snapshot = serde_json::to_string(&snapshot(&central)?)?;
        audit_connection.execute(
            "INSERT INTO events(revision,snapshot,outcome) VALUES (0,?,'committed')",
            [&snapshot],
        )?;
        audit_connection.execute("INSERT INTO control VALUES(1,?,'open')", [&journal_id])?;
        drop(audit_connection);
        file.sync_all()
            .map_err(|source| io("sync recovery audit", &staging, source))?;
        drop(file);
        #[cfg(test)]
        initialization_boundary("baseline");
        std::fs::rename(&staging, path)
            .map_err(|source| io("publish recovery audit", path, source))?;
        sync_parent(path)?;
        #[cfg(test)]
        initialization_boundary("published");

        let audit = Self {
            path: path.to_owned(),
            _lock: lock,
            serial: Mutex::new(()),
        };
        central.execute(
            "UPDATE recovery_anchor SET audit_required=1 WHERE singleton=1",
            [],
        )?;
        central.commit()?;
        drop(central_connection);
        standalone_current_database(database)?;
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
        let (revision, outcome) = head(&audit)?;
        let anchor: i64 = database
            .connection_factory()
            .connect_read_only()?
            .query_row(
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
        let (revision, outcome) = head(&audit)?;
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
            let mut audit = self.connection()?;
            let transaction = audit.transaction()?;
            let changed = transaction.execute(
                "UPDATE events SET outcome='committed' WHERE revision=? AND outcome='prepared'",
                [revision],
            )?;
            if changed != 1 {
                return Err(StorageError::RecoveryClosed(
                    "audit completion did not match intent",
                ));
            }
            release_superseded_snapshots(&transaction, revision)?;
            transaction.commit()?;
        }
        Ok(())
    }

    /// Offline only: the exclusive lifetime lock excludes the running daemon.
    /// Uncertain intents remain evidence, never authority to reopen public access.
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
        database.backup_to_unguarded(destination)?;
        sync_parent(destination)?;
        let backup = Database::open(destination)?;
        let verification = backup.recovery_verification()?;
        standalone_current_database(&backup)?;
        drop(backup);
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
        clients_quiesced: bool,
    ) -> Result<RecoveryApproval, StorageError> {
        let _guard = self.lock()?;
        self.require_resolved_recovery_input()?;
        self.close()?;
        if !clients_quiesced {
            return Err(StorageError::RecoveryClosed(
                "client quiescence was not attested",
            ));
        }
        protect_parent(destination)?;
        Database::verify_backup(backup)?;
        let backup_database = Database::open(backup)?;
        let expected = backup_database.recovery_verification()?;
        let latest = head_snapshot(&self.connection()?)?;
        validate_credential_history(&backup_database.connect_read_only()?, &latest)?;
        standalone_current_database(&backup_database)?;
        drop(backup_database);
        Database::restore_backup(backup, destination)?;
        sync_parent(destination)?;
        let restored = Database::open(destination)?;
        if restored.recovery_verification()? != expected {
            return Err(StorageError::RecoveryClosed(
                "restored hashes differ from backup",
            ));
        }
        self.reconcile(&restored, clients_quiesced)
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

    fn require_resolved_recovery_input(&self) -> Result<(), StorageError> {
        if head(&self.connection()?)?.1 == "prepared" {
            return Err(StorageError::RecoveryClosed(
                "archive/reset required: uncertain audit intent cannot be reconciled; preserve the database and external audit; a completed reconciliation may only reopen with its matching approval",
            ));
        }
        Ok(())
    }

    /// Reconcile only a resolved input snapshot; uncertain intents require reset.
    pub fn reconcile(
        &self,
        database: &Database,
        clients_quiesced: bool,
    ) -> Result<RecoveryApproval, StorageError> {
        self.require_resolved_recovery_input()?;
        self.close()?;
        if !clients_quiesced {
            return Err(StorageError::RecoveryClosed(
                "client quiescence was not attested",
            ));
        }
        let audit = self.connection()?;
        let (revision, _) = head(&audit)?;
        let latest = head_snapshot(&audit)?;
        let mut connection = database.connect_unchecked()?;
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
        let invalid_bindings: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )?;
        if invalid_bindings {
            return Err(StorageError::RecoveryClosed(
                "restored identity bindings violate foreign keys",
            ));
        }
        let revision = revision
            .checked_add(1)
            .ok_or(StorageError::RecoveryClosed("audit revision exhausted"))?;
        audit.execute(
            "INSERT INTO events(revision,snapshot,outcome) VALUES (?,?,'prepared')",
            params![revision, serde_json::to_string(&snapshot(&transaction)?)?],
        )?;
        transaction.execute(
            "UPDATE recovery_anchor SET revision=?1,audit_required=1,inbox_epoch=?1",
            [revision],
        )?;
        transaction.commit()?;
        let verification = database.recovery_verification_unguarded()?;
        let clients = identifiers(&connection, "SELECT id FROM principals ORDER BY id")?;
        let approval = RecoveryApproval {
            verification,
            audit_revision: revision,
            previous_space_heads: latest.space_heads,
            reconciled_clients: clients,
            inventory_complete: false,
            accepted_record_loss: false,
        };
        audit.execute(
            "INSERT INTO recovery_events(kind,detail) VALUES('reconciled',?)",
            [serde_json::to_string(&approval)?],
        )?;
        // A restored central path is handed back as a standalone current-schema
        // database. Checkpoint the audited reconciliation before dropping its
        // final writer so a later cold admission never has to interpret a WAL.
        connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE")?;
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

    /// The approval is an operator attestation, not automated client reconciliation.
    /// Its complete inventory and exact post-recovery hashes bind that attestation.
    pub fn reopen(
        &self,
        database: &Database,
        approval: &RecoveryApproval,
    ) -> Result<(), StorageError> {
        let _guard = self.lock()?;
        let mut audit = self.connection()?;
        let (revision, _) = head(&audit)?;
        let connection = database.connect_unchecked()?;
        let clients = identifiers(&connection, "SELECT id FROM principals ORDER BY id")?;
        let anchor: i64 =
            connection.query_row("SELECT revision FROM recovery_anchor", [], |row| row.get(0))?;
        let reconciled: Option<String> = audit.query_row(
            "SELECT detail FROM recovery_events WHERE kind='reconciled' ORDER BY id DESC LIMIT 1",
            [], |row| row.get(0),
        ).optional()?;
        let verification = database.recovery_verification_unguarded()?;
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
            || approval.reconciled_clients != clients
        {
            return Err(StorageError::RecoveryClosed(
                "recovery approval does not match probed state and inventory",
            ));
        }
        let live: i64 = connection.query_row(
            "SELECT count(*) FROM credentials WHERE revoked_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        if live != 0 {
            return Err(StorageError::RecoveryClosed(
                "restored credentials are not revoked",
            ));
        }
        let transaction = audit.transaction()?;
        transaction.execute(
            "UPDATE events SET outcome='reconciled' WHERE revision=?",
            [revision],
        )?;
        release_superseded_snapshots(&transaction, revision)?;
        transaction.execute("UPDATE control SET state='open' WHERE singleton=1", [])?;
        transaction.execute(
            "INSERT INTO recovery_events(kind,detail) VALUES('verified-restore',?)",
            [serde_json::to_string(approval)?],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

fn head(connection: &Connection) -> Result<(i64, String), StorageError> {
    Ok(connection.query_row(
        "SELECT revision,outcome FROM events ORDER BY revision DESC LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}

fn head_snapshot(connection: &Connection) -> Result<Snapshot, StorageError> {
    let body: Option<String> = connection.query_row(
        "SELECT snapshot FROM events ORDER BY revision DESC LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    let body = body.ok_or(StorageError::RecoveryClosed(
        "recovery audit head snapshot is missing",
    ))?;
    Ok(serde_json::from_str(&body)?)
}

/// Once a revision resolves, earlier snapshots are no longer authoritative.
/// Unresolved intents never reach this point, so their predecessor survives.
fn release_superseded_snapshots(
    transaction: &Transaction<'_>,
    revision: i64,
) -> Result<(), StorageError> {
    transaction.execute(
        "UPDATE events SET snapshot=NULL WHERE revision<? AND snapshot IS NOT NULL",
        [revision],
    )?;
    Ok(())
}

fn require_format(connection: &Connection) -> Result<(), StorageError> {
    let format: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if format != AUDIT_FORMAT {
        return Err(StorageError::RecoveryClosed(
            "unsupported recovery audit format; archive/reset required",
        ));
    }
    Ok(())
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
        schema_version: crate::CURRENT_SCHEMA_VERSION,
        tables,
        space_heads,
        inbox_heads: connection.prepare("SELECT recipient_principal_id,last_seq FROM inbox_sequences ORDER BY recipient_principal_id")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>,_>>()?,
    })
}

fn restore_security(transaction: &Transaction<'_>, latest: &Snapshot) -> Result<(), StorageError> {
    if latest.schema_version != crate::CURRENT_SCHEMA_VERSION
        || latest.tables.len() != SECURITY_TABLES.len()
    {
        return Err(StorageError::RecoveryClosed(
            "security snapshot shape differs",
        ));
    }
    validate_credential_history(transaction, latest)?;
    // Restoring identity bindings never restores active credentials.
    transaction.execute_batch(
        "UPDATE credentials SET revoked_at=coalesce(revoked_at,strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          revocation_reason='central restore';
         DELETE FROM memberships;",
    )?;
    for table in [
        "principals",
        "principal_names",
        "spaces",
        "memberships",
        "profile_idempotency_keys",
    ] {
        let rows = security_rows(latest, table)?;
        if table == "principal_names" {
            restore_principal_names(transaction, rows)?;
            continue;
        }
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
            .map(|(name, _)| format!("{name}=excluded.{name}"))
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
    restore_credential_history(transaction, latest)?;
    for (recipient, head) in &latest.inbox_heads {
        if *head <= 0 {
            return Err(StorageError::RecoveryClosed(
                "invalid inbox allocation head",
            ));
        }
        transaction.execute(
            "INSERT INTO inbox_sequences(recipient_principal_id,last_seq) VALUES (?,?)
             ON CONFLICT(recipient_principal_id) DO UPDATE SET last_seq=max(last_seq,excluded.last_seq)",
            params![recipient,head],
        )?;
    }
    Ok(())
}

fn security_rows<'a>(snapshot: &'a Snapshot, table: &str) -> Result<&'a [Vec<Cell>], StorageError> {
    if snapshot.schema_version != crate::CURRENT_SCHEMA_VERSION
        || snapshot.tables.len() != SECURITY_TABLES.len()
    {
        return Err(StorageError::RecoveryClosed(
            "security snapshot shape differs",
        ));
    }
    SECURITY_TABLES
        .iter()
        .position(|name| *name == table)
        .and_then(|index| snapshot.tables.get(index))
        .map(Vec::as_slice)
        .ok_or(StorageError::RecoveryClosed(
            "security snapshot table is missing",
        ))
}

fn validate_credential_history(
    connection: &Connection,
    latest: &Snapshot,
) -> Result<(), StorageError> {
    for (table, columns, immutable) in [("credentials", 9, 6), ("registration_receipts", 6, 6)] {
        let audited = security_rows(latest, table)?;
        if audited
            .iter()
            .any(|row| row.len() != columns || !matches!(row.first(), Some(Cell::Text(_))))
        {
            return Err(StorageError::RecoveryClosed(
                "credential snapshot shape differs",
            ));
        }
        let mut statement = connection.prepare(&format!("SELECT * FROM {table}"))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let existing = (0..columns)
                .map(|index| {
                    Ok(match row.get::<_, Value>(index)? {
                        Value::Null => Cell::Null,
                        Value::Integer(value) => Cell::Integer(value),
                        Value::Text(value) => Cell::Text(value),
                        _ => return Err(rusqlite::Error::InvalidQuery),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let matching = audited.iter().find(|candidate| candidate[0] == existing[0]);
            if !matching.is_some_and(|candidate| candidate[..immutable] == existing[..immutable]) {
                return Err(StorageError::RecoveryClosed(
                    "credential or registration binding conflicts",
                ));
            }
        }
        // A digest or principal cannot acquire a second binding under another key.
        for (index, row) in audited.iter().enumerate() {
            if audited[..index].iter().any(|prior| {
                prior[0] == row[0]
                    || if table == "credentials" {
                        prior[3] == row[3]
                    } else {
                        prior[1] == row[1] || prior[2] == row[2]
                    }
            }) {
                return Err(StorageError::RecoveryClosed(
                    "credential snapshot contains conflicting bindings",
                ));
            }
        }
    }
    Ok(())
}

fn restore_credential_history(
    transaction: &Transaction<'_>,
    latest: &Snapshot,
) -> Result<(), StorageError> {
    // Rotation successors need not sort after predecessors in snapshot order.
    transaction.execute_batch("PRAGMA defer_foreign_keys=ON;")?;
    for row in security_rows(latest, "credentials")? {
        let values = row.iter().map(Cell::value).collect::<Vec<_>>();
        transaction.execute(
            "INSERT INTO credentials(
                id,principal_id,class,token_hash,created_at,expires_at,revoked_at,
                replacement_credential_id,revocation_reason)
             VALUES (?1,?2,?3,?4,?5,?6,coalesce(?7,strftime('%Y-%m-%dT%H:%M:%fZ','now')),?8,
                CASE WHEN ?7 IS NULL THEN 'central restore' ELSE ?9 END)
             ON CONFLICT(id) DO UPDATE SET revoked_at=excluded.revoked_at,
                replacement_credential_id=excluded.replacement_credential_id,
                revocation_reason=excluded.revocation_reason",
            rusqlite::params_from_iter(values),
        )?;
    }
    for row in security_rows(latest, "registration_receipts")? {
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM registration_receipts WHERE token_hash=?)",
            [row[0].value()],
            |r| r.get(0),
        )?;
        if !exists {
            transaction.execute(
                "INSERT INTO registration_receipts(
                    token_hash,credential_id,principal_id,request_json,response_json,created_at)
                 VALUES (?,?,?,?,?,?)",
                rusqlite::params_from_iter(row.iter().map(Cell::value)),
            )?;
        }
    }
    Ok(())
}

fn restore_principal_names(
    transaction: &Transaction<'_>,
    rows: &[Vec<Cell>],
) -> Result<(), StorageError> {
    for row in rows {
        let [
            Cell::Text(name),
            Cell::Text(principal_id),
            Cell::Text(kind),
            Cell::Text(created_at),
        ] = row.as_slice()
        else {
            return Err(StorageError::RecoveryClosed(
                "principal-name snapshot row differs",
            ));
        };
        if !matches!(kind.as_str(), "current" | "alias") {
            return Err(StorageError::RecoveryClosed(
                "principal-name snapshot kind differs",
            ));
        }
        if kind == "current" {
            transaction.execute(
                "UPDATE principal_names SET kind='alias'
                 WHERE principal_id=? AND kind='current' AND name<>?",
                params![principal_id, name],
            )?;
        }
        let existing: Option<(String, String, String)> = transaction
            .query_row(
                "SELECT principal_id,kind,created_at FROM principal_names WHERE name=?",
                [name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        match existing {
            Some((stored_principal, stored_kind, stored_created))
                if stored_principal == *principal_id
                    && stored_kind == *kind
                    && stored_created == *created_at => {}
            Some((stored_principal, stored_kind, stored_created))
                if stored_principal == *principal_id
                    && stored_kind == "current"
                    && kind == "alias"
                    && stored_created == *created_at =>
            {
                transaction.execute(
                    "UPDATE principal_names SET kind='alias' WHERE name=?",
                    [name],
                )?;
            }
            Some(_) => {
                return Err(StorageError::RecoveryClosed(
                    "principal-name lineage conflicts",
                ));
            }
            None => {
                transaction.execute(
                    "INSERT INTO principal_names(name,principal_id,kind,created_at) VALUES(?,?,?,?)",
                    params![name, principal_id, kind, created_at],
                )?;
            }
        }
    }
    Ok(())
}

fn identifiers(connection: &Connection, sql: &str) -> Result<Vec<String>, StorageError> {
    Ok(connection
        .prepare(sql)?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?)
}

fn audit_connection(path: &Path) -> Result<Connection, StorageError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    connection.busy_timeout(crate::BUSY_TIMEOUT)?;
    connection.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA;")?;
    Ok(connection)
}

/// A protected backup/restore path is freshly reserved or already admitted as
/// current. Normalize only our own completed verification WAL before returning
/// the path to cold current-schema admission.
fn standalone_current_database(database: &Database) -> Result<(), StorageError> {
    let connection = database.connect_unchecked()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
    drop(connection);
    sync_parent(database.path())
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
fn initialization_boundary(stage: &str) {
    if std::env::var("JOURNAL_AUDIT_INIT_STAGE").as_deref() == Ok(stage) {
        let root = PathBuf::from(std::env::var_os("JOURNAL_AUDIT_INIT_ROOT").unwrap());
        std::fs::write(root.join("ready"), stage).unwrap();
        loop {
            std::thread::park();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::current_dir().unwrap().join(format!(
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
        database.connect_unchecked().unwrap().execute_batch(
                "INSERT INTO principals(id,display_name,description,profile_revision,created_at,disabled_at)
                     VALUES('018f1f59-6e90-7000-8000-000000000001','Principal',NULL,1,'2026-01-01T00:00:00Z',NULL);
                 INSERT INTO principal_names(name,principal_id,kind,created_at)
                     VALUES('principal','018f1f59-6e90-7000-8000-000000000001','current','2026-01-01T00:00:00Z');
                 INSERT INTO spaces VALUES ('s','Space','public','2026-01-01T00:00:00Z',NULL);
                 INSERT INTO memberships VALUES('s','018f1f59-6e90-7000-8000-000000000001',1,1,0,'2026-01-01T00:00:00Z');
                 INSERT INTO credentials(id,principal_id,class,token_hash,created_at)
                     VALUES('c','018f1f59-6e90-7000-8000-000000000001','principal-client','fixture-digest','2026-01-01T00:00:00Z');
                 INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
                     VALUES('r1','s',1,'018f1f59-6e90-7000-8000-000000000001','note','pending recovery','2026-01-01T00:00:00Z'),
                           ('r2','s',2,'018f1f59-6e90-7000-8000-000000000001','note','retained receipt','2026-01-01T00:00:00Z');
                 INSERT INTO attention VALUES('r1','018f1f59-6e90-7000-8000-000000000001','2026-01-01T00:00:00Z'),('r2','018f1f59-6e90-7000-8000-000000000001','2026-01-01T00:00:00Z');
                 INSERT INTO inbox_sequences VALUES ('018f1f59-6e90-7000-8000-000000000001',2);
                 INSERT INTO mailbox_items VALUES('m1','r1','018f1f59-6e90-7000-8000-000000000001','2026-01-01T00:00:00Z',1,NULL),
                     ('m2','r2','018f1f59-6e90-7000-8000-000000000001','2026-01-01T00:00:00Z',2,NULL);"
            ).unwrap();
    }

    fn mutate(database: &Database, audit: &RecoveryAudit, sql: &str) {
        let _guard = audit.lock().unwrap();
        audit.ensure_open(database).unwrap();
        let mut connection = database.connect_unchecked().unwrap();
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
    fn initialization_crash_child() {
        let Some(root) = std::env::var_os("JOURNAL_AUDIT_INIT_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let database = Database::open(root.join("central.db")).unwrap();
        let _audit = RecoveryAudit::open(&database, &root.join("audit.db")).unwrap();
        panic!("initialization boundary was not reached");
    }

    #[test]
    fn killed_initialization_resumes_startup_and_recovery() {
        for stage in ["created", "schema", "baseline", "published"] {
            let fixture = Fixture::new();
            let database = fixture.database();
            seed(&database);
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "recovery_audit::tests::initialization_crash_child",
                    "--nocapture",
                ])
                .env("JOURNAL_AUDIT_INIT_ROOT", &fixture.0)
                .env("JOURNAL_AUDIT_INIT_STAGE", stage)
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while !fixture.0.join("ready").exists() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited before {stage}"
                );
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("child did not reach {stage}");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert_eq!(fixture.0.join("audit.db").exists(), stage == "published");
            child.kill().unwrap();
            child.wait().unwrap();
            let published =
                (stage == "published").then(|| std::fs::read(fixture.0.join("audit.db")).unwrap());
            let anchor: (i64, bool) = database
                .connect_unchecked()
                .unwrap()
                .query_row(
                    "SELECT revision,audit_required FROM recovery_anchor",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(anchor, (0, false), "{stage}");
            let audit = fixture.audit(&database);
            audit.ensure_open(&database).unwrap();
            if let Some(published) = published {
                assert_eq!(
                    std::fs::read(fixture.0.join("audit.db")).unwrap(),
                    published,
                    "completed publication must be reused"
                );
            }
            let (revision, outcome) = head(&audit.connection().unwrap()).unwrap();
            assert_eq!(revision, 0);
            assert_eq!(outcome, "committed");
            assert_eq!(
                head_snapshot(&audit.connection().unwrap()).unwrap(),
                snapshot(&database.connect_unchecked().unwrap()).unwrap(),
                "{stage}"
            );
            assert!(!fixture.0.join("audit.db.initializing").exists());
            drop(audit);
            let audit = fixture.audit(&database);
            audit.ensure_open(&database).unwrap();
            let mut approval = audit.reconcile(&database, true).unwrap();
            approval.inventory_complete = true;
            approval.accepted_record_loss = true;
            audit.reopen(&database, &approval).unwrap();
            audit.ensure_open(&database).unwrap();
            drop(audit);
            std::fs::remove_file(fixture.0.join("audit.db")).unwrap();
            assert!(RecoveryAudit::open(&database, &fixture.0.join("audit.db")).is_err());
            assert!(!fixture.0.join("audit.db").exists());
        }
    }

    /// A durable audit may outlive the initializer that published it. The next
    /// protected owner must adopt that exact revision-zero baseline rather than
    /// treating the central store as still eligible for ordinary writers.
    #[test]
    fn published_baseline_crash_is_adopted_by_protected_restart() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "recovery_audit::tests::initialization_crash_child",
                "--nocapture",
            ])
            .env("JOURNAL_AUDIT_INIT_ROOT", &fixture.0)
            .env("JOURNAL_AUDIT_INIT_STAGE", "published")
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !fixture.0.join("ready").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before published audit boundary"
            );
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("child did not reach published audit boundary");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        child.kill().unwrap();
        child.wait().unwrap();

        let central = fixture.0.join("central.db");
        let audit_path = fixture.0.join("audit.db");
        let audit_before = std::fs::read(&audit_path).unwrap();
        // This owner survived the initializer crash and deliberately makes the
        // current schema standalone before handing it to cold protected startup.
        database.normalize_for_clean_shutdown().unwrap();
        drop(database);

        let protected = Database::open_protected(&central, &audit_path).unwrap();
        let anchor: (i64, bool) = protected
            .connect_unchecked()
            .unwrap()
            .query_row(
                "SELECT revision,audit_required FROM recovery_anchor WHERE singleton=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(anchor, (0, true));
        assert_eq!(std::fs::read(&audit_path).unwrap(), audit_before);

        let ordinary = Database::open(&central).unwrap();
        assert!(matches!(
            ordinary.connect(),
            Err(StorageError::RecoveryClosed(
                "protected writes require the audited transaction wrapper"
            ))
        ));
        assert!(matches!(
            ordinary.with_transaction(|transaction| {
                transaction.execute(
                    "INSERT INTO principals(id,display_name,created_at) VALUES ('018f1f59-6e90-7000-8000-000000000003','Unaudited','2026-01-01T00:00:00Z')",
                    [],
                )?;
                Ok::<_, StorageError>(())
            }),
            Err(StorageError::RecoveryClosed(
                "protected writes require the audited transaction wrapper"
            ))
        ));
        protected
            .recovery_audit()
            .unwrap()
            .ensure_open(&protected)
            .unwrap();
    }

    #[test]
    fn initialization_never_discards_required_or_nonzero_audit() {
        for (revision, required) in [(0, true), (1, false)] {
            let fixture = Fixture::new();
            let database = fixture.database();
            database
                .connect_unchecked()
                .unwrap()
                .execute(
                    "UPDATE recovery_anchor SET revision=?,audit_required=?",
                    params![revision, required],
                )
                .unwrap();
            let staging = fixture.0.join("audit.db.initializing");
            private_file(&staging, true).unwrap();
            std::fs::write(&staging, b"incomplete").unwrap();
            assert!(RecoveryAudit::open(&database, &fixture.0.join("audit.db")).is_err());
            assert_eq!(std::fs::read(&staging).unwrap(), b"incomplete");
            assert!(!fixture.0.join("audit.db").exists());
        }
        let fixture = Fixture::new();
        let database = fixture.database();
        drop(fixture.audit(&database));
        let path = fixture.0.join("audit.db");
        std::fs::write(&path, b"damaged adopted audit").unwrap();
        assert!(RecoveryAudit::open(&database, &path).is_err());
        assert_eq!(std::fs::read(path).unwrap(), b"damaged adopted audit");
    }

    #[test]
    fn initialization_staging_cannot_replace_the_central_database() {
        let fixture = Fixture::new();
        let path = fixture.0.join("audit.db.initializing");
        let database = Database::open(&path).unwrap();
        seed(&database);
        assert!(RecoveryAudit::open(&database, &fixture.0.join("audit.db")).is_err());
        assert_eq!(
            snapshot(&database.connect_unchecked().unwrap())
                .unwrap()
                .space_heads,
            vec![("s".to_owned(), 2)]
        );
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
    fn older_backup_loses_new_acks_but_preserves_inbox_allocation_high_water() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        let audit = fixture.audit(&database);
        mutate(
            &database,
            &audit,
            "UPDATE mailbox_items SET acknowledged_at='2026-01-01T00:00:00Z' WHERE id='m2'",
        );
        audit
            .backup(&database, &fixture.0.join("backup.db"))
            .unwrap();
        mutate(
            &database,
            &audit,
            "UPDATE mailbox_items SET acknowledged_at='2026-01-02T00:00:00Z' WHERE id='m1'; UPDATE inbox_sequences SET last_seq=7",
        );
        let snapshot_before = snapshot(&database.connect_unchecked().unwrap()).unwrap();
        assert_eq!(snapshot_before.inbox_heads[0].1, 7);
        let serialized = serde_json::to_string(&snapshot_before).unwrap();
        assert!(!serialized.contains("acknowledged_at"));
        assert!(!serialized.contains("\"m1\""));
        let mut approval = audit
            .restore(
                &fixture.0.join("backup.db"),
                &fixture.0.join("restored.db"),
                true,
            )
            .unwrap();
        let restored = Database::open(fixture.0.join("restored.db")).unwrap();
        let connection = restored.connect_unchecked().unwrap();
        let rows = connection
            .prepare("SELECT id,acknowledged_at FROM mailbox_items ORDER BY id")
            .unwrap()
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("m1".into(), None),
                ("m2".into(), Some("2026-01-01T00:00:00Z".into()))
            ]
        );
        assert_eq!(
            connection
                .query_row("SELECT last_seq FROM inbox_sequences", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            7
        );
        assert_eq!(
            connection
                .query_row("SELECT inbox_epoch FROM recovery_anchor", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            approval.audit_revision
        );
        assert!(audit.reopen(&restored, &approval).is_err());
        approval.accepted_record_loss = true;
        approval.inventory_complete = true;
        audit.reopen(&restored, &approval).unwrap();
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
                 UPDATE principals SET disabled_at='2026-01-02T00:00:00Z';",
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
        let connection = restored.connect_unchecked().unwrap();
        let state: (i64, bool, bool) = connection
            .query_row(
                "SELECT m.can_read,p.disabled_at IS NOT NULL,c.revoked_at IS NOT NULL
                 FROM memberships m,principals p,credentials c",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, (0, true, true));
        assert_eq!(
            approval.reconciled_clients,
            vec!["018f1f59-6e90-7000-8000-000000000001".to_owned()]
        );
        let pending: bool = connection
            .query_row(
                "SELECT acknowledged_at IS NULL FROM mailbox_items WHERE id='m1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pending);
        let retained: i64 = connection
            .query_row(
                "SELECT count(*) FROM mailbox_items WHERE id='m2'",
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
        incomplete.reconciled_clients.clear();
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

    fn audit_rows(audit: &RecoveryAudit) -> Vec<(i64, Option<String>, String)> {
        audit
            .connection()
            .unwrap()
            .prepare("SELECT revision,snapshot,outcome FROM events ORDER BY revision")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn retained_bodies(audit: &RecoveryAudit) -> Vec<i64> {
        audit
            .connection()
            .unwrap()
            .prepare("SELECT revision FROM events WHERE snapshot IS NOT NULL ORDER BY revision")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn protected_admission(fixture: &Fixture, database: Database) -> Result<(), StorageError> {
        database.normalize_for_clean_shutdown().unwrap();
        drop(database);
        Database::open_protected(fixture.0.join("central.db"), fixture.0.join("audit.db")).map(drop)
    }

    #[test]
    fn committed_mutations_retain_only_the_head_snapshot() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        let audit = fixture.audit(&database);
        for revision in 1..=5 {
            mutate(
                &database,
                &audit,
                &format!("UPDATE principals SET display_name='Principal {revision}'"),
            );
            assert_eq!(retained_bodies(&audit), vec![revision]);
        }
        drop(audit);
        protected_admission(&fixture, database).unwrap();
    }

    #[test]
    fn record_relations_do_not_grow_the_security_snapshot() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        let before = snapshot(&database.connect_unchecked().unwrap()).unwrap();
        database
            .connect_unchecked()
            .unwrap()
            .execute_batch(
                "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
                     VALUES('r3','s',3,'018f1f59-6e90-7000-8000-000000000001','note','reply','2026-01-01T00:00:00Z');
                 INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at)
                     VALUES('r3','reply-to','r1','2026-01-01T00:00:00Z');",
            )
            .unwrap();
        let after = snapshot(&database.connect_unchecked().unwrap()).unwrap();
        assert_eq!(after.tables, before.tables);
        assert_eq!(after.space_heads, vec![("s".to_owned(), 3)]);
    }

    #[test]
    fn unsupported_audit_format_is_refused_without_mutation() {
        let fixture = Fixture::new();
        let database = fixture.database();
        seed(&database);
        drop(fixture.audit(&database));
        let path = fixture.0.join("audit.db");
        Connection::open(&path)
            .unwrap()
            .execute_batch("PRAGMA user_version=0")
            .unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(RecoveryAudit::open(&database, &path).is_err());
        let error = protected_admission(&fixture, database).unwrap_err();
        assert!(
            error.to_string().contains("archive/reset required"),
            "{error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn misplaced_snapshot_bodies_are_refused_at_admission() {
        for tamper in [
            "UPDATE events SET snapshot=(SELECT snapshot FROM events WHERE revision=1) WHERE revision=0",
            "UPDATE events SET snapshot=NULL WHERE revision=1",
        ] {
            let fixture = Fixture::new();
            let database = fixture.database();
            seed(&database);
            let audit = fixture.audit(&database);
            mutate(&database, &audit, "UPDATE memberships SET can_read=0");
            drop(audit);
            Connection::open(fixture.0.join("audit.db"))
                .unwrap()
                .execute(tamper, [])
                .unwrap();
            assert!(protected_admission(&fixture, database).is_err(), "{tamper}");
        }
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
            let mut connection = database.connect_unchecked().unwrap();
            let transaction = connection.transaction().unwrap();
            transaction
                .execute("UPDATE memberships SET can_read=0", [])
                .unwrap();
            transaction
                .execute(
                    "UPDATE mailbox_items SET acknowledged_at='2026-01-02T00:00:00Z' WHERE id='m1'",
                    [],
                )
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
            if stage == "prepared" || stage == "committed" {
                let before = snapshot(&database.connect_unchecked().unwrap()).unwrap();
                let evidence = head(&audit.connection().unwrap()).unwrap();
                // An unresolved intent keeps its predecessor as evidence.
                assert_eq!(retained_bodies(&audit), vec![evidence.0 - 1, evidence.0]);
                let rows = audit_rows(&audit);
                for _ in 0..2 {
                    let error = audit.reconcile(&database, true).unwrap_err();
                    assert!(error.to_string().contains("archive/reset required"));
                    assert!(audit.ensure_open(&database).is_err());
                    assert_eq!(audit_rows(&audit), rows);
                    assert_eq!(
                        serde_json::to_string(
                            &snapshot(&database.connect_unchecked().unwrap()).unwrap()
                        )
                        .unwrap(),
                        serde_json::to_string(&before).unwrap()
                    );
                }
                let connection = database.connect_unchecked().unwrap();
                let forged = RecoveryApproval {
                    verification: database.recovery_verification_unguarded().unwrap(),
                    audit_revision: evidence.0,
                    previous_space_heads: before.space_heads,
                    reconciled_clients: identifiers(
                        &connection,
                        "SELECT id FROM principals ORDER BY id",
                    )
                    .unwrap(),
                    inventory_complete: true,
                    accepted_record_loss: true,
                };
                assert!(audit.reopen(&database, &forged).is_err());
                let destination = fixture.0.join("rejected-restore.db");
                let error = audit
                    .restore(&fixture.0.join("central.db"), &destination, true)
                    .unwrap_err();
                assert!(error.to_string().contains("archive/reset required"));
                assert!(!destination.exists());
                assert_eq!(audit_rows(&audit), rows);
                assert!(audit.ensure_open(&database).is_err());
                continue;
            }
            let mut approval = if stage == "reconciled" {
                let serialized: String = audit.connection().unwrap().query_row(
                    "SELECT detail FROM recovery_events WHERE kind='reconciled' ORDER BY id DESC LIMIT 1",
                    [], |row| row.get(0),
                ).unwrap();
                serde_json::from_str::<RecoveryApproval>(&serialized).unwrap()
            } else {
                audit.reconcile(&database, true).unwrap()
            };
            approval.accepted_record_loss = true;
            approval.inventory_complete = true;
            audit.reopen(&database, &approval).unwrap();
            audit.ensure_open(&database).unwrap();
        }
    }
}
