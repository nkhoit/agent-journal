//! Single-owner, synchronous SQLite custody spool. Runtime orchestration is separate.

use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::SystemTime,
};

use fs2::FileExt;
use journal_adapter_core::{
    AdapterSpool, Backoff, CoreError, CoreResult, CustodyResult, CustodyResultState, EventRequest,
    InjectionState, Spool, SpoolItem,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

pub const MAX_RECOVERY_BATCH: usize = 100;
const MAX_DETAIL_BYTES: usize = 4096;
const APPLICATION_ID: i64 = 0x414A5350;
const SPOOL_SCHEMA_VERSION: i64 = 3;
const SPOOL_SCHEMA: &str = r#"
    BEGIN IMMEDIATE;
    CREATE TABLE attempts (
        attempt_id TEXT PRIMARY KEY NOT NULL,
        fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32),
        item TEXT NOT NULL,
        bytes INTEGER NOT NULL CHECK(bytes>=0),
        terminal INTEGER NOT NULL CHECK(terminal IN (0,1)),
        retry_at INTEGER,
        event_pending INTEGER NOT NULL DEFAULT 0 CHECK(event_pending IN (0,1))
    ) STRICT;
    CREATE INDEX recovery ON attempts(terminal, attempt_id);
    CREATE INDEX outbox ON attempts(event_pending, attempt_id);
    CREATE TABLE scheduler (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL) STRICT;
    INSERT INTO scheduler VALUES(1, '{"failures":0,"until":null}');
    PRAGMA application_id=0x414A5350;
    PRAGMA user_version=3;
    COMMIT;
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaObject {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

static EXPECTED_SPOOL_SCHEMA_OBJECTS: OnceLock<Vec<SchemaObject>> = OnceLock::new();

#[cfg(test)]
static TEST_PRE_OPEN_SUBSTITUTION: Mutex<Option<(PathBuf, PathBuf)>> = Mutex::new(None);

// Rust 1.85 exposes stable file identity/link-count metadata only on Unix.
// Keep non-Unix admission portable rather than depending on unstable std APIs.
#[cfg(unix)]
type SpoolIdentity = (u64, u64);
#[cfg(not(unix))]
type SpoolIdentity = ();

fn current_spool_identity(path: &Path) -> CoreResult<SpoolIdentity> {
    let metadata = fs::metadata(path).map_err(unavailable)?;
    if !metadata.is_file() {
        return Err(unavailable("spool database path is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(unavailable(
                "multiply-linked spool databases are not supported",
            ));
        }
        Ok((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Ok(())
    }
}

fn verify_current_spool_identity(path: &Path, expected: SpoolIdentity) -> CoreResult<()> {
    if current_spool_identity(path)? != expected {
        return Err(unavailable("spool database path changed during admission"));
    }
    Ok(())
}

#[cfg(test)]
fn replace_spool_after_preflight_for_test(path: &Path) {
    let replacement = {
        let mut substitution = TEST_PRE_OPEN_SUBSTITUTION
            .lock()
            .expect("test substitution lock");
        if substitution
            .as_ref()
            .is_some_and(|(target, _)| target == path)
        {
            substitution.take().map(|(_, replacement)| replacement)
        } else {
            None
        }
    };
    if let Some(replacement) = replacement {
        fs::rename(replacement, path).expect("test spool substitution");
    }
}

pub const RECOVERABLE_INJECTION_STATES: [InjectionState; 3] = [
    InjectionState::Pending,
    InjectionState::InFlight,
    InjectionState::RetryableFailure,
];

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Includes retained tombstones. Exhaustion requires operator action.
    pub max_items: u64,
    /// Serialized retained rows, not physical SQLite file size.
    pub max_bytes: u64,
    /// Free space that admissions must leave for transitions and SQLite journals.
    pub min_free_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_items: 10_000,
            max_bytes: 256 * 1024 * 1024,
            min_free_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Local operational facts for a proposed claim batch, not a capacity reservation.
#[derive(Debug, Clone, Copy)]
pub struct PressureSnapshot {
    pub retained_items: u64,
    pub retained_bytes: u64,
    pub available_bytes: u64,
    pub required_free_bytes: u64,
    pub claiming_paused: bool,
}

impl PressureSnapshot {
    fn new(
        limits: Limits,
        retained_items: u64,
        retained_bytes: u64,
        available_bytes: u64,
        items: u64,
        bytes: u64,
    ) -> CoreResult<Self> {
        // Allow for page/index growth and rollback journal copies, not just JSON.
        let required_free_bytes = bytes
            .checked_mul(4)
            .and_then(|n| {
                items
                    .checked_mul(16384)
                    .and_then(|overhead| n.checked_add(overhead))
            })
            .and_then(|n| n.checked_add(limits.min_free_bytes))
            .ok_or_else(|| unavailable("spool capacity overflow"))?;
        Ok(Self {
            retained_items,
            retained_bytes,
            available_bytes,
            required_free_bytes,
            claiming_paused: retained_items
                .checked_add(items)
                .is_none_or(|n| n > limits.max_items)
                || retained_bytes
                    .checked_add(bytes)
                    .is_none_or(|n| n > limits.max_bytes)
                || available_bytes < required_free_bytes,
        })
    }
}

struct Inner {
    connection: Connection,
    // Keep the lock until after the database connection has closed.
    lock: OwnerLock,
}

struct OwnerLock {
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
        // Closing alone leaves flock held by descriptors inherited across fork.
        let _ = self.release();
    }
}

pub struct SqliteStore {
    inner: Mutex<Option<Inner>>,
    directory: PathBuf,
    limits: Limits,
}

fn unavailable(error: impl std::fmt::Display) -> CoreError {
    CoreError::SpoolUnavailable(error.to_string())
}

fn reset_required() -> CoreError {
    unavailable(
        "reset required: existing spool is not the current UUID-native spool; archive it and initialize a fresh path",
    )
}

fn terminal(state: InjectionState) -> bool {
    matches!(
        state,
        InjectionState::Accepted
            | InjectionState::RouteUnavailable
            | InjectionState::TerminalFailure
    )
}

fn time_key(time: SystemTime) -> CoreResult<i64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map_err(unavailable)?
        .as_nanos()
        .try_into()
        .map_err(unavailable)
}

pub fn is_recoverable(item: &SpoolItem, now: SystemTime) -> bool {
    !terminal(item.injection_state)
        && item.validate_binding().is_ok()
        && item.next_runtime_try_at.is_none_or(|retry| retry <= now)
}

fn verify_existing_spool(path: &Path) -> CoreResult<()> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| reset_required())?;
    let integrity: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|_| reset_required())?;
    let application: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(|_| reset_required())?;
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|_| reset_required())?;
    if integrity != "ok" || application != APPLICATION_ID || version != SPOOL_SCHEMA_VERSION {
        return Err(reset_required());
    }
    if spool_schema_objects(&connection).map_err(|_| reset_required())?
        != *expected_spool_schema_objects()
    {
        return Err(reset_required());
    }
    Ok(())
}

fn expected_spool_schema_objects() -> &'static Vec<SchemaObject> {
    EXPECTED_SPOOL_SCHEMA_OBJECTS.get_or_init(|| {
        let connection = Connection::open_in_memory()
            .expect("the compiled spool baseline must initialize in memory");
        connection
            .execute_batch(SPOOL_SCHEMA)
            .expect("the compiled spool baseline must be valid SQLite");
        spool_schema_objects(&connection)
            .expect("the compiled spool baseline must be introspectable")
    })
}

fn spool_schema_objects(connection: &Connection) -> rusqlite::Result<Vec<SchemaObject>> {
    let mut statement = connection.prepare(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema
         WHERE type IN ('table','index','trigger','view') AND name NOT LIKE 'sqlite_%'
         ORDER BY type,name",
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

fn normalize_schema_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut whitespace = false;
    for character in sql.chars() {
        if character.is_whitespace() {
            whitespace = true;
        } else {
            if whitespace && !normalized.is_empty() {
                normalized.push(' ');
            }
            normalized.push(character.to_ascii_lowercase());
            whitespace = false;
        }
    }
    normalized
}

fn sqlite_sidecar_entries_exist(path: &Path) -> CoreResult<bool> {
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        match fs::symlink_metadata(Path::new(&sidecar)) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(reset_required()),
        }
    }
    Ok(false)
}

impl SqliteStore {
    /// The parent directory must already exist and be private to the adapter.
    /// A sidecar lock is deliberately never unlinked, avoiding lock-inode races.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> CoreResult<Self> {
        let path = path.as_ref();
        let name = path
            .file_name()
            .ok_or_else(|| unavailable("missing database filename"))?;
        let directory = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(unavailable)?;
        let path = directory.join(name);
        if std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(unavailable("symlink database paths are not supported"));
        }
        let exists = path.exists();
        if sqlite_sidecar_entries_exist(&path)? {
            return Err(reset_required());
        }
        let identity = if exists {
            let identity = current_spool_identity(&path)?;
            verify_existing_spool(&path)?;
            verify_current_spool_identity(&path, identity)?;
            Some(identity)
        } else {
            None
        };
        let mut lock_name = name.to_os_string();
        lock_name.push(".lock");
        let lock_path = directory.join(lock_name);
        if std::fs::symlink_metadata(&lock_path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(unavailable("symlink lock paths are not supported"));
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(lock_path).map_err(unavailable)?;
        let owner_pid = std::process::id();
        lock.try_lock_exclusive().map_err(unavailable)?;
        let lock = OwnerLock {
            file: lock,
            owner_pid,
            released: false,
        };
        if sqlite_sidecar_entries_exist(&path)? {
            return Err(reset_required());
        }
        if let Some(identity) = identity {
            verify_current_spool_identity(&path, identity)?;
        }
        #[cfg(test)]
        replace_spool_after_preflight_for_test(&path);
        let connection = Connection::open(&path).map_err(unavailable)?;
        if let Some(identity) = identity {
            verify_current_spool_identity(&path, identity)?;
        }
        connection
            .busy_timeout(std::time::Duration::ZERO)
            .map_err(unavailable)?;
        // DELETE + EXTRA syncs the rollback journal and its directory removal.
        connection
            .execute_batch(
                "PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA; PRAGMA foreign_keys=ON;",
            )
            .map_err(unavailable)?;
        if !exists {
            connection
                .execute_batch(SPOOL_SCHEMA)
                .map_err(unavailable)?;
        }
        Ok(Self {
            inner: Mutex::new(Some(Inner { connection, lock })),
            directory,
            limits,
        })
    }

    pub fn close(&self) -> CoreResult<()> {
        if let Some(Inner {
            connection,
            mut lock,
        }) = self.inner.lock().map_err(unavailable)?.take()
        {
            drop(connection);
            lock.release().map_err(unavailable)?;
        }
        Ok(())
    }

    fn with_connection<T>(
        &self,
        f: impl FnOnce(&mut Connection) -> CoreResult<T>,
    ) -> CoreResult<T> {
        let mut guard = self.inner.lock().map_err(unavailable)?;
        let inner = guard
            .as_mut()
            .ok_or_else(|| unavailable("spool is closed"))?;
        f(&mut inner.connection)
    }

    /// Pre-claim admission check. The caller supplies the entire proposed batch's
    /// maximum serialized bytes; put checks again. This does not reserve capacity.
    pub fn check_capacity(&self, additional_items: u64, additional_bytes: u64) -> CoreResult<()> {
        self.with_connection(|connection| {
            self.capacity(connection, additional_items, additional_bytes)
        })
    }

    fn capacity(&self, connection: &Connection, items: u64, bytes: u64) -> CoreResult<()> {
        if self
            .read_pressure(connection, items, bytes)?
            .claiming_paused
        {
            return Err(unavailable("spool pressure: admission stopped"));
        }
        Ok(())
    }

    pub fn pressure_snapshot(
        &self,
        additional_items: u64,
        additional_bytes: u64,
    ) -> CoreResult<PressureSnapshot> {
        self.with_connection(|connection| {
            self.read_pressure(connection, additional_items, additional_bytes)
        })
    }

    fn read_pressure(
        &self,
        connection: &Connection,
        items: u64,
        bytes: u64,
    ) -> CoreResult<PressureSnapshot> {
        let (used_items, used_bytes): (i64, i64) = connection
            .query_row(
                "SELECT count(*), coalesce(sum(bytes),0) FROM attempts",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(unavailable)?;
        let used_items = u64::try_from(used_items).map_err(unavailable)?;
        let used_bytes = u64::try_from(used_bytes).map_err(unavailable)?;
        PressureSnapshot::new(
            self.limits,
            used_items,
            used_bytes,
            fs2::available_space(&self.directory).map_err(unavailable)?,
            items,
            bytes,
        )
    }

    fn load(connection: &Connection, attempt_id: &str) -> CoreResult<SpoolItem> {
        let data: String = connection
            .query_row(
                "SELECT item FROM attempts WHERE attempt_id=?1",
                [attempt_id],
                |r| r.get(0),
            )
            .map_err(unavailable)?;
        let item: SpoolItem = serde_json::from_str(&data).map_err(unavailable)?;
        item.validate_binding()?;
        if item.attempt_id != attempt_id {
            return Err(unavailable("corrupt attempt binding"));
        }
        Ok(item)
    }

    fn save(transaction: &Transaction<'_>, item: &SpoolItem) -> CoreResult<()> {
        item.validate_binding()?;
        let data = serde_json::to_string(item).map_err(unavailable)?;
        let retry = item.next_runtime_try_at.map(time_key).transpose()?;
        transaction.execute(
            "UPDATE attempts SET item=?2, bytes=?3, terminal=?4, retry_at=?5, event_pending=?6 WHERE attempt_id=?1",
            params![item.attempt_id, data, data.len() as i64, terminal(item.injection_state), retry, item.pending_event.is_some()],
        ).map_err(unavailable)?;
        Ok(())
    }

    fn commit(transaction: Transaction<'_>) -> CoreResult<()> {
        #[cfg(test)]
        checkpoint("before");
        transaction.commit().map_err(unavailable)?;
        #[cfg(test)]
        checkpoint("after");
        Ok(())
    }

    fn update(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
        change: impl FnOnce(&mut SpoolItem) -> CoreResult<()>,
    ) -> CoreResult<()> {
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(unavailable)?;
            let mut item = Self::load(&transaction, attempt_id)?;
            if item.instance_id != instance_id || item.generation != generation {
                return Err(unavailable("conflicting installation or generation"));
            }
            change(&mut item)?;
            Self::save(&transaction, &item)?;
            Self::commit(transaction)
        })
    }

    pub fn mark_retryable(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
        detail: &str,
        next_try_at: SystemTime,
    ) -> CoreResult<()> {
        time_key(next_try_at)?;
        self.fail(
            attempt_id,
            instance_id,
            generation,
            InjectionState::RetryableFailure,
            detail,
            Some(next_try_at),
        )
    }

    fn fail(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
        state: InjectionState,
        detail: &str,
        retry: Option<SystemTime>,
    ) -> CoreResult<()> {
        if !matches!(
            state,
            InjectionState::RetryableFailure
                | InjectionState::RouteUnavailable
                | InjectionState::TerminalFailure
        ) || detail.len() > MAX_DETAIL_BYTES
        {
            return Err(unavailable("invalid failure state or detail"));
        }
        self.update(attempt_id, instance_id, generation, |item| {
            if !item.custody_confirmed {
                return Err(CoreError::CustodyNotConfirmed);
            }
            if item.injection_state == state
                && item.failure_detail == detail
                && item.next_runtime_try_at == retry
            {
                return Ok(());
            }
            if terminal(item.injection_state) {
                return Err(unavailable("attempt outcome is final"));
            }
            item.injection_state = state;
            item.failure_detail = detail.into();
            item.next_runtime_try_at = retry;
            Ok(())
        })
    }

    /// Explicit payload-retention decision after outcome reporting. Keep the
    /// original fingerprint and all bindings so old puts cannot restore payload.
    pub fn compact(&self, attempt_id: &str) -> CoreResult<()> {
        let binding = self.get(attempt_id)?;
        self.update(
            attempt_id,
            &binding.instance_id,
            binding.generation,
            |item| {
                if !terminal(item.injection_state) || item.pending_event.is_some() {
                    return Err(unavailable("cannot compact unfinished attempt"));
                }
                item.envelope.body.clear();
                item.record = None;
                Ok(())
            },
        )
    }

    /// Keyset pagination includes unconfirmed custody work. Only confirmed rows
    /// are eligible for injection; callers must still recheck central fencing.
    pub fn recoverable_after(
        &self,
        now: SystemTime,
        limit: usize,
        after: Option<&str>,
    ) -> CoreResult<Vec<SpoolItem>> {
        if limit == 0 || limit > MAX_RECOVERY_BATCH {
            return Err(unavailable("invalid recovery limit"));
        }
        let now_key = time_key(now)?;
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT attempt_id FROM attempts WHERE terminal=0 AND (?1 IS NULL OR attempt_id>?1)
                 AND (retry_at IS NULL OR retry_at<=?2) ORDER BY attempt_id LIMIT ?3"
            ).map_err(unavailable)?;
            let ids = statement
                .query_map(params![after, now_key, limit as i64], |r| {
                    r.get::<_, String>(0)
                })
                .map_err(unavailable)?;
            let mut result = Vec::new();
            for id in ids {
                let item = Self::load(connection, &id.map_err(unavailable)?)?;
                if !is_recoverable(&item, now) {
                    return Err(unavailable("corrupt recovery index"));
                }
                result.push(item);
            }
            Ok(result)
        })
    }
}

impl Spool for SqliteStore {
    fn put(&self, item: &SpoolItem) -> CoreResult<()> {
        item.validate_binding()?;
        if item.attempt_id.is_empty()
            || item.mailbox_item_id.is_empty()
            || item.claim_id.is_empty()
            || item.instance_id.is_empty()
            || item.record_id.is_empty()
            || item.space_id.is_empty()
            || item.generation <= 0
            || item.custody_confirmed
            || item.injection_state != InjectionState::Pending
            || !item.runtime_receipt.is_empty()
            || !item.failure_detail.is_empty()
            || item.next_runtime_try_at.is_some()
            || item.pending_event.is_some()
            || item.event_sequence != 0
            || item.runtime_failures != 0
        {
            return Err(unavailable("put requires a complete new pending attempt"));
        }
        let data = serde_json::to_string(item).map_err(unavailable)?;
        let fingerprint = Sha256::digest(data.as_bytes()).to_vec();
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(unavailable)?;
            let existing: Option<Vec<u8>> = transaction.query_row("SELECT fingerprint FROM attempts WHERE attempt_id=?1", [&item.attempt_id], |r| r.get(0)).optional().map_err(unavailable)?;
            if let Some(existing) = existing {
                return if existing == fingerprint { Ok(()) } else { Err(unavailable("conflicting attempt binding or payload")) };
            }
            self.capacity(&transaction, 1, data.len() as u64)?;
            transaction.execute(
                "INSERT INTO attempts(attempt_id,fingerprint,item,bytes,terminal,retry_at) VALUES(?1,?2,?3,?4,0,NULL)",
                params![item.attempt_id, fingerprint, data, data.len() as i64]
            ).map_err(unavailable)?;
            Self::commit(transaction)
        })
    }

    fn get(&self, attempt_id: &str) -> CoreResult<SpoolItem> {
        self.with_connection(|connection| Self::load(connection, attempt_id))
    }

    fn reconcile_expired_claim(
        &self,
        expired: &CustodyResult,
        replacement: &SpoolItem,
    ) -> CoreResult<()> {
        replacement.validate_binding()?;
        if expired.claim_id.is_empty()
            || replacement.claim_id.is_empty()
            || expired.claim_id == replacement.claim_id
            || expired.generation != replacement.generation
            || expired
                .items
                .iter()
                .filter(|result| {
                    result.attempt_id == replacement.attempt_id
                        || result.mailbox_item_id == replacement.mailbox_item_id
                })
                .count()
                != 1
            || !expired.items.iter().any(|result| {
                result.attempt_id == replacement.attempt_id
                    && result.mailbox_item_id == replacement.mailbox_item_id
                    && result.result == CustodyResultState::LeaseExpired
            })
        {
            return Err(unavailable("exact expired claim custody result required"));
        }
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(unavailable)?;
            let mut current = Self::load(&transaction, &replacement.attempt_id)?;
            if current.custody_confirmed
                || current.injection_state != InjectionState::Pending
                || !current.runtime_receipt.is_empty()
                || !current.failure_detail.is_empty()
                || current.next_runtime_try_at.is_some()
                || (current.claim_id != expired.claim_id
                    && current.claim_id != replacement.claim_id)
            {
                return Err(unavailable("attempt cannot be rebound"));
            }
            let old_bytes = serde_json::to_vec(&current).map_err(unavailable)?.len();
            current.claim_id.clone_from(&replacement.claim_id);
            if current != *replacement {
                return Err(unavailable("conflicting attempt binding or payload"));
            }
            let data = serde_json::to_vec(&current).map_err(unavailable)?;
            self.capacity(&transaction, 0, data.len().saturating_sub(old_bytes) as u64)?;
            Self::save(&transaction, &current)?;
            transaction
                .execute(
                    "UPDATE attempts SET fingerprint=?2 WHERE attempt_id=?1",
                    params![current.attempt_id, Sha256::digest(&data).to_vec()],
                )
                .map_err(unavailable)?;
            Self::commit(transaction)
        })
    }

    fn confirm_custody(
        &self,
        attempt_id: &str,
        claim_id: &str,
        instance_id: &str,
        generation: i64,
    ) -> CoreResult<()> {
        self.update(attempt_id, instance_id, generation, |item| {
            if item.claim_id != claim_id {
                return Err(unavailable("conflicting claim"));
            }
            item.custody_confirmed = true;
            Ok(())
        })
    }

    fn mark_injection_started(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
    ) -> CoreResult<()> {
        self.update(attempt_id, instance_id, generation, |item| {
            if !item.custody_confirmed {
                return Err(CoreError::CustodyNotConfirmed);
            }
            if item.pending_event.is_some() {
                return Err(unavailable(
                    "telemetry must be acknowledged before another injection",
                ));
            }
            if terminal(item.injection_state) {
                return Err(unavailable("attempt outcome is final"));
            }
            item.injection_state = InjectionState::InFlight;
            item.next_runtime_try_at = None;
            Ok(())
        })
    }

    fn mark_injected(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
        receipt: &str,
    ) -> CoreResult<()> {
        if receipt.len() > MAX_DETAIL_BYTES {
            return Err(unavailable("receipt too large"));
        }
        self.update(attempt_id, instance_id, generation, |item| {
            if !item.custody_confirmed {
                return Err(CoreError::CustodyNotConfirmed);
            }
            if item.injection_state == InjectionState::Accepted && item.runtime_receipt == receipt {
                return Ok(());
            }
            if item.injection_state != InjectionState::InFlight {
                return Err(unavailable("injection was not started"));
            }
            item.injection_state = InjectionState::Accepted;
            item.runtime_receipt = receipt.into();
            item.failure_detail.clear();
            item.next_runtime_try_at = None;
            Ok(())
        })
    }

    fn mark_injection_failed(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
        state: InjectionState,
        detail: &str,
    ) -> CoreResult<()> {
        self.fail(attempt_id, instance_id, generation, state, detail, None)
    }

    fn recoverable(&self, now: SystemTime, limit: usize) -> CoreResult<Vec<SpoolItem>> {
        self.recoverable_after(now, limit, None)
    }
}

impl AdapterSpool for SqliteStore {
    fn check_capacity(&self, items: u64, bytes: u64) -> CoreResult<()> {
        Self::check_capacity(self, items, bytes)
    }

    fn find(&self, attempt: &str) -> CoreResult<Option<SpoolItem>> {
        self.with_connection(|connection| {
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1)",
                    [attempt],
                    |row| row.get(0),
                )
                .map_err(unavailable)?;
            if exists {
                Self::load(connection, attempt).map(Some)
            } else {
                Ok(None)
            }
        })
    }

    fn work_after(&self, now: SystemTime, after: Option<&str>) -> CoreResult<Option<SpoolItem>> {
        let now = time_key(now)?;
        self.with_connection(|connection| {
            let id: Option<String> = connection
                .query_row(
                    "SELECT attempt_id FROM attempts WHERE (?1 IS NULL OR attempt_id>?1)
                 AND (event_pending=1 OR (terminal=0 AND (retry_at IS NULL OR retry_at<=?2)))
                 ORDER BY attempt_id LIMIT 1",
                    params![after, now],
                    |row| row.get(0),
                )
                .optional()
                .map_err(unavailable)?;
            id.map(|id| Self::load(connection, &id)).transpose()
        })
    }

    fn finish(&self, before: &SpoolItem, after: &SpoolItem) -> CoreResult<()> {
        let event = after
            .pending_event
            .as_ref()
            .ok_or_else(|| unavailable("missing outcome event"))?;
        event.validate()?;
        if !before.ready_for_injection()
            || before.pending_event.is_some()
            || event.attempt_id != before.attempt_id
            || event.generation != before.generation
            || after.event_sequence
                != before
                    .event_sequence
                    .checked_add(1)
                    .ok_or_else(|| unavailable("event sequence overflow"))?
            || after.runtime_receipt.len() > MAX_DETAIL_BYTES
            || after.failure_detail.len() > MAX_DETAIL_BYTES
        {
            return Err(unavailable("invalid result transition"));
        }
        let mut expected = before.clone();
        expected.injection_state = after.injection_state;
        expected.runtime_receipt.clone_from(&after.runtime_receipt);
        expected.failure_detail.clone_from(&after.failure_detail);
        expected.next_runtime_try_at = after.next_runtime_try_at;
        expected.event_sequence = after.event_sequence;
        expected.runtime_failures = after.runtime_failures;
        expected.pending_event.clone_from(&after.pending_event);
        if expected != *after {
            return Err(unavailable("result changed immutable binding"));
        }
        let valid_state = match after.injection_state {
            InjectionState::Accepted => {
                before.injection_state == InjectionState::InFlight
                    && event.state
                        == journal_adapter_core::OutcomeState::AdapterReportedRuntimeAccepted
            }
            InjectionState::RetryableFailure => {
                event.state == journal_adapter_core::OutcomeState::AdapterReportedRetryableFailure
                    && after.next_runtime_try_at.is_some()
            }
            InjectionState::RouteUnavailable => {
                event.state == journal_adapter_core::OutcomeState::RouteUnavailable
            }
            InjectionState::TerminalFailure => {
                event.state == journal_adapter_core::OutcomeState::AdapterReportedTerminalFailure
            }
            _ => false,
        };
        if !valid_state {
            return Err(unavailable("outcome does not match telemetry"));
        }
        self.update(
            &before.attempt_id,
            &before.instance_id,
            before.generation,
            |item| {
                if *item == *after {
                    return Ok(());
                }
                if *item != *before {
                    return Err(unavailable("concurrent result change"));
                }
                *item = after.clone();
                Ok(())
            },
        )
    }

    fn acknowledge_event(&self, attempt: &str, event: &EventRequest) -> CoreResult<()> {
        let binding = self.get(attempt)?;
        self.update(attempt, &binding.instance_id, binding.generation, |item| {
            if item.pending_event.as_ref() != Some(event) {
                return Err(unavailable("event acknowledgement mismatch"));
            }
            item.pending_event = None;
            Ok(())
        })
    }

    fn suppress(&self, binding: &SpoolItem) -> CoreResult<()> {
        self.update(
            &binding.attempt_id,
            &binding.instance_id,
            binding.generation,
            |item| {
                if item.custody_confirmed || item.injection_state != InjectionState::Pending {
                    return Err(unavailable("cannot suppress custodied attempt"));
                }
                item.injection_state = InjectionState::TerminalFailure;
                item.failure_detail = "custody suppressed-revoked".into();
                Ok(())
            },
        )
    }

    fn backoff(&self) -> CoreResult<Backoff> {
        self.with_connection(|connection| {
            let value: String = connection
                .query_row("SELECT value FROM scheduler WHERE id=1", [], |row| {
                    row.get(0)
                })
                .map_err(unavailable)?;
            serde_json::from_str(&value).map_err(unavailable)
        })
    }

    fn set_backoff(&self, backoff: &Backoff) -> CoreResult<()> {
        self.with_connection(|connection| {
            let transaction = connection.transaction().map_err(unavailable)?;
            transaction
                .execute(
                    "UPDATE scheduler SET value=?1 WHERE id=1",
                    [serde_json::to_string(backoff).map_err(unavailable)?],
                )
                .map_err(unavailable)?;
            Self::commit(transaction)
        })
    }
}

#[cfg(test)]
fn checkpoint(phase: &str) {
    if std::env::var("SPOOL_CRASH_PHASE").is_ok_and(|value| value == phase) {
        let path = PathBuf::from(std::env::var_os("SPOOL_CHILD_PATH").unwrap()).join("checkpoint");
        File::create(path).unwrap().sync_all().unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod orchestration_tests;
