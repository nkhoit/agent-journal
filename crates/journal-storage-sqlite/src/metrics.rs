//! Host-local aggregate facts. Never expose this snapshot on a principal router.

use crate::{Database, StorageError};
use std::path::Path;

#[derive(Debug)]
pub struct OperationalSnapshot {
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub pending_mailbox_count: u64,
    pub oldest_pending_at: Option<String>,
    pub outstanding_claims: u64,
    pub expired_active_claims: u64,
    pub expired_claims: u64,
    pub oldest_active_heartbeat_at: Option<String>,
    pub stale_registrations_with_pending: u64,
    pub runtime_failure_events: u64,
}

fn count(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    value
        .try_into()
        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}

fn file_bytes(path: &Path, absent_is_empty: bool) -> Result<u64, StorageError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if absent_is_empty && error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(source) => Err(StorageError::Io {
            operation: "measure",
            path: path.to_owned(),
            source,
        }),
    }
}

impl Database {
    /// `now` is a server clock RFC 3339 timestamp. SQL facts share a read snapshot;
    /// filesystem sizes are adjacent observations and do not force a checkpoint.
    pub fn operational_snapshot(&self, now: &str) -> Result<OperationalSnapshot, StorageError> {
        let mut connection = self.connect_read_only()?;
        let transaction = connection.transaction()?;
        let (pending_mailbox_count, oldest_pending_at) = transaction.query_row(
            "SELECT count(*), min(created_at) FROM mailbox_items WHERE state='pending'",
            [],
            |r| Ok((count(r, 0)?, r.get(1)?)),
        )?;
        let (outstanding_claims, expired_active_claims, expired_claims) = transaction.query_row(
            "SELECT count(*) FILTER (WHERE state='active' AND julianday(lease_expires_at)>julianday(?1)),
             count(*) FILTER (WHERE state='active' AND julianday(lease_expires_at)<=julianday(?1)),
             count(*) FILTER (WHERE state='expired') FROM claims",
            [now], |r| Ok((count(r, 0)?, count(r, 1)?, count(r, 2)?)),
        )?;
        let oldest_active_heartbeat_at = transaction.query_row(
            "SELECT min(last_heartbeat_at) FROM adapter_registrations WHERE status='active'",
            [],
            |r| r.get(0),
        )?;
        let stale_registrations_with_pending = transaction.query_row(
            "SELECT count(*) FROM adapter_registrations r WHERE r.status='active'
             AND julianday(r.lease_expires_at)<=julianday(?1)
             AND EXISTS (SELECT 1 FROM mailbox_items m WHERE m.recipient_principal_id=r.principal_id AND m.state='pending')",
            [now], |r| count(r, 0),
        )?;
        let runtime_failure_events = transaction.query_row(
            "SELECT count(*) FROM delivery_events WHERE state IN
             ('adapter-reported-retryable-failure','adapter-reported-terminal-failure','route-unavailable')",
            [], |r| count(r, 0),
        )?;
        let database_bytes = file_bytes(self.path(), false)?;
        let mut wal = self.path().as_os_str().to_os_string();
        wal.push("-wal");
        let wal_bytes = file_bytes(Path::new(&wal), true)?;
        transaction.commit()?;
        Ok(OperationalSnapshot {
            database_bytes,
            wal_bytes,
            pending_mailbox_count,
            oldest_pending_at,
            outstanding_claims,
            expired_active_claims,
            expired_claims,
            oldest_active_heartbeat_at,
            stale_registrations_with_pending,
            runtime_failure_events,
        })
    }
}
