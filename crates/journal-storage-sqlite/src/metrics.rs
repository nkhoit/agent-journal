//! Host-local aggregate facts. Never expose this snapshot on a principal router.

use crate::{Database, StorageError};
use std::path::Path;

#[derive(Debug)]
pub struct OperationalSnapshot {
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub unacknowledged_inbox_count: u64,
    pub oldest_unacknowledged_at: Option<String>,
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
    /// SQL facts share a read snapshot; filesystem sizes are adjacent
    /// observations and do not force a checkpoint.
    pub fn operational_snapshot(&self) -> Result<OperationalSnapshot, StorageError> {
        let mut connection = self.connect_read_only()?;
        let transaction = connection.transaction()?;
        let (unacknowledged_inbox_count, oldest_unacknowledged_at) = transaction.query_row(
            "SELECT count(*), min(created_at) FROM mailbox_items WHERE acknowledged_at IS NULL",
            [],
            |r| Ok((count(r, 0)?, r.get(1)?)),
        )?;
        let database_bytes = file_bytes(self.path(), false)?;
        let mut wal = self.path().as_os_str().to_os_string();
        wal.push("-wal");
        let wal_bytes = file_bytes(Path::new(&wal), true)?;
        transaction.commit()?;
        Ok(OperationalSnapshot {
            database_bytes,
            wal_bytes,
            unacknowledged_inbox_count,
            oldest_unacknowledged_at,
        })
    }
}
