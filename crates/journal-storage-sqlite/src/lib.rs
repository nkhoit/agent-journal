//! SQLite policy and repository seams. No SQLite driver is selected yet.

use std::time::SystemTime;

use thiserror::Error;

pub const MIGRATION_VERSION: u32 = 1;

pub const CONNECTION_PRAGMAS: [&str; 4] = [
    "PRAGMA foreign_keys = ON",
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = FULL",
    "PRAGMA busy_timeout = 5000",
];

pub fn connection_pragmas() -> &'static [&'static str; 4] {
    &CONNECTION_PRAGMAS
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage is not implemented")]
    NotImplemented,
    #[error("transaction failed: {0}")]
    Transaction(String),
    #[error("backup failed: {0}")]
    Backup(String),
}

/// Minimal transaction ownership boundary for service implementations.
pub trait Transaction {
    fn commit(&mut self) -> Result<(), StorageError>;
    fn rollback(&mut self) -> Result<(), StorageError>;
}

pub trait Transactioner {
    fn with_transaction<T, F>(&self, operation: F) -> Result<T, StorageError>
    where
        F: FnOnce(&mut dyn Transaction) -> Result<T, StorageError>;
}

/// Backups are a recovery primitive, not part of the public record protocol.
pub trait BackupSource {
    fn backup(&self, destination: &str) -> Result<(), StorageError>;
    fn last_backup_at(&self) -> Result<SystemTime, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_policy_captures_durability_invariants() {
        assert_eq!(
            connection_pragmas(),
            &[
                "PRAGMA foreign_keys = ON",
                "PRAGMA journal_mode = WAL",
                "PRAGMA synchronous = FULL",
                "PRAGMA busy_timeout = 5000",
            ]
        );
        assert_eq!(MIGRATION_VERSION, 1);
    }
}
