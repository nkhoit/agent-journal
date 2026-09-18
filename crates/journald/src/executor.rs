use std::sync::Arc;

use journal_storage_sqlite::{Database, StorageError};
use thiserror::Error;
use tokio::sync::Semaphore;

#[derive(Debug, Error)]
pub enum BlockingError {
    #[error("blocking execution limit must be positive")]
    InvalidLimit,
    #[error("blocking database execution capacity is exhausted")]
    AtCapacity,
    #[error("database operation failed: {0}")]
    Storage(#[from] StorageError),
    #[error("blocking database task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone)]
pub struct BlockingExecutor {
    database: Database,
    permits: Arc<Semaphore>,
}

impl BlockingExecutor {
    pub fn new(database: Database, limit: usize) -> Result<Self, BlockingError> {
        if !(1..=Semaphore::MAX_PERMITS).contains(&limit) {
            return Err(BlockingError::InvalidLimit);
        }
        Ok(Self {
            database,
            permits: Arc::new(Semaphore::new(limit)),
        })
    }

    pub async fn execute<T, F>(&self, operation: F) -> Result<T, BlockingError>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> Result<T, StorageError> + Send + 'static,
    {
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| BlockingError::AtCapacity)?;
        let database = self.database.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(&database)
        })
        .await?;
        result.map_err(BlockingError::Storage)
    }
}
