//! Service-layer ports that keep authorization and durable storage separate.

use std::time::SystemTime;

use journal_domain::{AppendResult, Record, RecordInput, ValidationError};
use thiserror::Error;

mod bootstrap;
pub use bootstrap::*;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("validation failed: {0}")]
    Validation(#[from] ValidationError),
    #[error("service port failed: {0}")]
    Port(String),
}

/// Implementations must atomically insert a record and all mailbox obligations.
pub trait JournalStore {
    fn append(
        &self,
        actor: &str,
        space: &str,
        input: &RecordInput,
        idempotency_key: &str,
    ) -> Result<AppendResult, ServiceError>;
    fn get(&self, actor: &str, record_id: &str) -> Result<Record, ServiceError>;
    fn list(
        &self,
        actor: &str,
        space: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<String>), ServiceError>;
    fn search(
        &self,
        actor: &str,
        space: &str,
        query: &str,
        order: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<(Vec<Record>, Option<String>), ServiceError>;
}

pub trait Authorizer {
    fn can_read(&self, principal: &str, space: &str) -> Result<bool, ServiceError>;
    fn can_append(&self, principal: &str, space: &str) -> Result<bool, ServiceError>;
    /// Author and addressed recipient visibility is checked per entry. Other
    /// readers must receive neither delivery status nor existence signals.
    fn can_read_delivery_status(
        &self,
        principal: &str,
        record_id: &str,
        recipient: &str,
    ) -> Result<bool, ServiceError>;
    fn can_admin(&self, principal: &str) -> Result<bool, ServiceError>;
}

pub trait Clock {
    fn now(&self) -> SystemTime;
}

pub trait IdGenerator {
    fn new_id(&self) -> Result<String, ServiceError>;
}
