//! Service-layer ports that keep authorization and durable storage separate.

use std::time::{Duration, SystemTime};

use journal_domain::{
    AppendResult, DeliveryState, Record, RecordInput, TelemetryDetail, TelemetryState,
    ValidationError, validate_claim_limit, validate_generation, validate_telemetry_detail,
};
use serde::{Deserialize, Serialize};
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

pub trait MailboxStore {
    fn claim(
        &self,
        adapter: &AuthenticatedAdapter,
        request: &ClaimRequest,
    ) -> Result<Claim, ServiceError>;
    fn commit(
        &self,
        adapter: &AuthenticatedAdapter,
        request: &CommitRequest,
    ) -> Result<CommitResult, ServiceError>;
    fn record_event(
        &self,
        adapter: &AuthenticatedAdapter,
        mailbox_item_id: &str,
        request: &EventRequest,
    ) -> Result<(), ServiceError>;
    fn delivery_status(
        &self,
        record_id: &str,
        recipient: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<(Vec<DeliverySummary>, Option<String>), ServiceError>;
    fn status(&self, principal: &str) -> Result<MailboxStatus, ServiceError>;
}

/// Identity bound to the authenticated delivery credential. These fields are
/// server context and are never accepted from a delivery JSON body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedAdapter {
    pub principal: String,
    pub adapter_id: String,
    pub instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRequest {
    pub generation: i64,
    pub limit: usize,
    pub wait: Duration,
}

impl ClaimRequest {
    pub fn validate(&self) -> Result<(), ServiceError> {
        validate_generation(self.generation)?;
        validate_claim_limit(self.limit)?;
        if self.wait > Duration::from_secs(journal_domain::MAX_LONG_POLL_SECONDS) {
            return Err(ValidationError::InvalidLongPoll {
                max: journal_domain::MAX_LONG_POLL_SECONDS,
            }
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub id: String,
    pub state: ClaimState,
    pub items: Vec<ClaimItem>,
    pub lease_expires_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimItem {
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub record: Record,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimState {
    Active,
    Committed,
    Expired,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitItem {
    pub mailbox_item_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitItemResult {
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub result: CommitItemResultState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommitItemResultState {
    Committed,
    AlreadyCommitted,
    ClaimNotFound,
    StaleGeneration,
    AttemptMismatch,
    LeaseExpired,
    SuppressedRevoked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub claim_id: String,
    pub generation: i64,
    pub items: Vec<CommitItem>,
}

impl CommitRequest {
    pub fn validate(&self) -> Result<(), ServiceError> {
        validate_generation(self.generation)?;
        if self.items.is_empty() || self.items.len() > journal_domain::MAX_CLAIM_BATCH {
            return Err(ValidationError::InvalidClaimLimit {
                max: journal_domain::MAX_CLAIM_BATCH,
            }
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    pub claim_id: String,
    pub generation: i64,
    pub items: Vec<CommitItemResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRequest {
    pub attempt_id: String,
    pub event_id: String,
    pub generation: i64,
    pub state: TelemetryState,
    pub occurred_at: SystemTime,
    pub detail: TelemetryDetail,
}

impl EventRequest {
    pub fn validate(&self) -> Result<(), ServiceError> {
        validate_generation(self.generation)?;
        validate_telemetry_detail(&self.detail)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySummary {
    pub mailbox_item_id: String,
    pub recipient: String,
    pub state: DeliveryState,
    pub attempts: usize,
    pub last_attempt_id: Option<String>,
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxStatus {
    pub principal_id: String,
    pub pending: usize,
    pub oldest_pending_at: Option<SystemTime>,
    pub paused: bool,
}

pub trait Clock {
    fn now(&self) -> SystemTime;
}

pub trait IdGenerator {
    fn new_id(&self) -> Result<String, ServiceError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_request_reuses_domain_telemetry_limit() {
        let request = EventRequest {
            attempt_id: "attempt-1".into(),
            event_id: "event-1".into(),
            generation: 1,
            state: TelemetryState::AdapterReportedRuntimeAccepted,
            occurred_at: SystemTime::UNIX_EPOCH,
            detail: [
                ("message".into(), "界".repeat(1024)),
                ("padding".into(), "a".repeat(1024)),
            ]
            .into_iter()
            .collect(),
        };
        assert!(request.validate().is_err());
    }
}
