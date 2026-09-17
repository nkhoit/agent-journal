//! Stable, runtime-neutral wire helpers shared by clients and services.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use journal_domain as domain;
pub use journal_domain::{DeliveryState, TelemetryState};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterRegisterRequest {
    pub instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterHeartbeatRequest {
    pub instance_id: String,
    pub generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRequest {
    pub instance_id: String,
    pub generation: i64,
    pub limit: usize,
    #[serde(default)]
    pub wait_seconds: u64,
}

impl ClaimRequest {
    pub fn validate(&self) -> Result<(), domain::ValidationError> {
        domain::validate_generation(self.generation)?;
        domain::validate_claim_limit(self.limit)?;
        domain::validate_long_poll_seconds(self.wait_seconds)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimItem {
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub record: domain::Record,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClaimState {
    Active,
    Committed,
    Expired,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimResponse {
    pub claim_id: String,
    pub state: ClaimState,
    pub lease_expires_at: String,
    pub items: Vec<ClaimItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitItem {
    pub mailbox_item_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    pub generation: i64,
    pub items: Vec<CommitItem>,
}

impl CommitRequest {
    pub fn validate(&self) -> Result<(), domain::ValidationError> {
        domain::validate_generation(self.generation)?;
        if self.items.is_empty() || self.items.len() > domain::MAX_CLAIM_BATCH {
            return Err(domain::ValidationError::InvalidClaimLimit {
                max: domain::MAX_CLAIM_BATCH,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommitItemResult {
    Committed,
    AlreadyCommitted,
    ClaimNotFound,
    StaleGeneration,
    AttemptMismatch,
    LeaseExpired,
    SuppressedRevoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitItemResultEntry {
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub result: CommitItemResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitResponse {
    pub claim_id: String,
    pub generation: i64,
    pub items: Vec<CommitItemResultEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryEnvelope {
    pub record_id: String,
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub space_id: String,
    pub from_principal: String,
    pub source_run: Option<String>,
    pub reply_to: Option<String>,
    pub addressed_to: String,
    pub routing_key: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryEventRequest {
    pub event_id: String,
    pub attempt_id: String,
    pub generation: i64,
    pub occurred_at: String,
    pub state: TelemetryState,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub detail: domain::TelemetryDetail,
}

impl DeliveryEventRequest {
    pub fn validate(&self) -> Result<(), domain::ValidationError> {
        domain::validate_generation(self.generation)?;
        domain::validate_telemetry_detail(&self.detail)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryEventResponse {
    pub event_id: String,
    pub state: TelemetryState,
    pub received_at: String,
}

pub type Headers = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub path: String,
    /// Authentication, request correlation, and idempotency metadata belong
    /// to transport headers rather than JSON bodies.
    pub headers: Headers,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Headers,
    pub body: Vec<u8>,
}

impl Request {
    pub fn new(method: impl Into<String>, path: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            headers: Headers::new(),
            body,
        }
    }
}

impl Response {
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: Headers::new(),
            body,
        }
    }
}

/// Synchronous transport seam for the future authenticated HTTP client.
/// Concrete HTTP/TLS code is deliberately not part of this scaffold.
pub trait Transport: Send + Sync {
    fn send(&self, request: Request) -> Result<Response, TransportError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    Unavailable,
    Protocol(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("journal service unavailable"),
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
        }
    }
}

impl std::error::Error for TransportError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_messages_keep_headers_outside_json_body() {
        let mut request = Request::new("POST", "/v1/spaces/example/records", br"{}".to_vec());
        request
            .headers
            .insert("Idempotency-Key".into(), "test-key".into());
        assert_eq!(request.body, b"{}");
        assert_eq!(request.headers["Idempotency-Key"], "test-key");

        let mut response = Response::new(201, br"{}".to_vec());
        response
            .headers
            .insert("X-Request-ID".into(), "request-1".into());
        assert_eq!(response.headers["X-Request-ID"], "request-1");
    }

    #[test]
    fn claim_request_defaults_wait_and_rejects_null_or_unknown_fields() {
        let request: ClaimRequest =
            serde_json::from_str(r#"{"instance_id":"instance-1","generation":1,"limit":20}"#)
                .expect("claim request");
        assert_eq!(request.wait_seconds, 0);
        assert!(request.validate().is_ok());

        assert!(
            serde_json::from_str::<ClaimRequest>(
                r#"{"instance_id":"instance-1","generation":1,"limit":20,"wait_seconds":null}"#,
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ClaimRequest>(
                r#"{"instance_id":"instance-1","generation":1,"limit":20,"extra":true}"#,
            )
            .is_err()
        );
    }

    #[test]
    fn delivery_event_body_contains_only_wire_fields() {
        let request = DeliveryEventRequest {
            event_id: "event-1".into(),
            attempt_id: "attempt-1".into(),
            generation: 1,
            occurred_at: "2026-01-01T00:00:00Z".into(),
            state: TelemetryState::RouteUnavailable,
            detail: BTreeMap::new(),
        };
        let value = serde_json::to_value(&request).expect("event request");
        assert_eq!(value["state"], "route-unavailable");
        assert!(value.get("mailbox_item_id").is_none());
        assert!(value.get("adapter_id").is_none());
        assert!(
            serde_json::from_value::<DeliveryEventRequest>(serde_json::json!({
                "event_id": "event-1",
                "attempt_id": "attempt-1",
                "generation": 1,
                "occurred_at": "2026-01-01T00:00:00Z",
                "state": "route-unavailable",
                "mailbox_item_id": "not-a-wire-field"
            }))
            .is_err()
        );
    }
}
