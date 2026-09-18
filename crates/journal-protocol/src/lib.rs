//! Stable, runtime-neutral wire types and codecs shared by clients and services.

use std::collections::BTreeMap;

mod canonical;
mod cursor;
mod dto;
mod json;
mod query;

pub use canonical::{CanonicalizationError, canonical_append};
pub use cursor::{CursorCodec, CursorError, CursorOrder, CursorPosition, CursorRoute, CursorScope};
pub use dto::*;
pub use journal_domain as domain;
pub use journal_domain::{DeliveryState, TelemetryState};
pub use json::{decode_json, encode_json};
pub use query::*;

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
/// Concrete HTTP/TLS code is deliberately not part of this slice.
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
            decode_json(br#"{"instance_id":"instance-1","generation":1,"limit":20}"#)
                .expect("claim request");
        assert_eq!(request.wait_seconds, 0);
        assert!(request.validate().is_ok());

        assert!(
            decode_json::<ClaimRequest>(
                br#"{"instance_id":"instance-1","generation":1,"limit":20,"wait_seconds":null}"#,
            )
            .is_err()
        );
        assert!(
            decode_json::<ClaimRequest>(
                br#"{"instance_id":"instance-1","generation":1,"limit":20,"extra":true}"#,
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
            decode_json::<DeliveryEventRequest>(
                br#"{
                    "event_id":"event-1",
                    "attempt_id":"attempt-1",
                    "generation":1,
                    "occurred_at":"2026-01-01T00:00:00Z",
                    "state":"route-unavailable",
                    "mailbox_item_id":"not-a-wire-field"
                }"#,
            )
            .is_err()
        );
    }
}
