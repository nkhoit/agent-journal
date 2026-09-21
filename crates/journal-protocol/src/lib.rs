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
}
