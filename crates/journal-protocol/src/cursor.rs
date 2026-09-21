use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{MAX_CURSOR_CHARS, decode_json};

const CURSOR_VERSION: u8 = 1;
const MIN_KEY_BYTES: usize = 32;
const HMAC_BYTES: usize = 32;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CursorRoute {
    Principals,
    Spaces,
    Records,
    Search,
    RecordThread,
    RecordDeliveryStatus,
    Inbox,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CursorOrder {
    Identifier,
    Sequence,
    Rank,
    BoundedSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CursorPosition {
    Identifier { id: String },
    Sequence { sequence: u64, id: String },
    Rank { score_bits: u64, id: String },
    BoundedSequence { sequence: u64, upper_bound: u64 },
}

impl CursorPosition {
    fn matches(self_order: CursorOrder, position: &Self) -> bool {
        matches!(
            (self_order, position),
            (CursorOrder::Identifier, Self::Identifier { .. })
                | (CursorOrder::Sequence, Self::Sequence { .. })
                | (CursorOrder::Rank, Self::Rank { .. })
                | (CursorOrder::BoundedSequence, Self::BoundedSequence { .. })
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorScope {
    route: CursorRoute,
    filter_fingerprint: [u8; 32],
    order: CursorOrder,
}

impl CursorScope {
    pub fn new(route: CursorRoute, canonical_filters: &[u8], order: CursorOrder) -> Self {
        Self {
            route,
            filter_fingerprint: Sha256::digest(canonical_filters).into(),
            order,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CursorCodec {
    key: [u8; 32],
}

impl std::fmt::Debug for CursorCodec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CursorCodec")
            .field("key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CursorError {
    #[error("cursor key must contain at least 32 bytes")]
    KeyTooShort,
    #[error("cursor exceeds the 2048-character wire limit")]
    TooLong,
    #[error("cursor envelope is malformed")]
    Malformed,
    #[error("cursor contains invalid base64url data")]
    InvalidEncoding,
    #[error("cursor signature is invalid")]
    InvalidSignature,
    #[error("cursor payload is invalid")]
    InvalidPayload,
    #[error("cursor version is unsupported")]
    UnsupportedVersion,
    #[error("cursor belongs to another route")]
    WrongRoute,
    #[error("cursor belongs to another filter set")]
    WrongFilter,
    #[error("cursor belongs to another ordering mode")]
    WrongOrder,
    #[error("cursor position does not match its ordering mode")]
    PositionOrderMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorPayload {
    version: u8,
    route: CursorRoute,
    filter: String,
    order: CursorOrder,
    position: CursorPosition,
}

impl CursorCodec {
    pub fn new(key: &[u8]) -> Result<Self, CursorError> {
        if key.len() < MIN_KEY_BYTES {
            return Err(CursorError::KeyTooShort);
        }
        Ok(Self {
            key: Sha256::digest(key).into(),
        })
    }

    pub fn encode(
        &self,
        scope: &CursorScope,
        position: &CursorPosition,
    ) -> Result<String, CursorError> {
        if !CursorPosition::matches(scope.order, position) {
            return Err(CursorError::PositionOrderMismatch);
        }
        self.encode_payload(&CursorPayload {
            version: CURSOR_VERSION,
            route: scope.route,
            filter: URL_SAFE_NO_PAD.encode(scope.filter_fingerprint),
            order: scope.order,
            position: position.clone(),
        })
    }

    pub fn decode(&self, scope: &CursorScope, token: &str) -> Result<CursorPosition, CursorError> {
        if token.len() > MAX_CURSOR_CHARS {
            return Err(CursorError::TooLong);
        }
        let (payload_text, signature_text) = token.split_once('.').ok_or(CursorError::Malformed)?;
        if payload_text.is_empty() || signature_text.is_empty() || signature_text.contains('.') {
            return Err(CursorError::Malformed);
        }

        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_text)
            .map_err(|_| CursorError::InvalidEncoding)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature_text)
            .map_err(|_| CursorError::InvalidEncoding)?;
        if signature.len() != HMAC_BYTES {
            return Err(CursorError::InvalidEncoding);
        }

        let mut mac = self.mac();
        mac.update(&payload_bytes);
        mac.verify_slice(&signature)
            .map_err(|_| CursorError::InvalidSignature)?;

        let payload: CursorPayload =
            decode_json(&payload_bytes).map_err(|_| CursorError::InvalidPayload)?;
        if payload.version != CURSOR_VERSION {
            return Err(CursorError::UnsupportedVersion);
        }
        if payload.route != scope.route {
            return Err(CursorError::WrongRoute);
        }
        if payload.filter != URL_SAFE_NO_PAD.encode(scope.filter_fingerprint) {
            return Err(CursorError::WrongFilter);
        }
        if payload.order != scope.order {
            return Err(CursorError::WrongOrder);
        }
        if !CursorPosition::matches(payload.order, &payload.position) {
            return Err(CursorError::PositionOrderMismatch);
        }
        Ok(payload.position)
    }

    fn encode_payload(&self, payload: &CursorPayload) -> Result<String, CursorError> {
        let payload_bytes = serde_json::to_vec(payload).map_err(|_| CursorError::InvalidPayload)?;
        let mut mac = self.mac();
        mac.update(&payload_bytes);
        let signature = mac.finalize().into_bytes();
        let token = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload_bytes),
            URL_SAFE_NO_PAD.encode(signature)
        );
        if token.len() > MAX_CURSOR_CHARS {
            return Err(CursorError::TooLong);
        }
        Ok(token)
    }

    fn mac(&self) -> HmacSha256 {
        <HmacSha256 as Mac>::new_from_slice(&self.key)
            .expect("HMAC accepts keys of every non-empty length")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_authenticated_unknown_version() {
        let codec = CursorCodec::new(b"0123456789abcdef0123456789abcdef").expect("key");
        let scope = CursorScope::new(CursorRoute::Spaces, b"{}", CursorOrder::Identifier);
        let token = codec
            .encode_payload(&CursorPayload {
                version: CURSOR_VERSION + 1,
                route: scope.route,
                filter: URL_SAFE_NO_PAD.encode(scope.filter_fingerprint),
                order: scope.order,
                position: CursorPosition::Identifier {
                    id: "space-1".into(),
                },
            })
            .expect("token");
        assert_eq!(
            codec.decode(&scope, &token),
            Err(CursorError::UnsupportedVersion)
        );
    }
}
