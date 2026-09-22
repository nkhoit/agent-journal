use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de, de::Visitor};
use thiserror::Error;

use crate::domain;

pub type AppendRecordRequest = domain::RecordInput;
pub type AppendRecordResponse = domain::AppendResult;
pub type PrincipalPage = Page<domain::Principal>;
pub type SpacePage = Page<domain::Space>;
pub type RecordPage = Page<domain::Record>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationalMetrics {
    pub sampled_at: String,
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub unacknowledged_inbox_count: u64,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub oldest_unacknowledged_at: Option<String>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub last_backup_at: Option<String>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub last_verified_restore_at: Option<String>,
}

#[derive(Debug, Error)]
pub enum WireValidationError {
    #[error(transparent)]
    Domain(#[from] domain::ValidationError),
    #[error("{field}: must be {min}..={max} characters")]
    CharacterLength {
        field: &'static str,
        min: usize,
        max: usize,
    },
    #[error("{field}: must be between {min} and {max}")]
    Range {
        field: &'static str,
        min: u64,
        max: u64,
    },
    #[error("{field}: must be an RFC 3339 timestamp")]
    InvalidDateTime { field: &'static str },
    #[error("{field}: must be a lowercase UUIDv7")]
    InvalidUuidV7 { field: &'static str },
}

fn validate_chars(
    field: &'static str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), WireValidationError> {
    let length = value.chars().count();
    if !(min..=max).contains(&length) {
        return Err(WireValidationError::CharacterLength { field, min, max });
    }
    Ok(())
}

fn validate_rfc3339(field: &'static str, value: &str) -> Result<(), WireValidationError> {
    value
        .parse::<jiff::Timestamp>()
        .map(|_| ())
        .map_err(|_| WireValidationError::InvalidDateTime { field })
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), WireValidationError> {
    domain::validate_identifier(field, value)?;
    Ok(())
}

fn validate_uuid_v7(field: &'static str, value: &str) -> Result<(), WireValidationError> {
    let bytes = value.as_bytes();
    let valid = bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes[14] == b'7'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
        && bytes.iter().enumerate().all(|(index, byte)| {
            [8, 13, 18, 23].contains(&index)
                || byte.is_ascii_digit()
                || (b'a'..=b'f').contains(byte)
        });
    if valid {
        Ok(())
    } else {
        Err(WireValidationError::InvalidUuidV7 { field })
    }
}

fn validate_optional_identifier(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), WireValidationError> {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        validate_identifier(field, value)?;
    }
    Ok(())
}

fn deserialize_optional_non_null_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionalNonNullString;

    impl<'de> Visitor<'de> for OptionalNonNullString {
        type Value = Option<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a non-null string")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom("null is not allowed"))
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            String::deserialize(deserializer).map(Some)
        }
    }

    deserializer.deserialize_option(OptionalNonNullString)
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ok,
    NotReady,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthCheckStatus {
    Ok,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Health {
    pub status: HealthStatus,
    pub version: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checks: BTreeMap<String, HealthCheckStatus>,
}

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
pub struct PageInfo {
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub space_id: String,
    pub principal_id: String,
    pub can_read: bool,
    pub can_append: bool,
    pub can_admin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Me {
    pub principal: domain::Principal,
    pub memberships: Vec<Membership>,
    pub limits: domain::Limits,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    #[serde(flatten)]
    pub record: domain::Record,
    pub score: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SearchOrder {
    Rank,
    Seq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SearchConsistency {
    BestEffort,
    Deterministic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchPage {
    pub items: Vec<SearchResult>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub next_cursor: Option<String>,
    pub order: SearchOrder,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<SearchConsistency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptSummary {
    pub inbox_item_id: String,
    pub recipient: String,
    pub state: ReceiptState,
    pub created_at: String,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub acknowledged_at: Option<String>,
}

pub type ReceiptStatusPage = Page<ReceiptSummary>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReceiptState {
    Unacknowledged,
    Acknowledged,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InboxState {
    #[default]
    Unacknowledged,
    Acknowledged,
    All,
}

impl InboxState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unacknowledged => "unacknowledged",
            Self::Acknowledged => "acknowledged",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InboxQuery {
    pub state: InboxState,
    pub page: PageQuery,
    /// Long-poll hold in seconds applied when the first page is empty.
    /// 0 returns immediately; values above MAX_INBOX_WAIT_SECONDS are rejected.
    pub wait_seconds: u64,
}

/// Upper bound for `InboxQuery::wait_seconds`. A held request never outlives
/// this many seconds; clients needing longer waits reissue the request.
pub const MAX_INBOX_WAIT_SECONDS: u64 = 30;

impl InboxQuery {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        self.page.validate()?;
        if self.wait_seconds > MAX_INBOX_WAIT_SECONDS {
            return Err(WireValidationError::Range {
                field: "wait_seconds",
                min: 0,
                max: MAX_INBOX_WAIT_SECONDS,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxItem {
    pub inbox_item_id: String,
    pub recipient: String,
    pub seq: i64,
    pub created_at: String,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub acknowledged_at: Option<String>,
    pub record: domain::Record,
}

pub type InboxPage = Page<InboxItem>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalCreateRequest {
    pub handle: String,
    pub display_name: String,
}

impl PrincipalCreateRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("handle", &self.handle)?;
        validate_chars("display_name", &self.display_name, 1, 128)
    }
}

pub type RegistrationRequest = PrincipalCreateRequest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationReceipt {
    pub principal: domain::Principal,
    pub credential_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalRecoveryRequest {
    pub principal_id: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub reason: Option<String>,
}

impl PrincipalRecoveryRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_uuid_v7("principal_id", &self.principal_id)?;
        if let Some(reason) = &self.reason {
            validate_chars("reason", reason, 0, 512)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalRecoveryResponse {
    pub principal: domain::Principal,
    pub replacement_secret: OneTimeReplacementSecret,
}

/// A principal-client may update only its own mutable descriptor. A retired
/// current handle becomes a permanent alias; the immutable principal ID and
/// every authority-bearing binding remain unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileUpdateRequest {
    pub handle: String,
    pub display_name: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<String>,
    pub expected_profile_revision: i64,
}

impl ProfileUpdateRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("handle", &self.handle)?;
        validate_chars("display_name", &self.display_name, 1, 128)?;
        if let Some(description) = &self.description {
            validate_chars("description", description, 0, 512)?;
        }
        if self.expected_profile_revision < 1 {
            return Err(WireValidationError::Range {
                field: "expected_profile_revision",
                min: 1,
                max: i64::MAX as u64,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpaceCreateRequest {
    pub id: String,
    pub name: String,
    pub access: domain::SpaceAccess,
}

impl SpaceCreateRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("id", &self.id)?;
        validate_chars("name", &self.name, 1, 128)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRequest {
    pub space_id: String,
    pub principal_id: String,
    #[serde(default)]
    pub can_read: bool,
    #[serde(default)]
    pub can_append: bool,
    #[serde(default)]
    pub can_admin: bool,
}

impl MembershipRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("space_id", &self.space_id)?;
        validate_identifier("principal_id", &self.principal_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialClass {
    PrincipalClient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRotateRequest {
    pub credential_id: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub reason: Option<String>,
}

impl CredentialRotateRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("credential_id", &self.credential_id)?;
        if let Some(reason) = &self.reason {
            validate_chars("reason", reason, 0, 512)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialMetadata {
    pub credential_id: String,
    pub principal_id: String,
    pub class: CredentialClass,
    pub rotated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_credential_id: Option<String>,
}

pub type CredentialRevokeRequest = CredentialRotateRequest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRotationResponse {
    pub metadata: CredentialMetadata,
    pub replacement_secret: OneTimeReplacementSecret,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneTimeReplacementSecret {
    pub credential_id: String,
    pub secret: String,
}

impl fmt::Debug for OneTimeReplacementSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OneTimeReplacementSecret")
            .field("credential_id", &self.credential_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneTimePrincipalClientSecret {
    pub credential_id: String,
    pub secret: String,
}

impl fmt::Debug for OneTimePrincipalClientSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OneTimePrincipalClientSecret")
            .field("credential_id", &self.credential_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpacePath {
    pub space: String,
}

impl SpacePath {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("space", &self.space)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalPath {
    pub principal: String,
}
impl PrincipalPath {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("principal", &self.principal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPath {
    pub record_id: String,
}
impl RecordPath {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("record_id", &self.record_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxItemPath {
    pub item_id: String,
}
impl MailboxItemPath {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("item_id", &self.item_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendHeaders {
    pub idempotency_key: String,
}

impl AppendHeaders {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_chars("Idempotency-Key", &self.idempotency_key, 1, 255)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHeaders {
    pub request_id: String,
    pub retry_after_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalContext {
    pub principal_id: String,
    pub credential_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminContext {
    pub peer_identity: String,
}

pub const DEFAULT_PAGE_SIZE: usize = 50;
pub const MAX_CURSOR_CHARS: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PageQuery {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

impl PageQuery {
    pub fn new(cursor: Option<String>, limit: Option<usize>) -> Self {
        Self { cursor, limit }
    }

    pub fn effective_limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_PAGE_SIZE)
    }

    pub fn validate(&self) -> Result<(), WireValidationError> {
        if let Some(cursor) = &self.cursor {
            validate_chars("cursor", cursor, 0, MAX_CURSOR_CHARS)?;
        }
        domain::validate_page_size(self.effective_limit())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPrincipalsQuery {
    pub space: String,
    pub page: PageQuery,
}
impl ListPrincipalsQuery {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("space", &self.space)?;
        self.page.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListRecordsQuery {
    pub page: PageQuery,
    pub after_seq: Option<u64>,
    pub author: Option<String>,
    pub attention: Option<String>,
    pub kind: Option<String>,
    pub relation: Option<domain::RelationType>,
}

impl ListRecordsQuery {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        self.page.validate()?;
        validate_optional_identifier("author", self.author.as_deref())?;
        validate_optional_identifier("attention", self.attention.as_deref())?;
        validate_optional_identifier("kind", self.kind.as_deref())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRecordsQuery {
    pub q: String,
    pub page: PageQuery,
    pub author: Option<String>,
    pub attention: Option<String>,
    pub since: Option<String>,
    pub order: SearchOrder,
}

impl SearchRecordsQuery {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_chars("q", &self.q, 1, 512)?;
        self.page.validate()?;
        validate_optional_identifier("author", self.author.as_deref())?;
        validate_optional_identifier("attention", self.attention.as_deref())?;
        if let Some(since) = &self.since {
            validate_rfc3339("since", since)?;
        }
        Ok(())
    }
}
