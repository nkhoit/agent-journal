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

fn validate_identifier(field: &'static str, value: &str) -> Result<(), WireValidationError> {
    domain::validate_identifier(field, value)?;
    Ok(())
}

fn validate_optional_identifier(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), WireValidationError> {
    if let Some(value) = value {
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
pub struct AdapterProvisionRequest {
    pub principal_id: String,
    pub adapter_id: String,
}

impl AdapterProvisionRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("principal_id", &self.principal_id)?;
        validate_identifier("adapter_id", &self.adapter_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterProvisionResponse {
    pub adapter_id: String,
    pub principal_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterRegisterRequest {
    pub instance_id: String,
}

impl AdapterRegisterRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("instance_id", &self.instance_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterHeartbeatRequest {
    pub instance_id: String,
    pub generation: i64,
}

impl AdapterHeartbeatRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("instance_id", &self.instance_id)?;
        domain::validate_generation(self.generation)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterReplaceRequest {
    pub expected_generation: i64,
    pub new_instance_id: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub reason: Option<String>,
}

impl AdapterReplaceRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        domain::validate_generation(self.expected_generation)?;
        validate_identifier("new_instance_id", &self.new_instance_id)?;
        if let Some(reason) = &self.reason {
            validate_chars("reason", reason, 0, 512)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AdapterStatus {
    Active,
    Draining,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRegistration {
    pub adapter_id: String,
    pub principal_id: String,
    pub instance_id: String,
    pub generation: i64,
    pub status: AdapterStatus,
    pub lease_expires_at: String,
    pub heartbeat_after_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Adapter {
    #[serde(flatten)]
    pub registration: AdapterRegistration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

pub type AdapterPage = Page<Adapter>;

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
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("instance_id", &self.instance_id)?;
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
    pub fn validate(&self) -> Result<(), WireValidationError> {
        domain::validate_generation(self.generation)?;
        if self.items.is_empty() || self.items.len() > domain::MAX_CLAIM_BATCH {
            return Err(domain::ValidationError::InvalidClaimLimit {
                max: domain::MAX_CLAIM_BATCH,
            }
            .into());
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub addressed_to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    pub state: domain::TelemetryState,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub detail: domain::TelemetryDetail,
}

impl DeliveryEventRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("event_id", &self.event_id)?;
        validate_identifier("attempt_id", &self.attempt_id)?;
        domain::validate_generation(self.generation)?;
        domain::validate_telemetry_detail(&self.detail)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryEventResponse {
    pub event_id: String,
    pub state: domain::TelemetryState,
    pub received_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliverySummary {
    pub mailbox_item_id: String,
    pub recipient: String,
    pub state: domain::DeliveryState,
    pub attempts: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

pub type DeliveryStatusPage = Page<DeliverySummary>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxStatus {
    pub principal_id: String,
    pub pending: u64,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub oldest_pending_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
}

pub type MailboxStatusPage = Page<MailboxStatus>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalCreateRequest {
    pub id: String,
    pub display_name: String,
}

impl PrincipalCreateRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("id", &self.id)?;
        validate_chars("display_name", &self.display_name, 1, 128)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpaceCreateRequest {
    pub id: String,
    pub name: String,
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
    DeliveryAdapter,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentTicketCreateRequest {
    pub principal_id: String,
    pub adapter_id: String,
    pub ttl_seconds: u64,
}

impl EnrollmentTicketCreateRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("principal_id", &self.principal_id)?;
        validate_identifier("adapter_id", &self.adapter_id)?;
        if !(1..=900).contains(&self.ttl_seconds) {
            return Err(WireValidationError::Range {
                field: "ttl_seconds",
                min: 1,
                max: 900,
            });
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneTimeEnrollmentTicket {
    pub ticket: String,
}

impl fmt::Debug for OneTimeEnrollmentTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OneTimeEnrollmentTicket")
            .field("ticket", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentTicketCreateResponse {
    pub principal_id: String,
    pub adapter_id: String,
    pub expires_at: String,
    pub enrollment_ticket: OneTimeEnrollmentTicket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentExchangeRequest {
    pub instance_id: String,
}

impl EnrollmentExchangeRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("instance_id", &self.instance_id)
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneTimeDeliveryAdapterSecret {
    pub credential_id: String,
    pub secret: String,
}

impl fmt::Debug for OneTimeDeliveryAdapterSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OneTimeDeliveryAdapterSecret")
            .field("credential_id", &self.credential_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentExchangeResponse {
    pub adapter_id: String,
    pub principal_id: String,
    pub instance_id: String,
    pub generation: i64,
    pub principal_client_secret: OneTimePrincipalClientSecret,
    pub delivery_adapter_secret: OneTimeDeliveryAdapterSecret,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequeueRequest {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub reason: Option<String>,
}

impl RequeueRequest {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        if let Some(reason) = &self.reason {
            validate_chars("reason", reason, 0, 512)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RequeueState {
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequeueResponse {
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub state: RequeueState,
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
pub struct AdapterPath {
    pub adapter_id: String,
}
impl AdapterPath {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("adapter_id", &self.adapter_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimPath {
    pub claim_id: String,
}
impl ClaimPath {
    pub fn validate(&self) -> Result<(), WireValidationError> {
        validate_identifier("claim_id", &self.claim_id)
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
pub struct DeliveryAdapterContext {
    pub principal_id: String,
    pub adapter_id: String,
    pub credential_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollmentContext {
    pub ticket_id: String,
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
        validate_optional_identifier("attention", self.attention.as_deref())
    }
}
