//! Generic adapter ports and the custody-before-injection fence.

mod client;
mod orchestration;
pub use client::DeliveryJournal;
pub use journal_domain::MAX_LONG_POLL_SECONDS;
pub use journal_domain::TelemetryState as OutcomeState;
pub use orchestration::*;

use std::{collections::BTreeMap, time::SystemTime};

use journal_domain::{
    Record, TelemetryDetail, TelemetryState, ValidationError, validate_claim_limit,
    validate_generation, validate_long_poll_seconds, validate_telemetry_detail,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoreError {
    #[error("journal temporarily unavailable")]
    JournalUnavailable,
    #[error("journal rejected operation (HTTP {0})")]
    JournalRejected(u16),
    #[error("registration fence is not active")]
    Fenced,
    #[error("invalid journal response")]
    InvalidResponse,
    #[error("route unavailable: {space}/{key}")]
    RouteUnavailable { space: String, key: String },
    #[error("host custody not confirmed")]
    CustodyNotConfirmed,
    #[error("runtime unavailable: {0}")]
    RuntimeUnavailable(String),
    #[error("runtime rejected delivery")]
    RuntimeRejected,
    #[error("invalid telemetry detail: {0}")]
    InvalidTelemetry(String),
    #[error("spool unavailable: {0}")]
    SpoolUnavailable(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("inconsistent spool item binding: {field}")]
    InconsistentSpoolItem { field: &'static str },
}

impl From<ValidationError> for CoreError {
    fn from(error: ValidationError) -> Self {
        Self::Validation(error.to_string())
    }
}

/// Minimum central protocol surface required by an adapter.
pub trait Journal {
    fn register(&self, request: RegisterRequest) -> CoreResult<Registration>;
    fn heartbeat(&self, request: HeartbeatRequest) -> CoreResult<Registration>;
    fn claim(&self, request: ClaimRequest) -> CoreResult<ClaimBatch>;
    fn commit_host_custody(&self, request: CustodyRequest) -> CoreResult<CustodyResult>;
    fn record_event(&self, mailbox_item_id: &str, request: EventRequest) -> CoreResult<()>;
}

/// Durable local host custody. Implementations must persist the complete item
/// before committing custody, and reject conflicting bindings rather than
/// overwriting history. Each transition is idempotent for one attempt.
pub trait Spool {
    fn put(&self, item: &SpoolItem) -> CoreResult<()>;
    fn get(&self, attempt_id: &str) -> CoreResult<SpoolItem>;
    /// Rebind only an untouched, unconfirmed attempt after an authenticated
    /// central LeaseExpired result for its exact old claim/item/attempt.
    /// The caller must obtain replacement from a fresh claim under the same
    /// credential, installation and generation. Timeouts and local clocks are
    /// not evidence. Only claim_id may change; persistence is atomic with the
    /// put fingerprint. Exact replay is allowed only while still unconfirmed.
    fn reconcile_expired_claim(
        &self,
        expired: &CustodyResult,
        replacement: &SpoolItem,
    ) -> CoreResult<()>;
    fn confirm_custody(
        &self,
        attempt_id: &str,
        claim_id: &str,
        instance_id: &str,
        generation: i64,
    ) -> CoreResult<()>;
    fn mark_injection_started(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
    ) -> CoreResult<()>;
    fn mark_injected(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
        receipt: &str,
    ) -> CoreResult<()>;
    fn mark_injection_failed(
        &self,
        attempt_id: &str,
        instance_id: &str,
        generation: i64,
        state: InjectionState,
        detail: &str,
    ) -> CoreResult<()>;
    fn recoverable(&self, now: SystemTime, limit: usize) -> CoreResult<Vec<SpoolItem>>;
}

/// Vendor-specific runtime. It receives a resolved local route, never central
/// credentials or an unresolved portable routing key in place of that route.
/// Success must mean runtime acceptance and return a non-secret reference of at
/// most 4,096 UTF-8 bytes, not a raw vendor response or a semantic completion.
pub trait Runtime {
    fn inject(&self, route: &Route, envelope: &Envelope, rendered: &str) -> CoreResult<String>;
}

pub trait RouteResolver {
    fn resolve(&self, space: &str, routing_key: &str) -> CoreResult<Route>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterRequest {
    pub instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub instance_id: String,
    pub generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registration {
    pub adapter_id: String,
    pub principal_id: String,
    pub instance_id: String,
    pub generation: i64,
    pub status: RegistrationStatus,
    pub lease_expires_at: String,
    pub heartbeat_after_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RegistrationStatus {
    Active,
    Draining,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRequest {
    pub instance_id: String,
    pub generation: i64,
    pub limit: usize,
    pub wait_seconds: u64,
}

impl ClaimRequest {
    pub fn validate(&self) -> CoreResult<()> {
        validate_generation(self.generation)?;
        validate_claim_limit(self.limit)?;
        validate_long_poll_seconds(self.wait_seconds)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimBatch {
    pub claim_id: String,
    pub state: ClaimState,
    pub lease_expires_at: String,
    pub items: Vec<ClaimItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyItem {
    pub mailbox_item_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyItemResult {
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub result: CustodyResultState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CustodyResultState {
    Committed,
    AlreadyCommitted,
    ClaimNotFound,
    StaleGeneration,
    AttemptMismatch,
    LeaseExpired,
    SuppressedRevoked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodyRequest {
    pub claim_id: String,
    pub generation: i64,
    pub items: Vec<CustodyItem>,
}

impl CustodyRequest {
    pub fn validate(&self) -> CoreResult<()> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyResult {
    pub claim_id: String,
    pub generation: i64,
    pub items: Vec<CustodyItemResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRequest {
    pub attempt_id: String,
    pub event_id: String,
    pub generation: i64,
    pub occurred_at: String,
    pub state: TelemetryState,
    pub detail: TelemetryDetail,
}

impl EventRequest {
    pub fn validate(&self) -> CoreResult<()> {
        validate_generation(self.generation)?;
        journal_domain::validate_identifier("event_id", &self.event_id)?;
        journal_domain::validate_identifier("attempt_id", &self.attempt_id)?;
        self.occurred_at
            .parse::<jiff::Timestamp>()
            .map_err(|_| CoreError::InvalidTelemetry("invalid timestamp".into()))?;
        validate_telemetry_detail(&self.detail)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InjectionState {
    Pending,
    InFlight,
    Accepted,
    RetryableFailure,
    RouteUnavailable,
    TerminalFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<Record>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_event: Option<EventRequest>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub event_sequence: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub runtime_failures: u64,
    pub mailbox_item_id: String,
    pub attempt_id: String,
    pub claim_id: String,
    pub instance_id: String,
    pub generation: i64,
    pub record_id: String,
    pub space_id: String,
    pub routing_key: Option<String>,
    pub envelope: Envelope,
    pub custody_confirmed: bool,
    pub injection_state: InjectionState,
    pub runtime_receipt: String,
    pub failure_detail: String,
    pub next_runtime_try_at: Option<SystemTime>,
}

impl SpoolItem {
    /// Accepted, route-unavailable, and terminal rows remain as tombstones and are
    /// never normal recovery candidates.
    pub fn ready_for_injection(&self) -> bool {
        self.custody_confirmed
            && matches!(
                self.injection_state,
                InjectionState::Pending
                    | InjectionState::InFlight
                    | InjectionState::RetryableFailure
            )
    }

    /// The duplicated indexed fields and the authenticated envelope must refer
    /// to the same mailbox item before a route can be selected. This prevents
    /// stale metadata from changing the destination of a recovered envelope.
    pub fn validate_binding(&self) -> CoreResult<()> {
        if let Some(event) = &self.pending_event {
            event.validate()?;
            let state = match self.injection_state {
                InjectionState::Accepted => TelemetryState::AdapterReportedRuntimeAccepted,
                InjectionState::RetryableFailure => TelemetryState::AdapterReportedRetryableFailure,
                InjectionState::RouteUnavailable => TelemetryState::RouteUnavailable,
                InjectionState::TerminalFailure => TelemetryState::AdapterReportedTerminalFailure,
                _ => {
                    return Err(CoreError::InconsistentSpoolItem {
                        field: "pending_event",
                    });
                }
            };
            if !self.custody_confirmed
                || self.event_sequence == 0
                || event.attempt_id != self.attempt_id
                || event.generation != self.generation
                || event.state != state
            {
                return Err(CoreError::InconsistentSpoolItem {
                    field: "pending_event",
                });
            }
        }
        if let Some(record) = &self.record {
            let reply = record
                .relations
                .iter()
                .find(|r| r.relation_type == journal_domain::RelationType::ReplyTo)
                .map(|r| r.record_id.clone());
            if record.id != self.envelope.record_id
                || record.space_id != self.envelope.space_id
                || record.author != self.envelope.from_principal
                || record.run_id != self.envelope.source_run
                || record.routing_key != self.envelope.routing_key
                || record.content != self.envelope.body
                || reply != self.envelope.reply_to
                || !record.attention.contains(&self.envelope.addressed_to)
            {
                return Err(CoreError::InconsistentSpoolItem { field: "record" });
            }
        }
        if self.mailbox_item_id != self.envelope.mailbox_item_id {
            return Err(CoreError::InconsistentSpoolItem {
                field: "mailbox_item_id",
            });
        }
        if self.attempt_id != self.envelope.attempt_id {
            return Err(CoreError::InconsistentSpoolItem {
                field: "attempt_id",
            });
        }
        if self.record_id != self.envelope.record_id {
            return Err(CoreError::InconsistentSpoolItem { field: "record_id" });
        }
        if self.space_id != self.envelope.space_id {
            return Err(CoreError::InconsistentSpoolItem { field: "space_id" });
        }
        if self.routing_key != self.envelope.routing_key {
            return Err(CoreError::InconsistentSpoolItem {
                field: "routing_key",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
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
pub struct Route {
    pub key: String,
    pub runtime_target: String,
    pub enabled: bool,
}

pub type StaticRoutes = BTreeMap<String, Route>;

impl RouteResolver for StaticRoutes {
    fn resolve(&self, space: &str, routing_key: &str) -> CoreResult<Route> {
        let key = if routing_key.is_empty() {
            "default"
        } else {
            routing_key
        };
        match self.get(&format!("{space}/{key}")) {
            Some(route)
                if route.enabled && !route.runtime_target.is_empty() && route.key == key =>
            {
                Ok(route.clone())
            }
            _ => Err(CoreError::RouteUnavailable {
                space: space.to_owned(),
                key: key.to_owned(),
            }),
        }
    }
}

/// Resolve the private route before giving the authenticated envelope to the
/// runtime. The runtime is never called before host custody is confirmed.
pub fn inject_resolved_route<R, T>(
    resolver: &R,
    runtime: &T,
    item: &SpoolItem,
) -> CoreResult<String>
where
    R: RouteResolver,
    T: Runtime,
{
    if !item.ready_for_injection() {
        return Err(CoreError::CustodyNotConfirmed);
    }
    item.validate_binding()?;
    if item.routing_key.as_deref() == Some("") {
        return Err(CoreError::RouteUnavailable {
            space: item.space_id.clone(),
            key: String::new(),
        });
    }
    let route = resolver.resolve(
        &item.envelope.space_id,
        item.envelope.routing_key.as_deref().unwrap_or_default(),
    )?;
    runtime.inject(&route, &item.envelope, &item.envelope.render())
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    fn item(custody_confirmed: bool, injection_state: InjectionState) -> SpoolItem {
        SpoolItem {
            record: None,
            pending_event: None,
            event_sequence: 0,
            runtime_failures: 0,
            mailbox_item_id: "item-1".into(),
            attempt_id: "attempt-1".into(),
            claim_id: "claim-1".into(),
            instance_id: "instance-1".into(),
            generation: 1,
            record_id: "record-1".into(),
            space_id: "space".into(),
            routing_key: Some("project".into()),
            envelope: Envelope {
                record_id: "record-1".into(),
                mailbox_item_id: "item-1".into(),
                attempt_id: "attempt-1".into(),
                space_id: "space".into(),
                from_principal: "alpha".into(),
                source_run: None,
                reply_to: None,
                addressed_to: "beta".into(),
                routing_key: Some("project".into()),
                body: "safe body".into(),
            },
            custody_confirmed,
            injection_state,
            runtime_receipt: String::new(),
            failure_detail: String::new(),
            next_runtime_try_at: None,
        }
    }

    #[test]
    fn recovery_fence_requires_custody_and_recoverable_state() {
        let mut spool_item = item(false, InjectionState::Pending);
        assert!(!spool_item.ready_for_injection());
        spool_item.custody_confirmed = true;
        assert!(spool_item.ready_for_injection());
        for state in [
            InjectionState::Accepted,
            InjectionState::RouteUnavailable,
            InjectionState::TerminalFailure,
        ] {
            spool_item.injection_state = state;
            assert!(!spool_item.ready_for_injection());
        }
    }

    #[test]
    fn event_request_validates_serialized_detail_limit() {
        let detail = [
            ("message".into(), "界".repeat(1024)),
            ("padding".into(), "a".repeat(1024)),
        ]
        .into_iter()
        .collect();
        let request = EventRequest {
            attempt_id: "attempt-1".into(),
            event_id: "event-1".into(),
            generation: 1,
            occurred_at: "2023-11-14T22:13:20Z".into(),
            state: TelemetryState::AdapterReportedRuntimeAccepted,
            detail,
        };
        assert!(request.validate().is_err());
    }

    #[derive(Default)]
    struct RecordingRuntime {
        route: RefCell<Option<Route>>,
        envelope: RefCell<Option<Envelope>>,
    }

    impl Runtime for RecordingRuntime {
        fn inject(
            &self,
            route: &Route,
            envelope: &Envelope,
            _rendered: &str,
        ) -> CoreResult<String> {
            *self.route.borrow_mut() = Some(route.clone());
            *self.envelope.borrow_mut() = Some(envelope.clone());
            Ok("runtime-receipt".into())
        }
    }

    fn routes() -> StaticRoutes {
        [(
            "space/project".into(),
            Route {
                key: "project".into(),
                runtime_target: "local-target".into(),
                enabled: true,
            },
        )]
        .into_iter()
        .collect()
    }

    #[test]
    fn unknown_key_does_not_fall_back() {
        let mut routes = StaticRoutes::new();
        routes.insert(
            "space/default".into(),
            Route {
                key: "default".into(),
                runtime_target: "local-default".into(),
                enabled: true,
            },
        );
        assert!(matches!(
            routes.resolve("space", "typo"),
            Err(CoreError::RouteUnavailable { .. })
        ));
    }

    #[test]
    fn missing_key_uses_default_only_when_empty() {
        let mut routes = StaticRoutes::new();
        routes.insert(
            "space/default".into(),
            Route {
                key: "default".into(),
                runtime_target: "local-default".into(),
                enabled: true,
            },
        );
        assert_eq!(
            routes.resolve("space", "").expect("default").runtime_target,
            "local-default"
        );
    }

    #[test]
    fn resolved_route_and_exact_envelope_reach_runtime() {
        let runtime = RecordingRuntime::default();
        let spool_item = item(true, InjectionState::Pending);
        assert_eq!(
            inject_resolved_route(&routes(), &runtime, &spool_item).expect("inject"),
            "runtime-receipt"
        );
        assert_eq!(
            runtime
                .route
                .borrow()
                .as_ref()
                .expect("route")
                .runtime_target,
            "local-target"
        );
        assert_eq!(
            runtime.envelope.borrow().as_ref(),
            Some(&spool_item.envelope)
        );
    }

    #[test]
    fn unavailable_route_does_not_call_runtime() {
        let runtime = RecordingRuntime::default();
        let mut spool_item = item(true, InjectionState::Pending);
        spool_item.routing_key = Some("missing".into());
        spool_item.envelope.routing_key = Some("missing".into());
        assert!(matches!(
            inject_resolved_route(&StaticRoutes::new(), &runtime, &spool_item),
            Err(CoreError::RouteUnavailable { .. })
        ));
        assert!(runtime.route.borrow().is_none());
    }

    #[test]
    fn custody_is_required_before_runtime_call() {
        let runtime = RecordingRuntime::default();
        let spool_item = item(false, InjectionState::Pending);
        assert_eq!(
            inject_resolved_route(&routes(), &runtime, &spool_item),
            Err(CoreError::CustodyNotConfirmed)
        );
        assert!(runtime.route.borrow().is_none());
    }

    #[test]
    fn recovered_item_requires_consistent_indexed_bindings() {
        let runtime = RecordingRuntime::default();
        let mut spool_item = item(true, InjectionState::Pending);
        spool_item.envelope.record_id = "different-record".into();
        assert_eq!(
            inject_resolved_route(&routes(), &runtime, &spool_item),
            Err(CoreError::InconsistentSpoolItem { field: "record_id" })
        );
        assert!(runtime.route.borrow().is_none());
    }

    #[test]
    fn route_registration_statuses_and_claim_states_match_wire_values() {
        assert_eq!(
            serde_json::to_string(&RegistrationStatus::Draining).expect("status"),
            "\"draining\""
        );
        assert_eq!(
            serde_json::to_string(&ClaimState::Cancelled).expect("state"),
            "\"cancelled\""
        );
    }

    struct FailingSpool(SpoolItem);
    impl Spool for FailingSpool {
        fn put(&self, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected put")
        }
        fn get(&self, _: &str) -> CoreResult<SpoolItem> {
            Ok(self.0.clone())
        }
        fn reconcile_expired_claim(&self, _: &CustodyResult, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected reconciliation")
        }
        fn confirm_custody(&self, _: &str, _: &str, _: &str, _: i64) -> CoreResult<()> {
            Err(CoreError::SpoolUnavailable(
                "injected confirmation failure".into(),
            ))
        }
        fn mark_injection_started(&self, _: &str, _: &str, _: i64) -> CoreResult<()> {
            panic!("injection before durable confirmation")
        }
        fn mark_injected(&self, _: &str, _: &str, _: i64, _: &str) -> CoreResult<()> {
            panic!("unexpected result")
        }
        fn mark_injection_failed(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: InjectionState,
            _: &str,
        ) -> CoreResult<()> {
            panic!("unexpected failure")
        }
        fn recoverable(&self, _: SystemTime, _: usize) -> CoreResult<Vec<SpoolItem>> {
            Ok(vec![self.0.clone()])
        }
    }
    impl AdapterSpool for FailingSpool {
        fn check_capacity(&self, _: u64, _: u64) -> CoreResult<()> {
            panic!("unexpected claim")
        }
        fn find(&self, _: &str) -> CoreResult<Option<SpoolItem>> {
            Ok(Some(self.0.clone()))
        }
        fn work_after(&self, _: SystemTime, _: Option<&str>) -> CoreResult<Option<SpoolItem>> {
            Ok(Some(self.0.clone()))
        }
        fn finish(&self, _: &SpoolItem, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected result")
        }
        fn acknowledge_event(&self, _: &str, _: &EventRequest) -> CoreResult<()> {
            panic!("unexpected event")
        }
        fn suppress(&self, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected suppression")
        }
        fn backoff(&self) -> CoreResult<Backoff> {
            Ok(Backoff::default())
        }
        fn set_backoff(&self, _: &Backoff) -> CoreResult<()> {
            panic!("must propagate spool error")
        }
    }
    struct CustodyJournal;
    impl Journal for CustodyJournal {
        fn register(&self, _: RegisterRequest) -> CoreResult<Registration> {
            Ok(Registration {
                adapter_id: "adapter".into(),
                principal_id: "beta".into(),
                instance_id: "instance-1".into(),
                generation: 1,
                status: RegistrationStatus::Active,
                lease_expires_at: "2030-01-01T00:00:00Z".into(),
                heartbeat_after_seconds: 20,
            })
        }
        fn heartbeat(&self, _: HeartbeatRequest) -> CoreResult<Registration> {
            panic!("unexpected heartbeat")
        }
        fn claim(&self, _: ClaimRequest) -> CoreResult<ClaimBatch> {
            panic!("unexpected claim")
        }
        fn commit_host_custody(&self, request: CustodyRequest) -> CoreResult<CustodyResult> {
            Ok(CustodyResult {
                claim_id: request.claim_id,
                generation: request.generation,
                items: request
                    .items
                    .into_iter()
                    .map(|item| CustodyItemResult {
                        mailbox_item_id: item.mailbox_item_id,
                        attempt_id: item.attempt_id,
                        result: CustodyResultState::Committed,
                    })
                    .collect(),
            })
        }
        fn record_event(&self, _: &str, _: EventRequest) -> CoreResult<()> {
            panic!("unexpected event")
        }
    }
    struct ForbiddenResolver;
    impl RouteResolver for ForbiddenResolver {
        fn resolve(&self, _: &str, _: &str) -> CoreResult<Route> {
            panic!("route before custody")
        }
    }

    #[test]
    fn confirmation_storage_failure_stops_before_routing_or_runtime() {
        let spool = FailingSpool(item(false, InjectionState::Pending));
        let runtime = RecordingRuntime::default();
        let mut adapter = Adapter::new(
            &CustodyJournal,
            &spool,
            &ForbiddenResolver,
            &runtime,
            &SystemClock,
            "instance-1".into(),
        )
        .unwrap();
        assert!(matches!(
            adapter.tick(),
            Err(CoreError::SpoolUnavailable(_))
        ));
        assert!(runtime.route.borrow().is_none());
    }

    #[test]
    fn renderer_matches_normative_envelope_fixture() {
        let envelope = Envelope {
            record_id: "00000000-0000-7000-8000-000000000001".into(),
            mailbox_item_id: "item-example".into(),
            attempt_id: "attempt-example".into(),
            space_id: "example-space".into(),
            from_principal: "agent-source".into(),
            source_run: Some("example-run".into()),
            reply_to: Some("00000000-0000-7000-8000-000000000000".into()),
            addressed_to: "agent-destination".into(),
            routing_key: Some("default".into()),
            body: "Example body is inert test data.".into(),
        };
        let fixture = include_str!("../../../conformance/adapter/expected-envelope.txt")
            .replace("\r\n", "\n");
        let (_, expected) = fixture.split_once("\n\n").unwrap();
        assert_eq!(envelope.render(), expected);
    }

    struct RecordingJournal {
        claims: RefCell<Vec<ClaimRequest>>,
    }

    impl RecordingJournal {
        fn registration() -> Registration {
            Registration {
                adapter_id: "adapter".into(),
                principal_id: "beta".into(),
                instance_id: "instance-1".into(),
                generation: 1,
                status: RegistrationStatus::Active,
                lease_expires_at: "2030-01-01T00:00:00Z".into(),
                heartbeat_after_seconds: 20,
            }
        }
    }

    impl Journal for RecordingJournal {
        fn register(&self, _: RegisterRequest) -> CoreResult<Registration> {
            Ok(Self::registration())
        }
        fn heartbeat(&self, _: HeartbeatRequest) -> CoreResult<Registration> {
            Ok(Self::registration())
        }
        fn claim(&self, request: ClaimRequest) -> CoreResult<ClaimBatch> {
            self.claims.borrow_mut().push(request);
            Ok(ClaimBatch {
                claim_id: "claim-1".into(),
                state: ClaimState::Active,
                lease_expires_at: "2030-01-01T00:00:00Z".into(),
                items: Vec::new(),
            })
        }
        fn commit_host_custody(&self, _: CustodyRequest) -> CoreResult<CustodyResult> {
            panic!("unexpected custody commit")
        }
        fn record_event(&self, _: &str, _: EventRequest) -> CoreResult<()> {
            panic!("unexpected event")
        }
    }

    struct EmptySpool;

    impl Spool for EmptySpool {
        fn put(&self, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected put")
        }
        fn get(&self, _: &str) -> CoreResult<SpoolItem> {
            panic!("unexpected get")
        }
        fn reconcile_expired_claim(&self, _: &CustodyResult, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected reconcile")
        }
        fn confirm_custody(&self, _: &str, _: &str, _: &str, _: i64) -> CoreResult<()> {
            panic!("unexpected confirm")
        }
        fn mark_injection_started(&self, _: &str, _: &str, _: i64) -> CoreResult<()> {
            panic!("unexpected mark")
        }
        fn mark_injected(&self, _: &str, _: &str, _: i64, _: &str) -> CoreResult<()> {
            panic!("unexpected mark")
        }
        fn mark_injection_failed(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: InjectionState,
            _: &str,
        ) -> CoreResult<()> {
            panic!("unexpected mark")
        }
        fn recoverable(&self, _: SystemTime, _: usize) -> CoreResult<Vec<SpoolItem>> {
            Ok(Vec::new())
        }
    }

    impl AdapterSpool for EmptySpool {
        fn check_capacity(&self, _: u64, _: u64) -> CoreResult<()> {
            Ok(())
        }
        fn find(&self, _: &str) -> CoreResult<Option<SpoolItem>> {
            Ok(None)
        }
        fn work_after(&self, _: SystemTime, _: Option<&str>) -> CoreResult<Option<SpoolItem>> {
            Ok(None)
        }
        fn finish(&self, _: &SpoolItem, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected finish")
        }
        fn acknowledge_event(&self, _: &str, _: &EventRequest) -> CoreResult<()> {
            panic!("unexpected event")
        }
        fn suppress(&self, _: &SpoolItem) -> CoreResult<()> {
            panic!("unexpected suppression")
        }
        fn backoff(&self) -> CoreResult<Backoff> {
            Ok(Backoff::default())
        }
        fn set_backoff(&self, _: &Backoff) -> CoreResult<()> {
            Ok(())
        }
    }

    #[test]
    fn claim_carries_configured_long_poll_wait() {
        let journal = RecordingJournal {
            claims: RefCell::new(Vec::new()),
        };
        let spool = EmptySpool;
        let runtime = RecordingRuntime::default();
        let mut adapter = Adapter::new(
            &journal,
            &spool,
            &ForbiddenResolver,
            &runtime,
            &SystemClock,
            "instance-1".into(),
        )
        .unwrap()
        .with_wait_seconds(25)
        .unwrap();
        assert!(matches!(adapter.tick(), Ok(Progress::Idle)));
        let claims = journal.claims.borrow();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].wait_seconds, 25);
    }

    #[test]
    fn claim_defaults_to_immediate_return() {
        let journal = RecordingJournal {
            claims: RefCell::new(Vec::new()),
        };
        let spool = EmptySpool;
        let runtime = RecordingRuntime::default();
        let mut adapter = Adapter::new(
            &journal,
            &spool,
            &ForbiddenResolver,
            &runtime,
            &SystemClock,
            "instance-1".into(),
        )
        .unwrap();
        assert!(matches!(adapter.tick(), Ok(Progress::Idle)));
        assert_eq!(journal.claims.borrow()[0].wait_seconds, 0);
    }

    #[test]
    fn wait_seconds_rejects_above_server_bound() {
        let journal = RecordingJournal {
            claims: RefCell::new(Vec::new()),
        };
        let spool = EmptySpool;
        let runtime = RecordingRuntime::default();
        // `with_wait_seconds` consumes the adapter, so build one per case.
        let adapter = Adapter::new(
            &journal,
            &spool,
            &ForbiddenResolver,
            &runtime,
            &SystemClock,
            "instance-1".into(),
        )
        .unwrap();
        assert!(adapter.with_wait_seconds(MAX_LONG_POLL_SECONDS).is_ok());
        let adapter = Adapter::new(
            &journal,
            &spool,
            &ForbiddenResolver,
            &runtime,
            &SystemClock,
            "instance-1".into(),
        )
        .unwrap();
        assert!(
            adapter
                .with_wait_seconds(MAX_LONG_POLL_SECONDS + 1)
                .is_err()
        );
    }
}
