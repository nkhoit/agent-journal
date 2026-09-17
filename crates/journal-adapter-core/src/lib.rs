//! Generic adapter ports and the custody-before-injection fence.

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
    #[error("route unavailable: {space}/{key}")]
    RouteUnavailable { space: String, key: String },
    #[error("host custody not confirmed")]
    CustodyNotConfirmed,
    #[error("runtime unavailable: {0}")]
    RuntimeUnavailable(String),
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
pub trait Runtime {
    fn inject(&self, route: &Route, envelope: &Envelope) -> CoreResult<String>;
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
        validate_telemetry_detail(&self.detail)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectionState {
    Pending,
    InFlight,
    Accepted,
    RetryableFailure,
    RouteUnavailable,
    TerminalFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolItem {
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
    /// Accepted and terminal rows remain as deduplication tombstones and are
    /// never normal recovery candidates.
    pub fn ready_for_injection(&self) -> bool {
        self.custody_confirmed
            && matches!(
                self.injection_state,
                InjectionState::Pending
                    | InjectionState::InFlight
                    | InjectionState::RetryableFailure
                    | InjectionState::RouteUnavailable
            )
    }

    /// The duplicated indexed fields and the authenticated envelope must refer
    /// to the same mailbox item before a route can be selected. This prevents
    /// stale metadata from changing the destination of a recovered envelope.
    pub fn validate_binding(&self) -> CoreResult<()> {
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
        if normalized_route_key(self.routing_key.as_deref())
            != normalized_route_key(self.envelope.routing_key.as_deref())
        {
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    let route = resolver.resolve(
        &item.envelope.space_id,
        item.envelope.routing_key.as_deref().unwrap_or_default(),
    )?;
    runtime.inject(&route, &item.envelope)
}

fn normalized_route_key(routing_key: Option<&str>) -> &str {
    routing_key
        .filter(|key| !key.is_empty())
        .unwrap_or("default")
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    fn item(custody_confirmed: bool, injection_state: InjectionState) -> SpoolItem {
        SpoolItem {
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
        for state in [InjectionState::Accepted, InjectionState::TerminalFailure] {
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
            occurred_at: "now".into(),
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
        fn inject(&self, route: &Route, envelope: &Envelope) -> CoreResult<String> {
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
}
