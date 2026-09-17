//! Durable spool contract. The concrete SQLite-backed spool is intentionally
//! pending; this crate makes the crash-recovery obligations explicit.

use std::time::SystemTime;

use journal_adapter_core::{CoreError, CoreResult, InjectionState, Spool, SpoolItem};

pub const REQUIRED_PERSISTED_FIELDS: [&str; 14] = [
    "mailbox_item_id",
    "attempt_id",
    "claim_id",
    "instance_id",
    "generation",
    "record_id",
    "space_id",
    "routing_key",
    "envelope",
    "custody_confirmed",
    "injection_state",
    "runtime_receipt",
    "failure_detail",
    "next_runtime_try_at",
];

pub const RECOVERABLE_INJECTION_STATES: [InjectionState; 4] = [
    InjectionState::Pending,
    InjectionState::InFlight,
    InjectionState::RetryableFailure,
    InjectionState::RouteUnavailable,
];

/// A store must fsync a complete item before the central custody commit and
/// make every transition idempotent for the same attempt/binding.
pub trait Store: Spool {
    fn open(&self) -> CoreResult<()>;
    fn close(&self) -> CoreResult<()>;
}

pub fn is_recoverable(item: &SpoolItem, now: SystemTime) -> bool {
    item.ready_for_injection()
        && item.validate_binding().is_ok()
        && item
            .next_runtime_try_at
            .map(|retry_at| retry_at <= now)
            .unwrap_or(true)
}

/// Explicit placeholder for the future local transactional implementation.
#[derive(Debug, Default)]
pub struct NotImplementedStore;

fn not_ready<T>() -> CoreResult<T> {
    Err(CoreError::SpoolUnavailable(
        "durable spool is not implemented".into(),
    ))
}

impl Spool for NotImplementedStore {
    fn put(&self, _item: &SpoolItem) -> CoreResult<()> {
        not_ready()
    }

    fn get(&self, _attempt_id: &str) -> CoreResult<SpoolItem> {
        not_ready()
    }

    fn confirm_custody(
        &self,
        _attempt_id: &str,
        _claim_id: &str,
        _instance_id: &str,
        _generation: i64,
    ) -> CoreResult<()> {
        not_ready()
    }

    fn mark_injection_started(
        &self,
        _attempt_id: &str,
        _instance_id: &str,
        _generation: i64,
    ) -> CoreResult<()> {
        not_ready()
    }

    fn mark_injected(
        &self,
        _attempt_id: &str,
        _instance_id: &str,
        _generation: i64,
        _receipt: &str,
    ) -> CoreResult<()> {
        not_ready()
    }

    fn mark_injection_failed(
        &self,
        _attempt_id: &str,
        _instance_id: &str,
        _generation: i64,
        _state: InjectionState,
        _detail: &str,
    ) -> CoreResult<()> {
        not_ready()
    }

    fn recoverable(&self, _now: SystemTime, _limit: usize) -> CoreResult<Vec<SpoolItem>> {
        not_ready()
    }
}

impl Store for NotImplementedStore {
    fn open(&self) -> CoreResult<()> {
        not_ready()
    }

    fn close(&self) -> CoreResult<()> {
        not_ready()
    }
}

#[cfg(test)]
mod tests {
    use journal_adapter_core::Envelope;

    use super::*;

    fn item(next_runtime_try_at: Option<SystemTime>) -> SpoolItem {
        SpoolItem {
            mailbox_item_id: "item-1".into(),
            attempt_id: "attempt-1".into(),
            claim_id: "claim-1".into(),
            instance_id: "instance-1".into(),
            generation: 1,
            record_id: "record-1".into(),
            space_id: "space".into(),
            routing_key: Some("default".into()),
            envelope: Envelope {
                record_id: "record-1".into(),
                mailbox_item_id: "item-1".into(),
                attempt_id: "attempt-1".into(),
                space_id: "space".into(),
                from_principal: "source".into(),
                source_run: None,
                reply_to: None,
                addressed_to: "destination".into(),
                routing_key: Some("default".into()),
                body: "body".into(),
            },
            custody_confirmed: true,
            injection_state: InjectionState::RetryableFailure,
            runtime_receipt: String::new(),
            failure_detail: String::new(),
            next_runtime_try_at,
        }
    }

    #[test]
    fn crash_recovery_contract_names_custody_and_attempt_state() {
        assert!(REQUIRED_PERSISTED_FIELDS.contains(&"attempt_id"));
        assert!(REQUIRED_PERSISTED_FIELDS.contains(&"custody_confirmed"));
        assert!(REQUIRED_PERSISTED_FIELDS.contains(&"injection_state"));
        assert!(REQUIRED_PERSISTED_FIELDS.contains(&"next_runtime_try_at"));
        assert_eq!(RECOVERABLE_INJECTION_STATES.len(), 4);
    }

    #[test]
    fn placeholder_never_claims_a_durable_store() {
        assert!(NotImplementedStore.open().is_err());
    }

    #[test]
    fn recovery_honors_the_persisted_retry_time() {
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
        assert!(is_recoverable(&item(Some(now)), now));
        assert!(!is_recoverable(
            &item(Some(now + std::time::Duration::from_secs(1))),
            now
        ));
    }
}
