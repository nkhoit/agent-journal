use journal_protocol::{domain::RecordInput, *};
use journal_service::{BootstrapError, BootstrapService, Clock, OsSecretSource};
use journal_storage_sqlite::Database;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime};

struct FakeClock(AtomicU64);
struct CounterSecrets(AtomicU64);
impl journal_service::SecretSource for CounterSecrets {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
        let next = self.0.fetch_add(1, Ordering::SeqCst).to_be_bytes();
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&next);
        }
        Ok(())
    }
}
impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
}
struct Fixture {
    db: Database,
    service: BootstrapService,
    clock: Arc<FakeClock>,
    client: String,
    delivery: String,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/delivery-tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "{}-{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let db = Database::open(&path).unwrap();
        let clock = Arc::new(FakeClock(AtomicU64::new(1_800_000_000)));
        let service = BootstrapService::with_sources(
            db.clone(),
            clock.clone(),
            Arc::new(CounterSecrets(AtomicU64::new(1))),
        );
        service
            .create_principal(&PrincipalCreateRequest {
                id: "reader".into(),
                display_name: "Reader".into(),
            })
            .unwrap();
        service
            .create_space(&SpaceCreateRequest {
                id: "space".into(),
                name: "Space".into(),
            })
            .unwrap();
        service
            .set_membership(&MembershipRequest {
                space_id: "space".into(),
                principal_id: "reader".into(),
                can_read: true,
                can_append: true,
                can_admin: false,
            })
            .unwrap();
        service
            .provision_adapter(&AdapterProvisionRequest {
                principal_id: "reader".into(),
                adapter_id: "adapter".into(),
            })
            .unwrap();
        let ticket = service
            .create_ticket(&EnrollmentTicketCreateRequest {
                principal_id: "reader".into(),
                adapter_id: "adapter".into(),
                ttl_seconds: 900,
            })
            .unwrap();
        let credentials = service
            .exchange(
                &ticket.enrollment_ticket.ticket,
                &EnrollmentExchangeRequest {
                    instance_id: "installation".into(),
                },
            )
            .unwrap();
        Self {
            db,
            service,
            clock,
            client: credentials.principal_client_secret.secret,
            delivery: credentials.delivery_adapter_secret.secret,
            path,
        }
    }
    fn append(&self, key: &str) {
        self.service
            .append_record(
                &self.client,
                "space",
                key,
                &RecordInput {
                    kind: "note".into(),
                    content: "private content".into(),
                    attention: vec!["reader".into()],
                    relations: vec![],
                    run_id: None,
                    routing_key: None,
                },
            )
            .unwrap();
    }
    fn request(&self) -> ClaimRequest {
        ClaimRequest {
            instance_id: "installation".into(),
            generation: 1,
            limit: 20,
            wait_seconds: 0,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

fn commit_request(claim: &ClaimResponse) -> CommitRequest {
    CommitRequest {
        generation: 1,
        items: claim
            .items
            .iter()
            .map(|i| CommitItem {
                mailbox_item_id: i.mailbox_item_id.clone(),
                attempt_id: i.attempt_id.clone(),
            })
            .collect(),
    }
}

fn event(item: &ClaimItem, id: &str, state: domain::TelemetryState) -> DeliveryEventRequest {
    DeliveryEventRequest {
        event_id: id.into(),
        attempt_id: item.attempt_id.clone(),
        generation: 1,
        occurred_at: "2027-01-15T08:00:00Z".into(),
        state,
        detail: Default::default(),
    }
}

#[test]
fn partial_custody_replay_expiry_and_exact_binding() {
    let f = Fixture::new();
    f.append("one");
    f.append("two");
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let mut input = commit_request(&claim);
    input.items[1].attempt_id = "wrong".into();
    let result = f
        .service
        .commit_custody(&f.delivery, &claim.claim_id, &input)
        .unwrap();
    assert_eq!(result.items[0].result, CommitItemResult::Committed);
    assert_eq!(result.items[1].result, CommitItemResult::AttemptMismatch);
    f.clock.0.fetch_add(90, Ordering::SeqCst);
    let result = f
        .service
        .commit_custody(&f.delivery, &claim.claim_id, &commit_request(&claim))
        .unwrap();
    assert_eq!(result.items[0].result, CommitItemResult::AlreadyCommitted);
    assert_eq!(result.items[1].result, CommitItemResult::LeaseExpired);
    let mut stale = commit_request(&claim);
    stale.generation = 2;
    assert_eq!(
        f.service
            .commit_custody(&f.delivery, &claim.claim_id, &stale)
            .unwrap()
            .items[0]
            .result,
        CommitItemResult::StaleGeneration
    );
    assert_eq!(
        f.service
            .commit_custody(&f.delivery, "unknown", &input)
            .unwrap()
            .items[0]
            .result,
        CommitItemResult::ClaimNotFound
    );
    assert!(
        f.service
            .commit_custody(&f.client, &claim.claim_id, &input)
            .is_err()
    );
}

#[test]
fn retryable_success_replay_and_requeue_keep_history_and_fence_old_attempts() {
    use domain::TelemetryState::*;
    let f = Fixture::new();
    f.append("one");
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let item = &claim.items[0];
    let retryable = event(item, "retryable", AdapterReportedRetryableFailure);
    assert!(matches!(
        f.service
            .record_delivery_event(&f.delivery, &item.mailbox_item_id, &retryable),
        Err(BootstrapError::Conflict)
    ));
    f.service
        .commit_custody(&f.delivery, &claim.claim_id, &commit_request(&claim))
        .unwrap();
    let first = f
        .service
        .record_delivery_event(&f.delivery, &item.mailbox_item_id, &retryable)
        .unwrap();
    let accepted = event(item, "accepted", AdapterReportedRuntimeAccepted);
    f.service
        .record_delivery_event(&f.delivery, &item.mailbox_item_id, &accepted)
        .unwrap();
    assert_eq!(
        f.service
            .record_delivery_event(&f.delivery, &item.mailbox_item_id, &retryable)
            .unwrap(),
        first
    );
    let mut conflict = retryable.clone();
    conflict.detail.insert("error".into(), "changed".into());
    assert!(matches!(
        f.service
            .record_delivery_event(&f.delivery, &item.mailbox_item_id, &conflict),
        Err(BootstrapError::Conflict)
    ));
    assert!(matches!(
        f.service.record_delivery_event(
            &f.delivery,
            &item.mailbox_item_id,
            &event(item, "downgrade", AdapterReportedTerminalFailure)
        ),
        Err(BootstrapError::Conflict)
    ));
    let next = f
        .service
        .requeue_mailbox_item(&item.mailbox_item_id, &RequeueRequest::default())
        .unwrap();
    assert_ne!(next.attempt_id, item.attempt_id);
    assert_eq!(
        f.service
            .record_delivery_event(&f.delivery, &item.mailbox_item_id, &retryable)
            .unwrap(),
        first
    );
    let status = f
        .service
        .delivery_status(&f.client, &item.record.id, &PageQuery::default())
        .unwrap();
    assert_eq!(status.items[0].state, domain::DeliveryState::Pending);
    assert_eq!(status.items[0].attempts, 2);
    assert_eq!(
        status.items[0].last_attempt_id.as_deref(),
        Some(next.attempt_id.as_str())
    );
    let conn = f.db.connect().unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM delivery_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
    // Historical telemetry must not prevent replacement of the live registration.
    f.service
        .replace_adapter(
            "adapter",
            &AdapterReplaceRequest {
                expected_generation: 1,
                new_instance_id: "replacement".into(),
                reason: None,
            },
        )
        .unwrap();
}

#[test]
fn requeue_and_custody_failpoints_roll_back() {
    let f = Fixture::new();
    f.append("one");
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let item = &claim.items[0];
    for boundary in ["custody-recorded", "custody-state"] {
        assert!(matches!(
            f.service.clone().with_failpoint(boundary).commit_custody(
                &f.delivery,
                &claim.claim_id,
                &commit_request(&claim)
            ),
            Err(BootstrapError::Injected)
        ));
        assert_eq!(
            f.db.connect()
                .unwrap()
                .query_row("SELECT count(*) FROM host_custody", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    f.service
        .commit_custody(&f.delivery, &claim.claim_id, &commit_request(&claim))
        .unwrap();
    for boundary in ["requeue-ordinal", "requeue-attempt", "requeue-state"] {
        assert!(matches!(
            f.service
                .clone()
                .with_failpoint(boundary)
                .requeue_mailbox_item(&item.mailbox_item_id, &RequeueRequest::default()),
            Err(BootstrapError::Injected)
        ));
        assert_eq!(
            f.db.connect()
                .unwrap()
                .query_row("SELECT count(*) FROM delivery_attempts", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
}
fn add_identity(f: &Fixture, id: &str) -> EnrollmentExchangeResponse {
    f.service
        .create_principal(&PrincipalCreateRequest {
            id: id.into(),
            display_name: id.into(),
        })
        .unwrap();
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: id.into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    f.service
        .provision_adapter(&AdapterProvisionRequest {
            principal_id: id.into(),
            adapter_id: id.into(),
        })
        .unwrap();
    let ticket = f
        .service
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: id.into(),
            adapter_id: id.into(),
            ttl_seconds: 900,
        })
        .unwrap();
    f.service
        .exchange(
            &ticket.enrollment_ticket.ticket,
            &EnrollmentExchangeRequest {
                instance_id: id.into(),
            },
        )
        .unwrap()
}

#[test]
fn status_authorization_pagination_and_current_acl_matrix() {
    let f = Fixture::new();
    let recipient = add_identity(&f, "recipient");
    let reader = add_identity(&f, "other-reader");
    let posted = f
        .service
        .append_record(
            &f.client,
            "space",
            "matrix",
            &RecordInput {
                kind: "note".into(),
                content: "status".into(),
                attention: vec!["reader".into(), "recipient".into()],
                relations: vec![],
                run_id: None,
                routing_key: None,
            },
        )
        .unwrap();
    let record = &posted.record.id;
    let page = f
        .service
        .delivery_status(&f.client, record, &PageQuery::new(None, Some(1)))
        .unwrap();
    assert_eq!(page.items.len(), 1);
    let cursor = page.next_cursor.unwrap();
    let second = f
        .service
        .delivery_status(
            &f.client,
            record,
            &PageQuery::new(Some(cursor.clone()), Some(1)),
        )
        .unwrap();
    assert_eq!(second.items.len(), 1);
    assert_ne!(page.items[0].recipient, second.items[0].recipient);
    assert!(second.next_cursor.is_none());
    let own = f
        .service
        .delivery_status(
            &recipient.principal_client_secret.secret,
            record,
            &PageQuery::default(),
        )
        .unwrap();
    assert_eq!(own.items.len(), 1);
    assert_eq!(own.items[0].recipient, "recipient");
    assert!(
        f.service
            .delivery_status(
                &recipient.principal_client_secret.secret,
                record,
                &PageQuery::new(Some(cursor), None)
            )
            .is_err()
    );
    for token in [&reader.principal_client_secret.secret, &f.client] {
        assert!(matches!(
            f.service
                .delivery_status(token, "absent", &PageQuery::default()),
            Err(BootstrapError::NotFound)
        ));
    }
    assert!(matches!(
        f.service.delivery_status(
            &reader.principal_client_secret.secret,
            record,
            &PageQuery::default()
        ),
        Err(BootstrapError::NotFound)
    ));
    assert!(matches!(
        f.service
            .delivery_status(&f.delivery, record, &PageQuery::default()),
        Err(BootstrapError::Unauthorized)
    ));
    for (principal, token) in [
        ("reader", &f.client),
        ("recipient", &recipient.principal_client_secret.secret),
    ] {
        f.service
            .set_membership(&MembershipRequest {
                space_id: "space".into(),
                principal_id: principal.into(),
                can_read: false,
                can_append: false,
                can_admin: false,
            })
            .unwrap();
        assert!(matches!(
            f.service
                .delivery_status(token, record, &PageQuery::default()),
            Err(BootstrapError::NotFound)
        ));
    }
    let first = f
        .service
        .list_adapters(&PageQuery::new(None, Some(1)))
        .unwrap();
    assert!(first.next_cursor.is_some());
    assert!(
        f.service
            .list_adapters(&PageQuery::new(first.next_cursor, Some(1)))
            .unwrap()
            .items
            .len()
            == 1
    );
}

#[test]
fn custody_cross_principal_rotated_credential_and_suppression() {
    let f = Fixture::new();
    let other = add_identity(&f, "other");
    f.append("one");
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let input = commit_request(&claim);
    let foreign = f
        .service
        .commit_custody(
            &other.delivery_adapter_secret.secret,
            &claim.claim_id,
            &input,
        )
        .unwrap();
    assert_eq!(foreign.items[0].result, CommitItemResult::ClaimNotFound);
    let event = event(
        &claim.items[0],
        "foreign",
        domain::TelemetryState::RouteUnavailable,
    );
    assert!(matches!(
        f.service.record_delivery_event(
            &other.delivery_adapter_secret.secret,
            &claim.items[0].mailbox_item_id,
            &event
        ),
        Err(BootstrapError::NotFound)
    ));
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "reader".into(),
            can_read: false,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    assert_eq!(
        f.service
            .commit_custody(&f.delivery, &claim.claim_id, &input)
            .unwrap()
            .items[0]
            .result,
        CommitItemResult::SuppressedRevoked
    );
    assert!(
        f.service
            .requeue_mailbox_item(&claim.items[0].mailbox_item_id, &RequeueRequest::default())
            .is_err()
    );
    let credential = f
        .service
        .authenticate(&f.delivery, CredentialClass::DeliveryAdapter)
        .unwrap();
    let rotated = f
        .service
        .rotate(&CredentialRotateRequest {
            credential_id: credential.credential_id,
            reason: None,
        })
        .unwrap();
    assert!(
        f.service
            .commit_custody(&f.delivery, &claim.claim_id, &input)
            .is_err()
    );
    assert_eq!(
        f.service
            .commit_custody(&rotated.replacement_secret.secret, &claim.claim_id, &input)
            .unwrap()
            .items[0]
            .result,
        CommitItemResult::ClaimNotFound
    );
}

#[test]
fn telemetry_transition_matrix_old_attempt_projection_and_atomic_failure() {
    use domain::TelemetryState::*;
    for final_state in [
        AdapterReportedRuntimeAccepted,
        RouteUnavailable,
        AdapterReportedTerminalFailure,
    ] {
        let f = Fixture::new();
        f.append("one");
        let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
        let item = &claim.items[0];
        f.service
            .commit_custody(&f.delivery, &claim.claim_id, &commit_request(&claim))
            .unwrap();
        let retry = event(item, "retry", AdapterReportedRetryableFailure);
        assert!(matches!(
            f.service
                .clone()
                .with_failpoint("delivery-event")
                .record_delivery_event(&f.delivery, &item.mailbox_item_id, &retry),
            Err(BootstrapError::Injected)
        ));
        assert_eq!(
            f.db.connect()
                .unwrap()
                .query_row("SELECT count(*) FROM delivery_events", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        f.service
            .record_delivery_event(&f.delivery, &item.mailbox_item_id, &retry)
            .unwrap();
        f.service
            .record_delivery_event(
                &f.delivery,
                &item.mailbox_item_id,
                &event(item, "retry-again", AdapterReportedRetryableFailure),
            )
            .unwrap();
        let next = f
            .service
            .requeue_mailbox_item(&item.mailbox_item_id, &RequeueRequest::default())
            .unwrap();
        f.service
            .record_delivery_event(
                &f.delivery,
                &item.mailbox_item_id,
                &event(item, "final", final_state),
            )
            .unwrap();
        for state in [
            AdapterReportedRuntimeAccepted,
            AdapterReportedRetryableFailure,
            RouteUnavailable,
            AdapterReportedTerminalFailure,
        ] {
            assert!(matches!(
                f.service.record_delivery_event(
                    &f.delivery,
                    &item.mailbox_item_id,
                    &event(item, "invalid", state)
                ),
                Err(BootstrapError::Conflict)
            ));
        }
        let status = f
            .service
            .delivery_status(&f.client, &item.record.id, &PageQuery::default())
            .unwrap();
        assert_eq!(status.items[0].state, domain::DeliveryState::Pending);
        assert_eq!(
            status.items[0].last_attempt_id.as_ref(),
            Some(&next.attempt_id)
        );
        assert_eq!(
            f.service
                .commit_custody(&f.delivery, &claim.claim_id, &commit_request(&claim))
                .unwrap()
                .items[0]
                .result,
            CommitItemResult::AlreadyCommitted
        );
        assert!(matches!(
            f.service
                .requeue_mailbox_item(&item.mailbox_item_id, &RequeueRequest::default()),
            Err(BootstrapError::Conflict)
        ));
    }
}
#[test]
fn expired_claim_cannot_borrow_a_later_claims_receipt() {
    let f = Fixture::new();
    f.append("one");
    let first = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    f.clock.0.fetch_add(30, Ordering::SeqCst);
    let second = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    assert_eq!(first.items, second.items);
    f.service
        .commit_custody(&f.delivery, &second.claim_id, &commit_request(&second))
        .unwrap();
    assert_eq!(
        f.service
            .commit_custody(&f.delivery, &first.claim_id, &commit_request(&first))
            .unwrap()
            .items[0]
            .result,
        CommitItemResult::LeaseExpired
    );
    let item = &second.items[0];
    let input = event(
        item,
        "accepted",
        domain::TelemetryState::AdapterReportedRuntimeAccepted,
    );
    let accepted = f
        .service
        .record_delivery_event(&f.delivery, &item.mailbox_item_id, &input)
        .unwrap();
    f.clock.0.fetch_add(60, Ordering::SeqCst);
    assert_eq!(
        f.service
            .record_delivery_event(&f.delivery, &item.mailbox_item_id, &input)
            .unwrap(),
        accepted
    );
    let mut stale = input.clone();
    stale.generation = 2;
    assert!(matches!(
        f.service
            .record_delivery_event(&f.delivery, &item.mailbox_item_id, &stale),
        Err(BootstrapError::Conflict)
    ));
}

#[test]
fn status_reconciles_expiry_and_author_without_attention_sees_empty_page() {
    let f = Fixture::new();
    f.append("one");
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    f.clock.0.fetch_add(30, Ordering::SeqCst);
    assert_eq!(
        f.service
            .delivery_status(&f.client, &claim.items[0].record.id, &PageQuery::default())
            .unwrap()
            .items[0]
            .state,
        domain::DeliveryState::Pending
    );
    let posted = f
        .service
        .append_record(
            &f.client,
            "space",
            "no-attention",
            &RecordInput {
                kind: "note".into(),
                content: "no recipients".into(),
                attention: vec![],
                relations: vec![],
                run_id: None,
                routing_key: None,
            },
        )
        .unwrap();
    let page = f
        .service
        .delivery_status(&f.client, &posted.record.id, &PageQuery::default())
        .unwrap();
    assert!(page.items.is_empty());
    assert!(page.next_cursor.is_none());
}

#[test]
fn concurrent_custody_events_and_requeue_are_serialized() {
    let f = Fixture::new();
    f.append("one");
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let handles = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                let service = &f.service;
                let token = &f.delivery;
                let claim = &claim;
                scope.spawn(move || {
                    barrier.wait();
                    service
                        .commit_custody(token, &claim.claim_id, &commit_request(claim))
                        .unwrap()
                        .items[0]
                        .result
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(results.contains(&CommitItemResult::Committed));
    assert!(results.contains(&CommitItemResult::AlreadyCommitted));
    let item = &claim.items[0];
    let input = event(
        item,
        "race",
        domain::TelemetryState::AdapterReportedRuntimeAccepted,
    );
    let results = std::thread::scope(|scope| {
        let handles = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                let service = &f.service;
                let token = &f.delivery;
                let input = &input;
                scope.spawn(move || {
                    barrier.wait();
                    service
                        .record_delivery_event(token, &item.mailbox_item_id, input)
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results[0], results[1]);
    let results = std::thread::scope(|scope| {
        let handles = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                let service = &f.service;
                scope.spawn(move || {
                    barrier.wait();
                    service.requeue_mailbox_item(&item.mailbox_item_id, &RequeueRequest::default())
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(BootstrapError::Conflict)))
            .count(),
        1
    );
    let conn = f.db.connect().unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM host_custody", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM delivery_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        conn.query_row("SELECT max(ordinal) FROM delivery_attempts", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn restart_lost_response_expiry_and_bounds() {
    let f = Fixture::new();
    for n in 0..21 {
        f.append(&n.to_string());
    }
    let registration = f
        .service
        .register_adapter(
            &f.delivery,
            &AdapterRegisterRequest {
                instance_id: "installation".into(),
            },
        )
        .unwrap();
    assert_eq!(registration.generation, 1);
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    assert_eq!(claim.items.len(), 20);
    assert!(matches!(
        f.service.claim_mailbox(&f.delivery, &f.request()),
        Err(BootstrapError::Conflict)
    ));
    f.clock.0.fetch_add(30, Ordering::SeqCst);
    let retry = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    assert_ne!(retry.claim_id, claim.claim_id);
    assert_eq!(retry.items, claim.items);
    assert_eq!(
        f.db.connect()
            .unwrap()
            .query_row("SELECT count(*) FROM delivery_attempts", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        21
    );
    for limit in [0, 21] {
        let mut request = f.request();
        request.limit = limit;
        assert!(f.service.claim_mailbox(&f.delivery, &request).is_err());
    }
    let mut request = f.request();
    request.wait_seconds = 31;
    assert!(f.service.claim_mailbox(&f.delivery, &request).is_err());
    assert!(f.service.claim_mailbox(&f.client, &f.request()).is_err());
}

#[test]
fn revocation_suppresses_without_exposing_content() {
    let f = Fixture::new();
    f.append("one");
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "reader".into(),
            can_read: false,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    assert!(
        f.service
            .claim_mailbox(&f.delivery, &f.request())
            .unwrap()
            .items
            .is_empty()
    );
    let state: String =
        f.db.connect()
            .unwrap()
            .query_row("SELECT state FROM delivery_attempts", [], |r| r.get(0))
            .unwrap();
    assert_eq!(state, "suppressed-revoked");
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "reader".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    assert!(
        f.service
            .claim_mailbox(&f.delivery, &f.request())
            .unwrap()
            .items
            .is_empty()
    );
}

#[test]
fn replacement_fences_and_requires_new_enrollment() {
    let f = Fixture::new();
    f.append("one");
    let old = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let request = AdapterReplaceRequest {
        expected_generation: 1,
        new_instance_id: "replacement".into(),
        reason: None,
    };
    let replacement = f.service.replace_adapter("adapter", &request).unwrap();
    assert_eq!(replacement.generation, 2);
    assert!(matches!(
        f.service.replace_adapter("adapter", &request),
        Err(BootstrapError::Conflict)
    ));
    assert!(
        f.service
            .heartbeat_adapter(
                &f.delivery,
                &AdapterHeartbeatRequest {
                    instance_id: "installation".into(),
                    generation: 1
                }
            )
            .is_err()
    );
    assert!(f.service.claim_mailbox(&f.delivery, &f.request()).is_err());
    let ticket = f
        .service
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: "reader".into(),
            adapter_id: "adapter".into(),
            ttl_seconds: 900,
        })
        .unwrap();
    let enrolled = f
        .service
        .exchange(
            &ticket.enrollment_ticket.ticket,
            &EnrollmentExchangeRequest {
                instance_id: "replacement".into(),
            },
        )
        .unwrap();
    let mut request = f.request();
    request.instance_id = "replacement".into();
    request.generation = enrolled.generation;
    let claimed = f
        .service
        .claim_mailbox(&enrolled.delivery_adapter_secret.secret, &request)
        .unwrap();
    assert_eq!(claimed.items, old.items);
}

#[test]
fn competing_claims_and_transaction_rollback() {
    let f = Fixture::new();
    f.append("one");
    for boundary in ["claim-created", "claim-state", "claim-items"] {
        assert!(
            f.service
                .clone()
                .with_failpoint(boundary)
                .claim_mailbox(&f.delivery, &f.request())
                .is_err()
        );
    }
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let service = f.service.clone();
            let token = f.delivery.clone();
            let request = f.request();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                service.claim_mailbox(&token, &request)
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(BootstrapError::Conflict)))
            .count(),
        1
    );
}

#[test]
fn heartbeat_expiry_restart_and_credential_binding() {
    let f = Fixture::new();
    f.append("one");
    let mut heartbeat = AdapterHeartbeatRequest {
        instance_id: "installation".into(),
        generation: 2,
    };
    assert!(matches!(
        f.service.heartbeat_adapter(&f.delivery, &heartbeat),
        Err(BootstrapError::Conflict)
    ));
    heartbeat.generation = 1;
    let before = f
        .service
        .heartbeat_adapter(&f.delivery, &heartbeat)
        .unwrap();
    f.clock.0.fetch_add(20, Ordering::SeqCst);
    let after = f
        .service
        .heartbeat_adapter(&f.delivery, &heartbeat)
        .unwrap();
    assert_eq!(before.generation, after.generation);
    assert!(before.lease_expires_at < after.lease_expires_at);
    let claim = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let bound: String =
        f.db.connect()
            .unwrap()
            .query_row(
                "SELECT credential_id FROM claims WHERE id=?",
                [&claim.claim_id],
                |r| r.get(0),
            )
            .unwrap();
    let actor = f
        .service
        .authenticate(&f.delivery, CredentialClass::DeliveryAdapter)
        .unwrap();
    assert_eq!(bound, actor.credential_id);
    let restarted = BootstrapService::with_sources(
        Database::open(&f.path).unwrap(),
        f.clock.clone(),
        Arc::new(OsSecretSource),
    );
    assert_eq!(
        restarted
            .register_adapter(
                &f.delivery,
                &AdapterRegisterRequest {
                    instance_id: "installation".into()
                }
            )
            .unwrap()
            .generation,
        1
    );
    assert!(matches!(
        restarted.claim_mailbox(&f.delivery, &f.request()),
        Err(BootstrapError::Conflict)
    ));
    f.clock.0.fetch_add(60, Ordering::SeqCst);
    assert!(matches!(
        restarted.heartbeat_adapter(&f.delivery, &heartbeat),
        Err(BootstrapError::Conflict)
    ));
    assert!(matches!(
        restarted.claim_mailbox(&f.delivery, &f.request()),
        Err(BootstrapError::Conflict)
    ));
    assert_eq!(
        restarted
            .register_adapter(
                &f.delivery,
                &AdapterRegisterRequest {
                    instance_id: "installation".into()
                }
            )
            .unwrap()
            .generation,
        1
    );
    assert_eq!(
        restarted
            .claim_mailbox(&f.delivery, &f.request())
            .unwrap()
            .items,
        claim.items
    );
}

#[test]
fn replacement_cas_race_and_rollback() {
    let f = Fixture::new();
    f.append("one");
    f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    let input = AdapterReplaceRequest {
        expected_generation: 1,
        new_instance_id: "replacement".into(),
        reason: Some("planned replacement".into()),
    };
    assert!(
        f.service
            .clone()
            .with_failpoint("adapter-replaced")
            .replace_adapter("adapter", &input)
            .is_err()
    );
    assert!(
        f.service
            .heartbeat_adapter(
                &f.delivery,
                &AdapterHeartbeatRequest {
                    instance_id: "installation".into(),
                    generation: 1
                }
            )
            .is_ok()
    );
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|n| {
            let service = f.service.clone();
            let barrier = barrier.clone();
            let mut input = input.clone();
            input.new_instance_id = format!("replacement-{n}");
            std::thread::spawn(move || {
                barrier.wait();
                service.replace_adapter("adapter", &input)
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(BootstrapError::Conflict)))
            .count(),
        1
    );
    assert_eq!(
        f.db.connect()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM claims WHERE state='active'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

#[test]
fn status_counts_only_current_pending_and_rejects_foreign_cursors() {
    let f = Fixture::new();
    f.append("one");
    let page = f
        .service
        .mailbox_status(&f.delivery, &PageQuery::default())
        .unwrap();
    assert_eq!(page.items[0].principal_id, "reader");
    assert_eq!(page.items[0].pending, 1);
    assert!(page.items[0].oldest_pending_at.is_some());
    assert!(page.next_cursor.is_none());
    assert!(
        f.service
            .mailbox_status(&f.delivery, &PageQuery::new(Some("forged".into()), Some(1)))
            .is_err()
    );
    assert!(
        f.service
            .mailbox_status(&f.delivery, &PageQuery::new(None, Some(101)))
            .is_err()
    );
    f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    assert_eq!(
        f.service
            .mailbox_status(&f.delivery, &PageQuery::default())
            .unwrap()
            .items[0]
            .pending,
        0
    );
    f.clock.0.fetch_add(30, Ordering::SeqCst);
    assert_eq!(
        f.service
            .mailbox_status(&f.delivery, &PageQuery::default())
            .unwrap()
            .items[0]
            .pending,
        1
    );
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "reader".into(),
            can_read: false,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    let status = f
        .service
        .mailbox_status(&f.delivery, &PageQuery::default())
        .unwrap();
    assert_eq!(status.items[0].pending, 0);
    assert!(status.items[0].oldest_pending_at.is_none());
    assert_eq!(
        status,
        f.service
            .admin_mailbox_status("reader", &PageQuery::default())
            .unwrap()
    );
}

#[test]
fn recipient_isolation_and_credential_revocation() {
    let f = Fixture::new();
    f.service
        .create_principal(&PrincipalCreateRequest {
            id: "other".into(),
            display_name: "Other".into(),
        })
        .unwrap();
    f.service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "other".into(),
            can_read: true,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    let record = f
        .service
        .append_record(
            &f.client,
            "space",
            "other",
            &RecordInput {
                kind: "note".into(),
                content: "only another recipient".into(),
                attention: vec!["other".into()],
                relations: vec![],
                run_id: None,
                routing_key: None,
            },
        )
        .unwrap();
    assert!(
        f.service
            .claim_mailbox(&f.delivery, &f.request())
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        f.service
            .get_record(&f.client, &record.record.id)
            .unwrap()
            .content,
        "only another recipient"
    );
    let actor = f
        .service
        .authenticate(&f.delivery, CredentialClass::DeliveryAdapter)
        .unwrap();
    f.service.revoke(&actor.credential_id, None).unwrap();
    assert!(matches!(
        f.service.claim_mailbox(&f.delivery, &f.request()),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service.register_adapter(
            &f.delivery,
            &AdapterRegisterRequest {
                instance_id: "installation".into()
            }
        ),
        Err(BootstrapError::Unauthorized)
    ));
}

#[test]
fn selection_rechecks_membership_after_waiting_for_the_write_lock() {
    let f = Fixture::new();
    f.append("one");
    let connection = f.db.connect().unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker = {
        let barrier = barrier.clone();
        let service = f.service.clone();
        let token = f.delivery.clone();
        let request = f.request();
        std::thread::spawn(move || {
            barrier.wait();
            service.claim_mailbox(&token, &request)
        })
    };
    barrier.wait();
    connection
        .execute_batch("UPDATE memberships SET can_read=0; COMMIT;")
        .unwrap();
    assert!(worker.join().unwrap().unwrap().items.is_empty());
    assert_eq!(
        connection
            .query_row("SELECT state FROM mailbox_items", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "suppressed-revoked"
    );
}

#[test]
fn heartbeat_samples_lease_time_after_acquiring_the_write_lock() {
    let f = Fixture::new();
    let connection = f.db.connect().unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker = {
        let barrier = barrier.clone();
        let service = f.service.clone();
        let token = f.delivery.clone();
        std::thread::spawn(move || {
            barrier.wait();
            service.heartbeat_adapter(
                &token,
                &AdapterHeartbeatRequest {
                    instance_id: "installation".into(),
                    generation: 1,
                },
            )
        })
    };
    barrier.wait();
    f.clock.0.fetch_add(60, Ordering::SeqCst);
    connection.execute_batch("COMMIT").unwrap();
    assert!(matches!(
        worker.join().unwrap(),
        Err(BootstrapError::Conflict)
    ));
}

#[test]
fn claim_lease_is_clipped_to_registration_and_heartbeat_does_not_extend_it() {
    let f = Fixture::new();
    f.append("one");
    let registration = f
        .service
        .register_adapter(
            &f.delivery,
            &AdapterRegisterRequest {
                instance_id: "installation".into(),
            },
        )
        .unwrap();
    f.clock.0.fetch_add(50, Ordering::SeqCst);
    let first = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    assert_eq!(first.lease_expires_at, registration.lease_expires_at);
    f.service
        .heartbeat_adapter(
            &f.delivery,
            &AdapterHeartbeatRequest {
                instance_id: "installation".into(),
                generation: 1,
            },
        )
        .unwrap();
    f.clock.0.fetch_add(10, Ordering::SeqCst);
    let second = f.service.claim_mailbox(&f.delivery, &f.request()).unwrap();
    assert_eq!(first.items, second.items);
    assert_ne!(first.claim_id, second.claim_id);
}
