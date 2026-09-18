use journal_protocol::{domain::RecordInput, *};
use journal_service::{BootstrapError, BootstrapService, Clock, OsSecretSource};
use journal_storage_sqlite::Database;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime};

struct FakeClock(AtomicU64);
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
        let service =
            BootstrapService::with_sources(db.clone(), clock.clone(), Arc::new(OsSecretSource));
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
