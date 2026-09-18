use journal_protocol::*;
use journal_service::{BootstrapError, BootstrapService, Clock, SecretSource};
use journal_storage_sqlite::Database;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime};

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000)
    }
}
struct Sequence(AtomicU64);
impl SecretSource for Sequence {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
        bytes.fill(0);
        bytes[..8].copy_from_slice(&self.0.fetch_add(1, Ordering::SeqCst).to_le_bytes());
        Ok(())
    }
}
struct Fixture {
    service: BootstrapService,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("bootstrap-tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let service = BootstrapService::with_sources(
            Database::open(&path).unwrap(),
            Arc::new(FixedClock),
            Arc::new(Sequence(AtomicU64::new(1))),
        );
        service
            .create_principal(&PrincipalCreateRequest {
                id: "principal-test".into(),
                display_name: "Test".into(),
            })
            .unwrap();
        service
            .provision_adapter(&AdapterProvisionRequest {
                principal_id: "principal-test".into(),
                adapter_id: "adapter-test".into(),
            })
            .unwrap();
        Self { service, path }
    }
    fn ticket(&self) -> String {
        self.service
            .create_ticket(&EnrollmentTicketCreateRequest {
                principal_id: "principal-test".into(),
                adapter_id: "adapter-test".into(),
                ttl_seconds: 900,
            })
            .unwrap()
            .enrollment_ticket
            .ticket
    }
    fn exchange(&self, ticket: &str) -> Result<EnrollmentExchangeResponse, BootstrapError> {
        self.service.exchange(
            ticket,
            &EnrollmentExchangeRequest {
                instance_id: "installation-test".into(),
            },
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        for suffix in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

#[test]
fn exchange_is_one_use_and_authentication_is_class_separated() {
    let f = Fixture::new();
    let ticket = f.ticket();
    let result = f.exchange(&ticket).unwrap();
    assert!(f.exchange(&ticket).is_err());
    assert!(
        f.service
            .authenticate(
                &result.principal_client_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_ok()
    );
    assert!(
        f.service
            .authenticate(
                &result.delivery_adapter_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_err()
    );
    assert!(
        f.service
            .authenticate(
                &result.principal_client_secret.secret,
                CredentialClass::DeliveryAdapter
            )
            .is_err()
    );
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    let stored: String = connection
        .query_row(
            "SELECT token_hash FROM credentials WHERE id=?",
            [&result.principal_client_secret.credential_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_ne!(stored, result.principal_client_secret.secret);
    assert_eq!(stored.len(), 64);
    f.service
        .create_principal(&PrincipalCreateRequest {
            id: "other-principal".into(),
            display_name: "Other".into(),
        })
        .unwrap();
    assert!(matches!(
        f.service.create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: "other-principal".into(),
            adapter_id: "adapter-test".into(),
            ttl_seconds: 60,
        }),
        Err(BootstrapError::NotFound)
    ));
    let actor = f
        .service
        .authenticate(
            &result.delivery_adapter_secret.secret,
            CredentialClass::DeliveryAdapter,
        )
        .unwrap();
    assert_eq!(actor.principal_id, "principal-test");
    assert_eq!(actor.adapter_id.as_deref(), Some("adapter-test"));
    assert_eq!(actor.instance_id.as_deref(), Some("installation-test"));
}

#[test]
fn rotation_preserves_credential_expiration() {
    let f = Fixture::new();
    let issued = f.exchange(&f.ticket()).unwrap();
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    connection
        .execute(
            "UPDATE credentials SET expires_at='2027-01-15T08:01:00Z' WHERE id=?",
            [&issued.principal_client_secret.credential_id],
        )
        .unwrap();
    let rotated = f
        .service
        .rotate(&CredentialRotateRequest {
            credential_id: issued.principal_client_secret.credential_id,
            reason: None,
        })
        .unwrap();
    let expires: Option<String> = connection
        .query_row(
            "SELECT expires_at FROM credentials WHERE id=?",
            [&rotated.replacement_secret.credential_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(expires.as_deref(), Some("2027-01-15T08:01:00Z"));
}

#[test]
fn recovery_revokes_both_lineages_and_only_reenrolls_same_installation() {
    let f = Fixture::new();
    let old_ticket = f.ticket();
    let issued = f.exchange(&old_ticket).unwrap();
    let rotated = f
        .service
        .rotate(&CredentialRotateRequest {
            credential_id: issued.principal_client_secret.credential_id.clone(),
            reason: None,
        })
        .unwrap();
    assert!(
        f.service
            .authenticate(
                &issued.principal_client_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_err()
    );
    f.service
        .recover("adapter-test", "installation-test")
        .unwrap();
    assert!(
        f.service
            .authenticate(
                &rotated.replacement_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_err()
    );
    assert!(
        f.service
            .authenticate(
                &issued.delivery_adapter_secret.secret,
                CredentialClass::DeliveryAdapter
            )
            .is_err()
    );
    assert!(f.exchange(&old_ticket).is_err());
    let fresh = f.ticket();
    assert!(
        f.service
            .exchange(
                &fresh,
                &EnrollmentExchangeRequest {
                    instance_id: "other-installation".into()
                }
            )
            .is_err()
    );
    let replacement = f.exchange(&fresh).unwrap();
    assert_eq!(replacement.generation, 2);
    assert!(
        f.service
            .authenticate(
                &replacement.principal_client_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_ok()
    );
}

#[test]
fn concurrent_exchange_has_one_winner() {
    let f = Fixture::new();
    let ticket = f.ticket();
    std::thread::scope(|scope| {
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                let f = &f;
                let ticket = &ticket;
                scope.spawn(move || {
                    barrier.wait();
                    f.exchange(ticket).is_ok()
                })
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .filter_map(|h| h.join().ok())
                .filter(|v| *v)
                .count(),
            1
        );
    });
}

#[test]
fn failed_exchange_rolls_back_every_durable_boundary() {
    for boundary in [
        "ticket-lookup",
        "principal-credential",
        "delivery-credential",
        "registration",
        "ticket-consumed",
    ] {
        let f = Fixture::new();
        let ticket = f.ticket();
        let failing = f.service.clone().with_failpoint(boundary);
        assert!(
            failing
                .exchange(
                    &ticket,
                    &EnrollmentExchangeRequest {
                        instance_id: "installation-test".into()
                    }
                )
                .is_err()
        );
        let connection = Database::open(&f.path).unwrap().connect().unwrap();
        let count: i64 = connection
            .query_row("SELECT count(*) FROM credentials", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        assert!(f.exchange(&ticket).is_ok());
    }
}
#[test]
fn provisioning_validation_membership_and_revocation_are_persistent() {
    let f = Fixture::new();
    assert!(matches!(
        f.service.set_membership(&MembershipRequest {
            space_id: "missing-space".into(),
            principal_id: "principal-test".into(),
            can_read: true,
            can_append: false,
            can_admin: false,
        }),
        Err(BootstrapError::NotFound)
    ));
    assert!(
        f.service
            .create_principal(&PrincipalCreateRequest {
                id: "bad".into(),
                display_name: "".into()
            })
            .is_err()
    );
    assert!(
        f.service
            .create_ticket(&EnrollmentTicketCreateRequest {
                principal_id: "principal-test".into(),
                adapter_id: "adapter-test".into(),
                ttl_seconds: 901
            })
            .is_err()
    );
    assert!(
        f.service
            .create_ticket(&EnrollmentTicketCreateRequest {
                principal_id: "wrong-principal".into(),
                adapter_id: "adapter-test".into(),
                ttl_seconds: 1
            })
            .is_err()
    );
    f.service
        .create_space(&SpaceCreateRequest {
            id: "space-test".into(),
            name: "Test".into(),
        })
        .unwrap();
    let mut membership = MembershipRequest {
        space_id: "space-test".into(),
        principal_id: "principal-test".into(),
        can_read: true,
        can_append: false,
        can_admin: false,
    };
    f.service.set_membership(&membership).unwrap();
    membership.can_append = true;
    f.service.set_membership(&membership).unwrap();
    let result = f.exchange(&f.ticket()).unwrap();
    let actor = f
        .service
        .authenticate(
            &result.principal_client_secret.secret,
            CredentialClass::PrincipalClient,
        )
        .unwrap();
    let me = f.service.me(&actor).unwrap();
    assert_eq!(me.memberships.len(), 1);
    assert!(me.memberships[0].can_append);
    let reopened = BootstrapService::with_sources(
        Database::open(&f.path).unwrap(),
        Arc::new(FixedClock),
        Arc::new(Sequence(AtomicU64::new(100))),
    );
    reopened.revoke(&actor.credential_id, Some("test")).unwrap();
    assert!(
        f.service
            .authenticate(
                &result.principal_client_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_err()
    );
    assert!(f.service.me(&actor).is_err());
    assert!(
        f.service
            .authenticate("malformed", CredentialClass::PrincipalClient)
            .is_err()
    );
    assert!(
        f.service
            .recover("adapter-test", "different-installation")
            .is_err()
    );
}

struct AdjustableClock(AtomicU64);
impl Clock for AdjustableClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
}

#[test]
fn exchange_rechecks_expiry_after_waiting_for_write_lock() {
    use std::sync::mpsc;
    struct ObservedClock {
        seconds: AtomicU64,
        sampled: mpsc::Sender<()>,
    }
    impl Clock for ObservedClock {
        fn now(&self) -> SystemTime {
            let seconds = self.seconds.load(Ordering::SeqCst);
            self.sampled.send(()).unwrap();
            SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
        }
    }
    let f = Fixture::new();
    let ticket = f
        .service
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: "principal-test".into(),
            adapter_id: "adapter-test".into(),
            ttl_seconds: 1,
        })
        .unwrap()
        .enrollment_ticket
        .ticket;
    let database = Database::open(&f.path).unwrap();
    let connection = database.connect().unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (sampled, samples) = mpsc::channel();
    let clock = Arc::new(ObservedClock {
        seconds: AtomicU64::new(1_800_000_000),
        sampled,
    });
    let service = BootstrapService::with_sources(
        database,
        clock.clone(),
        Arc::new(Sequence(AtomicU64::new(100))),
    );
    let (started, starting) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started.send(()).unwrap();
        service.exchange(
            &ticket,
            &EnrollmentExchangeRequest {
                instance_id: "installation-test".into(),
            },
        )
    });
    starting.recv_timeout(Duration::from_secs(1)).unwrap();
    // The old implementation samples before waiting; the fixed one cannot
    // sample until this lock is released. Expiry itself uses only fake time.
    let early_sample = samples.recv_timeout(Duration::from_secs(1)).is_ok();
    clock.seconds.store(1_800_000_001, Ordering::SeqCst);
    connection.execute_batch("COMMIT").unwrap();
    let result = worker.join().unwrap();
    assert!(matches!(result, Err(BootstrapError::Unauthorized)));
    assert!(
        !early_sample,
        "exchange sampled time before acquiring its write lock"
    );
    let consumed: Option<String> = connection
        .query_row("SELECT consumed_at FROM enrollment_tickets", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(consumed.is_none());
    for table in [
        "credentials",
        "adapter_registrations",
        "enrollment_installations",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[test]
fn exchange_uses_one_instant_for_registration_and_lease() {
    struct TickingClock(AtomicU64);
    impl Clock for TickingClock {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(self.0.fetch_add(1, Ordering::SeqCst))
        }
    }
    let f = Fixture::new();
    let ticket = f.ticket();
    let clock = Arc::new(TickingClock(AtomicU64::new(1_800_000_000)));
    let service = BootstrapService::with_sources(
        Database::open(&f.path).unwrap(),
        clock.clone(),
        Arc::new(Sequence(AtomicU64::new(100))),
    );
    service
        .exchange(
            &ticket,
            &EnrollmentExchangeRequest {
                instance_id: "installation-test".into(),
            },
        )
        .unwrap();
    assert_eq!(clock.0.load(Ordering::SeqCst), 1_800_000_001);
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    let (created, lease): (String, String) = connection
        .query_row(
            "SELECT created_at,lease_expires_at FROM adapter_registrations",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let created: jiff::Timestamp = created.parse().unwrap();
    let lease: jiff::Timestamp = lease.parse().unwrap();
    assert_eq!(lease.as_second() - created.as_second(), 60);
}

#[test]
fn ticket_expiry_boundary_and_disabled_principal_fail_closed() {
    let f = Fixture::new();
    let clock = Arc::new(AdjustableClock(AtomicU64::new(1_800_000_000)));
    let service = BootstrapService::with_sources(
        Database::open(&f.path).unwrap(),
        clock.clone(),
        Arc::new(Sequence(AtomicU64::new(100))),
    );
    let ticket = service
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: "principal-test".into(),
            adapter_id: "adapter-test".into(),
            ttl_seconds: 1,
        })
        .unwrap();
    clock.0.fetch_add(1, Ordering::SeqCst);
    assert!(matches!(
        service.exchange(
            &ticket.enrollment_ticket.ticket,
            &EnrollmentExchangeRequest {
                instance_id: "installation-test".into()
            }
        ),
        Err(BootstrapError::Unauthorized)
    ));
    let issued = f.exchange(&f.ticket()).unwrap();
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    connection
        .execute(
            "UPDATE credentials SET expires_at='2027-01-15T08:00:00Z' WHERE id=?",
            [&issued.principal_client_secret.credential_id],
        )
        .unwrap();
    assert!(
        f.service
            .authenticate(
                &issued.principal_client_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_err()
    );
    connection
        .execute("UPDATE principals SET disabled_at='now'", [])
        .unwrap();
    assert!(
        f.service
            .authenticate(
                &issued.delivery_adapter_secret.secret,
                CredentialClass::DeliveryAdapter
            )
            .is_err()
    );
}

#[test]
fn recovery_and_rotation_failpoints_leave_old_credentials_live() {
    for boundary in [
        "rotation-issued",
        "rotation-revoked",
        "recovery-revoked",
        "recovery-authorized",
    ] {
        let f = Fixture::new();
        let issued = f.exchange(&f.ticket()).unwrap();
        let failing = f.service.clone().with_failpoint(boundary);
        if boundary.starts_with("rotation") {
            assert!(
                failing
                    .rotate(&CredentialRotateRequest {
                        credential_id: issued.principal_client_secret.credential_id.clone(),
                        reason: None
                    })
                    .is_err()
            );
        } else {
            assert!(
                failing
                    .recover("adapter-test", "installation-test")
                    .is_err()
            );
        }
        assert!(
            f.service
                .authenticate(
                    &issued.principal_client_secret.secret,
                    CredentialClass::PrincipalClient
                )
                .is_ok()
        );
        assert!(
            f.service
                .authenticate(
                    &issued.delivery_adapter_secret.secret,
                    CredentialClass::DeliveryAdapter
                )
                .is_ok()
        );
    }
}
struct TerminatingSource(AtomicU64);
impl SecretSource for TerminatingSource {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 3 {
            std::process::exit(73);
        }
        bytes.fill(n as u8);
        Ok(())
    }
}

#[test]
fn enrollment_crash_child() {
    let Some(path) = std::env::var_os("AJ_ENROLLMENT_CRASH_DB") else {
        return;
    };
    let service = BootstrapService::with_sources(
        Database::open(path).unwrap(),
        Arc::new(FixedClock),
        Arc::new(TerminatingSource(AtomicU64::new(1))),
    );
    let ticket = format!("01{}", "00".repeat(31));
    let _ = service.exchange(
        &ticket,
        &EnrollmentExchangeRequest {
            instance_id: "installation-test".into(),
        },
    );
    panic!("child did not terminate inside transaction");
}

#[test]
fn process_termination_during_exchange_preserves_replayable_unconsumed_ticket() {
    let f = Fixture::new();
    let ticket = f.ticket();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "enrollment_crash_child", "--nocapture"])
        .env("AJ_ENROLLMENT_CRASH_DB", &f.path)
        .output()
        .unwrap();
    assert_eq!(status.status.code(), Some(73));
    assert!(!String::from_utf8_lossy(&status.stdout).contains(&ticket));
    assert!(!String::from_utf8_lossy(&status.stderr).contains(&ticket));
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    let count: i64 = connection
        .query_row("SELECT count(*) FROM credentials", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    assert!(f.exchange(&ticket).is_ok());
}
