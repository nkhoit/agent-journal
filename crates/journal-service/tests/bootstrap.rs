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
    principal_id: String,
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
        let principal = service
            .create_principal(&PrincipalCreateRequest {
                handle: "principal-test".into(),
                display_name: "Test".into(),
            })
            .unwrap();
        Self {
            service,
            path,
            principal_id: principal.id,
        }
    }
    fn credential(&self) -> OneTimeReplacementSecret {
        self.service
            .recover_principal(&PrincipalRecoveryRequest {
                principal_id: self.principal_id.clone(),
                reason: None,
            })
            .unwrap()
            .replacement_secret
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

fn registration_request() -> RegistrationRequest {
    RegistrationRequest {
        handle: "self-registered".into(),
        display_name: "Self Registered".into(),
    }
}

#[test]
fn self_registration_replays_exactly_and_rejects_conflicts_and_dead_credentials() {
    let f = Fixture::new();
    let token = "ab".repeat(32);
    let request = registration_request();
    let first = f.service.register(&token, &request).unwrap();
    assert!(!first.replayed);
    assert_eq!(first.receipt.principal.handle, request.handle);
    f.service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "registration-space".into(),
            name: "Registration space".into(),
        })
        .unwrap();
    f.service
        .set_membership(&MembershipRequest {
            space_id: "registration-space".into(),
            principal_id: first.receipt.principal.id.clone(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    assert!(
        f.service
            .append_record(
                &token,
                "registration-space",
                "registration-append",
                &AppendRecordRequest {
                    kind: "note".into(),
                    content: "registered without adapter".into(),
                    run_id: None,
                    attention: Vec::new(),
                    routing_key: None,
                    relations: Vec::new(),
                },
            )
            .is_ok()
    );
    let replay = f.service.register(&token, &request).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, first.receipt);
    assert!(matches!(
        f.service.register(
            &token,
            &RegistrationRequest {
                display_name: "Changed".into(),
                ..request.clone()
            }
        ),
        Err(BootstrapError::Conflict)
    ));
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    connection
        .execute(
            "UPDATE credentials SET revoked_at='2027-01-15T08:00:00Z'
             WHERE id=?",
            [&first.receipt.credential_id],
        )
        .unwrap();
    assert!(matches!(
        f.service.register(&token, &request),
        Err(BootstrapError::Unauthorized)
    ));
    let expired_token = "78".repeat(32);
    let expired_request = RegistrationRequest {
        handle: "expired-registration".into(),
        display_name: "Expired".into(),
    };
    let expired = f
        .service
        .register(&expired_token, &expired_request)
        .unwrap();
    connection
        .execute(
            "UPDATE credentials SET expires_at='2020-01-01T00:00:00Z' WHERE id=?",
            [&expired.receipt.credential_id],
        )
        .unwrap();
    assert!(matches!(
        f.service.register(&expired_token, &expired_request),
        Err(BootstrapError::Unauthorized)
    ));
    let occupied = f
        .service
        .register(
            &"56".repeat(32),
            &RegistrationRequest {
                handle: "principal-test".into(),
                display_name: "Occupied".into(),
            },
        )
        .unwrap_err();
    assert!(matches!(occupied, BootstrapError::Conflict));
    let enrollment = f.credential();
    assert!(matches!(
        f.service.register(&enrollment.secret, &request),
        Err(BootstrapError::Unauthorized)
    ));
    assert!(matches!(
        f.service.register(&"CD".repeat(32), &request),
        Err(BootstrapError::InvalidJournal)
    ));
}

#[test]
fn concurrent_identical_registration_has_one_identity_and_receipt() {
    let f = Fixture::new();
    let token = "cd".repeat(32);
    let request = registration_request();
    std::thread::scope(|scope| {
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let results = (0..2)
            .map(|_| {
                let barrier = barrier.clone();
                let service = &f.service;
                let token = &token;
                let request = &request;
                scope.spawn(move || {
                    barrier.wait();
                    service.register(token, request).unwrap()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results[0].receipt, results[1].receipt);
        assert_ne!(results[0].replayed, results[1].replayed);
    });
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM registration_receipts", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        1
    );
}

#[test]
fn registration_and_principal_recovery_roll_back_at_every_boundary() {
    for boundary in [
        "registration-digest-lookup",
        "registration-principal",
        "registration-handle",
        "registration-credential",
        "registration-receipt",
    ] {
        let f = Fixture::new();
        let failing = f.service.clone().with_failpoint(boundary);
        assert!(matches!(
            failing.register(&"ef".repeat(32), &registration_request()),
            Err(BootstrapError::Injected)
        ));
        let connection = Database::open(&f.path).unwrap().connect().unwrap();
        for table in ["registration_receipts", "credentials"] {
            assert_eq!(
                connection
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM principals WHERE display_name='Self Registered'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }

    for boundary in ["principal-recovery-revoked", "principal-recovery-issued"] {
        let f = Fixture::new();
        let registered = f
            .service
            .register(&"12".repeat(32), &registration_request())
            .unwrap();
        let failing = f.service.clone().with_failpoint(boundary);
        assert!(matches!(
            failing.recover_principal(&PrincipalRecoveryRequest {
                principal_id: registered.receipt.principal.id.clone(),
                reason: Some("test".into()),
            }),
            Err(BootstrapError::Injected)
        ));
        assert!(
            f.service
                .authenticate(&"12".repeat(32), CredentialClass::PrincipalClient)
                .is_ok()
        );
    }
}

#[test]
fn principal_recovery_revokes_all_authority_and_is_repeatable_by_uuid() {
    let f = Fixture::new();
    let registered = f
        .service
        .register(&"34".repeat(32), &registration_request())
        .unwrap();
    let first = f
        .service
        .recover_principal(&PrincipalRecoveryRequest {
            principal_id: registered.receipt.principal.id.clone(),
            reason: None,
        })
        .unwrap();
    assert!(
        f.service
            .authenticate(&"34".repeat(32), CredentialClass::PrincipalClient)
            .is_err()
    );
    let second = f
        .service
        .recover_principal(&PrincipalRecoveryRequest {
            principal_id: registered.receipt.principal.id.clone(),
            reason: None,
        })
        .unwrap();
    assert!(
        f.service
            .authenticate(
                &first.replacement_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_err()
    );
    assert!(
        f.service
            .authenticate(
                &second.replacement_secret.secret,
                CredentialClass::PrincipalClient
            )
            .is_ok()
    );
    assert_eq!(second.principal.id, registered.receipt.principal.id);
}

#[cfg(unix)]
#[test]
fn principal_recovery_advances_protected_external_audit() {
    use std::os::unix::fs::PermissionsExt;
    let directory =
        std::env::temp_dir().join(format!("principal-recovery-audit-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let database =
        Database::open_protected(directory.join("journal.db"), directory.join("audit.db")).unwrap();
    let service = BootstrapService::with_sources(
        database.clone(),
        Arc::new(FixedClock),
        Arc::new(Sequence(AtomicU64::new(500))),
    );
    let registered = service
        .register(&"90".repeat(32), &registration_request())
        .unwrap();
    let before: i64 = database
        .connect_read_only()
        .unwrap()
        .query_row(
            "SELECT revision FROM recovery_anchor WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    service
        .recover_principal(&PrincipalRecoveryRequest {
            principal_id: registered.receipt.principal.id,
            reason: Some("audit test".into()),
        })
        .unwrap();
    let after: i64 = database
        .connect_read_only()
        .unwrap()
        .query_row(
            "SELECT revision FROM recovery_anchor WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after, before + 1);
    drop(service);
    drop(database);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn profile_rename_preserves_aliases_and_idempotent_uuid_looking_handles() {
    let f = Fixture::new();
    let issued = f.credential();
    let actor = f
        .service
        .authenticate(&issued.secret, CredentialClass::PrincipalClient)
        .unwrap();
    let before = f.service.me(&actor).unwrap().principal;
    let update = ProfileUpdateRequest {
        handle: "018f1f59-6e90-7000-8000-000000000009".into(),
        display_name: "Renamed".into(),
        description: Some("Mutable profile".into()),
        expected_profile_revision: before.profile_revision,
    };
    let updated = f
        .service
        .update_own_profile(&actor, "profile-rename", &update)
        .unwrap();
    assert_eq!(updated.id, before.id);
    assert_eq!(updated.handle, update.handle);
    assert_eq!(updated.display_name, update.display_name);
    assert_eq!(updated.description, update.description);
    assert_eq!(updated.profile_revision, before.profile_revision + 1);
    assert_eq!(
        f.service
            .update_own_profile(&actor, "profile-rename", &update)
            .unwrap(),
        updated
    );
    assert!(matches!(
        f.service.update_own_profile(
            &actor,
            "profile-rename",
            &ProfileUpdateRequest {
                display_name: "Different".into(),
                ..update.clone()
            }
        ),
        Err(BootstrapError::IdempotencyConflict)
    ));
    assert!(matches!(
        f.service.update_own_profile(
            &actor,
            "stale-profile",
            &ProfileUpdateRequest {
                expected_profile_revision: before.profile_revision,
                ..update.clone()
            }
        ),
        Err(BootstrapError::Conflict)
    ));
    f.service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "profile-space".into(),
            name: "Profile space".into(),
        })
        .unwrap();
    let through_uuid_looking_handle = f
        .service
        .set_membership(&MembershipRequest {
            space_id: "profile-space".into(),
            principal_id: update.handle.clone(),
            can_read: true,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    assert_eq!(through_uuid_looking_handle.principal_id, updated.id);
    let through_retired_alias = f
        .service
        .set_membership(&MembershipRequest {
            space_id: "profile-space".into(),
            principal_id: "principal-test".into(),
            can_read: true,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    assert_eq!(through_retired_alias.principal_id, updated.id);
    assert!(
        f.service
            .create_principal(&PrincipalCreateRequest {
                handle: "principal-test".into(),
                display_name: "Reused".into(),
            })
            .is_err()
    );
}

#[test]
fn rotation_preserves_credential_expiration() {
    let f = Fixture::new();
    let issued = f.credential();
    let connection = Database::open(&f.path).unwrap().connect().unwrap();
    connection
        .execute(
            "UPDATE credentials SET expires_at='2027-01-15T08:01:00Z' WHERE id=?",
            [&issued.credential_id],
        )
        .unwrap();
    let rotated = f
        .service
        .rotate(&CredentialRotateRequest {
            credential_id: issued.credential_id,
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
                handle: "bad".into(),
                display_name: "".into()
            })
            .is_err()
    );
    f.service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
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
    let result = f.credential();
    let actor = f
        .service
        .authenticate(&result.secret, CredentialClass::PrincipalClient)
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
            .authenticate(&result.secret, CredentialClass::PrincipalClient)
            .is_err()
    );
    assert!(f.service.me(&actor).is_err());
    assert!(
        f.service
            .authenticate("malformed", CredentialClass::PrincipalClient)
            .is_err()
    );
}
