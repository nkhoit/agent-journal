use journal_protocol::*;
use journal_service::{BootstrapError, BootstrapService, Clock, SecretSource};
use journal_storage_sqlite::Database;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(0x010203040506)
    }
}
struct FixedRandom;
impl SecretSource for FixedRandom {
    fn fill(&self, bytes: &mut [u8; 32]) -> Result<(), BootstrapError> {
        bytes.fill(255);
        Ok(())
    }
}

#[test]
fn record_id_encodes_uuid_v7_timestamp_version_variant_and_random_bits() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("uuid-v7-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Database::open(directory.join("journal.db")).unwrap();
    let bootstrap = BootstrapService::new(db.clone());
    let writer = bootstrap
        .create_principal(&PrincipalCreateRequest {
            handle: "writer".into(),
            display_name: "Writer".into(),
        })
        .unwrap();
    bootstrap
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "space".into(),
            name: "Space".into(),
        })
        .unwrap();
    bootstrap
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "writer".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    bootstrap
        .provision_adapter(&AdapterProvisionRequest {
            principal_id: "writer".into(),
            adapter_id: "adapter".into(),
        })
        .unwrap();
    let ticket = bootstrap
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: "writer".into(),
            adapter_id: "adapter".into(),
            ttl_seconds: 900,
        })
        .unwrap();
    let enrollment = bootstrap
        .exchange(
            &ticket.enrollment_ticket.ticket,
            &EnrollmentExchangeRequest {
                instance_id: "installation".into(),
            },
        )
        .unwrap();
    let service = BootstrapService::with_sources(db, Arc::new(FixedClock), Arc::new(FixedRandom));
    let input = decode_json(br#"{"kind":"note","content":"hello"}"#).unwrap();
    let result = service
        .append_record(
            &enrollment.principal_client_secret.secret,
            "space",
            "key",
            &input,
        )
        .unwrap();
    assert_eq!(result.record.id, "01020304-0506-7fff-bfff-ffffffffffff");
    assert_eq!(result.record.author, writer.id);
    assert_eq!(
        result.record.created_at,
        jiff::Timestamp::from_second(0x010203040506 / 1000)
            .unwrap()
            .to_string()
    );
    drop(service);
    drop(bootstrap);
    std::fs::remove_dir_all(directory).unwrap();
}
