//! End-to-end proof for the Muse adapter binary: real `journald`, real
//! spool, real custody, and the real drop-point handoff. No fake runtime and
//! no injected HTTP probe: the "runtime" under test is the local drop
//! directory a hook worker watches.

use journal_adapter_core::{InjectionState, Spool};
use journal_adapter_muse::{Config, run};
use journal_adapter_spool::{Limits, SqliteStore};
use journal_client::{Client, HttpTransport};
use journal_protocol as wire;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use serde_json::json;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Fixture {
    directory: PathBuf,
    _service: BootstrapService,
    principal_credential: String,
    delivery_credential: String,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "agent-journal-muse-cli-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        std::fs::create_dir_all(&directory).expect("fixture directory");
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
            .expect("fixture permissions");
        let database = Database::open(directory.join("journal.db")).expect("journal database");
        let service = BootstrapService::new(database);
        service
            .create_principal(&wire::PrincipalCreateRequest {
                handle: "destination".into(),
                display_name: "Destination".into(),
            })
            .expect("principal");
        service
            .create_space(&wire::SpaceCreateRequest {
                id: "space".into(),
                name: "Space".into(),
            })
            .expect("space");
        service
            .set_membership(&wire::MembershipRequest {
                space_id: "space".into(),
                principal_id: "destination".into(),
                can_read: true,
                can_append: true,
                can_admin: false,
            })
            .expect("membership");
        service
            .provision_adapter(&wire::AdapterProvisionRequest {
                principal_id: "destination".into(),
                adapter_id: "adapter".into(),
            })
            .expect("adapter");
        let ticket = service
            .create_ticket(&wire::EnrollmentTicketCreateRequest {
                principal_id: "destination".into(),
                adapter_id: "adapter".into(),
                ttl_seconds: 900,
            })
            .expect("ticket");
        let enrollment = service
            .exchange(
                &ticket.enrollment_ticket.ticket,
                &wire::EnrollmentExchangeRequest {
                    instance_id: "installation".into(),
                },
            )
            .expect("enrollment");
        Self {
            directory,
            _service: service,
            principal_credential: enrollment.principal_client_secret.secret,
            delivery_credential: enrollment.delivery_adapter_secret.secret,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

fn write_private(path: &Path, value: &str) {
    let mut file = std::fs::OpenOptions::new();
    file.write(true).create_new(true).mode(0o600);
    let mut file = file.open(path).expect("private file");
    file.write_all(value.as_bytes()).expect("private value");
    file.sync_all().expect("private sync");
}

fn start_journald(fixture: &Fixture, address: std::net::SocketAddr) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_journald"));
    child
        .current_dir(&fixture.directory)
        .args([
            "--database",
            "journal.db",
            "--admin-socket",
            "admin.sock",
            "--listen",
            &address.to_string(),
            "--blocking-limit",
            "2",
            "--max-body-bytes",
            "1048576",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    child.spawn().expect("journald process")
}

fn wait_for_journald(address: std::net::SocketAddr, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if child.try_wait().expect("journald status").is_some() {
            panic!("journald exited before readiness");
        }
        if let Ok(mut stream) = TcpStream::connect(address) {
            stream
                .write_all(
                    b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .expect("health request");
            let mut response = String::new();
            stream
                .read_to_string(&mut response)
                .expect("health response");
            if response.starts_with("HTTP/1.1 200 ") {
                return;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    child.kill().expect("kill journald");
    child.wait().expect("wait journald");
    panic!("journald did not become ready");
}

fn drop_files(drop_dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(drop_dir)
        .expect("read drop dir")
        .map(|entry| entry.expect("drop entry").path())
        .collect();
    files.sort();
    files
}

#[test]
#[cfg(unix)]
fn cli_once_drops_to_muse_after_custody_with_restart_idempotence() {
    let fixture = Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").expect("central address");
    let central_address = listener.local_addr().expect("central address");
    drop(listener);
    let mut journald = start_journald(&fixture, central_address);
    wait_for_journald(central_address, &mut journald);
    let central_endpoint = format!("http://{central_address}");
    let central = Client::new(HttpTransport::new(&central_endpoint).expect("central transport"));
    let record = central
        .append(
            &fixture.principal_credential,
            "space",
            "cli-test",
            &wire::AppendRecordRequest {
                kind: "note".into(),
                content: "Muse adapter integration".into(),
                attention: vec!["destination".into()],
                run_id: None,
                routing_key: Some("default".into()),
                relations: vec![],
            },
        )
        .expect("append record")
        .record;
    // The private drop directory: the only runtime configuration, and the
    // access control for the handoff.
    let drop_dir = fixture.directory.join("muse-drop");
    std::fs::create_dir_all(&drop_dir).expect("drop directory");
    std::fs::set_permissions(&drop_dir, std::fs::Permissions::from_mode(0o700))
        .expect("drop permissions");
    let delivery_file = fixture.directory.join("delivery.credential");
    write_private(
        &delivery_file,
        &serde_json::to_string(&wire::OneTimeDeliveryAdapterSecret {
            credential_id: "delivery-credential".into(),
            secret: fixture.delivery_credential.clone(),
        })
        .expect("delivery credential JSON"),
    );
    let routes_file = fixture.directory.join("routes.json");
    std::fs::write(
        &routes_file,
        serde_json::to_vec(&json!({
            "space/default": {
                "key": "default",
                "runtime_target": "private-muse-chat",
                "enabled": true
            }
        }))
        .expect("routes JSON"),
    )
    .expect("routes file");
    let spool_path = fixture.directory.join("spool.db");
    let config = Config::parse(
        [
            "--central-endpoint",
            &central_endpoint,
            "--delivery-credential-file",
            delivery_file.to_str().unwrap(),
            "--spool-db",
            spool_path.to_str().unwrap(),
            "--instance-id",
            "installation",
            "--routes-file",
            routes_file.to_str().unwrap(),
            "--muse-drop-dir",
            drop_dir.to_str().unwrap(),
            "--once",
        ]
        .into_iter()
        .map(Into::into),
    )
    .expect("adapter CLI config");
    run(config).expect("adapter once");

    // Exactly one drop file: one attempt, one handoff.
    let drops = drop_files(&drop_dir);
    assert_eq!(drops.len(), 1, "exactly one drop file per attempt");
    let receipt = drops[0]
        .file_name()
        .expect("drop file name")
        .to_str()
        .expect("drop file name UTF-8")
        .to_owned();
    assert!(receipt.starts_with("muse-"));
    assert!(receipt.ends_with(".drop.json"));

    // Central telemetry correlates with the drop payload.
    let status = central
        .delivery_status(
            &fixture.principal_credential,
            &record.id,
            &wire::PageQuery::default(),
        )
        .expect("delivery telemetry");
    assert_eq!(
        status.items[0].state,
        wire::domain::DeliveryState::AdapterReportedRuntimeAccepted
    );
    let attempt_id = status.items[0].last_attempt_id.clone().expect("attempt id");
    let payload: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&drops[0]).expect("drop bytes")).expect("drop JSON");
    assert_eq!(payload["dedupe_key"], attempt_id);
    assert_eq!(payload["attempt_id"], attempt_id);
    assert_eq!(payload["target_chat"], "private-muse-chat");
    assert_eq!(payload["record_id"], record.id);
    assert!(
        payload["body"]
            .as_str()
            .expect("drop body")
            .contains("Muse adapter integration"),
        "drop body carries the rendered record"
    );

    // Local spool: custody confirmed, accepted, receipt persisted.
    let spool = SqliteStore::open(&spool_path, Limits::default()).expect("spool reopen");
    let item = spool.get(&attempt_id).expect("persisted spool item");
    assert!(item.custody_confirmed, "custody precedes the drop");
    assert_eq!(item.injection_state, InjectionState::Accepted);
    assert_eq!(item.runtime_receipt, receipt);

    // The private route target never leaks into central status.
    assert!(
        !serde_json::to_string(&status)
            .unwrap()
            .contains("private-muse-chat")
    );
    // Release the test's spool handle so the restarted adapter can take the
    // lock, exactly as a real process restart would.
    drop(spool);

    // Restart: a second once-pass must not duplicate the drop.
    let config = Config::parse(
        [
            "--central-endpoint",
            &central_endpoint,
            "--delivery-credential-file",
            delivery_file.to_str().unwrap(),
            "--spool-db",
            spool_path.to_str().unwrap(),
            "--instance-id",
            "installation",
            "--routes-file",
            routes_file.to_str().unwrap(),
            "--muse-drop-dir",
            drop_dir.to_str().unwrap(),
            "--once",
        ]
        .into_iter()
        .map(Into::into),
    )
    .expect("adapter CLI config");
    run(config).expect("adapter restart");
    let drops_after = drop_files(&drop_dir);
    assert_eq!(drops_after.len(), 1, "restart must not duplicate the drop");
    assert_eq!(
        std::fs::read(&drops_after[0]).expect("drop bytes"),
        std::fs::read(&drops[0]).expect("drop bytes")
    );

    journald.kill().expect("stop journald");
    journald.wait().expect("wait journald");
}
