#![cfg(unix)]

//! End-to-end proof for the Muse adapter binary: real `journald`, the real
//! drop-point handoff followed by inbox acknowledgment. No fake runtime and
//! no injected HTTP probe: the "runtime" under test is the local drop
//! directory a hook worker watches.

#[path = "support/inbox.rs"]
mod support;

use journal_client::{Client, HttpTransport};
use journal_inbox_muse::{Config, run};
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
        let principal_credential = "ab".repeat(32);
        service
            .register(
                &principal_credential,
                &wire::RegistrationRequest {
                    handle: "destination".into(),
                    display_name: "Destination".into(),
                },
            )
            .expect("independent registration");
        service
            .create_space(&wire::SpaceCreateRequest {
                access: journal_protocol::domain::SpaceAccess::Public,
                id: "space".into(),
                name: "Space".into(),
            })
            .expect("space");
        Self {
            directory,
            _service: service,
            principal_credential,
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

fn start_journald(fixture: &Fixture, address: std::net::SocketAddr) -> support::Process {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(fixture.directory.join("audit"))
        .expect("audit directory");
    let mut child = Command::new(env!("CARGO_BIN_EXE_journald"));
    child
        .current_dir(&fixture.directory)
        .args([
            "--database",
            "journal.db",
            "--recovery-audit",
            "audit/journal.recovery.db",
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
    support::Process(child.spawn().expect("journald process"))
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
fn cli_once_drops_to_muse_then_acknowledges_with_restart_idempotence() {
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
                content: "Muse inbox integration".into(),
                attention: vec!["destination".into()],
                run_id: None,
                routing_key: Some("default".into()),
                relations: vec![],
                title: None,
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
    let credential_file = fixture.directory.join("principal.credential");
    write_private(
        &credential_file,
        &serde_json::to_string(&wire::OneTimePrincipalClientSecret {
            credential_id: "principal-credential".into(),
            secret: fixture.principal_credential.clone(),
        })
        .expect("principal credential JSON"),
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
    std::fs::set_permissions(&routes_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let config = || {
        Config::parse(
            [
                "--central-endpoint",
                &central_endpoint,
                "--credential-file",
                credential_file.to_str().unwrap(),
                "--routes-file",
                routes_file.to_str().unwrap(),
                "--muse-drop-dir",
                drop_dir.to_str().unwrap(),
                "--once",
            ]
            .into_iter()
            .map(Into::into),
        )
        .expect("inbox CLI config")
    };
    run(config()).expect("inbox once");

    // Exactly one drop file: one inbox item, one handoff.
    let drops = drop_files(&drop_dir);
    assert_eq!(drops.len(), 1, "exactly one drop file per inbox item");
    let receipt = drops[0]
        .file_name()
        .expect("drop file name")
        .to_str()
        .expect("drop file name UTF-8")
        .to_owned();
    assert!(receipt.starts_with("muse-"));
    assert!(receipt.ends_with(".drop.json"));

    // The inbox receipt correlates with the drop payload.
    let status = central
        .delivery_status(
            &fixture.principal_credential,
            &record.id,
            &wire::PageQuery::default(),
        )
        .expect("inbox receipts");
    assert_eq!(status.items[0].state, wire::ReceiptState::Acknowledged);
    let payload: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&drops[0]).expect("drop bytes")).expect("drop JSON");
    assert_eq!(payload["dedupe_key"], status.items[0].inbox_item_id);
    assert_eq!(payload["inbox_item_id"], status.items[0].inbox_item_id);
    assert_eq!(payload["target_chat"], "private-muse-chat");
    assert_eq!(payload["record_id"], record.id);
    assert!(
        payload["body"]
            .as_str()
            .expect("drop body")
            .contains("Muse inbox integration"),
        "drop body carries the rendered record"
    );

    // The private route target never leaks into central status.
    assert!(
        !serde_json::to_string(&status)
            .unwrap()
            .contains("private-muse-chat")
    );
    // Restart: a second once-pass must not duplicate the drop.
    let config = || {
        Config::parse(
            [
                "--central-endpoint",
                &central_endpoint,
                "--credential-file",
                credential_file.to_str().unwrap(),
                "--routes-file",
                routes_file.to_str().unwrap(),
                "--muse-drop-dir",
                drop_dir.to_str().unwrap(),
                "--once",
            ]
            .into_iter()
            .map(Into::into),
        )
        .expect("inbox CLI config")
    };
    run(config()).expect("inbox restart");
    let drops_after = drop_files(&drop_dir);
    assert_eq!(drops_after.len(), 1, "restart must not duplicate the drop");
    assert_eq!(
        std::fs::read(&drops_after[0]).expect("drop bytes"),
        std::fs::read(&drops[0]).expect("drop bytes")
    );

    let input = |key: &str| wire::AppendRecordRequest {
        kind: "note".into(),
        content: "retry integration".into(),
        attention: vec!["destination".into()],
        run_id: None,
        routing_key: Some(key.into()),
        relations: vec![],
        title: None,
    };
    central
        .append(
            &fixture.principal_credential,
            "space",
            "crash",
            &input("default"),
        )
        .unwrap();
    let item = central
        .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
        .unwrap()
        .items
        .remove(0);
    support::kill_after_handoff(&fixture.directory, &central_endpoint, "");
    let before = drop_files(&drop_dir)
        .into_iter()
        .map(|path| (path.clone(), std::fs::read(path).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(before.len(), 2);
    assert!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items[0]
            .acknowledged_at
            .is_none()
    );
    assert!(before.iter().any(|(_, bytes)| {
        serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["dedupe_key"]
            == item.inbox_item_id
    }));
    run(config()).unwrap();
    assert_eq!(drop_files(&drop_dir).len(), 2);
    for (path, bytes) in before {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    assert!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items
            .is_empty()
    );

    central
        .append(
            &fixture.principal_credential,
            "space",
            "runtime-failure",
            &input("default"),
        )
        .unwrap();
    let valid_routes = std::fs::read(&routes_file).unwrap();
    std::fs::write(
        &routes_file,
        serde_json::to_vec(&json!({
            "space/default": {"key":"default","runtime_target":"../invalid","enabled":true}
        }))
        .unwrap(),
    )
    .unwrap();
    run(config()).unwrap();
    assert_eq!(drop_files(&drop_dir).len(), 2);
    assert_eq!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items
            .len(),
        1
    );
    std::fs::write(&routes_file, valid_routes).unwrap();
    run(config()).unwrap();
    assert_eq!(drop_files(&drop_dir).len(), 3);
    assert!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items
            .is_empty()
    );

    central
        .append(
            &fixture.principal_credential,
            "space",
            "unknown-route",
            &input("unknown"),
        )
        .unwrap();
    run(config()).unwrap();
    assert_eq!(drop_files(&drop_dir).len(), 3);
    assert_eq!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items
            .len(),
        1
    );
    journald.kill().expect("stop journald");
    journald.wait().expect("wait journald");
}

#[test]
fn crash_child() {
    let Ok(root) = std::env::var("INBOX_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let runtime =
        journal_runtime_muse::MuseRuntime::new(root.join("muse-drop").to_str().unwrap()).unwrap();
    support::handoff_until_ack(
        root,
        &std::env::var("INBOX_CRASH_ENDPOINT").unwrap(),
        &runtime,
    );
}
