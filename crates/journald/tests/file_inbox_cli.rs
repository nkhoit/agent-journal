#![cfg(unix)]

//! Real `journald` plus the file spool: handoff is the durable envelope file,
//! and acknowledgment follows that publication.

#[path = "support/inbox.rs"]
mod support;

use journal_client::{Client, HttpTransport};
use journal_inbox_file::{Config, run};
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
            "agent-journal-file-cli-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let database = Database::open(directory.join("journal.db")).unwrap();
        let service = BootstrapService::new(database);
        let principal_credential = "cd".repeat(32);
        service
            .register(
                &principal_credential,
                &wire::RegistrationRequest {
                    handle: "destination".into(),
                    display_name: "Destination".into(),
                },
            )
            .unwrap();
        service
            .create_space(&wire::SpaceCreateRequest {
                access: journal_protocol::domain::SpaceAccess::Public,
                id: "space".into(),
                name: "Space".into(),
            })
            .unwrap();
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

fn write_private(path: &Path, value: &str) {
    let mut file = std::fs::OpenOptions::new();
    file.write(true).create_new(true).mode(0o600);
    let mut file = file.open(path).unwrap();
    file.write_all(value.as_bytes()).unwrap();
    file.sync_all().unwrap();
}

fn start_journald(fixture: &Fixture, address: std::net::SocketAddr) -> support::Process {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(fixture.directory.join("audit"))
        .unwrap();
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
    support::Process(child.spawn().unwrap())
}

fn wait_for_journald(address: std::net::SocketAddr, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            panic!("journald exited before readiness");
        }
        if let Ok(mut stream) = TcpStream::connect(address) {
            stream
                .write_all(
                    b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            if response.starts_with("HTTP/1.1 200 ") {
                return;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    panic!("journald did not become ready");
}

fn published_files(spool: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(spool)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("aj-") && name.ends_with(".envelope.json"))
        })
        .collect();
    files.sort();
    files
}

#[test]
fn cli_once_spools_then_acknowledges_with_restart_idempotence() {
    let fixture = Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let central_address = listener.local_addr().unwrap();
    drop(listener);
    let mut journald = start_journald(&fixture, central_address);
    wait_for_journald(central_address, &mut journald);
    let central_endpoint = format!("http://{central_address}");
    let central = Client::new(HttpTransport::new(&central_endpoint).unwrap());
    let record = central
        .append(
            &fixture.principal_credential,
            "space",
            "cli-test",
            &wire::AppendRecordRequest {
                kind: "note".into(),
                content: "File spool integration".into(),
                attention: vec!["destination".into()],
                run_id: None,
                routing_key: Some("default".into()),
                relations: vec![],
                title: None,
            },
        )
        .unwrap()
        .record;
    let spool = fixture.directory.join("inbox-spool");
    std::fs::create_dir_all(&spool).unwrap();
    std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
    let credential_file = fixture.directory.join("principal.credential");
    write_private(
        &credential_file,
        &serde_json::to_string(&wire::OneTimePrincipalClientSecret {
            credential_id: "principal-credential".into(),
            secret: fixture.principal_credential.clone(),
        })
        .unwrap(),
    );
    let routes_file = fixture.directory.join("routes.json");
    write_private(
        &routes_file,
        &serde_json::to_string(&json!({
            "space/default": {
                "key": "default",
                "runtime_target": "totoro",
                "enabled": true
            }
        }))
        .unwrap(),
    );
    let config = || {
        Config::parse(
            [
                "--central-endpoint",
                &central_endpoint,
                "--credential-file",
                credential_file.to_str().unwrap(),
                "--routes-file",
                routes_file.to_str().unwrap(),
                "--spool-dir",
                spool.to_str().unwrap(),
                "--wait-seconds",
                "0",
                "--once",
            ]
            .into_iter()
            .map(Into::into),
        )
        .unwrap()
    };
    run(config()).unwrap();
    let files = published_files(&spool);
    assert_eq!(files.len(), 1);
    let payload: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&files[0]).unwrap()).unwrap();
    assert_eq!(payload["runtime_target"], "totoro");
    assert_eq!(payload["record_id"], record.id);
    assert_eq!(payload["body"], "File spool integration");
    assert!(
        payload["rendered"]
            .as_str()
            .unwrap()
            .contains("UNTRUSTED CONTENT")
    );
    let status = central
        .delivery_status(
            &fixture.principal_credential,
            &record.id,
            &wire::PageQuery::default(),
        )
        .unwrap();
    assert_eq!(status.items[0].state, wire::ReceiptState::Acknowledged);
    assert_eq!(payload["dedupe_key"], status.items[0].inbox_item_id);
    assert!(!serde_json::to_string(&status).unwrap().contains("totoro"));

    let before = std::fs::read(&files[0]).unwrap();
    run(config()).unwrap();
    assert_eq!(published_files(&spool).len(), 1);
    assert_eq!(std::fs::read(&files[0]).unwrap(), before);

    central
        .append(
            &fixture.principal_credential,
            "space",
            "crash",
            &wire::AppendRecordRequest {
                kind: "note".into(),
                content: "crash before ack".into(),
                attention: vec!["destination".into()],
                run_id: None,
                routing_key: Some("default".into()),
                relations: vec![],
                title: None,
            },
        )
        .unwrap();
    let pending = central
        .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
        .unwrap()
        .items
        .remove(0);
    support::kill_after_handoff(&fixture.directory, &central_endpoint, "");
    assert!(pending.acknowledged_at.is_none());
    assert_eq!(published_files(&spool).len(), 2);
    assert!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items[0]
            .acknowledged_at
            .is_none()
    );
    let bytes_before: Vec<_> = published_files(&spool)
        .into_iter()
        .map(|path| (path.clone(), std::fs::read(&path).unwrap()))
        .collect();
    run(config()).unwrap();
    assert_eq!(published_files(&spool).len(), 2);
    for (path, bytes) in bytes_before {
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
            "unknown-route",
            &wire::AppendRecordRequest {
                kind: "note".into(),
                content: "unrouted".into(),
                attention: vec!["destination".into()],
                run_id: None,
                routing_key: Some("unknown".into()),
                relations: vec![],
                title: None,
            },
        )
        .unwrap();
    run(config()).unwrap();
    assert_eq!(published_files(&spool).len(), 2);
    assert_eq!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items
            .len(),
        1
    );
    journald.kill().unwrap();
    journald.wait().unwrap();
}

#[test]
fn crash_child() {
    let Ok(root) = std::env::var("INBOX_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let runtime =
        journal_runtime_file::FileRuntime::new(root.join("inbox-spool").to_str().unwrap()).unwrap();
    support::handoff_until_ack(
        root,
        &std::env::var("INBOX_CRASH_ENDPOINT").unwrap(),
        &runtime,
    );
}
