#![cfg(unix)]

#[path = "support/inbox.rs"]
mod support;

use journal_client::{Client, HttpTransport};
use journal_inbox_hermes::{Config, run};
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
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
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
            "agent-journal-hermes-cli-{}-{}",
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

struct HermesProbe {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    unacknowledged_before_run: Arc<AtomicBool>,
    runs: Arc<Mutex<RunState>>,
    stop: Arc<AtomicBool>,
    wake: std::net::SocketAddr,
    thread: Option<JoinHandle<()>>,
}

struct RunState {
    status: u16,
    keys: Vec<String>,
}

impl HermesProbe {
    fn new(database_path: PathBuf, record_id: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Hermes listener");
        let address = listener.local_addr().expect("Hermes address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let unacknowledged_before_run = Arc::new(AtomicBool::new(false));
        let runs = Arc::new(Mutex::new(RunState {
            status: 202,
            keys: vec![],
        }));
        let thread_runs = Arc::clone(&runs);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = Arc::clone(&requests);
        let thread_unacknowledged = Arc::clone(&unacknowledged_before_run);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            loop {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                if thread_stop.load(Ordering::Acquire) {
                    return;
                }
                handle_hermes(
                    stream,
                    &thread_requests,
                    &thread_unacknowledged,
                    &database_path,
                    &record_id,
                    &thread_runs,
                );
            }
        });
        Self {
            endpoint: format!("http://{address}"),
            requests,
            unacknowledged_before_run,
            runs,
            stop,
            wake: address,
            thread: Some(thread),
        }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("Hermes requests").clone()
    }
}

impl Drop for HermesProbe {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.wake);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("Hermes thread");
        }
    }
}

fn handle_hermes(
    mut stream: TcpStream,
    requests: &Arc<Mutex<Vec<String>>>,
    unacknowledged_before_run: &Arc<AtomicBool>,
    database_path: &Path,
    record_id: &str,
    runs: &Arc<Mutex<RunState>>,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("Hermes timeout");
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        let count = match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(count) => count,
            Err(_) => return,
        };
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = header.split("\r\n");
    let path = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_owned();
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let count = match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(count) => count,
            Err(_) => return,
        };
        bytes.extend_from_slice(&buffer[..count]);
    }
    requests.lock().expect("Hermes requests").push(path.clone());
    if path == "/v1/runs" {
        let connection = rusqlite::Connection::open(database_path).unwrap();
        let unacknowledged: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM mailbox_items WHERE record_id=? AND acknowledged_at IS NULL)",
            [record_id], |r| r.get(0)).unwrap();
        unacknowledged_before_run.store(unacknowledged, Ordering::Release);
    }
    let run_status = if path == "/v1/runs" {
        let key = String::from_utf8_lossy(&bytes[..header_end])
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("idempotency-key"))
            .map(|(_, value)| value.trim().to_owned())
            .expect("stable key");
        let mut state = runs.lock().unwrap();
        state.keys.push(key);
        state.status
    } else {
        202
    };
    let (status, body) = match path.as_str() {
        "/health" => (200, b"{}".to_vec()),
        "/v1/capabilities" => (
            200,
            serde_json::to_vec(&json!({
                "features": {
                    "run_submission": true,
                    "runs_idempotency": {
                        "supported": true,
                        "durable": true,
                        "retention_seconds": 86400
                    }
                }
            }))
            .expect("capabilities"),
        ),
        "/api/sessions" => (201, b"{}".to_vec()),
        "/v1/runs" => (
            run_status,
            br#"{"run_id":"hermes-run-1","status":"queued"}"#.to_vec(),
        ),
        _ => (404, b"{}".to_vec()),
    };
    let reason = match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .expect("Hermes headers");
    stream.write_all(&body).expect("Hermes body");
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

#[test]
#[cfg(unix)]
fn cli_once_uses_real_journald_and_hermes_handoff_before_ack() {
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
                content: "Hermes inbox integration".into(),
                attention: vec!["destination".into()],
                run_id: None,
                routing_key: Some("default".into()),
                relations: vec![],
                title: None,
            },
        )
        .expect("append record")
        .record;
    let probe = HermesProbe::new(fixture.directory.join("journal.db"), record.id.clone());
    let credential_file = fixture.directory.join("principal.credential");
    let hermes_key_file = fixture.directory.join("hermes.key");
    write_private(
        &credential_file,
        &serde_json::to_string(&wire::OneTimePrincipalClientSecret {
            credential_id: "principal-credential".into(),
            secret: fixture.principal_credential.clone(),
        })
        .expect("principal credential JSON"),
    );
    write_private(&hermes_key_file, "hermes-test-key");
    let routes_file = fixture.directory.join("routes.json");
    std::fs::write(
        &routes_file,
        serde_json::to_vec(&json!({
            "space/default": {
                "key": "default",
                "runtime_target": "private-hermes-session",
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
                "--hermes-base-url",
                &probe.endpoint,
                "--hermes-key-file",
                hermes_key_file.to_str().unwrap(),
                "--once",
            ]
            .into_iter()
            .map(Into::into),
        )
        .expect("inbox CLI config")
    };
    run(config()).expect("inbox once");
    assert!(probe.unacknowledged_before_run.load(Ordering::Acquire));
    assert_eq!(
        probe.requests(),
        ["/health", "/v1/capabilities", "/api/sessions", "/v1/runs"]
    );

    let status = central
        .delivery_status(
            &fixture.principal_credential,
            &record.id,
            &wire::PageQuery::default(),
        )
        .expect("inbox receipts");
    assert_eq!(status.items[0].state, wire::ReceiptState::Acknowledged);
    assert!(
        !serde_json::to_string(&status)
            .unwrap()
            .contains("private-hermes-session")
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
    support::kill_after_handoff(&fixture.directory, &central_endpoint, &probe.endpoint);
    assert!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items[0]
            .acknowledged_at
            .is_none()
    );
    run(config()).unwrap();
    let keys = probe.runs.lock().unwrap().keys.clone();
    assert_eq!(
        keys.iter()
            .filter(|key| **key == format!("agent-journal:{}", item.inbox_item_id))
            .count(),
        2
    );
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
    probe.runs.lock().unwrap().status = 503;
    run(config()).unwrap();
    assert_eq!(
        central
            .inbox(&fixture.principal_credential, &wire::InboxQuery::default())
            .unwrap()
            .items
            .len(),
        1
    );
    probe.runs.lock().unwrap().status = 202;
    run(config()).unwrap();
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
    let before = probe.runs.lock().unwrap().keys.len();
    run(config()).unwrap();
    assert_eq!(probe.runs.lock().unwrap().keys.len(), before);
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
    let key = journal_client::private_file::read(&root.join("hermes.key")).unwrap();
    let runtime = journal_runtime_hermes::HermesRuntime::new(
        &std::env::var("INBOX_RUNTIME_ENDPOINT").unwrap(),
        key,
    )
    .unwrap();
    support::handoff_until_ack(
        root,
        &std::env::var("INBOX_CRASH_ENDPOINT").unwrap(),
        &runtime,
    );
}
