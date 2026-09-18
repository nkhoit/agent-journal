use journal_adapter_core::{InjectionState, Spool};
use journal_adapter_hermes::{Config, run};
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
    delivery_credential: String,
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
        service
            .create_principal(&wire::PrincipalCreateRequest {
                id: "destination".into(),
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

struct HermesProbe {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    custody_before_run: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    wake: std::net::SocketAddr,
    thread: Option<JoinHandle<()>>,
}

impl HermesProbe {
    fn new(central_endpoint: String, principal: String, record_id: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Hermes listener");
        let address = listener.local_addr().expect("Hermes address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let custody_before_run = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = Arc::clone(&requests);
        let thread_custody = Arc::clone(&custody_before_run);
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
                    &thread_custody,
                    &central_endpoint,
                    &principal,
                    &record_id,
                );
            }
        });
        Self {
            endpoint: format!("http://{address}"),
            requests,
            custody_before_run,
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
    custody_before_run: &Arc<AtomicBool>,
    central_endpoint: &str,
    principal: &str,
    record_id: &str,
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
        let client = Client::new(HttpTransport::new(central_endpoint).expect("central transport"));
        let page = client
            .delivery_status(principal, record_id, &wire::PageQuery::default())
            .expect("central status while injecting");
        custody_before_run.store(
            page.items
                .first()
                .is_some_and(|item| item.state == wire::domain::DeliveryState::HostAccepted),
            Ordering::Release,
        );
    }
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
            202,
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

#[test]
#[cfg(unix)]
fn cli_once_uses_real_journald_spool_and_hermes_runs_ordering() {
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
                content: "Hermes adapter integration".into(),
                attention: vec!["destination".into()],
                run_id: None,
                routing_key: Some("default".into()),
                relations: vec![],
            },
        )
        .expect("append record")
        .record;
    let probe = HermesProbe::new(
        central_endpoint.clone(),
        fixture.principal_credential.clone(),
        record.id.clone(),
    );
    let delivery_file = fixture.directory.join("delivery.credential");
    let hermes_key_file = fixture.directory.join("hermes.key");
    write_private(
        &delivery_file,
        &serde_json::to_string(&wire::OneTimeDeliveryAdapterSecret {
            credential_id: "delivery-credential".into(),
            secret: fixture.delivery_credential.clone(),
        })
        .expect("delivery credential JSON"),
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
    let config = Config::parse(
        [
            "--central-endpoint",
            &central_endpoint,
            "--delivery-credential-file",
            delivery_file.to_str().unwrap(),
            "--spool-db",
            fixture.directory.join("spool.db").to_str().unwrap(),
            "--instance-id",
            "installation",
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
    .expect("adapter CLI config");
    run(config).expect("adapter once");
    assert!(probe.custody_before_run.load(Ordering::Acquire));
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
        .expect("delivery telemetry");
    assert_eq!(
        status.items[0].state,
        wire::domain::DeliveryState::AdapterReportedRuntimeAccepted
    );
    let attempt_id = status.items[0].last_attempt_id.clone().expect("attempt id");
    let spool = SqliteStore::open(fixture.directory.join("spool.db"), Limits::default())
        .expect("spool reopen");
    let item = spool.get(&attempt_id).expect("persisted spool item");
    assert_eq!(item.injection_state, InjectionState::Accepted);
    assert_eq!(item.runtime_receipt, "hermes-run-1");
    assert!(
        !serde_json::to_string(&status)
            .unwrap()
            .contains("private-hermes-session")
    );

    journald.kill().expect("stop journald");
    journald.wait().expect("wait journald");
}
