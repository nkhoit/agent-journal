use journal_adapter_core::*;
use journal_adapter_spool::{Limits, SqliteStore};
use journal_client::{Client, HttpTransport};
use journal_protocol as wire;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use std::{
    cell::{Cell, RefCell},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

struct Fixture {
    directory: PathBuf,
    service: BootstrapService,
    database: Database,
    principal: String,
    delivery: String,
    delivery_id: String,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::var_os("AJ_CONFORMANCE_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap().join("target"))
            .join(format!(
                "adapter-http-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let database = Database::open(directory.join("journal.db")).unwrap();
        let service = BootstrapService::new(database.clone());
        service
            .create_principal(&wire::PrincipalCreateRequest {
                handle: "destination".into(),
                display_name: "Destination".into(),
            })
            .unwrap();
        service
            .create_space(&wire::SpaceCreateRequest {
                access: journal_protocol::domain::SpaceAccess::Public,
                id: "space".into(),
                name: "Space".into(),
            })
            .unwrap();
        service
            .set_membership(&wire::MembershipRequest {
                space_id: "space".into(),
                principal_id: "destination".into(),
                can_read: true,
                can_append: true,
                can_admin: false,
            })
            .unwrap();
        service
            .provision_adapter(&wire::AdapterProvisionRequest {
                principal_id: "destination".into(),
                adapter_id: "adapter".into(),
            })
            .unwrap();
        let ticket = service
            .create_ticket(&wire::EnrollmentTicketCreateRequest {
                principal_id: "destination".into(),
                adapter_id: "adapter".into(),
                ttl_seconds: 900,
            })
            .unwrap();
        let enrollment = service
            .exchange(
                &ticket.enrollment_ticket.ticket,
                &wire::EnrollmentExchangeRequest {
                    instance_id: "installation".into(),
                },
            )
            .unwrap();
        Self {
            directory,
            database,
            service,
            principal: enrollment.principal_client_secret.secret,
            delivery: enrollment.delivery_adapter_secret.secret,
            delivery_id: enrollment.delivery_adapter_secret.credential_id,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if std::env::var_os("AJ_CONFORMANCE_STATE_DIR").is_some() {
            return;
        }
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

struct TestClock(Cell<SystemTime>);
impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0.get()
    }
}

struct LostResponses<'a> {
    inner: DeliveryJournal,
    revoke: Box<dyn Fn() + 'a>,
    commits: RefCell<Vec<CustodyRequest>>,
    events: RefCell<Vec<EventRequest>>,
    heartbeats: Cell<usize>,
    revoke_at: Cell<usize>,
}
impl Journal for LostResponses<'_> {
    fn register(&self, request: RegisterRequest) -> CoreResult<Registration> {
        self.inner.register(request)
    }
    fn heartbeat(&self, request: HeartbeatRequest) -> CoreResult<Registration> {
        let count = self.heartbeats.get() + 1;
        self.heartbeats.set(count);
        if count == self.revoke_at.get() {
            (self.revoke)();
        }
        self.inner.heartbeat(request)
    }
    fn claim(&self, request: ClaimRequest) -> CoreResult<ClaimBatch> {
        self.inner.claim(request)
    }
    fn commit_host_custody(&self, request: CustodyRequest) -> CoreResult<CustodyResult> {
        self.commits.borrow_mut().push(request.clone());
        let response = self.inner.commit_host_custody(request)?;
        if self.commits.borrow().len() == 1 {
            return Err(CoreError::JournalUnavailable);
        }
        Ok(response)
    }
    fn record_event(&self, item: &str, request: EventRequest) -> CoreResult<()> {
        self.events.borrow_mut().push(request.clone());
        self.inner.record_event(item, request)?;
        if self.events.borrow().len() == 1 {
            return Err(CoreError::JournalUnavailable);
        }
        Ok(())
    }
}

#[cfg(unix)]
fn revoke_via_admin(socket: &Path, credential_id: &str) {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let body = serde_json::to_vec(&wire::CredentialRevokeRequest {
        credential_id: credential_id.to_owned(),
        reason: None,
    })
    .unwrap();
    let mut stream = UnixStream::connect(socket).unwrap();
    write!(
        stream,
        "POST /v1/admin/credentials/revoke HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(
        response.starts_with("HTTP/1.1 204 "),
        "admin revoke failed: {response}"
    );
}

fn exercise<'a>(f: &'a Fixture, endpoint: &str, revoke: impl Fn() + 'a) {
    let client = Client::new(HttpTransport::new(endpoint).unwrap());
    let input = wire::AppendRecordRequest {
        kind: "note".into(),
        content: "inert addressed record".into(),
        attention: vec!["destination".into()],
        run_id: Some("untrusted\nmetadata".into()),
        routing_key: None,
        relations: vec![],
    };
    assert!(
        client
            .append(&f.delivery, "space", "wrong-class", &input)
            .is_err()
    );
    let posted = client
        .append(&f.principal, "space", "adapter-one", &input)
        .unwrap();
    let journal = LostResponses {
        inner: DeliveryJournal::new(
            Client::new(HttpTransport::new(endpoint).unwrap()),
            f.delivery.clone(),
        ),
        revoke: Box::new(revoke),
        commits: RefCell::new(vec![]),
        events: RefCell::new(vec![]),
        heartbeats: Cell::new(0),
        revoke_at: Cell::new(usize::MAX),
    };
    let clock = TestClock(Cell::new(SystemTime::now()));
    let store = SqliteStore::open(f.directory.join("spool.db"), Limits::default()).unwrap();
    let runtime =
        journal_runtime_fake::FakeRuntime::durable(f.directory.join("acceptances.jsonl")).unwrap();
    let routes: StaticRoutes = [(
        "space/default".into(),
        Route {
            key: "default".into(),
            runtime_target: "private-target".into(),
            enabled: true,
        },
    )]
    .into_iter()
    .collect();
    let mut adapter = Adapter::new(
        &journal,
        &store,
        &routes,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap();
    assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    assert!(runtime.acceptances().is_empty());
    clock.0.set(clock.now() + Duration::from_secs(1));
    assert_eq!(adapter.tick(), Err(CoreError::JournalUnavailable));
    assert_eq!(journal.commits.borrow()[0], journal.commits.borrow()[1]);
    let attempt = runtime.acceptances()[0].envelope.attempt_id.clone();
    assert!(store.get(&attempt).unwrap().pending_event.is_some());
    drop(adapter);
    drop(store);
    let store = SqliteStore::open(f.directory.join("spool.db"), Limits::default()).unwrap();
    clock.0.set(clock.now() + Duration::from_secs(2));
    let mut adapter = Adapter::new(
        &journal,
        &store,
        &routes,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap();
    assert_eq!(adapter.tick().unwrap(), Progress::Worked);
    assert_eq!(journal.events.borrow()[0], journal.events.borrow()[1]);
    let captured = runtime.acceptances();
    assert_eq!(captured.len(), 1);
    let journal_runtime_fake::Acceptance {
        route,
        envelope,
        rendered,
    } = &captured[0];
    assert_eq!(route.runtime_target, "private-target");
    assert_eq!(envelope.record_id, posted.record.id);
    assert_eq!(envelope.body, input.content);
    assert!(rendered.contains("source_run: \"untrusted\\nmetadata\""));
    assert_eq!(rendered, &envelope.render());
    let saved = store.get(&envelope.attempt_id).unwrap();
    assert_eq!(saved.record, Some(posted.record.clone()));
    assert!(saved.pending_event.is_none());
    let status = client
        .delivery_status(&f.principal, &posted.record.id, &wire::PageQuery::default())
        .unwrap();
    assert_eq!(
        status.items[0].state,
        wire::domain::DeliveryState::AdapterReportedRuntimeAccepted
    );
    let serialized = serde_json::to_string(&status).unwrap();
    assert!(!serialized.contains("private-target"));
    drop(captured);
    client
        .append(&f.principal, "space", "adapter-two", &input)
        .unwrap();
    journal.revoke_at.set(journal.heartbeats.get() + 3);
    assert_eq!(adapter.tick(), Err(CoreError::JournalRejected(401)));
    assert_eq!(runtime.acceptances().len(), 1);
}

#[test]
fn real_http_client_spool_runtime_and_revocation() {
    let f = Fixture::new();
    with_server(&f, |endpoint| {
        exercise(&f, endpoint, || {
            f.service.revoke(&f.delivery_id, None).unwrap();
        })
    });
}

fn with_server(f: &Fixture, exercise: impl FnOnce(&str)) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let state = journald::ServiceState::new(f.database.clone(), 2).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let thread = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(listener, journald::public_router(state, 1_048_576))
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
    });
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| exercise(&endpoint)));
    stop.send(()).unwrap();
    thread.join().unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[test]
fn accept_then_crash_http_child() {
    let Some(directory) = std::env::var_os("AJ_HTTP_CRASH_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let endpoint = std::env::var("AJ_HTTP_CRASH_ENDPOINT").unwrap();
    let credential = std::env::var("AJ_HTTP_CRASH_CREDENTIAL").unwrap();
    let journal = DeliveryJournal::new(
        Client::new(HttpTransport::new(&endpoint).unwrap()),
        credential,
    );
    let store = SqliteStore::open(directory.join("spool.db"), Limits::default()).unwrap();
    let runtime =
        journal_runtime_fake::FakeRuntime::durable(directory.join("acceptances.jsonl")).unwrap();
    runtime.set_accept_then_crash(true);
    let routes = crash_routes();
    let clock = TestClock(Cell::new(SystemTime::now()));
    Adapter::new(
        &journal,
        &store,
        &routes,
        &runtime,
        &clock,
        "installation".into(),
    )
    .unwrap()
    .tick()
    .unwrap();
    panic!("runtime did not terminate child");
}

fn crash_routes() -> StaticRoutes {
    [(
        "space/default".into(),
        Route {
            key: "default".into(),
            runtime_target: "private-test-target".into(),
            enabled: true,
        },
    )]
    .into_iter()
    .collect()
}

#[test]
fn real_http_accept_then_crash_recovers_same_attempt_with_duplicate_acceptance() {
    let f = Fixture::new();
    with_server(&f, |endpoint| {
        let client = Client::new(HttpTransport::new(endpoint).unwrap());
        let record = client
            .append(
                &f.principal,
                "space",
                "crash",
                &wire::AppendRecordRequest {
                    kind: "note".into(),
                    content: "inert crash fixture".into(),
                    attention: vec!["destination".into()],
                    run_id: None,
                    routing_key: None,
                    relations: vec![],
                },
            )
            .unwrap()
            .record;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "accept_then_crash_http_child"])
            .env("AJ_HTTP_CRASH_DIRECTORY", &f.directory)
            .env("AJ_HTTP_CRASH_ENDPOINT", endpoint)
            .env("AJ_HTTP_CRASH_CREDENTIAL", &f.delivery)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("runtime acceptance child timed out");
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert_eq!(
            status.code(),
            Some(journal_runtime_fake::ACCEPT_THEN_CRASH_EXIT)
        );
        let runtime =
            journal_runtime_fake::FakeRuntime::durable(f.directory.join("acceptances.jsonl"))
                .unwrap();
        let first = runtime.acceptances()[0].clone();
        let accepted_before_crash = runtime.acceptances().len();
        assert_eq!(accepted_before_crash, 1);
        let store = SqliteStore::open(f.directory.join("spool.db"), Limits::default()).unwrap();
        let saved = store.get(&first.envelope.attempt_id).unwrap();
        assert!(saved.custody_confirmed);
        assert_eq!(saved.injection_state, InjectionState::InFlight);
        assert_eq!(
            client
                .delivery_status(&f.principal, &record.id, &wire::PageQuery::default())
                .unwrap()
                .items[0]
                .state,
            wire::domain::DeliveryState::HostAccepted
        );
        if std::env::var_os("AJ_CONFORMANCE_STATE_DIR").is_some() {
            std::fs::copy(
                f.directory.join("spool.db"),
                f.directory.join("before-recovery.db"),
            )
            .unwrap();
        }
        let journal = DeliveryJournal::new(
            Client::new(HttpTransport::new(endpoint).unwrap()),
            f.delivery.clone(),
        );
        let clock = TestClock(Cell::new(SystemTime::now()));
        let routes = crash_routes();
        Adapter::new(
            &journal,
            &store,
            &routes,
            &runtime,
            &clock,
            "installation".into(),
        )
        .unwrap()
        .tick()
        .unwrap();
        assert_eq!(runtime.acceptances().len(), 2);
        assert_eq!(runtime.acceptances()[1], first);
        assert_eq!(
            store
                .get(&first.envelope.attempt_id)
                .unwrap()
                .injection_state,
            InjectionState::Accepted
        );
        assert_eq!(
            client
                .delivery_status(&f.principal, &record.id, &wire::PageQuery::default())
                .unwrap()
                .items[0]
                .state,
            wire::domain::DeliveryState::AdapterReportedRuntimeAccepted
        );
        if std::env::var_os("AJ_CONFORMANCE_STATE_DIR").is_some() {
            std::fs::write(
                f.directory.join("crash-evidence.json"),
                serde_json::to_vec(&serde_json::json!({
                    "phase": "runtime-accepted", "child_terminated": !status.success(),
                    "accepted_before_crash": accepted_before_crash,
                    "recovery_injections": runtime.acceptances().len() - accepted_before_crash
                }))
                .unwrap(),
            )
            .unwrap();
        }
    });
}

#[cfg(unix)]
#[test]
fn temporary_journald_process_with_real_spool() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let f = Fixture::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    use std::os::unix::fs::PermissionsExt;
    let socket_directory = std::env::temp_dir().join(format!("aj-admin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&socket_directory);
    std::fs::create_dir(&socket_directory).unwrap();
    std::fs::set_permissions(&socket_directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = socket_directory.join("admin.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_journald"))
        .current_dir(&f.directory)
        .arg("--database")
        .arg("journal.db")
        .arg("--admin-socket")
        .arg(&socket)
        .arg("--listen")
        .arg(address.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !socket.exists() || std::net::TcpStream::connect(address).is_err() {
        if Instant::now() > deadline || child.try_wait().unwrap().is_some() {
            let _ = child.kill();
            let _ = child.wait();
            use std::io::Read;
            let mut diagnostic = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut diagnostic)
                .unwrap();
            panic!("journald did not become ready: {diagnostic}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let credential_id = f.delivery_id.clone();
        exercise(&f, &format!("http://{address}"), || {
            revoke_via_admin(&socket, &credential_id);
        });
    }));
    child.kill().unwrap();
    child.wait().unwrap();
    let _ = std::fs::remove_dir_all(&socket_directory);
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
