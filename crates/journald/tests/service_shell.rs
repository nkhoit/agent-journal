#![cfg(unix)]

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use journal_storage_sqlite::{Database, StorageError};
use journald::{
    BlockingError, BlockingExecutor, Config, Server, ServerError, ServiceState, admin_router,
    public_router,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tower::ServiceExt;

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-journal-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create temporary directory");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("protect temporary directory");
        Self { path }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    /// The recovery audit must live outside the database's directory.
    fn audit(&self) -> PathBuf {
        use std::os::unix::fs::DirBuilderExt;
        let directory = self.path("audit");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&directory)
            .expect("create audit directory");
        directory.join("journal.recovery.db")
    }
}

#[derive(Debug, PartialEq, Eq)]
struct StartupArtifactState {
    present: bool,
    bytes: Option<Vec<u8>>,
    device: Option<u64>,
    inode: Option<u64>,
    modified_seconds: Option<i64>,
    modified_nanoseconds: Option<i64>,
}

fn startup_artifact_state(path: &std::path::Path) -> StartupArtifactState {
    match fs::symlink_metadata(path) {
        Ok(metadata) => StartupArtifactState {
            present: true,
            bytes: metadata
                .file_type()
                .is_file()
                .then(|| fs::read(path).unwrap()),
            device: Some(metadata.dev()),
            inode: Some(metadata.ino()),
            modified_seconds: Some(metadata.mtime()),
            modified_nanoseconds: Some(metadata.mtime_nsec()),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => StartupArtifactState {
            present: false,
            bytes: None,
            device: None,
            inode: None,
            modified_seconds: None,
            modified_nanoseconds: None,
        },
        Err(error) => panic!("inspect startup artifact {path:?}: {error}"),
    }
}

fn sidecar(socket: &std::path::Path, suffix: &str) -> PathBuf {
    let mut name = socket.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn write_private(path: &std::path::Path, bytes: &[u8]) {
    fs::write(path, bytes).expect("write private fixture");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("protect private fixture");
}

fn owner_lock_identity(socket: &std::path::Path) -> (u64, u64) {
    let lock_path = sidecar(socket, ".lock");
    write_private(&lock_path, b"");
    let metadata = fs::symlink_metadata(lock_path).expect("stat owner lock fixture");
    (metadata.dev(), metadata.ino())
}

fn published_marker(lock_device: u64, lock_inode: u64, device: u64, inode: u64) -> Vec<u8> {
    format!(
        "version=2\nstate=published\ntype=socket\nlock-dev={lock_device}\nlock-ino={lock_inode}\ndev={device}\nino={inode}\n"
    )
    .into_bytes()
}

fn marker_hex_name(name: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(name.len() * 2);
    for byte in name.bytes() {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    encoded
}

impl Drop for TempDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("remove temporary directory");
    }
}

fn state(temporary: &TempDir, blocking_limit: usize) -> ServiceState {
    let database = Database::open(temporary.path("journal.db")).expect("open database");
    ServiceState::new(database, blocking_limit).expect("create service state")
}

async fn json_response(
    router: axum::Router,
    request: Request<Body>,
) -> (StatusCode, String, Value) {
    let response = router.oneshot(request).await.expect("router response");
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .expect("response request ID")
        .to_str()
        .expect("request ID text")
        .to_owned();
    let body = to_bytes(response.into_body(), 1_048_576)
        .await
        .expect("response body");
    let value = serde_json::from_slice(&body).expect("JSON response");
    (status, request_id, value)
}

#[tokio::test]
async fn live_and_ready_health_responses_follow_the_wire_contract() {
    let temporary = TempDir::new("s3-health");
    let router = public_router(state(&temporary, 1), 1024);

    let (status, _, live) = json_response(
        router.clone(),
        Request::get("/health/live").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(live["status"], "ok");
    assert_eq!(live["version"], env!("CARGO_PKG_VERSION"));

    let (status, _, ready) = json_response(
        router,
        Request::get("/health/ready").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ready["status"], "ok");
    assert_eq!(ready["checks"]["database"], "ok");
}

#[tokio::test]
async fn public_router_hides_every_administrative_path() {
    let temporary = TempDir::new("s3-public-isolation");
    let router = public_router(state(&temporary, 1), 1024);
    let paths = [
        "/v1/admin/enrollment-tickets",
        "/v1/admin/adapters",
        "/v1/admin/adapters/adapter-1/replace",
        "/v1/admin/mailboxes/principal-1/status",
        "/v1/admin/principals",
        "/v1/admin/spaces",
        "/v1/admin/memberships",
        "/v1/admin/credentials/rotate",
        "/v1/admin/credentials/revoke",
        "/v1/admin/enrollment/recover",
        "/v1/admin/mailbox-items/item-1/requeue",
    ];

    for path in paths {
        let (status, request_id, body) = json_response(
            router.clone(),
            Request::post(path).body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "public path {path}");
        assert_eq!(body["error"]["code"], "not-found");
        assert_eq!(body["error"]["request_id"], request_id);
    }
}

#[tokio::test]
async fn oversized_bodies_use_the_common_error_envelope() {
    let temporary = TempDir::new("s3-body-limit");
    let router = public_router(state(&temporary, 1), 32);
    let (status, request_id, body) = json_response(
        router,
        Request::post("/v1/not-implemented")
            .body(Body::from(vec![b'x'; 33]))
            .unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "payload-too-large");
    assert_eq!(body["error"]["request_id"], request_id);
}

#[tokio::test]
async fn non_json_health_bodies_are_bounded() {
    let temporary = TempDir::new("s3-health-body-limit");
    let router = public_router(state(&temporary, 1), 4);
    for path in ["/health/live", "/health/ready"] {
        let (status, request_id, body) = json_response(
            router.clone(),
            Request::get(path)
                .header("content-type", "text/plain")
                .body(Body::from("12345"))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"]["code"], "payload-too-large");
        assert_eq!(body["error"]["request_id"], request_id);
    }
}

#[tokio::test]
async fn method_errors_use_the_common_error_envelope() {
    let temporary = TempDir::new("s3-method-error");
    let router = public_router(state(&temporary, 1), 1024);
    let (status, request_id, body) = json_response(
        router,
        Request::post("/health/live").body(Body::empty()).unwrap(),
    )
    .await;

    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body["error"]["code"], "method-not-allowed");
    assert_eq!(body["error"]["request_id"], request_id);
}

#[tokio::test]
async fn malformed_and_duplicate_key_json_use_the_common_error_envelope() {
    let temporary = TempDir::new("s3-invalid-json");
    let router = public_router(state(&temporary, 1), 1024);

    for body in [r#"{"unterminated":"#, r#"{"key":1,"key":2}"#] {
        let (status, request_id, response) = json_response(
            router.clone(),
            Request::post("/v1/not-implemented")
                .header("content-type", "application/json; charset=utf-8")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(response["error"]["code"], "invalid-json");
        assert_eq!(response["error"]["request_id"], request_id);
    }

    let (status, _, response) = json_response(
        router,
        Request::post("/v1/not-implemented")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(response["error"]["code"], "not-found");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_executor_refuses_work_beyond_its_bound() {
    let temporary = TempDir::new("s3-blocking-bound");
    let database = Database::open(temporary.path("journal.db")).expect("open database");
    let executor = BlockingExecutor::new(database, 1).expect("create executor");
    let occupied = executor.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let running = tokio::spawn(async move {
        occupied
            .execute(move |_| {
                started_tx.send(()).expect("report blocking start");
                release_rx.recv().expect("release blocking operation");
                Ok(())
            })
            .await
    });
    started_rx.await.expect("blocking operation started");

    assert!(matches!(
        executor.execute(|_| Ok(())).await,
        Err(BlockingError::AtCapacity)
    ));
    release_tx.send(()).expect("release blocking operation");
    running
        .await
        .expect("blocking task join")
        .expect("blocking operation result");
}

#[test]
fn blocking_executor_accepts_its_maximum_limit_and_rejects_one_over() {
    let temporary = TempDir::new("s3-blocking-max");
    let database = Database::open(temporary.path("journal.db")).expect("open database");
    let maximum = tokio::sync::Semaphore::MAX_PERMITS;
    assert!(BlockingExecutor::new(database.clone(), maximum).is_ok());
    assert!(matches!(
        BlockingExecutor::new(database, maximum + 1),
        Err(BlockingError::InvalidLimit)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_refuses_work_when_blocking_capacity_is_exhausted() {
    let temporary = TempDir::new("s3-readiness-capacity");
    let state = state(&temporary, 1);
    let occupied = state.blocking().clone();
    let router = public_router(state, 1024);
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let running = tokio::spawn(async move {
        occupied
            .execute(move |_| {
                started_tx.send(()).expect("report blocking start");
                release_rx.recv().expect("release blocking operation");
                Ok(())
            })
            .await
    });
    started_rx.await.expect("blocking operation started");

    let (status, request_id, body) = json_response(
        router,
        Request::get("/health/ready").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "capacity-unavailable");
    assert_eq!(body["error"]["request_id"], request_id);

    release_tx.send(()).expect("release blocking operation");
    running
        .await
        .expect("blocking task join")
        .expect("blocking operation result");
}

#[tokio::test]
async fn readiness_fails_closed_without_leaking_storage_details() {
    let temporary = TempDir::new("s3-readiness-failure");
    let database_path = temporary.path("journal.db");
    let router = public_router(state(&temporary, 1), 1024);
    fs::remove_file(&database_path).expect("remove database");

    let (status, request_id, body) = json_response(
        router.clone(),
        Request::get("/health/ready").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "service-unavailable");
    assert_eq!(body["error"]["request_id"], request_id);
    assert_eq!(
        body["error"]["message"],
        "required service dependency is unavailable"
    );
    assert!(!body.to_string().contains(database_path.to_str().unwrap()));

    let (status, _, live) = json_response(
        router,
        Request::get("/health/live").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(live["status"], "ok");
}

async fn tcp_request(address: SocketAddr, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(address).await.expect("connect TCP");
    stream.write_all(request).await.expect("write TCP request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read TCP response");
    String::from_utf8(response).expect("UTF-8 TCP response")
}

async fn unix_request(path: &std::path::Path, request: &[u8]) -> String {
    let mut stream = UnixStream::connect(path)
        .await
        .expect("connect Unix socket");
    stream.write_all(request).await.expect("write Unix request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read Unix response");
    String::from_utf8(response).expect("UTF-8 Unix response")
}

async fn begin_incomplete_json_body(address: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.expect("connect TCP");
    stream
        .write_all(
            b"POST /v1/not-implemented HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 64\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("write incomplete request headers");

    let mut interim = Vec::new();
    timeout(Duration::from_secs(1), async {
        let mut buffer = [0_u8; 128];
        while !interim.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream
                .read(&mut buffer)
                .await
                .expect("read interim response");
            assert!(read > 0, "connection closed before 100 Continue");
            interim.extend_from_slice(&buffer[..read]);
        }
    })
    .await
    .expect("wait for 100 Continue");
    assert!(
        String::from_utf8_lossy(&interim).starts_with("HTTP/1.1 100 Continue"),
        "{}",
        String::from_utf8_lossy(&interim)
    );
    stream
        .write_all(b"{")
        .await
        .expect("write partial request body");
    stream
}

/// A free loopback address below the Linux (32768+) and macOS (49152+)
/// ephemeral ranges, so port-0 binds elsewhere in the suite are never handed it.
fn non_ephemeral_loopback() -> SocketAddr {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let start = u64::from(std::process::id()) * 7;
    for _ in 0..12_000 {
        let port = 20_000 + (start + NEXT.fetch_add(1, Ordering::Relaxed)) % 12_000;
        let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port as u16));
        if std::net::TcpListener::bind(address).is_ok() {
            return address;
        }
    }
    panic!("no free non-ephemeral loopback port");
}

fn config(temporary: &TempDir) -> Config {
    Config {
        web: None,
        database_path: temporary.path("journal.db"),
        recovery_audit_path: temporary.audit(),
        public_address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
        admin_socket_path: temporary.path("admin.sock"),
        blocking_limit: 2,
        max_body_bytes: 1024,
        body_read_timeout: Duration::from_millis(200),
        shutdown_timeout: Duration::from_millis(500),
    }
}

#[tokio::test]
async fn daemon_recovery_refusal_preserves_current_central_before_rw_open() {
    let temporary = TempDir::new("rec-admit");
    let settings = config(&temporary);
    let audit = settings.recovery_audit_path.clone();
    drop(
        Database::open_protected(&settings.database_path, &audit)
            .expect("initialize protected central"),
    );
    fs::remove_file(&audit).expect("remove required audit");
    let artifacts = [
        settings.database_path.clone(),
        PathBuf::from(format!("{}-wal", settings.database_path.display())),
        PathBuf::from(format!("{}-shm", settings.database_path.display())),
        PathBuf::from(format!("{}-journal", settings.database_path.display())),
        audit.clone(),
        audit.with_extension("recovery-lock"),
    ];
    let before = artifacts
        .iter()
        .map(|path| (path.clone(), startup_artifact_state(path)))
        .collect::<Vec<_>>();

    let result = Server::bind(settings).await;
    match result {
        Err(error) => assert!(
            matches!(
                error,
                ServerError::Database(StorageError::RecoveryClosed(
                    "required external audit is missing"
                ))
            ),
            "unexpected daemon refusal: {error:?}"
        ),
        Ok(_) => panic!("daemon unexpectedly bound without its required audit"),
    }
    let after = artifacts
        .iter()
        .map(|path| (path.clone(), startup_artifact_state(path)))
        .collect::<Vec<_>>();
    assert_eq!(after, before, "daemon startup changed recovery evidence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_web_listener_is_opt_in_isolated_and_checks_viewer() {
    let temporary = TempDir::new("web-server");
    let mut configuration = config(&temporary);
    let disabled = Server::bind(configuration.clone()).await.unwrap();
    assert!(disabled.web_address().unwrap().is_none());
    drop(disabled);
    configuration.web = Some(journald::WebConfig {
        address: "127.0.0.1:0".parse().unwrap(),
        viewer: "viewer".into(),
    });
    assert!(Server::bind(configuration.clone()).await.is_err());
    let db = Database::open_protected(
        temporary.path("journal.db"),
        &configuration.recovery_audit_path,
    )
    .unwrap();
    journal_service::BootstrapService::new(db.clone())
        .create_principal(&journal_protocol::PrincipalCreateRequest {
            handle: "viewer".into(),
            display_name: "Viewer".into(),
        })
        .unwrap();
    drop(db);
    let server = Server::bind(configuration).await.unwrap();
    let public_address = server.public_address();
    let web_address = server.web_address().unwrap().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(async move {
        let _ = shutdown_rx.await;
    }));
    for (address, path, status) in [
        (web_address, "/web", "200"),
        (public_address, "/web", "404"),
        (web_address, "/v1/spaces", "404"),
        (web_address, "/v1/admin/principals", "404"),
        (web_address, "/v1/admin/metrics", "404"),
    ] {
        let response = tcp_request(
            address,
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}")),
            "{response}"
        );
    }

    shutdown_tx.send(()).unwrap();
    serving.await.unwrap().unwrap();
    assert!(TcpStream::connect(web_address).await.is_err());
}

#[tokio::test]
async fn closed_recovery_prevents_shared_viewer_startup() {
    let temporary = TempDir::new("web-recovery-startup");
    let mut configuration = config(&temporary);
    configuration.web = Some(journald::WebConfig {
        address: "127.0.0.1:0".parse().unwrap(),
        viewer: "viewer".into(),
    });
    let db = Database::open_protected(
        &configuration.database_path,
        configuration.recovery_audit_path.clone(),
    )
    .unwrap();
    journal_service::BootstrapService::new(db.clone())
        .create_principal(&journal_protocol::PrincipalCreateRequest {
            handle: "viewer".into(),
            display_name: "Viewer".into(),
        })
        .unwrap();
    db.recovery_audit().unwrap().close().unwrap();
    drop(db);
    let result = Server::bind(configuration).await;
    let unexpected_error = result.as_ref().err();
    assert!(
        matches!(
            &result,
            Err(ServerError::Database(StorageError::RecoveryClosed(_)))
        ),
        "unexpected server bind error: {unexpected_error:?}"
    );
    assert!(!temporary.path("admin.sock").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_server_serves_both_transports_survives_disconnect_and_shuts_down() {
    let temporary = TempDir::new("s3-server");
    let server = Server::bind(config(&temporary)).await.expect("bind server");
    let address = server.public_address();
    let admin_socket = server.admin_socket_path().to_owned();
    assert_eq!(
        fs::metadata(&admin_socket)
            .expect("admin socket metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(async move {
        let _ = shutdown_rx.await;
    }));

    let mut abandoned = TcpStream::connect(address)
        .await
        .expect("connect abandoned client");
    abandoned
        .write_all(b"GET /health/live HTTP/1.1\r\nHost:")
        .await
        .expect("write partial request");
    drop(abandoned);

    let public = tcp_request(
        address,
        b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(public.starts_with("HTTP/1.1 200 OK"), "{public}");
    assert!(public.contains("x-request-id:"), "{public}");

    let admin = unix_request(
        &admin_socket,
        b"POST /v1/admin/principals HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(admin.starts_with("HTTP/1.1 400 Bad Request"), "{admin}");

    shutdown_tx.send(()).expect("request shutdown");
    timeout(Duration::from_secs(2), serving)
        .await
        .expect("bounded graceful shutdown")
        .expect("server task")
        .expect("server result");
    assert!(!admin_socket.exists(), "admin socket removed on shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_request_body_has_a_finite_read_deadline() {
    let temporary = TempDir::new("body-deadline");
    let mut settings = config(&temporary);
    settings.body_read_timeout = Duration::from_millis(50);
    let server = Server::bind(settings).await.expect("bind server");
    let address = server.public_address();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(async move {
        let _ = shutdown_rx.await;
    }));
    let mut stream = begin_incomplete_json_body(address).await;

    let mut response = Vec::new();
    timeout(
        Duration::from_millis(500),
        stream.read_to_end(&mut response),
    )
    .await
    .expect("bounded body read response")
    .expect("read body timeout response");
    let response = String::from_utf8(response).expect("UTF-8 body timeout response");
    assert!(
        response.starts_with("HTTP/1.1 408 Request Timeout"),
        "{response}"
    );
    assert!(
        response.contains("\"code\":\"request-timeout\""),
        "{response}"
    );

    shutdown_tx.send(()).expect("request shutdown");
    timeout(Duration::from_secs(1), serving)
        .await
        .expect("bounded shutdown")
        .expect("server task")
        .expect("server result");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_cancels_an_incomplete_request_body() {
    let temporary = TempDir::new("body-shutdown");
    let mut settings = config(&temporary);
    settings.body_read_timeout = Duration::from_secs(10);
    settings.shutdown_timeout = Duration::from_millis(100);
    let server = Server::bind(settings).await.expect("bind server");
    let address = server.public_address();
    let admin_socket = server.admin_socket_path().to_owned();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(async move {
        let _ = shutdown_rx.await;
    }));
    let stream = begin_incomplete_json_body(address).await;

    shutdown_tx.send(()).expect("request shutdown");
    timeout(Duration::from_millis(500), serving)
        .await
        .expect("bounded shutdown with incomplete body")
        .expect("server task")
        .expect("server result");
    assert!(
        !admin_socket.exists(),
        "admin socket removed after shutdown"
    );
    drop(stream);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_shutdown_does_not_normalize_hot_database_state() {
    let temporary = TempDir::new("forced-hot-db");
    let database = temporary.path("journal.db");
    let mut settings = config(&temporary);
    settings.body_read_timeout = Duration::from_secs(10);
    settings.shutdown_timeout = Duration::from_millis(100);
    let server = Server::bind(settings).await.expect("bind server");
    let address = server.public_address();
    let admin_socket = server.admin_socket_path().to_owned();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(async move {
        let _ = shutdown_rx.await;
    }));
    let created = unix_request(
        &admin_socket,
        b"POST /v1/admin/principals HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 50\r\nConnection: close\r\n\r\n{\"handle\":\"forced-writer\",\"display_name\":\"Forced\"}",
    )
    .await;
    assert!(created.starts_with("HTTP/1.1 201 Created"), "{created}");
    let ready = tcp_request(
        address,
        b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(ready.starts_with("HTTP/1.1 200 OK"), "{ready}");
    let lock = rusqlite::Connection::open(&database).expect("open external lock holder");
    lock.execute_batch("BEGIN IMMEDIATE;")
        .expect("hold a central writer lock");
    let blocked_body = br#"{"handle":"blocked-writer","display_name":"Blocked"}"#;
    let blocked_request = format!(
        "POST /v1/admin/principals HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        blocked_body.len(),
        std::str::from_utf8(blocked_body).unwrap()
    )
    .into_bytes();
    let blocked_socket = admin_socket.clone();
    let mut blocked =
        tokio::spawn(async move { unix_request(&blocked_socket, &blocked_request).await });
    assert!(
        timeout(Duration::from_millis(50), &mut blocked)
            .await
            .is_err(),
        "the write must still be blocked when shutdown begins"
    );

    shutdown_tx.send(()).expect("request shutdown");
    let result = timeout(Duration::from_millis(500), serving)
        .await
        .expect("forced shutdown must return without checkpointing through an active writer")
        .expect("server task");
    assert!(
        result.is_err(),
        "forced shutdown must be reported as a failure"
    );
    assert!(
        ["-wal", "-shm", "-journal"]
            .into_iter()
            .map(|suffix| sidecar(&database, suffix))
            .any(|path| fs::symlink_metadata(path).is_ok()),
        "forced shutdown must leave hot state for fail-closed cold admission"
    );
    lock.execute_batch("ROLLBACK;")
        .expect("release external writer lock");
    drop(lock);
    blocked.abort();
    let _ = blocked.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunked_health_bodies_are_bounded() {
    let temporary = TempDir::new("chunk-limit");
    let mut settings = config(&temporary);
    settings.max_body_bytes = 4;
    let server = Server::bind(settings).await.expect("bind server");
    let address = server.public_address();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(async move {
        let _ = shutdown_rx.await;
    }));

    let response = tcp_request(
        address,
        b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\n12345\r\n0\r\n\r\n",
    )
    .await;
    assert!(
        response.starts_with("HTTP/1.1 413 Payload Too Large"),
        "{response}"
    );

    shutdown_tx.send(()).expect("request shutdown");
    timeout(Duration::from_secs(1), serving)
        .await
        .expect("bounded shutdown")
        .expect("server task")
        .expect("server result");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_socket_publication_keeps_pending_path_within_macos_budget() {
    let temporary = TempDir::new("socket-budget");
    let mut largest = None;
    for length in 1..=256 {
        let candidate = temporary.path(&"s".repeat(length));
        match std::os::unix::net::UnixListener::bind(&candidate) {
            Ok(listener) => {
                drop(listener);
                fs::remove_file(&candidate).expect("remove direct socket probe");
                largest = Some((length, candidate));
            }
            Err(_) if largest.is_some() => break,
            Err(_) => {}
        }
    }
    let (length, final_path) = largest.expect("find a direct Unix socket path boundary");
    assert!(
        std::os::unix::net::UnixListener::bind(&final_path).is_ok(),
        "the selected final path must bind directly"
    );
    fs::remove_file(&final_path).expect("remove final direct socket probe");
    assert!(
        std::os::unix::net::UnixListener::bind(temporary.path(&"s".repeat(length + 1))).is_err(),
        "the next longer path should exceed the platform socket budget"
    );

    let mut settings = config(&temporary);
    settings.admin_socket_path = final_path.clone();
    let server = Server::bind(settings)
        .await
        .expect("bind at socket boundary");
    assert_eq!(server.admin_socket_path(), final_path.as_path());
    let socket_path = server.admin_socket_path().to_owned();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(async move {
        let _ = shutdown_rx.await;
    }));
    let response = unix_request(
        &socket_path,
        b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    shutdown_tx.send(()).expect("request boundary shutdown");
    serving
        .await
        .expect("boundary server task")
        .expect("boundary server result");

    for name in ["a", "ab", "abc"] {
        let mut settings = config(&temporary);
        settings.admin_socket_path = temporary.path(name);
        let server = Server::bind(settings)
            .await
            .expect("bind short final socket filename");
        assert!(std::os::unix::net::UnixStream::connect(server.admin_socket_path()).is_ok());
        drop(server);
    }
}

#[tokio::test]
async fn bounded_pending_name_exhaustion_fails_closed() {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
    let temporary = TempDir::new("pending-exhaustion");
    for byte in ALPHABET {
        let name = std::str::from_utf8(std::slice::from_ref(byte)).expect("ASCII candidate");
        write_private(&temporary.path(name), b"occupied");
    }
    let mut settings = config(&temporary);
    settings.admin_socket_path = temporary.path("!");
    let error = match Server::bind(settings).await {
        Ok(_) => panic!("all bounded pending candidates were occupied"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("administrative pending socket name candidates exhausted"),
        "unexpected exhaustion error: {error}"
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn saturated_active_listener_is_rejected_and_preserved_without_connect_probe() {
    let temporary = TempDir::new("saturated-listener");
    let socket_path = temporary.path("admin.sock");
    let occupied =
        std::os::unix::net::UnixListener::bind(&socket_path).expect("bind active listener");
    let mut clients = Vec::new();
    let mut refused = false;
    for _ in 0..256 {
        match std::os::unix::net::UnixStream::connect(&socket_path) {
            Ok(client) => clients.push(client),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                refused = true;
                break;
            }
            Err(error) => panic!("unexpected saturated-listener connect error: {error}"),
        }
    }
    assert!(refused, "the active listener backlog must be saturated");
    assert!(Server::bind(config(&temporary)).await.is_err());
    assert!(
        socket_path.exists(),
        "rejected bind preserves the active listener path"
    );
    drop(clients);
    drop(occupied);
}

#[tokio::test]
async fn concurrent_server_is_rejected_by_the_admin_owner_lock() {
    let temporary = TempDir::new("admin-owner-lock");
    let first = Server::bind(config(&temporary))
        .await
        .expect("bind first server");
    let socket_path = first.admin_socket_path().to_owned();
    let second = Server::bind(config(&temporary)).await;
    assert!(matches!(second, Err(ServerError::BindAdmin { .. })));
    assert!(
        socket_path.exists(),
        "the live owner's socket was preserved"
    );
    assert!(std::os::unix::net::UnixStream::connect(&socket_path).is_ok());
    drop(first);
}

#[tokio::test]
async fn replacing_the_owner_lock_path_fails_closed_and_preserves_the_live_socket() {
    let temporary = TempDir::new("lock-pin");
    let first = Server::bind(config(&temporary))
        .await
        .expect("bind first server");
    let socket_path = first.admin_socket_path().to_owned();
    let lock_path = sidecar(&socket_path, ".lock");
    fs::remove_file(&lock_path).expect("remove owner lock pathname");
    write_private(&lock_path, b"replacement lock inode");

    let second = Server::bind(config(&temporary)).await;
    assert!(matches!(second, Err(ServerError::BindAdmin { .. })));
    assert!(socket_path.exists(), "the first socket path was preserved");
    assert!(
        std::os::unix::net::UnixStream::connect(&socket_path).is_ok(),
        "the first listener remains reachable"
    );
    drop(first);
    assert!(
        socket_path.exists(),
        "a replaced lock path must prevent cleanup of the first socket"
    );
}

#[tokio::test]
async fn unmarked_and_invalid_admin_artifacts_fail_closed() {
    let regular = TempDir::new("admin-regular");
    let regular_socket = regular.path("admin.sock");
    fs::write(&regular_socket, b"regular sentinel").expect("write regular artifact");
    assert!(Server::bind(config(&regular)).await.is_err());
    assert_eq!(fs::read(&regular_socket).unwrap(), b"regular sentinel");

    let directory = TempDir::new("admin-directory");
    let directory_socket = directory.path("admin.sock");
    fs::create_dir(&directory_socket).expect("create directory artifact");
    assert!(Server::bind(config(&directory)).await.is_err());
    assert!(directory_socket.is_dir());

    let symlink = TempDir::new("admin-symlink");
    let symlink_socket = symlink.path("admin.sock");
    let symlink_target = symlink.path("target");
    fs::write(&symlink_target, b"symlink target").expect("write symlink target");
    std::os::unix::fs::symlink(&symlink_target, &symlink_socket).expect("create symlink artifact");
    assert!(Server::bind(config(&symlink)).await.is_err());
    assert!(symlink_socket.is_symlink());
    assert_eq!(fs::read(&symlink_target).unwrap(), b"symlink target");

    let invalid = TempDir::new("admin-invalid-marker");
    let invalid_marker = sidecar(&invalid.path("admin.sock"), ".marker");
    write_private(&invalid_marker, b"invalid marker\n");
    assert!(Server::bind(config(&invalid)).await.is_err());
    assert_eq!(fs::read(&invalid_marker).unwrap(), b"invalid marker\n");

    let oversized = TempDir::new("admin-oversized-marker");
    let oversized_marker = sidecar(&oversized.path("admin.sock"), ".marker");
    let oversized_bytes = vec![b'x'; 4097];
    write_private(&oversized_marker, &oversized_bytes);
    assert!(Server::bind(config(&oversized)).await.is_err());
    assert_eq!(fs::read(&oversized_marker).unwrap(), oversized_bytes);

    let changed = TempDir::new("identity");
    let changed_socket = changed.path("admin.sock");
    let listener =
        std::os::unix::net::UnixListener::bind(&changed_socket).expect("bind identity fixture");
    let identity = fs::symlink_metadata(&changed_socket).expect("stat identity fixture");
    drop(listener);
    fs::remove_file(&changed_socket).expect("remove identity fixture");
    fs::write(&changed_socket, b"replacement").expect("write changed identity");
    let changed_marker = sidecar(&changed_socket, ".marker");
    let (changed_lock_dev, changed_lock_ino) = owner_lock_identity(&changed_socket);
    write_private(
        &changed_marker,
        &published_marker(
            changed_lock_dev,
            changed_lock_ino,
            identity.dev(),
            identity.ino(),
        ),
    );
    assert!(Server::bind(config(&changed)).await.is_err());
    assert_eq!(fs::read(&changed_socket).unwrap(), b"replacement");
    assert_eq!(
        fs::read(&changed_marker).unwrap(),
        published_marker(
            changed_lock_dev,
            changed_lock_ino,
            identity.dev(),
            identity.ino(),
        )
    );
}

#[tokio::test]
async fn marker_owned_pending_and_published_paths_recover_without_probes() {
    let pending = TempDir::new("pending-recovery");
    let pending_socket = pending.path("pfixture");
    let pending_listener =
        std::os::unix::net::UnixListener::bind(&pending_socket).expect("bind pending fixture");
    let pending_metadata = fs::symlink_metadata(&pending_socket).expect("stat pending fixture");
    drop(pending_listener);
    let pending_final = pending.path("admin.sock");
    fs::hard_link(&pending_socket, &pending_final).expect("publish pending fixture");
    let pending_marker = sidecar(&pending_final, ".marker");
    let (pending_lock_dev, pending_lock_ino) = owner_lock_identity(&pending_final);
    write_private(
        &pending_marker,
        format!(
            "version=2\nstate=pending\ntype=socket\nlock-dev={pending_lock_dev}\nlock-ino={pending_lock_ino}\ndev={}\nino={}\npending={}\n",
            pending_metadata.dev(),
            pending_metadata.ino(),
            marker_hex_name("pfixture")
        )
        .as_bytes(),
    );
    let recovered = Server::bind(config(&pending))
        .await
        .expect("recover pending marker");
    assert!(
        !pending_socket.exists(),
        "owned pending artifact is recovered"
    );
    drop(recovered);

    let published = TempDir::new("published-recovery");
    let published_socket = published.path("admin.sock");
    let published_listener =
        std::os::unix::net::UnixListener::bind(&published_socket).expect("bind published fixture");
    let published_metadata =
        fs::symlink_metadata(&published_socket).expect("stat published fixture");
    drop(published_listener);
    let published_marker_path = sidecar(&published_socket, ".marker");
    let (published_lock_dev, published_lock_ino) = owner_lock_identity(&published_socket);
    write_private(
        &published_marker_path,
        &published_marker(
            published_lock_dev,
            published_lock_ino,
            published_metadata.dev(),
            published_metadata.ino(),
        ),
    );
    let recovered = Server::bind(config(&published))
        .await
        .expect("recover published marker");
    assert!(
        published_socket.exists(),
        "new listener is published at the final path"
    );
    drop(recovered);
    assert!(
        !published_socket.exists(),
        "recovered listener cleans up normally"
    );
}

#[tokio::test]
async fn shutdown_preserves_a_replacement_at_the_admin_socket_path() {
    let temporary = TempDir::new("sock-replace");
    let server = Server::bind(config(&temporary)).await.expect("bind server");
    let socket_path = server.admin_socket_path().to_owned();
    fs::remove_file(&socket_path).expect("unlink owned socket");
    fs::write(&socket_path, b"replacement").expect("write replacement file");

    drop(server);
    assert_eq!(
        fs::read(&socket_path).expect("read replacement file"),
        b"replacement"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_serve_releases_admin_ownership_before_restart() {
    let temporary = TempDir::new("serve-cancellation");
    for _ in 0..16 {
        let server = Server::bind(config(&temporary))
            .await
            .expect("bind cancellable server");
        let (started_tx, started_rx) = oneshot::channel();
        let serving = tokio::spawn(server.serve(async move {
            started_tx.send(()).expect("report serve polling");
            std::future::pending::<()>().await;
        }));
        started_rx.await.expect("serve was polled");

        serving.abort();
        assert!(
            serving
                .await
                .expect_err("serve must be cancelled")
                .is_cancelled()
        );

        let replacement = Server::bind(config(&temporary))
            .await
            .expect("restart immediately after serve cancellation");
        assert!(std::os::unix::net::UnixStream::connect(replacement.admin_socket_path()).is_ok());
        drop(replacement);
    }

    let server = Server::bind(config(&temporary))
        .await
        .expect("bind replacement-preservation server");
    let socket_path = server.admin_socket_path().to_owned();
    fs::remove_file(&socket_path).expect("unlink socket before cancellation");
    fs::write(&socket_path, b"replacement").expect("write replacement before cancellation");
    let serving = tokio::spawn(server.serve(std::future::pending::<()>()));
    tokio::task::yield_now().await;
    serving.abort();
    assert!(
        serving
            .await
            .expect_err("replacement serve must be cancelled")
            .is_cancelled()
    );
    assert_eq!(
        fs::read(&socket_path).expect("read preserved replacement"),
        b"replacement"
    );
}

#[tokio::test]
async fn dropping_polled_serve_releases_listeners_without_scheduling_child_tasks() {
    for web_enabled in [false, true] {
        let temporary = TempDir::new("serve-drop");
        let mut configuration = config(&temporary);
        // Rebinding proves release only if no parallel test can be handed the
        // freed port by an ephemeral bind in between.
        configuration.public_address = non_ephemeral_loopback();
        if web_enabled {
            let database = Database::open_protected(
                &configuration.database_path,
                configuration.recovery_audit_path.clone(),
            )
            .unwrap();
            journal_service::BootstrapService::new(database)
                .create_principal(&journal_protocol::PrincipalCreateRequest {
                    handle: "viewer".into(),
                    display_name: "Viewer".into(),
                })
                .unwrap();
            configuration.web = Some(journald::WebConfig {
                address: non_ephemeral_loopback(),
                viewer: "viewer".into(),
            });
        }
        let server = Server::bind(configuration.clone())
            .await
            .expect("bind server");
        let public_address = server.public_address();
        let web_address = server.web_address().unwrap();
        let socket_path = server.admin_socket_path().to_owned();
        let mut serving = Box::pin(server.serve(std::future::pending::<()>()));
        std::future::poll_fn(|context| {
            assert!(serving.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(serving);
        assert!(!socket_path.exists(), "drop must release admin ownership");
        let public =
            std::net::TcpListener::bind(public_address).expect("public listener was dropped");
        let web = web_address
            .map(|address| std::net::TcpListener::bind(address).expect("web listener was dropped"));
        // The test itself now holds the released addresses; the restart only
        // needs fresh ones to prove administrative and audit ownership is free.
        configuration.public_address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        if let Some(web) = configuration.web.as_mut() {
            web.address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        }
        let replacement = Server::bind(configuration)
            .await
            .expect("restart without polling detached listener tasks");
        drop((public, web, replacement));
    }
}

#[tokio::test]
async fn server_refuses_an_audit_in_the_database_directory_before_creating_state() {
    let temporary = TempDir::new("audit-colocated");
    let mut settings = config(&temporary);
    settings.recovery_audit_path = temporary.path("journal.recovery.db");
    assert!(matches!(
        Server::bind(settings.clone()).await,
        Err(ServerError::InvalidConfig(message)) if message.contains("database's directory")
    ));
    for artifact in [
        settings.database_path,
        settings.recovery_audit_path,
        settings.admin_socket_path,
    ] {
        assert!(fs::symlink_metadata(&artifact).is_err(), "{artifact:?}");
    }
}

#[tokio::test]
async fn server_startup_rejects_bad_database_and_admin_socket_paths() {
    let temporary = TempDir::new("s3-startup-errors");
    let mut bad_database = config(&temporary);
    bad_database.database_path = temporary.path("missing").join("journal.db");
    assert!(Server::bind(bad_database).await.is_err());

    let occupied_path = temporary.path("occupied.sock");
    let _occupied =
        std::os::unix::net::UnixListener::bind(&occupied_path).expect("occupy admin socket path");
    let mut occupied = config(&temporary);
    occupied.admin_socket_path = occupied_path;
    assert!(Server::bind(occupied).await.is_err());

    let mut missing_parent = config(&temporary);
    missing_parent.admin_socket_path = temporary.path("absent").join("admin.sock");
    assert!(Server::bind(missing_parent).await.is_err());

    let insecure_parent = temporary.path("insecure");
    fs::create_dir(&insecure_parent).expect("create insecure admin directory");
    fs::set_permissions(&insecure_parent, fs::Permissions::from_mode(0o755))
        .expect("set insecure admin directory mode");
    let mut insecure = config(&temporary);
    insecure.admin_socket_path = insecure_parent.join("admin.sock");
    assert!(matches!(
        Server::bind(insecure).await,
        Err(ServerError::InsecureAdminDirectory(path)) if path == insecure_parent
    ));

    let incompatible_path = temporary.path("incompatible.db");
    let incompatible_database = Database::open(&incompatible_path).expect("open current database");
    incompatible_database
        .connect()
        .expect("connect current database")
        .execute_batch("DROP TABLE schema_contract")
        .expect("corrupt schema contract");
    drop(incompatible_database);
    let mut incompatible = config(&temporary);
    incompatible.database_path = incompatible_path;
    assert!(matches!(
        Server::bind(incompatible).await,
        Err(ServerError::Database(StorageError::ResetRequired { .. }))
    ));
}

#[tokio::test]
async fn server_rejects_an_excessive_blocking_limit_before_opening_storage() {
    let temporary = TempDir::new("s3-server-blocking-max");
    let mut settings = config(&temporary);
    settings.database_path = temporary.path("missing").join("journal.db");
    settings.blocking_limit = tokio::sync::Semaphore::MAX_PERMITS + 1;

    assert!(matches!(
        Server::bind(settings).await,
        Err(ServerError::InvalidConfig(_))
    ));
}

#[tokio::test]
async fn admin_router_is_constructed_independently() {
    let temporary = TempDir::new("s3-admin-router");
    let router = admin_router(state(&temporary, 1), 1024);
    let (status, _, body) = json_response(
        router,
        Request::get("/health/live").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

fn start_journald(temporary: &TempDir) -> (std::process::Child, mpsc::Receiver<String>, PathBuf) {
    use std::io::{BufRead, BufReader};

    let admin_socket = temporary.path("admin.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_journald"))
        .env("JOURNAL_LOG_LEVEL", "info")
        .args([
            "--database",
            temporary.path("journal.db").to_str().unwrap(),
            "--recovery-audit",
            temporary.audit().to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
            "--admin-socket",
            admin_socket.to_str().unwrap(),
            "--blocking-limit",
            "2",
            "--max-body-bytes",
            "1024",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start journald");
    let stderr = child.stderr.take().expect("capture journald logs");
    let (ready_tx, ready_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reported_ready = false;
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if !reported_ready && line.contains("\"event\":\"service_ready\"") {
                let _ = ready_tx.send(line);
                reported_ready = true;
            }
        }
    });
    (child, ready_rx, admin_socket)
}

fn wait_for_process_ready(
    child: &mut std::process::Child,
    ready_rx: &mpsc::Receiver<String>,
) -> SocketAddr {
    let ready_line = match ready_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(line) => line,
        Err(error) => {
            let status = child.try_wait().expect("inspect journald");
            let _ = child.kill();
            panic!("journald readiness failed ({error}); status: {status:?}");
        }
    };
    let ready: Value = serde_json::from_str(&ready_line).expect("parse readiness event");
    ready["fields"]["public_address"]
        .as_str()
        .expect("readiness public address")
        .parse()
        .expect("parse readiness public address")
}

fn terminate_journald(child: &mut std::process::Child) -> std::process::ExitStatus {
    let signal_status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("signal journald");
    assert!(signal_status.success());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("wait for journald") {
            return status;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().expect("kill stuck journald");
            panic!("journald graceful shutdown timeout");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn binary_recovers_the_admin_socket_after_sigkill() {
    let temporary = TempDir::new("sigkill-recovery");
    let (mut child, ready_rx, admin_socket) = start_journald(&temporary);
    let _ = wait_for_process_ready(&mut child, &ready_rx);

    let signal_status = Command::new("kill")
        .args(["-KILL", &child.id().to_string()])
        .status()
        .expect("signal journald with SIGKILL");
    assert!(signal_status.success());
    let killed = child.wait().expect("wait for SIGKILL");
    assert!(
        !killed.success(),
        "SIGKILL must not be mistaken for graceful exit"
    );
    assert!(
        admin_socket.exists(),
        "SIGKILL leaves the published socket path"
    );

    let (mut restarted, restarted_ready, restarted_socket) = start_journald(&temporary);
    let _ = wait_for_process_ready(&mut restarted, &restarted_ready);
    assert_eq!(restarted_socket, admin_socket);
    assert!(std::os::unix::net::UnixStream::connect(&restarted_socket).is_ok());
    let status = terminate_journald(&mut restarted);
    assert!(status.success(), "restarted journald exit status: {status}");
    assert!(
        !admin_socket.exists(),
        "restarted graceful cleanup removes the socket"
    );
}

#[test]
fn binary_starts_serves_and_handles_sigterm() {
    use std::io::{Read, Write};

    let temporary = TempDir::new("s3-process");
    let (mut child, ready_rx, admin_socket) = start_journald(&temporary);
    let address = wait_for_process_ready(&mut child, &ready_rx);

    let mut stream = std::net::TcpStream::connect(address).expect("connect journald");
    stream
        .write_all(b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("write health request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read health response");
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");

    let mut admin =
        std::os::unix::net::UnixStream::connect(&admin_socket).expect("connect admin socket");
    admin
        .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("write admin health request");
    let mut admin_response = String::new();
    admin
        .read_to_string(&mut admin_response)
        .expect("read admin health response");
    assert!(
        admin_response.starts_with("HTTP/1.1 200 OK"),
        "{admin_response}"
    );

    let status = terminate_journald(&mut child);
    assert!(status.success(), "journald exit status: {status}");
    assert!(!admin_socket.exists(), "admin socket removed after SIGTERM");
}

#[test]
fn graceful_daemon_shutdown_normalizes_runtime_sqlite_sidecars() {
    use std::io::{Read, Write};

    let temporary = TempDir::new("s3-clean");
    let database = temporary.path("journal.db");
    let (mut child, ready_rx, admin_socket) = start_journald(&temporary);
    let address = wait_for_process_ready(&mut child, &ready_rx);

    let body = br#"{"handle":"writer","display_name":"Writer"}"#;
    let mut admin =
        std::os::unix::net::UnixStream::connect(&admin_socket).expect("connect admin socket");
    admin
        .write_all(
            format!(
                "POST /v1/admin/principals HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .expect("write admin request headers");
    admin.write_all(body).expect("write admin request body");
    let mut created = String::new();
    admin
        .read_to_string(&mut created)
        .expect("read principal response");
    assert!(created.starts_with("HTTP/1.1 201 Created"), "{created}");

    let mut public = std::net::TcpStream::connect(address).expect("connect public listener");
    public
        .write_all(b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("write readiness request");
    let mut ready = String::new();
    public
        .read_to_string(&mut ready)
        .expect("read readiness response");
    assert!(ready.starts_with("HTTP/1.1 200 OK"), "{ready}");

    let status = terminate_journald(&mut child);
    assert!(status.success(), "journald exit status: {status}");
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sidecar(&database, suffix);
        assert!(
            fs::symlink_metadata(&sidecar).is_err(),
            "graceful shutdown retained {sidecar:?}"
        );
    }

    let (mut restarted, restarted_ready, restarted_socket) = start_journald(&temporary);
    let _ = wait_for_process_ready(&mut restarted, &restarted_ready);
    assert_eq!(restarted_socket, admin_socket);
    let status = terminate_journald(&mut restarted);
    assert!(status.success(), "cold restart exit status: {status}");
}

#[test]
fn abrupt_daemon_termination_is_recovered_only_by_protected_startup() {
    use std::io::{Read, Write};

    let temporary = TempDir::new("s3-crash");
    let database = temporary.path("journal.db");
    let (mut child, ready_rx, admin_socket) = start_journald(&temporary);
    let address = wait_for_process_ready(&mut child, &ready_rx);

    let body = br#"{"handle":"crash-writer","display_name":"Crash writer"}"#;
    let mut admin =
        std::os::unix::net::UnixStream::connect(&admin_socket).expect("connect admin socket");
    admin
        .write_all(
            format!(
                "POST /v1/admin/principals HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .expect("write admin request headers");
    admin.write_all(body).expect("write admin request body");
    let mut created = String::new();
    admin
        .read_to_string(&mut created)
        .expect("read principal response");
    assert!(created.starts_with("HTTP/1.1 201 Created"), "{created}");
    let mut public = std::net::TcpStream::connect(address).expect("connect public listener");
    public
        .write_all(b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("write readiness request");
    let mut ready = String::new();
    public
        .read_to_string(&mut ready)
        .expect("read readiness response");
    assert!(ready.starts_with("HTTP/1.1 200 OK"), "{ready}");

    child.kill().expect("SIGKILL daemon");
    assert!(!child.wait().expect("reap SIGKILL daemon").success());
    assert!(
        ["-wal", "-shm", "-journal"]
            .into_iter()
            .map(|suffix| sidecar(&database, suffix))
            .any(|path| fs::symlink_metadata(path).is_ok()),
        "abrupt termination must not normalize hot SQLite state"
    );
    assert!(matches!(
        Database::open(&database),
        Err(StorageError::ResetRequired {
            kind: "central database"
        })
    ));
    // Protected startup proves the hot state against the external audit and
    // replays it; the committed principal survives the SIGKILL.
    let recovered = Database::open_protected(&database, temporary.audit())
        .expect("protected startup recovers verified crash state");
    assert!(recovered.crash_recovered_revision().is_some());
    let survived: bool = recovered
        .connect_read_only()
        .expect("read recovered database")
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM principal_names WHERE name='crash-writer')",
            [],
            |row| row.get(0),
        )
        .expect("query recovered principal");
    assert!(survived, "the committed principal survives the crash");
}

#[test]
fn binary_handles_sigterm_immediately_after_readiness() {
    let temporary = TempDir::new("signal-ready");
    let (mut child, ready_rx, admin_socket) = start_journald(&temporary);
    let _ = wait_for_process_ready(&mut child, &ready_rx);

    let status = terminate_journald(&mut child);
    assert!(status.success(), "journald exit status: {status}");
    assert!(!admin_socket.exists(), "admin socket removed after SIGTERM");
}
