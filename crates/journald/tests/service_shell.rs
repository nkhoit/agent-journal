#![cfg(unix)]

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use journal_storage_sqlite::{Database, MIGRATION_VERSION, StorageError};
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

fn config(temporary: &TempDir) -> Config {
    Config {
        web: None,
        database_path: temporary.path("journal.db"),
        public_address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
        admin_socket_path: temporary.path("admin.sock"),
        blocking_limit: 2,
        max_body_bytes: 1024,
        body_read_timeout: Duration::from_millis(200),
        shutdown_timeout: Duration::from_millis(500),
    }
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
    let db = Database::open(temporary.path("journal.db")).unwrap();
    journal_service::BootstrapService::new(db.clone())
        .create_principal(&journal_protocol::PrincipalCreateRequest {
            id: "viewer".into(),
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

    let newer_path = temporary.path("newer.db");
    let newer = Database::open(&newer_path).expect("open newer-schema database");
    newer
        .connect()
        .expect("connect newer-schema database")
        .execute(
            "UPDATE schema_migrations SET version = ?1 WHERE version = (SELECT max(version) FROM schema_migrations)",
            [MIGRATION_VERSION + 1],
        )
        .expect("advance schema version");
    drop(newer);
    let mut incompatible = config(&temporary);
    incompatible.database_path = newer_path;
    assert!(matches!(
        Server::bind(incompatible).await,
        Err(ServerError::Database(StorageError::IncompatibleSchema {
            found,
            supported
        })) if found == MIGRATION_VERSION + 1 && supported == MIGRATION_VERSION
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
fn binary_handles_sigterm_immediately_after_readiness() {
    let temporary = TempDir::new("signal-ready");
    let (mut child, ready_rx, admin_socket) = start_journald(&temporary);
    let _ = wait_for_process_ready(&mut child, &ready_rx);

    let status = terminate_journald(&mut child);
    assert!(status.success(), "journald exit status: {status}");
    assert!(!admin_socket.exists(), "admin socket removed after SIGTERM");
}
