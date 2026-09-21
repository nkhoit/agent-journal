use journal_inbox_worker::{Envelope, Route, Runtime, RuntimeError};
use journal_runtime_hermes::HermesRuntime;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Clone)]
enum RunReply {
    Accepted(&'static str),
    AcceptedBody(Value),
    Status(u16),
}

struct RequestRecord {
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct FakeHermes {
    endpoint: String,
    requests: Arc<Mutex<Vec<RequestRecord>>>,
    stop: Arc<AtomicBool>,
    wake: std::net::SocketAddr,
    thread: Option<JoinHandle<()>>,
}

impl FakeHermes {
    fn new(reply: RunReply) -> Self {
        Self::with_capabilities(
            reply,
            json!({
                "features": {
                    "run_submission": true,
                    "runs_idempotency": {
                        "supported": true,
                        "durable": true,
                        "retention_seconds": 86400
                    }
                }
            }),
        )
    }

    fn with_capabilities(reply: RunReply, capabilities: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake Hermes");
        let address = listener.local_addr().expect("fake Hermes address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = Arc::clone(&requests);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            loop {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                if thread_stop.load(Ordering::Acquire) {
                    return;
                }
                handle_request(stream, &thread_requests, &reply, &capabilities);
            }
        });
        Self {
            endpoint: format!("http://{address}"),
            requests,
            stop,
            wake: address,
            thread: Some(thread),
        }
    }

    fn requests(&self) -> Vec<(String, BTreeMap<String, String>, Vec<u8>)> {
        self.requests
            .lock()
            .expect("request lock")
            .iter()
            .map(|request| {
                (
                    request.path.clone(),
                    request.headers.clone(),
                    request.body.clone(),
                )
            })
            .collect()
    }
}

impl Drop for FakeHermes {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.wake);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("fake Hermes thread");
        }
    }
}

fn handle_request(
    mut stream: TcpStream,
    requests: &Arc<Mutex<Vec<RequestRecord>>>,
    reply: &RunReply,
    capabilities: &Value,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout");
    let mut received = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        let count = match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(count) => count,
            Err(_) => return,
        };
        received.extend_from_slice(&buffer[..count]);
        if let Some(index) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if received.len() > 128 * 1024 {
            return;
        }
    };
    let header = String::from_utf8_lossy(&received[..header_end]);
    let mut lines = header.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let _method = request_parts.next().unwrap_or_default();
    let path = request_parts.next().unwrap_or_default().to_owned();
    let mut headers = BTreeMap::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.to_ascii_lowercase();
            let value = value.trim().to_owned();
            if name == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
            headers.insert(name, value);
        }
    }
    while received.len() < header_end + content_length {
        let count = match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(count) => count,
            Err(_) => return,
        };
        received.extend_from_slice(&buffer[..count]);
    }
    let body = received[header_end..header_end + content_length].to_vec();
    requests.lock().expect("request lock").push(RequestRecord {
        path: path.clone(),
        headers,
        body,
    });

    let (status, response_body) = match path.as_str() {
        "/health" => (200, b"{}".to_vec()),
        "/v1/capabilities" => (
            200,
            serde_json::to_vec(capabilities).expect("capabilities JSON"),
        ),
        "/api/sessions" => (201, b"{}".to_vec()),
        "/v1/runs" => match reply.clone() {
            RunReply::Accepted(run_id) => (
                202,
                serde_json::to_vec(&json!({"run_id": run_id, "status": "queued"})).unwrap(),
            ),
            RunReply::AcceptedBody(body) => (202, serde_json::to_vec(&body).unwrap()),
            RunReply::Status(status) => (status, b"{}".to_vec()),
        },
        _ => (404, b"{}".to_vec()),
    };
    let reason = match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        401 => "Unauthorized",
        409 => "Conflict",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response_body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.write_all(&response_body);
}

fn envelope(inbox_item_id: &str) -> Envelope {
    Envelope {
        record_id: "record-1".into(),
        inbox_item_id: inbox_item_id.into(),
        space_id: "space".into(),
        from_principal: "source".into(),
        source_run: None,
        reply_to: None,
        addressed_to: "destination".into(),
        routing_key: Some("default".into()),
        body: "body".into(),
    }
}

fn route() -> Route {
    Route {
        key: "default".into(),
        runtime_target: "private-session-1".into(),
        enabled: true,
    }
}

#[test]
fn accepted_run_contains_explicit_session_and_stable_idempotency_key() {
    let server = FakeHermes::new(RunReply::Accepted("run_abc"));
    let runtime = HermesRuntime::new(&server.endpoint, "hermes-secret").expect("preflight");
    assert!(!format!("{runtime:?}").contains("hermes-secret"));
    let receipt = runtime
        .inject(&route(), &envelope("item-42"), "rendered input")
        .expect("accepted run");
    assert_eq!(receipt, "run_abc");

    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.0.as_str())
            .collect::<Vec<_>>(),
        ["/health", "/v1/capabilities", "/api/sessions", "/v1/runs"]
    );
    let session: Value = serde_json::from_slice(&requests[2].2).expect("session body");
    assert_eq!(session, json!({"id": "private-session-1"}));
    let run: Value = serde_json::from_slice(&requests[3].2).expect("run body");
    assert_eq!(
        run,
        json!({"input": "rendered input", "session_id": "private-session-1"})
    );
    assert_eq!(
        requests[1].1.get("authorization"),
        Some(&"Bearer hermes-secret".into())
    );
    assert_eq!(
        requests[3].1.get("authorization"),
        Some(&"Bearer hermes-secret".into())
    );
    assert_eq!(
        requests[3].1.get("idempotency-key"),
        Some(&"agent-journal:item-42".into())
    );
}

#[test]
fn exact_replay_returns_the_same_run_receipt() {
    let server = FakeHermes::new(RunReply::Accepted("run_same"));
    let runtime = HermesRuntime::new(&server.endpoint, "key").expect("preflight");
    let first = runtime.inject(&route(), &envelope("item-replay"), "same input");
    let second = runtime.inject(&route(), &envelope("item-replay"), "same input");
    assert_eq!(first, second);
    assert_eq!(first.unwrap(), "run_same");
    let requests = server.requests();
    let runs: Vec<_> = requests
        .iter()
        .filter(|request| request.0 == "/v1/runs")
        .collect();
    assert_eq!(runs.len(), 2);
    assert_eq!(
        runs[0].1.get("idempotency-key"),
        Some(&"agent-journal:item-replay".into())
    );
    assert_eq!(
        runs[0].1.get("idempotency-key"),
        runs[1].1.get("idempotency-key")
    );
}

#[test]
fn auth_conflict_and_retryable_statuses_map_to_runtime_errors() {
    for (status, expected) in [
        (401, RuntimeError::RuntimeRejected),
        (409, RuntimeError::RuntimeRejected),
        (
            429,
            RuntimeError::RuntimeUnavailable("Hermes Runs API temporarily unavailable".into()),
        ),
        (
            500,
            RuntimeError::RuntimeUnavailable("Hermes Runs API temporarily unavailable".into()),
        ),
        (
            503,
            RuntimeError::RuntimeUnavailable("Hermes Runs API temporarily unavailable".into()),
        ),
    ] {
        let server = FakeHermes::new(RunReply::Status(status));
        let runtime = HermesRuntime::new(&server.endpoint, "key").expect("preflight");
        assert_eq!(
            runtime.inject(&route(), &envelope("item-status"), "input"),
            Err(expected)
        );
    }
}

#[test]
fn malformed_and_oversized_receipts_are_invalid_responses() {
    let replies = [
        json!({}),
        json!({"run_id": 42}),
        json!({"run_id": ""}),
        json!({"run_id": "run id"}),
        json!({"run_id": "é"}),
        json!({"run_id": "x".repeat(257)}),
    ];
    for body in replies {
        let server = FakeHermes::new(RunReply::AcceptedBody(body));
        let runtime = HermesRuntime::new(&server.endpoint, "key").expect("preflight");
        assert_eq!(
            runtime.inject(&route(), &envelope("item-receipt"), "input"),
            Err(RuntimeError::InvalidResponse)
        );
    }
}

#[test]
fn unsupported_capabilities_are_rejected_before_injection() {
    let supported = json!({
        "features": {
            "run_submission": true,
            "runs_idempotency": {
                "supported": true,
                "durable": true,
                "retention_seconds": 86400
            }
        }
    });
    for (field, value) in [
        ("/features/run_submission", json!(false)),
        ("/features/runs_idempotency/supported", json!(false)),
        ("/features/runs_idempotency/durable", json!(false)),
        ("/features/runs_idempotency/retention_seconds", json!(86399)),
        ("/features/runs_idempotency/retention_seconds", Value::Null),
    ] {
        let mut capabilities = supported.clone();
        *capabilities.pointer_mut(field).unwrap() = value;
        let server = FakeHermes::with_capabilities(RunReply::Status(202), capabilities);
        assert!(matches!(
            HermesRuntime::new(&server.endpoint, "key"),
            Err(RuntimeError::RuntimeRejected)
        ));
        assert!(
            !server
                .requests()
                .iter()
                .any(|request| request.0 == "/v1/runs")
        );
    }
    let server = FakeHermes::with_capabilities(RunReply::Status(202), supported);
    assert!(HermesRuntime::new(&server.endpoint, "key").is_ok());
}
