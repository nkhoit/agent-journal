use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use journal_protocol::*;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use journald::{ServiceState, public_router};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tower::ServiceExt;

struct Fixture {
    router: Router,
    state: ServiceState,
    client: String,
    delivery: String,
    directory: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "delivery-http-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
        std::fs::create_dir_all(&directory).unwrap();
        let db = Database::open(directory.join("journal.db")).unwrap();
        let s = BootstrapService::new(db.clone());
        s.create_principal(&PrincipalCreateRequest {
            id: "reader".into(),
            display_name: "Reader".into(),
        })
        .unwrap();
        s.create_space(&SpaceCreateRequest {
            id: "space".into(),
            name: "Space".into(),
        })
        .unwrap();
        s.set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "reader".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
        s.provision_adapter(&AdapterProvisionRequest {
            principal_id: "reader".into(),
            adapter_id: "adapter".into(),
        })
        .unwrap();
        let ticket = s
            .create_ticket(&EnrollmentTicketCreateRequest {
                principal_id: "reader".into(),
                adapter_id: "adapter".into(),
                ttl_seconds: 900,
            })
            .unwrap();
        let enrollment = s
            .exchange(
                &ticket.enrollment_ticket.ticket,
                &EnrollmentExchangeRequest {
                    instance_id: "installation".into(),
                },
            )
            .unwrap();
        let state = ServiceState::new(db, 1).unwrap();
        Self {
            router: public_router(state.clone(), 1_048_576),
            state,
            client: enrollment.principal_client_secret.secret,
            delivery: enrollment.delivery_adapter_secret.secret,
            directory,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn request(path: &str, token: &str, body: &str) -> Request<Body> {
    Request::post(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("idempotency-key", "one")
        .body(Body::from(body.to_owned()))
        .unwrap()
}
const CLAIM: &str = r#"{"instance_id":"installation","generation":1,"limit":20,"wait_seconds":30}"#;

#[tokio::test]
async fn custody_telemetry_and_status_http_vertical() {
    let f = Fixture::new();
    let posted = f
        .router
        .clone()
        .oneshot(request(
            "/v1/spaces/space/records",
            &f.client,
            r#"{"kind":"note","content":"hello","attention":["reader"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(posted.status(), StatusCode::CREATED);
    let claimed = f
        .router
        .clone()
        .oneshot(request(
            "/v1/mailbox/claims",
            &f.delivery,
            &CLAIM.replace("30", "0"),
        ))
        .await
        .unwrap();
    let claim: ClaimResponse =
        decode_json(&to_bytes(claimed.into_body(), 65536).await.unwrap()).unwrap();
    let item = &claim.items[0];
    let commit=serde_json::json!({"generation":1,"items":[{"mailbox_item_id":item.mailbox_item_id,"attempt_id":item.attempt_id}]}).to_string();
    let path = format!("/v1/claims/{}/commit", claim.claim_id);
    for expected in [
        CommitItemResult::Committed,
        CommitItemResult::AlreadyCommitted,
    ] {
        let result = f
            .router
            .clone()
            .oneshot(request(&path, &f.delivery, &commit))
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::OK);
        let body: CommitResponse =
            decode_json(&to_bytes(result.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(body.items[0].result, expected);
    }
    assert_eq!(
        f.router
            .clone()
            .oneshot(request(&path, &f.client, &commit))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let path = format!("/v1/mailbox-items/{}/events", item.mailbox_item_id);
    let mut event = serde_json::json!({"event_id":"retry","attempt_id":item.attempt_id,"generation":1,
        "occurred_at":"2026-09-18T00:00:00Z","state":"adapter-reported-retryable-failure"});
    for (id, state) in [
        ("retry", "adapter-reported-retryable-failure"),
        ("accepted", "adapter-reported-runtime-accepted"),
        ("retry", "adapter-reported-retryable-failure"),
    ] {
        event["event_id"] = id.into();
        event["state"] = state.into();
        let response = f
            .router
            .clone()
            .oneshot(request(&path, &f.delivery, &event.to_string()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    event["occurred_at"] = "invalid".into();
    assert_eq!(
        f.router
            .clone()
            .oneshot(request(&path, &f.delivery, &event.to_string()))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    event["occurred_at"] = "2026-09-18T00:00:00Z".into();
    event["detail"] = serde_json::json!({"error":"different"});
    assert_eq!(
        f.router
            .clone()
            .oneshot(request(&path, &f.delivery, &event.to_string()))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let status = f
        .router
        .clone()
        .oneshot(
            Request::get(format!(
                "/v1/records/{}/delivery-status?limit=1",
                item.record.id
            ))
            .header("authorization", format!("Bearer {}", f.client))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let body: DeliveryStatusPage =
        decode_json(&to_bytes(status.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(
        body.items[0].state,
        domain::DeliveryState::AdapterReportedRuntimeAccepted
    );
    let public_admin = f
        .router
        .clone()
        .oneshot(request(
            &format!("/v1/admin/mailbox-items/{}/requeue", item.mailbox_item_id),
            &f.delivery,
            "{}",
        ))
        .await
        .unwrap();
    assert_eq!(public_admin.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn long_poll_releases_worker_and_writer_and_wakes_after_append() {
    let f = Fixture::new();
    let mut poll = tokio::spawn(f.router.clone().oneshot(request(
        "/v1/mailbox/claims",
        &f.delivery,
        CLAIM,
    )));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut poll)
            .await
            .is_err()
    );
    assert!(!poll.is_finished());
    // With only one permit, this fails if the waiting request retains its worker.
    // A separate writer also proves the claim transaction has ended.
    f.state
        .blocking()
        .execute(|db| {
            let connection = db.connect()?;
            connection.execute_batch(
                "BEGIN IMMEDIATE; UPDATE spaces SET name='Renamed' WHERE id='space'; COMMIT;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let posted = f
        .router
        .clone()
        .oneshot(request(
            "/v1/spaces/space/records",
            &f.client,
            r#"{"kind":"note","content":"hello","attention":["reader"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(posted.status(), StatusCode::CREATED);
    let response = tokio::time::timeout(Duration::from_secs(2), poll)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let claim: ClaimResponse =
        decode_json(&to_bytes(response.into_body(), 1_048_576).await.unwrap()).unwrap();
    assert_eq!(claim.items.len(), 1);
    assert_eq!(claim.items[0].record.content, "hello");
}

#[tokio::test]
async fn delivery_wire_authentication_limits_and_empty_claims() {
    let f = Fixture::new();
    for (body, status) in [
        (r#"{"instance_id":"installation"}"#, StatusCode::OK),
        (r#"{"instance_id":"other"}"#, StatusCode::CONFLICT),
        (
            r#"{"instance_id":"installation","principal_id":"forged"}"#,
            StatusCode::BAD_REQUEST,
        ),
        (
            r#"{"instance_id":"installation","instance_id":"other"}"#,
            StatusCode::BAD_REQUEST,
        ),
        (r#"["installation"]"#, StatusCode::BAD_REQUEST),
    ] {
        let r = f
            .router
            .clone()
            .oneshot(request("/v1/adapters/self/register", &f.delivery, body))
            .await
            .unwrap();
        assert_eq!(r.status(), status);
    }
    assert_eq!(
        f.router
            .clone()
            .oneshot(request("/v1/mailbox/claims", &f.client, CLAIM))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        f.router
            .clone()
            .oneshot(request(
                "/v1/adapters/self/heartbeat",
                &f.delivery,
                r#"{"instance_id":"installation","generation":2}"#
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    for body in [
        r#"{"instance_id":"installation","generation":1,"limit":21}"#,
        r#"{"instance_id":"installation","generation":1,"limit":1,"wait_seconds":31}"#,
    ] {
        assert_eq!(
            f.router
                .clone()
                .oneshot(request("/v1/mailbox/claims", &f.delivery, body))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    for _ in 0..2 {
        let r = f
            .router
            .clone()
            .oneshot(request(
                "/v1/mailbox/claims",
                &f.delivery,
                &CLAIM.replace("30", "0"),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let empty: ClaimResponse =
            decode_json(&to_bytes(r.into_body(), 65536).await.unwrap()).unwrap();
        assert!(empty.items.is_empty());
        assert_eq!(empty.state, ClaimState::Committed);
    }
    for path in [
        "/v1/claims/example/commit",
        "/v1/mailbox-items/example/events",
    ] {
        assert_eq!(
            f.router
                .clone()
                .oneshot(request(path, &f.delivery, "{}"))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        f.router
            .clone()
            .oneshot(request(
                "/v1/admin/adapters/adapter/replace",
                &f.delivery,
                "{}"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn cancellation_of_wait_does_not_leave_an_active_claim() {
    let f = Fixture::new();
    let poll = tokio::spawn(f.router.clone().oneshot(request(
        "/v1/mailbox/claims",
        &f.delivery,
        CLAIM,
    )));
    tokio::time::sleep(Duration::from_millis(100)).await;
    poll.abort();
    assert!(poll.await.unwrap_err().is_cancelled());
    f.state
        .blocking()
        .execute(|db| {
            let count: i64 = db
                .connect()?
                .query_row("SELECT count(*) FROM claims", [], |r| r.get(0))?;
            assert_eq!(count, 0);
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn empty_long_poll_times_out_and_rechecks_revocation() {
    let f = Fixture::new();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        f.router.clone().oneshot(request(
            "/v1/mailbox/claims",
            &f.delivery,
            &CLAIM.replace("30", "1"),
        )),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let empty: ClaimResponse =
        decode_json(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert!(empty.items.is_empty());
    assert_eq!(empty.state, ClaimState::Committed);
    let poll = tokio::spawn(f.router.clone().oneshot(request(
        "/v1/mailbox/claims",
        &f.delivery,
        CLAIM,
    )));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let database_path = f.directory.join("journal.db");
    tokio::task::spawn_blocking(move || {
        let db = Database::open(database_path)?;
        db.connect()?.execute("UPDATE credentials SET revoked_at='2000-01-01T00:00:00Z' WHERE class='delivery-adapter'",[])?;
        Ok::<_, journal_storage_sqlite::StorageError>(())
    }).await.unwrap().unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), poll)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}
