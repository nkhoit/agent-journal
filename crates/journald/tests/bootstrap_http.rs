use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use journal_protocol::*;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use journald::{ServiceState, admin_router, public_router};
use tower::ServiceExt;

#[tokio::test]
async fn enrollment_authentication_and_public_admin_isolation() {
    let directory =
        std::path::Path::new("target").join(format!("http-bootstrap-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let database = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(database.clone());
    service
        .create_principal(&PrincipalCreateRequest {
            id: "agent-example".into(),
            display_name: "Example".into(),
        })
        .unwrap();
    service
        .provision_adapter(&AdapterProvisionRequest {
            adapter_id: "adapter-example".into(),
            principal_id: "agent-example".into(),
        })
        .unwrap();
    let ticket = service
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: "agent-example".into(),
            adapter_id: "adapter-example".into(),
            ttl_seconds: 60,
        })
        .unwrap();
    let state = ServiceState::new(database.clone(), 4).unwrap();
    let router = public_router(state.clone(), 65536);
    for body in [
        r#"["installation-example"]"#,
        r#"{"instance_id":"one","instance_id":"two"}"#,
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post("/v1/enrollment/exchange")
                    .header("content-type", "application/json")
                    .header(
                        "authorization",
                        format!("Bearer {}", ticket.enrollment_ticket.ticket),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
        assert!(body["error"]["request_id"].is_string());
    }
    let connection = database.connect().unwrap();
    let consumed: Option<String> = connection
        .query_row("SELECT consumed_at FROM enrollment_tickets", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(consumed.is_none());
    let count: i64 = connection
        .query_row("SELECT count(*) FROM credentials", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    drop(connection);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/enrollment/exchange")
                .header("content-type", "application/json")
                .header(
                    "authorization",
                    format!("Bearer {}", ticket.enrollment_ticket.ticket),
                )
                .body(Body::from(r#"{"instance_id":"installation-example"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let credentials: EnrollmentExchangeResponse =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    for (path, token, expected) in [
        (
            "/v1/me",
            Some(credentials.principal_client_secret.secret.as_str()),
            StatusCode::OK,
        ),
        (
            "/v1/me",
            Some(credentials.delivery_adapter_secret.secret.as_str()),
            StatusCode::UNAUTHORIZED,
        ),
        ("/v1/me", None, StatusCode::UNAUTHORIZED),
        (
            "/v1/mailbox/status",
            Some(credentials.delivery_adapter_secret.secret.as_str()),
            StatusCode::OK,
        ),
        (
            "/v1/mailbox/status",
            Some(credentials.principal_client_secret.secret.as_str()),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "/v1/admin/principals",
            Some(credentials.principal_client_secret.secret.as_str()),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let mut request = Request::builder().uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{path}");
    }
    let response = admin_router(state, 65536)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/principals")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"other","display_name":"Other"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    std::fs::remove_dir_all(directory).unwrap();
}
