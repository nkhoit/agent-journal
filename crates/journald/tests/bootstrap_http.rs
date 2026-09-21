use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use journal_protocol::*;
use journal_storage_sqlite::Database;
use journald::{ServiceState, admin_router, public_router};
use tower::ServiceExt;

#[tokio::test]
async fn registration_authentication_and_public_admin_isolation() {
    let directory =
        std::path::Path::new("target").join(format!("http-bootstrap-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let database = Database::open(directory.join("journal.db")).unwrap();
    let state = ServiceState::new(database.clone(), 4).unwrap();
    let router = public_router(state.clone(), 65536);
    let registration_token = "ab".repeat(32);
    let registration_request = || {
        Request::post("/v1/registrations")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {registration_token}"))
            .body(Body::from(
                r#"{"handle":"registered-example","display_name":"Registered Example"}"#,
            ))
            .unwrap()
    };
    let response = router
        .clone()
        .oneshot(registration_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let receipt: RegistrationReceipt =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    let response = router
        .clone()
        .oneshot(registration_request())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let replay: RegistrationReceipt =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(replay, receipt);
    let response = router
        .clone()
        .oneshot(
            Request::get("/v1/me")
                .header("authorization", format!("Bearer {registration_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for token in ["AB".repeat(32), "a".repeat(63)] {
        let response = router
            .clone()
            .oneshot(
                Request::post("/v1/registrations")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(
                        r#"{"handle":"invalid-token","display_name":"Invalid"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let response = router
        .clone()
        .oneshot(
            Request::post("/v1/admin/principals/recover")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"principal_id":"{}"}}"#,
                    receipt.principal.id
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    for body in [
        r#"["registered-example","Registered Example"]"#,
        r#"{"handle":"one","handle":"two","display_name":"Example"}"#,
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post("/v1/registrations")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {}", registration_token))
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
    let count: i64 = connection
        .query_row("SELECT count(*) FROM credentials", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    drop(connection);
    let unknown_token = "cd".repeat(32);
    for (path, token, expected) in [
        ("/v1/me", Some(registration_token.as_str()), StatusCode::OK),
        (
            "/v1/me",
            Some(unknown_token.as_str()),
            StatusCode::UNAUTHORIZED,
        ),
        ("/v1/me", None, StatusCode::UNAUTHORIZED),
        (
            "/v1/inbox",
            Some(registration_token.as_str()),
            StatusCode::OK,
        ),
        (
            "/v1/admin/principals",
            Some(registration_token.as_str()),
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
    let profile = ProfileUpdateRequest {
        handle: "018f1f59-6e90-7000-8000-000000000009".into(),
        display_name: "Renamed example".into(),
        description: Some("Mutable profile".into()),
        expected_profile_revision: 1,
    };
    let request = || {
        Request::builder()
            .method("PATCH")
            .uri("/v1/me/profile")
            .header("authorization", format!("Bearer {}", registration_token))
            .header("content-type", "application/json")
            .header("idempotency-key", "profile-example")
            .body(Body::from(serde_json::to_vec(&profile).unwrap()))
            .unwrap()
    };
    let response = router.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let updated: journal_protocol::domain::Principal =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(updated.handle, profile.handle);
    assert_eq!(updated.display_name, profile.display_name);
    assert_eq!(updated.description, profile.description);
    assert_eq!(updated.profile_revision, 2);
    let response = router.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let replayed: journal_protocol::domain::Principal =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(replayed, updated);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/v1/me/profile")
                .header(
                    "authorization",
                    format!("Bearer {}", registration_token),
                )
                .header("content-type", "application/json")
                .header("idempotency-key", "profile-example")
                .body(Body::from(
                    r#"{"handle":"018f1f59-6e90-7000-8000-000000000009","display_name":"Different","expected_profile_revision":1}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
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
