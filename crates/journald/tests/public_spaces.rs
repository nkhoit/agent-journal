use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use journal_protocol::{CredentialClass, RegistrationReceipt};
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use journald::{ServiceState, public_router};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn request(
    router: &axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
    expected: StatusCode,
) -> Value {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    if !body.is_null() {
        request = request.header("content-type", "application/json");
    }
    let response = router
        .clone()
        .oneshot(
            request
                .header("idempotency-key", "public-test")
                .body(if body.is_null() {
                    Body::empty()
                } else {
                    Body::from(body.to_string())
                })
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1_048_576).await.unwrap()).unwrap();
    assert_eq!(status, expected, "{method} {path}: {value}");
    value
}

#[tokio::test]
async fn registered_principals_share_public_spaces_without_grants() {
    let directory =
        std::path::Path::new("target").join(format!("public-spaces-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let database = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(database.clone());
    let state = ServiceState::new(database.clone(), 4).unwrap();
    let public = public_router(state.clone(), 1_048_576);
    let space = serde_json::to_value(
        service
            .create_space(&journal_protocol::SpaceCreateRequest {
                id: "space".into(),
                name: "Space".into(),
                access: journal_protocol::domain::SpaceAccess::Public,
            })
            .unwrap(),
    )
    .unwrap();
    assert_eq!(space["access"], "public");
    let tokens = ["a".repeat(64), "b".repeat(64), "c".repeat(64)];
    let mut receipts = Vec::new();
    for (handle, token) in ["alpha", "beta", "gamma"].iter().zip(&tokens) {
        receipts.push(
            serde_json::from_value::<RegistrationReceipt>(
                request(
                    &public,
                    "POST",
                    "/v1/registrations",
                    Some(token),
                    json!({"handle":handle,"display_name":handle}),
                    StatusCode::CREATED,
                )
                .await,
            )
            .unwrap(),
        );
    }
    let spaces = request(
        &public,
        "GET",
        "/v1/spaces?limit=1",
        Some(&tokens[0]),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    assert_eq!(spaces["items"], json!([space]));
    let principals = request(
        &public,
        "GET",
        "/v1/principals?space=space",
        Some(&tokens[1]),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    assert_eq!(principals["items"].as_array().unwrap().len(), 3);
    let body = json!({"kind":"note","content":"public searchable message","attention":["beta"]});
    let posted = request(
        &public,
        "POST",
        "/v1/spaces/space/records",
        Some(&tokens[0]),
        body.clone(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(posted["record"]["author"], receipts[0].principal.id);
    assert_eq!(posted["mailbox_created"], 1);
    let id = posted["record"]["id"].as_str().unwrap();
    let record_path = format!("/v1/records/{id}");
    let status_path = format!("{record_path}/delivery-status");
    for path in [
        "/v1/spaces/space".to_owned(),
        "/v1/spaces/space/records?author=alpha&attention=beta".into(),
        "/v1/spaces/space/search?q=searchable&author=alpha&attention=beta&order=seq".into(),
        "/v1/spaces/space/search?q=searchable&order=rank".into(),
        record_path.clone(),
        format!("{record_path}/thread"),
    ] {
        let result = request(
            &public,
            "GET",
            &path,
            Some(&tokens[1]),
            Value::Null,
            StatusCode::OK,
        )
        .await;
        if path.contains("search?") || path.contains("records?") || path.ends_with("/thread") {
            assert_eq!(result["items"].as_array().unwrap().len(), 1);
        }
        request(
            &public,
            "GET",
            &path,
            None,
            Value::Null,
            StatusCode::UNAUTHORIZED,
        )
        .await;
    }
    for token in &tokens[..2] {
        assert_eq!(
            request(
                &public,
                "GET",
                &status_path,
                Some(token),
                Value::Null,
                StatusCode::OK
            )
            .await["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    request(
        &public,
        "GET",
        &status_path,
        Some(&tokens[2]),
        Value::Null,
        StatusCode::NOT_FOUND,
    )
    .await;
    request(
        &public,
        "POST",
        "/v1/admin/spaces",
        Some(&tokens[0]),
        json!({"id":"forbidden","name":"Forbidden","access":"public"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    for query in ["limit=0", "limit=101", "cursor=forged"] {
        request(
            &public,
            "GET",
            &format!("/v1/spaces?{query}"),
            Some(&tokens[0]),
            Value::Null,
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let connection = database.connect().unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM memberships", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    connection
        .execute("UPDATE spaces SET archived_at='2026-01-01T00:00:00Z'", [])
        .unwrap();
    let replay = request(
        &public,
        "POST",
        "/v1/spaces/space/records",
        Some(&tokens[0]),
        body,
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["record"], posted["record"]);
    assert_eq!(replay["mailbox_created"], posted["mailbox_created"]);
    request(
        &public,
        "POST",
        "/v1/spaces/space/records",
        Some(&tokens[1]),
        json!({"kind":"note","content":"new archived append"}),
        StatusCode::NOT_FOUND,
    )
    .await;
    request(
        &public,
        "GET",
        &record_path,
        Some(&tokens[1]),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    connection
        .execute(
            "UPDATE principals SET disabled_at='2026-01-01T00:00:00Z' WHERE id=?",
            [&receipts[1].principal.id],
        )
        .unwrap();
    request(
        &public,
        "GET",
        &record_path,
        Some(&tokens[1]),
        Value::Null,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    let actor = service
        .authenticate(&tokens[2], CredentialClass::PrincipalClient)
        .unwrap();
    service.revoke(&actor.credential_id, None).unwrap();
    request(
        &public,
        "GET",
        &record_path,
        Some(&tokens[2]),
        Value::Null,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    drop(connection);
    drop(public);
    drop(state);
    drop(service);
    drop(database);
    std::fs::remove_dir_all(directory).unwrap();
}
