use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use journal_protocol::*;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use journald::{ServiceState, public_router};
use tower::ServiceExt;

#[tokio::test]
async fn record_routes_enforce_wire_contract() {
    let directory =
        std::path::Path::new("target").join(format!("http-records-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(db.clone());
    service
        .create_principal(&PrincipalCreateRequest {
            id: "writer".into(),
            display_name: "Writer".into(),
        })
        .unwrap();
    service
        .create_space(&SpaceCreateRequest {
            id: "space".into(),
            name: "Space".into(),
        })
        .unwrap();
    service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "writer".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    service
        .provision_adapter(&AdapterProvisionRequest {
            principal_id: "writer".into(),
            adapter_id: "adapter".into(),
        })
        .unwrap();
    let ticket = service
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: "writer".into(),
            adapter_id: "adapter".into(),
            ttl_seconds: 900,
        })
        .unwrap();
    let enrolled = service
        .exchange(
            &ticket.enrollment_ticket.ticket,
            &EnrollmentExchangeRequest {
                instance_id: "installation".into(),
            },
        )
        .unwrap();
    let token = enrolled.principal_client_secret.secret;
    let router = public_router(ServiceState::new(db, 4).unwrap(), 1_048_576);
    let mut first = None;
    for (body, key, status) in [
        (
            r#"{"kind":"note","content":"hello"}"#,
            Some("key"),
            StatusCode::CREATED,
        ),
        (
            r#"{"content":"hello","kind":"note","attention":[]}"#,
            Some("key"),
            StatusCode::CREATED,
        ),
        (
            r#"{"kind":"note","content":"changed"}"#,
            Some("key"),
            StatusCode::CONFLICT,
        ),
        (
            r#"{"kind":"note","content":"hello"}"#,
            None,
            StatusCode::BAD_REQUEST,
        ),
        (
            r#"{"kind":"note","content":"hello","author":"forged"}"#,
            Some("unknown"),
            StatusCode::BAD_REQUEST,
        ),
        (
            r#"{"kind":"note","kind":"note","content":"hello"}"#,
            Some("duplicate"),
            StatusCode::BAD_REQUEST,
        ),
        (
            r#"{"kind":"note","content":"hello","run_id":null}"#,
            Some("null"),
            StatusCode::BAD_REQUEST,
        ),
        (
            r#"["note","hello"]"#,
            Some("array"),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let mut request = Request::post("/v1/spaces/space/records")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"));
        if let Some(key) = key {
            request = request.header("idempotency-key", key);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{body}");
        assert!(response.headers().contains_key("x-request-id"));
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1_048_576).await.unwrap())
                .unwrap();
        if status == StatusCode::CREATED {
            if let Some(record) = &first {
                assert_eq!(record, &value["record"]);
                assert_eq!(value["replayed"], true);
            } else {
                first = Some(value["record"].clone());
            }
        } else if status == StatusCode::CONFLICT {
            assert_eq!(value["error"]["code"], "idempotency-conflict");
        }
    }
    let id = first.unwrap()["id"].as_str().unwrap().to_owned();
    for (path, status) in [
        ("/v1/spaces".to_owned(), StatusCode::OK),
        ("/v1/spaces/space".to_owned(), StatusCode::OK),
        ("/v1/principals?space=space".to_owned(), StatusCode::OK),
        (
            "/v1/principals?space=other".to_owned(),
            StatusCode::NOT_FOUND,
        ),
        (
            "/v1/spaces/space/records?limit=1".to_owned(),
            StatusCode::OK,
        ),
        (
            "/v1/spaces/space/records?limit=101".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/spaces/space/records?limit=1&limit=2".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/spaces/space/records?cursor=forged".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (format!("/v1/records/{id}"), StatusCode::OK),
        (format!("/v1/records/{id}/thread"), StatusCode::OK),
        (
            format!("/v1/records/{id}/thread?limit=101"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/v1/records/{id}/thread?cursor=forged"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/records/absent/thread".to_owned(),
            StatusCode::NOT_FOUND,
        ),
        ("/v1/spaces/space/search?q=hello".to_owned(), StatusCode::OK),
        (
            "/v1/spaces/space/search?q=hello&order=seq".to_owned(),
            StatusCode::OK,
        ),
        (
            "/v1/spaces/other/search?q=hello".to_owned(),
            StatusCode::NOT_FOUND,
        ),
        (
            "/v1/spaces/space/search?q=%22".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/spaces/space/search".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/spaces/space/search?q=a&q=b".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/spaces/space/search?q=a&cursor=forged".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/spaces/space/search?q=a&order=wrong".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/spaces/space/search?q=a&limit=101".to_owned(),
            StatusCode::BAD_REQUEST,
        ),
        ("/v1/records/absent".to_owned(), StatusCode::NOT_FOUND),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::get(&path)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{path}");
        assert!(response.headers().contains_key("x-request-id"));
        let body = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        if status == StatusCode::OK && path.contains("/search") {
            let page: SearchPage = decode_json(&body).unwrap();
            assert_eq!(page.items.len(), 1);
            assert_eq!(page.items[0].record.id, id);
            assert_eq!(page.items[0].snippet.as_deref(), Some("hello"));
        } else if status == StatusCode::OK && path.ends_with("/thread") {
            let page: RecordPage = decode_json(&body).unwrap();
            assert_eq!(page.items[0].id, id);
        }
    }
    for path in [
        format!("/v1/records/{id}/thread"),
        "/v1/spaces/space/search?q=hello".into(),
    ] {
        for credential in [None, Some(enrolled.delivery_adapter_secret.secret.as_str())] {
            let mut request = Request::get(&path);
            if let Some(token) = credential {
                request = request.header("authorization", format!("Bearer {token}"));
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }
    drop(router);
    drop(service);
    std::fs::remove_dir_all(directory).unwrap();
}
