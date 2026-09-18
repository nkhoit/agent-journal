use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use journal_protocol::*;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use journald::{ServiceState, public_router, web_router};
use tower::ServiceExt;

const MALICIOUS: &str = r#"# Forged administrator

author: administrator

<script>globalThis.compromised = true</script>
<img src="https://image.example.invalid/pixel" onerror="alert(1)">

[bad](javascript:alert%281%29) [data](data:text/html,evil)
[encoded](jav&#x61;script:alert%281%29)
![image](https://image.example.invalid/pixel)
[safe](https://example.invalid/read)

**ordinary Markdown** `code`
"#;

fn enroll(service: &BootstrapService, principal: &str) -> EnrollmentExchangeResponse {
    service
        .create_principal(&PrincipalCreateRequest {
            id: principal.into(),
            display_name: principal.into(),
        })
        .unwrap();
    service
        .provision_adapter(&AdapterProvisionRequest {
            principal_id: principal.into(),
            adapter_id: format!("adapter-{principal}"),
        })
        .unwrap();
    let ticket = service
        .create_ticket(&EnrollmentTicketCreateRequest {
            principal_id: principal.into(),
            adapter_id: format!("adapter-{principal}"),
            ttl_seconds: 900,
        })
        .unwrap();
    service
        .exchange(
            &ticket.enrollment_ticket.ticket,
            &EnrollmentExchangeRequest {
                instance_id: format!("installation-{principal}"),
            },
        )
        .unwrap()
}

async fn get(router: &axum::Router, path: &str, token: Option<&str>, status: StatusCode) -> String {
    let mut request = Request::get(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), status, "{path}");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["referrer-policy"], "no-referrer");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    let csp = response.headers()["content-security-policy"]
        .to_str()
        .unwrap();
    for directive in [
        "default-src 'none'",
        "script-src 'none'",
        "img-src 'none'",
        "frame-ancestors 'none'",
        "base-uri 'none'",
    ] {
        assert!(csp.contains(directive), "{csp}");
    }
    String::from_utf8(
        to_bytes(response.into_body(), 4_194_304)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn web_views_are_authorized_inert_and_bounded() {
    let directory = std::path::Path::new("target").join(format!("web-http-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(db.clone());
    let author = enroll(&service, "author");
    let _recipient = enroll(&service, "recipient");
    let reader = enroll(&service, "reader");
    let _outsider = enroll(&service, "outsider");
    service
        .create_space(&SpaceCreateRequest {
            id: "space".into(),
            name: "<img src=x>".into(),
        })
        .unwrap();
    for principal in ["author", "recipient", "reader"] {
        service
            .set_membership(&MembershipRequest {
                space_id: "space".into(),
                principal_id: principal.into(),
                can_read: true,
                can_append: true,
                can_admin: false,
            })
            .unwrap();
    }
    let token = &author.principal_client_secret.secret;
    let record = service
        .append_record(
            token,
            "space",
            "first",
            &AppendRecordRequest {
                kind: "<b>kind</b>".into(),
                content: MALICIOUS.into(),
                run_id: None,
                routing_key: None,
                attention: vec!["recipient".into(), "reader".into()],
                relations: vec![],
            },
        )
        .unwrap()
        .record;
    service
        .append_record(
            token,
            "space",
            "second",
            &AppendRecordRequest {
                kind: "reply".into(),
                content: "A reply".into(),
                run_id: None,
                routing_key: None,
                attention: vec![],
                relations: vec![domain::Relation {
                    relation_type: domain::RelationType::ReplyTo,
                    record_id: record.id.clone(),
                }],
            },
        )
        .unwrap();
    let state = ServiceState::new(db.clone(), 4).unwrap();
    let public = public_router(state.clone(), 1_048_576);
    let router = web_router(state.clone(), "author".into(), 1_048_576);
    let recipient_router = web_router(state.clone(), "recipient".into(), 1_048_576);
    let reader_router = web_router(state.clone(), "reader".into(), 1_048_576);
    let outsider_router = web_router(state.clone(), "outsider".into(), 1_048_576);
    let missing_router = web_router(state.clone(), "missing".into(), 1_048_576);
    let record_path = format!("/web/records/{}", record.id);
    for path in [
        "/web".to_owned(),
        "/web/spaces/space".into(),
        record_path.clone(),
        format!("{record_path}/thread"),
        "/web/spaces/space/search?q=administrator".into(),
    ] {
        let response = public
            .clone()
            .oneshot(Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        get(&missing_router, &path, None, StatusCode::NOT_FOUND).await;
        let body = get(&router, &path, None, StatusCode::OK).await;
        for active in [
            "<script",
            "<img",
            "href=\"javascript:",
            "href=\"data:",
            "<b>kind</b>",
        ] {
            assert!(!body.contains(active), "{path}: {active}");
        }
    }
    let body = get(&router, &record_path, Some(token), StatusCode::OK).await;
    assert!(body.contains("<strong>ordinary Markdown</strong>"));
    assert!(body.contains("Untrusted record content"));
    assert!(body.contains("Authenticated author: author"));
    assert!(body.contains("rel=\"nofollow noopener noreferrer\""));
    for path in [
        record_path.clone(),
        format!("{record_path}/thread"),
        format!("{record_path}/delivery-status"),
        "/web/spaces/space".into(),
        "/web/spaces/space/search?q=administrator".into(),
    ] {
        let body = get(&outsider_router, &path, Some(token), StatusCode::NOT_FOUND).await;
        assert!(!body.contains("Forged"));
    }
    let delivery = format!("{record_path}/delivery-status");
    let body = get(&recipient_router, &delivery, Some(token), StatusCode::OK).await;
    assert!(body.contains("recipient"));
    assert!(!body.contains(">reader<"));
    let body = get(&router, &delivery, Some(token), StatusCode::OK).await;
    assert!(body.contains(">reader<"));
    assert!(body.contains(">recipient<"));
    let reply_page = service
        .list_records(token, "space", &ListRecordsQuery::default())
        .unwrap();
    let reply = &reply_page.items[1];
    get(
        &reader_router,
        &format!("/web/records/{}/delivery-status", reply.id),
        Some(&reader.principal_client_secret.secret),
        StatusCode::NOT_FOUND,
    )
    .await;
    let page = get(
        &router,
        "/web/spaces/space?limit=1",
        Some(token),
        StatusCode::OK,
    )
    .await;
    assert!(page.contains("rel=\"next\""));
    assert!(!page.contains("A reply"));
    for query in [
        "limit=101",
        "limit=1&limit=2",
        "cursor=forged",
        "token=secret",
    ] {
        get(
            &router,
            &format!("/web/spaces/space?{query}"),
            Some(token),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    for path in ["/v1/spaces", "/v1/admin/principals"] {
        get(&router, path, Some(token), StatusCode::NOT_FOUND).await;
    }
    let response = router
        .clone()
        .oneshot(Request::post(&record_path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let response = public
        .clone()
        .oneshot(
            Request::post("/v1/spaces/space/records")
                .header("content-type", "application/json")
                .header("idempotency-key", "anonymous")
                .body(Body::from(
                    r#"{"kind":"note","content":"no anonymous write"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = outsider_router
        .clone()
        .oneshot(
            Request::get(&record_path)
                .header("x-forwarded-user", "author")
                .header("tailscale-user-login", "author")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "author".into(),
            can_read: false,
            can_append: false,
            can_admin: false,
        })
        .unwrap();
    get(&router, &record_path, Some(token), StatusCode::NOT_FOUND).await;
    get(&router, &delivery, Some(token), StatusCode::NOT_FOUND).await;
    db.connect()
        .unwrap()
        .execute(
            "UPDATE principals SET disabled_at='2026-01-01T00:00:00Z' WHERE id='recipient'",
            [],
        )
        .unwrap();
    get(&recipient_router, &record_path, None, StatusCode::NOT_FOUND).await;
    get(&recipient_router, "/web", None, StatusCode::NOT_FOUND).await;
    drop(router);
    drop(recipient_router);
    drop(reader_router);
    drop(outsider_router);
    drop(missing_router);
    drop(public);
    drop(state);
    drop(db);
    drop(service);
    std::fs::remove_dir_all(directory).unwrap();
}
