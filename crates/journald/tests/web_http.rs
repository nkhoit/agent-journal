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

#[tokio::test]
#[cfg(unix)]
async fn recovery_gates_web_and_metrics_and_preserves_durable_timestamps() {
    let directory = std::env::temp_dir().join(format!("web-recovery-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let central = directory.join("central.db");
    let audit_path = directory.join("audit.db");
    let backup = directory.join("backup.db");
    let db = Database::open_protected(&central, &audit_path).unwrap();
    let service = BootstrapService::new(db.clone());
    service
        .create_principal(&PrincipalCreateRequest {
            handle: "viewer".into(),
            display_name: "Viewer".into(),
        })
        .unwrap();
    let router = web_router(
        ServiceState::new(db.clone(), 2).unwrap(),
        "viewer".into(),
        131072,
    );
    get(&router, "/web", None, StatusCode::OK).await;
    assert!(
        service
            .operational_metrics()
            .unwrap()
            .last_backup_at
            .is_none()
    );
    db.recovery_audit().unwrap().backup(&db, &backup).unwrap();
    let expected_backup = db.recovery_status().unwrap().last_backup_at.unwrap();
    assert_eq!(
        service
            .operational_metrics()
            .unwrap()
            .last_backup_at
            .as_deref(),
        Some(expected_backup.as_str())
    );
    db.recovery_audit().unwrap().close().unwrap();
    get(&router, "/web", None, StatusCode::SERVICE_UNAVAILABLE).await;
    assert!(service.operational_metrics().is_err());
    drop(router);
    drop(service);
    drop(db);
    assert!(Database::open_protected(&central, &audit_path).is_err());
    let artifacts = [
        central.clone(),
        central.with_extension("db-wal"),
        central.with_extension("db-shm"),
        central.with_extension("db-journal"),
    ]
    .into_iter()
    .filter(|path| path.exists())
    .map(|path| (path.clone(), std::fs::read(path).unwrap()))
    .collect::<Vec<_>>();
    assert!(!artifacts.is_empty());
    assert!(Database::open_existing(&central).is_err());
    for (path, bytes) in artifacts {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    std::fs::remove_dir_all(directory).unwrap();
}

fn register(service: &BootstrapService, principal: &str) -> String {
    let id = match principal {
        "author" => 1,
        "recipient" => 2,
        "reader" => 3,
        "outsider" => 4,
        _ => panic!("unknown fixture principal"),
    };
    let token = format!("{id:064x}");
    service
        .register(
            &token,
            &RegistrationRequest {
                handle: principal.into(),
                display_name: principal.into(),
            },
        )
        .unwrap();
    token
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
    let author = register(&service, "author");
    let _recipient = register(&service, "recipient");
    let reader = register(&service, "reader");
    let _outsider = register(&service, "outsider");
    service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
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
    let token = &author;
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
                title: None,
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
                title: None,
            },
        )
        .unwrap();
    let state = ServiceState::new(db.clone(), 4).unwrap();
    assert!(service.shared_viewer("outsider").record(&record.id).is_ok());
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
    assert!(body.contains("Authenticated author:"));
    assert!(body.contains("rel=\"nofollow noopener noreferrer\""));
    for path in [
        record_path.clone(),
        format!("{record_path}/thread"),
        "/web/spaces/space".into(),
        "/web/spaces/space/search?q=administrator".into(),
    ] {
        get(&outsider_router, &path, Some(token), StatusCode::OK).await;
    }
    let delivery = format!("{record_path}/delivery-status");
    get(
        &outsider_router,
        &delivery,
        Some(token),
        StatusCode::NOT_FOUND,
    )
    .await;
    let body = get(&recipient_router, &delivery, Some(token), StatusCode::OK).await;
    assert_eq!(body.matches("<tr>").count(), 2);
    let body = get(&router, &delivery, Some(token), StatusCode::OK).await;
    assert_eq!(body.matches("<tr>").count(), 3);
    let reply_page = service
        .list_records(token, "space", &ListRecordsQuery::default())
        .unwrap();
    let reply = &reply_page.items[1];
    get(
        &reader_router,
        &format!("/web/records/{}/delivery-status", reply.id),
        Some(&reader),
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
    for path in ["/v1/spaces", "/v1/admin/principals", "/v1/admin/metrics"] {
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
            Request::get(&delivery)
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
    assert!(service.shared_viewer("author").record(&record.id).is_ok());
    get(&router, &record_path, Some(token), StatusCode::OK).await;
    get(&router, &delivery, Some(token), StatusCode::OK).await;
    db.connect()
        .unwrap()
        .execute(
            "UPDATE principals SET disabled_at='2026-01-01T00:00:00Z'
             WHERE id=(SELECT principal_id FROM principal_names WHERE name='recipient')",
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

#[tokio::test]
async fn thread_titles_render_per_matrix() {
    let directory =
        std::path::Path::new("target").join(format!("web-titles-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(db.clone());
    let author = register(&service, "author");
    service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "space".into(),
            name: "space".into(),
        })
        .unwrap();
    service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "author".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    let token = &author;
    let append = |key: &str, title: Option<&str>, reply_to: Option<&str>| {
        service
            .append_record(
                token,
                "space",
                key,
                &AppendRecordRequest {
                    kind: "note".into(),
                    content: "content".into(),
                    run_id: None,
                    routing_key: None,
                    attention: vec![],
                    relations: reply_to
                        .map(|id| {
                            vec![domain::Relation {
                                relation_type: domain::RelationType::ReplyTo,
                                record_id: id.into(),
                            }]
                        })
                        .unwrap_or_default(),
                    title: title.map(|t| t.into()),
                },
            )
            .unwrap()
            .record
    };
    // Titled root with a titled reply and an untitled reply.
    let root = append("titled-root", Some("Project <Alpha> & \"Beta\""), None);
    let reply = append("titled-reply", Some("Re: scope"), Some(&root.id));
    let _plain_reply = append("plain-reply", None, Some(&root.id));
    // Untitled root with a reply.
    let bare_root = append("bare-root", None, None);
    let _bare_reply = append("bare-reply", Some("Has subject"), Some(&bare_root.id));
    // Authored literal "(untitled)" stays distinguishable from the placeholder.
    let literal = append("literal-root", Some("(untitled)"), None);

    let state = ServiceState::new(db.clone(), 4).unwrap();
    let router = web_router(state.clone(), "author".into(), 1_048_576);

    // Timeline: titled root shows its subject, no self-breadcrumb.
    let timeline = get(&router, "/web/spaces/space", None, StatusCode::OK).await;
    assert!(timeline.contains(
        "Subject: <span dir=\"auto\">Project &lt;Alpha&gt; &amp; &quot;Beta&quot;</span>"
    ));
    // Untitled root shows the placeholder in its title slot.
    assert!(timeline.contains("<small><em>(untitled)</em></small>"));
    // Titled reply shows its subject plus the root breadcrumb.
    assert!(timeline.contains("Subject: <span dir=\"auto\">Re: scope</span>"));
    assert!(timeline.contains("in thread: <span dir=\"auto\">Project &lt;Alpha&gt;"));
    // Untitled reply shows the breadcrumb only, no fake subject.
    let reply_cards = timeline.matches("in thread:").count();
    assert!(
        reply_cards >= 3,
        "expected breadcrumbs for replies, got {reply_cards}"
    );
    // Authored literal "(untitled)" renders as normal text, not the placeholder.
    assert!(timeline.contains("Subject: <span dir=\"auto\">(untitled)</span>"));

    // Thread page: header carries the root title with trusted prefix; the root's
    // own subject line is suppressed to avoid duplication.
    let thread = get(
        &router,
        &format!("/web/records/{}/thread", root.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(thread.contains("<h1>Thread: <span dir=\"auto\">Project &lt;Alpha&gt;"));
    assert!(thread.contains("<title>Thread: \u{2068}Project &lt;Alpha&gt;"));
    // Root subject suppressed on its thread page; reply subjects still render.
    assert_eq!(
        thread
            .matches("Subject: <span dir=\"auto\">Project")
            .count(),
        0
    );
    assert!(thread.contains("Subject: <span dir=\"auto\">Re: scope</span>"));
    // No breadcrumbs inside the thread page.
    assert!(!thread.contains("in thread:"));

    // Untitled thread page: placeholder in the header, distinguishable markup.
    let bare_thread = get(
        &router,
        &format!("/web/records/{}/thread", bare_root.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(bare_thread.contains("<h1>Thread: <small><em>(untitled)</em></small></h1>"));
    assert!(bare_thread.contains("<title>Thread (untitled)</title>"));

    // Standalone pages behave like timeline rows.
    let standalone = get(
        &router,
        &format!("/web/records/{}", reply.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(standalone.contains("Subject: <span dir=\"auto\">Re: scope</span>"));
    assert!(standalone.contains("in thread: <span dir=\"auto\">Project &lt;Alpha&gt;"));
    let standalone_root = get(
        &router,
        &format!("/web/records/{}", bare_root.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(standalone_root.contains("<small><em>(untitled)</em></small>"));
    assert!(!standalone_root.contains("in thread:"));

    // Search results behave like timeline rows.
    let search = get(
        &router,
        "/web/spaces/space/search?q=content",
        None,
        StatusCode::OK,
    )
    .await;
    assert!(search.contains("Subject: <span dir=\"auto\">Project &lt;Alpha&gt;"));
    assert!(search.contains("in thread:"));

    // Literal "(untitled)" thread header uses normal text, not the placeholder.
    let literal_thread = get(
        &router,
        &format!("/web/records/{}/thread", literal.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(literal_thread.contains("<h1>Thread: <span dir=\"auto\">(untitled)</span></h1>"));

    drop(router);
    drop(state);
    drop(db);
    drop(service);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn thread_roots_depth_bound_cycle_and_cross_space_fallback() {
    let directory =
        std::path::Path::new("target").join(format!("web-roots-fallback-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(db.clone());
    let author = register(&service, "author");
    service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "space".into(),
            name: "space".into(),
        })
        .unwrap();
    service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "author".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    let token = &author;
    let append = |key: &str, title: Option<&str>, reply_to: Option<&str>| {
        service
            .append_record(
                token,
                "space",
                key,
                &AppendRecordRequest {
                    kind: "note".into(),
                    content: "content".into(),
                    run_id: None,
                    routing_key: None,
                    attention: vec![],
                    relations: reply_to
                        .map(|id| {
                            vec![domain::Relation {
                                relation_type: domain::RelationType::ReplyTo,
                                record_id: id.into(),
                            }]
                        })
                        .unwrap_or_default(),
                    title: title.map(|t| t.into()),
                },
            )
            .unwrap()
            .record
    };

    // 65-edge chain: r64 (64 edges to the root) resolves; r65 (65 edges)
    // exceeds the bound and falls back to itself with no title.
    let root = append("deep-root", Some("Deep Thread"), None);
    let mut parent = root.id.clone();
    let mut r64_id = String::new();
    let mut r65_id = String::new();
    for depth in 1..=65 {
        let record = append(
            &format!("deep-{depth}"),
            if depth == 65 {
                Some("Deep reply")
            } else {
                None
            },
            Some(&parent),
        );
        if depth == 64 {
            r64_id = record.id.clone();
        }
        if depth == 65 {
            r65_id = record.id.clone();
        }
        parent = record.id;
    }
    let roots = service
        .shared_viewer("author")
        .thread_roots(&[r64_id.clone(), r65_id.clone()])
        .unwrap();
    assert_eq!(
        roots.get(&r64_id),
        Some(&(root.id.clone(), Some("Deep Thread".to_string()))),
        "64 edges resolve to the real root"
    );
    assert_eq!(
        roots.get(&r65_id),
        Some(&(r65_id.clone(), None)),
        "65 edges fall back without promoting a reply title"
    );

    let state = ServiceState::new(db.clone(), 4).unwrap();
    let router = web_router(state.clone(), "author".into(), 1_048_576);

    // The over-bound reply renders as a reply: its own subject plus an
    // untitled breadcrumb, never the root's title.
    let deep = get(
        &router,
        &format!("/web/records/{r65_id}"),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(deep.contains("Subject: <span dir=\"auto\">Deep reply</span>"));
    assert!(deep.contains("in thread: <small><em>(untitled)</em></small>"));
    assert!(!deep.contains("Deep Thread"));

    // Cycle: A replies to the root, B replies to A, then A's edge is rewired
    // to B. Both directions of the cycle fall back to untitled breadcrumbs.
    let cycle_a = append("cycle-a", Some("Cycle parent"), Some(&root.id));
    let cycle_b = append("cycle-b", None, Some(&cycle_a.id));
    db.connect()
        .unwrap()
        .execute(
            "UPDATE record_relations SET target_record_id = ?1
             WHERE source_record_id = ?2 AND relation_type = 'reply-to'",
            [&cycle_b.id, &cycle_a.id],
        )
        .unwrap();
    let roots = service
        .shared_viewer("author")
        .thread_roots(&[cycle_a.id.clone(), cycle_b.id.clone()])
        .unwrap();
    assert_eq!(roots.get(&cycle_a.id), Some(&(cycle_a.id.clone(), None)));
    assert_eq!(roots.get(&cycle_b.id), Some(&(cycle_b.id.clone(), None)));
    let cycle_page = get(
        &router,
        &format!("/web/records/{}", cycle_b.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(cycle_page.contains("in thread: <small><em>(untitled)</em></small>"));
    assert!(!cycle_page.contains("Cycle parent"));

    // Cross-space parent: C's reply-to edge is rewired to a record in another
    // space. The chain cannot resolve there, so C falls back too.
    service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "other".into(),
            name: "other".into(),
        })
        .unwrap();
    service
        .set_membership(&MembershipRequest {
            space_id: "other".into(),
            principal_id: "author".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    let other_root = service
        .append_record(
            token,
            "other",
            "other-root",
            &AppendRecordRequest {
                kind: "note".into(),
                content: "content".into(),
                run_id: None,
                routing_key: None,
                attention: vec![],
                relations: vec![],
                title: Some("Other space".into()),
            },
        )
        .unwrap()
        .record;
    let cross = append("cross", Some("Cross parent"), Some(&root.id));
    db.connect()
        .unwrap()
        .execute(
            "UPDATE record_relations SET target_record_id = ?1
             WHERE source_record_id = ?2 AND relation_type = 'reply-to'",
            [&other_root.id, &cross.id],
        )
        .unwrap();
    let roots = service
        .shared_viewer("author")
        .thread_roots(&[cross.id.clone()])
        .unwrap();
    assert_eq!(roots.get(&cross.id), Some(&(cross.id.clone(), None)));
    let cross_page = get(
        &router,
        &format!("/web/records/{}", cross.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(cross_page.contains("Subject: <span dir=\"auto\">Cross parent</span>"));
    assert!(cross_page.contains("in thread: <small><em>(untitled)</em></small>"));
    assert!(!cross_page.contains("Other space"));
    // The thread page must not promote the unresolved reply title into the
    // thread header either.
    let cross_thread = get(
        &router,
        &format!("/web/records/{}/thread", cross.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(!cross_thread.contains("<h1>Thread: <span dir=\"auto\">Cross parent</span></h1>"));
    assert!(cross_thread.contains("<h1>Thread: <small><em>(untitled)</em></small></h1>"));

    drop(router);
    drop(state);
    drop(db);
    drop(service);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn thread_page_two_keeps_root_header() {
    let directory =
        std::path::Path::new("target").join(format!("web-thread-p2-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let db = Database::open(directory.join("journal.db")).unwrap();
    let service = BootstrapService::new(db.clone());
    let author = register(&service, "author");
    service
        .create_space(&SpaceCreateRequest {
            access: journal_protocol::domain::SpaceAccess::Public,
            id: "space".into(),
            name: "space".into(),
        })
        .unwrap();
    service
        .set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: "author".into(),
            can_read: true,
            can_append: true,
            can_admin: false,
        })
        .unwrap();
    let token = &author;
    let root = service
        .append_record(
            token,
            "space",
            "paged-root",
            &AppendRecordRequest {
                kind: "note".into(),
                content: "content".into(),
                run_id: None,
                routing_key: None,
                attention: vec![],
                relations: vec![],
                title: Some("Paged Thread".into()),
            },
        )
        .unwrap()
        .record;
    for (i, key) in ["paged-r1", "paged-r2", "paged-r3"].iter().enumerate() {
        service
            .append_record(
                token,
                "space",
                key,
                &AppendRecordRequest {
                    kind: "note".into(),
                    content: "content".into(),
                    run_id: None,
                    routing_key: None,
                    attention: vec![],
                    relations: vec![domain::Relation {
                        relation_type: domain::RelationType::ReplyTo,
                        record_id: root.id.clone(),
                    }],
                    title: Some(format!("Reply {i}")),
                },
            )
            .unwrap();
    }

    let state = ServiceState::new(db.clone(), 4).unwrap();
    let router = web_router(state.clone(), "author".into(), 1_048_576);
    let page_one = get(
        &router,
        &format!("/web/records/{}/thread?limit=2", root.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(page_one.contains("<h1>Thread: <span dir=\"auto\">Paged Thread</span></h1>"));
    assert!(page_one.contains("<title>Thread: \u{2068}Paged Thread\u{2069}</title>"));
    let cursor = page_one
        .find("rel=\"next\"")
        .and_then(|next| page_one[next..].find("cursor=").map(|at| next + at + 7))
        .and_then(|start| {
            page_one[start..]
                .find('"')
                .map(|end| page_one[start..start + end].to_string())
        })
        .expect("page one links to page two");
    let page_two = get(
        &router,
        &format!("/web/records/{}/thread?limit=2&cursor={cursor}", root.id),
        None,
        StatusCode::OK,
    )
    .await;
    assert!(page_two.contains("<h1>Thread: <span dir=\"auto\">Paged Thread</span></h1>"));
    assert!(page_two.contains("<title>Thread: \u{2068}Paged Thread\u{2069}</title>"));

    drop(router);
    drop(state);
    drop(db);
    drop(service);
    std::fs::remove_dir_all(directory).unwrap();
}
