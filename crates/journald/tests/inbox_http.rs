use journal_client::{Client, HttpTransport};
use journal_protocol::{domain::*, *};
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use journald::{ServiceState, public_router};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_principals_use_inbox_over_real_http() {
    let path = std::env::temp_dir().join(format!(
        "inbox-http-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Database::open(&path).unwrap();
    BootstrapService::new(db.clone())
        .create_space(&SpaceCreateRequest {
            access: SpaceAccess::Public,
            id: "public".into(),
            name: "Public".into(),
        })
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = public_router(ServiceState::new(db.clone(), 4).unwrap(), 1_048_576);
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    tokio::task::spawn_blocking(move || {
        let transport = HttpTransport::new(&endpoint).unwrap();
        let request = |method: &str, url: &str, token: &str, body: &[u8]| {
            let mut request = Request::new(method, url, body.to_vec());
            request
                .headers
                .insert("Authorization".into(), format!("Bearer {token}"));
            if method == "POST" {
                request
                    .headers
                    .insert("Content-Type".into(), "application/json".into());
            }
            transport.send(request).unwrap()
        };
        let alpha = "a".repeat(64);
        let beta = "b".repeat(64);
        for (token, handle) in [(&alpha, "alpha"), (&beta, "beta")] {
            let response = request(
                "POST",
                "/v1/registrations",
                token,
                &serde_json::to_vec(&RegistrationRequest {
                    handle: handle.into(),
                    display_name: handle.into(),
                })
                .unwrap(),
            );
            assert_eq!(response.status, 201);
        }
        let client = Client::new(HttpTransport::new(&endpoint).unwrap());
        let input = RecordInput {
            kind: "message".into(),
            content: "hello".into(),
            attention: vec!["beta".into()],
            run_id: None,
            routing_key: None,
            relations: vec![],
            title: None,
        };
        let posted = client.append(&alpha, "public", "key", &input).unwrap();
        let replay = client.append(&alpha, "public", "key", &input).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.record, posted.record);
        assert_eq!(replay.mailbox_created, posted.mailbox_created);
        let page = client.inbox(&beta, &InboxQuery::default()).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].record, posted.record);
        let item = &page.items[0].inbox_item_id;
        let ack = format!("/v1/inbox/{item}/ack");
        assert_eq!(request("POST", &ack, &alpha, b"").status, 404);
        for body in [b"{}".as_slice(), b"null", b" ", b"[]"] {
            assert_eq!(request("POST", &ack, &beta, body).status, 400);
        }
        assert_eq!(
            request("POST", &format!("{ack}?unexpected=1"), &beta, b"").status,
            400
        );
        for query in [
            "limit=0",
            "limit=101",
            "state=pending",
            "limit=1&limit=1",
            "recipient=alpha",
            "cursor=bad",
        ] {
            assert_eq!(
                request("GET", &format!("/v1/inbox?{query}"), &beta, b"").status,
                400
            );
        }
        client.acknowledge_inbox_item(&beta, item).unwrap();
        let before = client
            .inbox(
                &beta,
                &InboxQuery::from_query("state=acknowledged").unwrap(),
            )
            .unwrap();
        let response = request("POST", &ack, &beta, b"");
        assert_eq!(response.status, 204);
        assert!(response.body.is_empty());
        assert_eq!(
            client
                .inbox(
                    &beta,
                    &InboxQuery::from_query("state=acknowledged").unwrap()
                )
                .unwrap(),
            before
        );
        assert!(
            client
                .inbox(&beta, &InboxQuery::default())
                .unwrap()
                .items
                .is_empty()
        );
        let status = client
            .delivery_status(&alpha, &posted.record.id, &PageQuery::default())
            .unwrap();
        assert_eq!(status.items[0].state, ReceiptState::Acknowledged);
    })
    .await
    .unwrap();
    assert_eq!(
        db.connect()
            .unwrap()
            .query_row("SELECT count(*) FROM memberships", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    server.abort();
    let _ = server.await;
    drop(db);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}
