use journal_client::{Client, ClientError, journal_protocol::*};

struct Capture {
    method: &'static str,
    path: &'static str,
}

#[test]
fn typed_space_client_sends_and_reads_explicit_public_policy() {
    struct SpaceTransport;
    impl Transport for SpaceTransport {
        fn send(&self, request: Request) -> Result<Response, TransportError> {
            if request.method == "POST" {
                assert_eq!(request.path, "/v1/admin/spaces");
                let body: SpaceCreateRequest = decode_json(&request.body).unwrap();
                assert_eq!(body.access, domain::SpaceAccess::Public);
            } else {
                assert_eq!(request.path, "/v1/spaces/space");
            }
            Ok(Response::new(
                if request.method == "POST" { 201 } else { 200 },
                serde_json::to_vec(&domain::Space {
                    id: "space".into(),
                    name: "Space".into(),
                    access: domain::SpaceAccess::Public,
                    created_at: "2026-01-01T00:00:00Z".into(),
                    archived_at: None,
                    limits: domain::default_limits(),
                })
                .unwrap(),
            ))
        }
    }
    let client = Client::new(SpaceTransport);
    let created = client
        .create_space(&SpaceCreateRequest {
            id: "space".into(),
            name: "Space".into(),
            access: domain::SpaceAccess::Public,
        })
        .unwrap();
    assert_eq!(client.space(&"a".repeat(64), "space").unwrap(), created);
}
impl Transport for Capture {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        assert_eq!(request.method, self.method);
        assert_eq!(request.path, self.path);
        assert_eq!(
            request.headers["Authorization"],
            format!("Bearer {}", "a".repeat(64))
        );
        if self.method == "POST" {
            assert_eq!(request.headers["Idempotency-Key"], "stable-key");
            assert_eq!(request.headers["Content-Type"], "application/json");
            assert_eq!(request.body, br#"{"kind":"note","content":"hello"}"#);
        }
        Ok(Response::new(404, vec![]))
    }
}

#[test]
fn typed_methods_encode_path_query_and_idempotency_without_retries() {
    let token = "a".repeat(64);
    let client = |method, path| Client::new(Capture { method, path });
    assert!(matches!(
        client("GET", "/v1/me").me(&token),
        Err(ClientError::Http { status: 404 })
    ));
    assert!(
        client("GET", "/v1/spaces?limit=2")
            .spaces(&token, &PageQuery::new(None, Some(2)))
            .is_err()
    );
    assert!(
        client("GET", "/v1/principals?space=space+%2F+%E7%95%8C")
            .principals(
                &token,
                &ListPrincipalsQuery {
                    space: "space / 界".into(),
                    page: PageQuery::default()
                }
            )
            .is_err()
    );
    assert!(
        client("GET", "/v1/spaces/space%20%2F%20%E7%95%8C")
            .space(&token, "space / 界")
            .is_err()
    );
    assert!(
        client("GET", "/v1/records/record%2Fone")
            .get(&token, "record/one")
            .is_err()
    );
    assert!(
        client("GET", "/v1/spaces/space/records?after_seq=3&kind=a%2Bb")
            .list(
                &token,
                "space",
                &ListRecordsQuery {
                    after_seq: Some(3),
                    kind: Some("a+b".into()),
                    ..Default::default()
                }
            )
            .is_err()
    );
    let input: AppendRecordRequest = decode_json(br#"{"kind":"note","content":"hello"}"#).unwrap();
    assert!(
        client("POST", "/v1/spaces/space/records")
            .append(&token, "space", "stable-key", &input)
            .is_err()
    );
    assert!(matches!(
        Client::without_transport().append(&token, "space", "", &input),
        Err(ClientError::InvalidRequest)
    ));
    assert!(matches!(
        client(
            "GET",
            "/v1/spaces/space%2Fone/search?limit=2&q=hello+%2B+%E7%95%8C&order=seq"
        )
        .search(
            &token,
            "space/one",
            &SearchRecordsQuery::from_query("q=hello+%2B+%E7%95%8C&order=seq&limit=2").unwrap()
        ),
        Err(ClientError::Http { status: 404 })
    ));
    assert!(matches!(
        client("GET", "/v1/records/record%2Fone/thread?limit=2").thread(
            &token,
            "record/one",
            &PageQuery::new(None, Some(2))
        ),
        Err(ClientError::Http { status: 404 })
    ));
    assert!(matches!(
        Client::without_transport().thread(&token, "record", &PageQuery::new(None, Some(101))),
        Err(ClientError::InvalidRequest)
    ));
}
