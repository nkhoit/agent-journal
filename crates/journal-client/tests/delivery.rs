use journal_client::{Client, ClientError, journal_protocol::*};

struct Capture {
    path: &'static str,
    authenticated: bool,
}
impl Transport for Capture {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        assert_eq!(request.path, self.path);
        assert_eq!(
            request.headers.contains_key("Authorization"),
            self.authenticated
        );
        if self.authenticated {
            assert_eq!(
                request.headers["Authorization"],
                format!("Bearer {}", "a".repeat(64))
            );
        }
        if request.method == "POST" {
            assert_eq!(request.headers["Content-Type"], "application/json");
            let body: serde_json::Value = decode_json(&request.body).unwrap();
            assert!(body.get("principal_id").is_none());
            assert!(body.get("adapter_id").is_none());
        }
        Ok(Response::new(409, vec![]))
    }
}

#[test]
fn delivery_methods_validate_and_preserve_separate_authority() {
    let client = |path, authenticated| {
        Client::new(Capture {
            path,
            authenticated,
        })
    };
    let token = "a".repeat(64);
    let register = AdapterRegisterRequest {
        instance_id: "installation".into(),
    };
    assert!(matches!(
        client("/v1/adapters/self/register", true).register_adapter(&token, &register),
        Err(ClientError::Http { status: 409 })
    ));
    assert!(matches!(
        client("/v1/adapters/self/heartbeat", true).heartbeat_adapter(
            &token,
            &AdapterHeartbeatRequest {
                instance_id: "installation".into(),
                generation: 1
            }
        ),
        Err(ClientError::Http { status: 409 })
    ));
    let mut request = ClaimRequest {
        instance_id: "installation".into(),
        generation: 1,
        limit: 20,
        wait_seconds: 30,
    };
    assert!(matches!(
        client("/v1/mailbox/claims", true).claim_mailbox(&token, &request),
        Err(ClientError::Http { status: 409 })
    ));
    request.limit = 21;
    assert!(matches!(
        Client::without_transport().claim_mailbox(&token, &request),
        Err(ClientError::InvalidRequest)
    ));
    assert!(matches!(
        client("/v1/mailbox/status?limit=1", true)
            .mailbox_status(&token, &PageQuery::new(None, Some(1))),
        Err(ClientError::Http { status: 409 })
    ));
    assert!(matches!(
        client("/v1/admin/adapters/adapter%2Fone/replace", false).replace_adapter(
            "adapter/one",
            &AdapterReplaceRequest {
                expected_generation: 1,
                new_instance_id: "new".into(),
                reason: None
            }
        ),
        Err(ClientError::Http { status: 409 })
    ));
    assert!(matches!(
        client("/v1/admin/mailboxes/reader%2Fone/status?", false)
            .admin_mailbox_status("reader/one", &PageQuery::default()),
        Err(ClientError::Http { status: 409 })
    ));
}
