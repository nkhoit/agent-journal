use journal_client::{Client, ClientError, journal_protocol::*};

struct Capture {
    ack: bool,
    status: u16,
    body: Vec<u8>,
}
impl Transport for Capture {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        assert!(request.headers.contains_key("Authorization"));
        assert!(request.body.is_empty());
        if self.ack {
            assert_eq!(request.method, "POST");
            assert_eq!(request.path, "/v1/inbox/item%2Fexample/ack");
            assert!(!request.headers.contains_key("Content-Type"));
        } else {
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, "/v1/inbox?limit=100&state=all");
        }
        Ok(Response::new(self.status, self.body.clone()))
    }
}

#[test]
fn inbox_client_preserves_queries_and_requires_bodyless_204() {
    let token = "a".repeat(64);
    let client = Client::new(Capture {
        ack: false,
        status: 200,
        body: br#"{"items":[],"next_cursor":null}"#.to_vec(),
    });
    assert!(
        client
            .inbox(
                &token,
                &InboxQuery::from_query("state=all&limit=100").unwrap()
            )
            .unwrap()
            .items
            .is_empty()
    );
    for (status, body, success) in [
        (204, vec![], true),
        (200, vec![], false),
        (204, b"null".to_vec(), false),
        (404, vec![], false),
    ] {
        let client = Client::new(Capture {
            ack: true,
            status,
            body,
        });
        assert_eq!(
            client
                .acknowledge_inbox_item(&token, "item/example")
                .is_ok(),
            success
        );
    }
    assert!(matches!(
        Client::without_transport().acknowledge_inbox_item("invalid", "item"),
        Err(ClientError::InvalidRequest)
    ));
}
