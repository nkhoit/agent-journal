use journal_client::{Client, ClientError, journal_protocol::*};

struct Capture {
    path: &'static str,
    authenticated: bool,
}

impl Transport for Capture {
    fn send(&self, request: Request) -> Result<Response, TransportError> {
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, self.path);
        assert!(request.body.is_empty());
        assert_eq!(
            request.headers.contains_key("Authorization"),
            self.authenticated
        );
        Ok(Response::new(409, vec![]))
    }
}

#[test]
fn metrics_and_receipt_status_preserve_distinct_authority_and_path_escaping() {
    let metrics = Client::new(Capture {
        path: "/v1/admin/metrics",
        authenticated: false,
    });
    assert!(matches!(
        metrics.metrics(),
        Err(ClientError::Http { status: 409 })
    ));
    let receipts = Client::new(Capture {
        path: "/v1/records/record%2Fone/delivery-status?limit=1",
        authenticated: true,
    });
    assert!(matches!(
        receipts.delivery_status(
            &"a".repeat(64),
            "record/one",
            &PageQuery::new(None, Some(1))
        ),
        Err(ClientError::Http { status: 409 })
    ));
    assert!(matches!(
        Client::without_transport().delivery_status(
            &"a".repeat(64),
            "record",
            &PageQuery::new(None, Some(101))
        ),
        Err(ClientError::InvalidRequest)
    ));
}
