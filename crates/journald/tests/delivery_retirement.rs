use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use journal_storage_sqlite::Database;
use journald::{ServiceState, admin_router, public_router};
use tower::ServiceExt;

struct TemporaryDatabase(std::path::PathBuf);

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

#[tokio::test]
async fn retired_operations_are_absent_from_both_listeners() {
    let temporary = TemporaryDatabase(std::env::temp_dir().join(format!(
        "delivery-retirement-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    let database = Database::open(&temporary.0).unwrap();
    let state = ServiceState::new(database.clone(), 2).unwrap();
    for router in [
        public_router(state.clone(), 1_048_576),
        admin_router(state, 1_048_576),
    ] {
        for (method, uri) in [
            ("POST", "/v1/admin/enrollment-tickets"),
            ("POST", "/v1/enrollment/exchange"),
            ("POST", "/v1/admin/enrollment/recover"),
            ("GET", "/v1/admin/adapters"),
            ("POST", "/v1/admin/adapters"),
            ("POST", "/v1/admin/adapters/missing/replace"),
            ("POST", "/v1/adapters/self/register"),
            ("POST", "/v1/adapters/self/heartbeat"),
            ("POST", "/v1/mailbox/claims"),
            ("POST", "/v1/claims/missing/commit"),
            ("POST", "/v1/mailbox-items/missing/events"),
            ("GET", "/v1/mailbox/status"),
            ("GET", "/v1/admin/mailboxes/missing/status"),
            ("POST", "/v1/admin/mailbox-items/missing/requeue"),
        ] {
            let unbound = format!("Bearer {}", "a".repeat(64));
            for authorization in [None, Some(unbound.as_str())] {
                let mut request = Request::builder().method(method).uri(uri);
                if let Some(value) = authorization {
                    request = request.header("Authorization", value);
                }
                let response = router
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
            }
        }
    }
}
