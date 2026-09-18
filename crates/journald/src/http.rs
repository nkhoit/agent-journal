use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, HttpBody, to_bytes};
use axum::extract::{Extension, State};
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use journal_protocol::decode_json;
use journal_storage_sqlite::{Database, MIGRATION_VERSION};
use serde::Serialize;
use tokio::sync::watch;

use crate::config::DEFAULT_BODY_READ_TIMEOUT;
use crate::executor::{BlockingError, BlockingExecutor};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct ServiceState {
    blocking: BlockingExecutor,
}

impl ServiceState {
    pub fn new(database: Database, blocking_limit: usize) -> Result<Self, BlockingError> {
        Ok(Self {
            blocking: BlockingExecutor::new(database, blocking_limit)?,
        })
    }

    pub fn blocking(&self) -> &BlockingExecutor {
        &self.blocking
    }
}

#[derive(Debug, Clone)]
struct RequestId(String);

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    checks: Option<BTreeMap<&'static str, &'static str>>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: &'static str,
    request_id: String,
}

#[derive(Debug, Clone)]
struct BodyLimits {
    max_bytes: usize,
    read_timeout: Duration,
    shutdown: watch::Receiver<bool>,
}

pub fn public_router(state: ServiceState, max_body_bytes: usize) -> Router {
    let (_, shutdown) = watch::channel(false);
    public_router_with_timeout(state, max_body_bytes, DEFAULT_BODY_READ_TIMEOUT, shutdown)
}

pub(crate) fn public_router_with_timeout(
    state: ServiceState,
    max_body_bytes: usize,
    body_read_timeout: Duration,
    shutdown: watch::Receiver<bool>,
) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .fallback(unimplemented_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn_with_state(
            BodyLimits {
                max_bytes: max_body_bytes,
                read_timeout: body_read_timeout,
                shutdown,
            },
            validate_request_body,
        ))
        .layer(middleware::from_fn(assign_request_id))
        .with_state(state)
}

pub fn admin_router(state: ServiceState, max_body_bytes: usize) -> Router {
    let (_, shutdown) = watch::channel(false);
    admin_router_with_timeout(state, max_body_bytes, DEFAULT_BODY_READ_TIMEOUT, shutdown)
}

pub(crate) fn admin_router_with_timeout(
    state: ServiceState,
    max_body_bytes: usize,
    body_read_timeout: Duration,
    shutdown: watch::Receiver<bool>,
) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .fallback(unimplemented_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn_with_state(
            BodyLimits {
                max_bytes: max_body_bytes,
                read_timeout: body_read_timeout,
                shutdown,
            },
            validate_request_body,
        ))
        .layer(middleware::from_fn(assign_request_id))
        .with_state(state)
}

async fn live() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        checks: None,
    })
}

async fn ready(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
) -> Response {
    match state
        .blocking
        .execute(|database| database.schema_version())
        .await
    {
        Ok(MIGRATION_VERSION) => {
            let checks = BTreeMap::from([("database", "ok")]);
            Json(HealthResponse {
                status: "ok",
                version: env!("CARGO_PKG_VERSION"),
                checks: Some(checks),
            })
            .into_response()
        }
        Ok(_) | Err(BlockingError::Storage(_) | BlockingError::Task(_)) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "service-unavailable",
            "required service dependency is unavailable",
            request_id,
        ),
        Err(BlockingError::AtCapacity) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity-unavailable",
            "database work capacity is exhausted",
            request_id,
        ),
        Err(BlockingError::InvalidLimit) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "service-unavailable",
            "required service dependency is unavailable",
            request_id,
        ),
    }
}

async fn unimplemented_route(Extension(request_id): Extension<RequestId>) -> Response {
    error_response(
        StatusCode::NOT_FOUND,
        "not-found",
        "resource not found",
        request_id,
    )
}

async fn method_not_allowed(Extension(request_id): Extension<RequestId>) -> Response {
    error_response(
        StatusCode::METHOD_NOT_ALLOWED,
        "method-not-allowed",
        "request method is not allowed",
        request_id,
    )
}

fn error_response(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    request_id: RequestId,
) -> Response {
    (
        status,
        Json(ErrorBody {
            error: ErrorDetail {
                code,
                message,
                request_id: request_id.0,
            },
        }),
    )
        .into_response()
}

async fn validate_request_body(
    State(limits): State<BodyLimits>,
    Extension(request_id): Extension<RequestId>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let is_json = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    let (parts, body) = request.into_parts();
    let body = tokio::select! {
        result = tokio::time::timeout(
            limits.read_timeout,
            to_bytes(body, limits.max_bytes),
        ) => Some(result),
        () = wait_for_shutdown(limits.shutdown.clone()) => None,
    };
    let bytes = match body {
        Some(Ok(Ok(bytes))) => bytes,
        Some(Ok(Err(_))) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload-too-large",
                "request body exceeds the configured limit",
                request_id,
            );
        }
        Some(Err(_)) => {
            return error_response(
                StatusCode::REQUEST_TIMEOUT,
                "request-timeout",
                "request body was not received before the deadline",
                request_id,
            );
        }
        None => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "service-shutting-down",
                "service is shutting down",
                request_id,
            );
        }
    };
    if is_json && decode_json::<serde_json::Value>(&bytes).is_err() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid-json",
            "request body is not valid JSON",
            request_id,
        );
    }

    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
    std::future::pending::<()>().await;
}

async fn assign_request_id(mut request: Request<Body>, next: Next) -> Response {
    let started = Instant::now();
    let request_id = RequestId(new_request_id());
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    request.extensions_mut().insert(request_id.clone());

    let mut response = next.run(request).await;
    let status = response.status();
    response.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id.0).expect("generated request ID is a valid header"),
    );
    tracing::info!(
        event = "request_completed",
        request_id = request_id.0,
        method = %method,
        path,
        status = status.as_u16(),
        response_body_size_hint = response.body().size_hint().lower(),
        elapsed_micros = started.elapsed().as_micros() as u64
    );
    response
}

fn new_request_id() -> String {
    let sequence = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{timestamp:x}-{:x}-{sequence:x}", std::process::id())
}
