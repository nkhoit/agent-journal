use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, HttpBody, to_bytes};
use axum::extract::{Extension, FromRequest, FromRequestParts, State};
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use journal_protocol::decode_json;
use journal_service::{BootstrapError, BootstrapService};
use journal_storage_sqlite::{CURRENT_SCHEMA_VERSION, Database};
use serde::Serialize;
use tokio::sync::watch;

use crate::config::DEFAULT_BODY_READ_TIMEOUT;
use crate::executor::{BlockingError, BlockingExecutor};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(0);

#[path = "web.rs"]
mod web;
pub use web::web_router;
#[cfg(unix)]
pub(crate) use web::web_router_with_timeout;

#[derive(Debug, Clone)]
pub struct ServiceState {
    blocking: BlockingExecutor,
    inbox_arrivals: tokio::sync::watch::Sender<u64>,
}

impl ServiceState {
    pub fn new(database: Database, blocking_limit: usize) -> Result<Self, BlockingError> {
        Ok(Self {
            blocking: BlockingExecutor::new(database, blocking_limit)?,
            inbox_arrivals: tokio::sync::watch::channel(0).0,
        })
    }

    pub fn blocking(&self) -> &BlockingExecutor {
        &self.blocking
    }

    /// Wake held inbox long-poll requests. Called after a committed append
    /// created inbox items; the generation counter only needs to advance,
    /// since waiters re-query under their own credentials and filters.
    fn note_inbox_arrival(&self) {
        self.inbox_arrivals
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    fn inbox_arrivals(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inbox_arrivals.subscribe()
    }
}

#[derive(Debug, Clone)]
struct RequestId(String);

#[derive(Clone)]
pub(crate) struct AdminOwner(pub u32);

#[derive(Clone)]
pub(crate) struct AdminPeer(pub Option<u32>);

#[cfg(unix)]
impl
    axum::extract::connect_info::Connected<
        axum::serve::IncomingStream<'_, tokio::net::UnixListener>,
    > for AdminPeer
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, tokio::net::UnixListener>) -> Self {
        Self(
            stream
                .io()
                .peer_cred()
                .ok()
                .map(|credentials| credentials.uid()),
        )
    }
}

async fn require_local_peer(request: Request<Body>, next: Next) -> Response {
    let owner = request
        .extensions()
        .get::<AdminOwner>()
        .map(|owner| owner.0);
    let peer = request
        .extensions()
        .get::<axum::extract::ConnectInfo<AdminPeer>>()
        .and_then(|peer| peer.0.0);
    if owner.is_some() && peer == owner {
        next.run(request).await
    } else {
        tracing::warn!(
            event = "authentication_rejected",
            request_id = request
                .extensions()
                .get::<RequestId>()
                .expect("request ID middleware")
                .0,
            category = "local_peer_denied"
        );
        error_response(
            StatusCode::FORBIDDEN,
            "forbidden",
            "protected local administration required",
            request
                .extensions()
                .get::<RequestId>()
                .cloned()
                .expect("request ID middleware"),
        )
    }
}

async fn bootstrap<T: Serialize + Send + 'static>(
    state: ServiceState,
    request_id: RequestId,
    status: StatusCode,
    operation_name: &'static str,
    mutation: bool,
    operation: impl FnOnce(BootstrapService) -> Result<T, BootstrapError> + Send + 'static,
) -> Response {
    match bootstrap_result(&state, &request_id, operation_name, mutation, operation).await {
        Ok(value) => success_response(status, value),
        Err(response) => response,
    }
}

fn success_response(status: StatusCode, value: impl Serialize) -> Response {
    let mut response = if status == StatusCode::NO_CONTENT {
        status.into_response()
    } else {
        (status, Json(value)).into_response()
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn bootstrap_result<T: Send + 'static>(
    state: &ServiceState,
    request_id: &RequestId,
    operation_name: &'static str,
    mutation: bool,
    operation: impl FnOnce(BootstrapService) -> Result<T, BootstrapError> + Send + 'static,
) -> Result<T, Response> {
    let correlation = request_id.0.clone();
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    match state
        .blocking
        .execute(move |database| {
            let result = operation(BootstrapService::new(database.clone()));
            // Emit at the transaction boundary even if the caller disconnects.
            tracing::dispatcher::with_default(&dispatch, || {
                log_bootstrap_outcome(
                    operation_name,
                    &correlation,
                    mutation,
                    result.as_ref().err(),
                );
            });
            Ok(result)
        })
        .await
    {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            let (status, code, message) = match error {
                BootstrapError::Invalid(_) | BootstrapError::InvalidJournal => (
                    StatusCode::BAD_REQUEST,
                    "invalid-request",
                    "invalid request",
                ),
                BootstrapError::Unauthorized => (
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "invalid or expired credential",
                ),
                BootstrapError::NotFound => {
                    (StatusCode::NOT_FOUND, "not-found", "resource not found")
                }
                BootstrapError::Conflict => (
                    StatusCode::CONFLICT,
                    "conflict",
                    "operation conflicts with existing state",
                ),
                BootstrapError::IdempotencyConflict => (
                    StatusCode::CONFLICT,
                    "idempotency-conflict",
                    "idempotency key was used for another payload",
                ),
                _ => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "service dependency unavailable",
                ),
            };
            Err(error_response(status, code, message, request_id.clone()))
        }
        Err(error) => {
            let category = match error {
                BlockingError::AtCapacity => "blocking_capacity",
                BlockingError::Task(_) => "blocking_task",
                BlockingError::Storage(_) => "storage",
                BlockingError::InvalidLimit => "configuration",
            };
            tracing::error!(
                event = "bootstrap_failed",
                request_id = request_id.0,
                operation = operation_name,
                category,
                outcome = "unknown"
            );
            Err(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "service-unavailable",
                "service dependency unavailable",
                request_id.clone(),
            ))
        }
    }
}

fn log_bootstrap_outcome(
    operation: &'static str,
    request_id: &str,
    mutation: bool,
    error: Option<&BootstrapError>,
) {
    match error {
        None if mutation => tracing::info!(
            event = "bootstrap_committed",
            operation,
            request_id,
            outcome = "committed"
        ),
        None => {}
        Some(BootstrapError::Unauthorized) => tracing::warn!(
            event = "authentication_rejected",
            operation,
            request_id,
            category = "credential_rejected"
        ),
        Some(error) => {
            let sqlite_code = match error {
                BootstrapError::Sqlite(source)
                | BootstrapError::Storage(journal_storage_sqlite::StorageError::Sqlite(source))
                | BootstrapError::Storage(journal_storage_sqlite::StorageError::Baseline {
                    source,
                    ..
                }) => source.sqlite_error().map(|error| error.extended_code),
                _ => None,
            };
            let category = match error {
                BootstrapError::Invalid(_) | BootstrapError::InvalidJournal => "validation",
                BootstrapError::IdempotencyConflict => "idempotency_conflict",
                BootstrapError::CorruptJournal => "persisted_data",
                BootstrapError::NotFound => "not_found",
                BootstrapError::Conflict => "conflict",
                BootstrapError::Storage(
                    journal_storage_sqlite::StorageError::Sqlite(_)
                    | journal_storage_sqlite::StorageError::Baseline { .. },
                )
                | BootstrapError::Sqlite(_) => "sqlite",
                BootstrapError::Storage(_) => "storage",
                BootstrapError::Random => "random_source",
                BootstrapError::Clock => "clock",
                BootstrapError::Injected => "injected_failure",
                BootstrapError::Unauthorized => unreachable!(),
            };
            if matches!(
                error,
                BootstrapError::Invalid(_)
                    | BootstrapError::InvalidJournal
                    | BootstrapError::IdempotencyConflict
                    | BootstrapError::NotFound
                    | BootstrapError::Conflict
            ) {
                tracing::warn!(
                    event = "bootstrap_rejected",
                    operation,
                    request_id,
                    category,
                    outcome = "not_committed"
                );
            } else {
                tracing::error!(
                    event = "bootstrap_failed",
                    operation,
                    request_id,
                    category,
                    sqlite_code,
                    outcome = "not_confirmed"
                );
            }
        }
    }
}

fn malformed_bearer(request_id: &RequestId) {
    tracing::warn!(
        event = "authentication_rejected",
        request_id = request_id.0,
        category = "missing_or_malformed_bearer"
    );
}

async fn strict_request<T: serde::de::DeserializeOwned + Serialize>(
    request: Request<Body>,
) -> Result<T, ()> {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, usize::MAX).await.map_err(|_| ())?;
    // Retain Axum's media-type and extractor limit checks, then validate the
    // original bytes so normalization cannot erase duplicate keys.
    let _ = Json::<serde_json::Value>::from_request(
        Request::from_parts(parts, Body::from(bytes.clone())),
        &(),
    )
    .await
    .map_err(|_| ())?;
    decode_json(&bytes).map_err(|_| ())
}

macro_rules! admin_handler {
    ($name:ident, $input:ty, $status:ident, $operation:expr) => {
        async fn $name(
            State(state): State<ServiceState>,
            Extension(request_id): Extension<RequestId>,
            request: Request<Body>,
        ) -> Response {
            let Ok(input) = strict_request::<$input>(request).await else {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid-request",
                    "invalid request",
                    request_id,
                );
            };
            bootstrap(
                state,
                request_id,
                StatusCode::$status,
                stringify!($name),
                true,
                move |service| ($operation)(service, input),
            )
            .await
        }
    };
}

admin_handler!(
    create_principal,
    journal_protocol::PrincipalCreateRequest,
    CREATED,
    |s: BootstrapService, r| s.create_principal(&r)
);
admin_handler!(
    create_space,
    journal_protocol::SpaceCreateRequest,
    CREATED,
    |s: BootstrapService, r| s.create_space(&r)
);
admin_handler!(
    set_membership,
    journal_protocol::MembershipRequest,
    OK,
    |s: BootstrapService, r| s.set_membership(&r)
);
admin_handler!(
    rotate,
    journal_protocol::CredentialRotateRequest,
    OK,
    |s: BootstrapService, r| s.rotate(&r)
);
admin_handler!(
    revoke,
    journal_protocol::CredentialRevokeRequest,
    NO_CONTENT,
    |s: BootstrapService, r: journal_protocol::CredentialRevokeRequest| s
        .revoke(&r.credential_id, r.reason.as_deref())
);
admin_handler!(
    recover_principal,
    journal_protocol::PrincipalRecoveryRequest,
    OK,
    |s: BootstrapService, r| s.recover_principal(&r)
);

fn bearer(headers: &axum::http::HeaderMap) -> Option<String> {
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
        return None;
    }
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("Bearer")
        && token.len() == 64
        && token.bytes().all(|b| b.is_ascii_hexdigit()))
    .then(|| token.to_owned())
}

fn registration_bearer(headers: &axum::http::HeaderMap) -> Option<String> {
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
        return None;
    }
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("Bearer")
        && token.len() == 64
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then(|| token.to_owned())
}

async fn register_principal(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
    headers: axum::http::HeaderMap,
    request: Request<Body>,
) -> Response {
    let Some(token) = registration_bearer(&headers) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid-request",
            "invalid request",
            request_id,
        );
    };
    let Ok(input) = strict_request::<journal_protocol::RegistrationRequest>(request).await else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid-request",
            "invalid request",
            request_id,
        );
    };
    match bootstrap_result(
        &state,
        &request_id,
        "principal_registration",
        true,
        move |service| service.register(&token, &input),
    )
    .await
    {
        Ok(outcome) => success_response(
            if outcome.replayed {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            },
            outcome.receipt,
        ),
        Err(response) => response,
    }
}

async fn me(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Some(token) = bearer(&headers) else {
        malformed_bearer(&request_id);
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or expired credential",
            request_id,
        );
    };
    bootstrap(state, request_id, StatusCode::OK, "me", false, move |s| {
        let actor = s.authenticate(&token, journal_protocol::CredentialClass::PrincipalClient)?;
        s.me(&actor)
    })
    .await
}

async fn update_profile(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
    headers: axum::http::HeaderMap,
    request: Request<Body>,
) -> Response {
    let Some(token) = bearer(&headers) else {
        malformed_bearer(&request_id);
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or expired credential",
            request_id,
        );
    };
    let keys = headers.get_all("idempotency-key");
    let key = if keys.iter().count() == 1 {
        keys.iter()
            .next()
            .and_then(|value| std::str::from_utf8(value.as_bytes()).ok())
            .map(str::to_owned)
    } else {
        None
    };
    let Some(key) = key else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid-request",
            "Idempotency-Key is required",
            request_id,
        );
    };
    let Ok(input) = strict_request::<journal_protocol::ProfileUpdateRequest>(request).await else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid-request",
            "invalid request",
            request_id,
        );
    };
    bootstrap(
        state,
        request_id,
        StatusCode::OK,
        "update_profile",
        true,
        move |service| {
            let actor =
                service.authenticate(&token, journal_protocol::CredentialClass::PrincipalClient)?;
            service.update_own_profile(&actor, &key, &input)
        },
    )
    .await
}

/// Fetch one inbox page through the bounded blocking executor, preserving the
/// standard journal_read outcome logging for every attempt.
async fn fetch_inbox_page(
    state: &ServiceState,
    request_id: &RequestId,
    token: &str,
    query: &journal_protocol::InboxQuery,
) -> Result<journal_protocol::InboxPage, Response> {
    let token = token.to_owned();
    let query = query.clone();
    bootstrap_result(state, request_id, "journal_read", false, move |s| {
        s.inbox(&token, &query)
    })
    .await
}

/// Serve `GET /v1/inbox` with optional long-polling. When `wait_seconds` is
/// zero the first page returns immediately, preserving historical behavior.
/// When positive and the request carries no cursor, an empty first page holds
/// the connection until an inbox item is committed, the bound elapses, the
/// client disconnects, or the server begins shutting down; the query is then
/// re-executed and its result returned, except on shutdown, which returns the
/// empty page at once so graceful drain is not held for the full bound.
/// The wait never holds a database transaction or a blocking-executor permit:
/// each fetch runs separately inside the bounded executor, and the hold itself
/// is a plain async wait on the inbox-arrival generation counter. Spurious
/// wakeups only cause an extra re-query under the caller's own credentials.
async fn inbox_long_poll(
    state: ServiceState,
    request_id: RequestId,
    token: String,
    raw_query: &str,
    shutdown: watch::Receiver<bool>,
) -> Response {
    let query = match journal_protocol::InboxQuery::from_query(raw_query) {
        Ok(query) => query,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid-request",
                "invalid request",
                request_id,
            );
        }
    };
    // A cursor binds a fixed upper sequence bound, so cursor-bearing pages can
    // never observe new arrivals; long-polling applies to fresh traversals.
    let waits = query.wait_seconds > 0 && query.page.cursor.as_deref().is_none_or(|s| s.is_empty());
    // Subscribe before the first fetch so an arrival during the fetch still
    // trips the generation counter observed by `changed()`.
    let mut arrivals = state.inbox_arrivals();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(query.wait_seconds);
    loop {
        let page = match fetch_inbox_page(&state, &request_id, &token, &query).await {
            Ok(page) => page,
            Err(response) => return response,
        };
        if !page.items.is_empty() || !waits {
            return success_response(StatusCode::OK, page);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return success_response(StatusCode::OK, page);
        }
        tokio::select! {
            _ = arrivals.changed() => continue,
            () = wait_for_shutdown(shutdown.clone()) => {
                return success_response(StatusCode::OK, page);
            }
            _ = tokio::time::sleep(deadline - now) => {
                return match fetch_inbox_page(&state, &request_id, &token, &query).await {
                    Ok(page) => success_response(StatusCode::OK, page),
                    Err(response) => response,
                };
            }
        }
    }
}

async fn journal_operation(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
    Extension(shutdown): Extension<watch::Receiver<bool>>,
    request: Request<Body>,
) -> Response {
    use journal_protocol::{ListPrincipalsQuery, ListRecordsQuery, PageQuery, SearchRecordsQuery};
    let Some(token) = bearer(request.headers()) else {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or expired credential",
            request_id,
        );
    };
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("")
        .to_owned();
    let replayable_append =
        method == axum::http::Method::POST && route == "/v1/spaces/{space}/records";
    let authentication_token = token.clone();
    let authentication = bootstrap(
        state.clone(),
        request_id.clone(),
        StatusCode::NO_CONTENT,
        "principal_authentication",
        false,
        move |s| {
            if replayable_append {
                s.authenticate_append_replay(&authentication_token)
                    .map(|_| ())
            } else {
                s.authenticate(
                    &authentication_token,
                    journal_protocol::CredentialClass::PrincipalClient,
                )
                .map(|_| ())
            }
        },
    )
    .await;
    if authentication.status() != StatusCode::NO_CONTENT {
        return authentication;
    }
    let query = request.uri().query().unwrap_or("").to_owned();
    let (mut parts, body) = request.into_parts();
    let path =
        match axum::extract::Path::<BTreeMap<String, String>>::from_request_parts(&mut parts, &())
            .await
        {
            Ok(axum::extract::Path(path)) => path,
            Err(_) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid-request",
                    "invalid request",
                    request_id,
                );
            }
        };
    let request = Request::from_parts(parts, body);
    let space = path.get("space").cloned().unwrap_or_default();
    let record = path.get("record_id").cloned().unwrap_or_default();
    if route == "/v1/inbox/{item_id}/ack" {
        if !query.is_empty() || axum::body::to_bytes(request.into_body(), 0).await.is_err() {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid-request",
                "acknowledgment requires no body or query",
                request_id,
            );
        }
        let item = path.get("item_id").cloned().unwrap_or_default();
        return bootstrap(
            state,
            request_id,
            StatusCode::NO_CONTENT,
            "inbox_acknowledgment",
            true,
            move |s| s.acknowledge_inbox_item(&token, &item),
        )
        .await;
    }
    if method == axum::http::Method::POST {
        let keys = request.headers().get_all("idempotency-key");
        let key = if keys.iter().count() == 1 {
            keys.iter()
                .next()
                .and_then(|h| std::str::from_utf8(h.as_bytes()).ok())
                .map(str::to_owned)
        } else {
            None
        };
        let Some(key) = key else {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid-request",
                "Idempotency-Key is required",
                request_id,
            );
        };
        let Ok(input) = strict_request::<journal_protocol::AppendRecordRequest>(request).await
        else {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid-request",
                "invalid request",
                request_id,
            );
        };
        return match bootstrap_result(&state, &request_id, "append_record", true, move |s| {
            s.append_record(&token, &space, &key, &input)
        })
        .await
        {
            Ok(appended) => {
                // The transaction committed inside the blocking executor;
                // wake held long-poll readers only when items were created.
                if appended.mailbox_created > 0 {
                    state.note_inbox_arrival();
                }
                success_response(StatusCode::CREATED, appended)
            }
            Err(response) => response,
        };
    }
    if route == "/v1/inbox" {
        return inbox_long_poll(state, request_id, token, &query, shutdown).await;
    }
    bootstrap(
        state,
        request_id,
        StatusCode::OK,
        "journal_read",
        false,
        move |s| {
            let value = match route.as_str() {
                "/v1/spaces" => serde_json::to_value(s.list_spaces(
                    &token,
                    &PageQuery::from_query(&query).map_err(|_| BootstrapError::InvalidJournal)?,
                )?),
                "/v1/principals" => serde_json::to_value(
                    s.list_principals(
                        &token,
                        &ListPrincipalsQuery::from_query(&query)
                            .map_err(|_| BootstrapError::InvalidJournal)?,
                    )?,
                ),
                "/v1/spaces/{space}" => serde_json::to_value(s.get_space(&token, &space)?),
                "/v1/spaces/{space}/records" => serde_json::to_value(
                    s.list_records(
                        &token,
                        &space,
                        &ListRecordsQuery::from_query(&query)
                            .map_err(|_| BootstrapError::InvalidJournal)?,
                    )?,
                ),
                "/v1/records/{record_id}" => serde_json::to_value(s.get_record(&token, &record)?),
                "/v1/spaces/{space}/search" => serde_json::to_value(
                    s.search_records(
                        &token,
                        &space,
                        &SearchRecordsQuery::from_query(&query)
                            .map_err(|_| BootstrapError::InvalidJournal)?,
                    )?,
                ),
                "/v1/records/{record_id}/thread" => serde_json::to_value(s.get_thread(
                    &token,
                    &record,
                    &PageQuery::from_query(&query).map_err(|_| BootstrapError::InvalidJournal)?,
                )?),
                "/v1/records/{record_id}/delivery-status" => serde_json::to_value(
                    s.delivery_status(
                        &token,
                        &record,
                        &PageQuery::from_query(&query)
                            .map_err(|_| BootstrapError::InvalidJournal)?,
                    )?,
                ),
                _ => return Err(BootstrapError::NotFound),
            };
            value.map_err(|_| BootstrapError::CorruptJournal)
        },
    )
    .await
}

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
        .route("/v1/registrations", post(register_principal))
        .route("/v1/me", get(me))
        .route("/v1/me/profile", patch(update_profile))
        .route("/v1/principals", get(journal_operation))
        .route("/v1/inbox", get(journal_operation))
        .route("/v1/inbox/{item_id}/ack", post(journal_operation))
        .route("/v1/spaces", get(journal_operation))
        .route("/v1/spaces/{space}", get(journal_operation))
        .route(
            "/v1/spaces/{space}/records",
            get(journal_operation).post(journal_operation),
        )
        .route("/v1/spaces/{space}/search", get(journal_operation))
        .route("/v1/records/{record_id}", get(journal_operation))
        .route("/v1/records/{record_id}/thread", get(journal_operation))
        .route(
            "/v1/records/{record_id}/delivery-status",
            get(journal_operation),
        )
        .fallback(unimplemented_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(Extension(shutdown.clone()))
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
        .route("/v1/admin/metrics", get(operational_metrics))
        .route("/v1/admin/principals", post(create_principal))
        .route("/v1/admin/spaces", post(create_space))
        .route("/v1/admin/memberships", post(set_membership))
        .route("/v1/admin/credentials/rotate", post(rotate))
        .route("/v1/admin/credentials/revoke", post(revoke))
        .route("/v1/admin/principals/recover", post(recover_principal))
        .route_layer(middleware::from_fn(require_local_peer))
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

async fn operational_metrics(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
) -> Response {
    bootstrap(
        state,
        request_id,
        StatusCode::OK,
        "operational_metrics",
        false,
        |service| service.operational_metrics(),
    )
    .await
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
        Ok(CURRENT_SCHEMA_VERSION) => {
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
    let bodyless_ack = parts
        .extensions
        .get::<axum::extract::MatchedPath>()
        .is_some_and(|route| route.as_str() == "/v1/inbox/{item_id}/ack");
    if is_json
        && !(bodyless_ack && bytes.is_empty())
        && decode_json::<serde_json::Value>(&bytes).is_err()
    {
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
    // Never record caller-controlled URI segments, which can contain credentials.
    let path = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".into());
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

#[cfg(test)]
mod peer_tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn held_inbox_long_poll_returns_promptly_on_shutdown() {
        let directory = std::path::Path::new("target")
            .join(format!("longpoll-shutdown-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let database = Database::open(directory.join("journal.db")).unwrap();
        let token = "b".repeat(64);
        journal_service::BootstrapService::new(database.clone())
            .register(
                &token,
                &journal_protocol::RegistrationRequest {
                    handle: "beta".into(),
                    display_name: "Beta".into(),
                },
            )
            .unwrap();
        let (shutdown_tx, shutdown) = watch::channel(false);
        let router = public_router_with_timeout(
            ServiceState::new(database, 2).unwrap(),
            65536,
            DEFAULT_BODY_READ_TIMEOUT,
            shutdown,
        );
        let long_poll = || {
            Request::get("/v1/inbox?wait_seconds=30")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let held = tokio::spawn(router.clone().oneshot(long_poll()));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!held.is_finished(), "an empty inbox must be held");
        shutdown_tx.send(true).unwrap();
        let started = tokio::time::Instant::now();
        let released = tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .expect("shutdown must release a held long-poll")
            .unwrap()
            .unwrap();
        assert_eq!(released.status(), StatusCode::OK);
        let page: journal_protocol::InboxPage =
            serde_json::from_slice(&to_bytes(released.into_body(), 65536).await.unwrap()).unwrap();
        assert!(page.items.is_empty());
        // A request arriving during shutdown may be refused by the body gate
        // or served empty, but it must never be held.
        let late = tokio::time::timeout(Duration::from_secs(5), router.oneshot(long_poll()))
            .await
            .expect("a long-poll during shutdown must not be held")
            .unwrap();
        assert!(
            [StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE].contains(&late.status()),
            "{}",
            late.status()
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn operational_metrics_are_local_only_and_fail_explicitly() {
        let directory =
            std::path::Path::new("target").join(format!("metrics-admin-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let database = Database::open(directory.join("journal.db")).unwrap();
        let state = ServiceState::new(database, 2).unwrap();
        for (router, expected) in [
            (public_router(state.clone(), 65536), StatusCode::NOT_FOUND),
            (admin_router(state.clone(), 65536), StatusCode::FORBIDDEN),
            (
                admin_router(state.clone(), 65536)
                    .layer(Extension(AdminOwner(1000)))
                    .layer(Extension(axum::extract::ConnectInfo(AdminPeer(Some(1001))))),
                StatusCode::FORBIDDEN,
            ),
            (
                admin_router(state.clone(), 65536)
                    .layer(Extension(AdminOwner(1000)))
                    .layer(Extension(axum::extract::ConnectInfo(AdminPeer(Some(1000))))),
                StatusCode::OK,
            ),
        ] {
            let response = router
                .oneshot(
                    Request::get("/v1/admin/metrics")
                        .header("authorization", "Bearer not-admin")
                        .header("x-peer-uid", "1000")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            if expected == StatusCode::OK {
                assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
                let metrics: journal_protocol::OperationalMetrics =
                    serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap())
                        .unwrap();
                assert_eq!(metrics.unacknowledged_inbox_count, 0);
                assert!(metrics.last_backup_at.is_none());
            }
        }
        std::fs::remove_file(directory.join("journal.db")).unwrap();
        let router = admin_router(state, 65536)
            .layer(Extension(AdminOwner(1000)))
            .layer(Extension(axum::extract::ConnectInfo(AdminPeer(Some(1000)))));
        let response = router
            .oneshot(
                Request::get("/v1/admin/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn admin_requests_reject_positional_arrays_without_mutations() {
        let directory =
            std::path::Path::new("target").join(format!("strict-admin-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let database = Database::open(directory.join("journal.db")).unwrap();
        let state = ServiceState::new(database.clone(), 4).unwrap();
        let router = admin_router(state, 65536)
            .layer(Extension(AdminOwner(1000)))
            .layer(Extension(axum::extract::ConnectInfo(AdminPeer(Some(1000)))));
        for (path, content_type, body) in [
            (
                "principals",
                "application/json",
                r#"["array-principal","Array"]"#,
            ),
            ("spaces", "application/json", r#"["array-space","Array"]"#),
            ("spaces", "application/json", r#"{"id":"s","name":"Space"}"#),
            (
                "spaces",
                "application/json",
                r#"{"id":"s","name":"Space","access":"private"}"#,
            ),
            (
                "spaces",
                "application/json",
                r#"{"id":"s","name":"Space","access":"unknown"}"#,
            ),
            (
                "spaces",
                "application/json",
                r#"{"id":"s","name":"Space","access":null}"#,
            ),
            (
                "spaces",
                "application/json",
                r#"{"id":"s","name":"Space","access":true}"#,
            ),
            (
                "spaces",
                "application/json",
                r#"{"id":"s","name":"Space","access":1}"#,
            ),
            (
                "spaces",
                "application/json",
                r#"{"id":"s","name":"Space","access":"public","access":"private"}"#,
            ),
            (
                "memberships",
                "application/json",
                r#"["space","principal",true,true,false]"#,
            ),
            (
                "credentials/rotate",
                "application/json",
                r#"["credential","reason"]"#,
            ),
            (
                "credentials/revoke",
                "application/json",
                r#"["credential","reason"]"#,
            ),
            (
                "principals/recover",
                "application/json",
                r#"["018f1f59-6e90-7000-8000-000000000001","reason"]"#,
            ),
            (
                "principals",
                "application/json",
                r#"{"id":"one","id":"two","display_name":"Duplicate"}"#,
            ),
            (
                "principals",
                "application/problem+json",
                r#"{"id":"one","id":"two","display_name":"Duplicate"}"#,
            ),
            (
                "principals",
                "text/plain",
                r#"{"id":"wrong-media","display_name":"Wrong"}"#,
            ),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::post(format!("/v1/admin/{path}"))
                        .header(header::CONTENT_TYPE, content_type)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap())
                    .unwrap();
            assert!(body["error"]["request_id"].is_string());
        }
        for table in ["principals", "spaces", "memberships", "credentials"] {
            let count: i64 = database
                .connect()
                .unwrap()
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 0);
        }
        let response = router
            .oneshot(
                Request::post("/v1/admin/principals")
                    .header(
                        header::CONTENT_TYPE,
                        "application/problem+json; charset=utf-8",
                    )
                    .body(Body::from(
                        r#"{"handle":"valid-principal","display_name":"Valid"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn transaction_logs_redact_failure_details_and_correlate_outcomes() {
        use std::io::Write;
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let output = bytes.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || Capture(output.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let failure =
                BootstrapError::Storage(journal_storage_sqlite::StorageError::BackupIntegrity(
                    "secret-canary-error-body".into(),
                ));
            log_bootstrap_outcome("rotate", "abc-123-0", true, Some(&failure));
            log_bootstrap_outcome("rotate", "abc-123-1", true, None);
            malformed_bearer(&RequestId("abc-123-2".into()));
        });
        let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(!text.contains("secret-canary-error-body"));
        for expected in [
            "bootstrap_failed",
            "storage",
            "bootstrap_committed",
            "authentication_rejected",
            "abc-123-0",
            "abc-123-1",
            "abc-123-2",
        ] {
            assert!(text.contains(expected), "missing safe log field");
        }
    }

    #[tokio::test]
    async fn administration_requires_matching_kernel_peer_identity() {
        for (peer, expected) in [
            (Some(1000), StatusCode::NO_CONTENT),
            (Some(1001), StatusCode::FORBIDDEN),
            (Some(0), StatusCode::FORBIDDEN),
            (None, StatusCode::FORBIDDEN),
        ] {
            let router = Router::new()
                .route("/private", post(|| async { StatusCode::NO_CONTENT }))
                .route_layer(middleware::from_fn(require_local_peer))
                .layer(Extension(axum::extract::ConnectInfo(AdminPeer(peer))))
                .layer(Extension(AdminOwner(1000)))
                .layer(Extension(RequestId("test-request".into())));
            let response = router
                .oneshot(
                    Request::post("/private")
                        .header("x-peer-uid", "1000")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
    }
}
