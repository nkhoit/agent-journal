use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, HttpBody, to_bytes};
use axum::extract::{Extension, FromRequest, FromRequestParts, State};
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use journal_protocol::decode_json;
use journal_service::{BootstrapError, BootstrapService};
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
        Ok(Ok(value)) => {
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
            error_response(status, code, message, request_id)
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
            error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "service-unavailable",
                "service dependency unavailable",
                request_id,
            )
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
                | BootstrapError::Storage(journal_storage_sqlite::StorageError::Migration {
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
                BootstrapError::Storage(_) => "storage",
                BootstrapError::Sqlite(_) => "sqlite",
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
    provision_adapter,
    journal_protocol::AdapterProvisionRequest,
    CREATED,
    |s: BootstrapService, r| s.provision_adapter(&r)
);
admin_handler!(
    create_ticket,
    journal_protocol::EnrollmentTicketCreateRequest,
    CREATED,
    |s: BootstrapService, r| s.create_ticket(&r)
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
    recover,
    journal_protocol::EnrollmentRecoveryRequest,
    NO_CONTENT,
    |s: BootstrapService, r: journal_protocol::EnrollmentRecoveryRequest| s
        .recover(&r.adapter_id, &r.instance_id)
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

async fn exchange(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
    headers: axum::http::HeaderMap,
    request: Request<Body>,
) -> Response {
    let Some(ticket) = bearer(&headers) else {
        malformed_bearer(&request_id);
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or expired credential",
            request_id,
        );
    };
    let Ok(input) = strict_request::<journal_protocol::EnrollmentExchangeRequest>(request).await
    else {
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
        "enrollment_exchange",
        true,
        move |s| s.exchange(&ticket, &input),
    )
    .await
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

async fn journal_operation(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
    request: Request<Body>,
) -> Response {
    use journal_protocol::{ListPrincipalsQuery, ListRecordsQuery, PageQuery};
    let Some(token) = bearer(request.headers()) else {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or expired credential",
            request_id,
        );
    };
    let authentication_token = token.clone();
    let authentication = bootstrap(
        state.clone(),
        request_id.clone(),
        StatusCode::NO_CONTENT,
        "principal_authentication",
        false,
        move |s| {
            s.authenticate(
                &authentication_token,
                journal_protocol::CredentialClass::PrincipalClient,
            )
            .map(|_| ())
        },
    )
    .await;
    if authentication.status() != StatusCode::NO_CONTENT {
        return authentication;
    }
    let method = request.method().clone();
    let query = request.uri().query().unwrap_or("").to_owned();
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("")
        .to_owned();
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
        return bootstrap(
            state,
            request_id,
            StatusCode::CREATED,
            "append_record",
            true,
            move |s| s.append_record(&token, &space, &key, &input),
        )
        .await;
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
                _ => return Err(BootstrapError::NotFound),
            };
            value.map_err(|_| BootstrapError::CorruptJournal)
        },
    )
    .await
}

async fn authenticated_unimplemented(
    state: ServiceState,
    request_id: RequestId,
    headers: axum::http::HeaderMap,
    class: journal_protocol::CredentialClass,
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
    let result = bootstrap(
        state,
        request_id.clone(),
        StatusCode::NO_CONTENT,
        match class {
            journal_protocol::CredentialClass::PrincipalClient => "principal_authentication",
            journal_protocol::CredentialClass::DeliveryAdapter => "delivery_authentication",
        },
        false,
        move |s| s.authenticate(&token, class).map(|_| ()),
    )
    .await;
    if result.status() == StatusCode::NO_CONTENT {
        admin_unimplemented(Extension(request_id)).await
    } else {
        result
    }
}

async fn principal_unimplemented(
    State(state): State<ServiceState>,
    Extension(id): Extension<RequestId>,
    headers: axum::http::HeaderMap,
) -> Response {
    authenticated_unimplemented(
        state,
        id,
        headers,
        journal_protocol::CredentialClass::PrincipalClient,
    )
    .await
}
async fn delivery_unimplemented(
    State(state): State<ServiceState>,
    Extension(id): Extension<RequestId>,
    headers: axum::http::HeaderMap,
) -> Response {
    authenticated_unimplemented(
        state,
        id,
        headers,
        journal_protocol::CredentialClass::DeliveryAdapter,
    )
    .await
}
async fn admin_unimplemented(Extension(id): Extension<RequestId>) -> Response {
    error_response(
        StatusCode::NOT_IMPLEMENTED,
        "not-implemented",
        "operation is not implemented",
        id,
    )
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
        .route("/v1/enrollment/exchange", post(exchange))
        .route("/v1/me", get(me))
        .route("/v1/principals", get(journal_operation))
        .route("/v1/spaces", get(journal_operation))
        .route("/v1/spaces/{space}", get(journal_operation))
        .route(
            "/v1/spaces/{space}/records",
            get(journal_operation).post(journal_operation),
        )
        .route("/v1/spaces/{space}/search", get(principal_unimplemented))
        .route("/v1/records/{record_id}", get(journal_operation))
        .route(
            "/v1/records/{record_id}/thread",
            get(principal_unimplemented),
        )
        .route(
            "/v1/records/{record_id}/delivery-status",
            get(principal_unimplemented),
        )
        .route("/v1/adapters/self/register", post(delivery_unimplemented))
        .route("/v1/adapters/self/heartbeat", post(delivery_unimplemented))
        .route("/v1/mailbox/claims", post(delivery_unimplemented))
        .route("/v1/claims/{claim_id}/commit", post(delivery_unimplemented))
        .route(
            "/v1/mailbox-items/{item_id}/events",
            post(delivery_unimplemented),
        )
        .route("/v1/mailbox/status", get(delivery_unimplemented))
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
        .route("/v1/admin/principals", post(create_principal))
        .route("/v1/admin/spaces", post(create_space))
        .route("/v1/admin/memberships", post(set_membership))
        .route(
            "/v1/admin/adapters",
            post(provision_adapter).get(admin_unimplemented),
        )
        .route("/v1/admin/enrollment-tickets", post(create_ticket))
        .route("/v1/admin/credentials/rotate", post(rotate))
        .route("/v1/admin/credentials/revoke", post(revoke))
        .route("/v1/admin/enrollment/recover", post(recover))
        .route(
            "/v1/admin/adapters/{adapter_id}/replace",
            post(admin_unimplemented),
        )
        .route(
            "/v1/admin/mailboxes/{principal}/status",
            get(admin_unimplemented),
        )
        .route(
            "/v1/admin/mailbox-items/{item_id}/requeue",
            post(admin_unimplemented),
        )
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
            (
                "memberships",
                "application/json",
                r#"["space","principal",true,true,false]"#,
            ),
            ("adapters", "application/json", r#"["adapter","principal"]"#),
            (
                "enrollment-tickets",
                "application/json",
                r#"["principal","adapter",60]"#,
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
                "enrollment/recover",
                "application/json",
                r#"["adapter","installation"]"#,
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
        for table in [
            "principals",
            "spaces",
            "memberships",
            "adapter_identities",
            "enrollment_tickets",
            "credentials",
        ] {
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
                        r#"{"id":"valid-principal","display_name":"Valid"}"#,
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
