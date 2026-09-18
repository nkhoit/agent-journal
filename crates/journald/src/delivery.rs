use super::*;
use journal_protocol::{
    AdapterHeartbeatRequest, AdapterRegisterRequest, AdapterReplaceRequest, ClaimRequest, PageQuery,
};

macro_rules! delivery_handler {
    ($name:ident, $input:ty, $method:ident) => {
        pub(super) async fn $name(
            State(state): State<ServiceState>,
            Extension(id): Extension<RequestId>,
            request: Request<Body>,
        ) -> Response {
            let Some(token) = bearer(request.headers()) else {
                return delivery_unauthorized(id);
            };
            if let Some(response) = authenticate_delivery(&state, &id, &token).await {
                return response;
            }
            let Ok(input) = strict_request::<$input>(request).await else {
                return delivery_invalid(id);
            };
            bootstrap(
                state,
                id,
                StatusCode::OK,
                stringify!($name),
                true,
                move |s| s.$method(&token, &input),
            )
            .await
        }
    };
}
delivery_handler!(register_adapter, AdapterRegisterRequest, register_adapter);
delivery_handler!(
    heartbeat_adapter,
    AdapterHeartbeatRequest,
    heartbeat_adapter
);

fn delivery_unauthorized(id: RequestId) -> Response {
    error_response(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "invalid or expired credential",
        id,
    )
}
fn delivery_invalid(id: RequestId) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        "invalid-request",
        "invalid request",
        id,
    )
}

async fn authenticate_delivery(
    state: &ServiceState,
    id: &RequestId,
    token: &str,
) -> Option<Response> {
    let token = token.to_owned();
    let response = bootstrap(
        state.clone(),
        id.clone(),
        StatusCode::NO_CONTENT,
        "delivery_authentication",
        false,
        move |s| {
            s.authenticate(&token, journal_protocol::CredentialClass::DeliveryAdapter)
                .map(|_| ())
        },
    )
    .await;
    (response.status() != StatusCode::NO_CONTENT).then_some(response)
}

pub(super) async fn replace_adapter(
    State(state): State<ServiceState>,
    Extension(id): Extension<RequestId>,
    axum::extract::Path(adapter): axum::extract::Path<String>,
    request: Request<Body>,
) -> Response {
    let Ok(input) = strict_request::<AdapterReplaceRequest>(request).await else {
        return delivery_invalid(id);
    };
    bootstrap(
        state,
        id,
        StatusCode::OK,
        "replace_adapter",
        true,
        move |s| s.replace_adapter(&adapter, &input),
    )
    .await
}

pub(super) async fn mailbox_status(
    State(state): State<ServiceState>,
    Extension(id): Extension<RequestId>,
    request: Request<Body>,
) -> Response {
    let Some(token) = bearer(request.headers()) else {
        return delivery_unauthorized(id);
    };
    if let Some(response) = authenticate_delivery(&state, &id, &token).await {
        return response;
    }
    let Ok(query) = PageQuery::from_query(request.uri().query().unwrap_or("")) else {
        return delivery_invalid(id);
    };
    bootstrap(
        state,
        id,
        StatusCode::OK,
        "mailbox_status",
        false,
        move |s| s.mailbox_status(&token, &query),
    )
    .await
}

pub(super) async fn admin_mailbox_status(
    State(state): State<ServiceState>,
    Extension(id): Extension<RequestId>,
    axum::extract::Path(principal): axum::extract::Path<String>,
    request: Request<Body>,
) -> Response {
    let Ok(query) = PageQuery::from_query(request.uri().query().unwrap_or("")) else {
        return delivery_invalid(id);
    };
    bootstrap(
        state,
        id,
        StatusCode::OK,
        "admin_mailbox_status",
        false,
        move |s| s.admin_mailbox_status(&principal, &query),
    )
    .await
}

pub(super) async fn claim_mailbox(
    State(state): State<ServiceState>,
    Extension(id): Extension<RequestId>,
    Extension(mut shutdown): Extension<watch::Receiver<bool>>,
    request: Request<Body>,
) -> Response {
    let Some(token) = bearer(request.headers()) else {
        return delivery_unauthorized(id);
    };
    if let Some(response) = authenticate_delivery(&state, &id, &token).await {
        return response;
    }
    let Ok(input) = strict_request::<ClaimRequest>(request).await else {
        return delivery_invalid(id);
    };
    if input.validate().is_err() {
        return delivery_invalid(id);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(input.wait_seconds);
    // Subscribe before selection: a commit between selection and waiting must
    // remain visible. The periodic retry also observes out-of-process mutations.
    let mut changes = state.mailbox_changes.subscribe();
    let mut shutdown_open = true;
    loop {
        if *shutdown.borrow() {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "service-unavailable",
                "service shutting down",
                id,
            );
        }
        let token = token.clone();
        let input = input.clone();
        let finish = tokio::time::Instant::now() >= deadline;
        let selection = bootstrap_result(&state, &id, "claim_mailbox", false, move |s| {
            if finish {
                s.claim_mailbox(&token, &input).map(Some)
            } else {
                s.try_claim_mailbox(&token, &input)
            }
        })
        .await;
        match selection {
            Ok(Some(claim)) => return success_response(StatusCode::OK, claim),
            Ok(None) => {}
            Err(response) => return response,
        }
        let retry = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        tokio::select! {
            _ = tokio::time::sleep_until(retry) => {},
            result = changes.changed() => { if result.is_err() { break; } },
            result = shutdown.changed(), if shutdown_open => {
                shutdown_open = result.is_ok();
                if *shutdown.borrow() { break; }
            },
        }
    }
    error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "service-unavailable",
        "service shutting down",
        id,
    )
}
