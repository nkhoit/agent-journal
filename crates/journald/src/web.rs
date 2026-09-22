use super::*;
use journal_protocol::{ListRecordsQuery, PageQuery, SearchRecordsQuery};

#[path = "web_render.rs"]
mod render;
use render::*;

#[derive(Clone)]
struct Viewer(String);

/// Mount only on the separately configured loopback shared-viewer listener.
pub fn web_router(state: ServiceState, viewer: String, max_body_bytes: usize) -> Router {
    let (_, shutdown) = watch::channel(false);
    web_router_with_timeout(
        state,
        viewer,
        max_body_bytes,
        DEFAULT_BODY_READ_TIMEOUT,
        shutdown,
    )
}

pub(crate) fn web_router_with_timeout(
    state: ServiceState,
    viewer: String,
    max_body_bytes: usize,
    read_timeout: Duration,
    shutdown: watch::Receiver<bool>,
) -> Router {
    Router::new()
        .route("/web", get(view))
        .route("/web/spaces/{space}", get(view))
        .route("/web/spaces/{space}/search", get(view))
        .route("/web/records/{record_id}", get(view))
        .route("/web/records/{record_id}/thread", get(view))
        .route("/web/records/{record_id}/delivery-status", get(view))
        .fallback(unimplemented_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(Extension(Viewer(viewer)))
        .layer(middleware::from_fn_with_state(
            BodyLimits {
                max_bytes: max_body_bytes,
                read_timeout,
                shutdown,
            },
            validate_request_body,
        ))
        .layer(middleware::from_fn(assign_request_id))
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

async fn security_headers(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    for (name, value) in [
        ("content-security-policy", CSP),
        ("cache-control", "no-store"),
        ("referrer-policy", "no-referrer"),
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        (
            "permissions-policy",
            "camera=(), microphone=(), geolocation=()",
        ),
    ] {
        response.headers_mut().insert(
            axum::http::HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    response
}

async fn view(
    State(state): State<ServiceState>,
    Extension(request_id): Extension<RequestId>,
    Extension(viewer): Extension<Viewer>,
    request: Request<Body>,
) -> Response {
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|path| path.as_str())
        .unwrap_or("")
        .to_owned();
    let query = request.uri().query().unwrap_or("").to_owned();
    let (mut parts, _) = request.into_parts();
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
    let result = bootstrap_result(&state, &request_id, "web_read", false, move |service| {
        let viewer = service.shared_viewer(&viewer.0);
        let invalid = |_| BootstrapError::InvalidJournal;
        let mut body = String::from("<p>Shared read-only viewer. Everyone with access sees the same permitted records.</p>");
        let title;
        // Custom `<h1>` HTML for pages whose heading needs markup (thread pages);
        // defaults to the escaped title text.
        let mut heading: Option<String> = None;
        match route.as_str() {
            "/web" => {
                title = "Spaces".to_owned();
                let query = PageQuery::from_query(&query).map_err(invalid)?;
                let spaces = viewer.spaces(&query)?;
                body.push_str("<ul>");
                for space in spaces.items {
                    body.push_str(&format!("<li>{}</li>", link(&space_path(&space.id), &space.name)));
                }
                body.push_str("</ul>");
                body.push_str(&next_page("/web", query.pairs(), spaces.next_cursor));
            }
            "/web/spaces/{space}" | "/web/spaces/{space}/search" => {
                let space = path.get("space").ok_or(BootstrapError::InvalidJournal)?;
                let base = space_path(space);
                let search = format!("{base}/search");
                body.push_str(&format!("<form action=\"{}\" method=\"get\">\
                    <label>Search <input name=\"q\" required maxlength=\"512\"></label>\
                    <button type=\"submit\">Search</button></form>", escape(&search)));
                if route.ends_with("/search") {
                    title = "Search".to_owned();
                    let query = SearchRecordsQuery::from_query(&query).map_err(invalid)?;
                    let results = viewer.search(space, &query)?;
                    let ids: Vec<String> = results.items.iter().map(|r| r.record.id.clone()).collect();
                    let roots = viewer.thread_roots(&ids)?;
                    for result in results.items {
                        let ctx = roots.get(&result.record.id).map(|(_, root_title)| {
                            ThreadCtx {
                                root_title: root_title.as_deref(),
                                in_thread: false,
                            }
                        });
                        body.push_str(&record(
                            &result.record,
                            ctx,
                            result.snippet.as_deref(),
                        ));
                    }
                    body.push_str(&next_page(&search, query.pairs(), results.next_cursor));
                } else {
                    title = "Timeline".to_owned();
                    let query = ListRecordsQuery::from_query(&query).map_err(invalid)?;
                    let records = viewer.records(space, &query)?;
                    let ids: Vec<String> = records.items.iter().map(|r| r.id.clone()).collect();
                    let roots = viewer.thread_roots(&ids)?;
                    for item in records.items {
                        let ctx = roots.get(&item.id).map(|(_, root_title)| {
                            ThreadCtx {
                                root_title: root_title.as_deref(),
                                in_thread: false,
                            }
                        });
                        body.push_str(&record(&item, ctx, None));
                    }
                    body.push_str(&next_page(&base, query.pairs(), records.next_cursor));
                }
            }
            "/web/records/{record_id}" | "/web/records/{record_id}/thread" |
            "/web/records/{record_id}/delivery-status" => {
                let id = path.get("record_id").ok_or(BootstrapError::InvalidJournal)?;
                let base = record_path(id);
                let query = PageQuery::from_query(&query).map_err(invalid)?;
                if route.ends_with("/thread") {
                    let (root, resolved) = viewer.thread_root(id)?;
                    // On fallback the anchor is not a proven root: never
                    // promote a reply title into the thread header.
                    let header_title: Option<&str> =
                        if resolved { root.title.as_deref() } else { None };
                    let (page_title, h1) = thread_heading(header_title);
                    title = page_title;
                    heading = Some(h1);
                    let ctx_root_title: Option<String> =
                        if resolved { root.title.clone() } else { None };
                    let records = viewer.thread(id, &query)?;
                    for item in records.items {
                        body.push_str(&record(
                            &item,
                            Some(ThreadCtx {
                                root_title: ctx_root_title.as_deref(),
                                in_thread: true,
                            }),
                            None,
                        ));
                    }
                    body.push_str(&next_page(&format!("{base}/thread"), query.pairs(), records.next_cursor));
                } else if route.ends_with("/delivery-status") {
                    title = "Receipt status".to_owned();
                    let delivery = viewer.delivery(id, &query)?;
                    body.push_str("<p>Acknowledgment ends inbox reminders, not proof of runtime delivery, reading or completion.</p>\
                        <table><thead><tr><th>Recipient</th><th>State</th><th>Created</th><th>Acknowledged</th></tr></thead><tbody>");
                    for item in delivery.items {
                        let state = serde_json::to_value(item.state).map_err(|_| BootstrapError::CorruptJournal)?;
                        let state = state.as_str().ok_or(BootstrapError::CorruptJournal)?;
                        body.push_str(&format!("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                            escape(&item.recipient), escape(state), escape(&item.created_at),
                            escape(item.acknowledged_at.as_deref().unwrap_or("Not acknowledged"))));
                    }
                    body.push_str("</tbody></table>");
                    body.push_str(&next_page(&format!("{base}/delivery-status"), query.pairs(), delivery.next_cursor));
                } else {
                    title = "Record".to_owned();
                    if query != PageQuery::default() { return Err(BootstrapError::InvalidJournal); }
                    let item = viewer.record(id)?;
                    let roots = viewer.thread_roots(&[item.id.clone()])?;
                    let ctx = roots.get(&item.id).map(|(_, root_title)| {
                        ThreadCtx {
                            root_title: root_title.as_deref(),
                            in_thread: false,
                        }
                    });
                    body.push_str(&record(&item, ctx, None));
                }
            }
            _ => return Err(BootstrapError::NotFound),
        }
        let heading = heading.unwrap_or_else(|| escape(&title));
        Ok(page(&title, &heading, &body))
    }).await;
    match result {
        Ok(html) => axum::response::Html(html).into_response(),
        Err(response) => response,
    }
}
