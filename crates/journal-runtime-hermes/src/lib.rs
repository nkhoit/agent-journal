//! Hermes Runs API runtime boundary.
//!
//! The adapter deliberately uses only the supported authenticated Runs API. A
//! successful call means that Hermes durably admitted a run; it does not mean
//! that the model observed, understood, or completed the delivery.

use journal_adapter_core::{CoreError, CoreResult, Envelope, Route, Runtime};
use reqwest::{
    Method, Url,
    blocking::{Client, Response},
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
};
use serde_json::Value;
use std::{fmt, io::Read, time::Duration};

pub const STATUS: &str = "supported";
const MAX_VENDOR_BODY_BYTES: u64 = 64 * 1024;
const MAX_RUN_ID_BYTES: usize = 256;
const REQUIRED_IDEMPOTENCY_RETENTION_SECONDS: u64 = 86_400;

const UNAVAILABLE: &str = "Hermes Runs API temporarily unavailable";

/// A synchronous, narrow client for Hermes' authenticated Runs API.
///
/// Construction performs the health and capabilities preflight. The API key is
/// retained only in memory and is never included in errors or debug output.
pub struct HermesRuntime {
    client: Client,
    base: Url,
    api_key: String,
}

impl fmt::Debug for HermesRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HermesRuntime")
            .field("base", &self.base)
            .field("api_key_configured", &true)
            .finish()
    }
}

impl HermesRuntime {
    /// Build a runtime client and reject an endpoint that does not expose the
    /// durable Runs API contract required for exact retries.
    pub fn new(base_url: &str, api_key: impl Into<String>) -> CoreResult<Self> {
        let base = validate_base_url(base_url)?;
        let api_key = api_key.into();
        if api_key.is_empty() || !api_key.is_ascii() || api_key.bytes().any(|byte| byte < 0x20) {
            return Err(CoreError::RuntimeRejected);
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|_| CoreError::RuntimeUnavailable(UNAVAILABLE.into()))?;
        let runtime = Self {
            client,
            base,
            api_key,
        };
        runtime.preflight()?;
        Ok(runtime)
    }

    fn preflight(&self) -> CoreResult<()> {
        let health = self.request(Method::GET, "/health", None, false, None)?;
        if health.status().as_u16() != 200 {
            return Err(status_error(health.status().as_u16()));
        }
        discard_body(health)?;

        let capabilities = self.request(Method::GET, "/v1/capabilities", None, true, None)?;
        if capabilities.status().as_u16() != 200 {
            return Err(status_error(capabilities.status().as_u16()));
        }
        let body = read_body(capabilities)?;
        let value: Value = serde_json::from_slice(&body).map_err(|_| invalid_response())?;
        let run_submission = value
            .pointer("/features/run_submission")
            .and_then(Value::as_bool)
            == Some(true);
        let idempotency = value.pointer("/features/runs_idempotency");
        let durable_idempotency = idempotency
            .and_then(|features| features.get("supported"))
            .and_then(Value::as_bool)
            == Some(true)
            && idempotency
                .and_then(|features| features.get("durable"))
                .and_then(Value::as_bool)
                == Some(true)
            && idempotency
                .and_then(|features| features.get("retention_seconds"))
                .and_then(Value::as_u64)
                .is_some_and(|seconds| seconds >= REQUIRED_IDEMPOTENCY_RETENTION_SECONDS);
        if !run_submission || !durable_idempotency {
            return Err(CoreError::RuntimeRejected);
        }
        Ok(())
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        authenticated: bool,
        idempotency_key: Option<&str>,
    ) -> CoreResult<Response> {
        let url = self.base.join(path).map_err(|_| invalid_response())?;
        let mut request = self.client.request(method, url);
        if authenticated {
            let value = HeaderValue::from_str(&format!("Bearer {}", self.api_key))
                .map_err(|_| CoreError::RuntimeRejected)?;
            request = request.header(AUTHORIZATION, value);
        }
        if let Some(key) = idempotency_key {
            let value = HeaderValue::from_str(key).map_err(|_| CoreError::RuntimeRejected)?;
            request = request.header("Idempotency-Key", value);
        }
        if let Some(body) = body {
            request = request.header(CONTENT_TYPE, "application/json").body(body);
        }
        request
            .send()
            .map_err(|_| CoreError::RuntimeUnavailable(UNAVAILABLE.into()))
    }

    fn ensure_session(&self, session_id: &str) -> CoreResult<()> {
        validate_session_id(session_id)?;
        let body = serde_json::to_vec(&serde_json::json!({ "id": session_id }))
            .map_err(|_| invalid_response())?;
        let response = self.request(Method::POST, "/api/sessions", Some(body), true, None)?;
        match response.status().as_u16() {
            201 | 409 => {
                discard_body(response)?;
                Ok(())
            }
            status => Err(status_error(status)),
        }
    }
}

impl Runtime for HermesRuntime {
    fn inject(&self, route: &Route, envelope: &Envelope, rendered: &str) -> CoreResult<String> {
        if !route.enabled {
            return Err(CoreError::RuntimeRejected);
        }
        validate_session_id(&route.runtime_target)?;
        // The route target is the private Hermes session id. It is never placed
        // in the central envelope or telemetry, and is sent only to Hermes.
        let idempotency_key = idempotency_key(&envelope.attempt_id)?;
        self.ensure_session(&route.runtime_target)?;
        let body = serde_json::to_vec(&serde_json::json!({
            "input": rendered,
            "session_id": route.runtime_target,
        }))
        .map_err(|_| invalid_response())?;
        let response = self.request(
            Method::POST,
            "/v1/runs",
            Some(body),
            true,
            Some(&idempotency_key),
        )?;
        let status = response.status().as_u16();
        if status != 202 {
            return Err(status_error(status));
        }
        let body = read_body(response)?;
        let value: Value = serde_json::from_slice(&body).map_err(|_| invalid_response())?;
        let run_id = value
            .get("run_id")
            .and_then(Value::as_str)
            .ok_or_else(invalid_response)?;
        if !is_bounded_visible_ascii(run_id, MAX_RUN_ID_BYTES) {
            return Err(invalid_response());
        }
        Ok(run_id.to_owned())
    }
}

fn validate_base_url(base_url: &str) -> CoreResult<Url> {
    let base = Url::parse(base_url).map_err(|_| CoreError::RuntimeRejected)?;
    let loopback = matches!(
        base.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
    );
    if !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
        || base.path() != "/"
    {
        return Err(CoreError::RuntimeRejected);
    }
    Ok(base)
}

fn validate_session_id(session_id: &str) -> CoreResult<()> {
    if session_id.is_empty()
        || session_id.len() > 255
        || session_id.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        || session_id.contains("..")
        || session_id.contains(['/', '\\'])
        || (session_id.len() >= 2
            && session_id.as_bytes()[0].is_ascii_alphabetic()
            && session_id.as_bytes()[1] == b':')
    {
        return Err(CoreError::RuntimeRejected);
    }
    Ok(())
}

fn idempotency_key(attempt_id: &str) -> CoreResult<String> {
    let key = format!("agent-journal:{attempt_id}");
    if is_bounded_visible_ascii(&key, 255) {
        Ok(key)
    } else {
        Err(CoreError::RuntimeRejected)
    }
}

fn is_bounded_visible_ascii(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

fn read_body(response: Response) -> CoreResult<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_VENDOR_BODY_BYTES)
    {
        return Err(invalid_response());
    }
    let mut body = Vec::new();
    response
        .take(MAX_VENDOR_BODY_BYTES + 1)
        .read_to_end(&mut body)
        .map_err(|_| CoreError::RuntimeUnavailable(UNAVAILABLE.into()))?;
    if body.len() as u64 > MAX_VENDOR_BODY_BYTES {
        return Err(invalid_response());
    }
    Ok(body)
}

fn discard_body(response: Response) -> CoreResult<()> {
    let _ = read_body(response)?;
    Ok(())
}

fn status_error(status: u16) -> CoreError {
    if status == 429 || (500..=599).contains(&status) {
        CoreError::RuntimeUnavailable(UNAVAILABLE.into())
    } else {
        CoreError::RuntimeRejected
    }
}

fn invalid_response() -> CoreError {
    CoreError::InvalidResponse
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_is_stable_visible_ascii_and_bounded() {
        assert_eq!(
            idempotency_key("attempt-42").unwrap(),
            "agent-journal:attempt-42"
        );
        assert!(idempotency_key(&"x".repeat(242)).is_err());
        assert!(idempotency_key("attempt\n42").is_err());
    }

    #[test]
    fn receipts_require_visible_ascii() {
        assert!(is_bounded_visible_ascii("run_abc", MAX_RUN_ID_BYTES));
        assert!(!is_bounded_visible_ascii("run abc", MAX_RUN_ID_BYTES));
        assert!(!is_bounded_visible_ascii(
            &"x".repeat(MAX_RUN_ID_BYTES + 1),
            MAX_RUN_ID_BYTES
        ));
    }

    #[test]
    fn session_ids_are_explicit_and_path_safe() {
        assert!(validate_session_id("session-1").is_ok());
        assert!(validate_session_id("").is_err());
        assert!(validate_session_id("../escape").is_err());
        assert!(validate_session_id("session\n1").is_err());
    }
}
