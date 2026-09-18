//! Typed bootstrap client with HTTP and protected Unix-socket transports.

pub mod private_file;
mod transport;
pub use transport::HttpTransport;

use journal_protocol::{Request, Response, Transport, TransportError};
use thiserror::Error;

pub use journal_protocol;

pub use journal_protocol::{
    CredentialRotationResponse as RotationResponse, EnrollmentRecoveryRequest,
    OneTimeReplacementSecret as ReplacementSecret,
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP request failed with status {status}")]
    Http { status: u16 },
    #[error("invalid JSON payload")]
    Json,
    #[error("invalid request")]
    InvalidRequest,
    #[error("journal service unavailable")]
    Unavailable,
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
}

pub struct Client {
    transport: Option<Box<dyn Transport>>,
    last_request_id: std::sync::Mutex<Option<String>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("transport_configured", &self.transport.is_some())
            .finish()
    }
}

impl Client {
    fn principal<O: serde::de::DeserializeOwned + serde::Serialize>(
        &self,
        token: &str,
        mut request: Request,
    ) -> Result<O, ClientError> {
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ClientError::InvalidRequest);
        }
        request
            .headers
            .insert("Authorization".into(), format!("Bearer {token}"));
        let response = self.send(request)?;
        if !(200..300).contains(&response.status) {
            return Err(ClientError::Http {
                status: response.status,
            });
        }
        journal_protocol::decode_json(&response.body).map_err(|_| ClientError::Json)
    }

    pub fn me(&self, token: &str) -> Result<journal_protocol::Me, ClientError> {
        self.principal(token, Request::new("GET", "/v1/me", vec![]))
    }

    pub fn spaces(
        &self,
        token: &str,
        query: &journal_protocol::PageQuery,
    ) -> Result<journal_protocol::SpacePage, ClientError> {
        query.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.principal(
            token,
            Request::new(
                "GET",
                format!(
                    "/v1/spaces?{}",
                    journal_protocol::query_string(&query.pairs())
                ),
                vec![],
            ),
        )
    }

    pub fn principals(
        &self,
        token: &str,
        query: &journal_protocol::ListPrincipalsQuery,
    ) -> Result<journal_protocol::PrincipalPage, ClientError> {
        query.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.principal(
            token,
            Request::new(
                "GET",
                format!(
                    "/v1/principals?{}",
                    journal_protocol::query_string(&query.pairs())
                ),
                vec![],
            ),
        )
    }

    pub fn space(
        &self,
        token: &str,
        space: &str,
    ) -> Result<journal_protocol::domain::Space, ClientError> {
        journal_protocol::domain::validate_identifier("space", space)
            .map_err(|_| ClientError::InvalidRequest)?;
        self.principal(
            token,
            Request::new(
                "GET",
                format!("/v1/spaces/{}", journal_protocol::path_segment(space)),
                vec![],
            ),
        )
    }

    pub fn append(
        &self,
        token: &str,
        space: &str,
        key: &str,
        input: &journal_protocol::AppendRecordRequest,
    ) -> Result<journal_protocol::AppendRecordResponse, ClientError> {
        journal_protocol::domain::validate_identifier("space", space)
            .map_err(|_| ClientError::InvalidRequest)?;
        if !(1..=255).contains(&key.chars().count()) || key.chars().any(char::is_control) {
            return Err(ClientError::InvalidRequest);
        }
        let body =
            journal_protocol::canonical_append(input).map_err(|_| ClientError::InvalidRequest)?;
        let mut request = Request::new(
            "POST",
            format!(
                "/v1/spaces/{}/records",
                journal_protocol::path_segment(space)
            ),
            body,
        );
        request
            .headers
            .insert("Content-Type".into(), "application/json".into());
        request.headers.insert("Idempotency-Key".into(), key.into());
        self.principal(token, request)
    }

    pub fn get(
        &self,
        token: &str,
        id: &str,
    ) -> Result<journal_protocol::domain::Record, ClientError> {
        journal_protocol::domain::validate_identifier("record_id", id)
            .map_err(|_| ClientError::InvalidRequest)?;
        self.principal(
            token,
            Request::new(
                "GET",
                format!("/v1/records/{}", journal_protocol::path_segment(id)),
                vec![],
            ),
        )
    }

    pub fn list(
        &self,
        token: &str,
        space: &str,
        query: &journal_protocol::ListRecordsQuery,
    ) -> Result<journal_protocol::RecordPage, ClientError> {
        journal_protocol::domain::validate_identifier("space", space)
            .map_err(|_| ClientError::InvalidRequest)?;
        query.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.principal(
            token,
            Request::new(
                "GET",
                format!(
                    "/v1/spaces/{}/records?{}",
                    journal_protocol::path_segment(space),
                    journal_protocol::query_string(&query.pairs())
                ),
                vec![],
            ),
        )
    }

    fn json<I: serde::Serialize, O: serde::de::DeserializeOwned + serde::Serialize>(
        &self,
        path: &str,
        input: &I,
        bearer: Option<&str>,
    ) -> Result<O, ClientError> {
        let mut request = Request::new(
            "POST",
            path,
            serde_json::to_vec(input).map_err(|_| ClientError::Json)?,
        );
        request
            .headers
            .insert("Content-Type".into(), "application/json".into());
        if let Some(bearer) = bearer {
            if bearer.is_empty() || !bearer.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(ClientError::InvalidRequest);
            }
            request
                .headers
                .insert("Authorization".into(), format!("Bearer {bearer}"));
        }
        let response = self.send(request)?;
        if !(200..300).contains(&response.status) {
            return Err(ClientError::Http {
                status: response.status,
            });
        }
        journal_protocol::decode_json(&response.body).map_err(|_| ClientError::Json)
    }

    pub fn create_principal(
        &self,
        input: &journal_protocol::PrincipalCreateRequest,
    ) -> Result<journal_protocol::domain::Principal, ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.json("/v1/admin/principals", input, None)
    }

    pub fn create_space(
        &self,
        input: &journal_protocol::SpaceCreateRequest,
    ) -> Result<journal_protocol::domain::Space, ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.json("/v1/admin/spaces", input, None)
    }

    pub fn set_membership(
        &self,
        input: &journal_protocol::MembershipRequest,
    ) -> Result<journal_protocol::Membership, ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.json("/v1/admin/memberships", input, None)
    }

    pub fn provision_adapter(
        &self,
        input: &journal_protocol::AdapterProvisionRequest,
    ) -> Result<journal_protocol::AdapterProvisionResponse, ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.json("/v1/admin/adapters", input, None)
    }

    pub fn create_ticket(
        &self,
        input: &journal_protocol::EnrollmentTicketCreateRequest,
    ) -> Result<journal_protocol::EnrollmentTicketCreateResponse, ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.json("/v1/admin/enrollment-tickets", input, None)
    }

    pub fn enroll(
        &self,
        ticket: &str,
        input: &journal_protocol::EnrollmentExchangeRequest,
    ) -> Result<journal_protocol::EnrollmentExchangeResponse, ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.json("/v1/enrollment/exchange", input, Some(ticket))
    }

    pub fn rotate(
        &self,
        input: &journal_protocol::CredentialRotateRequest,
    ) -> Result<RotationResponse, ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.json("/v1/admin/credentials/rotate", input, None)
    }

    pub fn revoke(
        &self,
        input: &journal_protocol::CredentialRotateRequest,
    ) -> Result<(), ClientError> {
        input.validate().map_err(|_| ClientError::InvalidRequest)?;
        self.mutate("/v1/admin/credentials/revoke", input)
    }

    pub fn recover_enrollment(&self, input: &EnrollmentRecoveryRequest) -> Result<(), ClientError> {
        journal_protocol::domain::validate_identifier("adapter_id", &input.adapter_id)
            .map_err(|_| ClientError::InvalidRequest)?;
        journal_protocol::domain::validate_identifier("instance_id", &input.instance_id)
            .map_err(|_| ClientError::InvalidRequest)?;
        self.mutate("/v1/admin/enrollment/recover", input)
    }

    fn mutate(&self, path: &str, input: &impl serde::Serialize) -> Result<(), ClientError> {
        let mut request = Request::new(
            "POST",
            path,
            serde_json::to_vec(input).map_err(|_| ClientError::Json)?,
        );
        request
            .headers
            .insert("Content-Type".into(), "application/json".into());
        let response = self.send(request)?;
        if !(200..300).contains(&response.status) {
            return Err(ClientError::Http {
                status: response.status,
            });
        }
        Ok(())
    }

    pub fn new<T>(transport: T) -> Self
    where
        T: Transport + 'static,
    {
        Self {
            transport: Some(Box::new(transport)),
            last_request_id: std::sync::Mutex::new(None),
        }
    }

    pub fn without_transport() -> Self {
        Self {
            transport: None,
            last_request_id: std::sync::Mutex::new(None),
        }
    }

    pub fn transport_configured(&self) -> bool {
        self.transport.is_some()
    }

    pub fn send(&self, request: Request) -> Result<Response, ClientError> {
        if let Ok(mut id) = self.last_request_id.lock() {
            *id = None;
        }
        let response = self
            .transport
            .as_ref()
            .ok_or(ClientError::Unavailable)?
            .send(request)
            .map_err(ClientError::Transport)?;
        if let Ok(mut id) = self.last_request_id.lock() {
            *id = response
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("x-request-id"))
                .map(|(_, value)| value)
                .filter(|value| safe_request_id(value))
                .cloned();
        }
        Ok(response)
    }

    /// Correlation for sequential CLI calls, not concurrent request attribution.
    pub fn last_request_id(&self) -> Option<String> {
        self.last_request_id.lock().ok().and_then(|id| id.clone())
    }
}

fn safe_request_id(value: &str) -> bool {
    value.len() <= 80
        && value.split('-').count() == 3
        && value
            .split('-')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_transport_is_not_reported_as_configured() {
        assert!(!Client::without_transport().transport_configured());
    }

    #[test]
    fn correlation_rejects_credentials_and_log_injection() {
        assert!(safe_request_id("abc123-123-0"));
        for value in [
            "secret-canary",
            "abc-1-0\ninjected",
            "aa--bb",
            &"e".repeat(64),
        ] {
            assert!(!safe_request_id(value));
        }
    }
}
