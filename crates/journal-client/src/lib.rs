//! Transport seam for the authenticated client. Request construction and
//! network behavior are intentionally pending.

use journal_protocol::{Request, Response, Transport, TransportError};
use thiserror::Error;

pub use journal_protocol;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("journal service unavailable")]
    Unavailable,
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
}

pub struct Client {
    transport: Option<Box<dyn Transport>>,
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
    pub fn new<T>(transport: T) -> Self
    where
        T: Transport + 'static,
    {
        Self {
            transport: Some(Box::new(transport)),
        }
    }

    pub fn without_transport() -> Self {
        Self { transport: None }
    }

    pub fn transport_configured(&self) -> bool {
        self.transport.is_some()
    }

    pub fn send(&self, request: Request) -> Result<Response, ClientError> {
        self.transport
            .as_ref()
            .ok_or(ClientError::Unavailable)?
            .send(request)
            .map_err(ClientError::Transport)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_transport_is_not_reported_as_configured() {
        assert!(!Client::without_transport().transport_configured());
    }
}
