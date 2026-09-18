use crate::*;
use journal_client::{Client, ClientError, journal_protocol as wire};
use serde::{Serialize, de::DeserializeOwned};

/// Owns only a delivery credential. Publishing is deliberately not a port.
pub struct DeliveryJournal {
    client: Client,
    credential: String,
}

impl DeliveryJournal {
    pub fn new(client: Client, delivery_credential: String) -> Self {
        Self {
            client,
            credential: delivery_credential,
        }
    }
}

fn error(error: ClientError) -> CoreError {
    match error {
        ClientError::Transport(wire::TransportError::Unavailable) | ClientError::Unavailable => {
            CoreError::JournalUnavailable
        }
        ClientError::Http {
            status: 429 | 500..=599,
        } => CoreError::JournalUnavailable,
        ClientError::Http { status } => CoreError::JournalRejected(status),
        _ => CoreError::InvalidResponse,
    }
}

fn convert<T: Serialize, U: DeserializeOwned>(value: T) -> CoreResult<U> {
    serde_json::from_value(serde_json::to_value(value).map_err(|_| CoreError::InvalidResponse)?)
        .map_err(|_| CoreError::InvalidResponse)
}

impl Journal for DeliveryJournal {
    fn register(&self, request: RegisterRequest) -> CoreResult<Registration> {
        convert(
            self.client
                .register_adapter(
                    &self.credential,
                    &wire::AdapterRegisterRequest {
                        instance_id: request.instance_id,
                    },
                )
                .map_err(error)?,
        )
    }

    fn heartbeat(&self, request: HeartbeatRequest) -> CoreResult<Registration> {
        convert(
            self.client
                .heartbeat_adapter(
                    &self.credential,
                    &wire::AdapterHeartbeatRequest {
                        instance_id: request.instance_id,
                        generation: request.generation,
                    },
                )
                .map_err(error)?,
        )
    }

    fn claim(&self, request: ClaimRequest) -> CoreResult<ClaimBatch> {
        request.validate()?;
        convert(
            self.client
                .claim_mailbox(
                    &self.credential,
                    &wire::ClaimRequest {
                        instance_id: request.instance_id,
                        generation: request.generation,
                        limit: request.limit,
                        wait_seconds: request.wait_seconds,
                    },
                )
                .map_err(error)?,
        )
    }

    fn commit_host_custody(&self, request: CustodyRequest) -> CoreResult<CustodyResult> {
        request.validate()?;
        convert(
            self.client
                .commit_custody(
                    &self.credential,
                    &request.claim_id,
                    &wire::CommitRequest {
                        generation: request.generation,
                        items: request
                            .items
                            .into_iter()
                            .map(|item| wire::CommitItem {
                                mailbox_item_id: item.mailbox_item_id,
                                attempt_id: item.attempt_id,
                            })
                            .collect(),
                    },
                )
                .map_err(error)?,
        )
    }

    fn record_event(&self, mailbox_item_id: &str, request: EventRequest) -> CoreResult<()> {
        request.validate()?;
        let response = self
            .client
            .record_delivery_event(
                &self.credential,
                mailbox_item_id,
                &convert(request.clone())?,
            )
            .map_err(error)?;
        if response.event_id != request.event_id || response.state != request.state {
            return Err(CoreError::InvalidResponse);
        }
        Ok(())
    }
}
