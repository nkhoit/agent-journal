//! Opt-in custody of one frozen append. Credentials are supplied separately.
#[cfg(unix)]
use journal_client::{Client, HttpTransport, journal_protocol::*, private_file};
#[cfg(unix)]
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::path::Path;
use std::{collections::BTreeMap, io::Write};

// Two copies of a maximally escaped 64 KiB record plus bounded metadata fit
// below this cap. Keep the smaller credential-file bounds unchanged.
#[cfg(unix)]
const MAX_STATE_BYTES: u64 = 2_097_152;

#[cfg(unix)]
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedPost {
    version: u8,
    endpoint: String,
    principal_id: String,
    space: String,
    key: String,
    request: AppendRecordRequest,
    outcome: Outcome,
}

#[cfg(unix)]
#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase", deny_unknown_fields)]
enum Outcome {
    Pending,
    Completed { receipt: Box<AppendRecordResponse> },
}

#[cfg(unix)]
fn valid_uuid(id: &str) -> bool {
    PrincipalRecoveryRequest {
        principal_id: id.into(),
        reason: None,
    }
    .validate()
    .is_ok()
}

#[cfg(unix)]
fn valid_key(key: &str) -> bool {
    (1..=255).contains(&key.chars().count()) && !key.chars().any(char::is_control)
}

#[cfg(unix)]
impl PreparedPost {
    fn validate(&self) -> Result<(), &'static str> {
        if self.version != 1 || !valid_uuid(&self.principal_id) || !valid_key(&self.key) {
            return Err("prepared post state is invalid");
        }
        domain::validate_identifier("space", &self.space)
            .map_err(|_| "prepared post state is invalid")?;
        canonical_append(&self.request).map_err(|_| "prepared post state is invalid")?;
        if let Outcome::Completed { receipt } = &self.outcome {
            self.validate_receipt(receipt)?;
        }
        Ok(())
    }

    fn validate_receipt(&self, receipt: &AppendRecordResponse) -> Result<(), &'static str> {
        let record = &receipt.record;
        if !valid_uuid(&record.id)
            || record.author != self.principal_id
            || record.space_id != self.space
            || record.seq < 1
            || record.created_at.is_empty()
            || record.content != self.request.content
            || record.kind != self.request.kind
            || record.run_id != self.request.run_id
            || record.routing_key != self.request.routing_key
            || record.relations != self.request.relations
            || record.title != domain::normalize_title(self.request.title.clone())
            || record.attention.len() > self.request.attention.len()
            || receipt.mailbox_created != record.attention.len()
            || record.attention.iter().any(|id| !valid_uuid(id))
            || record
                .attention
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != record.attention.len()
        {
            return Err("prepared post receipt does not match frozen operation");
        }
        Ok(())
    }
}

pub(super) fn run(
    options: &BTreeMap<&str, &str>,
    output: &mut impl Write,
    error: &mut impl Write,
) -> Result<(), &'static str> {
    #[cfg(not(unix))]
    {
        let _ = (options, output, error);
        Err("private prepared post state requires Unix")
    }
    #[cfg(unix)]
    {
        let required = |key| {
            options
                .get(key)
                .copied()
                .ok_or("missing required command option")
        };
        let endpoint = required("--endpoint")?;
        let path = Path::new(required("--state-file")?);
        // Serialize preparation, identity checking, append and receipt publication.
        let _lock =
            private_file::lock(path).map_err(|_| "cannot lock private prepared post state")?;
        let existing = match private_file::read_with_limit(path, MAX_STATE_BYTES) {
            Ok(text) => Some(text),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                private_file::check_destination(path)
                    .map_err(|_| "cannot establish private prepared post state")?;
                None
            }
            Err(_) => return Err("cannot read private prepared post state"),
        };
        let resumed = existing.is_some();
        let mut state = if let Some(text) = &existing {
            let state: PreparedPost =
                decode_json(text.as_bytes()).map_err(|_| "prepared post state is invalid")?;
            state.validate()?;
            // Check endpoint before constructing a transport or sending credentials.
            if state.endpoint != endpoint
                || options
                    .get("--space")
                    .is_some_and(|space| *space != state.space)
                || options
                    .get("--idempotency-key")
                    .is_some_and(|key| *key != state.key)
            {
                return Err("prepared post does not match endpoint, space or key");
            }
            if let Some(input) = options.get("--input") {
                let request = super::post_input(input, options.get("--title").copied())?;
                if canonical_append(&request).map_err(|_| "invalid append request")?
                    != canonical_append(&state.request)
                        .map_err(|_| "prepared post state is invalid")?
                {
                    return Err("prepared post does not match input or title");
                }
            } else if options
                .get("--title")
                .is_some_and(|title| Some(*title) != state.request.title.as_deref())
            {
                return Err("prepared post does not match input or title");
            }
            state
        } else {
            let space = required("--space")?;
            domain::validate_identifier("space", space).map_err(|_| "invalid space")?;
            let request = super::post_input(required("--input")?, options.get("--title").copied())?;
            // Canonicalization sorts attention only. Preserve raw title bytes:
            // server title normalization happens AFTER its idempotency hash.
            let request =
                decode_json(&canonical_append(&request).map_err(|_| "invalid append request")?)
                    .map_err(|_| "invalid append request")?;
            let key = if let Some(key) = options.get("--idempotency-key") {
                if !valid_key(key) {
                    return Err("invalid idempotency key");
                }
                (*key).to_owned()
            } else {
                let mut bytes = [0_u8; 32];
                getrandom::fill(&mut bytes).map_err(|_| "secure random source failed")?;
                super::hex(&bytes)
            };
            PreparedPost {
                version: 1,
                endpoint: endpoint.into(),
                principal_id: String::new(),
                space: space.into(),
                key,
                request,
                outcome: Outcome::Pending,
            }
        };
        let client = Client::new(HttpTransport::new(endpoint).map_err(|_| "invalid endpoint")?);
        let credential = private_file::read(Path::new(required("--credential-file")?))
            .map_err(|_| "cannot read private credential file")?;
        let credential: OneTimePrincipalClientSecret =
            decode_json(credential.as_bytes()).map_err(|_| "invalid credential file")?;
        // Credential DTOs and mutable handles are not authoritative UUID evidence.
        let identity = client
            .me(&credential.secret)
            .map_err(|_| "cannot verify prepared post principal through authenticated me")?;
        if !valid_uuid(&identity.principal.id) {
            return Err("authenticated principal UUID is invalid");
        }
        if existing.is_some() && state.principal_id != identity.principal.id {
            return Err("prepared post does not match authenticated principal UUID");
        }
        state.principal_id = identity.principal.id;
        let encoded = match existing {
            Some(text) => text.into_bytes(),
            None => {
                let encoded = encode_state(&state)?;
                private_file::write(path, &encoded).map_err(
                    |_| "cannot persist private prepared post state; append was not sent",
                )?;
                encoded
            }
        };
        if let Outcome::Completed { receipt } = &state.outcome {
            let _ = writeln!(
                error,
                "aj: completed prepared post receipt is historical; record existence has not been verified; reconcile explicitly after server recovery"
            );
            return print_receipt(output, receipt);
        }
        if resumed {
            // Retry must also establish durability if an earlier publication
            // became visible but its directory sync failed. The byte check
            // rejects evidence changed during authenticated identity lookup.
            private_file::replace_with_limit(path, &encoded, &encoded, MAX_STATE_BYTES)
                .map_err(|_| "cannot persist private prepared post state; append was not sent")?;
        }
        let receipt = client
            .append(&credential.secret, &state.space, &state.key, &state.request)
            .map_err(
                |_| "append failed or response lost; retry the same prepared post state file",
            )?;
        state.validate_receipt(&receipt)
            .map_err(|_| "append receipt is invalid; server outcome uncertain; retry the same prepared post state file")?;
        state.outcome = Outcome::Completed {
            receipt: Box::new(receipt),
        };
        let completed = encode_state(&state)
            .map_err(|_| "append committed but receipt encoding failed; retry the same prepared post state file")?;
        private_file::replace_with_limit(path, &encoded, &completed, MAX_STATE_BYTES)
            .map_err(|_| "append committed but receipt persistence failed; retry the same prepared post state file")?;
        if let Outcome::Completed { receipt } = &state.outcome {
            if super::should_nudge(receipt) {
                let _ = writeln!(
                    error,
                    "hint: this thread has no title; pass --title \"...\" on future posts that start a discussion"
                );
            }
            print_receipt(output, receipt)
        } else {
            unreachable!()
        }
    }
}

#[cfg(unix)]
fn encode_state(state: &PreparedPost) -> Result<Vec<u8>, &'static str> {
    let encoded = serde_json::to_vec(state).map_err(|_| "cannot encode prepared post state")?;
    if encoded.len() as u64 > MAX_STATE_BYTES {
        return Err("prepared post state is too large");
    }
    Ok(encoded)
}

#[cfg(unix)]
fn print_receipt(
    output: &mut impl Write,
    receipt: &AppendRecordResponse,
) -> Result<(), &'static str> {
    serde_json::to_writer(&mut *output, receipt).map_err(|_| "cannot write response")?;
    writeln!(output).map_err(|_| "cannot write response")
}
