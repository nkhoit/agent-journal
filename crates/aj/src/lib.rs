use journal_client::{Client, HttpTransport, private_file};
use serde::{Deserialize, Serialize};
use std::{io::Write, path::Path};

const USAGE: &str = "Usage: aj COMMAND [OPTIONS]

Commands:
  register --endpoint URL --state-file PATH --handle HANDLE --display-name NAME
  me --endpoint URL --credential-file PATH
  spaces --endpoint URL --credential-file PATH [--cursor CURSOR] [--limit N]
  post --endpoint URL --credential-file PATH --space SPACE --idempotency-key KEY --input PATH|-
  get --endpoint URL --credential-file PATH --record RECORD_ID
  list --endpoint URL --credential-file PATH --space SPACE [filters]
  search --endpoint URL --credential-file PATH --space SPACE --q QUERY [filters]
  thread --endpoint URL --credential-file PATH --record RECORD_ID [--cursor CURSOR] [--limit N]
  inbox --endpoint URL --credential-file PATH [--state unacknowledged|acknowledged|all] [--cursor CURSOR] [--limit N]
  inbox-ack --endpoint URL --credential-file PATH --item ID
  delivery-status --endpoint URL --credential-file PATH --record RECORD_ID [filters]

Journal reads return compact JSON on stdout; inbox-ack returns no output. Registration writes
protected files and emits no secret-bearing output. Use `aj --help` for this summary.";

pub fn run(args: &[String], mut error: impl Write) -> i32 {
    run_with_output(args, std::io::stdout(), &mut error)
}

pub fn run_with_output(args: &[String], mut output: impl Write, mut error: impl Write) -> i32 {
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        let _ = writeln!(output, "{USAGE}");
        return 0;
    }
    let result = if args.first().is_some_and(|s| s == "register") {
        register(args, &mut output, &mut error)
    } else {
        journal(args, &mut output)
    };
    match result {
        Ok(()) => 0,
        Err(message) => {
            let _ = writeln!(error, "aj: {message}");
            1
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingRegistration {
    version: u8,
    endpoint: String,
    request: journal_client::journal_protocol::RegistrationRequest,
    token: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CompletedRegistration {
    credential_id: String,
    secret: String,
    principal: journal_client::journal_protocol::domain::Principal,
    endpoint: String,
    request: journal_client::journal_protocol::RegistrationRequest,
}

fn register(
    args: &[String],
    output: &mut impl Write,
    error: &mut impl Write,
) -> Result<(), &'static str> {
    if args.len() != 9
        || args[0] != "register"
        || args[1] != "--endpoint"
        || args[3] != "--state-file"
        || args[5] != "--handle"
        || args[7] != "--display-name"
    {
        return Err(
            "usage: aj register --endpoint URL --state-file PATH --handle HANDLE --display-name NAME",
        );
    }
    #[cfg(not(unix))]
    {
        let _ = (output, error);
        Err("private registration state requires Unix")
    }

    #[cfg(unix)]
    {
        let path = Path::new(&args[4]);
        let _state_lock =
            private_file::lock(path).map_err(|_| "cannot lock private registration state")?;
        let requested = journal_client::journal_protocol::RegistrationRequest {
            handle: args[6].clone(),
            display_name: args[8].clone(),
        };
        requested.validate().map_err(|_| "invalid registration")?;
        let (pending, encoded) = match private_file::read(path) {
            Ok(text) => {
                if let Ok(completed) = journal_client::journal_protocol::decode_json::<
                    CompletedRegistration,
                >(text.as_bytes())
                {
                    if completed.endpoint != args[2]
                        || completed.request != requested
                        || completed.principal.handle != completed.request.handle
                        || completed.principal.display_name != completed.request.display_name
                        || completed.credential_id.is_empty()
                        || completed.secret.len() != 64
                        || !completed
                            .secret
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    {
                        return Err("registration state does not match endpoint or profile");
                    }
                    let receipt = journal_client::journal_protocol::RegistrationReceipt {
                        principal: completed.principal,
                        credential_id: completed.credential_id,
                    };
                    serde_json::to_writer(&mut *output, &receipt)
                        .map_err(|_| "cannot write response")?;
                    return writeln!(output).map_err(|_| "cannot write response");
                }
                let pending: PendingRegistration =
                    journal_client::journal_protocol::decode_json(text.as_bytes())
                        .map_err(|_| "registration state is invalid")?;
                if pending.version != 1
                    || pending.endpoint != args[2]
                    || pending.request != requested
                {
                    return Err("registration state does not match endpoint or profile");
                }
                (pending, text.into_bytes())
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                private_file::check_destination(path)
                    .map_err(|_| "cannot establish private registration state")?;
                let mut bytes = [0_u8; 32];
                getrandom::fill(&mut bytes).map_err(|_| "secure random source failed")?;
                let pending = PendingRegistration {
                    version: 1,
                    endpoint: args[2].clone(),
                    request: requested,
                    token: hex(&bytes),
                };
                let encoded =
                    serde_json::to_vec(&pending).map_err(|_| "cannot encode registration state")?;
                private_file::write(path, &encoded)
                    .map_err(|_| "cannot persist private registration state")?;
                (pending, encoded)
            }
            Err(_) => return Err("cannot read private registration state"),
        };
        let client =
            Client::new(HttpTransport::new(&pending.endpoint).map_err(|_| "invalid endpoint")?);
        let response = client.register(&pending.token, &pending.request).map_err(|_| {
        let _ = writeln!(
            error,
            "aj: event=registration_response_failed server_outcome=unknown request_id={} recovery=retry-same-state-file",
            client.last_request_id().as_deref().unwrap_or("unavailable")
        );
        "registration failed or response lost; retry the same state file"
    })?;
        let credential = CompletedRegistration {
            credential_id: response.receipt.credential_id.clone(),
            secret: pending.token,
            principal: response.receipt.principal.clone(),
            endpoint: pending.endpoint,
            request: pending.request,
        };
        let completed =
            serde_json::to_vec(&credential).map_err(|_| "cannot encode credential file")?;
        private_file::replace(path, &encoded, &completed).map_err(|_| {
        let _ = writeln!(
            error,
            "aj: event=credential_write_failed operation=registration server_outcome=committed request_id={} recovery=retry-same-state-file",
            client.last_request_id().as_deref().unwrap_or("unavailable")
        );
        "credential persistence failed; retry the same state file"
    })?;
        serde_json::to_writer(&mut *output, &response.receipt)
            .map_err(|_| "cannot write response")?;
        writeln!(output).map_err(|_| "cannot write response")
    }
}

#[cfg(unix)]
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                DIGITS[(byte >> 4) as usize] as char,
                DIGITS[(byte & 15) as usize] as char,
            ]
        })
        .collect()
}

fn read_input(input: &str) -> Result<Vec<u8>, &'static str> {
    use std::io::Read;
    let mut bytes = Vec::new();
    let max = 1_048_576;
    if input == "-" {
        std::io::stdin()
            .take(max)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read input")?;
    } else {
        std::fs::File::open(input)
            .map_err(|_| "cannot open input")?
            .take(max)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read input")?;
    }
    if bytes.len() as u64 == max {
        return Err("input too large");
    }
    Ok(bytes)
}

fn journal(args: &[String], output: &mut impl Write) -> Result<(), &'static str> {
    use journal_client::journal_protocol::*;
    use std::collections::BTreeMap;
    let Some(command) = args.first().map(String::as_str) else {
        return Err(
            "expected register, me, spaces, post, get, list, search, thread, inbox, inbox-ack, or delivery-status",
        );
    };
    let allowed: &[&str] = match command {
        "delivery-status" => &["--record", "--cursor", "--limit"],
        "inbox" => &["--state", "--cursor", "--limit"],
        "inbox-ack" => &["--item"],
        "me" => &[],
        "spaces" => &["--cursor", "--limit"],
        "post" => &["--space", "--idempotency-key", "--input"],
        "get" => &["--record"],
        "thread" => &["--record", "--cursor", "--limit"],
        "search" => &[
            "--space",
            "--q",
            "--cursor",
            "--limit",
            "--author",
            "--attention",
            "--since",
            "--order",
        ],
        "list" => &[
            "--space",
            "--cursor",
            "--limit",
            "--after-seq",
            "--author",
            "--attention",
            "--kind",
            "--relation",
        ],
        _ => {
            return Err(
                "expected register, me, spaces, post, get, list, search, thread, inbox, inbox-ack, or delivery-status",
            );
        }
    };
    let mut options = BTreeMap::new();
    for pair in args[1..].chunks(2) {
        if pair.len() != 2
            || (!["--endpoint", "--credential-file"].contains(&pair[0].as_str())
                && !allowed.contains(&pair[0].as_str()))
            || options.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return Err("invalid or duplicate command option");
        }
    }
    let required = |key| {
        options
            .get(key)
            .copied()
            .ok_or("missing required command option")
    };
    let bytes = private_file::read(Path::new(required("--credential-file")?))
        .map_err(|_| "cannot read private credential file")?;
    let credential: OneTimePrincipalClientSecret =
        decode_json(bytes.as_bytes()).map_err(|_| "invalid credential file")?;
    let client =
        Client::new(HttpTransport::new(required("--endpoint")?).map_err(|_| "invalid endpoint")?);
    let query_pairs: Vec<_> = options
        .iter()
        .filter(|(key, _)| {
            [
                "--cursor",
                "--limit",
                "--after-seq",
                "--author",
                "--attention",
                "--kind",
                "--relation",
                "--q",
                "--since",
                "--order",
                "--state",
            ]
            .contains(key)
        })
        .map(|(key, value)| {
            (
                key.trim_start_matches("--").replace('-', "_"),
                (*value).to_owned(),
            )
        })
        .collect();
    let query = query_string(&query_pairs);
    let token = &credential.secret;
    let result = match command {
        "inbox" => serde_json::to_value(client.inbox(token, &InboxQuery::from_query(&query).map_err(|_|"invalid inbox query")?).map_err(|_|"inbox fetch failed")?),
        "inbox-ack" => {
            client.acknowledge_inbox_item(token,required("--item")?).map_err(|_|"acknowledgment failed or response lost; retry the same item")?;
            return Ok(());
        },
        "delivery-status" => serde_json::to_value(client.delivery_status(token,required("--record")?,&PageQuery::from_query(&query).map_err(|_|"invalid pagination")?).map_err(|_|"status failed")?),
        "me" => serde_json::to_value(client.me(token).map_err(|_|"request failed")?),
        "spaces" => serde_json::to_value(client.spaces(token,&PageQuery::from_query(&query).map_err(|_|"invalid pagination")?).map_err(|_|"request failed")?),
        "get" => serde_json::to_value(client.get(token,required("--record")?).map_err(|_|"request failed")?),
        "thread" => serde_json::to_value(client.thread(token,required("--record")?,&PageQuery::from_query(&query).map_err(|_|"invalid pagination")?).map_err(|_|"request failed")?),
        "search" => serde_json::to_value(client.search(token,required("--space")?,&SearchRecordsQuery::from_query(&query).map_err(|_|"invalid filters")?).map_err(|_|"request failed")?),
        "list" => serde_json::to_value(client.list(token,required("--space")?,&ListRecordsQuery::from_query(&query).map_err(|_|"invalid filters")?).map_err(|_|"request failed")?),
        "post" => {
            let bytes = read_input(required("--input")?)?;
            let request: AppendRecordRequest = decode_json(&bytes).map_err(|_|"invalid append JSON")?;
            serde_json::to_value(client.append(token,required("--space")?,required("--idempotency-key")?,&request).map_err(|_|"append failed or response lost; retry identical input with the same idempotency key")?)
        },
        _ => unreachable!(),
    }.map_err(|_|"cannot encode response")?;
    serde_json::to_writer(&mut *output, &result).map_err(|_| "cannot write response")?;
    writeln!(output).map_err(|_| "cannot write response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_is_local_and_describes_the_executable_surface() {
        let mut output = Vec::new();
        let mut errors = Vec::new();
        assert_eq!(
            run_with_output(&["--help".into()], &mut output, &mut errors),
            0
        );
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("Usage: aj COMMAND [OPTIONS]")
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn usage_does_not_echo_arguments() {
        let mut errors = Vec::new();
        assert_eq!(run(&["sensitive-input".into()], &mut errors), 1);
        assert!(
            !String::from_utf8(errors)
                .unwrap()
                .contains("sensitive-input")
        );
    }

    #[test]
    fn completed_registration_is_an_existing_credential_file() {
        let completed = CompletedRegistration {
            credential_id: "credential-example".into(),
            secret: "ab".repeat(32),
            principal: journal_client::journal_protocol::domain::Principal {
                id: "018f1f59-6e90-7000-8000-000000000001".into(),
                handle: "agent-example".into(),
                display_name: "Example".into(),
                description: None,
                profile_revision: 1,
                created_at: "2026-01-01T00:00:00Z".into(),
                disabled: false,
            },
            endpoint: "https://journal.example.invalid".into(),
            request: journal_client::journal_protocol::RegistrationRequest {
                handle: "agent-example".into(),
                display_name: "Example".into(),
            },
        };
        let encoded = serde_json::to_vec(&completed).unwrap();
        let credential: journal_client::journal_protocol::OneTimePrincipalClientSecret =
            journal_client::journal_protocol::decode_json(&encoded).unwrap();
        assert_eq!(credential.credential_id, completed.credential_id);
        assert_eq!(credential.secret, completed.secret);
    }

    #[cfg(not(unix))]
    #[test]
    fn registration_fails_before_network_without_private_storage_support() {
        let path =
            std::env::temp_dir().join(format!("unsupported-registration-{}", std::process::id()));
        let mut output = Vec::new();
        let mut errors = Vec::new();
        let status = run_with_output(
            &[
                "register".into(),
                "--endpoint".into(),
                "http://127.0.0.1:9".into(),
                "--state-file".into(),
                path.to_string_lossy().into_owned(),
                "--handle".into(),
                "agent-example".into(),
                "--display-name".into(),
                "Example".into(),
            ],
            &mut output,
            &mut errors,
        );
        assert_eq!(status, 1);
        assert!(output.is_empty());
        assert!(
            String::from_utf8(errors)
                .unwrap()
                .contains("private registration state requires Unix")
        );
        assert!(!path.exists());
    }
}
