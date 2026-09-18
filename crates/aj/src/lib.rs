use journal_client::{
    Client, HttpTransport, journal_protocol::EnrollmentExchangeRequest, private_file,
};
use std::{io::Write, path::Path};

pub fn run(args: &[String], mut error: impl Write) -> i32 {
    run_with_output(args, std::io::stdout(), &mut error)
}

pub fn run_with_output(args: &[String], mut output: impl Write, mut error: impl Write) -> i32 {
    let result = if args.first().is_some_and(|s| s == "enroll") {
        execute(args, &mut error)
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

fn journal(args: &[String], output: &mut impl Write) -> Result<(), &'static str> {
    use journal_client::journal_protocol::*;
    use std::collections::BTreeMap;
    let Some(command) = args.first().map(String::as_str) else {
        return Err(
            "expected me, spaces, post, get, list, search, thread, enroll, adapter-register, adapter-heartbeat, mailbox-claim, or mailbox-status",
        );
    };
    let allowed: &[&str] = match command {
        "adapter-register" => &["--instance"],
        "adapter-heartbeat" => &["--instance", "--generation"],
        "mailbox-claim" => &["--instance", "--generation", "--limit", "--wait-seconds"],
        "mailbox-status" => &["--cursor", "--limit"],
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
                "expected me, spaces, post, get, list, search, thread, enroll, adapter-register, adapter-heartbeat, mailbox-claim, or mailbox-status",
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
        "adapter-register" => serde_json::to_value(client.register_adapter(token,&AdapterRegisterRequest { instance_id:required("--instance")?.into() }).map_err(|_|"registration failed")?),
        "adapter-heartbeat" => serde_json::to_value(client.heartbeat_adapter(token,&AdapterHeartbeatRequest { instance_id:required("--instance")?.into(),generation:required("--generation")?.parse().map_err(|_|"invalid generation")? }).map_err(|_|"heartbeat failed")?),
        "mailbox-claim" => serde_json::to_value(client.claim_mailbox(token,&ClaimRequest { instance_id:required("--instance")?.into(),generation:required("--generation")?.parse().map_err(|_|"invalid generation")?,limit:required("--limit")?.parse().map_err(|_|"invalid limit")?,wait_seconds:options.get("--wait-seconds").unwrap_or(&"0").parse().map_err(|_|"invalid wait")? }).map_err(|_|"claim failed or response lost; wait for lease expiry before retrying")?),
        "mailbox-status" => serde_json::to_value(client.mailbox_status(token,&PageQuery::from_query(&query).map_err(|_|"invalid pagination")?).map_err(|_|"status failed")?),
        "me" => serde_json::to_value(client.me(token).map_err(|_|"request failed")?),
        "spaces" => serde_json::to_value(client.spaces(token,&PageQuery::from_query(&query).map_err(|_|"invalid pagination")?).map_err(|_|"request failed")?),
        "get" => serde_json::to_value(client.get(token,required("--record")?).map_err(|_|"request failed")?),
        "thread" => serde_json::to_value(client.thread(token,required("--record")?,&PageQuery::from_query(&query).map_err(|_|"invalid pagination")?).map_err(|_|"request failed")?),
        "search" => serde_json::to_value(client.search(token,required("--space")?,&SearchRecordsQuery::from_query(&query).map_err(|_|"invalid filters")?).map_err(|_|"request failed")?),
        "list" => serde_json::to_value(client.list(token,required("--space")?,&ListRecordsQuery::from_query(&query).map_err(|_|"invalid filters")?).map_err(|_|"request failed")?),
        "post" => {
            use std::io::Read;
            let input = required("--input")?;
            let mut bytes = Vec::new();
            let max = 1_048_576;
            if input=="-" { std::io::stdin().take(max).read_to_end(&mut bytes).map_err(|_|"cannot read input")?; }
            else { std::fs::File::open(input).map_err(|_|"cannot open input")?.take(max).read_to_end(&mut bytes).map_err(|_|"cannot read input")?; }
            if bytes.len() as u64 == max { return Err("input too large"); }
            let request: AppendRecordRequest = decode_json(&bytes).map_err(|_|"invalid append JSON")?;
            serde_json::to_value(client.append(token,required("--space")?,required("--idempotency-key")?,&request).map_err(|_|"append failed or response lost; retry identical input with the same idempotency key")?)
        },
        _ => unreachable!(),
    }.map_err(|_|"cannot encode response")?;
    serde_json::to_writer(&mut *output, &result).map_err(|_| "cannot write response")?;
    writeln!(output).map_err(|_| "cannot write response")
}

fn execute(args: &[String], error: &mut impl Write) -> Result<(), &'static str> {
    if args.len() != 11
        || args[0] != "enroll"
        || args[1] != "--endpoint"
        || args[3] != "--ticket-file"
        || args[5] != "--instance-id"
        || args[7] != "--principal-file"
        || args[9] != "--delivery-file"
    {
        return Err(
            "usage: aj enroll --endpoint URL --ticket-file PATH --instance-id ID --principal-file PATH --delivery-file PATH",
        );
    }
    if args[8] == args[10] {
        return Err("principal and delivery credentials require separate files");
    }
    private_file::check_destination(Path::new(&args[8]))
        .map_err(|_| "invalid private principal destination")?;
    private_file::check_destination(Path::new(&args[10]))
        .map_err(|_| "invalid private delivery destination")?;
    let ticket =
        private_file::read(Path::new(&args[4])).map_err(|_| "cannot read private ticket file")?;
    let transport = HttpTransport::new(&args[2]).map_err(|_| "invalid endpoint")?;
    let client = Client::new(transport);
    let response = client.enroll(&ticket, &EnrollmentExchangeRequest {
        instance_id: args[6].clone(),
    }).map_err(|_| {
        let _ = writeln!(error, "aj: event=enrollment_response_failed server_outcome=unknown request_id={} recovery=enrollment-recover",
            client.last_request_id().as_deref().unwrap_or("unavailable"));
        "enrollment failed or response lost; use protected local enrollment recovery before retrying"
    })?;
    let principal = serde_json::to_vec(&response.principal_client_secret)
        .map_err(|_| "cannot encode credential")?;
    let delivery = serde_json::to_vec(&response.delivery_adapter_secret)
        .map_err(|_| "cannot encode credential")?;
    if private_file::write(Path::new(&args[8]), &principal).is_err()
        || private_file::write(Path::new(&args[10]), &delivery).is_err()
    {
        let _ = writeln!(
            error,
            "aj: event=credential_write_failed operation=enrollment server_outcome=committed request_id={} recovery=enrollment-recover",
            client.last_request_id().as_deref().unwrap_or("unavailable")
        );
        return Err(
            "credential persistence failed; use protected local enrollment recovery to revoke BOTH credentials; ticket remains consumed",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
