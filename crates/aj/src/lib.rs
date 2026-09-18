use journal_client::{
    Client, HttpTransport, journal_protocol::EnrollmentExchangeRequest, private_file,
};
use std::{io::Write, path::Path};

pub fn run(args: &[String], mut error: impl Write) -> i32 {
    match execute(args, &mut error) {
        Ok(()) => 0,
        Err(message) => {
            let _ = writeln!(error, "aj: {message}");
            1
        }
    }
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
