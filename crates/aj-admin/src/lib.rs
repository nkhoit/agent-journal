use journal_client::{Client, HttpTransport, journal_protocol::*, private_file};
use std::{io::Write, path::Path};

pub fn run(args: &[String], mut output: impl Write, mut error: impl Write) -> i32 {
    match execute(args, &mut output, &mut error) {
        Ok(()) => 0,
        Err(message) => {
            let _ = writeln!(error, "aj-admin: {message}");
            1
        }
    }
}

fn execute(
    args: &[String],
    output: &mut impl Write,
    error: &mut impl Write,
) -> Result<(), &'static str> {
    if args.len() < 3 || args[0] != "--socket" {
        return Err("usage: aj-admin --socket PATH COMMAND ARGS");
    }
    let client = Client::new(
        HttpTransport::unix(Path::new(&args[1]))
            .map_err(|_| "protected Unix socket unavailable or unsupported")?,
    );
    let a = &args[3..];
    let value = match args[2].as_str() {
        "metrics" if a.is_empty() => serde_json::to_value(client.metrics().map_err(|_|"metrics unavailable")?),
        "principal-create" if a.len() == 2 => serde_json::to_value(client.create_principal(
            &PrincipalCreateRequest { handle: a[0].clone(), display_name: a[1].clone() })
            .map_err(|_| "principal creation failed")?),
        "space-create" if a.len() == 2 => serde_json::to_value(client.create_space(
            &SpaceCreateRequest { access: domain::SpaceAccess::Public, id: a[0].clone(), name: a[1].clone() })
            .map_err(|_| "space creation failed")?),
        "membership-set" if a.len() == 5 => serde_json::to_value(client.set_membership(
            &MembershipRequest { space_id: a[0].clone(), principal_id: a[1].clone(),
                can_read: boolean(&a[2])?, can_append: boolean(&a[3])?, can_admin: boolean(&a[4])? })
            .map_err(|_| "membership update failed")?),
        "credential-rotate" if a.len() == 2 || a.len() == 3 => {
            private_file::check_destination(Path::new(&a[1]))
                .map_err(|_| "invalid private replacement destination")?;
            let response = client.rotate(&CredentialRotateRequest {
                credential_id: a[0].clone(), reason: a.get(2).cloned(),
            }).map_err(|_| {
                let _ = writeln!(error, "aj-admin: event=rotation_response_failed server_outcome=unknown request_id={} recovery=principal-recover",
                    client.last_request_id().as_deref().unwrap_or("unavailable"));
                "rotation failed or response lost; repeat principal-recover with the principal UUID"
            })?;
            let encoded = serde_json::to_vec(&response.replacement_secret).map_err(|_| "cannot encode credential")?;
            private_file::write(Path::new(&a[1]), &encoded)
                .map_err(|_| {
                    let _ = writeln!(error, "aj-admin: event=credential_write_failed operation=rotation server_outcome=committed request_id={} recovery=principal-recover",
                        client.last_request_id().as_deref().unwrap_or("unavailable"));
                    "replacement credential file write failed; repeat principal-recover with the principal UUID"
                })?;
            return Ok(());
        },
        "credential-revoke" if a.len() == 1 || a.len() == 2 => {
            client.revoke(&CredentialRotateRequest {
                credential_id: a[0].clone(), reason: a.get(1).cloned(),
            }).map_err(|_| "credential revocation failed")?;
            return Ok(());
        },
        "principal-recover" if a.len() == 2 || a.len() == 3 => {
            private_file::check_destination(Path::new(&a[1]))
                .map_err(|_| "invalid private replacement destination")?;
            let response = client.recover_principal(&PrincipalRecoveryRequest {
                principal_id: a[0].clone(),
                reason: a.get(2).cloned(),
            }).map_err(|_| {
                let _ = writeln!(error, "aj-admin: event=principal_recovery_response_failed server_outcome=unknown request_id={} recovery=repeat-principal-recover",
                    client.last_request_id().as_deref().unwrap_or("unavailable"));
                "principal recovery failed or response lost; repeat recovery by principal UUID"
            })?;
            let encoded = serde_json::to_vec(&OneTimePrincipalClientSecret {
                credential_id: response.replacement_secret.credential_id,
                secret: response.replacement_secret.secret,
            }).map_err(|_| "cannot encode credential")?;
            private_file::write(Path::new(&a[1]), &encoded).map_err(|_| {
                let _ = writeln!(error, "aj-admin: event=credential_write_failed operation=principal_recovery server_outcome=committed request_id={} recovery=repeat-principal-recover",
                    client.last_request_id().as_deref().unwrap_or("unavailable"));
                "replacement credential file write failed; repeat recovery by principal UUID"
            })?;
            return Ok(());
        },
        _ => return Err("commands: metrics; principal-create HANDLE DISPLAY_NAME; principal-recover PRINCIPAL_UUID OUTPUT [REASON]; space-create ID NAME; membership-set SPACE PRINCIPAL READ APPEND ADMIN; credential-rotate ID OUTPUT [REASON]; credential-revoke ID [REASON]"),
    }.map_err(|_| "cannot encode response")?;
    serde_json::to_writer(&mut *output, &value).map_err(|_| "cannot write response")?;
    writeln!(output).map_err(|_| "cannot write response")
}

fn boolean(value: &str) -> Result<bool, &'static str> {
    value
        .parse()
        .map_err(|_| "permissions must be true or false")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn usage_never_echoes_secrets() {
        let mut output = Vec::new();
        let mut errors = Vec::new();
        assert_eq!(run(&["private-input".into()], &mut output, &mut errors), 1);
        assert!(output.is_empty());
        assert!(!String::from_utf8(errors).unwrap().contains("private-input"));
    }

    #[test]
    fn principal_create_usage_describes_a_server_generated_id() {
        let mut output = Vec::new();
        let mut errors = Vec::new();
        assert_eq!(
            run(
                &["--socket".into(), "/ignored".into(), "unknown".into()],
                &mut output,
                &mut errors
            ),
            1
        );
        let usage = String::from_utf8(errors).unwrap();
        assert!(usage.contains("principal-create HANDLE DISPLAY_NAME"));
        assert!(!usage.contains("principal-create ID NAME"));
    }
}
