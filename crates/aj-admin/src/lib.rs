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
        "mailbox-requeue" if a.len()==1 || a.len()==2 => serde_json::to_value(client.requeue_mailbox_item(
            &a[0],&RequeueRequest {reason:a.get(1).cloned()}).map_err(|_|"requeue failed or response lost; inspect status before retrying")?),
        "adapters" if a.len()<=2 => serde_json::to_value(client.list_adapters(
            &PageQuery {cursor:a.get(1).cloned(),limit:a.first().map(|s|s.parse()).transpose().map_err(|_|"invalid limit")?}).map_err(|_|"adapter listing failed")?),
        "adapter-replace" if a.len() == 3 || a.len() == 4 => serde_json::to_value(client.replace_adapter(
            &a[0], &AdapterReplaceRequest { expected_generation:a[1].parse().map_err(|_|"invalid generation")?,
                new_instance_id:a[2].clone(), reason:a.get(3).cloned() }).map_err(|_|"adapter replacement failed")?),
        "mailbox-status" if a.len() == 1 => serde_json::to_value(client.admin_mailbox_status(
            &a[0], &PageQuery { cursor:None,limit:None }).map_err(|_|"mailbox status failed")?),
        "principal-create" if a.len() == 2 => serde_json::to_value(client.create_principal(
            &PrincipalCreateRequest { id: a[0].clone(), display_name: a[1].clone() })
            .map_err(|_| "principal creation failed")?),
        "space-create" if a.len() == 2 => serde_json::to_value(client.create_space(
            &SpaceCreateRequest { id: a[0].clone(), name: a[1].clone() })
            .map_err(|_| "space creation failed")?),
        "membership-set" if a.len() == 5 => serde_json::to_value(client.set_membership(
            &MembershipRequest { space_id: a[0].clone(), principal_id: a[1].clone(),
                can_read: boolean(&a[2])?, can_append: boolean(&a[3])?, can_admin: boolean(&a[4])? })
            .map_err(|_| "membership update failed")?),
        "adapter-provision" if a.len() == 2 => serde_json::to_value(client.provision_adapter(
            &AdapterProvisionRequest { principal_id: a[0].clone(), adapter_id: a[1].clone() })
            .map_err(|_| "adapter provisioning failed")?),
        "ticket-create" if a.len() == 4 => {
            private_file::check_destination(Path::new(&a[3]))
                .map_err(|_| "invalid private ticket destination")?;
            let response = client.create_ticket(&EnrollmentTicketCreateRequest {
                principal_id: a[0].clone(), adapter_id: a[1].clone(),
                ttl_seconds: a[2].parse().map_err(|_| "invalid ticket TTL")?,
            }).map_err(|_| "ticket creation failed")?;
            private_file::write(Path::new(&a[3]), response.enrollment_ticket.ticket.as_bytes())
                .map_err(|_| {
                    let _ = writeln!(error, "aj-admin: event=ticket_write_failed server_outcome=committed request_id={} recovery=wait-ticket-expiration",
                        client.last_request_id().as_deref().unwrap_or("unavailable"));
                    "ticket file write failed; do not use partial output; the unexchanged ticket expires at its configured TTL"
                })?;
            return Ok(());
        },
        "credential-rotate" if a.len() == 2 || a.len() == 3 => {
            private_file::check_destination(Path::new(&a[1]))
                .map_err(|_| "invalid private replacement destination")?;
            let response = client.rotate(&CredentialRotateRequest {
                credential_id: a[0].clone(), reason: a.get(2).cloned(),
            }).map_err(|_| {
                let _ = writeln!(error, "aj-admin: event=rotation_response_failed server_outcome=unknown request_id={} recovery=enrollment-recover",
                    client.last_request_id().as_deref().unwrap_or("unavailable"));
                "rotation failed or response lost; use enrollment-recover with the bound adapter and installation before fresh enrollment"
            })?;
            let encoded = serde_json::to_vec(&response.replacement_secret).map_err(|_| "cannot encode credential")?;
            private_file::write(Path::new(&a[1]), &encoded)
                .map_err(|_| {
                    let _ = writeln!(error, "aj-admin: event=credential_write_failed operation=rotation server_outcome=committed request_id={} recovery=enrollment-recover",
                        client.last_request_id().as_deref().unwrap_or("unavailable"));
                    "replacement credential file write failed; use enrollment-recover with the bound adapter and installation to revoke both credential lineages before fresh enrollment"
                })?;
            return Ok(());
        },
        "credential-revoke" if a.len() == 1 || a.len() == 2 => {
            client.revoke(&CredentialRotateRequest {
                credential_id: a[0].clone(), reason: a.get(1).cloned(),
            }).map_err(|_| "credential revocation failed")?;
            return Ok(());
        },
        "enrollment-recover" if a.len() == 2 => {
            client.recover_enrollment(&journal_client::EnrollmentRecoveryRequest {
                adapter_id: a[0].clone(), instance_id: a[1].clone(),
            }).map_err(|_| "enrollment recovery failed; do not reuse the ticket or change installation identity")?;
            return Ok(());
        },
        _ => return Err("commands: metrics; principal-create ID NAME; space-create ID NAME; membership-set SPACE PRINCIPAL READ APPEND ADMIN; adapter-provision PRINCIPAL ADAPTER; adapter-replace ADAPTER GENERATION NEW_INSTANCE [REASON]; adapters [LIMIT [CURSOR]]; mailbox-status PRINCIPAL; mailbox-requeue ITEM [REASON]; ticket-create PRINCIPAL ADAPTER TTL OUTPUT; credential-rotate ID OUTPUT [REASON]; credential-revoke ID [REASON]; enrollment-recover ADAPTER INSTANCE"),
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
}
