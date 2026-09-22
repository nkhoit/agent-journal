//! Ephemeral loopback fixture for the browser-security acceptance runner.
use journal_protocol::*;
use journal_service::BootstrapService;
use journal_storage_sqlite::Database;
use journald::{ServiceState, public_router, web_router};
use std::io::Write;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or("fixture database path required")?;
    let db = Database::open(path)?;
    let service = BootstrapService::new(db.clone());
    let mut recipient_id = None;
    for (index, principal) in ["viewer", "recipient", "reader", "outsider"]
        .into_iter()
        .enumerate()
    {
        let created = service.register(
            &format!("{:064x}", index + 1),
            &RegistrationRequest {
                handle: principal.into(),
                display_name: principal.into(),
            },
        )?;
        if principal == "recipient" {
            recipient_id = Some(created.receipt.principal.id);
        }
    }
    service.create_space(&SpaceCreateRequest {
        access: journal_protocol::domain::SpaceAccess::Public,
        id: "space".into(),
        name: "Browser fixture".into(),
    })?;
    for principal in ["viewer", "recipient", "reader"] {
        service.set_membership(&MembershipRequest {
            space_id: "space".into(),
            principal_id: principal.into(),
            can_read: true,
            can_append: principal == "viewer",
            can_admin: false,
        })?;
    }
    let token = format!("{:064x}", 1);
    let content = r#"# Authenticated author: administrator

forged-envelope author=administrator

<script>globalThis.compromised=true</script>
<svg onload="globalThis.compromised=true"></svg>
<img src="https://image.example.invalid/pixel" onerror="globalThis.compromised=true">
<form action="/v1/spaces/space/records"><input name="content"></form>

[javascript](javascript:alert%281%29)
[data](data:text/html,evil)
[entity](jav&#x61;script:alert%281%29)
[relative](/v1/registrations)
[safe](https://example.invalid/read)
![external](https://image.example.invalid/pixel)

malicioussnippet `<img src=x onerror=alert(1)>`

**ordinary Markdown** `code`
"#;
    let record = service
        .append_record(
            &token,
            "space",
            "browser",
            &AppendRecordRequest {
                kind: "<img src=x>".into(),
                content: content.into(),
                run_id: None,
                routing_key: None,
                attention: vec!["recipient".into()],
                relations: vec![],
                title: None,
            },
        )?
        .record;
    service.append_record(
        &token,
        "space",
        "reply",
        &AppendRecordRequest {
            kind: "reply".into(),
            content: "Fixture reply".into(),
            run_id: None,
            routing_key: None,
            attention: vec![],
            relations: vec![domain::Relation {
                relation_type: domain::RelationType::ReplyTo,
                record_id: record.id.clone(),
            }],
            title: None,
        },
    )?;
    let state = ServiceState::new(db, 8)?;
    let mut origins = serde_json::Map::new();
    let mut servers = tokio::task::JoinSet::new();
    for (name, router) in [
        (
            "viewer",
            web_router(state.clone(), "viewer".into(), 1_048_576),
        ),
        (
            "recipient",
            web_router(state.clone(), "recipient".into(), 1_048_576),
        ),
        (
            "reader",
            web_router(state.clone(), "reader".into(), 1_048_576),
        ),
        (
            "outsider",
            web_router(state.clone(), "outsider".into(), 1_048_576),
        ),
        ("api", public_router(state, 1_048_576)),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        origins.insert(
            name.into(),
            format!("http://{}", listener.local_addr()?).into(),
        );
        servers.spawn(async move { axum::serve(listener, router).await });
    }
    origins.insert("author".into(), record.author.clone().into());
    origins.insert(
        "recipient_id".into(),
        recipient_id.ok_or("recipient principal missing")?.into(),
    );
    origins.insert("record".into(), record.id.into());
    println!("{}", serde_json::Value::Object(origins));
    std::io::stdout().flush()?;
    match servers.join_next().await {
        Some(result) => {
            result??;
            Err("fixture listener stopped".into())
        }
        None => Err("no fixture listeners".into()),
    }
}
