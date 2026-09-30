#![cfg(unix)]

use journal_protocol::*;
use journal_service::{BootstrapError, BootstrapService};
use journal_storage_sqlite::Database;
use std::{os::unix::fs::PermissionsExt, path::PathBuf};

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "principal-restore-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn security_history(database: &Database) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    let connection = database.connect_read_only().unwrap();
    [("credential_audit", 5), ("audit_events", 7)]
        .into_iter()
        .map(|(table, columns)| {
            connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY id"))
                .unwrap()
                .query_map([], |row| (0..columns).map(|index| row.get(index)).collect())
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        })
        .collect()
}

#[test]
fn older_backup_retains_revoked_post_backup_registration_and_rotation_bindings() {
    let directory = Directory::new();
    let central = directory.0.join("central.db");
    let audit_path = directory.0.join("audit.db");
    let backup = directory.0.join("backup.db");
    let restored = directory.0.join("restored.db");
    let database = Database::open_protected(&central, &audit_path).unwrap();
    let service = BootstrapService::new(database.clone());
    let alpha = "a".repeat(64);
    let beta = "b".repeat(64);
    let request = |handle: &str| RegistrationRequest {
        handle: handle.into(),
        display_name: handle.into(),
    };
    service.register(&alpha, &request("alpha")).unwrap();
    database
        .recovery_audit()
        .unwrap()
        .backup(&database, &backup)
        .unwrap();
    let alpha_actor = service
        .authenticate(&alpha, CredentialClass::PrincipalClient)
        .unwrap();
    service
        .update_own_profile(
            &alpha_actor,
            "rename",
            &ProfileUpdateRequest {
                handle: "zeta".into(),
                display_name: "Updated Alpha".into(),
                description: None,
                expected_profile_revision: 1,
            },
        )
        .unwrap();
    let registration = service.register(&beta, &request("beta")).unwrap().receipt;
    let rotation = service
        .rotate(&CredentialRotateRequest {
            credential_id: registration.credential_id.clone(),
            reason: None,
        })
        .unwrap();
    let recovered = service
        .recover_principal(&PrincipalRecoveryRequest {
            principal_id: registration.principal.id.clone(),
            reason: None,
        })
        .unwrap();
    service
        .revoke(
            &recovered.replacement_secret.credential_id,
            Some("post-backup revocation"),
        )
        .unwrap();
    let expected_history = security_history(&database);
    assert!(expected_history.iter().all(|rows| !rows.is_empty()));
    let last_history_id: i64 = database
        .connect_read_only()
        .unwrap()
        .query_row("SELECT max(id) FROM credential_audit", [], |row| row.get(0))
        .unwrap();
    let expected_credentials: i64 = database
        .connect_read_only()
        .unwrap()
        .query_row("SELECT count(*) FROM credentials", [], |r| r.get(0))
        .unwrap();
    let audit = database.recovery_audit().unwrap();
    let mut approval = audit.restore(&backup, &restored, true).unwrap();
    let restored_database = Database::open(&restored).unwrap();
    assert_eq!(security_history(&restored_database), expected_history);
    approval.inventory_complete = true;
    approval.accepted_record_loss = true;
    audit.reopen(&restored_database, &approval).unwrap();
    assert_eq!(security_history(&restored_database), expected_history);
    drop(restored_database);
    drop(service);
    drop(database);

    let database = Database::open_protected(&restored, &audit_path).unwrap();
    assert_eq!(security_history(&database), expected_history);
    let service = BootstrapService::new(database.clone());
    for token in [
        &alpha,
        &beta,
        &rotation.replacement_secret.secret,
        &recovered.replacement_secret.secret,
    ] {
        assert!(matches!(
            service.authenticate(token, CredentialClass::PrincipalClient),
            Err(BootstrapError::Unauthorized)
        ));
        assert!(matches!(
            service.register(token, &request("unused")),
            Err(BootstrapError::Unauthorized)
        ));
    }
    let connection = database.connect_read_only().unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM credentials", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        expected_credentials
    );
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM principals", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
    let retained: String = connection
        .query_row(
            "SELECT response_json FROM registration_receipts WHERE principal_id=?",
            [&registration.principal.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        decode_json::<RegistrationReceipt>(retained.as_bytes()).unwrap(),
        registration
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT principal_id FROM principal_names WHERE name='beta'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
        registration.principal.id
    );
    assert_eq!(connection.query_row(
        "SELECT n.name,p.display_name FROM principal_names n JOIN principals p ON p.id=n.principal_id
         WHERE p.id=? AND n.kind='current'", [&alpha_actor.principal_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    ).unwrap(), ("zeta".into(), "Updated Alpha".into()));
    assert_eq!(
        connection
            .query_row(
                "SELECT kind FROM principal_names WHERE name='alpha'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "alias"
    );
    service
        .recover_principal(&PrincipalRecoveryRequest {
            principal_id: registration.principal.id,
            reason: Some("after protected restart".into()),
        })
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT max(id) FROM credential_audit", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        last_history_id + 1
    );
}

#[test]
fn conflicting_credential_or_receipt_binding_refuses_destination_publication() {
    for conflict in ["credential", "receipt"] {
        let directory = Directory::new();
        let backup = directory.0.join("backup.db");
        let destination = directory.0.join("restored.db");
        let audit_path = directory.0.join("audit.db");
        let database =
            Database::open_protected(directory.0.join("central.db"), &audit_path).unwrap();
        let service = BootstrapService::new(database.clone());
        let registration = service
            .register(
                &"a".repeat(64),
                &RegistrationRequest {
                    handle: "alpha".into(),
                    display_name: "Alpha".into(),
                },
            )
            .unwrap();
        let rotated = service
            .rotate(&CredentialRotateRequest {
                credential_id: registration.receipt.credential_id,
                reason: None,
            })
            .unwrap();
        let audit = database.recovery_audit().unwrap();
        audit.backup(&database, &backup).unwrap();
        let connection = rusqlite::Connection::open(&backup).unwrap();
        if conflict == "credential" {
            connection
                .execute(
                    "UPDATE credentials SET token_hash=? WHERE id=?",
                    rusqlite::params!["f".repeat(64), rotated.replacement_secret.credential_id],
                )
                .unwrap();
        } else {
            let trigger: String = connection.query_row(
                "SELECT sql FROM sqlite_schema WHERE name='registration_receipts_are_immutable'",
                [], |row| row.get(0),
            ).unwrap();
            connection
                .execute_batch("DROP TRIGGER registration_receipts_are_immutable")
                .unwrap();
            connection
                .execute("UPDATE registration_receipts SET response_json='{}'", [])
                .unwrap();
            connection.execute_batch(&trigger).unwrap();
        }
        drop(connection);
        let head = || {
            rusqlite::Connection::open(&audit_path)
                .unwrap()
                .query_row("SELECT max(revision) FROM events", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        };
        let before = head();
        assert!(audit.restore(&backup, &destination, true).is_err());
        assert!(!destination.exists());
        assert_eq!(head(), before);
        assert!(audit.ensure_open(&database).is_err());
    }
}
