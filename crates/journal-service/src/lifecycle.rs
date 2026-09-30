//! Protected desired-state mutations share the security snapshot transaction.
use super::*;

impl BootstrapService {
    pub fn set_principal_state(
        &self,
        request: &PrincipalStateRequest,
    ) -> Result<PrincipalStateResponse, BootstrapError> {
        request.validate()?;
        self.transaction(|tx| {
            // Never use active_principal: disabled subjects must be selectable,
            // and even a UUID-looking name is not an ownership binding.
            let previous: Option<Option<String>> = tx.query_row(
                "SELECT disabled_at FROM principals WHERE id=?",
                [&request.principal_id], |row| row.get(0),
            ).optional()?;
            let previous = previous.ok_or(BootstrapError::NotFound)?;
            let disabled_at = if previous.is_some() == request.disabled {
                previous
            } else {
                let now = self.now()?;
                let next = request.disabled.then_some(now.clone());
                tx.execute("UPDATE principals SET disabled_at=? WHERE id=?",
                    params![next, request.principal_id])?;
                self.checkpoint("principal-state-updated")?;
                if request.disabled {
                    // Include unrevoked expired bindings too. Nothing issued
                    // before suspension can become usable on re-enable.
                    let reason = request.reason.as_deref().unwrap_or("principal disabled");
                    tx.execute(
                        "INSERT INTO credential_audit(credential_id,operation,occurred_at,reason)
                         SELECT id,'revoked',?,? FROM credentials
                         WHERE principal_id=? AND revoked_at IS NULL",
                        params![now, reason, request.principal_id],
                    )?;
                    tx.execute(
                        "UPDATE credentials SET revoked_at=?,revocation_reason=?
                         WHERE principal_id=? AND revoked_at IS NULL",
                        params![now, reason, request.principal_id],
                    )?;
                }
                self.checkpoint("principal-state-revoked")?;
                tx.execute(
                    "INSERT INTO audit_events(id,event_type,subject_type,subject_id,detail_json,created_at)
                     VALUES (?,'principal-state-changed','principal',?,?,?)",
                    params![format!("lifecycle-{}",self.secret()?),request.principal_id,
                        serde_json::json!({"disabled":request.disabled,"reason":request.reason}).to_string(),now],
                )?;
                self.checkpoint("principal-state-audited")?;
                next
            };
            Ok(PrincipalStateResponse {
                principal: principal_descriptor(tx, &request.principal_id)?,
                disabled_at,
            })
        })
    }

    pub fn set_space_archive(
        &self,
        request: &SpaceArchiveRequest,
    ) -> Result<Space, BootstrapError> {
        request.validate()?;
        self.transaction(|tx| {
            let previous: Option<Option<String>> = tx.query_row(
                "SELECT archived_at FROM spaces WHERE id=?",
                [&request.space_id], |row| row.get(0),
            ).optional()?;
            let previous = previous.ok_or(BootstrapError::NotFound)?;
            if previous.is_some() != request.archived {
                let now = self.now()?;
                tx.execute("UPDATE spaces SET archived_at=? WHERE id=?",
                    params![request.archived.then_some(&now),request.space_id])?;
                self.checkpoint("space-archive-updated")?;
                tx.execute(
                    "INSERT INTO audit_events(id,event_type,subject_type,subject_id,detail_json,created_at)
                     VALUES (?,'space-archive-changed','space',?,?,?)",
                    params![format!("lifecycle-{}",self.secret()?),request.space_id,
                        serde_json::json!({"archived":request.archived,"reason":request.reason}).to_string(),now],
                )?;
                self.checkpoint("space-archive-audited")?;
            }
            records::space(tx, &request.space_id)
        })
    }
}
