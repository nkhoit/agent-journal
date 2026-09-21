use super::*;

const REGISTRATION_SECONDS: u64 = 60;
const CLAIM_SECONDS: u64 = 30;

impl BootstrapService {
    pub(super) fn delivery_actor(
        &self,
        tx: &Transaction<'_>,
        token: &str,
        now: &str,
    ) -> Result<AuthenticatedCredential, BootstrapError> {
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(BootstrapError::Unauthorized);
        }
        tx.query_row(
            "SELECT c.id,c.principal_id,c.adapter_id,c.instance_id FROM credentials c
             JOIN principals p ON p.id=c.principal_id
             JOIN adapter_registrations r ON r.adapter_id=c.adapter_id AND r.principal_id=c.principal_id AND r.instance_id=c.instance_id
             WHERE c.token_hash=? AND c.class='delivery-adapter' AND c.revoked_at IS NULL
             AND p.disabled_at IS NULL AND r.status='active'
             AND (c.expires_at IS NULL OR julianday(c.expires_at)>julianday(?))",
            params![digest(token),now],
            |r| Ok(AuthenticatedCredential { credential_id:r.get(0)?, principal_id:r.get(1)?, adapter_id:r.get(2)?, instance_id:r.get(3)?, class:CredentialClass::DeliveryAdapter })
        ).optional()?.ok_or(BootstrapError::Unauthorized)
    }

    pub fn register_adapter(
        &self,
        token: &str,
        request: &AdapterRegisterRequest,
    ) -> Result<AdapterRegistration, BootstrapError> {
        request.validate()?;
        self.renew_adapter(token, &request.instance_id, None)
    }

    pub fn heartbeat_adapter(
        &self,
        token: &str,
        request: &AdapterHeartbeatRequest,
    ) -> Result<AdapterRegistration, BootstrapError> {
        request.validate()?;
        self.renew_adapter(token, &request.instance_id, Some(request.generation))
    }

    fn renew_adapter(
        &self,
        token: &str,
        instance: &str,
        generation: Option<i64>,
    ) -> Result<AdapterRegistration, BootstrapError> {
        self.transaction(|tx| {
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let actor = self.delivery_actor(tx, token, &now)?;
            let adapter = actor.adapter_id.as_deref().ok_or(BootstrapError::Unauthorized)?;
            let current = registration(tx, adapter)?;
            if actor.instance_id.as_deref() != Some(instance)
                || generation.is_some_and(|g| g != current.generation)
                || (generation.is_some() && expired(&current.lease_expires_at, instant)?) {
                return Err(BootstrapError::Conflict);
            }
            close_claims(tx, adapter, &now, false)?;
            let lease = deadline(instant, REGISTRATION_SECONDS)?;
            tx.execute("UPDATE adapter_registrations SET last_heartbeat_at=?,lease_expires_at=? WHERE adapter_id=?", params![now,lease,adapter])?;
            self.checkpoint("adapter-renewed")?;
            registration(tx, adapter)
        })
    }

    pub fn replace_adapter(
        &self,
        adapter: &str,
        request: &AdapterReplaceRequest,
    ) -> Result<AdapterRegistration, BootstrapError> {
        AdapterPath {
            adapter_id: adapter.into(),
        }
        .validate()?;
        request.validate()?;
        self.transaction(|tx| {
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let current = registration(tx, adapter)?;
            if current.generation != request.expected_generation || current.instance_id == request.new_instance_id {
                return Err(BootstrapError::Conflict);
            }
            let generation = current.generation.checked_add(1).ok_or(BootstrapError::Conflict)?;
            close_claims(tx, adapter, &now, true)?;
            // Replacement transfers installation ownership, never the old secrets.
            tx.execute("INSERT INTO credential_audit(credential_id,operation,occurred_at,reason)
                SELECT id,'revoked',?,? FROM credentials WHERE enrollment_adapter_id=? AND revoked_at IS NULL",
                params![now,request.reason,adapter])?;
            tx.execute("UPDATE credentials SET revoked_at=?,revocation_reason=? WHERE enrollment_adapter_id=? AND revoked_at IS NULL",
                params![now,request.reason,adapter])?;
            tx.execute("UPDATE enrollment_tickets SET invalidated_at=? WHERE adapter_id=? AND consumed_at IS NULL AND invalidated_at IS NULL", params![now,adapter])?;
            tx.execute("UPDATE enrollment_installations SET instance_id=?,recovery_authorized=1 WHERE adapter_id=?", params![request.new_instance_id,adapter])?;
            tx.execute("UPDATE adapter_registrations SET instance_id=?,generation=?,status='active',last_heartbeat_at=?,lease_expires_at=? WHERE adapter_id=?",
                params![request.new_instance_id,generation,now,deadline(instant,REGISTRATION_SECONDS)?,adapter])?;
            tx.execute("INSERT INTO audit_events(id,event_type,subject_type,subject_id,detail_json,created_at) VALUES (?,'adapter-replaced','adapter',?,?,?)",
                params![format!("audit-{}",self.secret()?),adapter,serde_json::to_string(request).map_err(|_|BootstrapError::InvalidJournal)?,now])?;
            self.checkpoint("adapter-replaced")?;
            registration(tx, adapter)
        })
    }

    /// An empty selection returns None without creating a claim, so async callers
    /// can wait without holding either the transaction or their worker permit.
    pub fn try_claim_mailbox(
        &self,
        token: &str,
        request: &ClaimRequest,
    ) -> Result<Option<ClaimResponse>, BootstrapError> {
        self.claim_selection(token, request, false)
    }

    pub fn claim_mailbox(
        &self,
        token: &str,
        request: &ClaimRequest,
    ) -> Result<ClaimResponse, BootstrapError> {
        self.claim_selection(token, request, true)?
            .ok_or(BootstrapError::CorruptJournal)
    }

    fn claim_selection(
        &self,
        token: &str,
        request: &ClaimRequest,
        finish_empty: bool,
    ) -> Result<Option<ClaimResponse>, BootstrapError> {
        request.validate()?;
        self.transaction(|tx| {
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let actor = self.delivery_actor(tx, token, &now)?;
            let adapter = actor.adapter_id.as_deref().ok_or(BootstrapError::Unauthorized)?;
            let current = registration(tx, adapter)?;
            if actor.instance_id.as_deref() != Some(&request.instance_id)
                || current.generation != request.generation || expired(&current.lease_expires_at, instant)? {
                return Err(BootstrapError::Conflict);
            }
            close_claims(tx, adapter, &now, false)?;
            suppress_revoked(tx, &actor.principal_id, &now)?;
            let active: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM claims WHERE adapter_id=? AND generation=? AND state='active')", params![adapter,request.generation], |r|r.get(0))?;
            if active { return Err(BootstrapError::Conflict); }
            let selected = tx.prepare(
                "SELECT m.id,a.attempt_id,m.record_id FROM mailbox_items m
                 JOIN delivery_attempts a ON a.mailbox_item_id=m.id AND a.state='pending'
                 JOIN records r ON r.id=m.record_id
                 JOIN spaces s ON s.id=r.space_id AND s.access='public'
                 WHERE m.recipient_principal_id=? AND m.state='pending'
                 ORDER BY m.created_at,m.id LIMIT ?"
            )?.query_map(params![actor.principal_id,request.limit as i64], |r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            if selected.is_empty() && !finish_empty { return Ok(None); }
            let claim_id = format!("claim-{}",self.secret()?);
            let lease = deadline(instant, CLAIM_SECONDS)?;
            let lease = if lease < current.lease_expires_at { lease } else { current.lease_expires_at };
            let empty = selected.is_empty();
            tx.execute("INSERT INTO claims(id,adapter_id,principal_id,instance_id,generation,state,lease_expires_at,closed_at,created_at,credential_id)
                VALUES (?,?,?,?,?,?,?,?,?,?)",
                params![claim_id,adapter,actor.principal_id,request.instance_id,request.generation,if empty {"committed"} else {"active"},lease,empty.then_some(&now),now,actor.credential_id])?;
            self.checkpoint("claim-created")?;
            let mut items = Vec::with_capacity(selected.len());
            for (mailbox_item_id, attempt_id, record_id) in selected {
                tx.execute("UPDATE mailbox_items SET state='claimed',updated_at=? WHERE id=?",params![now,mailbox_item_id])?;
                tx.execute("UPDATE delivery_attempts SET state='claimed',updated_at=? WHERE attempt_id=?",params![now,attempt_id])?;
                self.checkpoint("claim-state")?;
                tx.execute("INSERT INTO claim_items(claim_id,mailbox_item_id,attempt_id) VALUES (?,?,?)",params![claim_id,mailbox_item_id,attempt_id])?;
                items.push(ClaimItem { mailbox_item_id, attempt_id, record:records::record(tx,&record_id)? });
            }
            self.checkpoint("claim-items")?;
            Ok(Some(ClaimResponse { claim_id, state:if empty {ClaimState::Committed} else {ClaimState::Active}, lease_expires_at:lease, items }))
        })
    }

    pub fn mailbox_status(
        &self,
        token: &str,
        query: &PageQuery,
    ) -> Result<MailboxStatusPage, BootstrapError> {
        query.validate()?;
        self.transaction(|tx| {
            let now = self.now()?;
            let actor = self.delivery_actor(tx, token, &now)?;
            self.status_page(tx, &actor.principal_id, query, false, &now)
        })
    }

    pub fn admin_mailbox_status(
        &self,
        principal: &str,
        query: &PageQuery,
    ) -> Result<MailboxStatusPage, BootstrapError> {
        journal_domain::validate_identifier("principal", principal)
            .map_err(|_| BootstrapError::InvalidJournal)?;
        query.validate()?;
        self.transaction(|tx| {
            let principal = active_principal(tx, principal)?;
            self.status_page(tx, &principal, query, true, &self.now()?)
        })
    }

    fn status_page(
        &self,
        tx: &Transaction<'_>,
        principal: &str,
        query: &PageQuery,
        admin: bool,
        now: &str,
    ) -> Result<MailboxStatusPage, BootstrapError> {
        if let Some(adapter) = tx
            .query_row(
                "SELECT adapter_id FROM adapter_registrations WHERE principal_id=?",
                [principal],
                |r| r.get::<_, String>(0),
            )
            .optional()?
        {
            close_claims(tx, &adapter, now, false)?;
        }
        suppress_revoked(tx, principal, now)?;
        let scope = CursorScope::new(
            if admin {
                CursorRoute::AdminMailboxStatus
            } else {
                CursorRoute::MailboxStatus
            },
            principal.as_bytes(),
            CursorOrder::Identifier,
        );
        if let Some(cursor) = query.cursor.as_deref().filter(|c| !c.is_empty()) {
            self.cursor_codec(tx)?
                .decode(&scope, cursor)
                .map_err(|_| BootstrapError::InvalidJournal)?;
            return Ok(Page {
                items: vec![],
                next_cursor: None,
            });
        }
        let status = tx.query_row("SELECT count(*),min(created_at) FROM mailbox_items WHERE recipient_principal_id=? AND state='pending'",[principal], |r|Ok(MailboxStatus { principal_id:principal.into(),pending:r.get::<_,i64>(0)? as u64,oldest_pending_at:r.get(1)?,paused:None }))?;
        Ok(Page {
            items: vec![status],
            next_cursor: None,
        })
    }
}

fn deadline(instant: SystemTime, seconds: u64) -> Result<String, BootstrapError> {
    timestamp(
        instant
            .checked_add(Duration::from_secs(seconds))
            .ok_or(BootstrapError::Clock)?,
    )
}
pub(super) fn expired(value: &str, instant: SystemTime) -> Result<bool, BootstrapError> {
    let value: jiff::Timestamp = value.parse().map_err(|_| BootstrapError::CorruptJournal)?;
    let now = instant
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| BootstrapError::Clock)?;
    Ok(value.as_nanosecond() <= now.as_nanos() as i128)
}
pub(super) fn registration(
    tx: &Transaction<'_>,
    adapter: &str,
) -> Result<AdapterRegistration, BootstrapError> {
    tx.query_row("SELECT adapter_id,principal_id,instance_id,generation,status,lease_expires_at FROM adapter_registrations WHERE adapter_id=?", [adapter],
        |r| Ok(AdapterRegistration { adapter_id:r.get(0)?,principal_id:r.get(1)?,instance_id:r.get(2)?,generation:r.get(3)?,status:match r.get::<_,String>(4)?.as_str() {"active"=>AdapterStatus::Active,"draining"=>AdapterStatus::Draining,_=>AdapterStatus::Revoked},lease_expires_at:r.get(5)?,heartbeat_after_seconds:20 }))
        .optional()?.ok_or(BootstrapError::NotFound)
}

pub(super) fn close_claims(
    tx: &Transaction<'_>,
    adapter: &str,
    now: &str,
    cancel: bool,
) -> Result<(), BootstrapError> {
    tx.execute("UPDATE delivery_attempts SET state='pending',updated_at=? WHERE state='claimed' AND attempt_id IN (
        SELECT i.attempt_id FROM claim_items i JOIN claims c ON c.id=i.claim_id
        WHERE c.adapter_id=? AND c.state='active' AND (? OR julianday(c.lease_expires_at)<=julianday(?)))",params![now,adapter,cancel,now])?;
    tx.execute("UPDATE mailbox_items SET state='pending',updated_at=? WHERE state='claimed'
        AND id IN (SELECT i.mailbox_item_id FROM claim_items i JOIN claims c ON c.id=i.claim_id
        WHERE c.adapter_id=? AND c.state='active' AND (? OR julianday(c.lease_expires_at)<=julianday(?)))",params![now,adapter,cancel,now])?;
    tx.execute(
        "UPDATE claims SET state=CASE WHEN ? THEN 'cancelled' ELSE 'expired' END,closed_at=?
        WHERE adapter_id=? AND state='active' AND (? OR julianday(lease_expires_at)<=julianday(?))",
        params![cancel, now, adapter, cancel, now],
    )?;
    Ok(())
}

pub(super) fn suppress_revoked(
    tx: &Transaction<'_>,
    principal: &str,
    now: &str,
) -> Result<(), BootstrapError> {
    tx.execute(
        "UPDATE mailbox_items SET state='suppressed-revoked',updated_at=?
        WHERE recipient_principal_id=? AND state IN ('pending','claimed')
        AND NOT EXISTS(SELECT 1 FROM records r JOIN spaces s ON s.id=r.space_id AND s.access='public'
            JOIN principals p ON p.id=? AND p.disabled_at IS NULL
            WHERE r.id=mailbox_items.record_id)",
        params![now, principal, principal],
    )?;
    tx.execute("UPDATE delivery_attempts SET state='suppressed-revoked',updated_at=?
        WHERE state IN ('pending','claimed') AND mailbox_item_id IN
        (SELECT id FROM mailbox_items WHERE recipient_principal_id=? AND state='suppressed-revoked')",params![now,principal])?;
    Ok(())
}
