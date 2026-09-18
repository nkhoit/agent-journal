use super::*;
use delivery::{close_claims, expired, registration, suppress_revoked};
use records::{finish_page, identifier_position};

impl BootstrapService {
    pub fn commit_custody(
        &self,
        token: &str,
        claim: &str,
        request: &CommitRequest,
    ) -> Result<CommitResponse, BootstrapError> {
        ClaimPath {
            claim_id: claim.into(),
        }
        .validate()?;
        request.validate()?;
        self.transaction(|tx| {
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let actor = self.delivery_actor(tx, token, &now)?;
            let adapter = actor.adapter_id.as_deref().ok_or(BootstrapError::Unauthorized)?;
            let current = registration(tx, adapter)?;
            close_claims(tx, adapter, &now, false)?;
            suppress_revoked(tx, &actor.principal_id, &now)?;
            let binding = tx.query_row(
                "SELECT state,lease_expires_at,generation FROM claims
                 WHERE id=? AND credential_id=? AND principal_id=? AND adapter_id=? AND instance_id=?",
                params![claim,actor.credential_id,actor.principal_id,adapter,actor.instance_id],
                |r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?))
            ).optional()?;
            let mut items = Vec::with_capacity(request.items.len());
            for item in &request.items {
                let result = if let Some((state, lease, generation)) = &binding {
                    if *generation != request.generation || current.generation != request.generation {
                        CommitItemResult::StaleGeneration
                    } else {
                        let exact: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM claim_items
                            WHERE claim_id=? AND mailbox_item_id=? AND attempt_id=?)",
                            params![claim,item.mailbox_item_id,item.attempt_id], |r|r.get(0))?;
                        let custody: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM host_custody
                            WHERE claim_id=? AND mailbox_item_id=? AND attempt_id=?)",
                            params![claim,item.mailbox_item_id,item.attempt_id], |r|r.get(0))?;
                        if !exact { CommitItemResult::AttemptMismatch }
                        else if custody { CommitItemResult::AlreadyCommitted }
                        else if state != "active" || expired(lease,instant)? || expired(&current.lease_expires_at,instant)? {
                            CommitItemResult::LeaseExpired
                        } else {
                            let attempt: String = tx.query_row("SELECT state FROM delivery_attempts WHERE attempt_id=?", [&item.attempt_id], |r|r.get(0))?;
                            if attempt == "suppressed-revoked" { CommitItemResult::SuppressedRevoked }
                            else if attempt != "claimed" { CommitItemResult::AttemptMismatch }
                            else {
                                tx.execute("INSERT INTO host_custody(attempt_id,mailbox_item_id,claim_id,committed_at) VALUES (?,?,?,?)",
                                    params![item.attempt_id,item.mailbox_item_id,claim,now])?;
                                self.checkpoint("custody-recorded")?;
                                tx.execute("UPDATE delivery_attempts SET state='host-accepted',updated_at=? WHERE attempt_id=?",params![now,item.attempt_id])?;
                                tx.execute("UPDATE mailbox_items SET state='host-accepted',updated_at=? WHERE id=?",params![now,item.mailbox_item_id])?;
                                self.checkpoint("custody-state")?;
                                CommitItemResult::Committed
                            }
                        }
                    }
                } else { CommitItemResult::ClaimNotFound };
                items.push(CommitItemResultEntry { mailbox_item_id:item.mailbox_item_id.clone(), attempt_id:item.attempt_id.clone(), result });
            }
            tx.execute("UPDATE claims SET state='committed',closed_at=? WHERE id=? AND state='active'
                AND NOT EXISTS(SELECT 1 FROM claim_items i WHERE i.claim_id=claims.id
                    AND NOT EXISTS(SELECT 1 FROM host_custody h WHERE h.claim_id=i.claim_id AND h.attempt_id=i.attempt_id))",
                params![now,claim])?;
            Ok(CommitResponse {claim_id:claim.into(),generation:request.generation,items})
        })
    }

    pub fn record_delivery_event(
        &self,
        token: &str,
        item: &str,
        request: &DeliveryEventRequest,
    ) -> Result<DeliveryEventResponse, BootstrapError> {
        MailboxItemPath {
            item_id: item.into(),
        }
        .validate()?;
        request.validate()?;
        self.transaction(|tx| {
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let actor = self.delivery_actor(tx,token,&now)?;
            let adapter = actor.adapter_id.as_deref().ok_or(BootstrapError::Unauthorized)?;
            let current = registration(tx,adapter)?;
            if current.generation != request.generation {
                return Err(BootstrapError::Conflict);
            }
            let owned: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM mailbox_items m JOIN delivery_attempts a ON a.mailbox_item_id=m.id
                WHERE m.id=? AND a.attempt_id=? AND m.recipient_principal_id=?)",
                params![item,request.attempt_id,actor.principal_id],|r|r.get(0))?;
            if !owned { return Err(BootstrapError::NotFound); }
            let state = serde_json::to_value(request.state).map_err(|_|BootstrapError::InvalidJournal)?;
            let state = state.as_str().ok_or(BootstrapError::InvalidJournal)?;
            let detail = serde_json::to_string(&request.detail).map_err(|_|BootstrapError::InvalidJournal)?;
            let existing = tx.query_row("SELECT mailbox_item_id,attempt_id,adapter_id,instance_id,generation,state,coalesce(detail_json,'{}'),occurred_at,received_at
                FROM delivery_events WHERE event_id=?", [&request.event_id], |r|Ok((
                    r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,
                    r.get::<_,i64>(4)?,r.get::<_,String>(5)?,r.get::<_,String>(6)?,r.get::<_,String>(7)?,r.get::<_,String>(8)?
                ))).optional()?;
            if let Some((old_item,attempt,old_adapter,instance,generation,old_state,old_detail,occurred,received))=existing {
                let old_detail: journal_domain::TelemetryDetail = serde_json::from_str(&old_detail).map_err(|_|BootstrapError::CorruptJournal)?;
                if old_item!=item || attempt!=request.attempt_id || old_adapter!=adapter
                    || Some(instance.as_str())!=actor.instance_id.as_deref() || generation!=request.generation
                    || old_state!=state || old_detail!=request.detail || occurred!=request.occurred_at {
                    return Err(BootstrapError::Conflict);
                }
                return Ok(DeliveryEventResponse {event_id:request.event_id.clone(),state:request.state,received_at:received});
            }
            if expired(&current.lease_expires_at,instant)? { return Err(BootstrapError::Conflict); }
            // The SQL trigger checks exact custody and the allowed transition,
            // and advances only the latest mailbox projection, never a requeue.
            tx.execute("INSERT INTO delivery_events(event_id,mailbox_item_id,attempt_id,adapter_id,instance_id,generation,state,detail_json,occurred_at,received_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)",params![request.event_id,item,request.attempt_id,adapter,actor.instance_id,request.generation,state,detail,request.occurred_at,now])?;
            self.checkpoint("delivery-event")?;
            Ok(DeliveryEventResponse {event_id:request.event_id.clone(),state:request.state,received_at:now})
        })
    }

    pub fn requeue_mailbox_item(
        &self,
        item: &str,
        request: &RequeueRequest,
    ) -> Result<RequeueResponse, BootstrapError> {
        MailboxItemPath {
            item_id: item.into(),
        }
        .validate()?;
        request.validate()?;
        self.transaction(|tx| {
            let now = self.now()?;
            let (principal, state, space): (String,String,String) = tx.query_row(
                "SELECT m.recipient_principal_id,m.state,r.space_id FROM mailbox_items m JOIN records r ON r.id=m.record_id WHERE m.id=?",
                [item],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?.ok_or(BootstrapError::NotFound)?;
            if state=="pending" || state=="claimed" { return Err(BootstrapError::Conflict); }
            active_principal(tx,&principal)?;
            let readable: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM memberships WHERE principal_id=? AND space_id=? AND can_read=1)",
                params![principal,space],|r|r.get(0))?;
            if !readable { return Err(BootstrapError::Conflict); }
            let ordinal: i64=tx.query_row("SELECT max(ordinal) FROM delivery_attempts WHERE mailbox_item_id=?",[item],|r|r.get(0))?;
            let ordinal=ordinal.checked_add(1).ok_or(BootstrapError::Conflict)?;
            self.checkpoint("requeue-ordinal")?;
            let attempt_id=format!("attempt-{}",self.secret()?);
            tx.execute("INSERT INTO delivery_attempts(attempt_id,mailbox_item_id,ordinal,state,created_at,updated_at) VALUES (?,?,?,'pending',?,?)",
                params![attempt_id,item,ordinal,now,now])?;
            self.checkpoint("requeue-attempt")?;
            tx.execute("UPDATE mailbox_items SET state='pending',updated_at=? WHERE id=?",params![now,item])?;
            tx.execute("INSERT INTO audit_events(id,event_type,subject_type,subject_id,detail_json,created_at) VALUES (?,'mailbox-requeued','mailbox-item',?,?,?)",
                params![format!("audit-{}",self.secret()?),item,serde_json::json!({"attempt_id":attempt_id,"ordinal":ordinal,"reason":request.reason}).to_string(),now])?;
            self.checkpoint("requeue-state")?;
            Ok(RequeueResponse {mailbox_item_id:item.into(),attempt_id,state:RequeueState::Pending})
        })
    }

    pub fn list_adapters(&self, query: &PageQuery) -> Result<AdapterPage, BootstrapError> {
        query.validate()?;
        self.transaction(|tx| {
            let scope=CursorScope::new(CursorRoute::Adapters,b"admin",CursorOrder::Identifier);
            let codec=self.cursor_codec(tx)?;
            let after=identifier_position(&codec,&scope,query)?;
            let rows=tx.prepare("SELECT adapter_id,created_at FROM adapter_registrations WHERE adapter_id>? ORDER BY adapter_id LIMIT ?")?
                .query_map(params![after,(query.effective_limit()+1) as i64],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items=rows.into_iter().map(|(id,created_at)|Ok(Adapter {registration:registration(tx,&id)?,created_at:Some(created_at)}))
                .collect::<Result<Vec<_>,BootstrapError>>()?;
            finish_page(items,query,&codec,&scope,|a|CursorPosition::Identifier {id:a.registration.adapter_id.clone()})
        })
    }

    pub fn delivery_status(
        &self,
        token: &str,
        record: &str,
        query: &PageQuery,
    ) -> Result<DeliveryStatusPage, BootstrapError> {
        self.delivery_status_as(ReadIdentity::Bearer(token), record, query)
    }

    pub(super) fn delivery_status_as(
        &self,
        identity: ReadIdentity<'_>,
        record: &str,
        query: &PageQuery,
    ) -> Result<DeliveryStatusPage, BootstrapError> {
        RecordPath {
            record_id: record.into(),
        }
        .validate()?;
        query.validate()?;
        self.transaction(|tx| {
            let actor=self.read_actor(tx,identity)?;
            let author: String=tx.query_row("SELECT r.author_principal_id FROM records r
                JOIN memberships p ON p.space_id=r.space_id AND p.principal_id=? AND p.can_read=1
                WHERE r.id=? AND (r.author_principal_id=? OR EXISTS(
                    SELECT 1 FROM mailbox_items m WHERE m.record_id=r.id AND m.recipient_principal_id=?))",
                params![actor,record,actor,actor],|r|r.get(0)).optional()?.ok_or(BootstrapError::NotFound)?;
            let now=self.now()?;
            let recipients=tx.prepare("SELECT recipient_principal_id FROM mailbox_items WHERE record_id=? AND (? OR recipient_principal_id=?)")?
                .query_map(params![record,actor==author,actor],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for recipient in recipients {
                let adapter=tx.query_row("SELECT adapter_id FROM adapter_registrations WHERE principal_id=?",[&recipient],|r|r.get::<_,String>(0)).optional()?;
                if let Some(adapter)=adapter { close_claims(tx,&adapter,&now,false)?; }
                suppress_revoked(tx,&recipient,&now)?;
            }
            let filters=serde_json::to_vec(&(&actor,record)).map_err(|_|BootstrapError::InvalidJournal)?;
            let scope=CursorScope::new(CursorRoute::RecordDeliveryStatus,&filters,CursorOrder::Identifier);
            let codec=self.cursor_codec(tx)?;
            let after=identifier_position(&codec,&scope,query)?;
            let rows=tx.prepare("SELECT m.id,m.recipient_principal_id,m.state,a.ordinal,a.attempt_id,m.updated_at
                FROM mailbox_items m JOIN delivery_attempts a ON a.mailbox_item_id=m.id
                AND a.ordinal=(SELECT max(ordinal) FROM delivery_attempts WHERE mailbox_item_id=m.id)
                WHERE m.record_id=? AND (? OR m.recipient_principal_id=?) AND m.id>?
                ORDER BY m.id LIMIT ?")?.query_map(params![record,actor==author,actor,after,(query.effective_limit()+1) as i64],
                |r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i64>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items=rows.into_iter().map(|(mailbox_item_id,recipient,state,attempts,last,updated)| {
                Ok(DeliverySummary {mailbox_item_id,recipient,state:serde_json::from_value(serde_json::Value::String(state)).map_err(|_|BootstrapError::CorruptJournal)?,
                    attempts:attempts as u64,last_attempt_id:Some(last),updated_at:Some(updated)})
            }).collect::<Result<Vec<_>,BootstrapError>>()?;
            finish_page(items,query,&codec,&scope,|r|CursorPosition::Identifier {id:r.mailbox_item_id.clone()})
        })
    }
}
