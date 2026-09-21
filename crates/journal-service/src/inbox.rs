use super::*;
use records::{finish_page, identifier_position, permitted, record};

impl BootstrapService {
    pub fn inbox(&self, token: &str, query: &InboxQuery) -> Result<InboxPage, BootstrapError> {
        query.validate()?;
        let mut connection = self.database.connect_read_only()?;
        let tx = connection.transaction()?;
        let actor = self.journal_actor(&tx, token)?;
        let epoch: i64 =
            tx.query_row("SELECT inbox_epoch FROM recovery_anchor", [], |r| r.get(0))?;
        let filters = serde_json::to_vec(&(&actor, query.state, epoch))
            .map_err(|_| BootstrapError::InvalidJournal)?;
        let scope = CursorScope::new(CursorRoute::Inbox, &filters, CursorOrder::BoundedSequence);
        let secret: Option<Vec<u8>> = tx
            .query_row(
                "SELECT secret FROM journal_secrets WHERE name='cursor'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let Some(secret) = secret else {
            if query.page.cursor.as_deref().is_some_and(|s| !s.is_empty()) {
                return Err(BootstrapError::InvalidJournal);
            }
            // Appending initializes the key in the same transaction as the first item.
            let any: bool =
                tx.query_row("SELECT EXISTS(SELECT 1 FROM mailbox_items)", [], |r| {
                    r.get(0)
                })?;
            if any {
                return Err(BootstrapError::CorruptJournal);
            }
            return Ok(Page {
                items: vec![],
                next_cursor: None,
            });
        };
        let codec = CursorCodec::new(&secret).map_err(|_| BootstrapError::CorruptJournal)?;
        let (after, upper) = match query.page.cursor.as_deref().filter(|s| !s.is_empty()) {
            Some(cursor) => match codec.decode(&scope, cursor).map_err(|_| BootstrapError::InvalidJournal)? {
                CursorPosition::BoundedSequence { sequence, upper_bound }
                    if sequence <= upper_bound && upper_bound <= i64::MAX as u64 => (sequence as i64, upper_bound as i64),
                _ => return Err(BootstrapError::InvalidJournal),
            },
            None => (0, tx.query_row("SELECT coalesce(max(recipient_seq),0) FROM mailbox_items WHERE recipient_principal_id=?", [&actor], |r| r.get::<_, i64>(0))?),
        };
        let state = match query.state {
            InboxState::Unacknowledged => "AND m.acknowledged_at IS NULL",
            InboxState::Acknowledged => "AND m.acknowledged_at IS NOT NULL",
            InboxState::All => "",
        };
        let sql = format!(
            "SELECT m.id,m.record_id,m.recipient_seq,m.created_at,m.acknowledged_at
            FROM mailbox_items m JOIN records r ON r.id=m.record_id
            JOIN spaces s ON s.id=r.space_id AND s.access='public'
            WHERE m.recipient_principal_id=? AND m.recipient_seq>? AND m.recipient_seq<=? {state}
            ORDER BY m.recipient_seq LIMIT ?"
        );
        let rows = tx
            .prepare(&sql)?
            .query_map(
                params![
                    actor,
                    after,
                    upper,
                    (query.page.effective_limit() + 1) as i64
                ],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, Option<String>>(4)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let items = rows
            .into_iter()
            .map(|(id, record_id, seq, created_at, acknowledged_at)| {
                Ok(InboxItem {
                    inbox_item_id: id,
                    recipient: actor.clone(),
                    seq,
                    created_at,
                    acknowledged_at,
                    record: record(&tx, &record_id)?,
                })
            })
            .collect::<Result<Vec<_>, BootstrapError>>()?;
        finish_page(items, &query.page, &codec, &scope, |item| {
            CursorPosition::BoundedSequence {
                sequence: item.seq as u64,
                upper_bound: upper as u64,
            }
        })
    }

    pub fn acknowledge_inbox_item(&self, token: &str, item: &str) -> Result<(), BootstrapError> {
        MailboxItemPath {
            item_id: item.into(),
        }
        .validate()?;
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            let space: String = tx.query_row("SELECT r.space_id FROM mailbox_items m JOIN records r ON r.id=m.record_id WHERE m.id=? AND m.recipient_principal_id=?", params![item, actor], |r| r.get(0))
                .optional()?.ok_or(BootstrapError::NotFound)?;
            permitted(tx, &actor, &space, false)?;
            tx.execute("UPDATE mailbox_items SET acknowledged_at=? WHERE id=? AND acknowledged_at IS NULL", params![self.now()?, item])?;
            self.checkpoint("inbox-acknowledged")?;
            Ok(())
        })
    }

    pub fn delivery_status(
        &self,
        token: &str,
        id: &str,
        query: &PageQuery,
    ) -> Result<ReceiptStatusPage, BootstrapError> {
        self.delivery_status_as(ReadIdentity::Bearer(token), id, query)
    }

    pub(super) fn delivery_status_as(
        &self,
        identity: ReadIdentity<'_>,
        id: &str,
        query: &PageQuery,
    ) -> Result<ReceiptStatusPage, BootstrapError> {
        RecordPath {
            record_id: id.into(),
        }
        .validate()?;
        query.validate()?;
        let mut connection = self.database.connect_read_only()?;
        let tx = connection.transaction()?;
        let actor = self.read_actor(&tx, identity)?;
        let (author, space): (String, String) = tx
            .query_row(
                "SELECT author_principal_id,space_id FROM records WHERE id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or(BootstrapError::NotFound)?;
        permitted(&tx, &actor, &space, false)?;
        if author != actor {
            let addressed: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM mailbox_items WHERE record_id=? AND recipient_principal_id=?)", params![id,actor], |r| r.get(0))?;
            if !addressed {
                return Err(BootstrapError::NotFound);
            }
        }
        let secret: Vec<u8> = tx.query_row(
            "SELECT secret FROM journal_secrets WHERE name='cursor'",
            [],
            |r| r.get(0),
        )?;
        let codec = CursorCodec::new(&secret).map_err(|_| BootstrapError::CorruptJournal)?;
        let filters =
            serde_json::to_vec(&(&actor, id)).map_err(|_| BootstrapError::InvalidJournal)?;
        let scope = CursorScope::new(
            CursorRoute::RecordDeliveryStatus,
            &filters,
            CursorOrder::Identifier,
        );
        let after = identifier_position(&codec, &scope, query)?;
        let items = tx.prepare("SELECT id,recipient_principal_id,created_at,acknowledged_at FROM mailbox_items WHERE record_id=? AND (? OR recipient_principal_id=?) AND id>? ORDER BY id LIMIT ?")?
            .query_map(params![id,author==actor,actor,after,(query.effective_limit()+1) as i64], |r| {
                let acknowledged_at: Option<String> = r.get(3)?;
                Ok(ReceiptSummary { inbox_item_id: r.get(0)?, recipient: r.get(1)?, created_at: r.get(2)?, state: if acknowledged_at.is_some() { ReceiptState::Acknowledged } else { ReceiptState::Unacknowledged }, acknowledged_at })
            })?.collect::<rusqlite::Result<Vec<_>>>()?;
        finish_page(items, query, &codec, &scope, |item| {
            CursorPosition::Identifier {
                id: item.inbox_item_id.clone(),
            }
        })
    }
}
