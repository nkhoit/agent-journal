use super::*;
use journal_domain::{AppendResult, Record, RecordInput, Relation};
use serde::Serialize;

const LIST_RECORD_IDS: &str = "SELECT r.id FROM records r WHERE r.space_id=? AND r.space_seq>?
    AND (r.space_seq,r.id)>(?,?)
    AND (? IS NULL OR r.author_principal_id=?) AND (? IS NULL OR r.kind=?)
    AND (? IS NULL OR EXISTS(SELECT 1 FROM attention a WHERE a.record_id=r.id AND a.recipient_principal_id=?))
    AND (? IS NULL OR EXISTS(SELECT 1 FROM record_relations rel WHERE rel.source_record_id=r.id AND rel.relation_type=?))
    ORDER BY r.space_seq,r.id LIMIT ?";

fn record_sequence_lower_bound(after_seq: Option<u64>, sequence: i64) -> i64 {
    // Per-space sequences are unique, so the cursor row can be excluded by the index seek.
    i64::try_from(after_seq.unwrap_or(0))
        .unwrap_or(i64::MAX)
        .max(sequence)
}

impl BootstrapService {
    fn journal_actor(&self, tx: &Transaction<'_>, token: &str) -> Result<String, BootstrapError> {
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(BootstrapError::Unauthorized);
        }
        tx.query_row(
            "SELECT c.principal_id FROM credentials c JOIN principals p ON p.id=c.principal_id
             WHERE c.token_hash=? AND c.class='principal-client' AND c.revoked_at IS NULL
             AND p.disabled_at IS NULL AND (c.expires_at IS NULL OR julianday(c.expires_at)>julianday(?))",
            params![digest(token), self.now()?], |r| r.get(0),
        ).optional()?.ok_or(BootstrapError::Unauthorized)
    }

    fn cursor_codec(&self, tx: &Transaction<'_>) -> Result<CursorCodec, BootstrapError> {
        let secret: Option<Vec<u8>> = tx
            .query_row(
                "SELECT secret FROM journal_secrets WHERE name='cursor'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let secret = match secret {
            Some(secret) => secret,
            None => {
                let mut bytes = [0; 32];
                self.random.fill(&mut bytes)?;
                tx.execute(
                    "INSERT INTO journal_secrets(name,secret) VALUES ('cursor',?)",
                    [bytes.as_slice()],
                )?;
                bytes.to_vec()
            }
        };
        CursorCodec::new(&secret).map_err(|_| BootstrapError::CorruptJournal)
    }

    fn record_id(&self, instant: SystemTime) -> Result<String, BootstrapError> {
        let millis = instant
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| BootstrapError::Clock)?
            .as_millis();
        if millis >= (1_u128 << 48) {
            return Err(BootstrapError::Clock);
        }
        let mut random = [0; 32];
        self.random.fill(&mut random)?;
        // RFC 9562 UUIDv7: 48-bit Unix milliseconds, version 7, random payload,
        // and the RFC variant. Ordering authority remains the per-space sequence.
        let mut bytes: [u8; 16] = random[..16].try_into().expect("fixed size");
        bytes[..6].copy_from_slice(&(millis as u64).to_be_bytes()[2..]);
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let h = hex(&bytes);
        Ok(format!(
            "{}-{}-{}-{}-{}",
            &h[..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..]
        ))
    }

    pub fn append_record(
        &self,
        token: &str,
        space: &str,
        key: &str,
        input: &RecordInput,
    ) -> Result<AppendResult, BootstrapError> {
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, space, true)?;
            let canonical = canonical_append(input).map_err(|_| BootstrapError::InvalidJournal)?;
            if !(1..=255).contains(&key.chars().count()) { return Err(BootstrapError::InvalidJournal); }
            let path = format!("/v1/spaces/{space}/records");
            let hash = hex(&Sha256::digest(&canonical));
            let previous: Option<(String,String)> = tx.query_row(
                "SELECT payload_hash,response_json FROM idempotency_keys WHERE principal_id=? AND method='POST' AND path=? AND idempotency_key=?",
                params![actor,path,key], |r| Ok((r.get(0)?, r.get(1)?)),
            ).optional()?;
            if let Some((previous_hash, response)) = previous {
                if previous_hash != hash { return Err(BootstrapError::IdempotencyConflict); }
                let mut result: AppendResult = decode_json(response.as_bytes()).map_err(|_| BootstrapError::CorruptJournal)?;
                result.replayed = true;
                return Ok(result);
            }
            let seq: i64 = tx.query_row("SELECT coalesce(max(space_seq),0)+1 FROM records WHERE space_id=?", [space], |r| r.get(0))?;
            self.checkpoint("append-sequence")?;
            for relation in &input.relations {
                permitted(tx, &actor, space, false)?;
                let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND space_id=? AND space_seq<?)",
                    params![relation.record_id,space,seq], |r|r.get(0))?;
                if !exists { return Err(BootstrapError::NotFound); }
            }
            for recipient in &input.attention { permitted(tx, recipient, space, false)?; }
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let id = self.record_id(instant)?;
            tx.execute("INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,run_id,routing_key,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                params![id,space,seq,actor,input.kind,input.content,input.run_id,input.routing_key,now])?;
            self.checkpoint("append-record")?;
            for (position, relation) in input.relations.iter().enumerate() {
                tx.execute("INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at,position) VALUES (?,?,?,?,?)",
                    params![id,relation.relation_type.as_str(),relation.record_id,now,position as i64])?;
            }
            self.checkpoint("append-relations")?;
            let mut attention = input.attention.clone();
            attention.sort();
            for recipient in &attention {
                tx.execute("INSERT INTO attention(record_id,recipient_principal_id,created_at) VALUES (?,?,?)", params![id,recipient,now])?;
                self.checkpoint("append-attention")?;
                // The mailbox_initial_attempt trigger owns ordinal 1.
                tx.execute("INSERT INTO mailbox_items(id,record_id,recipient_principal_id,state,created_at,updated_at) VALUES (?,?,?,'pending',?,?)",
                    params![format!("item-{}", self.secret()?),id,recipient,now,now])?;
                self.checkpoint("append-mailbox")?;
            }
            let result = AppendResult {
                record: Record { id: id.clone(), space_id: space.into(), seq, author: actor.clone(), kind: input.kind.clone(), content: input.content.clone(), run_id: input.run_id.clone(), created_at: now.clone(), attention, routing_key: input.routing_key.clone(), relations: input.relations.clone() },
                mailbox_created: input.attention.len(), replayed: false,
            };
            let response = serde_json::to_string(&result).map_err(|_| BootstrapError::CorruptJournal)?;
            tx.execute("INSERT INTO idempotency_keys(principal_id,method,path,idempotency_key,payload_hash,record_id,response_json,created_at) VALUES (?,'POST',?,?,?,?,?,?)",
                params![actor,path,key,hash,id,response,now])?;
            self.checkpoint("append-idempotency")?;
            Ok(result)
        })
    }

    pub fn get_record(&self, token: &str, id: &str) -> Result<Record, BootstrapError> {
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            let space: String = tx
                .query_row("SELECT space_id FROM records WHERE id=?", [id], |r| {
                    r.get(0)
                })
                .optional()?
                .ok_or(BootstrapError::NotFound)?;
            permitted(tx, &actor, &space, false)?;
            record(tx, id)
        })
    }

    pub fn get_space(&self, token: &str, id: &str) -> Result<Space, BootstrapError> {
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, id, false)?;
            space(tx, id)
        })
    }

    pub fn list_spaces(&self, token: &str, query: &PageQuery) -> Result<SpacePage, BootstrapError> {
        query.validate()?;
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            let codec = self.cursor_codec(tx)?;
            let scope = scope(CursorRoute::Spaces, &(&actor,))?;
            let after = identifier_position(&codec, &scope, query)?;
            let mut stmt = tx.prepare("SELECT s.id FROM spaces s JOIN memberships m ON m.space_id=s.id WHERE m.principal_id=? AND m.can_read=1 AND s.id>? ORDER BY s.id LIMIT ?")?;
            let ids = stmt.query_map(params![actor,after,(query.effective_limit()+1) as i64], |r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let items = ids.iter().map(|id|space(tx,id)).collect::<Result<Vec<_>,_>>()?;
            finish_page(items, query, &codec, &scope, |s|CursorPosition::Identifier { id:s.id.clone() })
        })
    }

    pub fn list_principals(
        &self,
        token: &str,
        query: &ListPrincipalsQuery,
    ) -> Result<PrincipalPage, BootstrapError> {
        query.validate()?;
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, &query.space, false)?;
            let codec = self.cursor_codec(tx)?;
            let scope = scope(CursorRoute::Principals, &(&actor,&query.space))?;
            let after = identifier_position(&codec, &scope, &query.page)?;
            let mut stmt = tx.prepare("SELECT p.id,p.display_name,p.created_at FROM principals p JOIN memberships m ON m.principal_id=p.id WHERE m.space_id=? AND m.can_read=1 AND p.disabled_at IS NULL AND p.id>? ORDER BY p.id LIMIT ?")?;
            let items = stmt.query_map(params![query.space,after,(query.page.effective_limit()+1) as i64], |r|Ok(Principal { id:r.get(0)?,display_name:Some(r.get(1)?),created_at:r.get(2)?,disabled:false }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            finish_page(items, &query.page, &codec, &scope, |p|CursorPosition::Identifier { id:p.id.clone() })
        })
    }

    pub fn list_records(
        &self,
        token: &str,
        space: &str,
        query: &ListRecordsQuery,
    ) -> Result<RecordPage, BootstrapError> {
        query.validate()?;
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, space, false)?;
            let codec = self.cursor_codec(tx)?;
            let filters = serde_json::to_vec(&(
                &actor,
                space,
                query.after_seq.unwrap_or(0),
                &query.author,
                &query.attention,
                &query.kind,
                &query.relation,
            ))
            .map_err(|_| BootstrapError::InvalidJournal)?;
            let scope = CursorScope::new(CursorRoute::Records, &filters, CursorOrder::Sequence);
            let (sequence, id) = match query.page.cursor.as_deref().filter(|s| !s.is_empty()) {
                None => (0, String::new()),
                Some(cursor) => match codec
                    .decode(&scope, cursor)
                    .map_err(|_| BootstrapError::InvalidJournal)?
                {
                    CursorPosition::Sequence { sequence, id } => (sequence, id),
                    _ => return Err(BootstrapError::InvalidJournal),
                },
            };
            let sequence = i64::try_from(sequence).map_err(|_| BootstrapError::InvalidJournal)?;
            let lower = record_sequence_lower_bound(query.after_seq, sequence);
            let mut stmt = tx.prepare(LIST_RECORD_IDS)?;
            let relation = query.relation.map(|r| r.as_str());
            let ids = stmt
                .query_map(
                    params![
                        space,
                        lower,
                        sequence,
                        id,
                        query.author,
                        query.author,
                        query.kind,
                        query.kind,
                        query.attention,
                        query.attention,
                        relation,
                        relation,
                        (query.page.effective_limit() + 1) as i64
                    ],
                    |r| r.get::<_, String>(0),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items = ids
                .iter()
                .map(|id| record(tx, id))
                .collect::<Result<Vec<_>, _>>()?;
            finish_page(items, &query.page, &codec, &scope, |r| {
                CursorPosition::Sequence {
                    sequence: r.seq as u64,
                    id: r.id.clone(),
                }
            })
        })
    }
}

fn permitted(
    tx: &Transaction<'_>,
    principal: &str,
    space: &str,
    append: bool,
) -> Result<(), BootstrapError> {
    let allowed: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM memberships m JOIN principals p ON p.id=m.principal_id JOIN spaces s ON s.id=m.space_id
        WHERE m.principal_id=? AND m.space_id=? AND p.disabled_at IS NULL
        AND ((?=0 AND m.can_read=1) OR (?=1 AND m.can_append=1 AND s.archived_at IS NULL)))",
        params![principal,space,append,append], |r|r.get(0))?;
    if allowed {
        Ok(())
    } else {
        Err(BootstrapError::NotFound)
    }
}

fn record(tx: &Transaction<'_>, id: &str) -> Result<Record, BootstrapError> {
    let mut record = tx.query_row("SELECT id,space_id,space_seq,author_principal_id,kind,content,run_id,created_at,routing_key FROM records WHERE id=?", [id], |r|Ok(Record {
        id:r.get(0)?,space_id:r.get(1)?,seq:r.get(2)?,author:r.get(3)?,kind:r.get(4)?,content:r.get(5)?,run_id:r.get(6)?,created_at:r.get(7)?,routing_key:r.get(8)?,attention:vec![],relations:vec![],
    })).optional()?.ok_or(BootstrapError::NotFound)?;
    record.attention = tx.prepare("SELECT recipient_principal_id FROM attention WHERE record_id=? ORDER BY recipient_principal_id")?.query_map([id], |r|r.get(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let relations = tx.prepare("SELECT relation_type,target_record_id FROM record_relations WHERE source_record_id=? ORDER BY position,rowid")?.query_map([id], |r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    record.relations = relations
        .into_iter()
        .map(|(kind, record_id)| {
            let relation_type = journal_domain::RELATION_TYPES
                .into_iter()
                .find(|t| t.as_str() == kind)
                .ok_or(BootstrapError::CorruptJournal)?;
            Ok(Relation {
                relation_type,
                record_id,
            })
        })
        .collect::<Result<_, BootstrapError>>()?;
    Ok(record)
}

fn space(tx: &Transaction<'_>, id: &str) -> Result<Space, BootstrapError> {
    Ok(tx.query_row(
        "SELECT id,name,created_at,archived_at FROM spaces WHERE id=?",
        [id],
        |r| {
            Ok(Space {
                id: r.get(0)?,
                name: r.get(1)?,
                created_at: r.get(2)?,
                archived_at: r.get(3)?,
                limits: default_limits(),
            })
        },
    )?)
}

fn scope(route: CursorRoute, filters: &impl Serialize) -> Result<CursorScope, BootstrapError> {
    Ok(CursorScope::new(
        route,
        &serde_json::to_vec(filters).map_err(|_| BootstrapError::InvalidJournal)?,
        CursorOrder::Identifier,
    ))
}

fn identifier_position(
    codec: &CursorCodec,
    scope: &CursorScope,
    query: &PageQuery,
) -> Result<String, BootstrapError> {
    match query.cursor.as_deref().filter(|s| !s.is_empty()) {
        None => Ok(String::new()),
        Some(cursor) => match codec
            .decode(scope, cursor)
            .map_err(|_| BootstrapError::InvalidJournal)?
        {
            CursorPosition::Identifier { id } => Ok(id),
            _ => Err(BootstrapError::InvalidJournal),
        },
    }
}

fn finish_page<T>(
    mut items: Vec<T>,
    query: &PageQuery,
    codec: &CursorCodec,
    scope: &CursorScope,
    position: impl Fn(&T) -> CursorPosition,
) -> Result<Page<T>, BootstrapError> {
    let next_cursor = if items.len() > query.effective_limit() {
        items.truncate(query.effective_limit());
        Some(
            codec
                .encode(scope, &position(items.last().expect("nonempty page")))
                .map_err(|_| BootstrapError::CorruptJournal)?,
        )
    } else {
        None
    };
    Ok(Page { items, next_cursor })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, StatementStatus};

    #[test]
    fn late_record_page_seeks_past_cursor() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("../../../migrations/0001_initial.sql"))
            .unwrap();
        connection
            .execute_batch(
                "INSERT INTO principals(id,display_name,created_at) VALUES ('writer','Writer','2026-01-01T00:00:00Z');
                 INSERT INTO spaces(id,name,created_at) VALUES ('space','Space','2026-01-01T00:00:00Z');
                 WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<100000)
                 INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
                 SELECT printf('record-%06d',n),'space',n,'writer','note','hello','2026-01-01T00:00:00Z' FROM seq;",
            )
            .unwrap();
        let mut stmt = connection.prepare(LIST_RECORD_IDS).unwrap();
        let mut page = |lower: i64, sequence: i64| {
            stmt.reset_status(StatementStatus::VmStep);
            let ids = stmt
                .query_map(
                    params![
                        "space",
                        lower,
                        sequence,
                        format!("record-{sequence:06}"),
                        None::<String>,
                        None::<String>,
                        None::<String>,
                        None::<String>,
                        None::<String>,
                        None::<String>,
                        None::<String>,
                        None::<String>,
                        11
                    ],
                    |row| row.get::<_, String>(0),
                )
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (ids, stmt.get_status(StatementStatus::VmStep))
        };
        let (_, early_steps) = page(record_sequence_lower_bound(None, 0), 0);
        let (late, late_steps) = page(record_sequence_lower_bound(None, 99000), 99000);
        let (unbounded, unbounded_steps) = page(0, 99000);
        assert_eq!(late, unbounded);
        assert_eq!(
            late,
            (99001..=99011)
                .map(|n| format!("record-{n:06}"))
                .collect::<Vec<_>>()
        );
        assert!(
            late_steps < 2000 && late_steps <= early_steps * 2,
            "early={early_steps}, late={late_steps}"
        );
        assert!(
            unbounded_steps > late_steps * 100,
            "unbounded={unbounded_steps}, late={late_steps}"
        );
        for (after_seq, cursor, first) in [
            (None, 0, 1),
            (Some(99000), 0, 99001),
            (Some(98000), 99000, 99001),
            (Some(99000), 99000, 99001),
            (Some(99005), 99000, 99006),
            (None, 99999, 100000),
        ] {
            let (ids, _) = page(record_sequence_lower_bound(after_seq, cursor), cursor);
            assert_eq!(ids[0], format!("record-{first:06}"));
        }
        for (after_seq, cursor) in [
            (None, 100000),
            (Some(100000), 99000),
            (Some(i64::MAX as u64), 99000),
            (Some(u64::MAX), 99000),
        ] {
            assert!(
                page(record_sequence_lower_bound(after_seq, cursor), cursor)
                    .0
                    .is_empty()
            );
        }
        eprintln!(
            "record page VM steps: early={early_steps}, late={late_steps}, unbounded={unbounded_steps}"
        );
    }
}
