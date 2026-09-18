use super::*;
use journal_domain::{AppendResult, Record, RecordInput, Relation};
use serde::Serialize;
use std::collections::HashSet;

const THREAD_MAX_DEPTH: usize = 64;
const THREAD_MAX_NODES: usize = 4096;
const THREAD_MAX_EDGES: usize = 8192;

// Counting highlighted spans is document-local: unlike global BM25, inaccessible
// documents cannot change a visible document's score. Compare two renderings
// because FTS highlighting can omit embedded NUL bytes from the source.
const SEARCH_SCORE: &str = "CAST(length(CAST(highlight(records_fts,2,'x','') AS BLOB))
           -length(CAST(highlight(records_fts,2,'','') AS BLOB)) AS REAL)";
// CROSS JOIN keeps MATCH first so invalid syntax is rejected even in an empty
// space; authorization and the sequence cursor precede rendering.
const SEARCH_FROM: &str = "FROM records_fts CROSS JOIN records r ON r.id=records_fts.record_id
    CROSS JOIN memberships m ON m.space_id=r.space_id AND m.principal_id=?1 AND m.can_read=1
    WHERE records_fts MATCH ?2 AND r.space_id=?3
      AND (?4 IS NULL OR r.author_principal_id=?4)
      AND (?5 IS NULL OR EXISTS(SELECT 1 FROM attention a WHERE a.record_id=r.id AND a.recipient_principal_id=?5))
      AND (?6 IS NULL OR unixepoch(r.created_at)>=?6)";

fn search_page_sql(order: SearchOrder) -> String {
    match order {
        SearchOrder::Seq => format!(
            "SELECT records_fts.rowid,r.id,NULL {SEARCH_FROM}
             AND r.space_seq>?7 ORDER BY r.space_seq,r.id LIMIT ?9"
        ),
        SearchOrder::Rank => format!(
            "WITH matches AS MATERIALIZED (
                SELECT records_fts.rowid AS fts_id,r.id,{SEARCH_SCORE} AS score {SEARCH_FROM}
             )
             SELECT fts_id,id,score FROM matches
             WHERE score<?7 OR (score=?7 AND id>?8) ORDER BY score DESC,id LIMIT ?9"
        ),
    }
}

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
    pub fn search_records(
        &self,
        token: &str,
        space: &str,
        query: &SearchRecordsQuery,
    ) -> Result<SearchPage, BootstrapError> {
        query.validate()?;
        let mut connection = self.database.connect_read_only()?;
        // Existing databases initialize this key lazily. Only that one-time
        // initialization needs a short writer transaction, never the FTS query.
        let secret: Option<Vec<u8>> = connection
            .query_row(
                "SELECT secret FROM journal_secrets WHERE name='cursor'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let codec = match secret {
            Some(secret) => {
                CursorCodec::new(&secret).map_err(|_| BootstrapError::CorruptJournal)?
            }
            None => self.transaction(|tx| {
                let actor = self.journal_actor(tx, token)?;
                permitted(tx, &actor, space, false)?;
                self.cursor_codec(tx)
            })?,
        };
        let transaction = connection.transaction()?;
        let tx = &transaction;
        {
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, space, false)?;
            // Server timestamps have whole-second precision. Round the lower
            // bound upward without SQLite's millisecond date rounding.
            let since = query
                .since
                .as_ref()
                .map(|value| {
                    let instant: jiff::Timestamp =
                        value.parse().map_err(|_| BootstrapError::InvalidJournal)?;
                    let nanos = instant.as_nanosecond();
                    i64::try_from(
                        nanos.div_euclid(1_000_000_000)
                            + i128::from(nanos.rem_euclid(1_000_000_000) != 0),
                    )
                    .map_err(|_| BootstrapError::InvalidJournal)
                })
                .transpose()?;
            let order = match query.order {
                SearchOrder::Rank => CursorOrder::Rank,
                SearchOrder::Seq => CursorOrder::Sequence,
            };
            let filters = serde_json::to_vec(&(
                &actor,
                space,
                &query.q,
                &query.author,
                &query.attention,
                &query.since,
            ))
            .map_err(|_| BootstrapError::InvalidJournal)?;
            let scope = CursorScope::new(CursorRoute::Search, &filters, order);
            let position = query
                .page
                .cursor
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|c| {
                    codec
                        .decode(&scope, c)
                        .map_err(|_| BootstrapError::InvalidJournal)
                })
                .transpose()?;
            let (sequence, score, id) = match position {
                None => (0, f64::MAX, String::new()),
                Some(CursorPosition::Sequence { sequence, id }) => (
                    i64::try_from(sequence).map_err(|_| BootstrapError::InvalidJournal)?,
                    0.0,
                    id,
                ),
                Some(CursorPosition::Rank { score_bits, id })
                    if f64::from_bits(score_bits).is_finite() =>
                {
                    (0, f64::from_bits(score_bits), id)
                }
                _ => return Err(BootstrapError::InvalidJournal),
            };
            let key = match query.order {
                SearchOrder::Rank => rusqlite::types::Value::Real(score),
                SearchOrder::Seq => rusqlite::types::Value::Integer(sequence),
            };
            let mut stmt = tx.prepare(&search_page_sql(query.order))?;
            let rows = stmt
                .query_map(
                    params![
                        actor,
                        query.q,
                        space,
                        query.author,
                        query.attention,
                        since,
                        key,
                        id,
                        (query.page.effective_limit() + 1) as i64
                    ],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, Option<f64>>(2)?,
                        ))
                    },
                )
                .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
                .map_err(search_error)?;
            let mut render = tx.prepare(&format!(
                "SELECT COALESCE(?3,{SEARCH_SCORE}),snippet(records_fts,2,'','','…',24)
                 FROM records_fts WHERE rowid=?1 AND records_fts MATCH ?2"
            ))?;
            let items = rows
                .into_iter()
                .map(|(fts_id, id, score)| {
                    let (score, snippet): (f64, String) = render
                        .query_row(params![fts_id, query.q, score], |r| {
                            Ok((r.get(0)?, r.get(1)?))
                        })?;
                    Ok(SearchResult {
                        record: record(tx, &id)?,
                        score,
                        snippet: Some(snippet.chars().take(1024).collect()),
                    })
                })
                .collect::<Result<Vec<_>, BootstrapError>>()?;
            let page = finish_page(items, &query.page, &codec, &scope, |r| match query.order {
                SearchOrder::Rank => CursorPosition::Rank {
                    score_bits: r.score.to_bits(),
                    id: r.record.id.clone(),
                },
                SearchOrder::Seq => CursorPosition::Sequence {
                    sequence: r.record.seq as u64,
                    id: r.record.id.clone(),
                },
            })?;
            Ok(SearchPage {
                items: page.items,
                next_cursor: page.next_cursor,
                order: query.order,
                consistency: Some(match query.order {
                    SearchOrder::Rank => SearchConsistency::BestEffort,
                    SearchOrder::Seq => SearchConsistency::Deterministic,
                }),
            })
        }
    }

    pub fn get_thread(
        &self,
        token: &str,
        id: &str,
        query: &PageQuery,
    ) -> Result<RecordPage, BootstrapError> {
        query.validate()?;
        self.transaction(|tx| {
            let actor = self.journal_actor(tx, token)?;
            let space: String = tx
                .query_row("SELECT space_id FROM records WHERE id=?", [id], |r| {
                    r.get(0)
                })
                .optional()?
                .ok_or(BootstrapError::NotFound)?;
            permitted(tx, &actor, &space, false)?;
            let codec = self.cursor_codec(tx)?;
            let scope = CursorScope::new(
                CursorRoute::RecordThread,
                &serde_json::to_vec(&(&actor, id)).map_err(|_| BootstrapError::InvalidJournal)?,
                CursorOrder::Sequence,
            );
            let after = match query.cursor.as_deref().filter(|s| !s.is_empty()) {
                None => (0, String::new()),
                Some(c) => match codec
                    .decode(&scope, c)
                    .map_err(|_| BootstrapError::InvalidJournal)?
                {
                    CursorPosition::Sequence { sequence, id } => (
                        i64::try_from(sequence).map_err(|_| BootstrapError::InvalidJournal)?,
                        id,
                    ),
                    _ => return Err(BootstrapError::InvalidJournal),
                },
            };
            let ids = thread_ids(
                tx,
                id,
                &space,
                THREAD_MAX_DEPTH,
                THREAD_MAX_NODES,
                THREAD_MAX_EDGES,
            )?;
            let mut items = ids
                .into_iter()
                .filter(|(seq, id)| (*seq, id.as_str()) > (after.0, after.1.as_str()))
                .collect::<Vec<_>>();
            items.sort();
            items.truncate(query.effective_limit() + 1);
            let items = items
                .iter()
                .map(|(_, id)| record(tx, id))
                .collect::<Result<Vec<_>, _>>()?;
            finish_page(items, query, &codec, &scope, |r| CursorPosition::Sequence {
                sequence: r.seq as u64,
                id: r.id.clone(),
            })
        })
    }

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

    pub(super) fn cursor_codec(&self, tx: &Transaction<'_>) -> Result<CursorCodec, BootstrapError> {
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

fn search_error(error: rusqlite::Error) -> BootstrapError {
    match &error {
        rusqlite::Error::SqliteFailure(code, Some(message))
            if code.extended_code == 1
                && (message.starts_with("fts5:")
                    || message.starts_with("unterminated string")
                    || message.starts_with("no such column:")) =>
        {
            BootstrapError::InvalidJournal
        }
        _ => error.into(),
    }
}

fn thread_ids(
    tx: &Transaction<'_>,
    anchor: &str,
    space: &str,
    max_depth: usize,
    max_nodes: usize,
    max_edges: usize,
) -> Result<Vec<(i64, String)>, BootstrapError> {
    let mut root = anchor.to_owned();
    let mut ancestors = HashSet::new();
    let mut edges = 0;
    loop {
        if !ancestors.insert(root.clone()) {
            return Err(BootstrapError::CorruptJournal);
        }
        if ancestors.len() > max_nodes {
            return Err(BootstrapError::InvalidJournal);
        }
        let parent: Option<String> = tx.query_row(
            "SELECT rel.target_record_id FROM record_relations rel JOIN records r ON r.id=rel.target_record_id
             WHERE rel.source_record_id=? AND rel.relation_type='reply-to' AND r.space_id=?",
            params![root,space], |r|r.get(0)).optional()?;
        let Some(parent) = parent else {
            break;
        };
        edges += 1;
        if ancestors.len() > max_depth || edges > max_edges {
            return Err(BootstrapError::InvalidJournal);
        }
        root = parent;
    }
    let mut pending = vec![(root, 0)];
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    while let Some((id, depth)) = pending.pop() {
        if !seen.insert(id.clone()) {
            return Err(BootstrapError::CorruptJournal);
        }
        if seen.len() > max_nodes || depth > max_depth {
            return Err(BootstrapError::InvalidJournal);
        }
        let seq = tx.query_row(
            "SELECT space_seq FROM records WHERE id=? AND space_id=?",
            params![id, space],
            |r| r.get(0),
        )?;
        result.push((seq, id.clone()));
        let remaining = (max_nodes - seen.len() - pending.len()).min(max_edges - edges);
        let mut stmt = tx.prepare(
            "SELECT rel.source_record_id FROM record_relations rel JOIN records r ON r.id=rel.source_record_id
             WHERE rel.target_record_id=? AND rel.relation_type='reply-to' AND r.space_id=?
             ORDER BY rel.source_record_id LIMIT ?")?;
        let children = stmt
            .query_map(params![id, space, (remaining + 1) as i64], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        edges += children.len();
        if children.len() > remaining || (depth == max_depth && !children.is_empty()) {
            return Err(BootstrapError::InvalidJournal);
        }
        pending.extend(children.into_iter().map(|id| (id, depth + 1)));
    }
    Ok(result)
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

pub(super) fn record(tx: &Transaction<'_>, id: &str) -> Result<Record, BootstrapError> {
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
    fn search_selection_never_renders_snippets_or_sequence_scores() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("../../../migrations/0001_initial.sql"))
            .unwrap();
        for order in [SearchOrder::Seq, SearchOrder::Rank] {
            let mut statement = connection
                .prepare(&format!("EXPLAIN {}", search_page_sql(order)))
                .unwrap();
            let functions = statement
                .query_map(
                    params![
                        "writer",
                        "hello",
                        "space",
                        None::<String>,
                        None::<String>,
                        None::<i64>,
                        990,
                        "",
                        3,
                    ],
                    |row| row.get::<_, Option<String>>(5),
                )
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            let functions: Vec<_> = functions.into_iter().flatten().collect();
            assert!(!functions.iter().any(|f| f.starts_with("snippet(")));
            assert_eq!(
                functions
                    .iter()
                    .filter(|f| f.starts_with("highlight("))
                    .count(),
                if order == SearchOrder::Rank { 2 } else { 0 }
            );
        }
    }

    #[test]
    fn thread_budgets_are_independent_and_cycles_fail_closed() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("../../../migrations/0001_initial.sql"))
            .unwrap();
        connection
            .execute_batch(include_str!("../../../migrations/0004_thread_index.sql"))
            .unwrap();
        connection.execute_batch(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('writer','Writer','2026-01-01T00:00:00Z');
             INSERT INTO spaces(id,name,created_at) VALUES ('space','Space','2026-01-01T00:00:00Z');
             INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at) VALUES
             ('root','space',1,'writer','note','hello','2026-01-01T00:00:00Z'),
             ('child','space',2,'writer','note','hello','2026-01-01T00:00:00Z'),
             ('leaf','space',3,'writer','note','hello','2026-01-01T00:00:00Z');
             INSERT INTO record_relations VALUES ('child','reply-to','root','2026-01-01T00:00:00Z'),('leaf','reply-to','child','2026-01-01T00:00:00Z');"
        ).unwrap();
        let tx = connection.transaction().unwrap();
        assert_eq!(thread_ids(&tx, "leaf", "space", 2, 3, 4).unwrap().len(), 3);
        for (depth, nodes, edges) in [(1, 3, 4), (2, 2, 4), (2, 3, 3)] {
            assert!(matches!(
                thread_ids(&tx, "leaf", "space", depth, nodes, edges),
                Err(BootstrapError::InvalidJournal)
            ));
        }
        tx.execute_batch(
            "DROP TRIGGER relation_target_must_be_older_same_space;
            INSERT INTO record_relations VALUES ('root','reply-to','leaf','2026-01-01T00:00:00Z');",
        )
        .unwrap();
        assert!(matches!(
            thread_ids(&tx, "leaf", "space", 64, 4096, 8192),
            Err(BootstrapError::CorruptJournal)
        ));
    }

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
