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
    CROSS JOIN spaces s ON s.id=r.space_id AND s.access='public'
    CROSS JOIN principals p ON p.id=?1 AND p.disabled_at IS NULL
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

/// Stored in place of a copy of each append response. Records, relations and
/// attention are immutable, so replay rebuilds the identical response from
/// the record; any other stored value is a full copy from an older build.
const REBUILT_APPEND_RESPONSE: &str = "{}";

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
        self.search_records_as(ReadIdentity::Bearer(token), space, query)
    }

    pub(super) fn search_records_as(
        &self,
        identity: ReadIdentity<'_>,
        space: &str,
        query: &SearchRecordsQuery,
    ) -> Result<SearchPage, BootstrapError> {
        query.validate()?;
        let mut connection = self.database.connect_read_only()?;
        // Existing databases initialize this key lazily. Only that one-time
        // initialization needs a short write transaction, never the FTS query.
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
                let actor = self.read_actor(tx, identity)?;
                permitted(tx, &actor, space, false)?;
                self.cursor_codec(tx)
            })?,
        };
        let transaction = connection.transaction()?;
        let tx = &transaction;
        {
            let actor = self.read_actor(tx, identity)?;
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
            let author = resolve_space_principal(tx, space, query.author.as_deref())?;
            let attention = resolve_space_principal(tx, space, query.attention.as_deref())?;
            let filters =
                serde_json::to_vec(&(&actor, space, &query.q, &author, &attention, &query.since))
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
                        author,
                        attention,
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
        self.get_thread_as(ReadIdentity::Bearer(token), id, query)
    }

    /// Resolve the root record of the thread containing `id`.
    /// Internal viewer helper; not a public API endpoint.
    ///
    /// Returns the root record plus whether the reply-to chain resolved to a
    /// genuine root. When the chain is unresolved (missing or cross-space
    /// parent), the returned record is fallback context (the deepest
    /// resolvable node, which may be the anchor itself); callers must not
    /// treat its title as the thread title.
    pub(super) fn get_thread_root_as(
        &self,
        identity: ReadIdentity<'_>,
        id: &str,
    ) -> Result<(Record, bool), BootstrapError> {
        self.read(|tx| {
            let actor = self.read_actor(tx, identity)?;
            let space: String = tx
                .query_row("SELECT space_id FROM records WHERE id=?", [id], |r| {
                    r.get(0)
                })
                .optional()?
                .ok_or(BootstrapError::NotFound)?;
            permitted(tx, &actor, &space, false)?;
            let (root_id, _, resolved) = thread_root_id(
                tx,
                id,
                &space,
                THREAD_MAX_DEPTH,
                THREAD_MAX_NODES,
                THREAD_MAX_EDGES,
            )?;
            let root = record(tx, &root_id)?;
            Ok((root, resolved))
        })
    }

    /// Batch-resolve thread root IDs and titles for viewer breadcrumbs.
    /// Returns a map from record ID to (root_id, root_title).
    /// Internal viewer helper; not a public API endpoint.
    ///
    /// Uses one recursive query ascending the reply-to parent chains for all
    /// record IDs at once (depth-bounded, cycle-safe, same-space parents only,
    /// never enumerating descendants). A row whose chain cannot be resolved
    /// (depth bound, cycle, missing parent, or cross-space parent) falls back
    /// to its own ID with no title instead of failing the page; the viewer
    /// still renders such a record as a reply (rootness comes from the
    /// record's own reply-to relation), with an untitled thread breadcrumb.
    /// Records the viewer cannot read are omitted (fail-closed).
    pub(super) fn get_thread_roots_as(
        &self,
        identity: ReadIdentity<'_>,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, (String, Option<String>)>, BootstrapError> {
        self.read(|tx| {
            let actor = self.read_actor(tx, identity)?;
            let mut result = std::collections::HashMap::new();
            if ids.is_empty() {
                return Ok(result);
            }
            // Space lookup + permission check (cached per space).
            let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let spaces: Vec<(String, String)> = tx
                .prepare(&format!(
                    "SELECT id, space_id FROM records WHERE id IN ({placeholders})"
                ))?
                .query_map(rusqlite::params_from_iter(ids), |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?
                .collect::<Result<_, _>>()?;
            let mut space_allowed: std::collections::HashMap<String, bool> =
                std::collections::HashMap::new();
            let mut permitted_ids: Vec<String> = Vec::new();
            for (id, space) in spaces {
                let allowed = match space_allowed.get(&space) {
                    Some(&allowed) => allowed,
                    None => {
                        let allowed = permitted(tx, &actor, &space, false).is_ok();
                        space_allowed.insert(space.clone(), allowed);
                        allowed
                    }
                };
                if allowed {
                    permitted_ids.push(id);
                }
            }
            if permitted_ids.is_empty() {
                return Ok(result);
            }
            // One recursive ascent for every permitted record ID. The path
            // column makes the walk cycle-safe; the depth bound keeps it
            // within THREAD_MAX_DEPTH edges (which subsumes the node/edge
            // budgets: 4096 nodes and 8192 edges against at most 65 nodes).
            let placeholders = permitted_ids
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");
            let mut stmt = tx.prepare(&format!(
                "WITH RECURSIVE chain(record_id, node_id, space_id, depth, path) AS (
                    SELECT id, id, space_id, 0, ',' || id || ','
                    FROM records WHERE id IN ({placeholders})
                    UNION ALL
                    SELECT c.record_id, rel.target_record_id, c.space_id, c.depth + 1,
                           c.path || rel.target_record_id || ','
                    FROM chain c
                    JOIN record_relations rel
                      ON rel.source_record_id = c.node_id
                     AND rel.relation_type = 'reply-to'
                    JOIN records r
                      ON r.id = rel.target_record_id
                     AND r.space_id = c.space_id
                    WHERE c.depth < {}
                      AND instr(c.path, ',' || rel.target_record_id || ',') = 0
                ),
                deepest AS (
                    SELECT record_id, node_id AS root_id, space_id, MAX(depth) AS depth
                    FROM chain
                    GROUP BY record_id
                )
                SELECT d.record_id, d.root_id,
                       -- A chain is unresolved when the deepest node still
                       -- carries a reply-to edge: the walk stopped because of
                       -- the depth bound, a cycle, or a parent that is missing
                       -- or in another space. Checking the edge itself (not
                       -- only edges that resolve to a same-space record) keeps
                       -- a broken chain from being mistaken for a root, which
                       -- would promote a reply title into the root slot.
                       EXISTS (
                           SELECT 1
                           FROM record_relations rel
                           WHERE rel.source_record_id = d.root_id
                             AND rel.relation_type = 'reply-to'
                       ) AS unresolved
                FROM deepest d",
                THREAD_MAX_DEPTH
            ))?;
            let candidates: Vec<(String, String, bool)> = stmt
                .query_map(rusqlite::params_from_iter(&permitted_ids), |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, bool>(2)?,
                    ))
                })?
                .collect::<Result<_, _>>()?;
            // Per-row fallback: an unresolved chain (depth bound, cycle,
            // missing parent, or cross-space parent) resolves to the record
            // itself with no title. The viewer still renders the record as a
            // reply because rootness comes from its own reply-to relation.
            let mut root_ids: Vec<String> = Vec::new();
            let mut resolved: Vec<(String, String)> = Vec::new();
            for (record_id, root_id, unresolved) in candidates {
                if unresolved {
                    result.insert(record_id.clone(), (record_id, None));
                } else {
                    root_ids.push(root_id.clone());
                    resolved.push((record_id, root_id));
                }
            }
            // One batched title lookup for all distinct roots.
            if !root_ids.is_empty() {
                let placeholders = root_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let titles: std::collections::HashMap<String, Option<String>> = tx
                    .prepare(&format!(
                        "SELECT id, title FROM records WHERE id IN ({placeholders})"
                    ))?
                    .query_map(rusqlite::params_from_iter(&root_ids), |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
                    })?
                    .collect::<Result<_, _>>()?;
                for (record_id, root_id) in resolved {
                    let root_title = titles.get(&root_id).and_then(|t| t.clone());
                    result.insert(record_id, (root_id, root_title));
                }
            }
            Ok(result)
        })
    }

    pub(super) fn get_thread_as(
        &self,
        identity: ReadIdentity<'_>,
        id: &str,
        query: &PageQuery,
    ) -> Result<RecordPage, BootstrapError> {
        query.validate()?;
        self.read_paged(identity, |tx, codec| {
            let actor = self.read_actor(tx, identity)?;
            let space: String = tx
                .query_row("SELECT space_id FROM records WHERE id=?", [id], |r| {
                    r.get(0)
                })
                .optional()?
                .ok_or(BootstrapError::NotFound)?;
            permitted(tx, &actor, &space, false)?;
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

    pub(super) fn journal_actor(
        &self,
        tx: &Transaction<'_>,
        token: &str,
    ) -> Result<String, BootstrapError> {
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

    /// Resolve a still-valid client credential without loading mutable principal
    /// state. Append idempotency is bound to this durable credential principal so
    /// a committed response can be replayed after a later ACL, profile, or
    /// disable change.
    fn append_replay_actor(
        &self,
        tx: &Transaction<'_>,
        token: &str,
    ) -> Result<String, BootstrapError> {
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(BootstrapError::Unauthorized);
        }
        tx.query_row(
            "SELECT principal_id FROM credentials
             WHERE token_hash=? AND class='principal-client' AND revoked_at IS NULL
             AND (expires_at IS NULL OR julianday(expires_at)>julianday(?))",
            params![digest(token), self.now()?],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(BootstrapError::Unauthorized)
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

    /// Pure reads use a read-only snapshot, so they neither serialize with one
    /// another nor hold SQLite's writer lock.
    pub(super) fn read<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, BootstrapError>,
    ) -> Result<T, BootstrapError> {
        let mut connection = self.database.connect_read_only()?;
        let transaction = connection.transaction()?;
        operation(&transaction)
    }

    /// Paged reads also need the lazily created cursor key. Its one-time
    /// initialization is a short audited write for an authenticated caller,
    /// taken before the read snapshot opens so that snapshot cannot block it.
    fn read_paged<T>(
        &self,
        identity: ReadIdentity<'_>,
        operation: impl FnOnce(&Transaction<'_>, CursorCodec) -> Result<T, BootstrapError>,
    ) -> Result<T, BootstrapError> {
        let mut connection = self.database.connect_read_only()?;
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
                self.read_actor(tx, identity)?;
                self.cursor_codec(tx)
            })?,
        };
        let transaction = connection.transaction()?;
        operation(&transaction, codec)
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
        if !(1..=255).contains(&key.chars().count()) {
            return Err(BootstrapError::InvalidJournal);
        }
        // The durable idempotency hash preserves submitted handle spelling and
        // ordering. Resolve names only after deciding this is genuinely new.
        let hash = hex(&Sha256::digest(
            serde_json::to_vec(input).map_err(|_| BootstrapError::InvalidJournal)?,
        ));
        let path = format!("/v1/spaces/{space}/records");
        self.transaction(|tx| {
            let replay_actor = self.append_replay_actor(tx, token)?;
            let previous: Option<(String, String, String)> = tx
                .query_row(
                    "SELECT payload_hash,record_id,response_json FROM idempotency_keys
                     WHERE principal_id=? AND method='POST' AND path=? AND idempotency_key=?",
                    params![replay_actor, path, key],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((previous_hash, record_id, response)) = previous {
                if previous_hash != hash {
                    return Err(BootstrapError::IdempotencyConflict);
                }
                let mut result = if response == REBUILT_APPEND_RESPONSE {
                    let record = record(tx, &record_id)?;
                    AppendResult {
                        mailbox_created: record.attention.len(),
                        record,
                        replayed: false,
                    }
                } else {
                    decode_json(response.as_bytes()).map_err(|_| BootstrapError::CorruptJournal)?
                };
                result.replayed = true;
                return Ok(result);
            }
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, space, true)?;
            self.cursor_codec(tx)?;
            let mut input = input.clone();
            for recipient in &mut input.attention {
                let principal_id = active_principal(tx, recipient)?;
                permitted(tx, &principal_id, space, false)?;
                *recipient = principal_id;
            }
            // Normalize after the idempotency hash is computed from the
            // submitted bytes: distinct spellings stay distinct replays,
            // while persistence always sees the trimmed title.
            input.title = journal_domain::normalize_title(input.title.clone());
            canonical_append(&input).map_err(|_| BootstrapError::InvalidJournal)?;
            let seq: i64 = tx.query_row("SELECT coalesce(max(space_seq),0)+1 FROM records WHERE space_id=?", [space], |r| r.get(0))?;
            self.checkpoint("append-sequence")?;
            for relation in &input.relations {
                permitted(tx, &actor, space, false)?;
                let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND space_id=? AND space_seq<?)",
                    params![relation.record_id,space,seq], |r|r.get(0))?;
                if !exists { return Err(BootstrapError::NotFound); }
            }
            let instant = self.clock.now();
            let now = timestamp(instant)?;
            let id = self.record_id(instant)?;
            tx.execute("INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,run_id,routing_key,title,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                params![id,space,seq,actor,input.kind,input.content,input.run_id,input.routing_key,input.title,now])?;
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
                tx.execute("INSERT INTO inbox_sequences(recipient_principal_id,last_seq) VALUES (?,1)
                    ON CONFLICT(recipient_principal_id) DO UPDATE SET last_seq=last_seq+1", [recipient])?;
                let recipient_seq: i64 = tx.query_row("SELECT last_seq FROM inbox_sequences WHERE recipient_principal_id=?", [recipient], |r| r.get(0))?;
                self.checkpoint("append-inbox-sequence")?;
                tx.execute("INSERT INTO mailbox_items(id,record_id,recipient_principal_id,created_at,recipient_seq) VALUES (?,?,?,?,?)",
                    params![format!("item-{}", self.secret()?),id,recipient,now,recipient_seq])?;
                self.checkpoint("append-mailbox")?;
            }
            let result = AppendResult {
                record: Record { id: id.clone(), space_id: space.into(), seq, author: actor.clone(), kind: input.kind.clone(), content: input.content.clone(), run_id: input.run_id.clone(), created_at: now.clone(), attention, routing_key: input.routing_key.clone(), relations: input.relations.clone(), title: input.title.clone() },
                mailbox_created: input.attention.len(), replayed: false,
            };
            tx.execute("INSERT INTO idempotency_keys(principal_id,method,path,idempotency_key,payload_hash,record_id,response_json,created_at) VALUES (?,'POST',?,?,?,?,?,?)",
                params![actor,path,key,hash,id,REBUILT_APPEND_RESPONSE,now])?;
            self.checkpoint("append-idempotency")?;
            Ok(result)
        })
    }

    pub fn get_record(&self, token: &str, id: &str) -> Result<Record, BootstrapError> {
        self.get_record_as(ReadIdentity::Bearer(token), id)
    }

    pub(super) fn get_record_as(
        &self,
        identity: ReadIdentity<'_>,
        id: &str,
    ) -> Result<Record, BootstrapError> {
        self.read(|tx| {
            let actor = self.read_actor(tx, identity)?;
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
        self.read(|tx| {
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, id, false)?;
            space(tx, id)
        })
    }

    pub fn list_spaces(&self, token: &str, query: &PageQuery) -> Result<SpacePage, BootstrapError> {
        self.list_spaces_as(ReadIdentity::Bearer(token), query)
    }

    pub(super) fn list_spaces_as(
        &self,
        identity: ReadIdentity<'_>,
        query: &PageQuery,
    ) -> Result<SpacePage, BootstrapError> {
        query.validate()?;
        self.read_paged(identity, |tx, codec| {
            let actor = self.read_actor(tx, identity)?;
            let scope = scope(CursorRoute::Spaces, &(&actor,))?;
            let after = identifier_position(&codec, &scope, query)?;
            let mut stmt = tx.prepare("SELECT s.id FROM spaces s WHERE s.access='public' AND s.id>? ORDER BY s.id LIMIT ?")?;
            let ids = stmt.query_map(params![after,(query.effective_limit()+1) as i64], |r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
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
        self.read_paged(ReadIdentity::Bearer(token), |tx, codec| {
            let actor = self.journal_actor(tx, token)?;
            permitted(tx, &actor, &query.space, false)?;
            let scope = scope(CursorRoute::Principals, &(&actor,&query.space))?;
            let after = identifier_position(&codec, &scope, &query.page)?;
            let mut stmt = tx.prepare("SELECT p.id,n.name,p.display_name,p.description,p.profile_revision,p.created_at FROM principals p JOIN principal_names n ON n.principal_id=p.id AND n.kind='current' WHERE p.disabled_at IS NULL AND p.id>? ORDER BY p.id LIMIT ?")?;
            let items = stmt.query_map(params![after,(query.page.effective_limit()+1) as i64], |r|Ok(Principal { id:r.get(0)?,handle:r.get(1)?,display_name:r.get(2)?,description:r.get(3)?,profile_revision:r.get(4)?,created_at:r.get(5)?,disabled:false }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            finish_page(items, &query.page, &codec, &scope, |p|CursorPosition::Identifier { id:p.id.clone() })
        })
    }

    pub fn list_records(
        &self,
        token: &str,
        space: &str,
        query: &ListRecordsQuery,
    ) -> Result<RecordPage, BootstrapError> {
        self.list_records_as(ReadIdentity::Bearer(token), space, query)
    }

    pub(super) fn list_records_as(
        &self,
        identity: ReadIdentity<'_>,
        space: &str,
        query: &ListRecordsQuery,
    ) -> Result<RecordPage, BootstrapError> {
        query.validate()?;
        self.read_paged(identity, |tx, codec| {
            let actor = self.read_actor(tx, identity)?;
            permitted(tx, &actor, space, false)?;
            let author = resolve_space_principal(tx, space, query.author.as_deref())?;
            let attention = resolve_space_principal(tx, space, query.attention.as_deref())?;
            let filters = serde_json::to_vec(&(
                &actor,
                space,
                query.after_seq.unwrap_or(0),
                &author,
                &attention,
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
                        author,
                        author,
                        query.kind,
                        query.kind,
                        attention,
                        attention,
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

fn resolve_space_principal(
    tx: &Transaction<'_>,
    space: &str,
    selector: Option<&str>,
) -> Result<Option<String>, BootstrapError> {
    let Some(selector) = selector else {
        return Ok(None);
    };
    // Names win over UUID-shaped selectors. A handle resolves only when the
    // persisted name and active principal under the space policy both authorize it.
    let principal = tx
        .query_row(
            "SELECT p.id FROM principal_names n
               JOIN principals p ON p.id=n.principal_id AND p.disabled_at IS NULL
               JOIN spaces s ON s.id=?2 AND s.access='public'
             WHERE n.name=?1
             UNION ALL
             SELECT p.id FROM principals p
               JOIN spaces s ON s.id=?2 AND s.access='public'
             WHERE p.id=?1 AND p.disabled_at IS NULL
               AND NOT EXISTS(SELECT 1 FROM principal_names WHERE name=?1)
             LIMIT 1",
            params![selector, space],
            |row| row.get(0),
        )
        .optional()?;
    Ok(Some(principal.unwrap_or_default()))
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

fn thread_root_id(
    tx: &Transaction<'_>,
    anchor: &str,
    space: &str,
    max_depth: usize,
    max_nodes: usize,
    max_edges: usize,
) -> Result<(String, usize, bool), BootstrapError> {
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
    // The ascent above only follows same-space parents. If the final node still
    // carries any reply-to edge (dangling target or cross-space parent), the
    // chain is unresolved: report the anchor as fallback context, not as a
    // proven root, so callers never promote a reply title to a thread header.
    let resolved: bool = tx
        .query_row(
            "SELECT 1 FROM record_relations WHERE source_record_id=? AND relation_type='reply-to'",
            [root.clone()],
            |_| Ok(()),
        )
        .optional()?
        .is_none();
    Ok((root, edges, resolved))
}

fn thread_ids(
    tx: &Transaction<'_>,
    anchor: &str,
    space: &str,
    max_depth: usize,
    max_nodes: usize,
    max_edges: usize,
) -> Result<Vec<(i64, String)>, BootstrapError> {
    let (root, root_edges, _) = thread_root_id(tx, anchor, space, max_depth, max_nodes, max_edges)?;
    let mut pending = vec![(root, 0)];
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    let mut edges = root_edges;
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

pub(super) fn permitted(
    tx: &Transaction<'_>,
    principal: &str,
    space: &str,
    append: bool,
) -> Result<(), BootstrapError> {
    let allowed: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM principals p JOIN spaces s ON s.id=?
        WHERE p.id=? AND p.disabled_at IS NULL AND s.access='public'
        AND (?=0 OR s.archived_at IS NULL))",
        params![space, principal, append],
        |r| r.get(0),
    )?;
    if allowed {
        Ok(())
    } else {
        Err(BootstrapError::NotFound)
    }
}

pub(super) fn record(tx: &Transaction<'_>, id: &str) -> Result<Record, BootstrapError> {
    let mut record = tx.query_row("SELECT id,space_id,space_seq,author_principal_id,kind,content,run_id,created_at,routing_key,title FROM records WHERE id=?", [id], |r|Ok(Record {
        id:r.get(0)?,space_id:r.get(1)?,seq:r.get(2)?,author:r.get(3)?,kind:r.get(4)?,content:r.get(5)?,run_id:r.get(6)?,created_at:r.get(7)?,routing_key:r.get(8)?,title:r.get(9)?,attention:vec![],relations:vec![],
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
        "SELECT id,name,created_at,archived_at,access FROM spaces WHERE id=?",
        [id],
        |r| {
            Ok(Space {
                id: r.get(0)?,
                name: r.get(1)?,
                created_at: r.get(2)?,
                archived_at: r.get(3)?,
                access: match r.get::<_, String>(4)?.as_str() {
                    "public" => journal_domain::SpaceAccess::Public,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                },
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

pub(super) fn identifier_position(
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

pub(super) fn finish_page<T>(
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
            .execute_batch(include_str!("../../../migrations/0001_uuid_native.sql"))
            .unwrap();
        for order in [SearchOrder::Seq, SearchOrder::Rank] {
            let mut statement = connection
                .prepare(&format!("EXPLAIN {}", search_page_sql(order)))
                .unwrap();
            let functions = statement
                .query_map(
                    params![
                        "018f1f59-6e90-7000-8000-000000000001",
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
            .execute_batch(include_str!("../../../migrations/0001_uuid_native.sql"))
            .unwrap();
        connection.execute_batch(
            "INSERT INTO principals(id,display_name,created_at) VALUES ('018f1f59-6e90-7000-8000-000000000001','Writer','2026-01-01T00:00:00Z');
             INSERT INTO spaces(id,name,access,created_at) VALUES ('space','Space','public','2026-01-01T00:00:00Z');
             INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at) VALUES
             ('root','space',1,'018f1f59-6e90-7000-8000-000000000001','note','hello','2026-01-01T00:00:00Z'),
             ('child','space',2,'018f1f59-6e90-7000-8000-000000000001','note','hello','2026-01-01T00:00:00Z'),
             ('leaf','space',3,'018f1f59-6e90-7000-8000-000000000001','note','hello','2026-01-01T00:00:00Z');
             INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at) VALUES ('child','reply-to','root','2026-01-01T00:00:00Z'),('leaf','reply-to','child','2026-01-01T00:00:00Z');"
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
            INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at) VALUES ('root','reply-to','leaf','2026-01-01T00:00:00Z');",
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
            .execute_batch(include_str!("../../../migrations/0001_uuid_native.sql"))
            .unwrap();
        connection
            .execute_batch(
                "INSERT INTO principals(id,display_name,created_at) VALUES ('018f1f59-6e90-7000-8000-000000000001','Writer','2026-01-01T00:00:00Z');
                 INSERT INTO spaces(id,name,access,created_at) VALUES ('space','Space','public','2026-01-01T00:00:00Z');
                 WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<100000)
                 INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at)
                 SELECT printf('record-%06d',n),'space',n,'018f1f59-6e90-7000-8000-000000000001','note','hello','2026-01-01T00:00:00Z' FROM seq;",
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
