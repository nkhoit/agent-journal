PRAGMA foreign_keys=ON;

-- UUID-native clean-break baseline. This file is only for creation of a fresh
-- central database; existing database files are never migrated or rewritten.
CREATE TABLE schema_contract (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version = 9),
    format TEXT NOT NULL CHECK (format = 'uuid-native-v1')
);
INSERT INTO schema_contract(singleton, version, format) VALUES (1, 9, 'uuid-native-v1');

CREATE TABLE principals (
    id TEXT PRIMARY KEY CHECK (
        length(id) = 36 AND substr(id, 15, 1) = '7'
        AND substr(id, 20, 1) GLOB '[89ab]'
    ),
    display_name TEXT NOT NULL CHECK (length(display_name) BETWEEN 1 AND 128),
    created_at TEXT NOT NULL,
    disabled_at TEXT
, description TEXT CHECK (
    description IS NULL OR length(description) <= 512
), profile_revision INTEGER NOT NULL DEFAULT 1
    CHECK (profile_revision >= 1));
CREATE TABLE adapter_identities (
    adapter_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL UNIQUE REFERENCES principals(id),
    created_at TEXT NOT NULL,
    UNIQUE (adapter_id, principal_id)
);
CREATE TABLE credentials (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    class TEXT NOT NULL CHECK (class IN ('principal-client', 'delivery-adapter')),
    token_hash TEXT NOT NULL UNIQUE,
    adapter_id TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT, enrollment_adapter_id TEXT REFERENCES adapter_identities(adapter_id), instance_id TEXT, replacement_credential_id TEXT REFERENCES credentials(id), revocation_reason TEXT,
    CHECK ((class = 'delivery-adapter' AND adapter_id IS NOT NULL) OR
           (class = 'principal-client' AND adapter_id IS NULL)),
    FOREIGN KEY (adapter_id, principal_id)
        REFERENCES adapter_identities(adapter_id, principal_id)
);
CREATE TABLE registration_receipts (
    token_hash TEXT PRIMARY KEY REFERENCES credentials(token_hash),
    credential_id TEXT NOT NULL UNIQUE REFERENCES credentials(id),
    principal_id TEXT NOT NULL UNIQUE REFERENCES principals(id),
    request_json TEXT NOT NULL CHECK (json_valid(request_json)=1 AND json_type(request_json)='object'),
    response_json TEXT NOT NULL CHECK (json_valid(response_json)=1 AND json_type(response_json)='object'),
    created_at TEXT NOT NULL
);
CREATE TRIGGER registration_receipts_are_immutable
BEFORE UPDATE ON registration_receipts
BEGIN SELECT RAISE(ABORT, 'registration receipts are immutable'); END;
CREATE TRIGGER registration_receipts_are_retained
BEFORE DELETE ON registration_receipts
BEGIN SELECT RAISE(ABORT, 'registration receipts are retained'); END;
CREATE TRIGGER registration_credential_digest_is_immutable
BEFORE UPDATE OF token_hash ON credentials
WHEN EXISTS(SELECT 1 FROM registration_receipts WHERE credential_id=OLD.id)
BEGIN SELECT RAISE(ABORT, 'registration credential digest is immutable'); END;
CREATE TABLE spaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (length(name) BETWEEN 1 AND 128),
    created_at TEXT NOT NULL,
    archived_at TEXT
);
CREATE TABLE memberships (
    space_id TEXT NOT NULL REFERENCES spaces(id) ON DELETE CASCADE,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    can_read INTEGER NOT NULL DEFAULT 0 CHECK (can_read IN (0, 1)),
    can_append INTEGER NOT NULL DEFAULT 0 CHECK (can_append IN (0, 1)),
    can_admin INTEGER NOT NULL DEFAULT 0 CHECK (can_admin IN (0, 1)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (space_id, principal_id)
);
CREATE TABLE records (
    id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL REFERENCES spaces(id),
    space_seq INTEGER NOT NULL CHECK (space_seq > 0),
    author_principal_id TEXT NOT NULL REFERENCES principals(id),
    kind TEXT NOT NULL CHECK (length(kind) BETWEEN 1 AND 128),
    content TEXT NOT NULL CHECK (length(CAST(content AS BLOB)) BETWEEN 1 AND 65536),
    run_id TEXT CHECK (run_id IS NULL OR length(run_id) <= 128),
    routing_key TEXT,
    created_at TEXT NOT NULL,
    source_system TEXT,
    source_id TEXT,
    imported_created_at TEXT,
    UNIQUE (space_id, space_seq),
    UNIQUE (source_system, source_id)
);
CREATE TABLE record_relations (
    source_record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL CHECK (relation_type IN ('reply-to', 'supersedes', 'tombstones', 'refers-to', 'acknowledges')),
    target_record_id TEXT NOT NULL REFERENCES records(id),
    created_at TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0 AND position < 32),
    PRIMARY KEY (source_record_id, relation_type, target_record_id),
    CHECK (source_record_id <> target_record_id)
);
CREATE UNIQUE INDEX one_reply_to_per_record
    ON record_relations(source_record_id)
    WHERE relation_type = 'reply-to';
CREATE TABLE attention (
    record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    recipient_principal_id TEXT NOT NULL REFERENCES principals(id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (record_id, recipient_principal_id)
);
CREATE TRIGGER attention_limit
BEFORE INSERT ON attention
WHEN (SELECT count(*) FROM attention WHERE record_id = NEW.record_id) >= 16
BEGIN
    SELECT RAISE(ABORT, 'attention recipient limit exceeded');
END;
CREATE TABLE idempotency_keys (
    principal_id TEXT NOT NULL REFERENCES principals(id),
    method TEXT NOT NULL CHECK (length(method) BETWEEN 1 AND 16),
    path TEXT NOT NULL CHECK (length(path) BETWEEN 1 AND 2048),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    payload_hash TEXT NOT NULL,
    record_id TEXT NOT NULL REFERENCES records(id),
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (principal_id, method, path, idempotency_key)
);
CREATE TABLE adapter_registrations (
    adapter_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL,
    instance_id TEXT NOT NULL UNIQUE,
    generation INTEGER NOT NULL CHECK (generation > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'draining', 'revoked')),
    last_heartbeat_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (adapter_id, principal_id),
    UNIQUE (adapter_id, principal_id, instance_id, generation),
    UNIQUE (adapter_id, instance_id, generation),
    FOREIGN KEY (adapter_id, principal_id)
        REFERENCES adapter_identities(adapter_id, principal_id)
);
CREATE TABLE enrollment_tickets (
    ticket_hash TEXT PRIMARY KEY CHECK (length(ticket_hash) = 64),
    principal_id TEXT NOT NULL,
    adapter_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT, instance_id TEXT, invalidated_at TEXT,
    FOREIGN KEY (adapter_id, principal_id)
        REFERENCES adapter_identities(adapter_id, principal_id)
);
CREATE TRIGGER enrollment_ticket_consumed_once
BEFORE UPDATE OF consumed_at ON enrollment_tickets
WHEN OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'enrollment ticket already consumed');
END;
CREATE TABLE mailbox_items (
    id TEXT PRIMARY KEY,
    record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    recipient_principal_id TEXT NOT NULL REFERENCES principals(id),
    state TEXT NOT NULL CHECK (state IN ('pending', 'claimed', 'host-accepted', 'adapter-reported-runtime-accepted', 'adapter-reported-retryable-failure', 'route-unavailable', 'adapter-reported-terminal-failure', 'suppressed-revoked')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (record_id, recipient_principal_id)
);
CREATE INDEX mailbox_pending_by_recipient
    ON mailbox_items(recipient_principal_id, state, created_at, id);
CREATE TABLE delivery_attempts (
    attempt_id TEXT PRIMARY KEY,
    mailbox_item_id TEXT NOT NULL REFERENCES mailbox_items(id),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'claimed', 'host-accepted', 'adapter-reported-runtime-accepted', 'adapter-reported-retryable-failure', 'route-unavailable', 'adapter-reported-terminal-failure', 'suppressed-revoked')),
    detail TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (mailbox_item_id, ordinal),
    UNIQUE (mailbox_item_id, attempt_id)
);
CREATE TRIGGER mailbox_initial_attempt
AFTER INSERT ON mailbox_items
BEGIN
    INSERT INTO delivery_attempts(
        attempt_id, mailbox_item_id, ordinal, state, created_at, updated_at
    ) VALUES (
        'initial-' || NEW.id, NEW.id, 1, 'pending', NEW.created_at, NEW.updated_at
    );
END;
CREATE TABLE claims (
    id TEXT PRIMARY KEY,
    adapter_id TEXT NOT NULL,
    principal_id TEXT NOT NULL REFERENCES principals(id),
    instance_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'committed', 'expired', 'cancelled')),
    lease_expires_at TEXT NOT NULL,
    closed_at TEXT,
    created_at TEXT NOT NULL, credential_id TEXT REFERENCES credentials(id),
    CHECK (generation > 0),
    FOREIGN KEY (adapter_id, principal_id)
        REFERENCES adapter_registrations(adapter_id, principal_id)
);
CREATE UNIQUE INDEX one_active_claim_per_adapter_generation
    ON claims(adapter_id, generation)
    WHERE state = 'active';
CREATE TRIGGER claim_binding_must_match_registration
BEFORE INSERT ON claims
WHEN NOT EXISTS (
    SELECT 1 FROM adapter_registrations
    WHERE adapter_id = NEW.adapter_id
      AND principal_id = NEW.principal_id
      AND instance_id = NEW.instance_id
      AND generation = NEW.generation
      AND status = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'claim binding does not match active adapter registration');
END;
CREATE TRIGGER claim_lifecycle_is_one_way
BEFORE UPDATE OF state ON claims
WHEN NOT (
    NEW.state = OLD.state OR
    (OLD.state = 'active' AND NEW.state IN ('committed', 'expired', 'cancelled'))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid claim lifecycle transition');
END;
CREATE TRIGGER closed_claim_needs_timestamp
BEFORE INSERT ON claims
WHEN NEW.state <> 'active' AND NEW.closed_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'closed claim needs closed_at');
END;
CREATE TRIGGER closed_claim_update_needs_timestamp
BEFORE UPDATE OF state ON claims
WHEN NEW.state <> 'active' AND NEW.closed_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'closed claim needs closed_at');
END;
CREATE TABLE claim_items (
    claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    mailbox_item_id TEXT NOT NULL REFERENCES mailbox_items(id),
    attempt_id TEXT NOT NULL,
    PRIMARY KEY (claim_id, mailbox_item_id, attempt_id),
    FOREIGN KEY (mailbox_item_id, attempt_id)
        REFERENCES delivery_attempts(mailbox_item_id, attempt_id)
);
CREATE TRIGGER claim_item_requires_active_claimed_attempt
BEFORE INSERT ON claim_items
WHEN NOT EXISTS (
    SELECT 1
    FROM claims c
    JOIN delivery_attempts a ON a.mailbox_item_id = NEW.mailbox_item_id
                            AND a.attempt_id = NEW.attempt_id
    JOIN mailbox_items m ON m.id = NEW.mailbox_item_id
    WHERE c.id = NEW.claim_id
      AND c.state = 'active'
      AND c.principal_id = m.recipient_principal_id
      AND a.state = 'claimed'
      AND m.state = 'claimed'
)
BEGIN
    SELECT RAISE(ABORT, 'claim item must reference an active claim and claimed attempt');
END;
CREATE TABLE audit_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    actor_principal_id TEXT,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TRIGGER relation_count_limit
BEFORE INSERT ON record_relations
WHEN (SELECT count(*) FROM record_relations WHERE source_record_id = NEW.source_record_id) >= 32
BEGIN
    SELECT RAISE(ABORT, 'relation limit exceeded');
END;
CREATE TRIGGER relation_target_must_be_older_same_space
BEFORE INSERT ON record_relations
BEGIN
    SELECT CASE WHEN
        (SELECT space_id FROM records WHERE id = NEW.source_record_id) IS NULL OR
        (SELECT space_id FROM records WHERE id = NEW.target_record_id) IS NULL OR
        (SELECT space_id FROM records WHERE id = NEW.source_record_id) <>
        (SELECT space_id FROM records WHERE id = NEW.target_record_id) OR
        (SELECT space_seq FROM records WHERE id = NEW.target_record_id) >=
        (SELECT space_seq FROM records WHERE id = NEW.source_record_id)
    THEN RAISE(ABORT, 'relation target must be an older record in the source record space') END;
END;
CREATE TRIGGER records_are_immutable
BEFORE UPDATE ON records
BEGIN
    SELECT RAISE(ABORT, 'records are immutable');
END;
CREATE TRIGGER records_cannot_be_deleted
BEFORE DELETE ON records
BEGIN
    SELECT RAISE(ABORT, 'records cannot be deleted');
END;
CREATE VIRTUAL TABLE records_fts USING fts5(
    record_id UNINDEXED,
    space_id UNINDEXED,
    content,
    author_principal_id UNINDEXED,
    kind UNINDEXED,
    run_id UNINDEXED,
    tokenize = 'unicode61'
)
/* records_fts(record_id,space_id,content,author_principal_id,kind,run_id) */;
CREATE TRIGGER records_fts_insert AFTER INSERT ON records BEGIN
    INSERT INTO records_fts(record_id, space_id, content, author_principal_id, kind, run_id)
    VALUES (NEW.id, NEW.space_id, NEW.content, NEW.author_principal_id, NEW.kind, NEW.run_id);
END;
CREATE TRIGGER records_fts_delete AFTER DELETE ON records BEGIN
    DELETE FROM records_fts WHERE record_id = OLD.id;
END;
CREATE TABLE enrollment_installations (
    adapter_id TEXT PRIMARY KEY REFERENCES adapter_identities(adapter_id),
    instance_id TEXT NOT NULL UNIQUE,
    recovery_authorized INTEGER NOT NULL DEFAULT 0 CHECK (recovery_authorized IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE credential_audit (
    id INTEGER PRIMARY KEY,
    credential_id TEXT NOT NULL REFERENCES credentials(id),
    operation TEXT NOT NULL CHECK (operation IN ('issued', 'rotated', 'revoked', 'recovered')),
    occurred_at TEXT NOT NULL,
    reason TEXT
);
CREATE TABLE journal_secrets (
    name TEXT PRIMARY KEY CHECK (name = 'cursor'),
    secret BLOB NOT NULL CHECK (length(secret) = 32)
);
CREATE INDEX reply_children
    ON record_relations(target_record_id, source_record_id)
    WHERE relation_type = 'reply-to';
CREATE TRIGGER claim_requires_credential
BEFORE INSERT ON claims
WHEN NOT EXISTS (
    SELECT 1 FROM credentials c
    WHERE c.id=NEW.credential_id AND c.class='delivery-adapter'
      AND c.adapter_id=NEW.adapter_id AND c.principal_id=NEW.principal_id
      AND c.instance_id=NEW.instance_id AND c.revoked_at IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'claim requires bound delivery credential');
END;
CREATE TABLE host_custody (
    attempt_id TEXT PRIMARY KEY,
    mailbox_item_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    FOREIGN KEY (claim_id, mailbox_item_id, attempt_id)
        REFERENCES claim_items(claim_id, mailbox_item_id, attempt_id)
);
CREATE TRIGGER custody_requires_active_claim
BEFORE INSERT ON host_custody
WHEN NOT EXISTS (
    SELECT 1 FROM claims c
    JOIN claim_items i ON i.claim_id=c.id
    JOIN delivery_attempts a ON a.attempt_id=i.attempt_id
    JOIN mailbox_items m ON m.id=i.mailbox_item_id
    JOIN adapter_registrations r ON r.adapter_id=c.adapter_id
    JOIN credentials k ON k.id=c.credential_id
    WHERE c.id=NEW.claim_id AND i.mailbox_item_id=NEW.mailbox_item_id
      AND i.attempt_id=NEW.attempt_id AND c.state='active'
      AND a.state='claimed' AND m.state='claimed'
      AND a.ordinal=(SELECT max(ordinal) FROM delivery_attempts WHERE mailbox_item_id=m.id)
      AND c.principal_id=m.recipient_principal_id
      AND r.instance_id=c.instance_id AND r.generation=c.generation AND r.status='active'
      AND k.revoked_at IS NULL
      AND (k.expires_at IS NULL OR julianday(k.expires_at)>julianday(NEW.committed_at))
      AND julianday(c.lease_expires_at)>julianday(NEW.committed_at)
      AND julianday(r.lease_expires_at)>julianday(NEW.committed_at)
)
BEGIN
    SELECT RAISE(ABORT, 'custody requires exact active claim');
END;
CREATE TRIGGER custody_is_immutable BEFORE UPDATE ON host_custody
BEGIN SELECT RAISE(ABORT, 'custody is immutable'); END;
CREATE TRIGGER custody_is_retained BEFORE DELETE ON host_custody
BEGIN SELECT RAISE(ABORT, 'custody is retained'); END;
CREATE TABLE delivery_events (
    event_id TEXT PRIMARY KEY,
    mailbox_item_id TEXT NOT NULL REFERENCES mailbox_items(id),
    attempt_id TEXT NOT NULL,
    adapter_id TEXT NOT NULL REFERENCES adapter_identities(adapter_id),
    instance_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    state TEXT NOT NULL CHECK (state IN ('adapter-reported-runtime-accepted', 'adapter-reported-retryable-failure', 'route-unavailable', 'adapter-reported-terminal-failure')),
    detail_json TEXT CHECK (detail_json IS NULL OR (
        json_valid(detail_json)=1 AND json_type(detail_json)='object'
        AND length(CAST(detail_json AS BLOB))<=4096)),
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    FOREIGN KEY (mailbox_item_id, attempt_id)
        REFERENCES delivery_attempts(mailbox_item_id, attempt_id)
);
CREATE TRIGGER delivery_event_detail_shape
BEFORE INSERT ON delivery_events
WHEN NEW.detail_json IS NOT NULL AND (
    (SELECT count(*) FROM json_each(NEW.detail_json))>32 OR
    EXISTS (SELECT 1 FROM json_each(NEW.detail_json)
        WHERE length(key) NOT BETWEEN 1 AND 128 OR type<>'text' OR length(value)>1024)
)
BEGIN SELECT RAISE(ABORT, 'delivery event detail shape invalid'); END;
CREATE TRIGGER delivery_event_requires_host_accepted_attempt
BEFORE INSERT ON delivery_events
WHEN NOT EXISTS (
    SELECT 1 FROM host_custody h JOIN claims c ON c.id=h.claim_id
    JOIN delivery_attempts a ON a.attempt_id=h.attempt_id
    JOIN mailbox_items m ON m.id=h.mailbox_item_id
    JOIN adapter_registrations r ON r.adapter_id=c.adapter_id
    WHERE h.attempt_id=NEW.attempt_id AND h.mailbox_item_id=NEW.mailbox_item_id
      AND c.adapter_id=NEW.adapter_id AND c.instance_id=NEW.instance_id AND c.generation=NEW.generation
      AND r.instance_id=NEW.instance_id AND r.generation=NEW.generation AND r.status='active'
      AND r.principal_id=m.recipient_principal_id
      AND julianday(r.lease_expires_at)>julianday(NEW.received_at)
      AND a.state IN ('host-accepted','adapter-reported-retryable-failure')
)
BEGIN SELECT RAISE(ABORT, 'delivery event requires exact attempt host custody'); END;
CREATE TRIGGER delivery_event_advances_attempt_state
AFTER INSERT ON delivery_events
BEGIN
    UPDATE delivery_attempts SET state=NEW.state,updated_at=NEW.received_at
    WHERE attempt_id=NEW.attempt_id;
    UPDATE mailbox_items SET state=NEW.state,updated_at=NEW.received_at
    WHERE id=NEW.mailbox_item_id AND NEW.attempt_id=(
        SELECT attempt_id FROM delivery_attempts WHERE mailbox_item_id=NEW.mailbox_item_id
        ORDER BY ordinal DESC LIMIT 1);
END;
CREATE TRIGGER delivery_events_are_immutable BEFORE UPDATE ON delivery_events
BEGIN SELECT RAISE(ABORT, 'delivery events are immutable'); END;
CREATE TRIGGER delivery_events_are_retained BEFORE DELETE ON delivery_events
BEGIN SELECT RAISE(ABORT, 'delivery events are retained'); END;
CREATE TABLE recovery_anchor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    audit_required INTEGER NOT NULL CHECK (audit_required IN (0, 1))
);
-- Handles (including UUID-looking handles) resolve only through this table.
INSERT INTO recovery_anchor VALUES (1, lower(hex(randomblob(32))), 0, 0);

CREATE TABLE principal_names (
    name TEXT PRIMARY KEY CHECK (length(name) BETWEEN 1 AND 128),
    principal_id TEXT NOT NULL REFERENCES principals(id),
    kind TEXT NOT NULL CHECK (kind IN ('current', 'alias')),
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX principal_names_one_current_per_principal
    ON principal_names(principal_id) WHERE kind='current';
CREATE INDEX principal_names_principal ON principal_names(principal_id, kind);
CREATE TABLE profile_idempotency_keys (
    principal_id TEXT NOT NULL REFERENCES principals(id),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    payload_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (principal_id, idempotency_key)
);
CREATE TRIGGER principal_names_cannot_be_deleted
BEFORE DELETE ON principal_names
BEGIN SELECT RAISE(ABORT, 'principal names are permanent'); END;
CREATE TRIGGER principal_alias_is_immutable
BEFORE UPDATE ON principal_names
WHEN OLD.kind='alias'
BEGIN SELECT RAISE(ABORT, 'principal aliases are immutable'); END;
CREATE TRIGGER principal_current_name_transition
BEFORE UPDATE ON principal_names
WHEN OLD.kind='current' AND (
    NEW.name<>OLD.name OR NEW.principal_id<>OLD.principal_id OR NEW.kind<>'alias'
)
BEGIN SELECT RAISE(ABORT, 'principal current name transition is invalid'); END;
CREATE TRIGGER principal_ids_are_immutable
BEFORE UPDATE OF id ON principals
WHEN NEW.id<>OLD.id
BEGIN SELECT RAISE(ABORT, 'principal ids are immutable'); END;
CREATE TRIGGER principal_ids_do_not_shadow_names
BEFORE INSERT ON principals
WHEN EXISTS(SELECT 1 FROM principal_names WHERE name=NEW.id)
BEGIN SELECT RAISE(ABORT, 'principal id shadows a reserved name'); END;
CREATE TRIGGER principal_names_do_not_shadow_ids
BEFORE INSERT ON principal_names
WHEN EXISTS(SELECT 1 FROM principals WHERE id=NEW.name)
BEGIN SELECT RAISE(ABORT, 'principal name shadows a principal id'); END;
