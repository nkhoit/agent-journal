PRAGMA foreign_keys=ON;

-- UUID-native clean-break baseline. This file is only for creation of a fresh
-- central database; existing database files are never migrated or rewritten.
CREATE TABLE schema_contract (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version = 12),
    format TEXT NOT NULL CHECK (format = 'uuid-native-v1')
);
INSERT INTO schema_contract(singleton, version, format) VALUES (1, 12, 'uuid-native-v1');

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
CREATE TABLE credentials (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    class TEXT NOT NULL CHECK (class = 'principal-client'),
    token_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    replacement_credential_id TEXT REFERENCES credentials(id),
    revocation_reason TEXT
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
    access TEXT NOT NULL CHECK (access = 'public'),
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
CREATE TABLE inbox_sequences (
    recipient_principal_id TEXT PRIMARY KEY REFERENCES principals(id),
    last_seq INTEGER NOT NULL CHECK (typeof(last_seq) = 'integer' AND last_seq > 0)
);
CREATE TRIGGER inbox_sequence_is_monotonic
BEFORE UPDATE ON inbox_sequences
WHEN NEW.recipient_principal_id IS NOT OLD.recipient_principal_id OR NEW.last_seq < OLD.last_seq
BEGIN SELECT RAISE(ABORT, 'inbox sequence cannot decrease'); END;
CREATE TRIGGER inbox_sequence_is_retained
BEFORE DELETE ON inbox_sequences
BEGIN SELECT RAISE(ABORT, 'inbox sequence is retained'); END;
CREATE TABLE mailbox_items (
    id TEXT PRIMARY KEY,
    record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    recipient_principal_id TEXT NOT NULL REFERENCES principals(id),
    created_at TEXT NOT NULL,
    recipient_seq INTEGER NOT NULL CHECK (typeof(recipient_seq) = 'integer' AND recipient_seq > 0),
    acknowledged_at TEXT,
    UNIQUE (recipient_principal_id, recipient_seq),
    UNIQUE (record_id, recipient_principal_id)
);
CREATE INDEX inbox_unacknowledged ON mailbox_items(recipient_principal_id, recipient_seq)
    WHERE acknowledged_at IS NULL;
CREATE TRIGGER inbox_identity_is_immutable
BEFORE UPDATE OF id,record_id,recipient_principal_id,recipient_seq,created_at ON mailbox_items
WHEN NEW.id IS NOT OLD.id OR NEW.record_id IS NOT OLD.record_id
 OR NEW.recipient_principal_id IS NOT OLD.recipient_principal_id
 OR NEW.recipient_seq IS NOT OLD.recipient_seq OR NEW.created_at IS NOT OLD.created_at
BEGIN SELECT RAISE(ABORT, 'inbox identity is immutable'); END;
CREATE TRIGGER inbox_first_acknowledgment_is_immutable
BEFORE UPDATE OF acknowledged_at ON mailbox_items
WHEN OLD.acknowledged_at IS NOT NULL AND NEW.acknowledged_at IS NOT OLD.acknowledged_at
BEGIN SELECT RAISE(ABORT, 'first acknowledgment is immutable'); END;
CREATE TRIGGER inbox_items_are_retained
BEFORE DELETE ON mailbox_items
BEGIN SELECT RAISE(ABORT, 'inbox items are retained'); END;
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
CREATE TABLE recovery_anchor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    audit_required INTEGER NOT NULL CHECK (audit_required IN (0, 1)),
    inbox_epoch INTEGER NOT NULL DEFAULT 0 CHECK (inbox_epoch >= 0)
);
-- Handles (including UUID-looking handles) resolve only through this table.
INSERT INTO recovery_anchor VALUES (1, lower(hex(randomblob(32))), 0, 0, 0);

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
