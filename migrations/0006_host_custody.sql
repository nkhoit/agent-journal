-- Custody is an immutable receipt for an exact credential-bound claim item.
-- Legacy host-accepted rows have no provable receipt; retain them, but require
-- explicit requeue rather than manufacturing custody for historical attempts.
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

-- Historical events cannot reference the mutable registration generation:
-- replacement must fence future operations without destroying event history.
DROP TRIGGER delivery_event_detail_shape;
DROP TRIGGER delivery_event_principal_must_match_recipient;
DROP TRIGGER delivery_event_requires_host_accepted_attempt;
DROP TRIGGER delivery_event_advances_attempt_state;
ALTER TABLE delivery_events RENAME TO legacy_delivery_events;
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
INSERT INTO delivery_events SELECT * FROM legacy_delivery_events;
DROP TABLE legacy_delivery_events;

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
INSERT INTO schema_migrations(version, applied_at) VALUES (6, 'migration-time');
