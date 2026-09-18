-- Historical claims predate credential binding and must never be resumed.
ALTER TABLE claims ADD COLUMN credential_id TEXT REFERENCES credentials(id);
UPDATE delivery_attempts SET state='pending',updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
WHERE state='claimed' AND attempt_id IN (
    SELECT i.attempt_id FROM claim_items i JOIN claims c ON c.id=i.claim_id WHERE c.state='active'
);
UPDATE mailbox_items SET state='pending',updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
WHERE state='claimed' AND id IN (
    SELECT i.mailbox_item_id FROM claim_items i JOIN claims c ON c.id=i.claim_id WHERE c.state='active'
);
UPDATE claims SET state='cancelled',closed_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE state='active';
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
INSERT INTO schema_migrations(version, applied_at) VALUES (5, 'migration-time');
