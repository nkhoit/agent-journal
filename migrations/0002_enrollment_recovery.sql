-- Keep installation ownership after recovery, and track both credential lineages.
ALTER TABLE credentials ADD COLUMN enrollment_adapter_id TEXT REFERENCES adapter_identities(adapter_id);
ALTER TABLE credentials ADD COLUMN instance_id TEXT;
ALTER TABLE credentials ADD COLUMN replacement_credential_id TEXT REFERENCES credentials(id);
ALTER TABLE credentials ADD COLUMN revocation_reason TEXT;

CREATE TABLE enrollment_installations (
    adapter_id TEXT PRIMARY KEY REFERENCES adapter_identities(adapter_id),
    instance_id TEXT NOT NULL UNIQUE,
    recovery_authorized INTEGER NOT NULL DEFAULT 0 CHECK (recovery_authorized IN (0, 1)),
    created_at TEXT NOT NULL
);

-- Existing registrations retain ownership rather than becoming available for takeover.
INSERT INTO enrollment_installations(adapter_id, instance_id, created_at)
SELECT adapter_id, instance_id, created_at FROM adapter_registrations;

-- A principal has at most one adapter. Conservatively include legacy client
-- credentials in its recovery scope so an upgrade cannot leave a live secret.
UPDATE credentials
SET enrollment_adapter_id = (
        SELECT adapter_id FROM adapter_registrations WHERE principal_id = credentials.principal_id
    ),
    instance_id = (
        SELECT instance_id FROM adapter_registrations WHERE principal_id = credentials.principal_id
    )
WHERE EXISTS (
    SELECT 1 FROM adapter_registrations WHERE principal_id = credentials.principal_id
);

ALTER TABLE enrollment_tickets ADD COLUMN instance_id TEXT;
ALTER TABLE enrollment_tickets ADD COLUMN invalidated_at TEXT;

CREATE TABLE credential_audit (
    id INTEGER PRIMARY KEY,
    credential_id TEXT NOT NULL REFERENCES credentials(id),
    operation TEXT NOT NULL CHECK (operation IN ('issued', 'rotated', 'revoked', 'recovered')),
    occurred_at TEXT NOT NULL,
    reason TEXT
);

INSERT INTO schema_migrations(version, applied_at) VALUES (2, 'migration-time');
