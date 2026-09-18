-- Preserve submitted relation order independently of SQLite row identifiers.
ALTER TABLE record_relations ADD COLUMN position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0 AND position < 32);

-- A server-generated MAC secret persists so pagination survives process restart.
CREATE TABLE journal_secrets (
    name TEXT PRIMARY KEY CHECK (name = 'cursor'),
    secret BLOB NOT NULL CHECK (length(secret) = 32)
);

INSERT INTO schema_migrations(version, applied_at) VALUES (3, 'migration-time');
