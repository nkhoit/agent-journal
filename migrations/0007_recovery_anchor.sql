-- The external audit is a separate recovery unit. A restored anchor must not
-- silently become the head of that surviving audit.
CREATE TABLE recovery_anchor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    audit_required INTEGER NOT NULL CHECK (audit_required IN (0, 1))
);
INSERT INTO recovery_anchor VALUES (1, lower(hex(randomblob(32))), 0, 0);
INSERT INTO schema_migrations(version, applied_at) VALUES (7, 'migration-time');
