CREATE INDEX reply_children
    ON record_relations(target_record_id, source_record_id)
    WHERE relation_type = 'reply-to';

INSERT INTO schema_migrations(version, applied_at) VALUES (4, 'migration-time');
