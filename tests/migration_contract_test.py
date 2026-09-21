#!/usr/bin/env python3
"""Executable contract for the sole UUID-native SQLite baseline."""
from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "0001_uuid_native.sql"
NOW = "2026-09-16T18:00:00Z"
P1 = "018f1f59-6e90-7000-8000-000000000001"
P2 = "018f1f59-6e90-7000-8000-000000000002"


def rejects(connection: sqlite3.Connection, sql: str, parameters: tuple = ()) -> None:
    try:
        connection.execute(sql, parameters)
    except sqlite3.IntegrityError:
        return
    raise AssertionError(f"integrity constraint did not reject: {sql}")


def main() -> None:
    assert [path.name for path in (ROOT / "migrations").glob("*.sql")] == [
        "0001_uuid_native.sql"
    ]
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(MIGRATION.read_text(encoding="utf-8"))

    assert connection.execute("SELECT version, format FROM schema_contract").fetchone() == (
        11,
        "uuid-native-v1",
    )
    assert connection.execute("SELECT count(*) FROM schema_contract").fetchone() == (1,)
    rejects(connection, "UPDATE schema_contract SET version=8")

    connection.execute(
        "INSERT INTO principals(id,display_name,created_at) VALUES (?,?,?)",
        (P1, "Writer", NOW),
    )
    connection.execute(
        "INSERT INTO principal_names(name,principal_id,kind,created_at) VALUES ('writer',?,'current',?)",
        (P1, NOW),
    )
    connection.execute(
        "INSERT INTO principal_names(name,principal_id,kind,created_at) VALUES ('writer-old',?,'alias',?)",
        (P1, NOW),
    )
    rejects(
        connection,
        "INSERT INTO principals(id,display_name,created_at) VALUES ('writer','bad',?)",
        (NOW,),
    )
    rejects(
        connection,
        "INSERT INTO principal_names(name,principal_id,kind,created_at) VALUES ('another',?,'current',?)",
        (P1, NOW),
    )
    rejects(connection, "UPDATE principal_names SET principal_id=? WHERE name='writer'", (P2,))
    rejects(connection, "UPDATE principals SET id=? WHERE id=?", (P2, P1))
    connection.execute("UPDATE principal_names SET kind='alias' WHERE name='writer'")
    connection.execute(
        "INSERT INTO principal_names(name,principal_id,kind,created_at) VALUES ('writer-renamed',?,'current',?)",
        (P1, NOW),
    )
    assert connection.execute(
        "SELECT name FROM principal_names WHERE principal_id=? AND kind='current'", (P1,)
    ).fetchone() == ("writer-renamed",)
    rejects(connection, "DELETE FROM principal_names WHERE name='writer'")
    connection.execute(
        "INSERT INTO profile_idempotency_keys(principal_id,idempotency_key,payload_hash,response_json,created_at) VALUES (?,'profile-key','hash','{}',?)",
        (P1, NOW),
    )
    connection.execute(
        "INSERT INTO credentials(id,principal_id,class,token_hash,created_at) VALUES ('registration-credential',?,'principal-client',?,?)",
        (P1, "a" * 64, NOW),
    )
    connection.execute(
        "INSERT INTO registration_receipts(token_hash,credential_id,principal_id,request_json,response_json,created_at) VALUES (?,'registration-credential',?,'{}','{}',?)",
        ("a" * 64, P1, NOW),
    )
    rejects(
        connection,
        "UPDATE credentials SET token_hash=? WHERE id='registration-credential'",
        ("b" * 64,),
    )
    rejects(connection, "DELETE FROM registration_receipts")

    connection.execute(
        "INSERT INTO principals(id,display_name,created_at) VALUES (?,?,?)",
        (P2, "Reader", NOW),
    )
    connection.execute(
        "INSERT INTO principal_names(name,principal_id,kind,created_at) VALUES ('reader',?,'current',?)",
        (P2, NOW),
    )
    connection.execute("INSERT INTO spaces(id,name,access,created_at) VALUES ('s1','Space','public',?)", (NOW,))
    rejects(connection, "INSERT INTO spaces(id,name,created_at) VALUES ('missing','Missing',?)", (NOW,))
    for access in ("private", "", "PUBLIC", "unknown", None):
        rejects(connection, "INSERT INTO spaces(id,name,access,created_at) VALUES ('bad','Bad',?,?)", (access, NOW))
        rejects(connection, "UPDATE spaces SET access=? WHERE id='s1'", (access,))
    for principal, append in [(P1, 1), (P2, 0)]:
        connection.execute(
            "INSERT INTO memberships(space_id,principal_id,can_read,can_append,can_admin,created_at) VALUES ('s1',?,?,?,0,?)",
            (principal, 1, append, NOW),
        )

    connection.execute(
        "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at) VALUES ('record-1','s1',1,?,'note','findable UUID baseline',?)",
        (P1, NOW),
    )
    connection.execute(
        "INSERT INTO attention(record_id,recipient_principal_id,created_at) VALUES ('record-1',?,?)",
        (P2, NOW),
    )
    connection.execute(
        "INSERT INTO inbox_sequences VALUES (?,1)", (P2,)
    )
    connection.execute(
        "INSERT INTO mailbox_items(id,record_id,recipient_principal_id,state,created_at,updated_at,recipient_seq) VALUES ('mailbox-1','record-1',?,'pending',?,?,1)",
        (P2, NOW, NOW),
    )
    assert connection.execute(
        "SELECT attempt_id,ordinal,state FROM delivery_attempts WHERE mailbox_item_id='mailbox-1'"
    ).fetchone() == ("initial-mailbox-1", 1, "pending")
    rejects(connection, "UPDATE mailbox_items SET recipient_seq=2")
    rejects(connection, "DELETE FROM mailbox_items")
    rejects(connection, "UPDATE inbox_sequences SET last_seq=0")
    rejects(connection, "UPDATE inbox_sequences SET last_seq=9223372036854775807+1")
    connection.execute("UPDATE mailbox_items SET acknowledged_at=? WHERE id='mailbox-1'", (NOW,))
    rejects(connection, "UPDATE mailbox_items SET acknowledged_at=NULL")
    rejects(connection, "UPDATE mailbox_items SET acknowledged_at='2027-01-01T00:00:00Z'")
    connection.execute("UPDATE mailbox_items SET state='host-accepted'")
    assert connection.execute("SELECT acknowledged_at FROM mailbox_items").fetchone() == (NOW,)
    rejects(connection, "UPDATE records SET content='changed' WHERE id='record-1'")
    rejects(connection, "DELETE FROM records WHERE id='record-1'")

    connection.execute(
        "INSERT INTO records(id,space_id,space_seq,author_principal_id,kind,content,created_at) VALUES ('record-2','s1',2,?,'note','child',?)",
        (P1, NOW),
    )
    connection.execute(
        "INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at) VALUES ('record-2','reply-to','record-1',?)",
        (NOW,),
    )
    rejects(
        connection,
        "INSERT INTO record_relations(source_record_id,relation_type,target_record_id,created_at) VALUES ('record-1','reply-to','record-2',?)",
        (NOW,),
    )
    assert connection.execute(
        "SELECT record_id FROM records_fts WHERE records_fts MATCH 'findable'"
    ).fetchone() == ("record-1",)

    anchor = connection.execute(
        "SELECT singleton,length(journal_id),revision,audit_required FROM recovery_anchor"
    ).fetchone()
    assert anchor == (1, 64, 0, 0)
    rejects(connection, "UPDATE recovery_anchor SET revision=-1")
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    print("UUID-native baseline contract passed")


if __name__ == "__main__":
    main()
