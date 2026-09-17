#!/usr/bin/env python3
"""Executable SQLite contract checks for the public migration.

This deliberately uses only Python's stdlib sqlite3 so the migration contract is
checked in CI without selecting or hiding behind a Go SQLite driver.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "0001_initial.sql"
NOW = "2026-09-16T18:00:00Z"


def expect_integrity(connection: sqlite3.Connection, sql: str, parameters: tuple = ()) -> None:
    try:
        connection.execute(sql, parameters)
    except sqlite3.IntegrityError:
        return
    raise AssertionError(f"integrity constraint did not reject: {sql}")


def make_record(connection: sqlite3.Connection, record_id: str, space: str, seq: int, content: str = "content") -> None:
    connection.execute(
        """
        INSERT INTO records(
            id, space_id, space_seq, author_principal_id, kind, content, created_at
        ) VALUES (?, ?, ?, 'p1', 'message', ?, ?)
        """,
        (record_id, space, seq, content, NOW),
    )


def main() -> None:
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(MIGRATION.read_text(encoding="utf-8"))
    assert connection.execute("PRAGMA foreign_keys").fetchone() == (1,)

    for principal_id in [f"p{i}" for i in range(1, 31)]:
        connection.execute(
            "INSERT INTO principals(id, display_name, created_at) VALUES (?, ?, ?)",
            (principal_id, principal_id, NOW),
        )
    connection.execute("INSERT INTO spaces(id, name, created_at) VALUES ('s1', 'Space 1', ?)", (NOW,))
    connection.execute("INSERT INTO spaces(id, name, created_at) VALUES ('s2', 'Space 2', ?)", (NOW,))

    # Relation vocabulary, backward-only same-space edges, and immutability.
    make_record(connection, "rel-target", "s1", 1)
    make_record(connection, "rel-source", "s1", 2)
    make_record(connection, "other-space", "s2", 1)
    exact_types = ("reply-to", "supersedes", "tombstones", "refers-to", "acknowledges")
    for relation_type in exact_types:
        connection.execute(
            "INSERT INTO record_relations(source_record_id, relation_type, target_record_id, created_at) VALUES ('rel-source', ?, 'rel-target', ?)",
            (relation_type, NOW),
        )
    assert connection.execute("SELECT relation_type FROM record_relations ORDER BY relation_type").fetchall() == sorted(
        (relation,) for relation in exact_types
    )
    expect_integrity(
        connection,
        "INSERT INTO record_relations VALUES ('rel-source', 'references', 'rel-target', ?)",
        (NOW,),
    )
    expect_integrity(
        connection,
        "INSERT INTO record_relations VALUES ('rel-source', 'refers-to', 'other-space', ?)",
        (NOW,),
    )
    expect_integrity(
        connection,
        "INSERT INTO record_relations VALUES ('rel-target', 'refers-to', 'rel-source', ?)",
        (NOW,),
    )
    expect_integrity(
        connection,
        "INSERT INTO record_relations VALUES ('rel-source', 'refers-to', 'missing', ?)",
        (NOW,),
    )
    expect_integrity(connection, "UPDATE records SET content = 'changed' WHERE id = 'rel-source'")
    expect_integrity(connection, "DELETE FROM records WHERE id = 'rel-source'")

    # The relation and attention limits remain executable constraints.
    make_record(connection, "limit-source", "s1", 100)
    for seq in range(50, 83):
        make_record(connection, f"limit-target-{seq}", "s1", seq)
    for seq in range(50, 82):
        connection.execute(
            "INSERT INTO record_relations VALUES ('limit-source', 'refers-to', ?, ?)",
            (f"limit-target-{seq}", NOW),
        )
    expect_integrity(
        connection,
        "INSERT INTO record_relations VALUES ('limit-source', 'refers-to', 'limit-target-82', ?)",
        (NOW,),
    )
    make_record(connection, "attention-source", "s1", 101)
    for principal_id in [f"p{i}" for i in range(2, 18)]:
        connection.execute(
            "INSERT INTO attention(record_id, recipient_principal_id, created_at) VALUES ('attention-source', ?, ?)",
            (principal_id, NOW),
        )
    expect_integrity(
        connection,
        "INSERT INTO attention(record_id, recipient_principal_id, created_at) VALUES ('attention-source', 'p18', ?)",
        (NOW,),
    )

    # Idempotency scope includes method and path, not just principal and key.
    for method, path in (("POST", "/v1/spaces/s1/records"), ("PUT", "/v1/other")):
        connection.execute(
            """
            INSERT INTO idempotency_keys(
                principal_id, method, path, idempotency_key, payload_hash,
                record_id, response_json, created_at
            ) VALUES ('p1', ?, ?, 'same-key', 'hash', 'rel-target', '{}', ?)
            """,
            (method, path, NOW),
        )
    expect_integrity(
        connection,
        """
        INSERT INTO idempotency_keys(
            principal_id, method, path, idempotency_key, payload_hash,
            record_id, response_json, created_at
        ) VALUES ('p1', 'POST', '/v1/spaces/s1/records', 'same-key', 'other', 'rel-target', '{}', ?)
        """,
        (NOW,),
    )

    # A mailbox insert models append's atomic obligation and creates attempt 1.
    connection.execute(
        "INSERT INTO mailbox_items(id, record_id, recipient_principal_id, state, created_at, updated_at) VALUES ('item-1', 'rel-source', 'p2', 'pending', ?, ?)",
        (NOW, NOW),
    )
    assert connection.execute(
        "SELECT state FROM mailbox_items WHERE id = 'item-1'"
    ).fetchone() == ("pending",)
    assert connection.execute(
        "SELECT attempt_id, ordinal, state FROM delivery_attempts WHERE mailbox_item_id = 'item-1'"
    ).fetchone() == ("initial-item-1", 1, "pending")

    # Provisioning creates an adapter identity; enrollment is possible before registration.
    connection.execute(
        "INSERT INTO adapter_identities(adapter_id, principal_id, created_at) VALUES ('adapter-1', 'p2', ?)",
        (NOW,),
    )
    connection.execute(
        "INSERT INTO enrollment_tickets(ticket_hash, principal_id, adapter_id, expires_at) VALUES (?, 'p2', 'adapter-1', ?)",
        ("a" * 64, "2026-09-16T18:05:00Z"),
    )
    assert connection.execute("SELECT count(*) FROM adapter_registrations").fetchone() == (0,)

    # Adapter registration is created only after the destination supplies instance_id.
    connection.execute(
        """
        INSERT INTO adapter_registrations(
            adapter_id, principal_id, instance_id, generation, status,
            last_heartbeat_at, lease_expires_at, created_at
        ) VALUES ('adapter-1', 'p2', 'install-1', 1, 'active', ?, ?, ?)
        """,
        (NOW, NOW, NOW),
    )
    connection.execute(
        """
        INSERT INTO claims(
            id, adapter_id, principal_id, instance_id, generation, state,
            lease_expires_at, created_at
        ) VALUES ('claim-1', 'adapter-1', 'p2', 'install-1', 1, 'active', ?, ?)
        """,
        (NOW, NOW),
    )
    expect_integrity(
        connection,
        """
        INSERT INTO claims(
            id, adapter_id, principal_id, instance_id, generation, state,
            lease_expires_at, created_at
        ) VALUES ('claim-duplicate', 'adapter-1', 'p2', 'install-1', 1, 'active', ?, ?)
        """,
        (NOW, NOW),
    )
    expect_integrity(
        connection,
        """
        INSERT INTO claims(
            id, adapter_id, principal_id, instance_id, generation, state,
            lease_expires_at, created_at
        ) VALUES ('claim-wrong-binding', 'adapter-1', 'p3', 'install-1', 1, 'active', ?, ?)
        """,
        (NOW, NOW),
    )
    connection.execute("UPDATE delivery_attempts SET state = 'claimed' WHERE attempt_id = 'initial-item-1'")
    connection.execute("UPDATE mailbox_items SET state = 'claimed', updated_at = ? WHERE id = 'item-1'", (NOW,))
    connection.execute("INSERT INTO claim_items VALUES ('claim-1', 'item-1', 'initial-item-1')")
    expect_integrity(
        connection,
        "INSERT INTO claim_items VALUES ('claim-1', 'item-1', 'not-this-attempt')",
    )
    connection.execute("UPDATE claims SET state = 'expired', closed_at = ? WHERE id = 'claim-1'", (NOW,))
    connection.execute("UPDATE delivery_attempts SET state = 'pending' WHERE attempt_id = 'initial-item-1'")
    connection.execute("UPDATE mailbox_items SET state = 'pending', updated_at = ? WHERE id = 'item-1'", (NOW,))
    connection.execute(
        """
        INSERT INTO claims(
            id, adapter_id, principal_id, instance_id, generation, state,
            lease_expires_at, created_at
        ) VALUES ('claim-2', 'adapter-1', 'p2', 'install-1', 1, 'active', ?, ?)
        """,
        (NOW, NOW),
    )
    connection.execute("UPDATE delivery_attempts SET state = 'claimed' WHERE attempt_id = 'initial-item-1'")
    connection.execute("UPDATE mailbox_items SET state = 'claimed', updated_at = ? WHERE id = 'item-1'", (NOW,))
    connection.execute("INSERT INTO claim_items VALUES ('claim-2', 'item-1', 'initial-item-1')")
    connection.execute("UPDATE claims SET state = 'committed', closed_at = ? WHERE id = 'claim-2'", (NOW,))
    expect_integrity(connection, "UPDATE claims SET state = 'active' WHERE id = 'claim-2'")
    connection.execute("UPDATE delivery_attempts SET state = 'pending' WHERE attempt_id = 'initial-item-1'")
    connection.execute("UPDATE mailbox_items SET state = 'pending', updated_at = ? WHERE id = 'item-1'", (NOW,))
    connection.execute(
        "INSERT INTO delivery_attempts(attempt_id, mailbox_item_id, ordinal, state, created_at, updated_at) VALUES ('attempt-2', 'item-1', 2, 'pending', ?, ?)",
        (NOW, NOW),
    )
    assert connection.execute(
        "SELECT attempt_id, ordinal, state FROM delivery_attempts WHERE mailbox_item_id = 'item-1' ORDER BY ordinal"
    ).fetchall() == [("initial-item-1", 1, "pending"), ("attempt-2", 2, "pending")]
    connection.execute(
        "INSERT INTO mailbox_items(id, record_id, recipient_principal_id, state, created_at, updated_at) VALUES ('item-2', 'limit-source', 'p2', 'pending', ?, ?)",
        (NOW, NOW),
    )
    expect_integrity(
        connection,
        "INSERT INTO delivery_events(event_id, mailbox_item_id, attempt_id, adapter_id, instance_id, generation, state, occurred_at, received_at) VALUES ('event-pending', 'item-2', 'initial-item-2', 'adapter-1', 'install-1', 1, 'adapter-reported-runtime-accepted', ?, ?)",
        (NOW, NOW),
    )
    connection.execute("UPDATE delivery_attempts SET state = 'host-accepted' WHERE attempt_id = 'initial-item-2'")
    connection.execute("UPDATE mailbox_items SET state = 'host-accepted', updated_at = ? WHERE id = 'item-2'", (NOW,))
    valid_detail = json.dumps({"receipt": "queued-é"}, ensure_ascii=False, separators=(",", ":"))
    connection.execute(
        "INSERT INTO delivery_events(event_id, mailbox_item_id, attempt_id, adapter_id, instance_id, generation, state, detail_json, occurred_at, received_at) VALUES ('event-1', 'item-2', 'initial-item-2', 'adapter-1', 'install-1', 1, 'adapter-reported-runtime-accepted', ?, ?, ?)",
        (valid_detail, NOW, NOW),
    )
    assert connection.execute(
        "SELECT state FROM delivery_attempts WHERE attempt_id = 'initial-item-2'"
    ).fetchone() == ("adapter-reported-runtime-accepted",)
    expect_integrity(
        connection,
        "INSERT INTO delivery_events(event_id, mailbox_item_id, attempt_id, adapter_id, instance_id, generation, state, occurred_at, received_at) VALUES ('event-1', 'item-2', 'initial-item-2', 'adapter-1', 'install-1', 1, 'adapter-reported-runtime-accepted', ?, ?)",
        (NOW, NOW),
    )
    expect_integrity(
        connection,
        "INSERT INTO delivery_events(event_id, mailbox_item_id, attempt_id, adapter_id, instance_id, generation, state, occurred_at, received_at) VALUES ('event-invalid-state', 'item-2', 'initial-item-2', 'adapter-1', 'install-1', 1, 'claimed', ?, ?)",
        (NOW, NOW),
    )

    # Claims and telemetry cannot cross the mailbox recipient principal.
    connection.execute("INSERT INTO mailbox_items(id, record_id, recipient_principal_id, state, created_at, updated_at) VALUES ('item-cross-principal', 'rel-source', 'p3', 'pending', ?, ?)", (NOW, NOW))
    connection.execute("UPDATE delivery_attempts SET state = 'claimed' WHERE attempt_id = 'initial-item-cross-principal'")
    connection.execute("UPDATE mailbox_items SET state = 'claimed', updated_at = ? WHERE id = 'item-cross-principal'", (NOW,))
    connection.execute(
        "INSERT INTO claims(id, adapter_id, principal_id, instance_id, generation, state, lease_expires_at, created_at) VALUES ('claim-cross-principal', 'adapter-1', 'p2', 'install-1', 1, 'active', ?, ?)",
        (NOW, NOW),
    )
    expect_integrity(connection, "INSERT INTO claim_items VALUES ('claim-cross-principal', 'item-cross-principal', 'initial-item-cross-principal')")
    connection.execute("UPDATE delivery_attempts SET state = 'host-accepted' WHERE attempt_id = 'initial-item-cross-principal'")
    connection.execute("UPDATE mailbox_items SET state = 'host-accepted', updated_at = ? WHERE id = 'item-cross-principal'", (NOW,))
    expect_integrity(
        connection,
        "INSERT INTO delivery_events(event_id, mailbox_item_id, attempt_id, adapter_id, instance_id, generation, state, occurred_at, received_at) VALUES ('event-cross-principal', 'item-cross-principal', 'initial-item-cross-principal', 'adapter-1', 'install-1', 1, 'adapter-reported-runtime-accepted', ?, ?)",
        (NOW, NOW),
    )
    connection.execute("UPDATE claims SET state = 'cancelled', closed_at = ? WHERE id = 'claim-cross-principal'", (NOW,))

    # The serialized detail limit is measured in UTF-8 bytes, not characters.
    connection.execute(
        "INSERT INTO mailbox_items(id, record_id, recipient_principal_id, state, created_at, updated_at) VALUES ('item-detail-limit', 'rel-target', 'p2', 'host-accepted', ?, ?)",
        (NOW, NOW),
    )
    connection.execute("UPDATE delivery_attempts SET state = 'host-accepted' WHERE attempt_id = 'initial-item-detail-limit'")
    oversized_detail = json.dumps(
        {"base": "界" * 1024, "padding": "a" * 1024},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    expect_integrity(
        connection,
        "INSERT INTO delivery_events(event_id, mailbox_item_id, attempt_id, adapter_id, instance_id, generation, state, detail_json, occurred_at, received_at) VALUES ('event-detail-too-large', 'item-detail-limit', 'initial-item-detail-limit', 'adapter-1', 'install-1', 1, 'adapter-reported-runtime-accepted', ?, ?, ?)",
        (oversized_detail, NOW, NOW),
    )

    # Ticket storage is hash-only, bound to the adapter's principal, and one-use.
    ticket_columns = {row[1] for row in connection.execute("PRAGMA table_info(enrollment_tickets)")}
    assert ticket_columns == {"ticket_hash", "principal_id", "adapter_id", "expires_at", "consumed_at"}
    ticket_hash = "b" * 64
    connection.execute(
        "INSERT INTO enrollment_tickets(ticket_hash, principal_id, adapter_id, expires_at) VALUES (?, 'p2', 'adapter-1', ?)",
        (ticket_hash, "2026-09-16T18:05:00Z"),
    )
    expect_integrity(
        connection,
        "INSERT INTO enrollment_tickets(ticket_hash, principal_id, adapter_id, expires_at) VALUES (?, 'p2', 'adapter-1', ?)",
        (ticket_hash, "2026-09-16T18:05:00Z"),
    )
    expect_integrity(
        connection,
        "INSERT INTO enrollment_tickets(ticket_hash, principal_id, adapter_id, expires_at) VALUES (?, 'p3', 'adapter-1', ?)",
        ("b" * 64, "2026-09-16T18:05:00Z"),
    )
    connection.execute(
        "UPDATE enrollment_tickets SET consumed_at = ? WHERE ticket_hash = ?",
        (NOW, ticket_hash),
    )
    expect_integrity(
        connection,
        "UPDATE enrollment_tickets SET consumed_at = ? WHERE ticket_hash = ?",
        ("2026-09-16T18:01:00Z", ticket_hash),
    )

    # FTS remains populated by the record trigger.
    make_record(connection, "fts-record", "s1", 400, "findable migration needle")
    assert connection.execute(
        "SELECT record_id FROM records_fts WHERE records_fts MATCH 'needle'"
    ).fetchone() == ("fts-record",)
    print("migration contract passed")


if __name__ == "__main__":
    main()
