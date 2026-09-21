#!/usr/bin/env python3
"""The inbox baseline contains no legacy delivery authority or state."""
import sqlite3
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RETIRED = {
    "adapter_identities",
    "adapter_registrations",
    "enrollment_tickets",
    "enrollment_installations",
    "delivery_attempts",
    "claims",
    "claim_items",
    "host_custody",
    "delivery_events",
}


class DeliveryRetirement(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.executescript(
            (ROOT / "migrations" / "0001_uuid_native.sql").read_text(encoding="utf-8")
        )

    def test_clean_break_has_no_delivery_tables(self):
        self.assertEqual(
            self.db.execute("SELECT version FROM schema_contract").fetchone(), (12,)
        )
        tables = {
            row[0]
            for row in self.db.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        }
        self.assertFalse(RETIRED & tables)

    def test_delivery_tables_are_absent(self):
        tables = {
            row[0]
            for row in self.db.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        }
        self.assertEqual(RETIRED & tables, set())

    def test_inbox_identity_has_receipts_not_attempt_state(self):
        columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(mailbox_items)")
        }
        self.assertEqual(
            columns,
            {
                "id", "record_id", "recipient_principal_id", "recipient_seq",
                "created_at", "acknowledged_at",
            },
        )

    def test_credentials_have_no_adapter_binding(self):
        columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(credentials)")
        }
        self.assertFalse(
            {"adapter_id", "instance_id", "enrollment_adapter_id"} & columns
        )

    def test_delivery_class_cannot_be_stored(self):
        principal = "018f1f59-6e90-7000-8000-000000000001"
        self.db.execute(
            "INSERT INTO principals(id,display_name,created_at) VALUES (?,'Alpha','2026-01-01T00:00:00Z')",
            (principal,),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "INSERT INTO credentials(id,principal_id,class,token_hash,created_at) "
                "VALUES ('retired',?,'delivery-adapter','test-digest','2026-01-01T00:00:00Z')",
                (principal,),
            )
        self.db.execute(
            "INSERT INTO credentials(id,principal_id,class,token_hash,created_at) "
            "VALUES ('current',?,'principal-client','test-digest','2026-01-01T00:00:00Z')",
            (principal,),
        )


if __name__ == "__main__":
    unittest.main()
