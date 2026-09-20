"""S5 product acceptance through protected S4 provisioning and real aj processes."""

import json
import sqlite3
import unittest

import s4_bootstrap_test


class RecordsTest(s4_bootstrap_test.BootstrapTest):
    # Reuse bootstrap helpers, not the S4 test cases.
    def test_journal_cli_and_lost_response(self):
        self.provision()
        self.ticket("ticket")
        self.enroll("ticket", "principal", "delivery")
        self.credential("principal")
        self.credential("delivery")
        common = ["--endpoint", self.endpoint, "--credential-file",
                  str(self.directory / "principal")]

        def aj(command, *args, succeeds=True):
            result = self.cli("aj", command, *common, *args, succeeds=succeeds)
            return json.loads(result.stdout) if succeeds else None

        self.assertEqual(aj("me")["principal"]["handle"], "principal-example")
        self.assertEqual(len(aj("spaces")["items"]), 1)
        payload = self.directory / "record.json"
        payload.write_text(json.dumps({"kind": "note", "content": "hello 界",
                                      "attention": ["principal-example"]}))
        args = ["--space", "space-example", "--idempotency-key", "post-key",
                "--input", str(payload)]
        first = aj("post", *args)
        self.assertFalse(first["replayed"])
        self.assertEqual(first["record"]["seq"], 1)
        self.assertEqual(aj("get", "--record", first["record"]["id"]), first["record"])
        self.assertEqual(aj("list", "--space", "space-example")["items"], [first["record"]])
        self.assertEqual(aj("post", *args)["record"], first["record"])
        search = aj("search", "--space", "space-example", "--q", "hello",
                    "--order", "seq", "--limit", "1")
        self.assertEqual(search["items"][0]["id"], first["record"]["id"])
        self.assertEqual(search["consistency"], "deterministic")
        self.assertEqual(aj("thread", "--record", first["record"]["id"])["items"],
                         [first["record"]])
        aj("search", "--space", "space-example", "--q", '"', succeeds=False)
        aj("thread", "--record", first["record"]["id"], "--limit", "101",
           succeeds=False)

        endpoint, thread, failures = self.proxy(lambda _: None, lose_response=True,
                                                 expected_status=201)
        self.cli("aj", "post", "--endpoint", endpoint, "--credential-file",
                 self.directory / "principal", "--space", "space-example",
                 "--idempotency-key", "lost-key", "--input", payload, succeeds=False)
        thread.join(timeout=60)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        replay = aj("post", "--space", "space-example", "--idempotency-key",
                    "lost-key", "--input", str(payload))
        self.assertFalse(replay["replayed"])
        self.assertEqual(replay["record"]["seq"], 2)

        page = aj("list", "--space", "space-example", "--limit", "1")
        second = aj("list", "--space", "space-example", "--cursor",
                    page["next_cursor"], "--limit", "1")
        self.assertEqual(second["items"], [replay["record"]])
        self.assertIsNone(second["next_cursor"])
        ranked = aj("search", "--space", "space-example", "--q", "hello",
                    "--limit", "1")
        self.assertEqual(ranked["consistency"], "best-effort")
        ranked_next = aj("search", "--space", "space-example", "--q", "hello",
                        "--limit", "1", "--cursor", ranked["next_cursor"])
        self.assertNotEqual(ranked["items"][0]["id"], ranked_next["items"][0]["id"])
        self.assertIsNone(ranked_next["next_cursor"])
        aj("search", "--space", "space-example", "--q", "hello", "--order",
           "seq", "--cursor", ranked["next_cursor"], succeeds=False)
        payload.write_text('{"kind":"note","content":"different"}')
        aj("post", *args, succeeds=False)
        with sqlite3.connect(self.database) as connection:
            for table in ["records", "attention", "mailbox_items",
                          "delivery_attempts", "idempotency_keys"]:
                self.assertEqual(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 2)
        payload.write_text(json.dumps({
            "kind": "note", "content": "reply",
            "relations": [{"type": "reply-to", "record_id": first["record"]["id"]}],
        }))
        reply = aj("post", "--space", "space-example", "--idempotency-key",
                   "reply-key", "--input", str(payload))
        tree = aj("thread", "--record", reply["record"]["id"], "--limit", "1")
        self.assertEqual(tree["items"], [first["record"]])
        next_tree = aj("thread", "--record", reply["record"]["id"], "--limit",
                       "1", "--cursor", tree["next_cursor"])
        self.assertEqual(next_tree["items"], [reply["record"]])
        self.assertIsNone(next_tree["next_cursor"])
        self.admin("membership-set", "space-example", "principal-example",
                   "false", "false", "false")
        aj("get", "--record", first["record"]["id"], succeeds=False)
        aj("list", "--space", "space-example", succeeds=False)
        aj("search", "--space", "space-example", "--q", "hello", succeeds=False)
        aj("thread", "--record", first["record"]["id"], succeeds=False)
        self.assertEqual(aj("spaces")["items"], [])


if __name__ == "__main__":
    suite = unittest.TestSuite([RecordsTest("test_journal_cli_and_lost_response")])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
