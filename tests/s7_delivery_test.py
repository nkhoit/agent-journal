"""S7 delivery CLI acceptance through protected provisioning and real processes."""

import json
import sqlite3
import unittest

import s4_bootstrap_test


class DeliveryTest(s4_bootstrap_test.BootstrapTest):
    def test_delivery_cli_replacement_and_lost_claim(self):
        self.provision()
        self.ticket("ticket")
        self.enroll("ticket", "principal", "delivery")
        self.credential("principal")
        self.credential("delivery")

        def aj(command, *args, credential="delivery", succeeds=True):
            result = self.cli("aj", command, "--endpoint", self.endpoint,
                              "--credential-file", self.directory / credential,
                              *args, succeeds=succeeds)
            return json.loads(result.stdout) if succeeds else None

        registration = aj("adapter-register", "--instance", "installation-example")
        self.assertEqual(registration["generation"], 1)
        self.assertEqual(aj("adapter-register", "--instance", "installation-example")["generation"], 1)
        aj("adapter-heartbeat", "--instance", "installation-example", "--generation", "1")
        self.assertEqual(aj("mailbox-status")["items"][0]["pending"], 0)
        payload = self.directory / "record.json"
        payload.write_text(json.dumps({"kind": "note", "content": "claim body",
                                      "attention": ["principal-example"]}))
        aj("post", "--space", "space-example", "--idempotency-key", "one",
           "--input", payload, credential="principal")
        self.assertEqual(aj("mailbox-status")["items"][0]["pending"], 1)
        status = json.loads(self.admin("mailbox-status", "principal-example").stdout)
        self.assertEqual(status["items"][0]["pending"], 1)
        claim_args = ["--instance", "installation-example", "--generation", "1", "--limit", "20"]
        received = []
        endpoint, thread, failures = self.proxy(
            lambda body: received.append(json.loads(body)), lose_response=True)
        self.cli("aj", "mailbox-claim", "--endpoint", endpoint,
                 "--credential-file", self.directory / "delivery",
                 *claim_args, succeeds=False)
        thread.join(timeout=60)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(len(received[0]["items"]), 1)
        aj("mailbox-claim", *claim_args, succeeds=False)
        # Fake-clock service tests cover natural expiry. This persisted deadline
        # exercises the daemon's expiry path without a thirty-second test sleep.
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE claims SET lease_expires_at='2000-01-01T00:00:00Z'")
        retry = aj("mailbox-claim", *claim_args)
        self.assertEqual(retry["items"], received[0]["items"])
        self.assertNotEqual(retry["claim_id"], received[0]["claim_id"])
        replaced = json.loads(self.admin("adapter-replace", "adapter-example", "1",
                                         "installation-replacement").stdout)
        self.assertEqual(replaced["generation"], 2)
        aj("adapter-heartbeat", "--instance", "installation-example",
           "--generation", "1", succeeds=False)
        self.admin("adapter-replace", "adapter-example", "1",
                   "installation-third", succeeds=False)
        self.ticket("replacement-ticket")
        self.cli("aj", "enroll", "--endpoint", self.endpoint,
                 "--ticket-file", self.directory / "replacement-ticket",
                 "--instance-id", "installation-replacement",
                 "--principal-file", self.directory / "new-principal",
                 "--delivery-file", self.directory / "new-delivery")
        self.credential("new-principal")
        self.credential("new-delivery")
        registration = aj("adapter-register", "--instance", "installation-replacement",
                          credential="new-delivery")
        self.admin("membership-set", "space-example", "principal-example",
                   "false", "false", "false")
        suppressed = aj("mailbox-claim", "--instance", "installation-replacement",
                        "--generation", str(registration["generation"]), "--limit", "1",
                        credential="new-delivery")
        self.assertEqual(suppressed["items"], [])
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("SELECT state FROM delivery_attempts").fetchall(),
                             [("suppressed-revoked",)])
            self.assertEqual(connection.execute("SELECT count(*) FROM delivery_attempts").fetchone()[0], 1)


if __name__ == "__main__":
    suite = unittest.TestSuite([DeliveryTest("test_delivery_cli_replacement_and_lost_claim")])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
