"""Runner coverage and public evidence regression tests."""
import importlib.util
from contextlib import closing
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location(
    "adapter_conformance", Path(__file__).with_name("adapter_conformance.py")
)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def test_checked_in_cases_are_exhaustive(self):
        cases = runner.load_cases(runner.ROOT / "conformance/adapter/scenarios.yaml")
        self.assertEqual(len(cases), 19)
        self.assertEqual({case["id"] for case in cases}, set(runner.SCENARIOS))

    def test_unknown_missing_duplicate_and_changed_contract_fail_closed(self):
        import yaml
        fixture = yaml.safe_load(
            (runner.ROOT / "conformance/adapter/scenarios.yaml").read_text()
        )
        for change in ("unknown", "missing", "duplicate", "expectation", "operations", "extra"):
            value = json.loads(json.dumps(fixture))
            if change == "unknown":
                value["cases"][0]["id"] = "not-implemented"
            elif change == "missing":
                value["cases"].pop()
            elif change == "duplicate":
                value["cases"].append(value["cases"][0])
            elif change == "expectation":
                value["cases"][0]["expectation"] = "different"
            elif change == "operations":
                value["cases"][0]["operation_ids"] = []
            else:
                value["cases"][0]["secret"] = "must-not-be-published"
            with self.subTest(change=change), self.assertRaises(ValueError):
                runner.validate_cases(value)

    def test_redaction_allowlists_fields_and_preserves_identity_relationships(self):
        db = sqlite3.connect(":memory:")
        db.executescript("""
            CREATE TABLE mailbox_items (
                id TEXT, record_id TEXT, recipient_principal_id TEXT,
                state TEXT, current_attempt_id TEXT, secret TEXT);
            INSERT INTO mailbox_items VALUES
                ('private-item', 'private-record', 'private-person',
                 'pending', 'private-attempt', 'credential-that-must-not-escape');
        """)
        result = runner.central_evidence(db)
        db.close()
        serialized = json.dumps(result)
        self.assertNotIn("private-", serialized)
        self.assertNotIn("credential", serialized)
        self.assertEqual(result["mailbox_items"][0]["state"], "pending")
        self.assertEqual(result["mailbox_items"][0]["id"], "mailbox:1")

    def test_no_state_is_not_evidence(self):
        for evidence in ([], [{"kind": "spool", "state": {"attempts": []}}],
                         [{"kind": "runtime", "state": {"acceptances": [1]}}]):
            with self.subTest(evidence=evidence), self.assertRaises(ValueError):
                runner.require_evidence(evidence)

    def test_runtime_acceptance_requires_persisted_custody(self):
        evidence = [
            {"fixture": "fixture:1", "kind": "spool", "checkpoint": "final", "state": {"attempts": [{
                "attempt_id": "attempt:1", "custody_confirmed": False,
            }]}},
            {"fixture": "fixture:1", "kind": "runtime",
             "state": {"acceptances": [{"attempt_id": "attempt:1"}]}},
        ]
        with self.assertRaises(ValueError):
            runner.require_evidence(evidence)

    def test_zero_selected_tests_cannot_certify_a_scenario(self):
        result = SimpleNamespace(returncode=0, stdout="test result: ok. 0 passed;")
        with patch.object(runner, "run_process_tree", return_value=result):
            with self.assertRaises(ValueError):
                runner.run_test("central", "missing_test", Path("unused"), SimpleNamespace())

    def test_timeout_terminates_descendant_before_private_state_cleanup(self):
        for parent_exits in (False, True):
            with self.subTest(parent_exits=parent_exits):
                directory = runner.ROOT / "target" / f"runner-timeout-{uuid.uuid4().hex}"
                directory.mkdir(parents=True)
                # Keep both an inherited output pipe and a private file open.
                descendant = (
                    "import pathlib,time; "
                    "f=pathlib.Path('held-state').open('w'); "
                    "pathlib.Path('ready').write_text('ready'); "
                    "\nwhile True:\n f.write('alive\\n'); f.flush(); time.sleep(0.02)"
                )
                launcher = (
                    "import subprocess,sys,time; "
                    f"subprocess.Popen([sys.executable, '-c', {descendant!r}]); "
                    + ("sys.exit(0)" if parent_exits else "time.sleep(60)")
                )
                try:
                    started = time.monotonic()
                    with self.assertRaises(subprocess.TimeoutExpired):
                        runner.run_process_tree(
                            [sys.executable, "-c", launcher], cwd=directory,
                            env=os.environ.copy(), timeout=2)
                    self.assertLess(time.monotonic() - started, 8)
                    self.assertTrue((directory / "ready").exists())
                    before = (directory / "held-state").read_bytes()
                    time.sleep(0.1)
                    self.assertEqual((directory / "held-state").read_bytes(), before)
                    shutil.rmtree(directory)
                    self.assertFalse(directory.exists())
                finally:
                    if directory.exists():
                        shutil.rmtree(directory)

    def test_fixture_scopes_snapshots_runtime_and_crash_custody(self):
        directory = runner.ROOT / "target" / f"runner-fixtures-{uuid.uuid4().hex}"
        try:
            for name, custodied in (("private-first", True), ("private-second", False)):
                fixture = directory / name
                fixture.mkdir(parents=True)
                item = {
                    "attempt_id": "attempt-a", "mailbox_item_id": "item-a",
                    "record_id": "record-a", "claim_id": "claim-a", "instance_id": "host-a",
                    "generation": 1, "custody_confirmed": custodied,
                    "injection_state": "Accepted", "record": {},
                }
                with closing(sqlite3.connect(fixture / "spool.db")) as db, db:
                    db.execute("PRAGMA application_id = 0x414A5350")
                    db.execute("CREATE TABLE attempts (attempt_id TEXT, item TEXT)")
                    db.execute("INSERT INTO attempts VALUES (?, ?)",
                               ("attempt-a", json.dumps(item)))
                shutil.copyfile(fixture / "spool.db", fixture / "before-recovery.db")
                (fixture / "acceptances.jsonl").write_text(json.dumps({
                    "envelope": {key: item[key] for key in
                                 ("attempt_id", "mailbox_item_id", "record_id")},
                    "route": {"runtime_target": "private-route", "enabled": True},
                    "rendered": "private-rendering",
                }) + "\n", encoding="utf-8")
                (fixture / "crash-evidence.json").write_text(json.dumps({
                    "phase": "runtime-accepted", "child_terminated": True,
                    "accepted_before_crash": 1, "recovery_injections": 1,
                }), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "no persisted custody"):
                runner.collect_evidence(directory)
            with closing(sqlite3.connect(directory / "private-second" / "spool.db")) as db, db:
                item["custody_confirmed"] = True
                db.execute("UPDATE attempts SET item = ?", (json.dumps(item),))
            evidence = runner.collect_evidence(directory)
            self.assertNotIn("private-", json.dumps(evidence))
            self.assertEqual({entry["fixture"] for entry in evidence},
                             {"fixture:1", "fixture:2"})
            for token in ("fixture:1", "fixture:2"):
                entries = [entry for entry in evidence if entry["fixture"] == token]
                self.assertEqual(sorted(entry["kind"] for entry in entries),
                                 ["crash", "runtime", "spool", "spool"])
                attempts = [row["attempt_id"] for entry in entries
                            for row in entry["state"].get(
                                "attempts", entry["state"].get("acceptances", []))]
                self.assertEqual(set(attempts), {"attempt:1"})
        finally:
            shutil.rmtree(directory)

    def test_custody_cannot_cross_execution_scope(self):
        evidence = [
            {"execution": 1, "fixture": "fixture:1", "kind": "spool",
             "state": {"attempts": [{"attempt_id": "attempt:1", "custody_confirmed": True}]}},
            {"execution": 2, "fixture": "fixture:1", "kind": "runtime",
             "state": {"acceptances": [{"attempt_id": "attempt:1"}]}},
        ]
        with self.assertRaisesRegex(ValueError, "no persisted custody"):
            runner.require_evidence(evidence)


if __name__ == "__main__":
    unittest.main()
