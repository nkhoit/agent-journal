"""Black-box acceptance for journal-recover argument and gate behavior."""

import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest


PROCESS_TIMEOUT = 60


@unittest.skipUnless(os.name == "posix", "journal-recover requires Unix file semantics")
class RecoveryCliTest(unittest.TestCase):
    def setUp(self):
        self.bin_dir = Path(os.environ.get("AJ_BIN_DIR", "target/debug")).resolve()
        self.directory = Path(tempfile.mkdtemp(prefix="recovery-cli-", dir="target"))
        self.database = self.directory / "journal.db"
        self.audit = self.directory / "audit.db"
        self.socket = self.directory / "admin.sock"
        self.trace = self.directory / "trace"
        self.trace_file = self.trace.open("wb")
        self.service = subprocess.Popen(
            [
                str(self.bin_dir / "journald"),
                "--database",
                str(self.database),
                "--recovery-audit",
                str(self.audit),
                "--listen",
                "127.0.0.1:0",
                "--admin-socket",
                str(self.socket),
            ],
            stdout=subprocess.DEVNULL,
            stderr=self.trace_file,
            env={**os.environ, "JOURNAL_LOG_LEVEL": "info"},
        )
        self.addCleanup(self.cleanup)
        deadline = time.monotonic() + PROCESS_TIMEOUT
        while time.monotonic() < deadline:
            if self.service.poll() is not None:
                self.fail(f"journald exited before ready: {self.trace.read_text()}")
            fields = []
            try:
                fields = [
                    json.loads(line).get("fields", {})
                    for line in self.trace.read_text().splitlines()
                ]
            except (OSError, UnicodeError, json.JSONDecodeError):
                pass
            if any(field.get("event") == "service_ready" for field in fields):
                self.stop_service()
                with sqlite3.connect(self.database) as connection:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                return
            time.sleep(0.01)
        self.fail("journald readiness deadline exceeded")

    def cleanup(self):
        self.stop_service()
        trace_file = getattr(self, "trace_file", None)
        if trace_file is not None:
            trace_file.close()
        if hasattr(self, "directory"):
            shutil.rmtree(self.directory, ignore_errors=True)

    def stop_service(self):
        service = getattr(self, "service", None)
        if service is not None and service.poll() is None:
            service.terminate()
            try:
                service.wait(timeout=PROCESS_TIMEOUT)
            except subprocess.TimeoutExpired:
                service.kill()
                service.wait(timeout=PROCESS_TIMEOUT)

    def recovery(self, *arguments):
        binary = Path(
            os.environ.get("JOURNAL_RECOVER_BIN", self.bin_dir / "journal-recover")
        )
        return subprocess.run(
            [str(binary), *map(str, arguments)],
            cwd=self.directory,
            capture_output=True,
            timeout=PROCESS_TIMEOUT,
            check=False,
        )

    def assert_failure(self, *arguments):
        result = self.recovery(*arguments)
        self.assertNotEqual(
            result.returncode,
            0,
            f"recovery CLI unexpectedly succeeded: {arguments!r}",
        )
        self.assertEqual(result.stdout, b"")
        self.assertIn(b"journal-recover:", result.stderr)
        return result

    def test_usage_dispatch_and_negative_gate(self):
        # This is the non-vacuous gate: a CLI that accepts an invalid command
        # or malformed arguments would make the acceptance job fail here.
        usage = self.assert_failure()
        self.assertIn(b"usage: journal-recover", usage.stderr)
        invalid = self.assert_failure("not-a-command", self.database, self.audit)
        self.assertIn(b"invalid recovery command", invalid.stderr)
        self.assert_failure(
            "backup", self.database, self.audit, self.directory / "backup.db", "extra"
        )

        backup = self.directory / "backup.db"
        result = self.recovery("backup", self.database, self.audit, backup)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout, b"")
        self.assertTrue(backup.exists())
        for suffix in ("-journal", "-wal", "-shm"):
            self.assertFalse(Path(f"{self.database}{suffix}").exists(), suffix)

        restore_arguments = (
            "restore",
            backup,
            self.directory / "restored.db",
            self.directory / "restored-approval.json",
        )
        self.assert_failure(*restore_arguments)

        result = self.recovery("close", self.database, self.audit)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout, b"")

        approval = self.directory / "approval.json"
        self.assert_failure("reconcile", self.database, self.audit, approval)
        result = self.recovery(
            "reconcile",
            self.database,
            self.audit,
            approval,
            "--clients-quiesced",
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertTrue(approval.exists())

        # Reopen must reject the operator template until its inventory and
        # accepted-loss attestations are explicitly reviewed.
        reopen = self.assert_failure("reopen", self.database, self.audit, approval)
        self.assertIn(b"recovery", reopen.stderr)

        reviewed = self.directory / "reviewed-approval.json"
        approval_document = json.loads(approval.read_text())
        approval_document["inventory_complete"] = True
        approval_document["accepted_record_loss"] = True
        reviewed.write_text(json.dumps(approval_document))
        reviewed.chmod(0o600)
        result = self.recovery("reopen", self.database, self.audit, reviewed)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout, b"")


if __name__ == "__main__":
    unittest.main(verbosity=2)
