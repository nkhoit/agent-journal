"""Prepared append acceptance with real private files, CLI processes and service."""

import http.client
import json
import os
import sqlite3
import subprocess
import threading
import time
import unittest
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import s4_bootstrap_test


class PreparedPostTest(s4_bootstrap_test.BootstrapTest):
    def setUp(self):
        super().setUp()
        self.provision()
        self.state = self.directory / "post-state"
        self.input = self.directory / "post-input"
        self.input.write_text(json.dumps({"kind": "note", "content": "private content",
                                         "attention": ["principal-example"]}))

    def post(self, *args, endpoint=None, credential="principal", succeeds=True):
        return self.cli("aj", "post", "--endpoint", endpoint or self.endpoint,
                        "--credential-file", self.directory / credential,
                        "--state-file", self.state, *args, succeeds=succeeds)

    def fresh(self, *args, **kwargs):
        return self.post("--space", "space-example", "--input", self.input,
                         *args, **kwargs)

    def counts(self):
        with closing(sqlite3.connect(self.database)) as connection:
            return [connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                    for table in ["records", "mailbox_items", "idempotency_keys"]]

    def forwarding(self, before=None, after=None):
        """Keep endpoint stable across retries; callbacks mark exact crash boundaries."""
        owner = self
        failures, calls = [], []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def forward(self):
                try:
                    body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                    calls.append((self.command, self.path))
                    if self.command == "POST":
                        frozen = json.loads(owner.state.read_text())
                        owner.assertEqual(owner.state.stat().st_mode & 0o777, 0o600)
                        owner.assertEqual(frozen["request"]["content"], json.loads(body)["content"])
                        owner.assertEqual(frozen["key"], self.headers["Idempotency-Key"])
                        if before and before(body):
                            self.close_connection = True
                            return
                    host = owner.endpoint.removeprefix("http://")
                    upstream = http.client.HTTPConnection(host, timeout=35)
                    upstream.request(self.command, self.path, body, dict(self.headers))
                    response = upstream.getresponse()
                    payload = response.read()
                    upstream.close()
                    if self.command == "POST" and after and after(payload):
                        self.close_connection = True
                        return
                    self.send_response(response.status)
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # Expected after a killed client.
                except Exception as error:
                    failures.append(type(error).__name__)
                    self.close_connection = True

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}", failures, calls

    def test_lost_response_freezes_title_and_resumes_without_source(self):
        lose = [True]

        def after(_):
            return lose.pop() if lose else False

        endpoint, failures, _ = self.forwarding(after=after)
        self.fresh("--title", "  Frozen title\u2003", endpoint=endpoint, succeeds=False)
        pending = json.loads(self.state.read_text())
        self.assertEqual(pending["request"]["title"], "  Frozen title\u2003")
        self.assertEqual(pending["outcome"]["status"], "pending")
        self.assertEqual(len(pending["key"]), 64)
        self.assertNotIn(self.credential("principal")["secret"], self.state.read_text())
        self.input.write_text('{"kind":"note","content":"changed"}')
        self.fresh(endpoint=endpoint, succeeds=False)
        self.input.unlink()
        result = json.loads(self.post(endpoint=endpoint).stdout)
        self.assertTrue(result["replayed"])
        self.assertEqual(result["record"]["title"], "Frozen title")
        self.assertEqual(self.counts(), [1, 1, 1])
        self.assertFalse(failures)

    def test_process_kills_before_send_and_after_commit(self):
        for boundary in ["before", "after"]:
            with self.subTest(boundary=boundary):
                self.state = self.directory / f"state-{boundary}"
                entered, release = threading.Event(), threading.Event()
                first = [True]

                def pause(_):
                    if not first:
                        return False
                    first.pop()
                    entered.set()
                    self.assertTrue(release.wait(60))
                    return boundary == "before"

                endpoint, failures, _ = self.forwarding(
                    before=pause if boundary == "before" else None,
                    after=pause if boundary == "after" else None)
                child = subprocess.Popen([
                    str(self.bin / "aj"), "post", "--endpoint", endpoint,
                    "--credential-file", str(self.directory / "principal"),
                    "--state-file", str(self.state), "--space", "space-example",
                    "--input", str(self.input)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    self.assertTrue(entered.wait(60))
                    child.kill()
                    child.communicate(timeout=60)
                    pending = self.state.read_bytes()
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.communicate(timeout=60)
                    release.set()
                self.assertEqual(json.loads(pending)["outcome"]["status"], "pending")
                # Disable the boundary for the explicit resume.
                entered.clear()
                result = json.loads(self.post(endpoint=endpoint).stdout)
                self.assertEqual(result["replayed"], boundary == "after")
                self.assertFalse(failures)
        self.assertEqual(self.counts(), [2, 2, 2])

    def test_receipt_write_failure_keeps_pending_operation(self):
        once = [True]

        def deny_receipt(_):
            if once:
                once.pop()
                self.directory.chmod(0o500)
            return False

        endpoint, failures, _ = self.forwarding(after=deny_receipt)
        try:
            failed = self.fresh(endpoint=endpoint, succeeds=False)
            self.assertIn(b"append committed but receipt persistence failed", failed.stderr)
            pending = json.loads(self.state.read_text())
            self.assertEqual(pending["outcome"]["status"], "pending")
        finally:
            self.directory.chmod(0o700)
        result = json.loads(self.post(endpoint=endpoint).stdout)
        self.assertTrue(result["replayed"])
        self.assertEqual(self.counts(), [1, 1, 1])
        self.assertFalse(failures)

    def test_relations_order_and_stdin_are_frozen(self):
        endpoint, failures, _ = self.forwarding(before=lambda _: True)
        parent = json.loads(self.cli(
            "aj", "post", "--endpoint", self.endpoint,
            "--credential-file", self.directory / "principal", "--space", "space-example",
            "--idempotency-key", "parent", "--input", self.input).stdout)["record"]["id"]
        request = {"kind": "note", "content": "private content", "title": "  raw  ",
                   "relations": [{"type": "refers-to", "record_id": parent},
                                 {"type": "reply-to", "record_id": parent}]}
        args = [str(self.bin / "aj"), "post", "--endpoint", endpoint,
                "--credential-file", str(self.directory / "principal"),
                "--state-file", str(self.state), "--space", "space-example", "--input", "-"]
        first = subprocess.run(args, input=json.dumps(request).encode(), capture_output=True, timeout=60)
        self.assertEqual(first.returncode, 1)
        original = self.state.read_bytes()
        request["relations"].reverse()
        second = subprocess.run(args, input=json.dumps(request).encode(), capture_output=True, timeout=60)
        self.assertEqual(second.returncode, 1)
        self.assertIn(b"does not match input or title", second.stderr)
        self.assertEqual(self.state.read_bytes(), original)
        self.input.write_text(json.dumps(request))
        self.fresh(endpoint=endpoint, succeeds=False)
        self.assertEqual(self.state.read_bytes(), original)
        self.assertFalse(failures)

    def test_mismatches_and_corrupt_state_fail_before_contacting_endpoint(self):
        endpoint, failures, calls = self.forwarding(before=lambda _: True)
        self.fresh("--title", " raw ", "--idempotency-key", "explicit",
                   endpoint=endpoint, succeeds=False)
        original = self.state.read_bytes()
        for args in [("--space", "other"), ("--idempotency-key", "other"),
                     ("--title", "raw")]:
            count = len(calls)
            self.post(*args, endpoint=endpoint, succeeds=False)
            self.assertEqual(len(calls), count)
            self.assertEqual(self.state.read_bytes(), original)
        self.post(endpoint="http://127.0.0.1:9", succeeds=False)
        state = json.loads(original)
        for damaged in [b'{', b'[]', original.replace(b'"version":1', b'"version":2'),
                        json.dumps({**state, "secret": "unwanted"}).encode(),
                        json.dumps({**state, "principal_id": "principal-example"}).encode(),
                        json.dumps({**state, "key": ""}).encode(),
                        original.replace(b'"version":1', b'"version":1,"version":1')]:
            self.state.write_bytes(damaged)
            count = len(calls)
            self.post(endpoint=endpoint, succeeds=False)
            self.assertEqual(len(calls), count)
            self.assertEqual(self.state.read_bytes(), damaged)
        self.assertFalse(failures)
        self.assertEqual(self.counts(), [0, 0, 0])

    def test_rotation_and_forged_credential_identity(self):
        blocked = [True]
        endpoint, failures, calls = self.forwarding(before=lambda _: blocked[0])
        self.fresh(endpoint=endpoint, succeeds=False)
        original = self.state.read_bytes()
        old = self.credential("principal")
        self.admin("credential-rotate", old["credential_id"], self.directory / "rotated")
        self.credential("rotated")
        self.post(endpoint=endpoint, succeeds=False)  # Revoked old credential.
        blocked[0] = False
        self.post(endpoint=endpoint, credential="rotated")
        self.assertEqual(calls[-1][0], "POST")
        original = self.state.read_bytes()
        self.cli("aj", "register", "--endpoint", self.endpoint,
                 "--state-file", self.directory / "other", "--handle", "principal-similar",
                 "--display-name", "Example")
        other = self.credential("other")
        other["principal"] = old["principal"]  # Credential DTO cannot prove ownership.
        (self.directory / "other").write_text(json.dumps(other))
        count = len(calls)
        self.post(endpoint=endpoint, credential="other", succeeds=False)
        self.assertEqual(calls[count:], [("GET", "/v1/me")])
        self.assertEqual(self.state.read_bytes(), original)
        self.assertFalse(failures)
        self.assertEqual(self.counts(), [1, 1, 1])

    def test_private_files_and_validation_prevent_append(self):
        endpoint, failures, calls = self.forwarding()
        for payload in ['{"kind":"note","content":""}',
                        '{"kind":"note","content":"x","unknown":1}',
                        '{"kind":"note","content":"x","content":"y"}']:
            self.input.write_text(payload)
            self.fresh(endpoint=endpoint, succeeds=False)
            self.assertFalse(self.state.exists())
            self.assertFalse(calls)
        self.input.write_text('{"kind":"note","content":"private content"}')
        target = self.directory / "untouched"
        target.write_text("private content")
        target.chmod(0o600)
        for path in [self.state, self.directory / ".post-state.lock"]:
            path.unlink(missing_ok=True)
            path.symlink_to(target.resolve())
            self.fresh(endpoint=endpoint, succeeds=False)
            self.assertEqual(target.read_text(), "private content")
            path.unlink()
        self.state.write_text('{}')
        self.state.chmod(0o644)
        self.fresh(endpoint=endpoint, succeeds=False)
        self.state.unlink()
        unsafe = self.directory / "unsafe"
        unsafe.mkdir(mode=0o755)
        unsafe.chmod(0o755)
        self.state = unsafe / "post"
        self.fresh(endpoint=endpoint, succeeds=False)
        self.assertFalse(calls)
        self.assertFalse(failures)
        self.assertEqual(self.counts(), [0, 0, 0])

    def test_concurrent_first_use_and_resumes_publish_one_operation(self):
        entered, release = threading.Event(), threading.Event()

        def pause(_):
            entered.set()
            self.assertTrue(release.wait(60))
            return False

        endpoint, failures, _ = self.forwarding(before=pause)
        args = [str(self.bin / "aj"), "post", "--endpoint", endpoint,
                "--credential-file", str(self.directory / "principal"),
                "--state-file", str(self.state), "--space", "space-example",
                "--input", str(self.input)]
        children = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)]
        try:
            self.assertTrue(entered.wait(60))
            frozen = json.loads(self.state.read_text())
            children += [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE),
                         subprocess.Popen(args + ["--idempotency-key", "conflicting"],
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)]
            release.set()
            results = [child.communicate(timeout=60) for child in children]
            self.assertEqual([child.returncode for child in children], [0, 0, 1])
            self.assertEqual(json.loads(results[0][0]), json.loads(results[1][0]))
            self.assertEqual(json.loads(self.state.read_text())["key"], frozen["key"])
        finally:
            release.set()
            for child in children:
                if child.poll() is None:
                    child.kill()
                    child.communicate(timeout=60)
        self.assertEqual(self.counts(), [1, 1, 1])
        self.assertFalse(failures)

    def test_stdin_large_body_and_historical_completed_receipt(self):
        payload = {"kind": "note", "content": "\u0001" * 65536}
        result = subprocess.run([
            str(self.bin / "aj"), "post", "--endpoint", self.endpoint,
            "--credential-file", str(self.directory / "principal"),
            "--state-file", str(self.state), "--space", "space-example", "--input", "-"],
            input=json.dumps(payload).encode(), capture_output=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreater(self.state.stat().st_size, 16384)
        completed = self.state.read_bytes()
        again = self.post()
        self.assertEqual(json.loads(again.stdout), json.loads(result.stdout))
        self.assertIn(b"historical", again.stderr)
        self.assertEqual(self.state.read_bytes(), completed)
        self.assertEqual(self.counts(), [1, 0, 1])

    def test_completed_receipt_after_approved_older_backup_is_historical(self):
        def stop():
            self.process.terminate()
            self.process.wait(timeout=60)

        def start(database):
            self.trace_file.seek(0)
            self.trace_file.truncate()
            self.process = subprocess.Popen([
                str(self.bin / "journald"), "--database", str(database),
                "--recovery-audit", str(self.audit), "--admin-socket", str(self.socket),
                "--listen", self.endpoint.removeprefix("http://")],
                stdout=subprocess.DEVNULL, stderr=self.trace_file,
                env={**os.environ, "JOURNAL_LOG_LEVEL": "info"})
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                self.assertIsNone(self.process.poll(), "service exited during restart")
                if any(json.loads(line).get("fields", {}).get("event") == "service_ready"
                       for line in self.trace.read_text().splitlines()):
                    return
                time.sleep(0.01)
            self.fail("restart readiness deadline exceeded")

        stop()
        backup = self.directory / "older.db"
        self.cli("journal-recover", "backup", self.database, self.audit, backup)
        start(self.database)
        posted = json.loads(self.fresh().stdout)
        completed_path, completed = self.state, self.state.read_bytes()
        self.state = self.directory / "pending-state"
        endpoint, failures, _ = self.forwarding(after=lambda _: True)
        self.fresh(endpoint=endpoint, succeeds=False)
        pending_path, pending = self.state, self.state.read_bytes()
        self.assertEqual(self.counts(), [2, 2, 2])
        stop()
        restored = self.directory / "restored.db"
        approval = self.directory / "approval.json"
        restore = subprocess.run([str(self.bin / "journal-recover"), "restore",
                                  *map(str, [self.database, self.audit, backup, restored, approval]),
                                  "--clients-quiesced"], capture_output=True, timeout=60)
        self.assertEqual(restore.returncode, 0, restore.stderr)
        review = json.loads(approval.read_text())
        review["inventory_complete"] = True
        review["accepted_record_loss"] = True
        approval.write_text(json.dumps(review))
        self.cli("journal-recover", "reopen", restored, self.audit, approval)
        self.database = restored
        start(restored)
        self.admin("principal-recover", self.principal_id, self.directory / "recovered")
        self.credential("recovered")
        self.assertEqual(self.counts(), [0, 0, 0])
        self.state = completed_path
        historical = self.post(credential="recovered")
        self.assertEqual(json.loads(historical.stdout), posted)
        self.assertIn(b"historical", historical.stderr)
        self.assertEqual(self.request("/v1/records/" + posted["record"]["id"],
                                      self.credential("recovered")["secret"])[0], 404)
        self.assertEqual(completed_path.read_bytes(), completed)
        self.assertEqual(pending_path.read_bytes(), pending)
        self.assertEqual(self.counts(), [0, 0, 0])
        self.assertFalse(failures)


if __name__ == "__main__":
    names = [name for name in PreparedPostTest.__dict__ if name.startswith("test_")]
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(PreparedPostTest(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
