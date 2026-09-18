"""Unix black-box bootstrap, one-time secrets, and explicit recovery."""

import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import threading
import time
import unittest
import urllib.error
import urllib.request


# Allow the client's 35-second request deadline plus bounded process startup.
PROCESS_TIMEOUT = 60


@unittest.skipUnless(os.name == "posix", "protected administration requires Unix")
class BootstrapTest(unittest.TestCase):
    def setUp(self):
        self.outputs = []
        self.arguments = []
        self.directory = Path("target") / f"s4-bootstrap-{os.getpid()}"
        self.directory.mkdir(mode=0o700, parents=True)
        self.socket = self.directory / "admin.sock"
        self.database = self.directory / "journal.db"
        self.bin = Path(os.environ.get("AJ_BIN_DIR", "target/debug")).resolve()
        self.trace = self.directory / "trace"
        self.trace_file = self.trace.open("wb")
        self.process = subprocess.Popen(
            [str(self.bin / "journald"), "--database", str(self.database),
             "--listen", "127.0.0.1:0", "--admin-socket", str(self.socket)],
            stdout=subprocess.DEVNULL, stderr=self.trace_file,
            env={**os.environ, "JOURNAL_LOG_LEVEL": "info"},
        )
        self.addCleanup(self.cleanup)
        deadline = time.monotonic() + PROCESS_TIMEOUT
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.fail("journald exited before ready")
            for line in self.trace.read_text().splitlines():
                event = json.loads(line).get("fields", {})
                if event.get("event") == "service_ready":
                    self.endpoint = "http://" + event["public_address"]
                    return
            time.sleep(0.01)
        self.fail("journald readiness deadline exceeded")

    def cleanup(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=30)
        self.trace_file.close()
        try:
            secrets = getattr(self, "secrets", [])
            for secret in secrets + [hashlib.sha256(value.encode()).hexdigest() for value in secrets]:
                for output in self.outputs:
                    self.assertTrue(secret.encode() not in output, "secret appeared in CLI output")
                for arguments in self.arguments:
                    self.assertTrue(secret not in arguments, "secret appeared in argv")
                self.assertTrue(secret not in self.trace.read_text(), "secret appeared in traces")
        finally:
            shutil.rmtree(self.directory)

    def admin(self, *args, socket_path=None, succeeds=True):
        return self.cli("aj-admin", "--socket", str(socket_path or self.socket),
                        *args, succeeds=succeeds)

    def cli(self, binary, *args, succeeds=True):
        result = subprocess.run([str(self.bin / binary), *map(str, args)],
                                capture_output=True, timeout=PROCESS_TIMEOUT)
        self.outputs.extend([result.stdout, result.stderr])
        self.arguments.append(" ".join(map(str, args)))
        self.assertEqual(result.returncode == 0, succeeds,
                         "CLI exit status differs from expected outcome")
        for secret in getattr(self, "secrets", []):
            self.assertNotIn(secret.encode(), result.stdout)
            self.assertNotIn(secret.encode(), result.stderr)
            self.assertNotIn(secret, " ".join(map(str, args)))
        return result

    def enroll(self, ticket, principal, delivery, endpoint=None, succeeds=True):
        result = self.cli("aj", "enroll", "--endpoint", endpoint or self.endpoint,
                         "--ticket-file", self.directory / ticket,
                         "--instance-id", "installation-example",
                         "--principal-file", self.directory / principal,
                         "--delivery-file", self.directory / delivery,
                         succeeds=succeeds)
        self.assertEqual(result.stdout, b"")
        return result

    def request(self, path, token=None, method="GET", body=None):
        headers = {"Content-Type": "application/json"} if body is not None else {}
        if token:
            headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(self.endpoint + path, data=body,
                                         headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def provision(self):
        self.secrets = []
        self.admin("principal-create", "principal-example", "Example")
        self.admin("space-create", "space-example", "Example")
        self.admin("membership-set", "space-example", "principal-example",
                   "true", "true", "false")
        self.admin("adapter-provision", "principal-example", "adapter-example")

    def ticket(self, name):
        result = self.admin("ticket-create", "principal-example", "adapter-example",
                            "60", str(self.directory / name))
        self.assertEqual(result.stdout, b"")
        value = (self.directory / name).read_text()
        self.secrets.append(value)
        return value

    def credential(self, name):
        path = self.directory / name
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        value = json.loads(path.read_text())
        self.secrets.append(value["secret"])
        return value

    def proxy(self, callback, unix=False, lose_response=False, expected_status=200):
        """Forward one exchange, then fail at a deterministic post-commit boundary."""
        listener = socket.socket(socket.AF_UNIX if unix else socket.AF_INET)
        address = str(self.directory / "proxy.sock") if unix else ("127.0.0.1", 0)
        listener.bind(address)
        listener.listen(1)
        listener.settimeout(PROCESS_TIMEOUT)
        endpoint = address if unix else f"http://127.0.0.1:{listener.getsockname()[1]}"
        failures = []

        def run():
            try:
                with listener, listener.accept()[0] as downstream:
                    downstream.settimeout(35)
                    source = downstream.makefile("rb")
                    first = source.readline()
                    headers = []
                    length = 0
                    while (line := source.readline()) != b"\r\n":
                        if not line:
                            raise RuntimeError("incomplete request")
                        headers.append(line)
                        if line.lower().startswith(b"content-length:"):
                            length = int(line.split(b":", 1)[1])
                    body = source.read(length)
                    if unix:
                        upstream = socket.socket(socket.AF_UNIX)
                        upstream.connect(str(self.socket))
                    else:
                        host, port = self.endpoint.removeprefix("http://").split(":")
                        upstream = socket.create_connection((host, int(port)), timeout=35)
                    upstream.settimeout(35)
                    with upstream:
                        upstream.sendall(first + b"".join(headers) + b"\r\n" + body)
                        response = http.client.HTTPResponse(upstream)
                        response.begin()
                        payload = response.read()
                        request_id = response.getheader("X-Request-ID", "")
                        if response.status != expected_status:
                            raise RuntimeError("forwarded operation failed")
                        callback(payload)
                        if not lose_response:
                            downstream.sendall(
                                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                                + f"X-Request-ID: {request_id}\r\n".encode()
                                + f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                                + payload)
            except Exception as error:
                failures.append(type(error).__name__)

        thread = threading.Thread(target=run)
        thread.start()
        return endpoint, thread, failures

    def test_bootstrap_rotation_auth_and_recovery(self):
        self.provision()
        self.assertEqual(self.socket.stat().st_mode & 0o777, 0o600)
        ticket = self.ticket("ticket")
        self.enroll("ticket", "principal", "delivery")
        principal = self.credential("principal")
        delivery = self.credential("delivery")
        principal_paths = [("/v1/me", "GET"), ("/v1/principals", "GET"),
                           ("/v1/spaces", "GET"), ("/v1/spaces/example", "GET"),
                           ("/v1/spaces/example/records", "GET"),
                           ("/v1/spaces/example/records", "POST"),
                           ("/v1/spaces/example/search", "GET"),
                           ("/v1/records/example", "GET"),
                           ("/v1/records/example/thread", "GET"),
                           ("/v1/records/example/delivery-status", "GET")]
        delivery_paths = [("/v1/adapters/self/register", "POST"),
                          ("/v1/adapters/self/heartbeat", "POST"),
                          ("/v1/mailbox/claims", "POST"),
                          ("/v1/claims/example/commit", "POST"),
                          ("/v1/mailbox-items/example/events", "POST"),
                          ("/v1/mailbox/status", "GET")]
        for paths, good, wrong in [(principal_paths, principal, delivery),
                                    (delivery_paths, delivery, principal)]:
            for path, method in paths:
                for token in [None, "malformed", ticket, wrong["secret"]]:
                    self.assertEqual(self.request(path, token, method)[0], 401)
                implemented = {
                    ("/v1/me", "GET"): 200,
                    ("/v1/principals", "GET"): 400,
                    ("/v1/spaces", "GET"): 200,
                    ("/v1/spaces/example", "GET"): 404,
                    ("/v1/spaces/example/records", "GET"): 404,
                    ("/v1/spaces/example/records", "POST"): 400,
                    ("/v1/spaces/example/search", "GET"): 400,
                    ("/v1/records/example", "GET"): 404,
                    ("/v1/records/example/thread", "GET"): 404,
                    ("/v1/records/example/delivery-status", "GET"): 404,
                    ("/v1/adapters/self/register", "POST"): 400,
                    ("/v1/adapters/self/heartbeat", "POST"): 400,
                    ("/v1/mailbox/claims", "POST"): 400,
                    ("/v1/claims/example/commit", "POST"): 400,
                    ("/v1/mailbox-items/example/events", "POST"): 400,
                    ("/v1/mailbox/status", "GET"): 200,
                }
                self.assertEqual(self.request(path, good["secret"], method)[0],
                                 implemented[(path, method)])
        for token in [None, ticket, principal["secret"], delivery["secret"]]:
            self.assertEqual(self.request("/v1/admin/principals", token, "POST")[0], 404)
            self.assertEqual(self.request("/v1/admin/metrics", token)[0], 404)
        metrics = json.loads(self.admin("metrics").stdout)
        self.assertGreater(metrics["database_bytes"], 0)
        self.assertEqual(metrics["pending_mailbox_count"], 0)
        self.assertIsNone(metrics["oldest_pending_at"])
        self.assertIsNone(metrics["last_backup_at"])
        self.assertIsNone(metrics["last_verified_restore_at"])
        self.assertEqual(self.request("/unknown/" + principal["secret"])[0], 404)
        self.enroll("ticket", "replay-principal", "replay-delivery", succeeds=False)
        result = self.admin("credential-rotate", principal["credential_id"],
                            str(self.directory / "replacement"))
        self.assertEqual(result.stdout, b"")
        replacement = self.credential("replacement")
        self.assertEqual(self.request("/v1/me", principal["secret"])[0], 401)
        self.assertEqual(self.request("/v1/me", replacement["secret"])[0], 200)
        self.admin("credential-revoke", replacement["credential_id"])
        self.assertEqual(self.request("/v1/me", replacement["secret"])[0], 401)
        self.admin("enrollment-recover", "adapter-example", "other-installation",
                   succeeds=False)
        self.admin("enrollment-recover", "adapter-example", "installation-example")
        self.assertEqual(self.request("/v1/mailbox/status", delivery["secret"])[0], 401)
        fresh = self.ticket("fresh-ticket")
        self.enroll("fresh-ticket", "fresh-principal", "fresh-delivery")
        self.credential("fresh-principal")
        self.credential("fresh-delivery")
        connection = sqlite3.connect(self.database)
        hashes = {row[0] for row in connection.execute("SELECT token_hash FROM credentials")}
        for secret in self.secrets:
            if secret not in [ticket, fresh]:
                self.assertIn(hashlib.sha256(secret.encode()).hexdigest(), hashes)
        connection.close()
        for path in self.directory.glob("journal.db*"):
            for secret in self.secrets:
                self.assertNotIn(secret.encode(), path.read_bytes())
        for secret in self.secrets:
            self.assertNotIn(secret, self.trace.read_text())

    def test_postcommit_enrollment_failures_require_recovery(self):
        self.provision()
        for index, failure in enumerate(["lost", "principal", "delivery"]):
            name = f"ticket-{index}"
            self.ticket(name)
            principal, delivery = f"principal-{index}", f"delivery-{index}"

            def committed(payload):
                value = json.loads(payload)
                self.secrets.extend([value["principal_client_secret"]["secret"],
                                     value["delivery_adapter_secret"]["secret"]])
                if failure != "lost":
                    (self.directory / (principal if failure == "principal" else delivery)).mkdir()

            endpoint, thread, errors = self.proxy(committed, lose_response=failure == "lost")
            self.enroll(name, principal, delivery, endpoint=endpoint, succeeds=False)
            thread.join(timeout=PROCESS_TIMEOUT)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.enroll(name, f"replay-p-{index}", f"replay-d-{index}", succeeds=False)
            self.admin("enrollment-recover", "adapter-example", "installation-example")
            connection = sqlite3.connect(self.database)
            self.assertEqual(connection.execute(
                "SELECT count(*) FROM credentials WHERE revoked_at IS NULL").fetchone()[0], 0)
            connection.close()

    def test_rotation_file_failure_does_not_restore_old_credential(self):
        self.provision()
        self.ticket("ticket")
        self.enroll("ticket", "principal", "delivery")
        principal = self.credential("principal")
        replacements = []

        def committed(payload):
            value = json.loads(payload)["replacement_secret"]
            replacements.append(value)
            self.secrets.append(value["secret"])
            (self.directory / "replacement").mkdir()

        endpoint, thread, errors = self.proxy(committed, unix=True)
        result = self.admin("credential-rotate", principal["credential_id"],
                            str(self.directory / "replacement"),
                            socket_path=endpoint, succeeds=False)
        thread.join(timeout=PROCESS_TIMEOUT)
        self.assertEqual(errors, [])
        self.assertIn(b"file write failed", result.stderr)
        self.assertIn(b"event=credential_write_failed", result.stderr)
        self.assertIn(b"server_outcome=committed", result.stderr)
        commits = [json.loads(line)["fields"] for line in self.trace.read_text().splitlines()
                   if json.loads(line)["fields"].get("event") == "bootstrap_committed"]
        rotation = next(event for event in commits if event["operation"] == "rotate")
        self.assertIn(rotation["request_id"].encode(), result.stderr)
        self.assertEqual(self.request("/v1/me", principal["secret"])[0], 401)
        self.assertEqual(self.request("/v1/me", replacements[0]["secret"])[0], 200)
        self.admin("enrollment-recover", "adapter-example", "installation-example")
        self.assertEqual(self.request("/v1/me", replacements[0]["secret"])[0], 401)

    def test_lost_rotation_response_can_be_recovered_without_replacement_id(self):
        self.provision()
        self.ticket("ticket")
        self.enroll("ticket", "principal", "delivery")
        principal = self.credential("principal")
        endpoint, thread, errors = self.proxy(lambda _: None, unix=True, lose_response=True)
        self.admin("credential-rotate", principal["credential_id"],
                   str(self.directory / "replacement"), socket_path=endpoint, succeeds=False)
        thread.join(timeout=PROCESS_TIMEOUT)
        self.assertEqual(errors, [])
        self.assertEqual(self.request("/v1/me", principal["secret"])[0], 401)
        self.assertFalse((self.directory / "replacement").exists())
        self.admin("enrollment-recover", "adapter-example", "installation-example")
        self.ticket("fresh-ticket")
        self.enroll("fresh-ticket", "fresh-principal", "fresh-delivery")
        credential = self.credential("fresh-principal")
        self.assertEqual(self.request("/v1/me", credential["secret"])[0], 200)

    def test_safe_failure_categories_are_correlated(self):
        self.provision()
        self.secrets.append("e" * 64)
        self.assertEqual(self.request("/v1/me")[0], 401)
        self.assertEqual(self.request("/v1/me", "e" * 64)[0], 401)
        self.admin("principal-create", "principal-example", "Duplicate", succeeds=False)
        events = [json.loads(line)["fields"] for line in self.trace.read_text().splitlines()]
        rejections = [event for event in events if event.get("event") == "authentication_rejected"]
        self.assertTrue(any(event["category"] == "missing_or_malformed_bearer" for event in rejections))
        self.assertTrue(any(event["category"] == "credential_rejected" for event in rejections))
        self.assertTrue(all(event.get("request_id") for event in rejections))
        self.assertTrue(any(event.get("category") == "conflict" and
                            event.get("operation") == "create_principal" for event in events))

    def test_storage_contention_logs_safe_failure_code(self):
        self.provision()
        connection = sqlite3.connect(self.database)
        try:
            connection.execute("BEGIN IMMEDIATE")
            self.admin("principal-create", "blocked-principal", "Blocked", succeeds=False)
        finally:
            connection.rollback()
            connection.close()
        failures = [json.loads(line)["fields"] for line in self.trace.read_text().splitlines()
                    if json.loads(line)["fields"].get("event") == "bootstrap_failed"]
        self.assertTrue(any(event.get("category") == "sqlite" and
                            event.get("sqlite_code") == 5 and
                            event.get("outcome") == "not_confirmed" and
                            event.get("request_id") for event in failures),
                        failures)


if __name__ == "__main__":
    unittest.main()
