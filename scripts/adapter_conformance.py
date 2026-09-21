#!/usr/bin/env python3
"""Execute adapter scenarios and publish only allowlisted persisted-state evidence.

Future runtime crates provide the same runtime integration test entry points with
--runtime-package/--runtime-test. Central policy and generic spool assertions stay
shared. No vendor runtime is certified by the default generic run.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import time

import yaml

ROOT = Path(__file__).resolve().parents[1]
HTTP = "real_http_client_spool_runtime_and_revocation"
CRASH = "real_http_accept_then_crash_recovers_same_attempt_with_duplicate_acceptance"
SPOOL_CRASH = "process_death_at_orchestration_boundaries_exposes_duplicate_risk"
REPLAY = "retryable_success_replay_and_requeue_keep_history_and_fence_old_attempts"
MATRIX = "telemetry_transition_matrix_old_attempt_projection_and_atomic_failure"

# Each recipe executes assertions, then independently reads the resulting SQLite
# files. Exact test selection must execute one test, never silently match zero.
SCENARIOS = {
    "register-heartbeat-fencing": (
        "stale-generation-rejected_and_lease_renewed",
        ["registerAdapter", "heartbeatAdapter", "replaceAdapter"],
        [("central", "heartbeat_expiry_restart_and_credential_binding"),
         ("central", "replacement_fences_and_requires_new_enrollment")]),
    "spool-before-custody": (
        "no_runtime_injection_before_host_acceptance",
        ["claimMailbox", "commitHostCustody"],
        [("spool", "custody_precedes_injection_and_terminal_event_survives_lost_response"),
         ("runtime", HTTP)]),
    "lost-commit-response": (
        "retry_same_claim_and_attempt_id", ["commitHostCustody"],
        [("runtime", HTTP)]),
    "restart-recovery": (
        "enumerate_claim_instance_generation_and_injection_lifecycle",
        ["registerAdapter", "claimMailbox", "commitHostCustody", "recordDeliveryEvent"],
        [("spool", SPOOL_CRASH), ("runtime", CRASH)]),
    "lease-expiry": (
        "redelivery_preserves_attempt_id", ["claimMailbox", "commitHostCustody"],
        [("central", "restart_lost_response_expiry_and_bounds"),
         ("spool", "authoritative_expiry_reclaims_same_attempt_without_overwriting_payload")]),
    "explicit-requeue": (
        "new_attempt_id_and_retained_history", ["requeueMailboxItem", "claimMailbox"],
        [("central", REPLAY)]),
    "stale-generation": (
        "reject_central_operation_and_stop",
        ["heartbeatAdapter", "claimMailbox", "commitHostCustody", "recordDeliveryEvent"],
        [("central", "replacement_fences_and_requires_new_enrollment"),
         ("spool", "changed_generation_renewal_preserves_old_authority_and_fails_closed"),
         ("spool", "fence_immediately_before_send_and_pressure_before_claim")]),
    "unknown-route": (
        "route_unavailable_without_default_fallback", ["claimMailbox"],
        [("spool", "explicit_unknown_empty_disabled_and_removed_routes_never_fall_back")]),
    "resolved-route-injection": (
        "runtime_receives_resolved_route_and_exact_envelope",
        ["claimMailbox", "commitHostCustody"], [("runtime", HTTP)]),
    "telemetry-before-custody": (
        "reject_pending_or_claimed_attempt", ["recordDeliveryEvent", "commitHostCustody"],
        [("central", REPLAY)]),
    "telemetry-cross-principal": (
        "reject_adapter_principal_mismatch", ["recordDeliveryEvent"],
        [("central", "custody_cross_principal_rotated_credential_and_public_policy")]),
    "retryable-telemetry-recovery": (
        "retryable_failure_then_runtime_acceptance_on_same_custodied_attempt",
        ["recordDeliveryEvent", "commitHostCustody"],
        [("central", REPLAY), ("spool", "retry_schedule_is_durable_and_recovery_is_fair")]),
    "telemetry-replay-and-terminal-protection": (
        "exact_replay_preserves_received_at_conflicting_reuse_and_final_state_downgrades_rejected",
        ["recordDeliveryEvent"], [("central", REPLAY), ("central", MATRIX)]),
    "late-telemetry-after-requeue": (
        "old_attempt_history_retained_without_advancing_new_mailbox_projection",
        ["recordDeliveryEvent", "requeueMailboxItem", "getRecordDeliveryStatus"],
        [("central", MATRIX)]),
    "partial-custody-and-expired-replay": (
        "exact_valid_items_commit_and_receipts_replay_after_expiry",
        ["commitHostCustody"], [("central", "partial_custody_replay_expiry_and_exact_binding")]),
    "duplicate-runtime-send": (
        "stable_record_id_and_honest_at_least_once",
        ["claimMailbox", "commitHostCustody", "recordDeliveryEvent"],
        [("spool", SPOOL_CRASH), ("runtime", CRASH)]),
    "oversized-runtime-success": (
        "oversized_success_is_accepted_without_reinjection",
        ["claimMailbox", "commitHostCustody", "recordDeliveryEvent"],
        [("spool", "oversized_success_does_not_reinject_after_runtime_call")]),
    "ambiguous-runtime-error": (
        "ambiguous_runtime_error_is_terminal_without_reinjection",
        ["claimMailbox", "commitHostCustody", "recordDeliveryEvent"],
        [("spool", "ambiguous_runtime_error_does_not_reinject_after_runtime_call")]),
    "revocation": (
        "reject_revoked_credentials_without_recalling_content", ["revokeCredential", "claimMailbox"],
        [("central", "recipient_isolation_and_credential_revocation"), ("runtime", HTTP)]),
}

STATES = {
    "pending", "claimed", "host-accepted", "adapter-reported-runtime-accepted",
    "adapter-reported-retryable-failure", "adapter-reported-terminal-failure",
    "route-unavailable", "suppressed-revoked", "active", "committed", "expired",
    "cancelled", "draining", "revoked", "in-flight", "accepted",
    "retryable-failure", "terminal-failure",
    "Pending", "InFlight", "Accepted", "RetryableFailure", "RouteUnavailable", "TerminalFailure",
}
IDENTITIES = {
    "record_id": "record", "mailbox_item_id": "mailbox", "attempt_id": "attempt",
    "claim_id": "claim", "principal_id": "principal",
    "recipient_principal_id": "principal", "adapter_id": "adapter",
    "instance_id": "installation", "event_id": "event",
    "received_at": "timestamp", "occurred_at": "timestamp",
}
TABLES = {
    "adapter_registrations": ["adapter_id", "principal_id", "instance_id", "generation", "status"],
    "mailbox_items": ["id", "record_id", "recipient_principal_id", "state"],
    "delivery_attempts": ["attempt_id", "mailbox_item_id", "ordinal", "state"],
    "claims": ["id", "adapter_id", "principal_id", "instance_id", "generation", "state"],
    "claim_items": ["claim_id", "mailbox_item_id", "attempt_id"],
    "delivery_events": ["event_id", "mailbox_item_id", "attempt_id", "adapter_id",
                        "instance_id", "generation", "state", "received_at", "occurred_at"],
}


def validate_cases(value):
    if set(value) != {"fixture_version", "cases", "limits"} or value.get("fixture_version") != 1:
        raise ValueError("unsupported scenario fixture version")
    cases = value.get("cases", [])
    ids = [case["id"] for case in cases]
    if len(ids) != len(set(ids)) or set(ids) != set(SCENARIOS):
        raise ValueError("unknown, duplicate, or uncovered scenario IDs")
    for case in cases:
        if set(case) != {"id", "expectation", "operation_ids"}:
            raise ValueError("unsupported scenario fields")
        expectation, operations, _ = SCENARIOS[case["id"]]
        if case.get("expectation") != expectation or case.get("operation_ids") != operations:
            raise ValueError("scenario expectation or operation coverage changed")
    if value.get("limits") != {
        "claim_items_max": 20, "envelope_bytes_max": 65536,
        "telemetry_detail_serialized_utf8_bytes_max": 4096,
    }:
        raise ValueError("scenario limits changed")
    return cases


def load_cases(path):
    return validate_cases(yaml.safe_load(path.read_text(encoding="utf-8")))


class Redactor:
    def __init__(self):
        self.values = {}

    def identity(self, kind, value):
        if value is None:
            return None
        values = self.values.setdefault(kind, {})
        if value not in values:
            values[value] = f"{kind}:{len(values) + 1}"
        return values[value]

    def state(self, value):
        if value not in STATES:
            raise ValueError("unrecognized persisted state")
        return value


def central_evidence(db, redactor=None):
    redactor = redactor or Redactor()
    available = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    result = {}
    for table, fields in TABLES.items():
        if table not in available:
            continue
        rows = []
        for values in db.execute(f"SELECT {', '.join(fields)} FROM {table} ORDER BY rowid"):
            row = {}
            for field, value in zip(fields, values):
                if field in ("state", "status"):
                    row[field] = redactor.state(value)
                elif field in ("generation", "ordinal"):
                    if not isinstance(value, int):
                        raise ValueError("invalid persisted integer")
                    row[field] = value
                else:
                    kind = IDENTITIES.get(field)
                    if field == "id":
                        kind = "mailbox" if table == "mailbox_items" else "claim"
                    row[field] = redactor.identity(kind, value)
            rows.append(row)
        result[table] = rows
    return result


def spool_evidence(db, redactor):
    rows = []
    for (serialized,) in db.execute("SELECT item FROM attempts ORDER BY attempt_id"):
        item = json.loads(serialized)
        row = {field: redactor.identity(IDENTITIES[field], item[field]) for field in (
            "attempt_id", "mailbox_item_id", "record_id", "claim_id", "instance_id"
        )}
        row.update({
            "generation": int(item["generation"]),
            "custody_confirmed": item["custody_confirmed"] is True,
            "injection_state": redactor.state(item["injection_state"]),
            "record_retained": item.get("record") is not None,
            "pending_event": item.get("pending_event") is not None,
            "runtime_failures": int(item.get("runtime_failures", 0)),
            "event_sequence": int(item.get("event_sequence", 0)),
            "retry_scheduled": item.get("next_runtime_try_at") is not None,
        })
        rows.append(row)
    return {"attempts": rows}


def require_evidence(evidence):
    if not any(
        entry["kind"] in ("central", "spool")
        and any(isinstance(rows, list) and rows for rows in entry["state"].values())
        for entry in evidence
    ):
        raise ValueError("scenario did not produce persisted state evidence")
    custodied = {
        (entry.get("execution"), entry["fixture"], row["attempt_id"])
        for entry in evidence if entry["kind"] == "spool"
        for row in entry["state"]["attempts"] if row["custody_confirmed"]
    }
    for entry in evidence:
        if entry["kind"] == "runtime":
            if any((entry.get("execution"), entry["fixture"], row["attempt_id"]) not in custodied
                   for row in entry["state"]["acceptances"]):
                raise ValueError("runtime acceptance has no persisted custody evidence")


def collect_evidence(directory):
    fixtures = {}

    def fixture(path):
        if path.parent not in fixtures:
            fixtures[path.parent] = (f"fixture:{len(fixtures) + 1}", Redactor())
        return fixtures[path.parent]

    evidence = []
    for path in sorted(directory.rglob("*.db")):
        token, redactor = fixture(path)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("persisted database failed integrity check")
            spool = db.execute("PRAGMA application_id").fetchone()[0] == 0x414A5350
            state = spool_evidence(db, redactor) if spool else central_evidence(db, redactor)
        evidence.append({
            "fixture": token,
            "kind": "spool" if spool else "central",
            "checkpoint": "before-recovery" if path.name == "before-recovery.db" else "final",
            "state": state,
        })
    for path in sorted(directory.rglob("acceptances.jsonl")):
        token, redactor = fixture(path)
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            accepted = json.loads(line)
            envelope = accepted["envelope"]
            rows.append({
                "record_id": redactor.identity("record", envelope["record_id"]),
                "attempt_id": redactor.identity("attempt", envelope["attempt_id"]),
                "mailbox_item_id": redactor.identity("mailbox", envelope["mailbox_item_id"]),
                "resolved_route": redactor.identity("route", accepted["route"]["runtime_target"]),
                "route_enabled": accepted["route"]["enabled"] is True,
                "envelope": redactor.identity("envelope", json.dumps(envelope, sort_keys=True)),
                "rendered": redactor.identity("rendered", accepted["rendered"]),
            })
        evidence.append({"fixture": token, "kind": "runtime", "state": {"acceptances": rows}})
    for path in sorted(directory.rglob("crash-evidence.json")):
        token, _ = fixture(path)
        value = json.loads(path.read_text(encoding="utf-8"))
        if value["phase"] not in {
            "claimed", "spooled", "central-custody", "injection-started",
            "runtime-accepted", "result-persisted", "telemetry-accepted",
        }:
            raise ValueError("unrecognized crash checkpoint")
        evidence.append({"fixture": token, "kind": "crash", "state": {
            "phase": value["phase"], "child_terminated": value["child_terminated"] is True,
            "accepted_before_crash": int(value["accepted_before_crash"]),
            "recovery_injections": int(value["recovery_injections"]),
        }})
    require_evidence(evidence)
    return evidence


class WindowsJob:
    """Own descendants before releasing the launcher's stdin gate."""

    def __init__(self):
        import ctypes
        from ctypes import wintypes

        self.ctypes = ctypes
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        for name, arguments, result in (
            ("CreateJobObjectW", [wintypes.LPVOID, wintypes.LPCWSTR], wintypes.HANDLE),
            ("AssignProcessToJobObject", [wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
            ("TerminateJobObject", [wintypes.HANDLE, wintypes.UINT], wintypes.BOOL),
            ("QueryInformationJobObject", [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID,
                                          wintypes.DWORD, wintypes.LPVOID], wintypes.BOOL),
            ("CloseHandle", [wintypes.HANDLE], wintypes.BOOL),
        ):
            function = getattr(self.api, name)
            function.argtypes = arguments
            function.restype = result

        class Accounting(ctypes.Structure):
            _fields_ = [
                ("times", ctypes.c_int64 * 4),
                ("page_faults", wintypes.DWORD),
                ("total", wintypes.DWORD),
                ("active", wintypes.DWORD),
                ("terminated", wintypes.DWORD),
            ]

        self.accounting = Accounting()
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())

    def check(self, result):
        if not result:
            raise self.ctypes.WinError(self.ctypes.get_last_error())

    def assign(self, process):
        self.check(self.api.AssignProcessToJobObject(self.handle, int(process._handle)))

    def terminate(self):
        self.check(self.api.TerminateJobObject(self.handle, 1))
        # TerminateJobObject is asynchronous. Wait for all members, not just Cargo.
        while True:
            self.check(self.api.QueryInformationJobObject(
                self.handle, 1, self.ctypes.byref(self.accounting),
                self.ctypes.sizeof(self.accounting), None))
            if not self.accounting.active:
                break
            time.sleep(0.01)

    def close(self):
        self.check(self.api.CloseHandle(self.handle))


def run_process_tree(command, *, cwd, env, timeout):
    job = WindowsJob() if os.name == "nt" else None
    process = None
    try:
        if job:
            # The launcher cannot spawn until it belongs to our job. Descendants
            # inherit that membership even if their immediate parent exits.
            command = [sys.executable, "-c",
                       "import subprocess,sys; "
                       "sys.stdin.buffer.read(1); "
                       "sys.exit(subprocess.call(sys.argv[1:]))", *command]
        process = subprocess.Popen(
            command, cwd=cwd, env=env, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            start_new_session=job is None)
        if job:
            job.assign(process)
        try:
            stdout, stderr = process.communicate(input="x", timeout=timeout)
            return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        finally:
            if job:
                job.terminate()
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.communicate()
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.communicate()
        if job:
            job.close()


def run_test(kind, test, directory, args):
    if kind == "central":
        package, target, name = "journal-service", ["--test", "delivery"], test
    elif kind == "spool":
        package, target, name = "journal-adapter-spool", ["--lib"], f"orchestration_tests::{test}"
    else:
        package, target, name = args.runtime_package, ["--test", args.runtime_test], test
    command = ["cargo", "+1.85.0", "test", "--locked", "-p", package, *target,
               name, "--", "--exact", "--test-threads=1"]
    environment = os.environ.copy()
    environment["AJ_CONFORMANCE_STATE_DIR"] = str(directory)
    result = run_process_tree(command, cwd=ROOT, env=environment, timeout=180)
    # Do not put panic output, paths, fixture credentials, or envelopes in artifacts.
    if result.returncode or "test result: ok. 1 passed;" not in result.stdout:
        raise ValueError(f"{kind} assertion failed or did not execute exactly one test: {test}")
    return collect_evidence(directory)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenarios", type=Path, default=ROOT / "conformance/adapter/scenarios.yaml")
    parser.add_argument("--output", type=Path, default=ROOT / "target/adapter-conformance")
    parser.add_argument("--runtime-package", default="journald")
    parser.add_argument("--runtime-test", default="adapter_orchestration")
    args = parser.parse_args(argv)
    output = args.output.resolve()
    output.relative_to(ROOT)
    output.mkdir(parents=True, exist_ok=True)
    # Exclusive work-directory creation prevents concurrent runs mixing state.
    work = output / ".private-state"
    work.mkdir(mode=0o700)
    manifest = {
        "schema_version": 2, "redacted": True, "runtime_package": args.runtime_package,
        "scenarios": [],
    }
    try:
        (output / "manifest.json").unlink(missing_ok=True)
        for scenario in SCENARIOS:
            (output / f"{scenario}.json").unlink(missing_ok=True)
        cases = load_cases(args.scenarios)
        for case in cases:
            scenario = case["id"]
            evidence = []
            for index, (kind, test) in enumerate(SCENARIOS[scenario][2]):
                directory = work / scenario / str(index)
                directory.mkdir(parents=True, mode=0o700)
                observed = run_test(kind, test, directory, args)
                for entry in observed:
                    entry["execution"] = index + 1
                    entry["assertion"] = test
                evidence.extend(observed)
                shutil.rmtree(directory)
            require_evidence(evidence)
            artifact = {**case, "schema_version": 2, "redacted": True, "evidence": evidence}
            (output / f"{scenario}.json").write_text(
                json.dumps(artifact, indent=2) + "\n", encoding="utf-8"
            )
            manifest["scenarios"].append({"id": scenario, "evidence": f"{scenario}.json"})
            print(f"{scenario}: persisted-state assertions passed", flush=True)
        (output / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
    finally:
        shutil.rmtree(work)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, subprocess.TimeoutExpired, sqlite3.Error) as error:
        # Exception text from SQLite, the OS or subprocess may contain private paths.
        print(f"adapter conformance failed ({type(error).__name__}); no successful manifest",
              file=sys.stderr)
        sys.exit(1)
