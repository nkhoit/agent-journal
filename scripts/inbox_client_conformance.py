#!/usr/bin/env python3
"""Execute the fixed inbox-client acceptance cases, never empty test selectors."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

import yaml


ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "envelope-boundary": ("journal-inbox-worker", "worker", "rendered_envelope_matches_public_fixture_and_quotes_metadata"),
    "handoff-before-ack": ("journald", "hermes_inbox_cli", "cli_once_uses_real_journald_and_hermes_handoff_before_ack"),
    "muse-real-journal": ("journald", "muse_inbox_cli", "cli_once_drops_to_muse_then_acknowledges_with_restart_idempotence"),
    "route-and-runtime-failure": ("journal-inbox-worker", "worker", "runtime_failure_and_unknown_route_never_ack_or_block_later_items"),
    "lost-ack-response": ("journal-inbox-worker", "worker", "lost_ack_response_retries_only_ack_even_if_item_reappears"),
    "inaccessible-after-handoff": ("journal-inbox-worker", "worker", "inaccessible_after_handoff_is_not_inferred_success_and_does_not_block"),
    "bounded-fair-traversal": ("journal-inbox-worker", "worker", "fixed_bound_traversal_finishes_then_revisits_earlier_failure"),
    "central-backoff": ("journal-inbox-worker", "worker", "transient_central_failure_backs_off_without_handoff"),
    "runtime-backoff": ("journal-inbox-worker", "worker", "runtime_retry_delay_survives_pass_wrap_within_process"),
    "credential-and-page-boundary": ("journal-inbox-worker", "worker", "unauthorized_client_stops_and_oversized_pages_are_not_processed"),
    "hermes-stable-dedupe": ("journal-runtime-hermes", "http", "exact_replay_returns_the_same_run_receipt"),
    "hermes-capabilities": ("journal-runtime-hermes", "http", "unsupported_capabilities_are_rejected_before_injection"),
    "muse-durable-replay": ("journal-runtime-muse", "drop_point", "exact_replay_returns_the_same_receipt_without_rewriting"),
    "muse-publication-crash": ("journal-runtime-muse", "--lib", "tests::killed_publication_replays_only_the_stable_durable_drop"),
    "muse-conflict": ("journal-runtime-muse", "drop_point", "conflicting_payload_at_the_stable_name_fails_closed"),
    "muse-unsafe-replay": ("journal-runtime-muse", "drop_point", "identical_payload_through_a_symlink_is_not_handoff_evidence"),
    "ambiguous-acceptance-restart": ("journal-runtime-fake", "runtime", "acceptance_survives_child_termination_and_duplicate_replay"),
}


def validate_manifest(path: Path) -> list[str]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or set(document) != {"fixture_version", "cases"}:
        raise ValueError("inbox-client manifest fields differ")
    cases = document["cases"]
    if document["fixture_version"] != 1 or not isinstance(cases, list):
        raise ValueError("unsupported inbox-client manifest")
    if not all(isinstance(case, str) for case in cases):
        raise ValueError("inbox-client case IDs must be strings")
    if len(cases) != len(set(cases)) or set(cases) != set(CASES):
        raise ValueError("inbox-client fixture coverage differs: missing, duplicate or unknown case")
    return cases


def matched_success(output: str, name: str) -> bool:
    return any(line.strip() == f"test {name} ... ok" for line in output.splitlines())


def main() -> int:
    cases = validate_manifest(ROOT / "conformance" / "inbox-client" / "scenarios.yaml")
    destination = ROOT / "target" / "inbox-client-conformance"
    destination.mkdir(parents=True, exist_ok=True)
    manifest = destination / "complete.json"
    manifest.unlink(missing_ok=True)
    cargo = shlex.split(os.environ.get("CARGO", "cargo +1.85.0"))
    completed = []
    for case in cases:
        package, target, test = CASES[case]
        selector = ["--lib"] if target == "--lib" else ["--test", target]
        result = subprocess.run(
            [*cargo, "test", "--locked", "-p", package, *selector, test, "--", "--exact"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        if result.returncode or not matched_success(result.stdout, test):
            print(f"inbox-client case failed or selected no successful test: {case}", file=sys.stderr)
            print(result.stdout, file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            return 1
        evidence = {"case": case, "package": package, "target": target, "test": test, "passed": True}
        (destination / f"{case}.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        completed.append(case)
    manifest.write_text(json.dumps({"cases": completed, "passed": True}, indent=2) + "\n", encoding="utf-8")
    print(f"Inbox-client conformance passed: {len(completed)} executable cases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
