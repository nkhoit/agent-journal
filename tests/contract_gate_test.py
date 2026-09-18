#!/usr/bin/env python3
"""Black-box mutation tests for the executable OpenAPI contract gate."""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts" / "validate_openapi.py"
SOURCE_OPENAPI = ROOT / "api" / "openapi.yaml"
SOURCE_CONFORMANCE = ROOT / "conformance"


class ContractGateTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.work = Path(temporary_directory.name)
        self.openapi = self.work / "openapi.yaml"
        self.conformance = self.work / "conformance"
        shutil.copy2(SOURCE_OPENAPI, self.openapi)
        shutil.copytree(SOURCE_CONFORMANCE, self.conformance)

    def run_gate(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(VALIDATOR),
                str(self.openapi),
                str(self.conformance),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def load_yaml(self, path: Path) -> dict:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        self.assertIsInstance(document, dict)
        return document

    def write_yaml(self, path: Path, document: dict) -> None:
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    def assert_gate_rejects(self, *expected_fragments: str) -> str:
        result = self.run_gate()
        output = f"{result.stdout}\n{result.stderr}"
        self.assertNotEqual(result.returncode, 0, output)
        for fragment in expected_fragments:
            self.assertIn(fragment.casefold(), output.casefold(), output)
        return output

    def test_unmodified_contract_and_fixtures_pass(self) -> None:
        result = self.run_gate()
        output = f"{result.stdout}\n{result.stderr}"
        self.assertEqual(result.returncode, 0, output)
        self.assertIn("29 paths", output)
        self.assertIn("31 operations", output)
        self.assertIn("fixture coverage", output.casefold())

    def test_rotation_requires_one_time_response(self) -> None:
        document = self.load_yaml(self.openapi)
        response = document["paths"]["/v1/admin/credentials/rotate"]["post"]["responses"]["200"]
        response["content"]["application/json"]["schema"]["$ref"] = "#/components/schemas/CredentialMetadata"
        self.write_yaml(self.openapi, document)
        self.assert_gate_rejects("CredentialRotationResponse")

    def test_recovery_must_not_return_content(self) -> None:
        document = self.load_yaml(self.openapi)
        response = document["paths"]["/v1/admin/enrollment/recover"]["post"]["responses"]["204"]
        response["content"] = {"application/json": {"schema": {"type": "object"}}}
        self.write_yaml(self.openapi, document)
        self.assert_gate_rejects("204", "content")

    def test_uncovered_client_operation_is_rejected(self) -> None:
        requests_path = self.conformance / "client" / "requests.yaml"
        requests = self.load_yaml(requests_path)
        requests["operations"] = [
            operation
            for operation in requests["operations"]
            if operation["operation_id"] != "healthLive"
        ]
        self.write_yaml(requests_path, requests)

        self.assert_gate_rejects("fixture coverage", "healthLive")

    def test_client_operation_method_mismatch_is_rejected(self) -> None:
        requests_path = self.conformance / "client" / "requests.yaml"
        requests = self.load_yaml(requests_path)
        get_me = next(
            operation
            for operation in requests["operations"]
            if operation["operation_id"] == "getMe"
        )
        get_me["method"] = "POST"
        self.write_yaml(requests_path, requests)

        self.assert_gate_rejects("getMe", "GET /v1/me")

    def test_required_append_response_field_is_enforced(self) -> None:
        document = self.load_yaml(self.openapi)
        required = document["components"]["schemas"]["AppendRecordResponse"][
            "required"
        ]
        required.remove("record")
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("AppendRecordResponse", "record")

    def test_append_request_schema_reference_is_enforced(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/spaces/{space}/records"]["post"]
        operation["requestBody"]["content"]["application/json"]["schema"]["$ref"] = (
            "#/components/schemas/Record"
        )
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("appendRecord", "AppendRecordRequest")

    def test_append_request_schema_rejects_ref_siblings(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/spaces/{space}/records"]["post"]
        request_schema = operation["requestBody"]["content"]["application/json"][
            "schema"
        ]
        request_schema["type"] = "string"
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("appendRecord", "request schema")

    def test_append_request_rejects_additional_media_types(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/spaces/{space}/records"]["post"]
        content = operation["requestBody"]["content"]
        content["application/xml"] = {
            "schema": {"$ref": "#/components/schemas/AppendRecordRequest"}
        }
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("appendRecord", "application/xml")

    def test_append_response_schema_reference_is_enforced(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/spaces/{space}/records"]["post"]
        operation["responses"]["201"]["content"]["application/json"]["schema"][
            "$ref"
        ] = "#/components/schemas/ErrorResponse"
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("appendRecord", "AppendRecordResponse")

    def test_append_response_rejects_additional_media_types(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/spaces/{space}/records"]["post"]
        content = operation["responses"]["201"]["content"]
        content["application/xml"] = {
            "schema": {"$ref": "#/components/schemas/AppendRecordResponse"}
        }
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects(
            "appendRecord", "response", "application/xml"
        )

    def test_append_response_must_remain_an_object_schema(self) -> None:
        document = self.load_yaml(self.openapi)
        document["components"]["schemas"]["AppendRecordResponse"]["type"] = "string"
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("AppendRecordResponse", "type")

    def test_append_request_must_remain_an_object_schema(self) -> None:
        document = self.load_yaml(self.openapi)
        document["components"]["schemas"]["AppendRecordRequest"]["type"] = "string"
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("AppendRecordRequest", "type")

    def test_adapter_provision_request_rejects_credential_property(self) -> None:
        document = self.load_yaml(self.openapi)
        provision_request = document["components"]["schemas"][
            "AdapterProvisionRequest"
        ]
        provision_request["properties"]["credential"] = {"type": "string"}
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("AdapterProvisionRequest", "credential")

    def test_adapter_provision_request_rejects_credential_pattern(self) -> None:
        document = self.load_yaml(self.openapi)
        provision_request = document["components"]["schemas"][
            "AdapterProvisionRequest"
        ]
        provision_request["patternProperties"] = {
            "^credential$": {"type": "string"}
        }
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects(
            "AdapterProvisionRequest", "patternProperties"
        )

    def test_limit_parameter_maximum_is_enforced(self) -> None:
        document = self.load_yaml(self.openapi)
        document["components"]["parameters"]["Limit"]["schema"]["maximum"] = 1000
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("Limit", "maximum")

    def test_component_parameter_rejects_remote_admin_header(self) -> None:
        document = self.load_yaml(self.openapi)
        cursor = document["components"]["parameters"]["Cursor"]
        cursor["name"] = "X-Admin-Authorization"
        cursor["in"] = "header"
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("Cursor", "X-Admin-Authorization")

    def test_component_parameter_ref_rejects_remote_admin_header(self) -> None:
        document = self.load_yaml(self.openapi)
        parameters = document["components"]["parameters"]
        parameters["AdminAuthorization"] = {
            "name": "X-Admin-Authorization",
            "in": "header",
            "required": True,
            "schema": {"type": "string"},
        }
        parameters["Cursor"] = {
            "$ref": "#/components/parameters/AdminAuthorization"
        }
        self.write_yaml(self.openapi, document)

        output = self.assert_gate_rejects("X-Admin-Authorization")
        self.assertTrue(
            "cursor" in output.casefold()
            or "adminauthorization" in output.casefold(),
            output,
        )

    def test_delivery_status_other_reader_visibility_is_enforced(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"][
            "/v1/records/{record_id}/delivery-status"
        ]["get"]
        operation["x-agent-journal-delivery-status-visibility"][
            "other_reader"
        ] = "all-recipient-entries"
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("getRecordDeliveryStatus", "other_reader")

    def test_expected_envelope_requires_from_principal(self) -> None:
        envelope_path = self.conformance / "adapter" / "expected-envelope.txt"
        envelope_lines = envelope_path.read_text(encoding="utf-8").splitlines()
        envelope_path.write_text(
            "\n".join(
                line
                for line in envelope_lines
                if not line.startswith("from_principal:")
            )
            + "\n",
            encoding="utf-8",
        )

        self.assert_gate_rejects("expected-envelope.txt", "from_principal")

    def test_expected_envelope_requires_stable_attempt_and_quoted_metadata(self) -> None:
        envelope_path = self.conformance / "adapter" / "expected-envelope.txt"
        original = envelope_path.read_text(encoding="utf-8")
        for field in ("mailbox_item_id", "attempt_id"):
            envelope_path.write_text(
                "\n".join(line for line in original.splitlines() if not line.startswith(field + ":")) + "\n",
                encoding="utf-8",
            )
            self.assert_gate_rejects("expected-envelope.txt", field)
        envelope_path.write_text(
            original.replace('from_principal: "agent-source"', "from_principal: agent-source"),
            encoding="utf-8",
        )
        self.assert_gate_rejects("expected-envelope.txt", "JSON quoted")

    def test_expected_envelope_rejects_renamed_from_principal(self) -> None:
        envelope_path = self.conformance / "adapter" / "expected-envelope.txt"
        envelope = envelope_path.read_text(encoding="utf-8")
        self.assertIn("\nfrom_principal:", envelope)
        envelope_path.write_text(
            envelope.replace(
                "\nfrom_principal:", "\nnot_from_principal:", 1
            ),
            encoding="utf-8",
        )

        self.assert_gate_rejects("expected-envelope.txt", "from_principal")

    def test_removed_required_path_is_rejected(self) -> None:
        document = self.load_yaml(self.openapi)
        del document["paths"]["/v1/me"]
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("missing designed paths", "/v1/me")

    def test_broken_local_reference_is_rejected(self) -> None:
        document = self.load_yaml(self.openapi)
        broken_reference = "#/components/schemas/MissingContractSchema"
        document["paths"]["/v1/me"]["get"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]["$ref"] = broken_reference
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("unresolved local reference", broken_reference)

    def test_missing_operation_security_declaration_is_rejected(self) -> None:
        document = self.load_yaml(self.openapi)
        del document["paths"]["/v1/me"]["get"]["security"]
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("getMe", "security")

    def test_admin_operation_exposed_on_public_https_is_rejected(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/admin/enrollment-tickets"]["post"]
        operation["x-agent-journal-public-https"] = True
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("createEnrollmentTicket", "public HTTPS")

    def test_admin_operation_without_unix_socket_restriction_is_rejected(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/admin/enrollment-tickets"]["post"]
        del operation["x-agent-journal-authorization"]
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects("createEnrollmentTicket", "Unix-socket")

    def test_admin_operation_rejects_inline_authorization_header(self) -> None:
        document = self.load_yaml(self.openapi)
        operation = document["paths"]["/v1/admin/enrollment-tickets"]["post"]
        operation.setdefault("parameters", []).append(
            {
                "name": "X-Admin-Authorization",
                "in": "header",
                "required": True,
                "schema": {"type": "string"},
            }
        )
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects(
            "createEnrollmentTicket", "X-Admin-Authorization"
        )

    def test_admin_path_item_rejects_inline_authorization_header(self) -> None:
        document = self.load_yaml(self.openapi)
        path_item = document["paths"]["/v1/admin/adapters"]
        path_item.setdefault("parameters", []).append(
            {
                "name": "X-Admin-Authorization",
                "in": "header",
                "required": True,
                "schema": {"type": "string"},
            }
        )
        self.write_yaml(self.openapi, document)

        self.assert_gate_rejects(
            "/v1/admin/adapters", "X-Admin-Authorization"
        )

    def test_private_identifier_in_conformance_data_is_rejected(self) -> None:
        scenarios_path = self.conformance / "adapter" / "scenarios.yaml"
        scenarios = self.load_yaml(scenarios_path)
        private_runtime_path = str(
            Path("/", "Users", "sample-operator", "runtime-route")
        )
        scenarios["cases"][0]["expectation"] = private_runtime_path
        self.write_yaml(scenarios_path, scenarios)

        self.assert_gate_rejects(
            "private fixture identifier", "adapter/scenarios.yaml"
        )

    def test_empty_adapter_scenarios_are_rejected(self) -> None:
        scenarios_path = self.conformance / "adapter" / "scenarios.yaml"
        scenarios = self.load_yaml(scenarios_path)
        scenarios["cases"] = []
        self.write_yaml(scenarios_path, scenarios)

        self.assert_gate_rejects("adapter", "coverage")

    def test_adapter_operation_coverage_is_enforced(self) -> None:
        scenarios_path = self.conformance / "adapter" / "scenarios.yaml"
        scenarios = self.load_yaml(scenarios_path)
        for case in scenarios["cases"]:
            case["operation_ids"] = [
                operation_id
                for operation_id in case["operation_ids"]
                if operation_id != "replaceAdapter"
            ]
        self.write_yaml(scenarios_path, scenarios)

        self.assert_gate_rejects("adapter", "coverage", "replaceAdapter")


if __name__ == "__main__":
    unittest.main()
