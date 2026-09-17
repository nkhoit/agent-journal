#!/usr/bin/env python3
"""Run the deterministic structural/reference check for the public OpenAPI contract."""
from __future__ import annotations

import sys
from pathlib import Path

try:
    import yaml
except ImportError as exc:  # pragma: no cover - exercised by CLI environment
    raise SystemExit("PyYAML is required for OpenAPI validation: python -m pip install PyYAML") from exc


def main() -> int:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "api/openapi.yaml")
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("openapi") != "3.1.0":
        raise SystemExit("expected an OpenAPI 3.1.0 document")
    if not isinstance(document.get("info"), dict) or not document["info"].get("title"):
        raise SystemExit("OpenAPI info.title is required")
    paths = document.get("paths")
    if not isinstance(paths, dict) or len(paths) < 20:
        raise SystemExit("contract unexpectedly contains fewer than 20 paths")
    required_paths = {
        "/health/live", "/health/ready", "/v1/me", "/v1/principals", "/v1/spaces",
        "/v1/spaces/{space}", "/v1/spaces/{space}/records", "/v1/records/{record_id}",
        "/v1/spaces/{space}/search", "/v1/records/{record_id}/thread",
        "/v1/admin/enrollment-tickets", "/v1/enrollment/exchange", "/v1/admin/adapters",
        "/v1/adapters/self/register", "/v1/adapters/self/heartbeat",
        "/v1/admin/adapters/{adapter_id}/replace", "/v1/mailbox/claims", "/v1/claims/{claim_id}/commit",
        "/v1/mailbox-items/{item_id}/events", "/v1/mailbox/status",
        "/v1/records/{record_id}/delivery-status", "/v1/admin/mailboxes/{principal}/status",
        "/v1/admin/principals", "/v1/admin/spaces", "/v1/admin/memberships",
        "/v1/admin/credentials/rotate", "/v1/admin/mailbox-items/{item_id}/requeue",
    }
    missing_paths = required_paths - set(paths)
    if missing_paths:
        raise SystemExit(f"missing designed paths: {sorted(missing_paths)}")
    schemas = document.get("components", {}).get("schemas", {})
    responses = document.get("components", {}).get("responses", {})
    schemes = document.get("components", {}).get("securitySchemes", {})
    for name in (
        "ErrorResponse", "PageInfo", "Record", "Limits", "Relation", "DeliveryState", "TelemetryState", "ClaimState",
        "EnrollmentTicketCreateRequest", "EnrollmentTicketCreateResponse", "OneTimeEnrollmentTicket",
        "EnrollmentExchangeRequest", "EnrollmentExchangeResponse", "OneTimePrincipalClientSecret", "OneTimeDeliveryAdapterSecret",
        "DeliveryEnvelope",
    ):
        if name not in schemas:
            raise SystemExit(f"missing required schema: {name}")
    for name in ("Unauthorized", "Forbidden", "NotFound", "Conflict"):
        if name not in responses:
            raise SystemExit(f"missing required response: {name}")
    for name in ("principalClient", "deliveryAdapter", "enrollmentTicket"):
        if name not in schemes:
            raise SystemExit(f"missing required security scheme: {name}")
    if "serviceAdmin" in schemes:
        raise SystemExit("serviceAdmin must not model a remote admin secret")
    if document.get("x-agent-journal-admin-transport", {}).get("kind") != "protected-unix-socket":
        raise SystemExit("admin transport must be declared as a protected Unix socket")
    for path_name, path_item in paths.items():
        if path_name.startswith("/v1/admin/"):
            for method, operation in path_item.items():
                if method.lower() in {"get", "post", "put", "patch", "delete"}:
                    if operation.get("security") != []:
                        raise SystemExit(f"admin operation must not advertise HTTP credentials: {path_name} {method}")
                    if operation.get("x-agent-journal-authorization") != "protected-unix-socket-peer-credentials":
                        raise SystemExit(f"admin operation missing Unix-socket authorization extension: {path_name} {method}")

    append_request = schemas["AppendRecordRequest"]
    append_properties = append_request.get("properties", {})
    if {"source_system", "source_id", "imported_created_at"} & set(append_properties):
        raise SystemExit("ordinary append must not expose imported provenance fields")
    append_response = schemas.get("AppendRecordResponse", {})
    if append_response.get("required") != ["record", "mailbox_created", "replayed"]:
        raise SystemExit("AppendRecordResponse must require record, mailbox_created, replayed")
    provision_request = schemas.get("AdapterProvisionRequest", {})
    if provision_request.get("required") != ["principal_id", "adapter_id"]:
        raise SystemExit("adapter provisioning must require principal_id and adapter_id")
    if "credential" in " ".join(provision_request.get("properties", {})).lower():
        raise SystemExit("adapter provisioning must not create credentials")
    registration = schemas.get("AdapterRegistration", {})
    if "lease_expires_at" not in registration.get("properties", {}):
        raise SystemExit("adapter registration must expose lease_expires_at")
    for schema_name, property_name in (("AppendRecordRequest", "content"), ("Record", "content"), ("DeliveryEnvelope", "body")):
        property_schema = schemas[schema_name].get("properties", {}).get(property_name, {})
        if property_schema.get("x-agent-journal-max-utf8-bytes") != 65536:
            raise SystemExit(f"{schema_name}.{property_name} must declare the 65536 UTF-8 byte limit")
    detail_schema = schemas.get("DeliveryEventRequest", {}).get("properties", {}).get("detail", {})
    if detail_schema.get("x-agent-journal-max-serialized-json-utf8-bytes") != 4096:
        raise SystemExit("telemetry detail must declare the 4096 serialized UTF-8 byte limit")
    delivery_status_description = paths["/v1/records/{record_id}/delivery-status"]["get"].get("description", "")
    if "record author" not in delivery_status_description or "addressed recipient" not in delivery_status_description:
        raise SystemExit("delivery-status visibility policy is not explicit")

    # Resolve local JSON references enough to catch stale contract links.
    def exists(ref: str) -> bool:
        if not ref.startswith("#/" ):
            return True
        value: object = document
        for component in ref[2:].split("/"):
            if not isinstance(value, dict) or component not in value:
                return False
            value = value[component]
        return True

    def walk(value: object) -> None:
        if isinstance(value, dict):
            ref = value.get("$ref")
            if isinstance(ref, str) and not exists(ref):
                raise SystemExit(f"unresolved local reference: {ref}")
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(document)
    operations = sum(
        1 for item in paths.values() if isinstance(item, dict)
        for method in item if method.lower() in {"get", "post", "put", "patch", "delete", "head", "options", "trace"}
    )
    if operations < 20:
        raise SystemExit("contract unexpectedly contains fewer than 20 operations")
    print(f"OpenAPI structural/reference check passed: {path} ({len(paths)} paths, {operations} operations)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
