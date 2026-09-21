SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3
PYTHON_NO_BYTECODE = PYTHONDONTWRITEBYTECODE=1 $(PYTHON)
OPENAPI_FILE := api/openapi.yaml
OPENAPI_STANDARDS_LINT ?= 0

.PHONY: all build fmt fmt-check test migration-test contract-test bootstrap-test records-test inbox-test recovery-test inbox-client-conformance browser-security foreign-uid-test clippy openapi-check redocly-check markdown-check hygiene-check check

all: check

build:
	$(CARGO) build --locked --workspace

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

test:
	$(CARGO) test --locked --workspace --all-targets

migration-test:
	$(PYTHON_NO_BYTECODE) tests/migration_contract_test.py
	$(PYTHON_NO_BYTECODE) tests/delivery_retirement_test.py

contract-test:
	$(PYTHON_NO_BYTECODE) -m unittest tests/contract_gate_test.py

bootstrap-test: build
	$(PYTHON_NO_BYTECODE) tests/s4_bootstrap_test.py

records-test: build
	$(PYTHON_NO_BYTECODE) tests/s5_records_test.py

inbox-test: build
	$(PYTHON_NO_BYTECODE) -m unittest tests.s4_bootstrap_test.BootstrapTest.test_registered_principal_inbox_cli_without_adapter

recovery-test: build
	$(PYTHON_NO_BYTECODE) tests/recovery_cli_test.py

inbox-client-conformance:
	$(PYTHON_NO_BYTECODE) scripts/test_inbox_client_conformance.py
	CARGO="$(CARGO)" $(PYTHON_NO_BYTECODE) scripts/inbox_client_conformance.py

browser-security:
	$(CARGO) build --locked -p journald --example web_fixture
	$(PYTHON_NO_BYTECODE) tests/web_browser_test.py

# Explicit privileged harness; never count an unprivileged skip as evidence.
foreign-uid-test:
	$(PYTHON_NO_BYTECODE) tests/s4_foreign_uid_test.py

clippy:
	$(CARGO) clippy --locked --workspace --all-targets -- -D warnings

openapi-check:
	$(PYTHON_NO_BYTECODE) scripts/validate_openapi.py $(OPENAPI_FILE)

redocly-check:
	@if [ "$(OPENAPI_STANDARDS_LINT)" = "1" ]; then \
		npx --yes @redocly/cli@1.34.3 lint $(OPENAPI_FILE) --config=redocly.yaml; \
	else \
		echo "OpenAPI standards lint skipped (set OPENAPI_STANDARDS_LINT=1 to run pinned Redocly)"; \
	fi

markdown-check:
	@if command -v markdownlint >/dev/null 2>&1; then markdownlint '**/*.md'; else echo "markdownlint not installed; skipped"; fi

hygiene-check:
	$(PYTHON_NO_BYTECODE) scripts/public_hygiene.py

check: fmt-check test clippy build migration-test contract-test bootstrap-test records-test inbox-test recovery-test inbox-client-conformance openapi-check redocly-check markdown-check hygiene-check
