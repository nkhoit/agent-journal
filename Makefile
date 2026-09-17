SHELL := /bin/sh
CARGO ?= cargo
PYTHON ?= python3
OPENAPI_FILE := api/openapi.yaml
OPENAPI_STANDARDS_LINT ?= 0

.PHONY: all build fmt fmt-check test migration-test clippy openapi-check redocly-check markdown-check hygiene-check check

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
	$(PYTHON) tests/migration_contract_test.py

clippy:
	$(CARGO) clippy --locked --workspace --all-targets -- -D warnings

openapi-check:
	$(PYTHON) scripts/validate_openapi.py $(OPENAPI_FILE)

redocly-check:
	@if [ "$(OPENAPI_STANDARDS_LINT)" = "1" ]; then \
		npx --yes @redocly/cli@1.34.3 lint $(OPENAPI_FILE) --config=redocly.yaml; \
	else \
		echo "OpenAPI standards lint skipped (set OPENAPI_STANDARDS_LINT=1 to run pinned Redocly)"; \
	fi

markdown-check:
	@if command -v markdownlint >/dev/null 2>&1; then markdownlint '**/*.md'; else echo "markdownlint not installed; skipped"; fi

hygiene-check:
	$(PYTHON) scripts/public_hygiene.py

check: fmt-check test clippy build migration-test openapi-check redocly-check markdown-check hygiene-check
