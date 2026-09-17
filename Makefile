SHELL := /bin/sh

GO ?= go
OPENAPI_FILE := api/openapi.yaml
OPENAPI_STANDARDS_LINT ?= 0

.PHONY: all build fmt fmt-check test migration-test vet openapi-check markdown-check hygiene-check check

all: check

build:
	$(GO) build ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

fmt-check:
	@files="$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))"; \
	if [ -n "$$files" ]; then echo "gofmt required:"; echo "$$files"; exit 1; fi

test:
	$(GO) test ./...

migration-test:
	python3 tests/migration_contract_test.py

vet:
	$(GO) vet ./...

openapi-check:
	python3 scripts/validate_openapi.py $(OPENAPI_FILE)
	@if [ "$(OPENAPI_STANDARDS_LINT)" = "1" ]; then \
		command -v redocly >/dev/null 2>&1 || { echo "OPENAPI_STANDARDS_LINT=1 requires the pinned Redocly CLI"; exit 1; }; \
		redocly lint $(OPENAPI_FILE) --config=redocly.yaml; \
	else \
		echo "OpenAPI standards lint skipped (set OPENAPI_STANDARDS_LINT=1 when redocly is installed)"; \
	fi

markdown-check:
	@if command -v markdownlint >/dev/null 2>&1; then markdownlint '**/*.md'; else echo "markdownlint not installed; skipped"; fi

hygiene-check:
	python3 scripts/public_hygiene.py

check: fmt-check test migration-test vet openapi-check markdown-check hygiene-check
