GO ?= go
MIN_COVERAGE ?= 75
STATICCHECK_VERSION ?= v0.6.1
STATICCHECK_BIN := $(shell $(GO) env GOPATH)/bin/staticcheck
GOLANGCI_LINT_VERSION ?= v2.11.3
GOLANGCI_LINT_BIN := $(shell $(GO) env GOPATH)/bin/golangci-lint

.PHONY: ci-local test race vet lint lint-install staticcheck staticcheck-install coverage openapi proto proto-lint config-lint chart-assert helm-lint helm-template onboard onboard-smoke setup-hooks install-hooks proto-check

ci-local: vet test race lint staticcheck coverage openapi proto-lint config-lint chart-assert helm-lint helm-template

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

lint-install:
	GOTOOLCHAIN=go1.26.2 $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

lint: lint-install
	GOTOOLCHAIN=go1.26.2 $(GOLANGCI_LINT_BIN) run ./...

staticcheck-install:
	GOTOOLCHAIN=go1.26.2 $(GO) install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)

staticcheck: staticcheck-install
	GOTOOLCHAIN=go1.26.2 $(STATICCHECK_BIN) ./...

coverage:
	$(GO) test ./... -coverprofile=/tmp/siphon.coverage.out
	@{ \
		head -n 1 /tmp/siphon.coverage.out; \
		tail -n +2 /tmp/siphon.coverage.out | grep -Ev '(^|/).*(\.pb|\.connect)\.go:'; \
	} >/tmp/siphon.coverage.filtered.out
	$(GO) tool cover -func=/tmp/siphon.coverage.filtered.out
	@total="$$( $(GO) tool cover -func=/tmp/siphon.coverage.filtered.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}' )"; \
	echo "Total coverage: $${total}% (minimum: $(MIN_COVERAGE)%)"; \
	awk -v got="$$total" -v min="$(MIN_COVERAGE)" 'BEGIN { if (got + 0 < min + 0) { printf("coverage %.1f%% is below minimum %.1f%%\n", got, min); exit 1 } }'

openapi:
	$(GO) test ./cmd/tap -run TestAdminOpenAPIContractMatchesRuntime -count=1

proto:
	buf generate

proto-lint:
	buf lint

proto-check: proto
	@if git diff --quiet -- 'proto/**/*.pb.go' 'proto/**/*connect.go'; then \
		echo "ok: generated proto code is up to date"; \
	else \
		echo "error: proto generated code is stale — run 'make proto' and commit"; \
		git diff --stat -- 'proto/**/*.pb.go' 'proto/**/*connect.go'; \
		exit 1; \
	fi

setup-hooks:
	git config core.hooksPath scripts
	@echo "ok: git hooks configured to use scripts/"

install-hooks: setup-hooks

config-lint:
	./scripts/lint-config.sh

chart-assert:
	./scripts/assert-chart-render.sh

helm-lint:
	helm lint charts/siphon

helm-template:
	helm template siphon charts/siphon >/dev/null

onboard:
	./scripts/bootstrap.sh

onboard-smoke:
	@if [ -z "$(ONBOARD_SECRET)" ]; then echo "ONBOARD_SECRET is required"; exit 1; fi
	./scripts/smoke-onboarding.sh --provider $(or $(ONBOARD_PROVIDER),generic) --release $(or $(ONBOARD_RELEASE),siphon) --namespace $(or $(ONBOARD_NAMESPACE),siphon) --secret "$(ONBOARD_SECRET)"
