GO ?= go
MIN_COVERAGE ?= 75
STATICCHECK_VERSION ?= v0.6.1
STATICCHECK_BIN := $(shell $(GO) env GOPATH)/bin/staticcheck

.PHONY: ci-local test race vet staticcheck staticcheck-install coverage openapi config-lint chart-assert helm-lint helm-template onboard onboard-smoke

ci-local: vet test race staticcheck coverage openapi config-lint chart-assert helm-lint helm-template

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

staticcheck-install:
	GOTOOLCHAIN=go1.26.1 $(GO) install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)

staticcheck: staticcheck-install
	GOTOOLCHAIN=go1.26.1 $(STATICCHECK_BIN) ./...

coverage:
	$(GO) test ./... -coverprofile=/tmp/ensemble-tap.coverage.out
	$(GO) tool cover -func=/tmp/ensemble-tap.coverage.out
	@total="$$( $(GO) tool cover -func=/tmp/ensemble-tap.coverage.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}' )"; \
	echo "Total coverage: $${total}% (minimum: $(MIN_COVERAGE)%)"; \
	awk -v got="$$total" -v min="$(MIN_COVERAGE)" 'BEGIN { if (got + 0 < min + 0) { printf("coverage %.1f%% is below minimum %.1f%%\n", got, min); exit 1 } }'

openapi:
	$(GO) test ./cmd/tap -run TestAdminOpenAPIContractMatchesRuntime -count=1

config-lint:
	./scripts/lint-config.sh

chart-assert:
	./scripts/assert-chart-render.sh

helm-lint:
	helm lint charts/ensemble-tap

helm-template:
	helm template ensemble-tap charts/ensemble-tap >/dev/null

onboard:
	./scripts/bootstrap.sh

onboard-smoke:
	@if [ -z "$(ONBOARD_SECRET)" ]; then echo "ONBOARD_SECRET is required"; exit 1; fi
	./scripts/smoke-onboarding.sh --provider $(or $(ONBOARD_PROVIDER),generic) --release $(or $(ONBOARD_RELEASE),ensemble-tap) --namespace $(or $(ONBOARD_NAMESPACE),ensemble) --secret "$(ONBOARD_SECRET)"
