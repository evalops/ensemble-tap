# AGENTS.md

This file is the operating guide for coding agents working in `siphon`.

## 1. Mission and Scope

- Build and maintain a production-safe ingestion service that:
  - receives provider webhooks,
  - normalizes to CloudEvents,
  - publishes to NATS JetStream,
  - optionally sinks to ClickHouse,
  - exposes replay/admin operations and metrics.
- Keep behavior deterministic, observable, and safe under retries, duplicates, and transient outages.

## 2. Repo Map

- `cmd/tap`
  - process entrypoint and runtime wiring (`main.go`, `run.go`)
  - admin endpoints and runtime registries
- `config`
  - config structs, defaults, validation, env overrides (`TAP_` prefix)
- `internal/ingress`
  - HTTP webhook ingress and provider verification
- `internal/normalize`
  - provider payload to normalized event + CloudEvent conversion
- `internal/publish`
  - NATS publisher and ClickHouse sink
- `internal/poller`
  - poll-mode providers and scheduling/failure-budget logic
- `internal/dlq`
  - dead-letter queue publication/replay support
- `internal/health`
  - liveness/readiness handlers and Prometheus metrics
- `charts/siphon`
  - Helm chart values, schema, templates
- `docs/admin-openapi.yaml`
  - admin API contract expected to match runtime behavior

## 3. Runtime Data Flow

1. Ingress receives webhook at `/webhooks/{provider}` or `/webhooks/{provider}/{tenant}`.
2. Provider signature/auth verification executes.
3. Payload is normalized into internal event shape and CloudEvent.
4. CloudEvent is published to NATS JetStream with dedupe headers.
5. Optional ClickHouse sink consumes from JetStream and batch-inserts.
6. Failures in verify/normalize/publish can be recorded to DLQ.
7. Admin APIs can inspect/replay DLQ and inspect poller runtime status.

## 4. Toolchain and Baseline Commands

- Go toolchain: as defined in CI (`go1.26.2` via `GOTOOLCHAIN`).
- Core local commands:
  - `go test ./...`
  - `go test -race ./...`
  - `go vet ./...`
  - `make ci-local`
- CI parity command:
  - `make ci-local`
  - includes: vet, tests, race, staticcheck, coverage gate, OpenAPI contract test, Helm lint/template.

## 5. CI Contract (Do Not Break)

`/.github/workflows/ci.yml` enforces these gates:

- `test`: `go vet` + coverage threshold (minimum `75%`).
- `staticcheck`
- `openapi-contract`: `TestAdminOpenAPIContractMatchesRuntime`
- `config-lint`: runtime/chart config lint + Helm render hardening assertions
- `docker-build`
- `helm-lint` + `helm template`
- `integration`: real NATS + ClickHouse integration test
- `perf-smoke`: k6 smoke against `/livez` and `/readyz`
- `security`: `gosec`, `govulncheck`, Trivy CRITICAL scan, SBOM generation
- `flake-tracker`: per-run `integration`/`perf-smoke` status + duration artifact trend
- failed `main` runs auto-open a triage issue with failed job log snippets

If your change affects behavior, assume at least one of these can fail and run the relevant subset locally before committing.

## 6. Definition of Done for Any Code Change

A change is not done until all apply:

1. Code compiles and targeted tests pass.
2. `go test ./...` passes.
3. `make ci-local` passes.
4. Documentation/config surfaces are updated when needed.
5. No regressions in API contract, observability, or security posture.

## 7. Required Update Matrix by Change Type

### 7.1 Adding or changing config fields

Always update all relevant surfaces:

- `config/config.go`
  - struct field
  - defaults in `ApplyDefaults()`
  - validation in `Validate()` and helpers
- `config/config_test.go`
  - defaults
  - valid and invalid cases
  - env override behavior if applicable
- `config.example.yaml`
- `charts/siphon/values.yaml`
- `charts/siphon/values.schema.json`
- docs:
  - root `README.md`
  - chart `charts/siphon/README.md` when Helm-visible

### 7.2 Changing admin API behavior

- Update runtime handlers in `cmd/tap/*`.
- Keep `docs/admin-openapi.yaml` in sync.
- Run:
  - `go test ./cmd/tap -run TestAdminOpenAPIContractMatchesRuntime -count=1`

### 7.3 Changing NATS or ClickHouse publish/sink behavior

- Update runtime code in `internal/publish`.
- Add/adjust tests in:
  - `internal/publish/nats_integration_test.go`
  - `internal/publish/clickhouse_test.go`
  - `internal/publish/real_integration_test.go` if behavior spans dependencies
- Validate observability impact in `internal/health/metrics.go` and tests/docs.

### 7.4 Changing Helm templates/values

- Validate locally:
  - `helm lint charts/siphon`
  - `helm template siphon charts/siphon >/dev/null`

## 8. Critical Invariants

### 8.1 NATS invariants

- `subject_prefix` must not contain wildcards or whitespace.
- Dedupe window must be positive and `<= max_age`.
- Auth modes are mutually exclusive:
  - username/password
  - token
  - creds file
- TLS flags/files must obey validation constraints.
- Stream constraints must remain valid:
  - storage in `file|memory`
  - discard in `old|new`
  - compression in `none|s2`
  - max-size and count fields non-negative and bounded where required

### 8.2 ClickHouse invariants

- `addr` entries must be valid `host:port`.
- TLS settings require `secure=true` where applicable.
- consumer timings and pool settings must stay positive/consistent.
- `insert_timeout < consumer_ack_wait`.
- `consumer_backoff` values must be positive, non-decreasing.
- if both configured: `consumer_max_deliver == len(consumer_backoff)`.

### 8.3 Admin invariants

- Role-scoped tokens and rotation behavior must remain coherent.
- Rate limiting and optional CIDR/mTLS guards must continue to apply.
- Replay queue/job limits must remain bounded and validated.

## 9. Observability Requirements

Do not add major behavior without metrics and tests.

Current high-value metrics include:

- ingress: received, verification failures, processing duration
- publish: published, failures, dedupe hits, retry counters and delay histogram
- JetStream advisories by kind
- ClickHouse dedupe skipped counter
- admin request/replay lifecycle metrics
- poller health and fetch budget metrics

When adding metrics:

- register in `internal/health/metrics.go`
- add/extend tests in `internal/health/metrics_test.go`
- document in README if user-facing/operationally important

## 10. Testing Strategy

- Prefer table-driven tests for validation logic.
- For pure logic, write narrow unit tests.
- For transport behavior (NATS/ClickHouse), prefer integration-style tests with controlled mocks or local servers.
- Keep tests deterministic and bounded with explicit timeouts.
- If coverage drops near threshold, add tests in changed package first.

Helpful coverage drill-down:

```bash
go test ./... -coverprofile=/tmp/siphon.coverage.out
go tool cover -func=/tmp/siphon.coverage.out
```

## 11. Performance and Reliability Guidance

- Preserve idempotency and dedupe behavior end-to-end.
- Avoid unbounded retries, queues, or backoff growth.
- Keep publish/sink timeouts explicit and context-driven.
- Treat reconnects and transient network failures as normal; instrument retry paths.

## 12. Security Expectations

- Maintain current CI security posture (gosec/govulncheck/trivy/SBOM).
- Avoid weakening TLS defaults without explicit config gate.
- Keep secret handling via env references and mounted files; avoid logging secrets.
- Maintain auth checks and scope checks on admin endpoints.

## 13. Local Dependency Commands

Bring up local stack:

```bash
docker compose up --build
```

NATS and ClickHouse images are pinned in repo-managed configs and CI; keep version updates deliberate and consistent across:

- `docker-compose.yml`
- `.github/workflows/ci.yml`
- `go.mod` (client/server libraries, where applicable)

## 14. Release Notes

Tag-based release workflow (`v*`) builds/pushes multi-arch image, generates SBOMs, signs artifacts with cosign, and packages Helm chart. Ensure release-impacting changes (image behavior, chart values, runtime flags) are documented.

## 15. Agent Work Style Expectations

- Make minimal, targeted diffs.
- Keep changes consistent with existing code style and naming.
- Do not silently change API/contract behavior without tests and docs.
- Prefer fixing root cause over introducing bypass flags.
- If a change touches multiple layers (runtime/config/chart/docs), complete all layers in one pass.

## 16. Pre-Commit Checklist

Run before pushing:

```bash
go test ./...
make ci-local
```

Then verify clean state:

```bash
git status --short
```

Only commit when the tree is clean except intended changes.
