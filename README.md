# Ensemble Tap

Ensemble Tap is a standalone Go service that ingests SaaS webhook events, normalizes them into CloudEvents, publishes them to NATS JetStream, and optionally persists events into ClickHouse.

## Implemented Scope

- Webhook ingress for Stripe, GitHub, HubSpot, Linear, Shopify, and generic HMAC providers.
- Multi-tenant webhook routing via `POST /webhooks/{provider}` and `POST /webhooks/{provider}/{tenant}`.
- Polling engine with provider pollers for HubSpot, Salesforce, QuickBooks, and Notion.
- Poll-mode supports tenant fan-out from provider tenant overrides with tenant-scoped state tracking.
- Poll config supports per-tenant `poll_interval`, `poll_rate_limit_per_sec`, `poll_burst`, `poll_failure_budget`, `poll_circuit_break_duration`, and `poll_jitter_ratio` overrides.
- Durable poll state backends (`memory` or `sqlite`).
- CloudEvents normalization and schema validation (`tapversion=v1`).
- NATS JetStream publisher with dedup IDs and optional tenant-scoped subjects.
- Optional ClickHouse sink consuming from NATS with batched inserts.
- Dead-letter queue recording for verification/normalization/publish failures.
- Admin DLQ replay endpoints:
  - `POST /admin/replay-dlq?limit=100` creates async replay jobs (with `dry_run` and idempotency support).
  - `GET /admin/replay-dlq/{job_id}` fetches replay job status/results.
- Admin poller runtime status endpoint: `GET /admin/poller-status` guarded by `X-Admin-Token`, with optional `provider` and `tenant` filters.
- Health and observability endpoints:
  - `GET /livez`
  - `GET /readyz`
  - `GET /metrics`

## Quickstart

1. Create config:

```bash
cp config.example.yaml config.yaml
```

2. Start dependencies + tap:

```bash
docker compose up --build
```

3. Send webhooks:

- `POST /webhooks/stripe`
- `POST /webhooks/github`
- `POST /webhooks/hubspot`
- `POST /webhooks/linear`
- `POST /webhooks/shopify`

## Local Development

```bash
go test ./...
go run ./cmd/tap -config ./config.yaml
```

## Poll State Backends

- `state.backend=memory` keeps checkpoints/snapshots in memory.
- `state.backend=sqlite` persists poll state to `state.sqlite_path`.

## Admin Endpoints

When `server.admin_token` is set, these endpoints are available:

- `POST /admin/replay-dlq?limit=100`
  - Requires header `X-Admin-Token`.
  - Supports token rotation with `server.admin_token_secondary` (either primary or secondary token is accepted). `admin_token_secondary` requires `admin_token`, and both values must differ.
  - Optional header `Idempotency-Key` to reuse an existing replay job instead of creating duplicates.
  - Optional header `X-Request-ID` (echoed back in `X-Request-ID` response header and `request_id` body field).
  - Error responses are JSON (`{"request_id":"...","error":"..."}`) for consistent automation and audit correlation.
  - Admin endpoints are token-bucket rate-limited (`server.admin_rate_limit_per_sec`, `server.admin_rate_limit_burst`) and return `429` with `Retry-After`.
  - Optional guardrails: CIDR allowlist (`server.admin_allowed_cidrs`) and client-cert requirement (`server.admin_mtls_required`, `server.admin_mtls_client_cert_header`).
  - `limit` must be a positive integer.
  - `dry_run=true` computes replayable count without consuming DLQ entries.
  - Replay is capped by `server.admin_replay_max_limit` (default `2000`, valid range `1..100000`); accepted response includes replay job metadata (`job_id`, `status`, `effective_limit`, `max_limit`, `capped`, `dry_run`).
- `GET /admin/replay-dlq/{job_id}`
  - Requires header `X-Admin-Token`.
  - Returns current replay job state (`queued`, `running`, `succeeded`, `failed`) and result fields (`replayed`, `error`).
- `GET /admin/poller-status`
  - Requires header `X-Admin-Token`.
  - Supports token rotation with `server.admin_token_secondary`.
  - Optional header `X-Request-ID` (echoed back in `X-Request-ID` response header and `request_id` body field).
  - Error responses are JSON (`{"request_id":"...","error":"..."}`).
  - Optional filters: `provider` (case-insensitive), `tenant`.
  - Response includes `count` and per-poller runtime fields (`interval`, rate limiter values, failure budget, circuit-break duration, jitter ratio, last run/success/error details).
  - Structured audit logs are emitted for authorized and unauthorized admin calls (`request_id`, requester IP, user-agent, path/method, and duration).
  - Prometheus metrics include `tap_admin_requests_total{endpoint,outcome}` and `tap_admin_request_duration_seconds{endpoint,outcome}`.
  - Poller health metrics include `tap_poller_stuck{provider,tenant}` and `tap_poller_consecutive_failures{provider,tenant}`.

## Admin API Contract

- OpenAPI contract: `docs/admin-openapi.yaml`

```bash
# Replay DLQ with explicit request id
curl -i -X POST 'http://localhost:8080/admin/replay-dlq?limit=50' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'Idempotency-Key: replay-tenant-a-20260304' \
  -H 'X-Request-ID: replay-manual-001'

# Replay dry-run
curl -i -X POST 'http://localhost:8080/admin/replay-dlq?limit=200&dry_run=true' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Request-ID: replay-dry-run-001'

# Replay job status
curl -i 'http://localhost:8080/admin/replay-dlq/replay_1234567890_1' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Request-ID: replay-status-001'

# Poller status (filtered)
curl -i 'http://localhost:8080/admin/poller-status?provider=hubspot&tenant=tenant-a' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Request-ID: status-manual-001'

# Example error payload (401/429/400/500):
# {"request_id":"status-manual-001","error":"rate limit exceeded"}
```

## Kubernetes

Install with Helm:

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --create-namespace
```

See chart-specific usage in `charts/ensemble-tap/README.md`.

## Release and Operations

- CI workflow validates unit tests, OpenAPI contract/runtime parity, Docker build, Helm chart render/lint, and real NATS+ClickHouse integration.
- CI security gates run `gosec`, `govulncheck`, Trivy CRITICAL scan, and source SBOM generation.
- CI performance smoke gate runs a lightweight `k6` probe against `/livez` and `/readyz`.
- Tag pushes matching `v*` trigger release workflow to publish multi-arch images to GHCR, generate source/image SBOMs, sign image digests with cosign keyless OIDC, and package the Helm chart.
- Branch protection can be applied with `.github/scripts/apply_branch_protection.sh` or via the manual `Branch Protection` workflow.
