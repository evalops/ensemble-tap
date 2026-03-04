# Ensemble Tap

Ensemble Tap is a standalone Go service that ingests SaaS webhook events, normalizes them into CloudEvents, publishes them to NATS JetStream, and optionally persists events into ClickHouse.

## Implemented Scope

- Webhook ingress for Stripe, GitHub, HubSpot, Linear, Shopify, and generic HMAC providers.
- Multi-tenant webhook routing via `POST /webhooks/{provider}` and `POST /webhooks/{provider}/{tenant}`.
- Polling engine with provider pollers for HubSpot, Salesforce, QuickBooks, and Notion.
- Poll-mode supports tenant fan-out from provider tenant overrides with tenant-scoped state tracking.
- Poll config supports per-tenant `poll_interval`, `poll_rate_limit_per_sec`, and `poll_burst` overrides.
- Durable poll state backends (`memory` or `sqlite`).
- CloudEvents normalization and schema validation (`tapversion=v1`).
- NATS JetStream publisher with dedup IDs and optional tenant-scoped subjects.
- Optional ClickHouse sink consuming from NATS with batched inserts.
- Dead-letter queue recording for verification/normalization/publish failures.
- Admin DLQ replay endpoint: `POST /admin/replay-dlq?limit=100` guarded by `X-Admin-Token`.
- Admin poller runtime status endpoint: `GET /admin/poller-status` guarded by `X-Admin-Token`.
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

## Kubernetes

Install with Helm:

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --create-namespace
```

See chart-specific usage in `charts/ensemble-tap/README.md`.

## Release and Operations

- CI workflow validates unit tests, Docker build, Helm chart render/lint, and real NATS+ClickHouse integration.
- Tag pushes matching `v*` trigger release workflow to publish multi-arch images to GHCR and package the Helm chart.
- Branch protection can be applied with `.github/scripts/apply_branch_protection.sh` or via the manual `Branch Protection` workflow.
