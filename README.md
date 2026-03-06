# Ensemble Tap

Ensemble Tap is a standalone Go service that ingests SaaS webhook events, normalizes them into CloudEvents, publishes them to NATS JetStream, and optionally persists events into ClickHouse.

## Implemented Scope

- Webhook ingress for Stripe, GitHub, HubSpot, Linear, Shopify, and generic HMAC providers.
- Multi-tenant webhook routing via `POST /webhooks/{provider}` and `POST /webhooks/{provider}/{tenant}`.
- Polling engine with provider pollers for HubSpot, Salesforce, QuickBooks, and Notion.
- Poll-mode supports tenant fan-out from provider tenant overrides with tenant-scoped state tracking.
- Poll config supports per-tenant `poll_interval`, `poll_rate_limit_per_sec`, `poll_burst`, `poll_max_pages`, `poll_max_requests`, `poll_failure_budget`, `poll_circuit_break_duration`, and `poll_jitter_ratio` overrides.
- Poll fetch telemetry includes request/page counts and truncation metrics for bounded fetch budgets.
- Durable poll state backends (`memory` or `sqlite`).
- CloudEvents normalization and schema validation (`tapversion=v1`).
- NATS JetStream publisher with dedup IDs and optional tenant-scoped subjects.
- Ingress request IDs propagate through CloudEvents (`taprequestid`/`request_id`), NATS headers (`X-Request-ID`), and DLQ records.
- Optional ClickHouse sink consuming from NATS with batched inserts.
- Dead-letter queue recording for verification/normalization/publish failures.
- Admin DLQ replay endpoints:
  - `GET /admin/replay-dlq` lists replay jobs with optional `status` and `limit` filters.
  - `POST /admin/replay-dlq?limit=100` creates async replay jobs (with `dry_run` and idempotency support).
  - `GET /admin/replay-dlq/{job_id}` fetches replay job status/results.
  - `DELETE /admin/replay-dlq/{job_id}` cancels queued replay jobs.
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

Configure provider credentials (or remove providers you are not enabling) before startup; runtime now validates webhook/poll provider configs at boot.

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

## Simplest Onboarding Path (Helm)

Use the guided bootstrap script to get from install to first accepted event with one flow:

```bash
./scripts/bootstrap.sh
```

What it does:

- prompts for provider + webhook secret,
- optionally wires ClickHouse username/password secrets for sink mode,
- generates `values.onboarding.yaml`,
- creates/updates a Kubernetes Secret with webhook credentials,
- installs/upgrades the Helm release with `--wait`,
- runs a signed provider-aware webhook smoke test.

Run smoke test only (for an existing install):

```bash
./scripts/smoke-onboarding.sh --provider generic --release ensemble-tap --namespace ensemble --secret '<your-secret>'
```

## Local Development

```bash
go test ./...
go run ./cmd/tap -config ./config.yaml
make ci-local
```

## Poll State Backends

- `state.backend=memory` keeps checkpoints/snapshots in memory.
- `state.backend=sqlite` persists poll state to `state.sqlite_path`.

## NATS + ClickHouse Tuning

- `nats.url` accepts a comma-separated endpoint list; each endpoint must use `nats|tls|ws|wss` and a valid host (and optional valid port).
- `nats.connect_timeout`, `nats.reconnect_wait`, `nats.max_reconnects`, and `nats.publish_timeout` tune connection and publish behavior.
- `nats.publish_max_retries` and `nats.publish_retry_backoff` tune publish retry resilience for transient JetStream errors.
- `nats.username/password`, `nats.token`, and `nats.creds_file` are mutually exclusive auth modes.
- `nats.secure`, `nats.insecure_skip_verify`, `nats.ca_file`, `nats.cert_file`, and `nats.key_file` tune NATS TLS and optional mTLS.
- `nats.stream_replicas`, `nats.stream_storage` (`file|memory`), and `nats.stream_discard` (`old|new`) tune JetStream durability and pressure behavior.
- `nats.stream_max_consumers` and `nats.stream_max_msgs_per_subject` cap consumer and per-subject cardinality at stream level (`0` keeps JetStream defaults/unlimited behavior).
- `nats.stream_compression` (`none|s2`) and `nats.stream_allow_msg_ttl` control JetStream storage compression and message TTL support.
- `nats.stream_max_msgs`, `nats.stream_max_bytes`, and `nats.stream_max_msg_size` apply stream-level retention and message-size limits.
- `clickhouse.username`/`clickhouse.password`, `clickhouse.secure`, and `clickhouse.insecure_skip_verify` tune ClickHouse auth/TLS.
- `clickhouse.tls_server_name`, `clickhouse.ca_file`, `clickhouse.cert_file`, and `clickhouse.key_file` support ClickHouse TLS verification and optional mTLS.
- `clickhouse.max_open_conns`, `clickhouse.max_idle_conns`, and `clickhouse.conn_max_lifetime` tune connection pool behavior.
- `clickhouse.consumer_name`, `clickhouse.consumer_fetch_batch_size`, `clickhouse.consumer_fetch_max_wait`, `clickhouse.consumer_ack_wait`, `clickhouse.consumer_max_ack_pending`, and `clickhouse.insert_timeout` tune sink throughput and ack latency.
- `clickhouse.consumer_max_deliver`, `clickhouse.consumer_backoff`, `clickhouse.consumer_max_waiting`, and `clickhouse.consumer_max_request_max_bytes` tune pull-consumer redelivery, retry timing, and pull pressure limits.
- `clickhouse.addr` entries must be valid `host:port` values with ports in `1..65535`.
- To reduce redelivery churn, keep `clickhouse.consumer_fetch_max_wait < clickhouse.consumer_ack_wait` and `clickhouse.insert_timeout + clickhouse.flush_interval < clickhouse.consumer_ack_wait`.
- `clickhouse.retention_ttl` controls MergeTree TTL for event-time retention at table level.
- ClickHouse sink de-duplicates within each batch and skips IDs already present in ClickHouse before insert; skipped rows are exposed via `tap_clickhouse_dedup_skipped_total`.
- NATS publish retries and JetStream advisories are exposed via `tap_nats_publish_retries_total{reason}`, `tap_nats_publish_retry_delay_seconds`, and `tap_jetstream_advisories_total{kind}`.

## Vault-backed secrets

Any string config value can be sourced from Vault by using a `vault://` reference:

- Format: `vault://<path>#<key>` (defaults to key `value` when `#<key>` is omitted).
- Example: `providers.generic.secret: vault://secret/data/homelab/ensemble-tap/runtime#generic-webhook-secret`
- Example: `server.admin_token: vault://secret/data/homelab/ensemble-tap/runtime#admin-token`

Vault auth config lives under `vault.*`:

- `vault.address` (or `VAULT_ADDR`) and optional `vault.namespace`
- `vault.auth_method`: `kubernetes` (default) or `token`
- `vault.auth_method=kubernetes`: set `vault.kubernetes_role`, optional `vault.kubernetes_mount_path` (default `kubernetes`), and optional `vault.kubernetes_jwt_file` (default `/var/run/secrets/kubernetes.io/serviceaccount/token`)
- `vault.auth_method=token`: set `vault.token`, `vault.token_file`, or `VAULT_TOKEN`

In Kubernetes, if you use `vault.auth_method=kubernetes`, ensure the Pod has a service-account JWT available (for Helm chart: set `serviceAccount.automount=true` or mount a projected token and set `vault.kubernetes_jwt_file` accordingly).

## Admin Endpoints

When any admin token is configured (`server.admin_token`, `server.admin_token_secondary`, `server.admin_token_read`, `server.admin_token_replay`, `server.admin_token_cancel`), these endpoints are available:

- `POST /admin/replay-dlq?limit=100`
  - Requires header `X-Admin-Token` with replay permission (`server.admin_token` / `server.admin_token_secondary` or `server.admin_token_replay`).
  - Supports token rotation with `server.admin_token_secondary` (`admin_token_secondary` requires `admin_token`, and both values must differ).
  - Optional least-privilege role tokens:
    - `server.admin_token_read`: read/list/status access.
    - `server.admin_token_replay`: replay submit access.
    - `server.admin_token_cancel`: cancel access.
  - Optional header `Idempotency-Key` to reuse an existing equivalent replay job instead of creating duplicates (`409` if reused with different `limit`/`dry_run` parameters).
  - Optional/required (configurable) header `X-Admin-Reason`:
    - Required when `server.admin_replay_require_reason=true`.
    - Minimum length enforced by `server.admin_replay_reason_min_length` (default `12`).
  - Optional header `X-Request-ID` (echoed back in `X-Request-ID` response header and `request_id` body field).
  - Error responses are JSON (`{"request_id":"...","error":"..."}`) for consistent automation and audit correlation.
  - Admin endpoints are token-bucket rate-limited (`server.admin_rate_limit_per_sec`, `server.admin_rate_limit_burst`) and return `429` with `Retry-After`.
  - Optional guardrails: CIDR allowlist (`server.admin_allowed_cidrs`) and client-cert requirement (`server.admin_mtls_required`, `server.admin_mtls_client_cert_header`).
  - `limit` must be a positive integer.
  - `dry_run=true` computes replayable count without consuming DLQ entries.
  - Replay is capped by `server.admin_replay_max_limit` (default `2000`, valid range `1..100000`); accepted response includes replay job metadata (`job_id`, `status`, `effective_limit`, `max_limit`, `capped`, `dry_run`).
  - Replay job metadata retention/capacity is configurable (`server.admin_replay_job_ttl`, `server.admin_replay_job_max_jobs`), and backend is configurable (`server.admin_replay_store_backend=memory|sqlite`, `server.admin_replay_sqlite_path`).
  - Replay execution is configurable (`server.admin_replay_job_timeout`, `server.admin_replay_max_concurrent_jobs`) for bounded runtime and concurrency.
  - Queue fan-out safety rails are configurable (`server.admin_replay_max_queued_per_ip`, `server.admin_replay_max_queued_per_token`) and return `409` when exceeded.
- `GET /admin/replay-dlq`
  - Requires header `X-Admin-Token` with read permission (`admin_token`/`admin_token_secondary`/`admin_token_read`/`admin_token_replay`/`admin_token_cancel`).
  - Optional query params: `status` (`queued|running|succeeded|failed|cancelled`), `limit` (max `500`, default `50`), and `cursor` (from prior `next_cursor`).
  - Returns replay job list plus per-status summary counts and pagination cursors for queue introspection.
- `GET /admin/replay-dlq/{job_id}`
  - Requires header `X-Admin-Token` with read permission.
  - Returns current replay job state (`queued`, `running`, `succeeded`, `failed`, `cancelled`) and result fields (`replayed`, `error`, `operator_reason`, `cancel_reason`).
- `DELETE /admin/replay-dlq/{job_id}`
  - Requires header `X-Admin-Token` with cancel permission (`server.admin_token`/`server.admin_token_secondary` or `server.admin_token_cancel`).
  - Optional/required (configurable) header `X-Admin-Reason` (same requirement rules as replay endpoint).
  - Cancels replay jobs that are still `queued`; returns `409` if the job is already `running` or completed.
- `GET /admin/poller-status`
  - Requires header `X-Admin-Token` with read permission.
  - Supports token rotation with `server.admin_token_secondary`.
  - Optional header `X-Request-ID` (echoed back in `X-Request-ID` response header and `request_id` body field).
  - Error responses are JSON (`{"request_id":"...","error":"..."}`).
  - Optional filters: `provider` (case-insensitive), `tenant`.
  - Response includes `count` and per-poller runtime fields (`interval`, rate limiter values, failure budget, circuit-break duration, jitter ratio, last run/success/error details).
  - Structured audit logs are emitted for authorized and unauthorized admin calls (`request_id`, requester IP, user-agent, path/method, and duration).
  - Prometheus metrics include `tap_admin_requests_total{endpoint,outcome}` and `tap_admin_request_duration_seconds{endpoint,outcome}`.
  - Replay lifecycle metrics include `tap_admin_replay_jobs_total{stage}` and `tap_admin_replay_jobs_in_flight`.
  - Poller health metrics include `tap_poller_stuck{provider,tenant}` and `tap_poller_consecutive_failures{provider,tenant}`.

## Admin API Contract

- OpenAPI contract: `docs/admin-openapi.yaml`

```bash
# Replay DLQ with explicit request id
curl -i -X POST 'http://localhost:8080/admin/replay-dlq?limit=50' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Admin-Reason: replay after incident #1234' \
  -H 'Idempotency-Key: replay-tenant-a-20260304' \
  -H 'X-Request-ID: replay-manual-001'

# Replay dry-run
curl -i -X POST 'http://localhost:8080/admin/replay-dlq?limit=200&dry_run=true' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Admin-Reason: estimate replay volume before run' \
  -H 'X-Request-ID: replay-dry-run-001'

# Replay job list (latest succeeded jobs)
curl -i 'http://localhost:8080/admin/replay-dlq?status=succeeded&limit=20' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Request-ID: replay-list-001'

# Replay job list next page
curl -i 'http://localhost:8080/admin/replay-dlq?status=succeeded&limit=20&cursor=<next_cursor_from_previous_response>' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Request-ID: replay-list-002'

# Replay job status
curl -i 'http://localhost:8080/admin/replay-dlq/replay_1234567890_1' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Request-ID: replay-status-001'

# Cancel queued replay job
curl -i -X DELETE 'http://localhost:8080/admin/replay-dlq/replay_1234567890_1' \
  -H 'X-Admin-Token: your-admin-token' \
  -H 'X-Admin-Reason: duplicate replay request queued' \
  -H 'X-Request-ID: replay-cancel-001'

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

Notes:
- Chart default keeps ClickHouse sink disabled (`config.clickhouse.addr: ""`) for simpler initial onboarding.
- NetworkPolicy automatically allows NATS/ClickHouse TCP egress ports derived from chart config (`networkPolicy.allowConfigPorts=true`).
- Image pinning by digest is supported via `image.digest` (uses `repository@sha256:...`).

See chart-specific usage in `charts/ensemble-tap/README.md`.

## Release and Operations

- CI workflow validates unit tests, static analysis (`staticcheck`), OpenAPI contract/runtime parity, Docker build, Helm chart render/lint, and real NATS+ClickHouse integration.
- CI security gates run `gosec`, `govulncheck`, Trivy CRITICAL scan, and source SBOM generation.
- CI performance smoke gate runs a lightweight `k6` probe against `/livez` and `/readyz`.
- Tag pushes matching `v*` trigger release workflow to publish multi-arch images to GHCR, generate source/image SBOMs, sign image digests with cosign keyless OIDC, and package the Helm chart.
- Branch protection can be applied with `.github/scripts/apply_branch_protection.sh` or via the manual `Branch Protection` workflow.
