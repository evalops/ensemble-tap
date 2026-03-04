# Ensemble Tap

Ensemble Tap is a standalone Go service that ingests SaaS webhook events, normalizes them into CloudEvents, publishes them to NATS JetStream, and optionally persists events into ClickHouse.

## Implemented Scope

- Phase 1 webhook ingress for:
  - Stripe
  - GitHub
  - HubSpot
  - Linear
  - Shopify
  - Generic HMAC provider fallback
- CloudEvents normalization (`ensemble.tap.{provider}.{entity_type}.{action}`)
- NATS JetStream publisher with stream auto-provisioning and dedup IDs (`Nats-Msg-Id`)
- Optional ClickHouse sink consuming from NATS with batched inserts
- Health + observability:
  - `GET /livez`
  - `GET /readyz`
  - `GET /metrics`
- Config loading from YAML + `TAP_` env overrides
- Polling engine with checkpoint + snapshot diff event generation
- Provider pollers for:
  - HubSpot (CRM search by `hs_lastmodifieddate`)
  - Salesforce (SOQL `LastModifiedDate` fallback)
  - QuickBooks (`MetaData.LastUpdatedTime` query path)
  - Notion (`/v1/search` with pagination and edited-time filtering)

## Quickstart

1. Create config:

```bash
cp config.example.yaml config.yaml
# Fill in provider secrets and endpoints.
```

2. Run dependencies + tap:

```bash
docker compose up --build
```

3. Expose webhook URLs:

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

## Notes

- For HubSpot verification, Tap uses `X-HubSpot-Signature-v3` and reconstructs the signed URL from forwarded headers when present.
- ClickHouse is optional; leave `clickhouse.addr` empty to disable it.
