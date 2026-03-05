# ensemble-tap Helm Chart

## Install

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --create-namespace
```

## Configure providers

The chart renders `config.yaml` from `.Values.config`. Provide provider secrets through environment variables referenced by the config.

Example:

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set env[0].name=STRIPE_WEBHOOK_SECRET \
  --set env[0].value=your-secret \
  --set config.providers.stripe.mode=webhook \
  --set config.providers.stripe.secret='${STRIPE_WEBHOOK_SECRET}'

# Optional admin rotation, scoped tokens, and replay safety settings
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set env[0].name=TAP_ADMIN_TOKEN \
  --set env[0].value=primary-token \
  --set env[1].name=TAP_ADMIN_TOKEN_SECONDARY \
  --set env[1].value=next-token \
  --set env[2].name=TAP_ADMIN_TOKEN_READ \
  --set env[2].value=read-token \
  --set env[3].name=TAP_ADMIN_TOKEN_REPLAY \
  --set env[3].value=replay-token \
  --set env[4].name=TAP_ADMIN_TOKEN_CANCEL \
  --set env[4].value=cancel-token \
  --set config.server.admin_token='${TAP_ADMIN_TOKEN}' \
  --set config.server.admin_token_secondary='${TAP_ADMIN_TOKEN_SECONDARY}' \
  --set config.server.admin_token_read='${TAP_ADMIN_TOKEN_READ}' \
  --set config.server.admin_token_replay='${TAP_ADMIN_TOKEN_REPLAY}' \
  --set config.server.admin_token_cancel='${TAP_ADMIN_TOKEN_CANCEL}' \
  --set config.server.admin_replay_max_limit=2000 \
  --set config.server.admin_replay_job_ttl=24h \
  --set config.server.admin_replay_job_max_jobs=512 \
  --set config.server.admin_replay_job_timeout=5m \
  --set config.server.admin_replay_max_concurrent_jobs=2 \
  --set config.server.admin_replay_store_backend=sqlite \
  --set config.server.admin_replay_sqlite_path=/var/lib/ensemble-tap/state/tap-admin-replay.db \
  --set config.server.admin_replay_require_reason=true \
  --set config.server.admin_replay_reason_min_length=12 \
  --set config.server.admin_replay_max_queued_per_ip=100 \
  --set config.server.admin_replay_max_queued_per_token=20 \
  --set config.server.admin_rate_limit_per_sec=5 \
  --set config.server.admin_rate_limit_burst=20 \
  --set config.server.admin_allowed_cidrs[0]=203.0.113.0/24 \
  --set config.server.admin_mtls_required=true \
  --set config.server.admin_mtls_client_cert_header=X-Forwarded-Client-Cert \
  --set config.state.backend=sqlite \
  --set persistence.enabled=true
```

Notes:
- `config.server.admin_replay_max_limit` is validated in chart schema and runtime (`1..100000`).
- `config.server.admin_replay_job_ttl` and `config.server.admin_replay_job_max_jobs` control in-memory replay-job retention and capacity.
- `config.server.admin_replay_job_timeout` and `config.server.admin_replay_max_concurrent_jobs` control replay execution duration and concurrency.
- `config.server.admin_replay_store_backend=sqlite` persists replay-job metadata across process restarts.
- `config.server.admin_replay_require_reason` and `config.server.admin_replay_reason_min_length` enforce explicit operator reason headers.
- `config.server.admin_replay_max_queued_per_ip` and `config.server.admin_replay_max_queued_per_token` cap queued-job fan-out per caller scope.
- `config.server.admin_token_secondary` should only be used with `config.server.admin_token`.
- `config.server.admin_token_read`, `config.server.admin_token_replay`, and `config.server.admin_token_cancel` support role-scoped least-privilege access.
- `config.server.admin_rate_limit_per_sec` and `config.server.admin_rate_limit_burst` must both be greater than `0`.
- `config.server.admin_allowed_cidrs` and `config.server.admin_mtls_required` provide network and client-cert guardrails for admin routes.

## Tune NATS and ClickHouse

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set env[0].name=NATS_URL \
  --set env[0].value='nats://nats-a:4222,nats-b:4222' \
  --set env[1].name=CLICKHOUSE_PASSWORD \
  --set env[1].value='super-secret' \
  --set config.nats.url='${NATS_URL}' \
  --set config.nats.connect_timeout=5s \
  --set config.nats.reconnect_wait=2s \
  --set config.nats.max_reconnects=-1 \
  --set config.nats.publish_timeout=5s \
  --set config.nats.publish_max_retries=3 \
  --set config.nats.publish_retry_backoff=100ms \
  --set config.nats.secure=true \
  --set config.nats.ca_file=/var/run/ensemble-tap/nats/ca.crt \
  --set config.nats.creds_file=/var/run/ensemble-tap/nats/client.creds \
  --set config.nats.stream_replicas=3 \
  --set config.nats.stream_storage=file \
  --set config.nats.stream_discard=old \
  --set config.nats.stream_max_msgs=5000000 \
  --set config.nats.stream_max_bytes=21474836480 \
  --set config.nats.stream_max_msg_size=1048576 \
  --set config.clickhouse.addr='clickhouse-a:9000,clickhouse-b:9000' \
  --set config.clickhouse.username=default \
  --set config.clickhouse.password='${CLICKHOUSE_PASSWORD}' \
  --set config.clickhouse.secure=true \
  --set config.clickhouse.dial_timeout=5s \
  --set config.clickhouse.max_open_conns=8 \
  --set config.clickhouse.max_idle_conns=4 \
  --set config.clickhouse.conn_max_lifetime=30m \
  --set config.clickhouse.consumer_name=tap_clickhouse_sink_prod \
  --set config.clickhouse.consumer_fetch_batch_size=200 \
  --set config.clickhouse.consumer_fetch_max_wait=750ms \
  --set config.clickhouse.consumer_ack_wait=45s \
  --set config.clickhouse.consumer_max_ack_pending=2000 \
  --set config.clickhouse.insert_timeout=15s \
  --set config.clickhouse.retention_ttl=2160h \
  --set extraVolumes[0].name=nats-auth \
  --set extraVolumes[0].secret.secretName=ensemble-tap-nats-auth \
  --set extraVolumeMounts[0].name=nats-auth \
  --set extraVolumeMounts[0].mountPath=/var/run/ensemble-tap/nats \
  --set extraVolumeMounts[0].readOnly=true
```

Auth notes:
- Use only one NATS auth mode at a time: `username/password`, `token`, or `creds_file`.
- If NATS TLS files are used (`ca_file`, `cert_file`, `key_file`), set `config.nats.secure=true` and mount files via `extraVolumes` + `extraVolumeMounts`.
- If `config.clickhouse.insecure_skip_verify=true`, `config.clickhouse.secure` must also be `true`.

## Ops hardening defaults

- `podDisruptionBudget.enabled=true` with `minAvailable=1`.
- `networkPolicy.enabled=true` with explicit ingress/egress policy stanzas (DNS, HTTPS, NATS, and ClickHouse by default).
- `envSecrets` supports direct `env` values from secret key references.
- `autoscaling.customMetrics` enables HPA custom metrics in addition to CPU/memory targets.

## Enable sqlite state persistence

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set config.state.backend=sqlite \
  --set persistence.enabled=true
```
