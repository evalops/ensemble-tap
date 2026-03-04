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

# Optional admin rotation + replay cap settings
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set env[0].name=TAP_ADMIN_TOKEN \
  --set env[0].value=primary-token \
  --set env[1].name=TAP_ADMIN_TOKEN_SECONDARY \
  --set env[1].value=next-token \
  --set config.server.admin_token='${TAP_ADMIN_TOKEN}' \
  --set config.server.admin_token_secondary='${TAP_ADMIN_TOKEN_SECONDARY}' \
  --set config.server.admin_replay_max_limit=2000 \
  --set config.server.admin_rate_limit_per_sec=5 \
  --set config.server.admin_rate_limit_burst=20 \
  --set config.server.admin_allowed_cidrs[0]=203.0.113.0/24 \
  --set config.server.admin_mtls_required=true \
  --set config.server.admin_mtls_client_cert_header=X-Forwarded-Client-Cert
```

Notes:
- `config.server.admin_replay_max_limit` is validated in chart schema and runtime (`1..100000`).
- `config.server.admin_token_secondary` should only be used with `config.server.admin_token`.
- `config.server.admin_rate_limit_per_sec` and `config.server.admin_rate_limit_burst` must both be greater than `0`.
- `config.server.admin_allowed_cidrs` and `config.server.admin_mtls_required` provide network and client-cert guardrails for admin routes.

## Ops hardening defaults

- `podDisruptionBudget.enabled=true` with `minAvailable=1`.
- `networkPolicy.enabled=true` with explicit ingress/egress policy stanzas.
- `envSecrets` supports direct `env` values from secret key references.
- `autoscaling.customMetrics` enables HPA custom metrics in addition to CPU/memory targets.

## Enable sqlite state persistence

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set config.state.backend=sqlite \
  --set persistence.enabled=true
```
