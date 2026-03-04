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
  --set config.server.admin_replay_max_limit=2000
```

Notes:
- `config.server.admin_replay_max_limit` is validated in chart schema and runtime (`1..100000`).
- `config.server.admin_token_secondary` should only be used with `config.server.admin_token`.

## Enable sqlite state persistence

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set config.state.backend=sqlite \
  --set persistence.enabled=true
```
