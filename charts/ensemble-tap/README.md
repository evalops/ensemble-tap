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
```

## Enable sqlite state persistence

```bash
helm upgrade --install ensemble-tap ./charts/ensemble-tap \
  --namespace ensemble \
  --set config.state.backend=sqlite \
  --set persistence.enabled=true
```
