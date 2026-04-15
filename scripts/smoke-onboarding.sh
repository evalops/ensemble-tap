#!/usr/bin/env bash
set -euo pipefail

release="siphon"
namespace="siphon"
provider=""
provider_secret=""
service_name=""
service_port="8080"
local_port="18080"
base_url=""
tenant=""
request_timeout="10"

usage() {
  cat <<USAGE
Usage: $0 --provider <name> [options]

Provider must be one of: stripe, github, hubspot, linear, shopify, generic

Options:
  --provider <name>         Provider name used in /webhooks/{provider}.
  --secret <value>          Provider webhook secret.
  --release <name>          Helm release name (default: siphon).
  --namespace <name>        Kubernetes namespace (default: siphon).
  --service <name>          Kubernetes Service name (auto-discovered by default).
  --service-port <port>     Service HTTP port (default: 8080).
  --local-port <port>       Local port for kubectl port-forward (default: 18080).
  --base-url <url>          Base URL to call directly; skips port-forward.
  --tenant <id>             Optional tenant path segment.
  --request-timeout <sec>   Curl max time in seconds (default: 10).
  -h, --help                Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --provider)
      provider="${2:-}"
      shift 2
      ;;
    --secret)
      provider_secret="${2:-}"
      shift 2
      ;;
    --release)
      release="${2:-}"
      shift 2
      ;;
    --namespace)
      namespace="${2:-}"
      shift 2
      ;;
    --service)
      service_name="${2:-}"
      shift 2
      ;;
    --service-port)
      service_port="${2:-}"
      shift 2
      ;;
    --local-port)
      local_port="${2:-}"
      shift 2
      ;;
    --base-url)
      base_url="${2:-}"
      shift 2
      ;;
    --tenant)
      tenant="${2:-}"
      shift 2
      ;;
    --request-timeout)
      request_timeout="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

require_bin curl
require_bin kubectl
require_bin openssl

if [[ -z "$provider" ]]; then
  echo "--provider is required" >&2
  usage
  exit 1
fi

provider="$(printf '%s' "$provider" | tr '[:upper:]' '[:lower:]')"
case "$provider" in
  stripe|github|hubspot|linear|shopify|generic)
    ;;
  *)
    echo "Unsupported provider: $provider" >&2
    exit 1
    ;;
esac

if [[ -z "$provider_secret" ]]; then
  if [[ -n "${ONBOARDING_PROVIDER_SECRET:-}" ]]; then
    provider_secret="$ONBOARDING_PROVIDER_SECRET"
  else
    echo "Provider secret is required via --secret or ONBOARDING_PROVIDER_SECRET" >&2
    exit 1
  fi
fi

trim_slash() {
  local v="$1"
  v="${v%/}"
  printf '%s' "$v"
}

get_service_name() {
  local discovered
  discovered="$(kubectl -n "$namespace" get svc \
    -l "app.kubernetes.io/instance=$release,app.kubernetes.io/name=siphon" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "$discovered" ]]; then
    printf '%s' "$discovered"
    return
  fi
  discovered="$(kubectl -n "$namespace" get svc "${release}" -o jsonpath='{.metadata.name}' 2>/dev/null || true)"
  if [[ -n "$discovered" ]]; then
    printf '%s' "$discovered"
    return
  fi
  discovered="$(kubectl -n "$namespace" get svc "${release}-siphon" -o jsonpath='{.metadata.name}' 2>/dev/null || true)"
  printf '%s' "$discovered"
}

wait_http_ok() {
  local url="$1"
  local attempts="$2"
  local delay_secs="$3"
  for _ in $(seq 1 "$attempts"); do
    if curl -fsS --max-time "$request_timeout" "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay_secs"
  done
  return 1
}

PF_PID=""
cleanup() {
  if [[ -n "$PF_PID" ]]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ -z "$base_url" ]]; then
  if [[ -z "$service_name" ]]; then
    service_name="$(get_service_name)"
  fi
  if [[ -z "$service_name" ]]; then
    echo "Unable to discover service for release '$release' in namespace '$namespace'" >&2
    exit 1
  fi

  pf_log="/tmp/siphon-port-forward-${release}-${namespace}.log"
  kubectl -n "$namespace" port-forward "svc/$service_name" "${local_port}:${service_port}" >"$pf_log" 2>&1 &
  PF_PID="$!"

  base_url="http://127.0.0.1:${local_port}"
fi

base_url="$(trim_slash "$base_url")"
base_scheme="${base_url%%://*}"
if [[ "$base_scheme" == "$base_url" ]]; then
  base_scheme="http"
fi
base_authority="${base_url#*://}"
base_authority="${base_authority%%/*}"

if ! wait_http_ok "$base_url/livez" 60 1; then
  echo "Service did not become live at $base_url/livez" >&2
  exit 1
fi
if ! wait_http_ok "$base_url/readyz" 60 1; then
  echo "Service did not become ready at $base_url/readyz" >&2
  exit 1
fi

event_id="onboarding-$(date +%s)"
rfc3339_now="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
epoch_now="$(date +%s)"

webhook_path="/webhooks/${provider}"
if [[ -n "$tenant" ]]; then
  webhook_path="${webhook_path}/${tenant}"
fi
webhook_url="${base_url}${webhook_path}"

headers=(
  -H "Content-Type: application/json"
  -H "X-Request-ID: onboarding-smoke-${event_id}"
)
body="{}"

hmac_sha256_hex() {
  local key="$1"
  local payload="$2"
  printf '%s' "$payload" | openssl dgst -sha256 -hmac "$key" -binary | xxd -p -c 256
}

hmac_sha256_b64() {
  local key="$1"
  local payload="$2"
  printf '%s' "$payload" | openssl dgst -sha256 -hmac "$key" -binary | openssl base64 -A
}

case "$provider" in
  generic)
    body=$(cat <<JSON
{"id":"${event_id}","event":"onboarding.test.created","created_at":"${rfc3339_now}","source":"onboarding"}
JSON
)
    sig="$(hmac_sha256_hex "$provider_secret" "$body")"
    headers+=(
      -H "X-Signature: sha256=${sig}"
      -H "X-Event-Type: onboarding.test.created"
      -H "X-Event-Action: created"
      -H "X-Event-Id: ${event_id}"
    )
    ;;
  github)
    body='{"action":"opened","pull_request":{"id":12345},"repository":{"id":1}}'
    sig="$(hmac_sha256_hex "$provider_secret" "$body")"
    headers+=(
      -H "X-Hub-Signature-256: sha256=${sig}"
      -H "X-GitHub-Event: pull_request"
      -H "X-GitHub-Delivery: ${event_id}"
    )
    ;;
  stripe)
    stripe_event_id="evt_${event_id}"
    body=$(cat <<JSON
{"id":"${stripe_event_id}","object":"event","api_version":"2024-12-18.acacia","created":${epoch_now},"data":{"object":{"id":"cus_onboarding","object":"customer"}},"livemode":false,"pending_webhooks":1,"request":{"id":null,"idempotency_key":null},"type":"customer.created"}
JSON
)
    signed_payload="${epoch_now}.${body}"
    sig="$(hmac_sha256_hex "$provider_secret" "$signed_payload")"
    headers+=(
      -H "Stripe-Signature: t=${epoch_now},v1=${sig}"
    )
    ;;
  hubspot)
    ts_ms="$((epoch_now * 1000))"
    body=$(cat <<JSON
[{"eventId":1,"subscriptionType":"contact.propertyChange","objectId":12345,"propertyName":"email","propertyValue":"onboarding@example.com","changeSource":"CRM","occurredAt":"${rfc3339_now}"}]
JSON
)
    # The HubSpot verifier computes URL from forwarded host/proto when present.
    hubspot_url="${base_scheme}://${base_authority}${webhook_path}"
    signed_payload="POST${hubspot_url}${body}${ts_ms}"
    sig="$(hmac_sha256_b64 "$provider_secret" "$signed_payload")"
    headers+=(
      -H "X-HubSpot-Signature-v3: ${sig}"
      -H "X-HubSpot-Request-Timestamp: ${ts_ms}"
      -H "X-HubSpot-Event-Id: ${event_id}"
      -H "X-Forwarded-Proto: ${base_scheme}"
      -H "X-Forwarded-Host: ${base_authority}"
    )
    ;;
  linear)
    body=$(cat <<JSON
{"type":"Issue","action":"create","webhookTimestamp":"${rfc3339_now}","data":{"id":"lin_123"}}
JSON
)
    sig="$(hmac_sha256_hex "$provider_secret" "$body")"
    headers+=(
      -H "Linear-Signature: ${sig}"
      -H "Linear-Event: Issue.create"
      -H "Linear-Delivery: ${event_id}"
    )
    ;;
  shopify)
    body='{"id":12345,"admin_graphql_api_id":"gid://shopify/Order/12345"}'
    sig="$(hmac_sha256_b64 "$provider_secret" "$body")"
    headers+=(
      -H "X-Shopify-Hmac-SHA256: ${sig}"
      -H "X-Shopify-Topic: orders/create"
      -H "X-Shopify-Event-Id: ${event_id}"
    )
    ;;
esac

resp_file="/tmp/siphon-onboarding-smoke-response.json"
http_code="$(curl -sS -o "$resp_file" -w '%{http_code}' --max-time "$request_timeout" -X POST "${webhook_url}" "${headers[@]}" --data "$body")"

if [[ "$http_code" != "202" ]]; then
  echo "Webhook smoke failed with HTTP ${http_code}" >&2
  cat "$resp_file" >&2 || true
  exit 1
fi

extract_metric_value() {
  local metrics="$1"
  local pattern="$2"
  local line
  line="$(printf '%s\n' "$metrics" | grep "$pattern" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    printf '0'
    return
  fi
  printf '%s' "${line##* }"
}

is_gt_zero() {
  local value="$1"
  awk -v v="$value" 'BEGIN { exit !(v+0 > 0) }'
}

accepted_metric_pattern="tap_webhooks_received_total{provider=\"${provider}\",status=\"accepted\"}"
published_metric_pattern="tap_events_published_total{provider=\"${provider}\""

for _ in $(seq 1 30); do
  metrics_payload="$(curl -fsS --max-time "$request_timeout" "$base_url/metrics" || true)"
  accepted_value="$(extract_metric_value "$metrics_payload" "$accepted_metric_pattern")"
  published_value="$(extract_metric_value "$metrics_payload" "$published_metric_pattern")"
  if is_gt_zero "$accepted_value" && is_gt_zero "$published_value"; then
    echo "Onboarding smoke passed for provider '${provider}'."
    echo "Webhook endpoint: ${webhook_url}"
    echo "Accepted metric value: ${accepted_value}"
    echo "Published metric value: ${published_value}"
    exit 0
  fi
  sleep 1
done

echo "Webhook was accepted but metrics did not confirm publish within timeout." >&2
cat "$resp_file" >&2 || true
exit 1
