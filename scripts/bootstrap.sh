#!/usr/bin/env bash
set -euo pipefail

release="siphon"
namespace="siphon"
chart_path="./charts/siphon"
values_file="./values.onboarding.yaml"
secret_name=""
provider=""
provider_secret=""
nats_url="nats://nats:4222"
clickhouse_addr=""
clickhouse_username=""
clickhouse_password=""
tenant_id=""
skip_smoke="false"
local_port="18080"

usage() {
  cat <<USAGE
Usage: $0 [options]

Guided Helm bootstrap for the fastest path to first event.

Options:
  --release <name>           Helm release name (default: siphon)
  --namespace <name>         Kubernetes namespace (default: siphon)
  --chart <path>             Helm chart path (default: ./charts/siphon)
  --values-file <path>       Generated values file (default: ./values.onboarding.yaml)
  --secret-name <name>       Kubernetes secret name (default: <release>-onboarding-secrets)
  --provider <name>          stripe|github|hubspot|linear|shopify|generic
  --provider-secret <value>  Webhook secret for chosen provider
  --nats-url <url>           NATS URL (default: nats://nats:4222)
  --clickhouse-addr <addr>   Optional ClickHouse host:port (empty disables sink)
  --clickhouse-username <v>  Optional ClickHouse username (default: siphon_ingest when sink enabled)
  --clickhouse-password <v>  Optional ClickHouse password
  --tenant-id <id>           Optional tenant path segment for smoke test
  --skip-smoke               Skip webhook smoke test
  --local-port <port>        Local port for smoke port-forward (default: 18080)
  -h, --help                 Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      release="${2:-}"
      shift 2
      ;;
    --namespace)
      namespace="${2:-}"
      shift 2
      ;;
    --chart)
      chart_path="${2:-}"
      shift 2
      ;;
    --values-file)
      values_file="${2:-}"
      shift 2
      ;;
    --secret-name)
      secret_name="${2:-}"
      shift 2
      ;;
    --provider)
      provider="${2:-}"
      shift 2
      ;;
    --provider-secret)
      provider_secret="${2:-}"
      shift 2
      ;;
    --nats-url)
      nats_url="${2:-}"
      shift 2
      ;;
    --clickhouse-addr)
      clickhouse_addr="${2:-}"
      shift 2
      ;;
    --clickhouse-username)
      clickhouse_username="${2:-}"
      shift 2
      ;;
    --clickhouse-password)
      clickhouse_password="${2:-}"
      shift 2
      ;;
    --tenant-id)
      tenant_id="${2:-}"
      shift 2
      ;;
    --skip-smoke)
      skip_smoke="true"
      shift
      ;;
    --local-port)
      local_port="${2:-}"
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

require_bin helm
require_bin kubectl
require_bin bash

if [[ -z "$secret_name" ]]; then
  secret_name="${release}-onboarding-secrets"
fi

choose_provider_interactive() {
  local options
  options=(stripe github hubspot linear shopify generic)
  echo "Select provider:"
  local idx=1
  for p in "${options[@]}"; do
    echo "  ${idx}) ${p}"
    idx=$((idx + 1))
  done

  local selected
  while true; do
    read -r -p "Provider [1-${#options[@]}]: " selected
    if [[ "$selected" =~ ^[0-9]+$ ]] && (( selected >= 1 && selected <= ${#options[@]} )); then
      provider="${options[selected-1]}"
      return
    fi
    echo "Invalid selection."
  done
}

if [[ -z "$provider" ]]; then
  choose_provider_interactive
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
  read -r -s -p "Enter webhook secret for '${provider}': " provider_secret
  echo
fi
if [[ -z "$provider_secret" ]]; then
  echo "Provider secret cannot be empty" >&2
  exit 1
fi

if [[ -z "$clickhouse_addr" ]]; then
  read -r -p "ClickHouse addr (host:port, leave empty to disable sink): " clickhouse_addr
fi
if [[ -n "$clickhouse_addr" && -z "$clickhouse_username" ]]; then
  clickhouse_username="siphon_ingest"
fi
if [[ -n "$clickhouse_addr" && -z "$clickhouse_password" ]]; then
  read -r -s -p "ClickHouse password (optional, press Enter to skip): " clickhouse_password
  echo
fi

provider_upper="$(printf '%s' "$provider" | tr '[:lower:]' '[:upper:]')"
provider_env_name="${provider_upper}_WEBHOOK_SECRET"
provider_secret_key="${provider}WebhookSecret"

mkdir -p "$(dirname "$values_file")"

{
  echo "envSecrets:"
  echo "  - name: ${provider_env_name}"
  echo "    secretName: ${secret_name}"
  echo "    secretKey: ${provider_secret_key}"
  if [[ -n "$clickhouse_addr" && -n "$clickhouse_username" ]]; then
    echo "  - name: CLICKHOUSE_USERNAME"
    echo "    secretName: ${secret_name}"
    echo "    secretKey: clickhouseUsername"
  fi
  if [[ -n "$clickhouse_addr" && -n "$clickhouse_password" ]]; then
    echo "  - name: CLICKHOUSE_PASSWORD"
    echo "    secretName: ${secret_name}"
    echo "    secretKey: clickhousePassword"
  fi
  echo "config:"
  echo "  providers:"
  echo "    ${provider}:"
  echo "      mode: webhook"
  echo "      secret: \${${provider_env_name}}"
  echo "  nats:"
  echo "    url: ${nats_url}"
  echo "  clickhouse:"
  if [[ -n "$clickhouse_addr" ]]; then
    echo "    addr: ${clickhouse_addr}"
    if [[ -n "$clickhouse_username" ]]; then
      echo "    username: \${CLICKHOUSE_USERNAME}"
    fi
    if [[ -n "$clickhouse_password" ]]; then
      echo "    password: \${CLICKHOUSE_PASSWORD}"
    else
      echo "    password: \"\""
    fi
  else
    echo "    addr: \"\""
    echo "    password: \"\""
  fi
  echo "  server:"
  echo "    base_path: /webhooks"
} >"$values_file"

secret_cmd=(kubectl -n "$namespace" create secret generic "$secret_name" --from-literal="${provider_secret_key}=${provider_secret}")
if [[ -n "$clickhouse_addr" && -n "$clickhouse_username" ]]; then
  secret_cmd+=(--from-literal="clickhouseUsername=${clickhouse_username}")
fi
if [[ -n "$clickhouse_addr" && -n "$clickhouse_password" ]]; then
  secret_cmd+=(--from-literal="clickhousePassword=${clickhouse_password}")
fi

kubectl get namespace "$namespace" >/dev/null 2>&1 || kubectl create namespace "$namespace" >/dev/null
"${secret_cmd[@]}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

helm upgrade --install "$release" "$chart_path" \
  --namespace "$namespace" \
  --create-namespace \
  --wait \
  --timeout 5m \
  -f "$values_file"

echo ""
echo "Bootstrap complete."
echo "Release: ${release}"
echo "Namespace: ${namespace}"
echo "Provider: ${provider}"
echo "Generated values file: ${values_file}"
echo "Kubernetes secret: ${secret_name}"
echo "Webhook path: /webhooks/${provider}${tenant_id:+/${tenant_id}}"

if [[ "$skip_smoke" == "false" ]]; then
  echo ""
  echo "Running onboarding smoke test..."
  export ONBOARDING_PROVIDER_SECRET="$provider_secret"
  smoke_cmd=("$(dirname "$0")/smoke-onboarding.sh" --provider "$provider" --release "$release" --namespace "$namespace" --local-port "$local_port")
  if [[ -n "$tenant_id" ]]; then
    smoke_cmd+=(--tenant "$tenant_id")
  fi
  "${smoke_cmd[@]}"
fi
