#!/usr/bin/env bash
set -euo pipefail

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: missing required command: $1" >&2
    exit 1
  fi
}

require go
require helm
require yq

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "${ROOT}" ]]; then
  echo "error: must run inside a git repository" >&2
  exit 1
fi
cd "${ROOT}"

seed_env_from_placeholders() {
  local file="$1"
  local var
  while IFS= read -r var; do
    [[ -z "${var}" ]] && continue
    if [[ -z "${!var+x}" ]]; then
      export "${var}=lint-${var,,}"
    fi
  done < <(grep -o '\${[A-Z0-9_][A-Z0-9_]*}' "${file}" | tr -d '${}' | sort -u)
}

echo "==> Linting config.example.yaml with runtime validator"
seed_env_from_placeholders "config.example.yaml"
go run ./cmd/tap -config config.example.yaml -check-config >/dev/null

echo "==> Linting Helm-rendered config.yaml with runtime validator"
rendered_manifest="$(mktemp)"
rendered_config="$(mktemp)"
cleanup() {
  rm -f "${rendered_manifest}" "${rendered_config}"
}
trap cleanup EXIT

helm template siphon charts/siphon > "${rendered_manifest}"

yq -r '
  select(.kind == "ConfigMap" and (.data."config.yaml" // "") != "")
  | .data."config.yaml"
' "${rendered_manifest}" | awk 'NF {print}' > "${rendered_config}"

if [[ ! -s "${rendered_config}" ]]; then
  echo "error: failed to extract config.yaml from rendered Helm manifest" >&2
  exit 1
fi

go run ./cmd/tap -config "${rendered_config}" -check-config >/dev/null

echo "==> Checking config surface parity across runtime/example/chart schema"
go test ./config -run TestConfigSurfaceParity -count=1 >/dev/null

echo "ok: config lint passed (config.example + Helm-rendered config + key parity)"
