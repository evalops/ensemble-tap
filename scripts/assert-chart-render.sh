#!/usr/bin/env bash
set -euo pipefail

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: missing required command: $1" >&2
    exit 1
  fi
}

fail() {
  echo "error: $*" >&2
  exit 1
}

require helm
require yq

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "${ROOT}" ]]; then
  fail "must run inside a git repository"
fi
cd "${ROOT}"

rendered_default="$(mktemp)"
rendered_fixture="$(mktemp)"
rendered_automount="$(mktemp)"
fixture_values="$(mktemp)"
cleanup() {
  rm -f "${rendered_default}" "${rendered_fixture}" "${rendered_automount}" "${fixture_values}"
}
trap cleanup EXIT

digest="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

cat >"${fixture_values}" <<EOF
image:
  repository: ghcr.io/evalops/siphon
  digest: ${digest}
serviceAccount:
  automount: false
networkPolicy:
  allowConfigPorts: true
  extraEgressPorts:
    - 9555
  natsEgressTo:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: messaging
      podSelector:
        matchLabels:
          app: nats
  clickhouseEgressTo:
    - ipBlock:
        cidr: 10.42.0.0/16
config:
  nats:
    url: nats://nats-a:4222,nats://nats-b:4333
  clickhouse:
    addr: clickhouse-a:9000,clickhouse-b:9440
EOF

helm template siphon charts/siphon >"${rendered_default}"
helm template siphon charts/siphon -f "${fixture_values}" >"${rendered_fixture}"
helm template siphon charts/siphon --set serviceAccount.automount=true >"${rendered_automount}"

default_automount="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.automountServiceAccountToken' "${rendered_default}")"
[[ "${default_automount}" == "false" ]] || fail "default automountServiceAccountToken should be false, got ${default_automount}"

override_automount="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.automountServiceAccountToken' "${rendered_automount}")"
[[ "${override_automount}" == "true" ]] || fail "serviceAccount.automount=true should render true, got ${override_automount}"

rendered_image="$(yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[] | select(.name == "tap") | .image' "${rendered_fixture}")"
expected_image="ghcr.io/evalops/siphon@${digest}"
[[ "${rendered_image}" == "${expected_image}" ]] || fail "digest image rendering mismatch: got ${rendered_image}, expected ${expected_image}"

ports="$(yq -r 'select(.kind == "NetworkPolicy") | .spec.egress[]?.ports[]?.port' "${rendered_fixture}" | sort -n | uniq | tr '\n' ' ')"
for port in 4222 4333 9000 9440 9555; do
  grep -Eq "(^| )${port}( |$)" <<<"${ports}" || fail "expected derived egress port ${port} in rendered NetworkPolicy, got: ${ports}"
done

nats_scope="$(yq -r 'select(.kind == "NetworkPolicy") | .spec.egress[] | select(.ports[]?.port == 4222) | .to[0].namespaceSelector.matchLabels."kubernetes.io/metadata.name"' "${rendered_fixture}")"
[[ "${nats_scope}" == "messaging" ]] || fail "expected NATS scoped namespaceSelector to render, got ${nats_scope}"

clickhouse_scope="$(yq -r 'select(.kind == "NetworkPolicy") | .spec.egress[] | select(.ports[]?.port == 9000) | .to[0].ipBlock.cidr' "${rendered_fixture}")"
[[ "${clickhouse_scope}" == "10.42.0.0/16" ]] || fail "expected ClickHouse scoped ipBlock.cidr to render, got ${clickhouse_scope}"

echo "ok: chart render assertions passed"
