#!/usr/bin/env bash
set -euo pipefail

repo="${1:-${GITHUB_REPOSITORY:-}}"
branch="${2:-main}"

if [[ -z "$repo" ]]; then
  echo "usage: $0 <owner/repo> [branch]" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required" >&2
  exit 1
fi

payload_file="$(mktemp)"
trap 'rm -f "$payload_file"' EXIT

cat >"$payload_file" <<JSON
{
  "required_status_checks": {
    "strict": true,
    "checks": [
      {"context": "test", "app_id": 15368},
      {"context": "staticcheck", "app_id": 15368},
      {"context": "openapi-contract", "app_id": 15368},
      {"context": "config-lint", "app_id": 15368},
      {"context": "docker-build", "app_id": 15368},
      {"context": "helm-lint", "app_id": 15368},
      {"context": "security-gosec", "app_id": 15368},
      {"context": "security-govulncheck", "app_id": 15368},
      {"context": "security-trivy", "app_id": 15368},
      {"context": "security-sbom", "app_id": 15368},
      {"context": "perf-smoke", "app_id": 15368},
      {"context": "integration", "app_id": 15368}
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false,
    "required_approving_review_count": 1,
    "require_last_push_approval": true
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": true,
  "lock_branch": false,
  "allow_fork_syncing": true
}
JSON

gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  "/repos/${repo}/branches/${branch}/protection" \
  --input "$payload_file"

echo "Applied branch protection for ${repo}:${branch}"
