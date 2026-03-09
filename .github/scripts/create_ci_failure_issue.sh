#!/usr/bin/env bash
set -euo pipefail

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

require_bin gh
require_bin jq

if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "GH_TOKEN is required" >&2
  exit 1
fi
if [[ -z "${REPO:-}" ]]; then
  echo "REPO is required (owner/repo)" >&2
  exit 1
fi
if [[ -z "${RUN_ID:-}" ]]; then
  echo "RUN_ID is required" >&2
  exit 1
fi

run_file="$(mktemp)"
jobs_file="$(mktemp)"
failed_file="$(mktemp)"
body_file="$(mktemp)"
work_dir="$(mktemp -d)"
trap 'rm -f "${run_file}" "${jobs_file}" "${failed_file}" "${body_file}"; rm -rf "${work_dir}"' EXIT

gh api "repos/${REPO}/actions/runs/${RUN_ID}" >"${run_file}"
gh api "repos/${REPO}/actions/runs/${RUN_ID}/jobs?per_page=100" >"${jobs_file}"

jq -r '.jobs[] | select(.conclusion == "failure") | [.id, .name, .html_url] | @tsv' "${jobs_file}" >"${failed_file}"

if [[ ! -s "${failed_file}" ]]; then
  echo "No failed jobs found for run ${RUN_ID}; skipping issue creation"
  exit 0
fi

title="CI failure on main"
existing_issue="$(gh issue list -R "${REPO}" --state open --search "\"${title}\" in:title" --json number --jq '.[0].number // empty')"

# Ensure label exists for triage routing.
gh label create ci-failure -R "${REPO}" --description "Automated CI failure reports on main" --color B60205 2>/dev/null || true

run_url="$(jq -r '.html_url' "${run_file}")"
head_sha="$(jq -r '.head_sha' "${run_file}")"
run_attempt="$(jq -r '.run_attempt' "${run_file}")"
created_at="$(jq -r '.created_at' "${run_file}")"

{
  echo "Automated CI failure report for a \\`main\\` push run."
  echo
  echo "- Run: ${run_url}"
  echo "- Run ID: ${RUN_ID}"
  echo "- Attempt: ${run_attempt}"
  echo "- Head SHA: \\`${head_sha}\\`"
  echo "- Created At: ${created_at}"
  echo
  echo "## Failed Jobs"
  while IFS=$'\t' read -r job_id job_name job_url; do
    echo "- [${job_name}](${job_url}) (job id: ${job_id})"
  done <"${failed_file}"
} >"${body_file}"

while IFS=$'\t' read -r job_id job_name _job_url; do
  log_file="${work_dir}/job-${job_id}.log"
  if ! gh run view "${RUN_ID}" -R "${REPO}" --job "${job_id}" --log-failed >"${log_file}" 2>/dev/null; then
    gh run view "${RUN_ID}" -R "${REPO}" --job "${job_id}" --log >"${log_file}" 2>/dev/null || true
  fi

  {
    echo
    echo "<details><summary>${job_name} failed log (truncated)</summary>"
    echo
    echo "\\`\\`\\`text"
    if [[ -s "${log_file}" ]]; then
      head -n 220 "${log_file}" | sed 's/\r$//'
    else
      echo "No log output available for job ${job_id}."
    fi
    echo "\\`\\`\\`"
    echo
    echo "</details>"
  } >>"${body_file}"
done <"${failed_file}"

if [[ -n "${existing_issue}" ]]; then
  gh issue comment "${existing_issue}" -R "${REPO}" --body-file "${body_file}" >/dev/null
  echo "Added CI failure details to existing issue #${existing_issue}"
  exit 0
fi

issue_url="$(gh issue create -R "${REPO}" --title "${title}" --body-file "${body_file}" --label ci-failure)"
echo "Created CI failure issue: ${issue_url}"
