#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
guard="${root_dir}/scripts/ci/common_ancestor_guard.sh"
workflow="${root_dir}/.github/workflows/ci.yml"
fixture_dir="$(mktemp -d)"
trap 'rm -rf "$fixture_dir"' EXIT

repo="${fixture_dir}/repo"
git init --quiet --initial-branch=master "$repo"
git -C "$repo" config user.name "ZeroClaw Test"
git -C "$repo" config user.email "zeroclaw-test@example.invalid"
git -C "$repo" commit --quiet --allow-empty -m "base"
base_sha="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" update-ref refs/remotes/origin/master "$base_sha"
git -C "$repo" update-ref refs/remotes/origin/main "$base_sha"

git -C "$repo" commit --quiet --allow-empty -m "related"
related_sha="$(git -C "$repo" rev-parse HEAD)"
for branch in main master; do
  if ! (cd "$repo" && bash "$guard" "origin/$branch" "$related_sha") >/dev/null; then
    echo "expected a related history on $branch to pass" >&2
    exit 1
  fi
done

git -C "$repo" checkout --quiet --orphan unrelated
git -C "$repo" commit --quiet --allow-empty -m "unrelated"
unrelated_sha="$(git -C "$repo" rev-parse HEAD)"
for branch in main master; do
  if (cd "$repo" && bash "$guard" "origin/$branch" "$unrelated_sha") >/dev/null 2>&1; then
    echo "expected independent valid histories on $branch to fail" >&2
    exit 1
  fi
done

if (cd "$repo" && bash "$guard" origin/master missing-target) >/dev/null 2>&1; then
  echo "expected a missing target ref to fail" >&2
  exit 1
fi

expected_target='          HISTORY_CHECK_SHA: ${{ github.event_name == '\''pull_request'\'' && github.event.pull_request.head.sha || github.sha }}'
grep -Fqx "$expected_target" "$workflow" || {
  echo "history guard must select pull_request.head.sha for PRs and github.sha otherwise" >&2
  exit 1
}
grep -Fqx '        run: bash scripts/ci/common_ancestor_guard.sh "origin/$CI_BASE_BRANCH" "$HISTORY_CHECK_SHA"' "$workflow" || {
  echo "history guard must pass the selected commit to the guard script" >&2
  exit 1
}

grep -Fqx '          CI_BASE_BRANCH: ${{ github.event.pull_request.base.ref || github.event.repository.default_branch }}' "$workflow" || {
  echo "history guard must resolve the PR base or repository default branch" >&2
  exit 1
}

echo "common ancestor guard tests passed"
