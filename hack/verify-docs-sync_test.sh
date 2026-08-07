#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

#
# Tests for verify-docs-sync.sh. Self-contained: each case builds a throwaway
# git repo in a temp dir, so it needs no network and touches nothing tracked.
#
# Run: hack/verify-docs-sync_test.sh   (also: make test-docs-sync)
set -euo pipefail

# Git hooks (notably pre-push, which runs `make ci`) export GIT_DIR / GIT_WORK_TREE
# into the environment. Left set, they redirect this test's throwaway-repo git
# commands at the real repository, breaking isolation. Clear them up front.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
      GIT_COMMON_DIR GIT_PREFIX GIT_NAMESPACE \
      GIT_CONFIG GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM 2>/dev/null || true

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/verify-docs-sync.sh"
TMPROOT="$(mktemp -d)"
trap 'rm -rf "${TMPROOT}"' EXIT
fails=0

# Run the gate inside repo $1 against base ref $2; assert its exit code == $3.
expect() {
  local dir="$1" base="$2" want="$3" name="$4" rc=0 out
  out="$(cd "${dir}" && bash "${SCRIPT}" "${base}" 2>&1)" || rc=$?
  if [ "${rc}" -eq "${want}" ]; then
    echo "  ok   ${name} (exit ${rc})"
  else
    echo "  FAIL ${name}: expected exit ${want}, got ${rc}"
    printf '%s\n' "${out}" | sed 's/^/         | /'
    fails=$((fails + 1))
  fi
}

# Fresh repo seeded with one file per relevant path; echoes "<dir> <base-sha>".
new_repo() {
  local dir
  dir="$(mktemp -d "${TMPROOT}/repo.XXXXXX")"
  git -C "${dir}" init -q
  git -C "${dir}" config user.email t@example.com
  git -C "${dir}" config user.name test
  git -C "${dir}" config commit.gpgsign false
  mkdir -p "${dir}/api/v1alpha1" "${dir}/proto/p" "${dir}/docs/design" "${dir}/site/content"
  echo "package v1alpha1"       > "${dir}/api/v1alpha1/cachebackend_types.go"
  echo 'syntax = "proto3";'     > "${dir}/proto/p/p.proto"
  echo "# contract"             > "${dir}/docs/design/grpc-contract.md"
  echo "# site"                 > "${dir}/site/content/x.md"
  echo "misc"                   > "${dir}/other.txt"
  git -C "${dir}" add -A
  git -C "${dir}" commit -qm base
  echo "${dir} $(git -C "${dir}" rev-parse HEAD)"
}

commit() { git -C "$1" add -A; git -C "$1" commit -qm "$2"; }

echo "verify-docs-sync tests:"

# 1. CRD types change without docs -> FAIL.
read -r d b <<<"$(new_repo)"
echo "// change" >> "${d}/api/v1alpha1/cachebackend_types.go"; commit "${d}" c
expect "${d}" "${b}" 1 "CRD-only change fails"

# 2. CRD types change WITH a docs update -> PASS.
read -r d b <<<"$(new_repo)"
echo "// change" >> "${d}/api/v1alpha1/cachebackend_types.go"
echo "more"      >> "${d}/docs/design/grpc-contract.md"; commit "${d}" c
expect "${d}" "${b}" 0 "CRD + docs passes"

# 3. proto change without docs -> FAIL.
read -r d b <<<"$(new_repo)"
echo "// change" >> "${d}/proto/p/p.proto"; commit "${d}" c
expect "${d}" "${b}" 1 "proto-only change fails"

# 4. Unrelated change only -> PASS (no public surface touched).
read -r d b <<<"$(new_repo)"
echo "x" >> "${d}/other.txt"; commit "${d}" c
expect "${d}" "${b}" 0 "unrelated change passes"

# 5. No changes at all -> PASS.
read -r d b <<<"$(new_repo)"
expect "${d}" "${b}" 0 "no changes passes"

# 6. Missing base ref -> FAIL closed (must not silently skip).
read -r d b <<<"$(new_repo)"
echo "// change" >> "${d}/api/v1alpha1/cachebackend_types.go"; commit "${d}" c
expect "${d}" "no-such-ref" 1 "missing base fails closed"

# 7. CRD change + DELETED doc (no other doc change) -> FAIL (deletion != update).
read -r d b <<<"$(new_repo)"
echo "// change" >> "${d}/api/v1alpha1/cachebackend_types.go"
git -C "${d}" rm -q "site/content/x.md"; commit "${d}" c
expect "${d}" "${b}" 1 "deleted doc does not satisfy"

# 8. No merge base (orphan base) + CRD + docs -> PASS via two-endpoint fallback
#    (and must not error with "no merge base").
read -r d b <<<"$(new_repo)"
echo "// change" >> "${d}/api/v1alpha1/cachebackend_types.go"
echo "more"      >> "${d}/docs/design/grpc-contract.md"; commit "${d}" c
orphan="$(git -C "${d}" commit-tree "$(git -C "${d}" hash-object -t tree /dev/null)" -m orphan)"
expect "${d}" "${orphan}" 0 "no-merge-base fallback works"

if [ "${fails}" -eq 0 ]; then
  echo "All verify-docs-sync tests passed."
else
  echo "${fails} verify-docs-sync test(s) failed." >&2
  exit 1
fi
