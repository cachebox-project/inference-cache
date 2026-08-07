#!/usr/bin/env bash
# Self-contained tests for verify-dco.sh using throwaway Git repositories.
set -euo pipefail

unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
      GIT_COMMON_DIR GIT_PREFIX GIT_NAMESPACE \
      GIT_CONFIG GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM 2>/dev/null || true

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/verify-dco.sh"
tmp_root="$(mktemp -d)"
trap 'rm -rf "${tmp_root}"' EXIT
failures=0

new_repo() {
  local dir="$tmp_root/$1"
  git init -q -b main "$dir"
  git -C "$dir" config user.name "Alice Example"
  git -C "$dir" config user.email "alice@example.com"
  git -C "$dir" config commit.gpgsign false
  git -C "$dir" commit --allow-empty -q -m "base"
  case_repo="$dir"
  case_base="$(git -C "$dir" rev-parse HEAD)"
}

expect() {
  local want="$1" name="$2" dir="$3" base="$4" head="$5"
  local rc=0 output
  output="$(cd "$dir" && bash "$script" "$base" "$head" 2>&1)" || rc=$?
  if [ "$rc" -eq "$want" ]; then
    echo "  ok   $name (exit $rc)"
  else
    echo "  FAIL $name: expected exit $want, got $rc"
    printf '%s\n' "$output" | sed 's/^/         | /'
    failures=$((failures + 1))
  fi
}

echo "verify-dco tests:"

new_repo signed
git -C "$case_repo" commit --allow-empty -q --signoff -m "signed"
expect 0 "matching author sign-off passes" "$case_repo" "$case_base" HEAD

new_repo missing
git -C "$case_repo" commit --allow-empty -q -m "unsigned"
expect 1 "missing sign-off fails" "$case_repo" "$case_base" HEAD

new_repo wrong-identity
git -C "$case_repo" commit --allow-empty -q -m "wrong identity" \
  -m "Signed-off-by: Mallory Example <mallory@example.com>"
expect 1 "unrelated sign-off fails" "$case_repo" "$case_base" HEAD

new_repo malformed
git -C "$case_repo" commit --allow-empty -q -m "malformed" \
  -m "Signed-off-by: Alice Example alice@example.com"
expect 1 "malformed sign-off fails" "$case_repo" "$case_base" HEAD

new_repo committer
GIT_AUTHOR_NAME="Bob Author" GIT_AUTHOR_EMAIL="bob@example.com" \
  git -C "$case_repo" commit --allow-empty -q --signoff -m "committer sign-off"
expect 0 "matching committer sign-off passes" "$case_repo" "$case_base" HEAD

new_repo case-insensitive
git -C "$case_repo" commit --allow-empty -q -m "case-insensitive" \
  -m "sIgNeD-oFf-By: alice example <ALICE@EXAMPLE.COM>"
expect 0 "identity and label comparison ignores case" "$case_repo" "$case_base" HEAD

new_repo mixed-range
git -C "$case_repo" commit --allow-empty -q --signoff -m "signed first"
git -C "$case_repo" commit --allow-empty -q -m "unsigned second"
expect 1 "one unsigned commit fails the whole range" "$case_repo" "$case_base" HEAD

new_repo merge
git -C "$case_repo" checkout -q -b side
git -C "$case_repo" commit --allow-empty -q --signoff -m "side change"
git -C "$case_repo" checkout -q main
git -C "$case_repo" commit --allow-empty -q --signoff -m "main change"
git -C "$case_repo" merge -q --no-ff side -m "unsigned merge"
expect 0 "merge commits are skipped" "$case_repo" "$case_base" HEAD

new_repo empty-range
expect 0 "empty range passes" "$case_repo" "$case_base" "$case_base"

new_repo missing-ref
expect 1 "missing refs fail closed" "$case_repo" no-such-ref HEAD

if [ "$failures" -ne 0 ]; then
  echo "$failures verify-dco test(s) failed." >&2
  exit 1
fi

echo "All verify-dco tests passed."
