#!/usr/bin/env bash
#
# Verify that every non-merge commit in a revision range carries a valid DCO
# Signed-off-by trailer. A trailer is valid when its name and email match the
# commit author or committer, case-insensitively.
#
# Usage:
#   hack/verify-dco.sh <base-ref> <head-ref>
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <base-ref> <head-ref>" >&2
  exit 2
fi

base="$1"
head="$2"

for ref in "$base" "$head"; do
  if ! git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    echo "ERROR: DCO verification ref '${ref}' is not a commit" >&2
    exit 1
  fi
done

# Match the canonical DCO App's identity behavior: trailer labels and
# author/committer identities are compared case-insensitively.
shopt -s nocasematch
signoff_pattern='^[[:space:]]*Signed-off-by:[[:space:]]+(.+)[[:space:]]+<([^<>[:space:]]+)>[[:space:]]*$'
signoff_prefix='^[[:space:]]*Signed-off-by:'
email_pattern='^[^@[:space:]<>]+@[^@[:space:]<>]+$'

same_identity() {
  local actual_name="$1" actual_email="$2" expected_name="$3" expected_email="$4"
  [[ "$actual_name" == "$expected_name" && "$actual_email" == "$expected_email" ]]
}

failed=0
checked=0

while IFS= read -r commit; do
  [ -n "$commit" ] || continue

  parent_count="$(git show -s --format=%P "$commit" | awk '{print NF}')"
  if [ "$parent_count" -gt 1 ]; then
    echo "SKIP ${commit:0:12} (merge commit)"
    continue
  fi

  checked=$((checked + 1))
  author_name="$(git show -s --format=%an "$commit")"
  author_email="$(git show -s --format=%ae "$commit")"
  committer_name="$(git show -s --format=%cn "$commit")"
  committer_email="$(git show -s --format=%ce "$commit")"
  subject="$(git show -s --format=%s "$commit")"
  matched=0
  found_signoffs=()

  while IFS= read -r line; do
    if [[ "$line" =~ $signoff_prefix ]]; then
      found_signoffs+=("$line")
    fi

    if [[ "$line" =~ $signoff_pattern ]]; then
      name="${BASH_REMATCH[1]}"
      email="${BASH_REMATCH[2]}"
      # The name capture is greedy; trim any whitespace immediately before
      # the angle-bracketed email before comparing identities.
      name="${name#"${name%%[![:space:]]*}"}"
      name="${name%"${name##*[![:space:]]}"}"

      if [[ "$email" =~ $email_pattern ]] && {
        same_identity "$name" "$email" "$author_name" "$author_email" ||
          same_identity "$name" "$email" "$committer_name" "$committer_email"
      }; then
        matched=1
      fi
    fi
  done < <(git show -s --format=%B "$commit")

  if [ "$matched" -eq 1 ]; then
    echo "OK   ${commit:0:12} ${subject}"
    continue
  fi

  failed=1
  {
    echo "ERROR: ${commit:0:12} ${subject}"
    echo "  requires Signed-off-by matching either:"
    echo "    author:    ${author_name} <${author_email}>"
    echo "    committer: ${committer_name} <${committer_email}>"
    if [ "${#found_signoffs[@]}" -eq 0 ]; then
      echo "  found: no Signed-off-by trailer"
    else
      echo "  found:"
      printf '    %s\n' "${found_signoffs[@]}"
    fi
  } >&2
done < <(git rev-list --reverse "${base}..${head}")

if [ "$failed" -ne 0 ]; then
  {
    echo
    echo "Add your certification when creating a commit:"
    echo "  git commit --signoff"
    echo "For the latest commit, after reviewing it:"
    echo "  git commit --amend --no-edit --signoff"
  } >&2
  exit 1
fi

if [ "$checked" -eq 0 ]; then
  echo "OK   no non-merge commits to verify in ${base}..${head}"
else
  echo "OK   DCO sign-off verified for ${checked} non-merge commit(s)"
fi
