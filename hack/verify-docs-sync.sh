#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

#
# verify-docs-sync.sh — keep the documentation from drifting behind the API.
#
# Fails if a change touches a documented *public surface* — the CRD API types
# (api/v1alpha1/*_types.go) or the gRPC contract (proto/**) — without also
# updating the documentation (the docs site under site/ and/or the design docs
# under docs/). See CONTRIBUTING.md "Documentation stays in sync".
#
# Usage:
#   hack/verify-docs-sync.sh [base-ref]      # base-ref defaults to origin/main
#
# The scope is intentionally narrow so false positives are near zero: only the
# two public-contract surfaces trigger the requirement. Widen TRIGGER_RE below
# to cover more surfaces (e.g. the CLI) if the project wants a broader rule.
#
# CI escape hatch: the workflow skips this check when the PR carries the
# 'no-docs-needed' label, for the rare public-surface change that genuinely
# needs no doc update.
set -euo pipefail

BASE="${1:-origin/main}"

# Public surfaces whose change must be reflected in the docs.
TRIGGER_RE='^(api/v1alpha1/[^/]*_types\.go|proto/.*\.proto)$'
# Paths that count as a documentation update.
DOC_RE='^(site/|docs/)'

# Fail closed: a gate presented as enforcement must not pass silently when it
# cannot run. A missing base ref is an operator/CI error, not a reason to skip.
if ! git rev-parse --verify --quiet "${BASE}^{commit}" >/dev/null; then
  {
    echo "✗ docs-sync: base ref '${BASE}' not found — cannot verify docs are in sync."
    echo "  Fetch it first (e.g. 'git fetch origin main'), or pass the correct base:"
    echo "    hack/verify-docs-sync.sh <base-ref>"
    echo "    make verify-docs-sync DOCS_SYNC_BASE=<base-ref>"
  } >&2
  exit 1
fi

# Prefer the merge base (three-dot semantics) so only THIS branch's changes are
# considered. Shallow CI checkouts often have no reachable merge base; there the
# PR HEAD is a base+head merge commit, so a plain two-endpoint diff against the
# base tip yields exactly the PR's changes.
if merge_base="$(git merge-base "${BASE}" HEAD 2>/dev/null)"; then
  diff_base="${merge_base}"
else
  diff_base="${BASE}"
fi

changed="$(git diff --name-only "${diff_base}" HEAD)"
if [ -z "${changed}" ]; then
  echo "✓ docs-sync: no file changes vs ${BASE}"
  exit 0
fi

triggers="$(printf '%s\n' "${changed}" | grep -E "${TRIGGER_RE}" || true)"
if [ -z "${triggers}" ]; then
  echo "✓ docs-sync: no documented public-surface change"
  exit 0
fi

# Only ADDED / MODIFIED / RENAMED-to docs count as a documentation update.
# --diff-filter=ACMR excludes deletions (D), so removing a file under docs/ or
# site/ can't masquerade as "the docs were updated".
docs="$(git diff --name-only --diff-filter=ACMR "${diff_base}" HEAD | grep -E "${DOC_RE}" || true)"
if [ -n "${docs}" ]; then
  echo "✓ docs-sync: public-surface change is accompanied by a docs update"
  exit 0
fi

{
  echo "✗ docs-sync: this change touches a documented public surface but updates no docs."
  echo ""
  echo "  Changed public-surface file(s):"
  printf '%s\n' "${triggers}" | sed 's/^/    /'
  echo ""
  echo "  Update the documentation to match, in the same PR — the docs site"
  echo "  under site/ and/or the design docs under docs/. For example:"
  echo "    - proto/ change            -> docs/design/grpc-contract.md (+ the site gRPC reference)"
  echo "    - CRD api/v1alpha1 change  -> the matching site/content concept + reference page"
  echo ""
  echo "  If this change genuinely needs no doc update, add the 'no-docs-needed'"
  echo "  label to the PR to waive this check."
} >&2
exit 1
