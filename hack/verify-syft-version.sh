#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

action=".github/actions/setup-syft/action.yml"
version="$(awk '
  $1 == "version:" { in_block = 1; next }
  in_block && $1 == "linux-amd64-sha256:" { in_block = 0 }
  in_block && $1 == "default:" { print $2; exit }
' "$action")"
checksum="$(awk '
  $1 == "linux-amd64-sha256:" { in_block = 1; next }
  in_block && $1 == "runs:" { in_block = 0 }
  in_block && $1 == "default:" { print $2; exit }
' "$action")"

if [ -z "$version" ] || [ -z "$checksum" ]; then
  echo "could not read Syft defaults from $action" >&2
  exit 1
fi

failed=0
require_literal() {
  file="$1"
  needle="$2"
  label="$3"
  if ! grep -Fq "$needle" "$file"; then
    echo "missing Syft pin sync in $file: $label" >&2
    failed=1
  fi
}

require_literal Makefile "SYFT_VERSION ?= $version" "Makefile SYFT_VERSION"
require_literal .github/workflows/ci.yml "SYFT_VERSION: $version" "CI workflow SYFT_VERSION"
require_literal .github/workflows/release-sbom.yml "SYFT_VERSION: $version" "Release SBOM workflow SYFT_VERSION"
require_literal "$action" "default_version=\"$version\"" "setup action shell default version"
require_literal "$action" "default_sha256=\"$checksum\"" "setup action shell default checksum"

if [ "$failed" -ne 0 ]; then
  echo "Syft pin drift detected. Keep $action, Makefile, and workflows coordinated." >&2
  exit 1
fi

echo "Syft pin is synchronized at $version"
