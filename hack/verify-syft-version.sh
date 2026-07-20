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
require_equal() {
  label="$1"
  actual="$2"
  expected="$3"
  if [ "$actual" != "$expected" ]; then
    echo "Syft pin mismatch for $label: got '$actual', expected '$expected'" >&2
    failed=1
  fi
}

makefile_version="$(sed -n 's/^SYFT_VERSION[[:space:]]*?=[[:space:]]*//p' Makefile | head -n1)"
ci_version="$(awk '$1 == "SYFT_VERSION:" { print $2; exit }' .github/workflows/ci.yml)"
release_version="$(awk '$1 == "SYFT_VERSION:" { print $2; exit }' .github/workflows/release-sbom.yml)"
action_shell_version="$(awk -F '"' '/default_version=/{ print $2; exit }' "$action")"
action_shell_checksum="$(awk -F '"' '/default_sha256=/{ print $2; exit }' "$action")"

require_equal "Makefile SYFT_VERSION" "$makefile_version" "$version"
require_equal "CI workflow SYFT_VERSION" "$ci_version" "$version"
require_equal "Release SBOM workflow SYFT_VERSION" "$release_version" "$version"
require_equal "setup action shell default version" "$action_shell_version" "$version"
require_equal "setup action shell default checksum" "$action_shell_checksum" "$checksum"

if [ "$failed" -ne 0 ]; then
  echo "Syft pin drift detected. Keep $action, Makefile, and workflows coordinated." >&2
  exit 1
fi

echo "Syft pin is synchronized at $version"
