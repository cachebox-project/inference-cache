#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

controller='ghcr.io/cachebox-project/inference-cache-controller@sha256:1111111111111111111111111111111111111111111111111111111111111111'
server='ghcr.io/cachebox-project/inference-cache-server@sha256:2222222222222222222222222222222222222222222222222222222222222222'
cleanup='ghcr.io/cachebox-project/inference-cache-shm-cleanup@sha256:3333333333333333333333333333333333333333333333333333333333333333'
output="$workdir/install.yaml"

CONTROLLER_IMAGE="$controller" \
SERVER_IMAGE="$server" \
CLEANUP_IMAGE="$cleanup" \
OUTPUT_FILE="$output" \
KUSTOMIZE_CMD="${KUSTOMIZE_CMD:?KUSTOMIZE_CMD is required}" \
  hack/render-release-install.sh

for ref in "$controller" "$server" "$cleanup"; do
  grep -Fq -- "$ref" "$output"
done
if grep -Fq -- 'sha256:0000000000000000000000000000000000000000000000000000000000000000' "$output"; then
  echo "rendered install retained an all-zero digest" >&2
  exit 1
fi
grep -Fq -- 'sha256:0000000000000000000000000000000000000000000000000000000000000000' config/manager/manager.yaml

if CONTROLLER_IMAGE='controller:latest' SERVER_IMAGE="$server" CLEANUP_IMAGE="$cleanup" \
  OUTPUT_FILE="$workdir/invalid.yaml" KUSTOMIZE_CMD="$KUSTOMIZE_CMD" \
  hack/render-release-install.sh >/dev/null 2>&1; then
  echo "expected a mutable controller image to fail" >&2
  exit 1
fi

echo "release install rendering tests passed"
