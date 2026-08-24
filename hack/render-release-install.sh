#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

: "${CONTROLLER_IMAGE:?CONTROLLER_IMAGE is required}"
: "${SERVER_IMAGE:?SERVER_IMAGE is required}"
: "${CLEANUP_IMAGE:?CLEANUP_IMAGE is required}"
: "${OUTPUT_FILE:?OUTPUT_FILE is required}"

kustomize_cmd="${KUSTOMIZE_CMD:-kustomize}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
placeholder='ghcr.io/cachebox-project/inference-cache-shm-cleanup@sha256:0000000000000000000000000000000000000000000000000000000000000000'

for ref in "$CONTROLLER_IMAGE" "$SERVER_IMAGE" "$CLEANUP_IMAGE"; do
  if [[ ! "$ref" =~ ^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}$ ]]; then
    echo "release install image is not digest-pinned: $ref" >&2
    exit 1
  fi
done
if ! command -v "$kustomize_cmd" >/dev/null 2>&1; then
  echo "kustomize is unavailable: $kustomize_cmd" >&2
  exit 1
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
cp -R "$repo_root/config" "$workdir/config"

manager="$workdir/config/manager/manager.yaml"
if [[ "$(grep -Fc -- "$placeholder" "$manager")" != 1 ]]; then
  echo "cleanup image placeholder is missing or duplicated in $manager" >&2
  exit 1
fi
sed "s|$placeholder|$CLEANUP_IMAGE|" "$manager" >"$manager.tmp"
mv "$manager.tmp" "$manager"

(
  cd "$workdir/config/default"
  "$kustomize_cmd" edit set image \
    "controller=$CONTROLLER_IMAGE" \
    "server=$SERVER_IMAGE"
)

mkdir -p "$(dirname "$OUTPUT_FILE")"
"$kustomize_cmd" build "$workdir/config/default" >"$OUTPUT_FILE"

for ref in "$CONTROLLER_IMAGE" "$SERVER_IMAGE" "$CLEANUP_IMAGE"; do
  grep -Fq -- "$ref" "$OUTPUT_FILE" || {
    echo "rendered install manifest is missing $ref" >&2
    exit 1
  }
done
if grep -Fq -- "$placeholder" "$OUTPUT_FILE"; then
  echo "rendered install manifest retained the cleanup image placeholder" >&2
  exit 1
fi
