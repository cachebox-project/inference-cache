#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG is required}"

if [[ ! "$RELEASE_TAG" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
  echo "release tag is not Docker-compatible: $RELEASE_TAG" >&2
  exit 1
fi

docker_cmd="${DOCKER_BUILD_CMD:-docker}"
controller_repo="${CONTROLLER_IMAGE_REPO:-ghcr.io/cachebox-project/inference-cache-controller}"
server_repo="${SERVER_IMAGE_REPO:-ghcr.io/cachebox-project/inference-cache-server}"
subscriber_repo="${SUBSCRIBER_IMAGE_REPO:-ghcr.io/cachebox-project/inference-cache-subscriber}"

if ! command -v "$docker_cmd" >/dev/null 2>&1; then
  echo "registry client is unavailable: $docker_cmd" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to resolve release image digests" >&2
  exit 1
fi

resolve_digest() {
  local component="$1"
  local repo="$2"
  local ref="${repo}:${RELEASE_TAG}"
  local manifest digest

  if ! manifest="$("$docker_cmd" buildx imagetools inspect --format '{{json .Manifest}}' "$ref")"; then
    echo "unable to inspect release image: $ref" >&2
    return 1
  fi
  digest="$(jq -r '.digest // empty' <<<"$manifest")"
  if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "release image did not resolve to a valid sha256 digest: $ref" >&2
    return 1
  fi

  printf '%s=%s\n' "$component" "$digest"
}

resolve_digest controller "$controller_repo"
resolve_digest server "$server_repo"
resolve_digest subscriber "$subscriber_repo"
