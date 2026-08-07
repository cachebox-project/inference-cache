#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

dockerfile="${MINIMAL_IMAGE_DOCKERFILE:-dockerfiles/Dockerfile}"
expected_base="${MINIMAL_RUNTIME_BASE:-gcr.io/distroless/static-debian13:nonroot@sha256:f7f8f729987ad0fdf6b05eeeae94b26e6a0f613bdf46feea7fc40f7bd72953e6}"
docker_cmd_string="${DOCKER:-docker}"
read -r -a docker_cmd <<<"$docker_cmd_string"
dockerfile_only=0

if [ "${1:-}" = "--dockerfile-only" ]; then
  dockerfile_only=1
  shift
fi
if [ "$#" -gt 1 ]; then
  echo "usage: $0 [--dockerfile-only] [Dockerfile]" >&2
  exit 2
fi
if [ "$#" -eq 1 ]; then
  dockerfile="$1"
fi
if [ ! -f "$dockerfile" ]; then
  echo "minimal-image check: Dockerfile not found: $dockerfile" >&2
  exit 1
fi

targets=(controller server subscriber)
entrypoints=(/controller /server /kvevent-subscriber)
images=(
  "${IMG:-ghcr.io/cachebox-project/inference-cache-controller:dev}"
  "${SERVER_IMG:-ghcr.io/cachebox-project/inference-cache-server:dev}"
  "${SUBSCRIBER_IMG:-ghcr.io/cachebox-project/inference-cache-subscriber:dev}"
)

stage_base() {
  local target="$1"
  awk -v target="$target" '
    toupper($1) == "FROM" {
      for (i = 2; i < NF; i++) {
        if (toupper($i) == "AS" && $(i + 1) == target) {
          print $(i - 1)
        }
      }
    }
  ' "$dockerfile"
}

for target in "${targets[@]}"; do
  bases="$(stage_base "$target")"
  base_count="$(printf '%s\n' "$bases" | awk 'NF { count++ } END { print count + 0 }')"
  if [ "$base_count" -ne 1 ]; then
    echo "minimal-image check: target $target must have exactly one final stage in $dockerfile" >&2
    exit 1
  fi
  if [ "$bases" != "$expected_base" ]; then
    echo "minimal-image check: target $target uses $bases, want $expected_base" >&2
    exit 1
  fi
done
echo "minimal-image check: production targets use $expected_base"

if [ "$dockerfile_only" -eq 1 ]; then
  exit 0
fi

if [ "${#docker_cmd[@]}" -eq 0 ] || ! command -v "${docker_cmd[0]}" >/dev/null 2>&1; then
  echo "minimal-image check: Docker command not found: $docker_cmd_string" >&2
  exit 1
fi

tmp_root="${RUNNER_TEMP:-/tmp}"
workdir="$(mktemp -d "${tmp_root%/}/minimal-images.XXXXXX")"
active_container=""
cleanup() {
  if [ -n "$active_container" ]; then
    "${docker_cmd[@]}" rm -f "$active_container" >/dev/null 2>&1 || true
  fi
  rm -rf "$workdir"
}
trap cleanup EXIT

for i in "${!targets[@]}"; do
  target="${targets[$i]}"
  image="${images[$i]}"
  entrypoint="${entrypoints[$i]}"

  if ! "${docker_cmd[@]}" image inspect "$image" >/dev/null 2>&1; then
    echo "minimal-image check: image not found: $image (run make image-build first)" >&2
    exit 1
  fi

  user="$("${docker_cmd[@]}" image inspect --format '{{.Config.User}}' "$image")"
  if [ "$user" != "65532:65532" ]; then
    echo "minimal-image check: $target image user is '$user', want '65532:65532'" >&2
    exit 1
  fi

  actual_entrypoint="$("${docker_cmd[@]}" image inspect --format '{{json .Config.Entrypoint}}' "$image")"
  expected_entrypoint="[\"$entrypoint\"]"
  if [ "$actual_entrypoint" != "$expected_entrypoint" ]; then
    echo "minimal-image check: $target entrypoint is $actual_entrypoint, want $expected_entrypoint" >&2
    exit 1
  fi

  active_container="$("${docker_cmd[@]}" create "$image")"
  archive="$workdir/$target.tar"
  members="$workdir/$target.members"
  member_modes="$workdir/$target.member-modes"
  "${docker_cmd[@]}" export -o "$archive" "$active_container"
  "${docker_cmd[@]}" rm "$active_container" >/dev/null
  active_container=""
  tar -tf "$archive" >"$members"
  tar -tvf "$archive" | awk '{ print $1 }' >"$member_modes"
  member_count="$(wc -l <"$members")"
  member_mode_count="$(wc -l <"$member_modes")"
  if [ "$member_count" -ne "$member_mode_count" ]; then
    echo "minimal-image check: could not map $target archive members to modes" >&2
    exit 1
  fi

  payload="${entrypoint#/}"
  payload_count=0
  payload_mode=""
  while IFS= read -r member && IFS= read -r entry_mode <&3; do
    path="${member#./}"
    path="${path%/}"
    if [ "$path" = "$payload" ]; then
      payload_count=$((payload_count + 1))
      payload_mode="$entry_mode"
    fi
  done 3<"$member_modes" <"$members"
  if [ "$payload_count" -ne 1 ]; then
    echo "minimal-image check: $target image must contain exactly one payload at $entrypoint" >&2
    exit 1
  fi
  if [[ "$payload_mode" != -* || "${payload_mode:9:1}" != [xt] ]]; then
    echo "minimal-image check: $target payload $entrypoint must be a regular file executable by the declared non-root user (mode: $payload_mode)" >&2
    exit 1
  fi

  forbidden_names=(
    sh bash ash dash zsh ksh fish csh tcsh pwsh powershell busybox
    apk apt apt-get dpkg dpkg-deb dpkg-query
    rpm rpm2cpio dnf microdnf yum zypper pacman
  )
  # Pair the two tar listings by archive order so member names can contain spaces.
  while IFS= read -r member && IFS= read -r entry_mode <&3; do
    path="${member#./}"
    path="${path%/}"
    basename="${path##*/}"
    entry_type="${entry_mode:0:1}"
    for forbidden in "${forbidden_names[@]}"; do
      if [ "$basename" = "$forbidden" ]; then
        case "$entry_type" in
          -|l|h)
            echo "minimal-image check: $target image contains forbidden runtime tool /$path" >&2
            exit 1
            ;;
        esac
      fi
    done
  done 3<"$member_modes" <"$members"

  echo "minimal-image check: $target is non-root and contains no shell or package manager"
done
