#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

: "${IMAGE_TAG:=ci-smoke}"

syft_fake_version="$(awk '
  $1 == "version:" { in_block = 1; next }
  in_block && $1 == "linux-amd64-sha256:" { in_block = 0 }
  in_block && $1 == "default:" { print $2; exit }
' .github/actions/setup-syft/action.yml)"
export SYFT_FAKE_VERSION="${syft_fake_version#v}"

tmp_root="${RUNNER_TEMP:-/tmp}"
workdir="$(mktemp -d "${tmp_root%/}/sbom-registry-smoke.XXXXXX")"
trap 'rm -rf "$workdir"' EXIT

fakebin="$workdir/fakebin"
outdir="$workdir/out"
mkdir -p "$fakebin"

cat >"$fakebin/docker" <<'DOCKER_FAKE'
#!/bin/sh
set -eu

if [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "inspect" ]; then
  case "$4" in
    ghcr.io/cachebox-project/inference-cache-controller:ci-*|\
    ghcr.io/cachebox-project/inference-cache-server:ci-*|\
    ghcr.io/cachebox-project/inference-cache-subscriber:ci-*) ;;
    *) echo "unexpected inspect ref: $4" >&2; exit 2 ;;
  esac
  case "${DOCKER_FAKE_MODE:-missing}" in
    authfail) echo "unauthorized: authentication required" >&2; exit 42 ;;
    helperfail) echo 'error getting credentials - err: exec: "docker-credential-ghcr": executable file not found in $PATH' >&2; exit 1 ;;
    existing)
      printf 'Name:      %s\nMediaType: application/vnd.oci.image.index.v1+json\nDigest:    sha256:1111111111111111111111111111111111111111111111111111111111111111\nManifests:\n  Name:      %s@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n  Platform:  linux/amd64\n  Name:      %s@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n  Platform:  linux/arm64\n' "$4" "${4%:*}" "${4%:*}"
      exit 0
      ;;
    existing-amd64)
      printf 'Name:      %s\nMediaType: application/vnd.oci.image.index.v1+json\nDigest:    sha256:3333333333333333333333333333333333333333333333333333333333333333\nManifests:\n  Name:      %s@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\n  Platform:  linux/amd64\n' "$4" "${4%:*}"
      exit 0
      ;;
    existing-single)
      printf 'Name:      %s\nMediaType: application/vnd.oci.image.manifest.v1+json\nDigest:    sha256:4444444444444444444444444444444444444444444444444444444444444444\n' "$4"
      exit 0
      ;;
    missing) echo "manifest unknown: manifest unknown" >&2; exit 1 ;;
    denied) echo "denied: requested access to the resource is denied" >&2; exit 1 ;;
    missing-notfound) echo "$4: not found" >&2; exit 1 ;;
    *) echo "unexpected fake mode: ${DOCKER_FAKE_MODE:-}" >&2; exit 2 ;;
  esac
fi

if [ "$1" = "buildx" ] && [ "$2" = "build" ]; then
  case "${DOCKER_FAKE_MODE:-missing}" in
    missing|missing-notfound) ;;
    *)
      echo "build must not run for mode ${DOCKER_FAKE_MODE:-}" >&2
      exit 2
      ;;
  esac
  shift 2
  metadata=""
  platform=""
  push=0
  tag=""
  target=""
  dockerfile=""
  context=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --push) push=1; shift ;;
      --metadata-file) metadata="$2"; shift 2 ;;
      --platform) platform="$2"; shift 2 ;;
      --target) target="$2"; shift 2 ;;
      -f) dockerfile="$2"; shift 2 ;;
      -t) tag="$2"; shift 2 ;;
      *)
        if [ "$#" -eq 1 ]; then
          context="$1"
          shift
        else
          echo "unexpected build arg: $1" >&2
          exit 2
        fi
        ;;
    esac
  done
  case "$tag" in
    ghcr.io/cachebox-project/inference-cache-controller:ci-*) expected_target=controller ;;
    ghcr.io/cachebox-project/inference-cache-server:ci-*) expected_target=server ;;
    ghcr.io/cachebox-project/inference-cache-subscriber:ci-*) expected_target=subscriber ;;
    *) echo "unexpected build tag: $tag" >&2; exit 2 ;;
  esac
  test -n "$metadata"
  test "$push" = "1"
  test "$platform" = "linux/amd64,linux/arm64"
  test "$target" = "$expected_target"
  test "$dockerfile" = "dockerfiles/Dockerfile"
  test "$context" = "."
  printf '{"containerimage.digest":"sha256:2222222222222222222222222222222222222222222222222222222222222222"}\n' >"$metadata"
  exit 0
fi

echo "unexpected docker invocation: $*" >&2
exit 2
DOCKER_FAKE

cat >"$fakebin/syft" <<'SYFT_FAKE'
#!/bin/sh
set -eu

if [ "${1:-}" = "version" ]; then
  echo "Application: syft"
  echo "Version:    ${SYFT_FAKE_VERSION:?missing fake Syft version}"
  exit 0
fi

test "${1:-}" = "scan"
shift
out=""
platform=""
source=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --platform) platform="$2"; shift 2 ;;
    -o) out=${2#spdx-json=}; shift 2 ;;
    spdx-json=*) out=${1#spdx-json=}; shift ;;
    registry:*) source="$1"; shift ;;
    *) shift ;;
  esac
done
case "$platform" in
  linux/amd64|linux/arm64|"") ;;
  *) echo "unexpected syft platform: $platform" >&2; exit 2 ;;
esac
if [ -z "$platform" ]; then
  case "$source" in
    registry:ghcr.io/cachebox-project/inference-cache-controller@sha256:4444444444444444444444444444444444444444444444444444444444444444|\
    registry:ghcr.io/cachebox-project/inference-cache-server@sha256:4444444444444444444444444444444444444444444444444444444444444444|\
    registry:ghcr.io/cachebox-project/inference-cache-subscriber@sha256:4444444444444444444444444444444444444444444444444444444444444444) ;;
    *) echo "unexpected syft source/platform: $source $platform" >&2; exit 2 ;;
  esac
else
  case "$source|$platform" in
    registry:ghcr.io/cachebox-project/inference-cache-controller@sha256:1111111111111111111111111111111111111111111111111111111111111111\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-controller@sha256:1111111111111111111111111111111111111111111111111111111111111111\|linux/arm64|\
    registry:ghcr.io/cachebox-project/inference-cache-server@sha256:1111111111111111111111111111111111111111111111111111111111111111\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-server@sha256:1111111111111111111111111111111111111111111111111111111111111111\|linux/arm64|\
    registry:ghcr.io/cachebox-project/inference-cache-subscriber@sha256:1111111111111111111111111111111111111111111111111111111111111111\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-subscriber@sha256:1111111111111111111111111111111111111111111111111111111111111111\|linux/arm64|\
    registry:ghcr.io/cachebox-project/inference-cache-controller@sha256:2222222222222222222222222222222222222222222222222222222222222222\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-controller@sha256:2222222222222222222222222222222222222222222222222222222222222222\|linux/arm64|\
    registry:ghcr.io/cachebox-project/inference-cache-server@sha256:2222222222222222222222222222222222222222222222222222222222222222\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-server@sha256:2222222222222222222222222222222222222222222222222222222222222222\|linux/arm64|\
    registry:ghcr.io/cachebox-project/inference-cache-subscriber@sha256:2222222222222222222222222222222222222222222222222222222222222222\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-subscriber@sha256:2222222222222222222222222222222222222222222222222222222222222222\|linux/arm64|\
    registry:ghcr.io/cachebox-project/inference-cache-controller@sha256:3333333333333333333333333333333333333333333333333333333333333333\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-server@sha256:3333333333333333333333333333333333333333333333333333333333333333\|linux/amd64|\
    registry:ghcr.io/cachebox-project/inference-cache-subscriber@sha256:3333333333333333333333333333333333333333333333333333333333333333\|linux/amd64) ;;
    *) echo "unexpected syft source/platform: $source $platform" >&2; exit 2 ;;
  esac
fi
if [ -z "$out" ]; then
  echo "missing spdx-json output" >&2
  exit 2
fi
mkdir -p "$(dirname "$out")"
printf '{"spdxVersion":"SPDX-2.3","packages":[{"name":"fake"}]}\n' >"$out"
SYFT_FAKE

chmod +x "$fakebin/docker" "$fakebin/syft"

if PATH="$fakebin:$PATH" DOCKER_FAKE_MODE=authfail make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir" SBOM_REGISTRY_PUBLISH_MISSING=1 SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile; then
  echo "expected registry auth failure to stop without publishing" >&2
  exit 1
fi
if PATH="$fakebin:$PATH" DOCKER_FAKE_MODE=helperfail make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir" SBOM_REGISTRY_PUBLISH_MISSING=1 SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile; then
  echo "expected credential-helper failure to stop without publishing" >&2
  exit 1
fi
if PATH="$fakebin:$PATH" DOCKER_FAKE_MODE=denied make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir" SBOM_REGISTRY_PUBLISH_MISSING=1 SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile; then
  echo "expected registry access denial to stop without publishing" >&2
  exit 1
fi

PATH="$fakebin:$PATH" DOCKER_FAKE_MODE=existing make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir/existing" SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile
for component in controller server subscriber; do
  for platform in linux_amd64 linux_arm64; do
    sbom="$outdir/existing/inference-cache-${component}-${platform}-${IMAGE_TAG}.spdx.json"
    test -s "$sbom"
    jq -e '.spdxVersion and ((.packages | type) == "array") and ((.packages | length) > 0)' "$sbom" >/dev/null
  done
done

PATH="$fakebin:$PATH" DOCKER_FAKE_MODE=existing-amd64 make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir/existing-amd64" SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile
for component in controller server subscriber; do
  sbom="$outdir/existing-amd64/inference-cache-${component}-linux_amd64-${IMAGE_TAG}.spdx.json"
  test -s "$sbom"
  jq -e '.spdxVersion and ((.packages | type) == "array") and ((.packages | length) > 0)' "$sbom" >/dev/null
  test ! -e "$outdir/existing-amd64/inference-cache-${component}-linux_arm64-${IMAGE_TAG}.spdx.json"
done

PATH="$fakebin:$PATH" DOCKER_FAKE_MODE=existing-single make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir/existing-single" SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile
for component in controller server subscriber; do
  sbom="$outdir/existing-single/inference-cache-${component}-${IMAGE_TAG}.spdx.json"
  test -s "$sbom"
  jq -e '.spdxVersion and ((.packages | type) == "array") and ((.packages | length) > 0)' "$sbom" >/dev/null
  test ! -e "$outdir/existing-single/inference-cache-${component}-linux_amd64-${IMAGE_TAG}.spdx.json"
  test ! -e "$outdir/existing-single/inference-cache-${component}-linux_arm64-${IMAGE_TAG}.spdx.json"
done

PATH="$fakebin:$PATH" DOCKER_FAKE_MODE=missing make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir/missing" SBOM_REGISTRY_PUBLISH_MISSING=1 SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile
for component in controller server subscriber; do
  for platform in linux_amd64 linux_arm64; do
    sbom="$outdir/missing/inference-cache-${component}-${platform}-${IMAGE_TAG}.spdx.json"
    test -s "$sbom"
    jq -e '.spdxVersion and ((.packages | type) == "array") and ((.packages | length) > 0)' "$sbom" >/dev/null
  done
done

for mode in missing-notfound; do
  PATH="$fakebin:$PATH" DOCKER_FAKE_MODE="$mode" make sbom-registry-images TAG="$IMAGE_TAG" SBOM_DIR="$outdir/$mode" SBOM_REGISTRY_PUBLISH_MISSING=1 SBOM_IMAGE_CONTEXT=. SBOM_DOCKERFILE=dockerfiles/Dockerfile
  for component in controller server subscriber; do
    for platform in linux_amd64 linux_arm64; do
      sbom="$outdir/$mode/inference-cache-${component}-${platform}-${IMAGE_TAG}.spdx.json"
      test -s "$sbom"
      jq -e '.spdxVersion and ((.packages | type) == "array") and ((.packages | length) > 0)' "$sbom" >/dev/null
    done
  done
done

echo "SBOM registry smoke passed"
