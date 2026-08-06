#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

tmp_root="${RUNNER_TEMP:-/tmp}"
workdir="$(mktemp -d "${tmp_root%/}/release-image-digests.XXXXXX")"
trap 'rm -rf "$workdir"' EXIT

fake_docker="$workdir/docker"
cat >"$fake_docker" <<'DOCKER_FAKE'
#!/usr/bin/env bash
set -euo pipefail

test "$1" = buildx
test "$2" = imagetools
test "$3" = inspect
test "$4" = --format
test "$5" = '{{json .Manifest}}'
ref="$6"
printf '%s\n' "$ref" >>"${DOCKER_FAKE_LOG:?}"

if [[ "${DOCKER_FAKE_MODE:-valid}" == inspect-failure ]]; then
  echo "registry unavailable" >&2
  exit 1
fi

case "$ref" in
  ghcr.io/cachebox-project/inference-cache-controller:v1.2.3)
    digit=1
    ;;
  ghcr.io/cachebox-project/inference-cache-server:v1.2.3)
    digit=2
    ;;
  ghcr.io/cachebox-project/inference-cache-subscriber:v1.2.3)
    digit=3
    ;;
  *)
    echo "unexpected image ref: $ref" >&2
    exit 2
    ;;
esac

if [[ "${DOCKER_FAKE_MODE:-valid}" == invalid-digest && "$digit" == 2 ]]; then
  printf '{"digest":"sha256:not-a-digest"}\n'
else
  printf '{"digest":"sha256:%064d"}\n' "$digit"
fi
DOCKER_FAKE
chmod +x "$fake_docker"

export DOCKER_BUILD_CMD="$fake_docker"
export DOCKER_FAKE_LOG="$workdir/docker.log"

actual="$(RELEASE_TAG=v1.2.3 hack/resolve-release-image-digests.sh)"
expected="$(printf '%s\n' \
  'controller=sha256:0000000000000000000000000000000000000000000000000000000000000001' \
  'server=sha256:0000000000000000000000000000000000000000000000000000000000000002' \
  'subscriber=sha256:0000000000000000000000000000000000000000000000000000000000000003')"
if [[ "$actual" != "$expected" ]]; then
  echo "unexpected digest output" >&2
  diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2
  exit 1
fi

if DOCKER_FAKE_MODE=invalid-digest RELEASE_TAG=v1.2.3 hack/resolve-release-image-digests.sh >/dev/null; then
  echo "expected malformed registry digest to fail closed" >&2
  exit 1
fi

if DOCKER_FAKE_MODE=inspect-failure RELEASE_TAG=v1.2.3 hack/resolve-release-image-digests.sh >/dev/null; then
  echo "expected registry inspection failure to fail closed" >&2
  exit 1
fi

before_invalid_tag="$(wc -l <"$DOCKER_FAKE_LOG")"
if RELEASE_TAG='../bad' hack/resolve-release-image-digests.sh >/dev/null; then
  echo "expected invalid release tag to fail before registry access" >&2
  exit 1
fi
after_invalid_tag="$(wc -l <"$DOCKER_FAKE_LOG")"
if [[ "$before_invalid_tag" != "$after_invalid_tag" ]]; then
  echo "invalid release tag reached the registry client" >&2
  exit 1
fi

echo "release image digest resolution tests passed"
