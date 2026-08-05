#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="$repo_root/hack/verify-minimal-images.sh"
tmp_root="${RUNNER_TEMP:-/tmp}"
workdir="$(mktemp -d "${tmp_root%/}/minimal-images-test.XXXXXX")"
trap 'rm -rf "$workdir"' EXIT

valid_dockerfile="$workdir/Dockerfile.valid"
cat >"$valid_dockerfile" <<'EOF'
FROM golang:1.26.5 AS builder
FROM gcr.io/distroless/static-debian13:nonroot AS controller
FROM gcr.io/distroless/static-debian13:nonroot AS server
FROM gcr.io/distroless/static-debian13:nonroot AS subscriber
EOF

expect_failure() {
  local name="$1"
  shift
  if "$@" >"$workdir/$name.log" 2>&1; then
    echo "expected failure: $name" >&2
    exit 1
  fi
  echo "  ok   $name fails closed"
}

"$verifier" --dockerfile-only "$valid_dockerfile" >/dev/null
echo "  ok   supported distroless stages pass"

wrong_base="$workdir/Dockerfile.wrong-base"
sed 's#static-debian13:nonroot#base-debian13:nonroot#' "$valid_dockerfile" >"$wrong_base"
expect_failure wrong-base "$verifier" --dockerfile-only "$wrong_base"

missing_target="$workdir/Dockerfile.missing-target"
sed '/ AS subscriber$/d' "$valid_dockerfile" >"$missing_target"
expect_failure missing-target "$verifier" --dockerfile-only "$missing_target"

fake_docker="$workdir/docker"
cat >"$fake_docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--test-wrapper" ]; then
  shift
fi

if [ "$1" = "image" ] && [ "$2" = "inspect" ]; then
  shift 2
  if [ "${1:-}" != "--format" ]; then
    if [ "${FAKE_MISSING_IMAGE:-0}" = "1" ]; then
      exit 1
    fi
    exit 0
  fi
  format="$2"
  image="$3"
  case "$format" in
    '{{.Config.User}}') printf '%s\n' "${FAKE_IMAGE_USER:-65532:65532}" ;;
    '{{json .Config.Entrypoint}}')
      if [ "${FAKE_BAD_ENTRYPOINT:-0}" = "1" ]; then
        echo '["/wrong"]'
        exit 0
      fi
      case "$image" in
        controller:test) echo '["/controller"]' ;;
        server:test) echo '["/server"]' ;;
        subscriber:test) echo '["/kvevent-subscriber"]' ;;
        *) echo "unexpected image: $image" >&2; exit 2 ;;
      esac
      ;;
    *) echo "unexpected inspect format: $format" >&2; exit 2 ;;
  esac
  exit 0
fi

if [ "$1" = "create" ]; then
  case "$2" in
    controller:test) echo cid-controller ;;
    server:test) echo cid-server ;;
    subscriber:test) echo cid-subscriber ;;
    *) echo "unexpected create image: $2" >&2; exit 2 ;;
  esac
  exit 0
fi

if [ "$1" = "export" ] && [ "$2" = "-o" ]; then
  archive="$3"
  component="${4#cid-}"
  root="$FAKE_DOCKER_ROOT/$component"
  rm -rf "$root"
  mkdir -p "$root"
  case "$component" in
    controller) payload="$root/controller" ;;
    server) payload="$root/server" ;;
    subscriber) payload="$root/kvevent-subscriber" ;;
    *) echo "unexpected container: $4" >&2; exit 2 ;;
  esac
  if [ "${FAKE_MISSING_PAYLOAD:-0}" != "1" ]; then
    if [ "${FAKE_DIRECTORY_PAYLOAD:-0}" = "1" ]; then
      mkdir -p "$payload"
    elif [ "${FAKE_SYMLINK_PAYLOAD:-0}" = "1" ]; then
      ln -s /missing-target "$payload"
    else
      touch "$payload"
      if [ "${FAKE_NON_EXECUTABLE_PAYLOAD:-0}" != "1" ]; then
        chmod +x "$payload"
      fi
    fi
  fi
  if [ "${FAKE_INCLUDE_SHELL:-0}" = "1" ]; then
    mkdir -p "$root/bin"
    touch "$root/bin/sh"
  fi
  if [ "${FAKE_INCLUDE_APT:-0}" = "1" ]; then
    mkdir -p "$root/usr/bin"
    touch "$root/usr/bin/apt-get"
  fi
  if [ -n "${FAKE_EXTRA_PATH:-}" ]; then
    mkdir -p "$(dirname "$root/$FAKE_EXTRA_PATH")"
    touch "$root/$FAKE_EXTRA_PATH"
  fi
  if [ -n "${FAKE_EXTRA_DIRECTORY:-}" ]; then
    mkdir -p "$root/$FAKE_EXTRA_DIRECTORY"
  fi
  if [ -n "${FAKE_EXTRA_SYMLINK:-}" ]; then
    mkdir -p "$(dirname "$root/$FAKE_EXTRA_SYMLINK")"
    ln -s /missing-target "$root/$FAKE_EXTRA_SYMLINK"
  fi
  tar -cf "$archive" -C "$root" .
  exit 0
fi

if [ "$1" = "rm" ]; then
  exit 0
fi

echo "unexpected docker invocation: $*" >&2
exit 2
EOF
chmod +x "$fake_docker"

run_runtime_check() {
  env \
    DOCKER="${DOCKER_OVERRIDE:-$fake_docker}" \
    FAKE_DOCKER_ROOT="$workdir/rootfs" \
    IMG=controller:test \
    SERVER_IMG=server:test \
    SUBSCRIBER_IMG=subscriber:test \
    "$@" \
    "$verifier" "$valid_dockerfile"
}

run_runtime_check >/dev/null
echo "  ok   non-root shell-free images pass"
DOCKER_OVERRIDE="$fake_docker --test-wrapper" run_runtime_check >/dev/null
echo "  ok   Docker command arguments are preserved"
expect_failure root-user run_runtime_check FAKE_IMAGE_USER=0:0
expect_failure wrong-entrypoint run_runtime_check FAKE_BAD_ENTRYPOINT=1
expect_failure missing-image run_runtime_check FAKE_MISSING_IMAGE=1
expect_failure missing-payload run_runtime_check FAKE_MISSING_PAYLOAD=1
expect_failure non-executable-payload run_runtime_check FAKE_NON_EXECUTABLE_PAYLOAD=1
expect_failure directory-payload run_runtime_check FAKE_DIRECTORY_PAYLOAD=1
expect_failure symlink-payload run_runtime_check FAKE_SYMLINK_PAYLOAD=1
expect_failure shell-present run_runtime_check FAKE_INCLUDE_SHELL=1
expect_failure package-manager-present run_runtime_check FAKE_INCLUDE_APT=1
expect_failure ash-present run_runtime_check FAKE_EXTRA_PATH=bin/ash
expect_failure microdnf-present run_runtime_check FAKE_EXTRA_PATH=usr/bin/microdnf
expect_failure alternate-shell-path run_runtime_check FAKE_EXTRA_PATH=usr/local/bin/sh
expect_failure alternate-package-manager-path run_runtime_check FAKE_EXTRA_PATH=usr/local/bin/apt-get
run_runtime_check FAKE_EXTRA_DIRECTORY=etc/dpkg >/dev/null
echo "  ok   package metadata directories pass"
expect_failure alternate-shell-symlink run_runtime_check FAKE_EXTRA_SYMLINK=usr/local/bin/sh
expect_failure docker-unavailable env \
  DOCKER="$workdir/missing-docker" \
  IMG=controller:test \
  SERVER_IMG=server:test \
  SUBSCRIBER_IMG=subscriber:test \
  "$verifier" "$valid_dockerfile"

echo "All minimal-image verifier tests passed."
