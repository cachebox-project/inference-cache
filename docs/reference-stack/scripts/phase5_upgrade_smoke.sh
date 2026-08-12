#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

# Installs the last Phase 5 revision, creates only Phase 5 typed PodLocal MP
# objects, then upgrades the CRD and controller to the current checkout. This
# intentionally does not create or migrate legacy IP objects: the recorded
# Phase 0/5 consumer audit found no supported population for them.

set -euo pipefail

PHASE5_COMMIT="${PHASE5_COMMIT:-10178558bfca308ee3a4b0d584efe4ed3b91197d}"
PHASE5_TAG="${PHASE5_TAG:-phase5-upgrade-base}"
TAG="${TAG:-${GITHUB_SHA:-$(git rev-parse HEAD)}}"
REGISTRY="${REGISTRY:-ghcr.io/cachebox-project}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-phase5-upgrade}"
SYSTEM_NAMESPACE="${SYSTEM_NAMESPACE:-inference-cache-system}"
SMOKE_NAMESPACE="${SMOKE_NAMESPACE:-ic-phase5-upgrade}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.1}"
READY_TIMEOUT="${READY_TIMEOUT:-180s}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
LOG_DIR="${LOG_DIR:-/tmp/phase5-upgrade-smoke-logs}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CURRENT_CONTROLLER_IMG="$REGISTRY/inference-cache-controller:$TAG"
CURRENT_SERVER_IMG="$REGISTRY/inference-cache-server:$TAG"
PHASE5_CONTROLLER_IMG="$REGISTRY/inference-cache-controller:$PHASE5_TAG"
PHASE5_SERVER_IMG="$REGISTRY/inference-cache-server:$PHASE5_TAG"
KIND="${KIND:-$REPO_ROOT/bin/kind}"
[ -x "$KIND" ] || KIND=kind

log() { printf '[phase5-upgrade-smoke] %s\n' "$*"; }
fail() { printf '[phase5-upgrade-smoke] ERROR: %s\n' "$*" >&2; exit 1; }

for binary in docker git kubectl tar "$KIND"; do
  command -v "$binary" >/dev/null 2>&1 || fail "missing required tool: $binary"
done
git -C "$REPO_ROOT" cat-file -e "$PHASE5_COMMIT^{commit}" \
  || fail "Phase 5 commit is unavailable: $PHASE5_COMMIT"

mkdir -p "$LOG_DIR"
tmpdir="$(mktemp -d)"
created_cluster=0

collect_diagnostics() {
  kubectl get cachebackends -A -o yaml >"$LOG_DIR/cachebackends.yaml" 2>&1 || true
  kubectl -n "$SYSTEM_NAMESPACE" get all -o wide >"$LOG_DIR/system.txt" 2>&1 || true
  kubectl -n "$SYSTEM_NAMESPACE" logs deployment/inference-cache-controller-manager --all-containers >"$LOG_DIR/controller.log" 2>&1 || true
}

cleanup() {
  rm -rf "$tmpdir"
  if [ "$created_cluster" = "1" ] && [ "$KEEP_CLUSTER" != "1" ]; then
    "$KIND" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
  fi
}

on_exit() {
  rc=$?
  [ "$rc" -eq 0 ] || collect_diagnostics
  cleanup
  exit "$rc"
}
trap on_exit EXIT

render_config() {
  local source_dir="$1" destination="$2" controller_image="$3" server_image="$4"
  cp -R "$source_dir/config" "$destination"
  sed -i.bak \
    -e "/^- name: controller$/,/^- name: server$/ { s|^  newName: .*|  newName: ${controller_image%:*}|; s|^  newTag: .*|  newTag: ${controller_image##*:}|; }" \
    -e "/^- name: server$/,$ { s|^  newName: .*|  newName: ${server_image%:*}|; s|^  newTag: .*|  newTag: ${server_image##*:}|; }" \
    "$destination/default/kustomization.yaml"
  rm -f "$destination/default/kustomization.yaml.bak"
}

if "$KIND" get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  log "reusing kind cluster $KIND_CLUSTER"
else
  "$KIND" create cluster --name "$KIND_CLUSTER" --wait 120s
  created_cluster=1
fi
kubectl config use-context "kind-$KIND_CLUSTER" >/dev/null

log "installing cert-manager $CERT_MANAGER_VERSION"
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml" >/dev/null
kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=180s

phase5_src="$tmpdir/phase5-src"
mkdir -p "$phase5_src"
git -C "$REPO_ROOT" archive "$PHASE5_COMMIT" | tar -x -C "$phase5_src"

log "building and installing Phase 5 at $PHASE5_COMMIT"
make -C "$phase5_src" image-build TAG="$PHASE5_TAG" REGISTRY="$REGISTRY"
"$KIND" load docker-image "$PHASE5_CONTROLLER_IMG" --name "$KIND_CLUSTER"
"$KIND" load docker-image "$PHASE5_SERVER_IMG" --name "$KIND_CLUSTER"
render_config "$phase5_src" "$tmpdir/phase5-config" "$PHASE5_CONTROLLER_IMG" "$PHASE5_SERVER_IMG"
kubectl apply -k "$tmpdir/phase5-config/default" >/dev/null
kubectl -n "$SYSTEM_NAMESPACE" wait --for=condition=Available --timeout="$READY_TIMEOUT" \
  deployment/inference-cache-controller-manager deployment/inference-cache-server

kubectl create namespace "$SMOKE_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
log "creating Phase 5 typed MP objects"
cat <<'EOF' | kubectl -n "$SMOKE_NAMESPACE" apply -f - >/dev/null
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: phase5-host-only
spec:
  runtime: VLLM
  type: LMCache
  engineSelector:
    matchLabels:
      app: phase5-engine
  integration:
    role: ReadWrite
  lmCache:
    topology: PodLocal
    podLocal:
      server:
        image: docker.io/lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13
        port: 5555
        l1Capacity: 1Gi
        maxWorkers: 1
        resources:
          requests: {cpu: "1", memory: 2Gi}
          limits: {memory: 2Gi}
---
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: phase5-managed-redis
spec:
  runtime: SGLang
  type: LMCache
  engineSelector:
    matchLabels:
      app: phase5-sglang
  integration:
    role: ReadWrite
  lmCache:
    topology: PodLocal
    podLocal:
      server:
        image: docker.io/lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13
        port: 5555
        l1Capacity: 1Gi
        maxWorkers: 1
        resources:
          requests: {cpu: "1", memory: 2Gi}
          limits: {memory: 2Gi}
  remoteStorage:
    provider: Redis
    ownership: Managed
    redis: {}
EOF

host_uid="$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend phase5-host-only -o jsonpath='{.metadata.uid}')"
redis_uid="$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend phase5-managed-redis -o jsonpath='{.metadata.uid}')"
[ -n "$host_uid" ] && [ -n "$redis_uid" ] || fail "Phase 5 objects were not persisted"

log "upgrading CRDs and workloads to the current Phase 7 checkout"
make -C "$REPO_ROOT" image-build TAG="$TAG" REGISTRY="$REGISTRY"
"$KIND" load docker-image "$CURRENT_CONTROLLER_IMG" --name "$KIND_CLUSTER"
"$KIND" load docker-image "$CURRENT_SERVER_IMG" --name "$KIND_CLUSTER"
render_config "$REPO_ROOT" "$tmpdir/current-config" "$CURRENT_CONTROLLER_IMG" "$CURRENT_SERVER_IMG"
kubectl apply -k "$tmpdir/current-config/default" >/dev/null
kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment/inference-cache-controller-manager --timeout="$READY_TIMEOUT"
kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment/inference-cache-server --timeout="$READY_TIMEOUT"

[ "$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend phase5-host-only -o jsonpath='{.metadata.uid}')" = "$host_uid" ] \
  || fail "host-only Phase 5 object was replaced during upgrade"
[ "$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend phase5-managed-redis -o jsonpath='{.metadata.uid}')" = "$redis_uid" ] \
  || fail "managed-Redis Phase 5 object was replaced during upgrade"
for backend in phase5-host-only phase5-managed-redis; do
  [ "$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend "$backend" -o jsonpath='{.spec.lmCache.topology}')" = "PodLocal" ] \
    || fail "$backend lost its typed MP topology"
done

for _ in $(seq 1 60); do
  endpoint="$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend phase5-managed-redis -o jsonpath='{.status.remoteStorage.endpoint}' 2>/dev/null || true)"
  [ "$endpoint" = "phase5-managed-redis.$SMOKE_NAMESPACE.svc.cluster.local:6379" ] && break
  sleep 1
done
[ "${endpoint:-}" = "phase5-managed-redis.$SMOKE_NAMESPACE.svc.cluster.local:6379" ] \
  || fail "managed Redis did not reconcile after upgrade"

pod_json="$tmpdir/upgraded-admission.json"
cat <<'EOF' | kubectl -n "$SMOKE_NAMESPACE" apply --dry-run=server -o json -f - >"$pod_json"
apiVersion: v1
kind: Pod
metadata:
  name: phase5-engine
  labels:
    app: phase5-engine
spec:
  containers:
    - name: vllm
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
EOF
grep -Fq 'lmcache-mp-server' "$pod_json" || fail "upgraded admission did not inject the MP server"
grep -Fq 'LMCacheMPConnector' "$pod_json" || fail "upgraded admission did not inject the MP connector"

log "PASS: Phase 5 typed objects persisted and reconciled through the Phase 7 upgrade"
