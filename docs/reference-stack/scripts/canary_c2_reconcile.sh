#!/usr/bin/env bash
# CPU canary for the C2 CacheBackend reconciler. Proves the controller stands up a
# healthy, serving backend from a CR on a GPU-free cluster (kind):
#
#   kubectl apply CacheBackend(profile=cpu) --> controller --> Deployment + Service
#     --> CPU vLLM pods become Ready --> Ready condition True, status.endpoint set
#
# Optionally drives prefix traffic through the Service and checks an engine
# prefix-cache hit. Deleting the CR garbage-collects the children via owner refs.
#
# This exercises the reconciler end to end against real pods — the gap envtest
# can't cover. It uses the CPU profile (no GPU, no LMCache offload); real LMCache
# offload needs a GPU (default profile).
#
# On-demand canary (NOT a per-PR gate): needs Docker + kind + kubectl, pulls the
# multi-GB vLLM CPU image, and a Docker VM with ~10+ GiB RAM (CPU runtime baseline
# ~5 GiB + KV cache). See docs/reference-stack/VERSIONS.md.
#
# Usage:  docs/reference-stack/scripts/canary_c2_reconcile.sh
# Tunables via env: IMAGE, MODEL, KIND_CLUSTER, NAMESPACE, READY_TIMEOUT, SKIP_TRAFFIC.
set -euo pipefail

arch="$(uname -m)"
case "$arch" in
  arm64 | aarch64) IMAGE_TAG="${IMAGE_TAG:-latest-arm64}" ;;
  *) IMAGE_TAG="${IMAGE_TAG:-latest-x86_64}" ;;
esac
IMAGE="${IMAGE:-vllm/vllm-openai-cpu:$IMAGE_TAG}"
MODEL="${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-c2-canary}"
NAMESPACE="${NAMESPACE:-c2-canary}"
CR_NAME="${CR_NAME:-canary}"
READY_TIMEOUT="${READY_TIMEOUT:-900}" # seconds for the CPU model to load + become Ready
SKIP_TRAFFIC="${SKIP_TRAFFIC:-0}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

KIND="${KIND:-$([ -x ./bin/kind ] && echo ./bin/kind || echo kind)}"
controller_pid=""
pf_pid=""
log() { echo "[c2-canary] $*"; }
fail() {
  echo "[c2-canary] FAIL: $*" >&2
  exit 1
}

cleanup() {
  [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true
  [ -n "$controller_pid" ] && kill "$controller_pid" 2>/dev/null || true
  "$KIND" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- cluster ----------------------------------------------------------------
log "creating kind cluster $KIND_CLUSTER"
"$KIND" create cluster --name "$KIND_CLUSTER" --wait 120s
KUBECONFIG_ARGS=(--context "kind-$KIND_CLUSTER")

log "pulling CPU image and loading it into the node ($IMAGE)"
docker pull "$IMAGE"
"$KIND" load docker-image "$IMAGE" --name "$KIND_CLUSTER"

# --- controller -------------------------------------------------------------
log "installing CRD"
kubectl "${KUBECONFIG_ARGS[@]}" apply -f config/crd/bases/inferencecache.io_cachebackends.yaml

log "building + starting the controller"
go build -o bin/controller ./cmd/controller
./bin/controller --leader-elect=false >/tmp/c2-canary-controller.log 2>&1 &
controller_pid=$!

kubectl "${KUBECONFIG_ARGS[@]}" create namespace "$NAMESPACE"

# --- apply the CacheBackend (CPU profile) -----------------------------------
log "applying CacheBackend $NAMESPACE/$CR_NAME (profile=cpu, image=$IMAGE)"
kubectl "${KUBECONFIG_ARGS[@]}" apply -f - <<EOF
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: $CR_NAME
  namespace: $NAMESPACE
  annotations:
    # Opt OUT of the KV-event readiness gate: this canary exercises the C2
    # reconciler's Deployment-rollout path and the operator-managed cache
    # server has no engine pods wired in, so the gate would deadlock on the
    # default-on AwaitingFirstKVEvent state. A separate canary covers the gate.
    inferencecache.io/require-kv-events: "false"
spec:
  type: LMCache
  deploymentKind: Deployment
  replicas: 1
  backendConfig:
    profile: cpu
    image: $IMAGE
    model: $MODEL
EOF

# --- wait for the reconciler to report Ready --------------------------------
log "waiting up to ${READY_TIMEOUT}s for the Ready condition to be True (CPU model load is slow)"
deadline=$(($(date +%s) + READY_TIMEOUT))
ready=""
until [ "$ready" = "True" ]; do
  ready="$(kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" get cachebackend "$CR_NAME" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" get pods -o wide || true
    kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" describe deployment "$CR_NAME" || true
    fail "backend did not become Ready within ${READY_TIMEOUT}s (last Ready status='$ready')"
  fi
  sleep 5
done
log "Ready=True"

endpoint="$(kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" get cachebackend "$CR_NAME" -o jsonpath='{.status.endpoint}')"
[ -n "$endpoint" ] || fail "status.endpoint was not published"
log "status.endpoint=$endpoint"

avail="$(kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" get deployment "$CR_NAME" -o jsonpath='{.status.availableReplicas}')"
[ "${avail:-0}" -ge 1 ] || fail "deployment has no available replicas"

# --- optional: drive prefix traffic + check a cache hit ---------------------
if [ "$SKIP_TRAFFIC" != "1" ]; then
  log "port-forwarding the Service to drive prefix traffic"
  kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" port-forward "svc/$CR_NAME" 18000:8000 >/tmp/c2-canary-pf.log 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 30); do
    curl -sf -o /dev/null "http://localhost:18000/health" && break
    sleep 1
  done
  hits() { curl -s "http://localhost:18000/metrics" | awk '/^vllm:prefix_cache_hits_total/{s+=$2} END{print s+0}'; }
  PREFIX="$(python3 -c 'print(("You are a meticulous canary assistant. Follow the rules precisely. " * 200).strip())')"
  fire() {
    curl -s -o /dev/null -w '%{http_code}' "http://localhost:18000/v1/chat/completions" \
      -H 'Content-Type: application/json' \
      -d "$(python3 -c 'import json,sys;print(json.dumps({"model":sys.argv[3],"max_tokens":8,"temperature":0,"messages":[{"role":"system","content":sys.argv[1]},{"role":"user","content":sys.argv[2]}]}))' "$PREFIX" "$1" "$MODEL")"
  }
  h0=$(hits)
  log "request 1 (cold prefix): HTTP $(fire 'summarize in one word')"
  log "request 2 (same prefix):  HTTP $(fire 'summarize in two words')"
  h1=$(hits)
  log "prefix_cache_hits: $h0 -> $h1"
  [ "$h1" -gt "$h0" ] || fail "no engine prefix-cache hit (hits did not increase)"
fi

# --- delete the CR -> owner-ref GC ------------------------------------------
log "deleting the CR; expecting owner-ref GC of the Deployment + Service"
kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" delete cachebackend "$CR_NAME" --wait=true
gc_deadline=$(($(date +%s) + 60))
until [ "$(kubectl "${KUBECONFIG_ARGS[@]}" -n "$NAMESPACE" get deploy,svc -o name 2>/dev/null | wc -l | tr -d ' ')" = "0" ]; do
  [ "$(date +%s)" -lt "$gc_deadline" ] || fail "children were not garbage-collected after CR deletion"
  sleep 2
done

log "PASS — reconciler stood up a healthy CPU backend, published its endpoint, and cleaned up on delete"
