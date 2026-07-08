#!/usr/bin/env bash
# CPU canary for the C6 mutating Pod webhook + cross-pod cache reuse.
#
# Proves the engine-wiring webhook injects the LMCache connector env onto
# user-supplied vLLM pods at admission time, and (when not skipped) that two
# such pods share KV state via a single managed lmcache-server:
#
#   apply CacheBackend(type=LMCache, engineSelector={app: vllm-engine})
#     -> reconciler stands up lmcache-server (Ready)
#     -> apply Pod A (label app=vllm-engine)  -- webhook injects LMCACHE_*
#     -> apply Pod B (label app=vllm-engine)  -- webhook injects LMCACHE_*
#
# Then (SKIP_TRAFFIC != 1) sends the same long-prefix prompt to Pod A then
# Pod B, and confirms Pod B reports a vllm:prefix_cache_hits increment that
# Pod A did not produce from its own cold start -- the shared lmcache-server
# made the prefix available to Pod B without re-prefill.
#
# Heavy: two CPU vLLM pods + the lmcache-server + cert-manager + the
# controller image all run on one kind node. The vLLM CPU runtime baseline
# is ~5 GiB per pod, so the Docker VM needs ~12 GiB RAM (see
# docs/reference-stack/VERSIONS.md for the documented memory floor).
# Pulls the multi-GB vLLM image. This is NOT a per-PR gate; it runs on a
# schedule and on manual dispatch.
#
# Usage:    docs/reference-stack/scripts/canary_c6_engine_wiring.sh
# Tunables: IMAGE, MODEL, KIND_CLUSTER, NAMESPACE, READY_TIMEOUT,
#           SKIP_TRAFFIC, CERT_MANAGER_VERSION.

set -euo pipefail

arch="$(uname -m)"
case "$arch" in
  arm64 | aarch64) IMAGE_TAG="${IMAGE_TAG:-latest-arm64}" ;;
  *) IMAGE_TAG="${IMAGE_TAG:-latest-x86_64}" ;;
esac
IMAGE="${IMAGE:-vllm/vllm-openai-cpu:$IMAGE_TAG}"
MODEL="${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-c6-canary}"
NAMESPACE="${NAMESPACE:-c6-canary}"
CR_NAME="${CR_NAME:-canary-lmcache}"
READY_TIMEOUT="${READY_TIMEOUT:-900}"
SKIP_TRAFFIC="${SKIP_TRAFFIC:-0}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.1}"
CONTROLLER_IMG="${CONTROLLER_IMG:-localhost/inference-cache-controller:canary}"
SERVER_IMG="${SERVER_IMG:-localhost/inference-cache-server:canary}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

KIND="${KIND:-$([ -x ./bin/kind ] && echo ./bin/kind || echo kind)}"
pf_pids=()
log() { echo "[c6-canary] $*"; }
fail() {
  echo "[c6-canary] FAIL: $*" >&2
  exit 1
}

cleanup() {
  for pid in "${pf_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  "$KIND" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- cluster ----------------------------------------------------------------
log "creating kind cluster $KIND_CLUSTER"
"$KIND" create cluster --name "$KIND_CLUSTER" --wait 120s
KCTX=(--context "kind-$KIND_CLUSTER")

log "installing cert-manager $CERT_MANAGER_VERSION (webhook serving cert)"
kubectl "${KCTX[@]}" apply -f \
  "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml"
kubectl "${KCTX[@]}" -n cert-manager wait --for=condition=Available deployment --all --timeout=180s

# --- controller + server images + install ----------------------------------
log "building controller image $CONTROLLER_IMG"
docker build -f dockerfiles/Dockerfile --target controller -t "$CONTROLLER_IMG" .
"$KIND" load docker-image "$CONTROLLER_IMG" --name "$KIND_CLUSTER"

log "building server image $SERVER_IMG"
docker build -f dockerfiles/Dockerfile --target server -t "$SERVER_IMG" .
"$KIND" load docker-image "$SERVER_IMG" --name "$KIND_CLUSTER"

log "rendering + applying config/default (controller + server + webhook + cert-manager wiring)"
# Point both the controller and server Deployments at our canary images.
# The default overlay now ships an inference-cache-server Deployment too;
# without rewriting its image the canary would resolve to the published
# :dev tag, not the image built from this commit. Prefer `kustomize edit`
# when available; otherwise fall back to a sed that scopes the rewrite to
# each `- name: ...` block (both blocks share `newTag: dev`, so an
# unscoped substitute would collapse them onto the same value).
tmpdir="$(mktemp -d)"
trap "rm -rf $tmpdir; cleanup" EXIT
cp -r config "$tmpdir/config"
(
  cd "$tmpdir/config/default"
  if command -v kustomize >/dev/null 2>&1; then
    kustomize edit set image controller="$CONTROLLER_IMG" server="$SERVER_IMG"
  else
    # Split on the LAST `:` so refs that include a registry port
    # (e.g. localhost:5001/inference-cache-server:canary) keep their
    # full registry/repo path. `${X%:*}` strips the shortest suffix
    # from the last `:`, leaving everything before the tag.
    sed -i.bak \
      -e "/^- name: controller$/,/^- name: server$/ {
            s|^  newName: .*|  newName: ${CONTROLLER_IMG%:*}|
            s|^  newTag: .*|  newTag: ${CONTROLLER_IMG##*:}|
          }" \
      -e "/^- name: server$/,\$ {
            s|^  newName: .*|  newName: ${SERVER_IMG%:*}|
            s|^  newTag: .*|  newTag: ${SERVER_IMG##*:}|
          }" \
      kustomization.yaml
  fi
)
kubectl "${KCTX[@]}" apply -k "$tmpdir/config/default"
kubectl "${KCTX[@]}" -n inference-cache-system wait --for=condition=Available deployment --all --timeout=180s

# --- pre-existing namespace ------------------------------------------------
kubectl "${KCTX[@]}" create namespace "$NAMESPACE"

# --- apply the CacheBackend ------------------------------------------------
log "applying CacheBackend $NAMESPACE/$CR_NAME (engineSelector matches app=vllm-engine)"
kubectl "${KCTX[@]}" apply -f - <<EOF
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: $CR_NAME
  namespace: $NAMESPACE
  annotations:
    # Opt OUT of the KV-event readiness gate: this canary applies the CR and
    # waits for the cache-server to be Ready BEFORE creating the engine pods
    # that would produce KV events. With the default-on gate, that ordering
    # would deadlock at AwaitingFirstKVEvent. The gate is exercised by a
    # dedicated canary; here Ready is workload-availability driven.
    inferencecache.io/require-kv-events: "false"
spec:
  type: LMCache
  deploymentKind: Deployment
  replicas: 1
  integration:
    engine: vllm
    role: ReadWrite
  engineSelector:
    matchLabels:
      app: vllm-engine
EOF

log "waiting up to ${READY_TIMEOUT}s for the lmcache-server CacheBackend to reach Ready"
deadline=$(($(date +%s) + READY_TIMEOUT))
until [ "$(kubectl "${KCTX[@]}" -n "$NAMESPACE" get cachebackend "$CR_NAME" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = "True" ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl "${KCTX[@]}" -n "$NAMESPACE" get pods -o wide || true
    kubectl "${KCTX[@]}" -n "$NAMESPACE" describe cachebackend "$CR_NAME" || true
    fail "lmcache-server did not become Ready within ${READY_TIMEOUT}s"
  fi
  sleep 5
done
endpoint="$(kubectl "${KCTX[@]}" -n "$NAMESPACE" get cachebackend "$CR_NAME" -o jsonpath='{.status.endpoint}')"
log "CacheBackend Ready; endpoint=$endpoint"

# --- two engine pods, labels matching the EngineSelector -------------------
log "loading vLLM CPU image into the kind node ($IMAGE)"
docker pull "$IMAGE"
"$KIND" load docker-image "$IMAGE" --name "$KIND_CLUSTER"

apply_engine_pod() {
  local name="$1"
  kubectl "${KCTX[@]}" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: $NAMESPACE
  labels:
    app: vllm-engine
spec:
  restartPolicy: Never
  containers:
  - name: vllm
    image: $IMAGE
    imagePullPolicy: IfNotPresent
    args:
    - --model
    - $MODEL
    - --enable-prefix-caching
    - --max-model-len
    - "2048"
    env:
    - name: VLLM_CPU_KVCACHE_SPACE
      value: "2"
    ports:
    - containerPort: 8000
      name: http
    # Gate pod-Ready on vLLM actually serving /health. Without this a
    # probe-less pod reports Ready the instant the container starts, so the
    # downstream "wait for Ready" returns in ~1s and the short port-forward
    # health loop then expires long before the CPU model finishes loading
    # (~3-4 min). Tying Ready to /health makes READY_TIMEOUT the real budget.
    readinessProbe:
      httpGet:
        path: /health
        port: http
      initialDelaySeconds: 10
      periodSeconds: 5
      timeoutSeconds: 3
      failureThreshold: 180
EOF
}

log "applying engine pods A and B (webhook should inject LMCache wiring at admission)"
apply_engine_pod "engine-a"
apply_engine_pod "engine-b"

# Assert the webhook actually injected by re-reading the pod spec.
verify_webhook_inject() {
  local name="$1"
  local remote_url
  remote_url="$(kubectl "${KCTX[@]}" -n "$NAMESPACE" get pod "$name" -o jsonpath='{.spec.containers[?(@.name=="vllm")].env[?(@.name=="LMCACHE_REMOTE_URL")].value}')"
  if [ -z "$remote_url" ]; then
    kubectl "${KCTX[@]}" -n "$NAMESPACE" get pod "$name" -o yaml >&2 || true
    fail "webhook did not inject LMCACHE_REMOTE_URL onto $name"
  fi
  log "verified $name carries LMCACHE_REMOTE_URL=$remote_url"
}
verify_webhook_inject "engine-a"
verify_webhook_inject "engine-b"

if [ "$SKIP_TRAFFIC" = "1" ]; then
  log "SKIP_TRAFFIC=1 -> skipping traffic + cache-hit assertion"
  log "PASS - webhook injected wiring on both engine pods (traffic step skipped)"
  exit 0
fi

# Wait for both engine pods to be Ready (heavy CPU model load).
for name in engine-a engine-b; do
  log "waiting up to ${READY_TIMEOUT}s for $name to become Ready"
  kubectl "${KCTX[@]}" -n "$NAMESPACE" wait --for=condition=Ready --timeout="${READY_TIMEOUT}s" "pod/$name" \
    || fail "$name did not become Ready"
done

# Port-forward each engine's HTTP port to a distinct local port so we can
# hit them independently and read their /metrics.
forward_pod() {
  local name="$1" local_port="$2"
  kubectl "${KCTX[@]}" -n "$NAMESPACE" port-forward "pod/$name" "$local_port:8000" \
    >"/tmp/c6-canary-pf-$name.log" 2>&1 &
  pf_pids+=($!)
  for _ in $(seq 1 30); do
    curl -sf -o /dev/null "http://localhost:$local_port/health" && return 0
    sleep 1
  done
  fail "engine $name port-forward never became healthy"
}
forward_pod engine-a 18001
forward_pod engine-b 18002

hits() {
  local port="$1"
  curl -s "http://localhost:$port/metrics" | awk '/^vllm:prefix_cache_hits_total/{s+=$2} END{print s+0}'
}

PREFIX="$(python3 -c 'print(("You are a meticulous canary assistant. Follow the rules precisely. " * 200).strip())')"
fire() {
  local port="$1" q="$2"
  curl -s -o /dev/null -w '%{http_code}' "http://localhost:$port/v1/chat/completions" \
    -H 'Content-Type: application/json' \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"model":sys.argv[3],"max_tokens":8,"temperature":0,"messages":[{"role":"system","content":sys.argv[1]},{"role":"user","content":sys.argv[2]}]}))' "$PREFIX" "$q" "$MODEL")"
}

a_h0=$(hits 18001); b_h0=$(hits 18002)
log "cold counters: engine-a vllm:prefix_cache_hits=$a_h0 ; engine-b=$b_h0"

log "request 1 to engine-a (cold; should produce no prefix-cache hit on A)"
log "  HTTP $(fire 18001 'summarize in one word')"
a_h1=$(hits 18001)
log "engine-a hits delta after self-cold prompt: $((a_h1 - a_h0))"

log "request 2 to engine-b (same prefix; SHOULD hit via the shared lmcache-server)"
log "  HTTP $(fire 18002 'summarize in two words')"
b_h1=$(hits 18002)
log "engine-b hits delta after cross-pod prompt: $((b_h1 - b_h0))"

# Pod A's first request was cold so it should have produced no hit on A.
# Pod B's first request, with the same prefix, must hit via the shared
# lmcache-server -- otherwise the webhook did not actually wire the engines
# into the same backend (which is the contract this canary asserts).
if [ "$((b_h1 - b_h0))" -le 0 ]; then
  log "engine-a metrics tail:"; curl -s http://localhost:18001/metrics | grep -E '^vllm:prefix_cache_' || true
  log "engine-b metrics tail:"; curl -s http://localhost:18002/metrics | grep -E '^vllm:prefix_cache_' || true
  fail "engine-b reported no prefix-cache hit increment for a prompt prefix that engine-a had populated via the shared lmcache-server"
fi

log "PASS - webhook auto-wired both engines and the shared lmcache-server delivered a cross-pod prefix-cache hit"
