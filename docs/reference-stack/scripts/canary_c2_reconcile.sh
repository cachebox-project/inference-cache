#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

# Canary for the C2 CacheBackend reconciler. Proves the controller stands up a
# healthy, serving backend from a CR on a GPU-free cluster (kind):
#
#   kubectl apply CacheBackend(profile=cpu) --> controller --> Deployment + Service
#     --> cache-server Deployment becomes Available --> Ready condition True,
#         status.endpoint set
#
# Optionally drives prefix traffic and checks an engine prefix-cache hit — but
# this is opt-in (SKIP_TRAFFIC=0). The traffic block expects a vLLM HTTP surface
# on a port-forward target the script does NOT set up by default (the cache-
# server Service exposes only the LMCache TCP lm:// port). Operators wiring a
# vLLM engine alongside the canary need to also point the port-forward at the
# engine Service before flipping the toggle. Deleting the CR garbage-collects
# the children via owner refs.
#
# This exercises the reconciler end to end against real pods — the gap envtest
# can't cover. The managed standalone server uses CPU storage and does not need
# an inference engine or GPU for this controller lifecycle check.
#
# On-demand canary (NOT a per-PR gate): needs Docker + kind + kubectl, pulls the
# standalone LMCache server image. See docs/reference-stack/VERSIONS.md.
#
# Usage:  docs/reference-stack/scripts/canary_c2_reconcile.sh
# Tunables via env: CACHE_SERVER_IMAGE, MODEL, KIND_CLUSTER, NAMESPACE,
# READY_TIMEOUT, SKIP_TRAFFIC.
set -euo pipefail

CACHE_SERVER_IMAGE="${CACHE_SERVER_IMAGE:-lmcache/standalone:v0.4.7}"
MODEL="${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-c2-canary}"
NAMESPACE="${NAMESPACE:-c2-canary}"
CR_NAME="${CR_NAME:-canary}"
READY_TIMEOUT="${READY_TIMEOUT:-900}" # seconds for the CPU model to load + become Ready
# The traffic block port-forwards `svc/$CR_NAME` to a `:8000` vLLM HTTP
# surface and asserts a prefix-cache hit via the vllm:prefix_cache_hits_total
# metric — a holdover from the retired colocated-rendering profile that
# bundled vLLM into the cache-server pod. The modern split layout exposes
# only the LMCache server on `:65432` (TCP lm://), with no vLLM and no
# HTTP /metrics on the cache-server Service, so the traffic path cannot
# succeed unless the operator wires a separate engine Service AND repoints
# the port-forward below at it. Default the toggle OFF; operators who set up
# both pieces flip SKIP_TRAFFIC=0 to opt back in.
SKIP_TRAFFIC="${SKIP_TRAFFIC:-1}"

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

log "pulling LMCache server image and loading it into the node ($CACHE_SERVER_IMAGE)"
docker pull "$CACHE_SERVER_IMAGE"
"$KIND" load docker-image "$CACHE_SERVER_IMAGE" --name "$KIND_CLUSTER"

# --- controller -------------------------------------------------------------
log "installing CRD"
kubectl "${KUBECONFIG_ARGS[@]}" apply -f config/crd/bases/inferencecache.io_cachebackends.yaml

log "building + starting the controller"
go build -o bin/controller ./cmd/controller

# The controller registers its admission webhooks unconditionally, so the
# manager starts an in-process webhook server that reads its TLS serving
# cert from this directory at startup — mgr.Start() returns an error and the
# whole manager exits if tls.crt/tls.key are absent, before the reconciler
# ever runs. This canary installs no WebhookConfiguration (only the CRD), so
# the apiserver never calls the webhook; the cert only has to exist for the
# server to bind. Mint a throwaway self-signed pair — nothing verifies it.
# The cert dir must match controller-runtime's default webhook CertDir,
# which is os.TempDir()/k8s-webhook-server/serving-certs — i.e. honour
# TMPDIR (unset on the CI runner, so this resolves to /tmp there).
webhook_cert_dir="${TMPDIR:-/tmp}/k8s-webhook-server/serving-certs"
mkdir -p "$webhook_cert_dir"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=c2-canary-webhook" \
  -keyout "$webhook_cert_dir/tls.key" -out "$webhook_cert_dir/tls.crt" >/dev/null 2>&1

./bin/controller --leader-elect=false >/tmp/c2-canary-controller.log 2>&1 &
controller_pid=$!

kubectl "${KUBECONFIG_ARGS[@]}" create namespace "$NAMESPACE"

# --- apply the CacheBackend --------------------------------------------------
log "applying CacheBackend $NAMESPACE/$CR_NAME (image=$CACHE_SERVER_IMAGE)"
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
  runtime: VLLM
  type: LMCache
  deploymentKind: Deployment
  replicas: 1
  observation:
    modelID: $MODEL
  remoteStorage:
    provider: LMCacheServer
    ownership: Managed
    lmCacheServer:
      image: $CACHE_SERVER_IMAGE
EOF

# --- wait for the reconciler to report Ready --------------------------------
log "waiting up to ${READY_TIMEOUT}s for the Ready condition to be True"
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

log "PASS — reconciler stood up a healthy backend, published its endpoint, and cleaned up on delete"
