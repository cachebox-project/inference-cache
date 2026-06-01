#!/usr/bin/env bash
# Per-PR install smoke for `kubectl apply -k config/default`.
#
# Builds controller + server images at a deterministic tag, loads them into a
# kind cluster, installs cert-manager (same pinned version the C6 engine-wiring
# canary uses), renders config/default with the SHA-tagged images, applies it,
# and asserts the install actually came up:
#
#   1. inference-cache-controller-manager + inference-cache-server reach
#      condition=Available within 120s.
#   2. The CacheIndex poller is writing status: `cacheindex/cluster-default`
#      has a non-empty `.status.observedServer` within ~60s (one or two poll
#      cycles past the 30s default refresh).
#   3. The gRPC surface is reachable: a `LookupRoute` for an unknown model
#      returns the fail-open default (`reason_code: NO_HINT`).
#   4. The CacheBackend ↔ engine-pod binding surfaces operators rely on
#      actually wire up end-to-end: applying config/samples/cachebackend-
#      with-engine.yaml drives status.matchedEnginePods=1, stamps the
#      injected-by annotation on the engine pod, and surfaces the
#      InjectedByCacheBackend Event (with the persisted pod UID — the
#      regression that hides events from `kubectl describe pod`). Then
#      scaling the engine to 0 drives status.matchedEnginePods=0 via the
#      reconciler's self-RequeueAfter cadence (no CR or owned-workload
#      event needed) within ~30s, the bound on stale-Matched the
#      cadence guarantees.
#   5. The External CacheBackend type end-to-end: applying a `type: External`
#      CR renders NO Deployment/Service in its namespace, status.endpoint
#      mirrors spec.endpoint, the CR goes Ready=True/ExternalEndpointAccepted,
#      and a matching engine pod is admitted with `LMCACHE_REMOTE_URL=lm://
#      <spec.endpoint>` injected by the pod-mutating webhook. Also exercises
#      the new admission validation rules (non-External + endpoint and
#      External + bad scheme are both rejected at write time).
#   6. The /snapshot endpoint rejects unauthenticated callers: a side curl
#      pod outside the controller's SA identity gets either an HTTP 401 (L7
#      auth middleware) or a curl timeout (L3/L4 NetworkPolicy drop).
#
# Distinct from the C2/C6 canaries: those exercise real engine pods + cross-pod
# cache reuse (multi-GB image, ~10+ GiB RAM, schedule-only). This smoke stops
# at "the default install bundle wires together; gRPC fail-open works; the
# CacheBackend ↔ engine-pod binding surfaces operators rely on actually
# wire up end-to-end" -- light enough to run on every PR. The paired-sample
# phase swaps the engine container's image to busybox before pod CREATE so
# the smoke does not pay the multi-GB vLLM pull; the signals it asserts
# (status, annotation, Event) all materialize from pod CREATE, not from the
# engine actually running.
#
# Designed to catch the class of install regression that surfaced when the
# default overlay was missing a Namespace resource: `kubectl apply -k` silently
# fails namespace-scoped creates on a clean cluster, and the heavier canaries'
# `wait --for=condition=Available` mask it.
#
# Prereqs (fresh kind cluster + this repo, nothing else):
#   - docker          (for `make image-build` and `kind load docker-image`)
#   - kind            (./bin/kind picked up if present, else `kind` on PATH)
#   - kubectl
#   - grpcurl         (probes the gRPC surface)
#   - kustomize       (optional; sed fallback handles the image rewrite if absent)
#
# Usage:    docs/reference-stack/scripts/default_install_smoke.sh
# Tunables: TAG, KIND_CLUSTER, NAMESPACE, CERT_MANAGER_VERSION, READY_TIMEOUT,
#           CACHEINDEX_TIMEOUT, GRPC_LOCAL_PORT, LOG_DIR, SAMPLE_NS,
#           SAMPLE_ENDPOINT_TIMEOUT, SAMPLE_MATCH_TIMEOUT, SAMPLE_DRIFT_TIMEOUT,
#           SAMPLE_ENGINE_IMAGE, EXTERNAL_BACKEND_TIMEOUT,
#           EXTERNAL_INJECT_TIMEOUT.

set -euo pipefail

TAG="${TAG:-${GITHUB_SHA:-$(git rev-parse HEAD)}}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-install-smoke}"
NAMESPACE="${NAMESPACE:-inference-cache-system}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.1}"
READY_TIMEOUT="${READY_TIMEOUT:-120s}"
CACHEINDEX_TIMEOUT="${CACHEINDEX_TIMEOUT:-90}" # seconds; ~3x the 30s refresh, absorbs leader-election + first-tick jitter
GRPC_LOCAL_PORT="${GRPC_LOCAL_PORT:-19090}"
LOG_DIR="${LOG_DIR:-/tmp/install-smoke-logs}"
# External-backend gate timeouts. The reconciler patches status on the next
# reconcile (sub-second on a fresh CR), and the pod webhook resolves the
# endpoint synchronously at admission, so these are short. The values give
# headroom for the initial APIReader cache warm-up and the leader-election
# lease the External-reconcile path inherits from the C2 reconciler loop.
EXTERNAL_BACKEND_TIMEOUT="${EXTERNAL_BACKEND_TIMEOUT:-30}" # seconds
EXTERNAL_INJECT_TIMEOUT="${EXTERNAL_INJECT_TIMEOUT:-30}"  # seconds

# Sample-smoke tunables — apply config/samples/cachebackend-with-engine.yaml,
# assert the operator-facing signals, exercise the RequeueAfter drift case.
#
# Default namespace is dedicated to this smoke so re-runs against an existing
# cluster (KEEP_CLUSTER=1) don't mutate or delete a developer's own resources
# in `default`. The script creates the namespace on entry and deletes it on
# the way out.
SAMPLE_NS="${SAMPLE_NS:-cb-engine-smoke}"
# CacheBackend reconciler publishes status.endpoint once the managed
# lmcache-server Service is created — typically within ~5s. 60s absorbs
# cold-start jitter.
SAMPLE_ENDPOINT_TIMEOUT="${SAMPLE_ENDPOINT_TIMEOUT:-60}"
# Reconciler runs initial CacheBackend reconcile + first Matched refresh
# within a few seconds of CB Create. 60s absorbs cold-start jitter.
SAMPLE_MATCH_TIMEOUT="${SAMPLE_MATCH_TIMEOUT:-60}"
# Drift case waits for the 30s self-RequeueAfter cadence to fire after the
# engine pod is gone. 75s = one full cadence + buffer for the patch + pod-
# terminate round-trip.
SAMPLE_DRIFT_TIMEOUT="${SAMPLE_DRIFT_TIMEOUT:-75}"
# Tiny stand-in for the vLLM image. The webhook injects on pod CREATE; the
# engine doesn't need to run for the operator-facing signals (Matched,
# annotation, Event) to materialize. Avoids a multi-GB pull in CI.
SAMPLE_ENGINE_IMAGE="${SAMPLE_ENGINE_IMAGE:-busybox:1.36}"

# Image refs match the Makefile's REGISTRY/repo defaults so `kustomize edit set
# image` (or the sed fallback) rewrites the in-tree controller=/server= entries
# without changing their registry/repo paths.
REGISTRY="${REGISTRY:-ghcr.io/cachebox-project}"
CONTROLLER_IMG="$REGISTRY/inference-cache-controller:$TAG"
SERVER_IMG="$REGISTRY/inference-cache-server:$TAG"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

# External-backend smoke fixture identifiers. Declared up front so the
# diagnostics helper can reference them even if the smoke aborts before
# the External section creates the objects.
EXT_SMOKE_NS="${EXT_SMOKE_NS:-ic-smoke-external}"
EXT_SMOKE_CB_NAME="${EXT_SMOKE_CB_NAME:-smoke-external}"
EXT_SMOKE_POD_NAME="${EXT_SMOKE_POD_NAME:-smoke-engine}"
EXT_SMOKE_LABEL="${EXT_SMOKE_LABEL:-app=ic-smoke-engine}"
EXT_SMOKE_ENDPOINT="${EXT_SMOKE_ENDPOINT:-smoke-cache.example.com:8200}"

KIND="${KIND:-$([ -x ./bin/kind ] && echo ./bin/kind || echo kind)}"
pf_pid=""
tmpdir=""

log() { echo "[install-smoke] $*"; }
fail() {
  echo "[install-smoke] FAIL: $*" >&2
  collect_diagnostics || true
  exit 1
}

collect_diagnostics() {
  mkdir -p "$LOG_DIR"
  log "collecting diagnostics into $LOG_DIR"
  kubectl get pods -A -o wide >"$LOG_DIR/pods-all.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" describe deployment/inference-cache-controller-manager \
    >"$LOG_DIR/describe-controller.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" describe deployment/inference-cache-server \
    >"$LOG_DIR/describe-server.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" logs deployment/inference-cache-controller-manager --all-containers --tail=-1 \
    >"$LOG_DIR/logs-controller.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" logs deployment/inference-cache-server --all-containers --tail=-1 \
    >"$LOG_DIR/logs-server.txt" 2>&1 || true
  kubectl get cacheindex cluster-default -o yaml \
    >"$LOG_DIR/cacheindex.yaml" 2>&1 || true
  kubectl -n cert-manager get pods -o wide \
    >"$LOG_DIR/cert-manager-pods.txt" 2>&1 || true
  # Paired-sample state — only populated if the sample-smoke phase ran;
  # safe (|| true) if it didn't.
  kubectl -n "$SAMPLE_NS" get cb -o yaml \
    >"$LOG_DIR/sample-cb.yaml" 2>&1 || true
  kubectl -n "$SAMPLE_NS" get pod -l app=qwen-demo -o yaml \
    >"$LOG_DIR/sample-pods.yaml" 2>&1 || true
  kubectl -n "$SAMPLE_NS" get events.events.k8s.io -o yaml \
    >"$LOG_DIR/sample-events.yaml" 2>&1 || true
  # External-backend smoke artefacts. Best-effort — the CR/pod may not
  # exist if the smoke aborted before that section.
  kubectl get cb -A -o wide \
    >"$LOG_DIR/cachebackends.txt" 2>&1 || true
  kubectl get cb -n "$EXT_SMOKE_NS" "$EXT_SMOKE_CB_NAME" -o yaml \
    >"$LOG_DIR/external-cb.yaml" 2>&1 || true
  kubectl get pod -n "$EXT_SMOKE_NS" "$EXT_SMOKE_POD_NAME" -o yaml \
    >"$LOG_DIR/external-engine-pod.yaml" 2>&1 || true
  kubectl get deploy,svc -n "$EXT_SMOKE_NS" \
    >"$LOG_DIR/external-ns-workloads.txt" 2>&1 || true
}

cleanup() {
  [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true
  [ -n "$tmpdir" ] && rm -rf "$tmpdir"
  # Only tear the cluster down if we created it (lets local devs pre-create a
  # cluster and re-run the smoke without paying the create cost each time).
  if [ "${KEEP_CLUSTER:-0}" != "1" ] && [ "${CREATED_CLUSTER:-0}" = "1" ]; then
    "$KIND" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
  fi
}

# Catch ANY non-zero exit, not just the ones routed through fail(), and dump
# diagnostics BEFORE cleanup deletes the cluster. Without this, a `set -e`
# abort from an unwrapped command (kubectl apply -k, make image-build, kind
# load, the cert-manager apply) tears the cluster down with no artifact left
# behind -- which is exactly the case (e.g. a missing Namespace resource in
# config/default) this gate is meant to surface.
on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    collect_diagnostics || true
  fi
  cleanup
}
trap on_exit EXIT

# --- prereq checks ----------------------------------------------------------
for bin in docker kubectl grpcurl "$KIND"; do
  command -v "$bin" >/dev/null 2>&1 || fail "missing required tool on PATH: $bin"
done

# --- cluster ----------------------------------------------------------------
if "$KIND" get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  log "reusing existing kind cluster $KIND_CLUSTER"
  CREATED_CLUSTER=0
else
  log "creating kind cluster $KIND_CLUSTER"
  "$KIND" create cluster --name "$KIND_CLUSTER" --wait 120s
  CREATED_CLUSTER=1
fi
kubectl config use-context "kind-$KIND_CLUSTER" >/dev/null

# --- build + load images ----------------------------------------------------
log "building controller + server images at TAG=$TAG"
make image-build TAG="$TAG" REGISTRY="$REGISTRY"

log "loading $CONTROLLER_IMG into the kind node"
"$KIND" load docker-image "$CONTROLLER_IMG" --name "$KIND_CLUSTER"
log "loading $SERVER_IMG into the kind node"
"$KIND" load docker-image "$SERVER_IMG" --name "$KIND_CLUSTER"

# --- cert-manager -----------------------------------------------------------
log "installing cert-manager $CERT_MANAGER_VERSION"
kubectl apply -f \
  "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml"
kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=180s

# --- render config/default with SHA-tagged images --------------------------
# Don't mutate the tracked kustomization.yaml -- copy the whole config tree into
# a tmpdir and edit there. Prefer `kustomize edit set image` when the binary is
# on PATH; sed fallback (scoped to each `- name:` block) keeps the script
# self-contained on a fresh laptop without the kustomize CLI installed.
tmpdir="$(mktemp -d)"
cp -r config "$tmpdir/config"
(
  cd "$tmpdir/config/default"
  if command -v kustomize >/dev/null 2>&1; then
    kustomize edit set image \
      "controller=$CONTROLLER_IMG" \
      "server=$SERVER_IMG"
  else
    # Each `- name: …` block is followed by `newName:` + `newTag:`. Split on
    # the LAST `:` so registry-with-port refs (host:port/repo:tag) keep their
    # registry path; `${X%:*}` strips the shortest suffix from the final `:`.
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

# --- apply + wait -----------------------------------------------------------
log "applying config/default (controller + server + CRDs + RBAC + webhook)"
kubectl apply -k "$tmpdir/config/default"

log "waiting up to $READY_TIMEOUT for controller + server deployments to reach Available"
kubectl -n "$NAMESPACE" wait --for=condition=Available --timeout="$READY_TIMEOUT" \
  deployment/inference-cache-controller-manager \
  deployment/inference-cache-server \
  || fail "controller and/or server did not reach Available within $READY_TIMEOUT"

# --- CacheIndex poller assertion -------------------------------------------
# The controller's CacheIndex poller is leader-elected and refreshes on a 30s
# ticker, so a non-empty observedServer within ~60s of Available proves the
# poller acquired the lease, reached the server's /snapshot endpoint, and wrote
# the singleton CR's status.
log "waiting up to ${CACHEINDEX_TIMEOUT}s for cacheindex/cluster-default to be populated"
deadline=$(($(date +%s) + CACHEINDEX_TIMEOUT))
observed=""
until [ -n "$observed" ]; do
  observed="$(kubectl get cacheindex cluster-default \
    -o jsonpath='{.status.observedServer}' 2>/dev/null || true)"
  if [ -n "$observed" ]; then break; fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl get cacheindex cluster-default -o yaml || true
    fail "cacheindex/cluster-default.status.observedServer was empty after ${CACHEINDEX_TIMEOUT}s"
  fi
  sleep 3
done
log "cacheindex/cluster-default.status.observedServer=$observed"

# --- gRPC fail-open assertion ----------------------------------------------
log "port-forwarding svc/inference-cache-server :9090 -> localhost:$GRPC_LOCAL_PORT"
mkdir -p "$LOG_DIR"
kubectl -n "$NAMESPACE" port-forward svc/inference-cache-server "$GRPC_LOCAL_PORT:9090" \
  >"$LOG_DIR/port-forward.log" 2>&1 &
pf_pid=$!

# Wait for the local port to actually accept connections; grpcurl errors out
# loudly if the forward isn't up yet, and we'd misattribute that to the server.
for _ in $(seq 1 30); do
  if grpcurl -plaintext -max-time 2 "localhost:$GRPC_LOCAL_PORT" list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# Probe twice in priority order: server reflection first (what a real gateway
# client would do), proto-file second (survives the reflection registration
# being temporarily reverted or moved). The smoke passes as long as one form
# returns the fail-open default.
probe_lookup_route() {
  local payload='{"modelId":"install-smoke-unknown"}'
  if grpcurl -plaintext -max-time 5 -d "$payload" \
       "localhost:$GRPC_LOCAL_PORT" \
       inferencecache.v1alpha1.InferenceCache/LookupRoute \
       2>"$LOG_DIR/grpcurl.err"; then
    return 0
  fi
  log "reflection probe failed; falling back to proto-file probe"
  grpcurl -plaintext -max-time 5 \
    -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto \
    -d "$payload" \
    "localhost:$GRPC_LOCAL_PORT" \
    inferencecache.v1alpha1.InferenceCache/LookupRoute \
    2>>"$LOG_DIR/grpcurl.err"
}

resp="$(probe_lookup_route)" || {
  cat "$LOG_DIR/grpcurl.err" >&2 || true
  fail "grpcurl LookupRoute did not return a response"
}
log "LookupRoute response: $resp"

# JSON field name varies between gRPC encoders (camelCase reasonCode from
# grpcurl's default JSONPB, snake_case reason_code in the .proto). Accept
# either form; require the value to be NO_HINT (the documented fail-open code
# for an unknown model).
if ! grep -Eq '"(reasonCode|reason_code)"[[:space:]]*:[[:space:]]*"NO_HINT"' <<<"$resp"; then
  fail "expected reason_code=NO_HINT for an unknown model, got: $resp"
fi

# --- paired-sample smoke ---------------------------------------------------
# Applies config/samples/cachebackend-with-engine.yaml and asserts the
# CacheBackend ↔ engine-pod binding's operator-facing signals materialize
# end-to-end:
#   - status.matchedEnginePods → 1
#   - inferencecache.io/injected-by annotation on the engine pod
#   - InjectedByCacheBackend Event on the engine pod whose regarding.uid
#     equals the persisted pod's metadata.uid (so the Event surfaces
#     under `kubectl describe pod`, not just `kubectl get events`)
# Then exercises the drift case: scale the engine Deployment to 0, force-
# delete the (terminating) pod, and assert status.matchedEnginePods → 0
# via the reconciler's RequeueAfter cadence within SAMPLE_DRIFT_TIMEOUT.
#
# Engine-side image is swapped to SAMPLE_ENGINE_IMAGE (busybox by default)
# BEFORE the engine Deployment lands so the sample exercises the wiring
# without paying a multi-GB vLLM pull. The webhook injects on pod CREATE;
# the signals here all materialize from object state, not from the engine
# actually running.
#
# Two-step apply (CB first, wait for status.endpoint, then engine
# Deployment) avoids a race the webhook would otherwise fail-open
# through: if the engine pod's admission lands before the reconciler has
# published the CacheBackend's status.endpoint, the webhook admits the
# pod unmodified (no annotation, no Event) — the rest of the smoke
# would then assert against a pod that's missing the signals through no
# product fault. Pre-publishing the endpoint closes the race.
log "creating sample namespace $SAMPLE_NS"
kubectl create namespace "$SAMPLE_NS" --dry-run=client -o yaml \
  | kubectl apply -f - >/dev/null

log "splitting paired sample into CacheBackend doc and engine Deployment doc"
sample_file="config/samples/cachebackend-with-engine.yaml"
# Place the split files under the trapped $tmpdir so an early failure
# between split and apply (or a SIGINT mid-run) does not leak temp files
# in /tmp. $tmpdir is created at script init and removed by cleanup().
sample_tmp_cb="$(mktemp "$tmpdir/sample-cb.XXXXXX")"
sample_tmp_engine="$(mktemp "$tmpdir/sample-engine.XXXXXX")"
# The paired sample is a two-doc YAML stream (CacheBackend, ---,
# Deployment) — guaranteed ordering, so awk on the `---` separator is
# enough. yq would be cleaner but isn't a guaranteed dependency on the
# CI image.
awk -v cb="$sample_tmp_cb" -v engine="$sample_tmp_engine" '
  /^---$/ { sep=1; next }
  !sep    { print > cb }
  sep     { print > engine }
' "$sample_file"

# Patch the engine container's image to the lightweight stand-in. The
# Deployment's metadata stays untouched (still qwen-engine, still
# labeled app=qwen-demo), so the binding label flow is exercised exactly
# as a real operator would experience it.
sed -i.bak "s|vllm/vllm-openai-cpu:latest-x86_64|$SAMPLE_ENGINE_IMAGE|g" \
  "$sample_tmp_engine"
rm -f "${sample_tmp_engine}.bak"

log "applying CacheBackend"
kubectl -n "$SAMPLE_NS" apply -f "$sample_tmp_cb" >/dev/null

log "waiting up to ${SAMPLE_ENDPOINT_TIMEOUT}s for status.endpoint"
deadline=$(($(date +%s) + SAMPLE_ENDPOINT_TIMEOUT))
endpoint=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  endpoint=$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.endpoint}' 2>/dev/null || true)
  if [ -n "$endpoint" ]; then break; fi
  sleep 2
done
if [ -z "$endpoint" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "CacheBackend.status.endpoint not populated after ${SAMPLE_ENDPOINT_TIMEOUT}s"
fi
log "status.endpoint=$endpoint"

log "applying engine Deployment (image=$SAMPLE_ENGINE_IMAGE)"
kubectl -n "$SAMPLE_NS" apply -f "$sample_tmp_engine" >/dev/null
# Split files live under $tmpdir and are removed by the trap; no
# explicit rm here so a failure between split and apply still cleans up.

log "waiting up to ${SAMPLE_MATCH_TIMEOUT}s for status.matchedEnginePods=1"
deadline=$(($(date +%s) + SAMPLE_MATCH_TIMEOUT))
matched=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  matched=$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.matchedEnginePods}' 2>/dev/null || true)
  if [ "$matched" = "1" ]; then break; fi
  sleep 2
done
if [ "$matched" != "1" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  kubectl -n "$SAMPLE_NS" get pod -l app=qwen-demo -o wide || true
  fail "status.matchedEnginePods=$matched, want 1 after ${SAMPLE_MATCH_TIMEOUT}s"
fi
log "status.matchedEnginePods=1"

# --- KV-event readiness gate assertion (operator-facing) --------------------
# The managed backend now has an engine pod attached (matchedEnginePods=1), but
# the smoke's stub engine (busybox) emits no KV events and the controller runs
# with no kvevent-subscriber sidecar, so NO KV event will ever be observed —
# the exact demo-day failure mode the gate exists to surface (engine present,
# KV-event stream silent).
#
# We assert the two DETERMINISTIC, gate-specific surfaces this PR adds, both of
# which hold regardless of whether the managed cache-server Deployment has
# reached Available yet (the smoke does not gate on its readiness, so the Ready
# *condition* could legitimately read RolloutInProgress rather than
# AwaitingFirstKVEvent — asserting the Ready reason would be racy and is
# deliberately avoided):
#   - spec.integration.firstEventTimeout defaulted to 5m — the new CRD field is
#     present in the shipped schema and admission defaults it.
#   - status.firstKVEventObservedAt stays UNSET — the gate's durable latch is
#     written the instant a KV event is observed; with an engine attached but
#     no event source, its continued absence is the gate-specific behavioral
#     signal that nothing has been observed. (Ready can never be True here, so
#     the gate also never lets the backend advertise readiness.)
# Both values are stable (no event source exists), so single reads suffice.
fet="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.spec.integration.firstEventTimeout}' 2>/dev/null || true)"
# Accept both "5m" (CRD-schema default, applied when the integration block is
# present) and "5m0s" (Go Duration.String(), the webhook-materialized form) —
# both decode to the same 5m duration.
if [ "$fet" != "5m" ] && [ "$fet" != "5m0s" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "spec.integration.firstEventTimeout=$fet, want 5m (CRD default / webhook defaulter not applied)"
fi
gate_latch="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.status.firstKVEventObservedAt}' 2>/dev/null || true)"
if [ -n "$gate_latch" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "status.firstKVEventObservedAt=$gate_latch, want unset (no KV event source exists in the smoke, so the gate latch must never be written)"
fi
log "KV-event gate: firstEventTimeout=$fet, firstKVEventObservedAt unset (no KV events observed, as expected)"

# Persisted pod identity (UID is server-assigned post-admission; the
# whole point of the engine-pod-events controller is to record the
# Event with this UID, not the empty one a webhook-recorded Event
# would have).
engine_pod=$(kubectl -n "$SAMPLE_NS" get pod -l app=qwen-demo \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
engine_uid=$(kubectl -n "$SAMPLE_NS" get pod -l app=qwen-demo \
  -o jsonpath='{.items[0].metadata.uid}' 2>/dev/null || true)
if [ -z "$engine_pod" ] || [ -z "$engine_uid" ]; then
  fail "no engine pod labeled app=qwen-demo found in namespace $SAMPLE_NS"
fi
log "engine pod: $engine_pod (uid=$engine_uid)"

# The mutating webhook stamps the annotation on successful injection.
# Absence here means the webhook either didn't fire or fail-opened.
injected_by=$(kubectl -n "$SAMPLE_NS" get pod "$engine_pod" \
  -o jsonpath='{.metadata.annotations.inferencecache\.io/injected-by}' 2>/dev/null || true)
if [ "$injected_by" != "$SAMPLE_NS/qwen-demo-cache" ]; then
  fail "annotation inferencecache.io/injected-by=$injected_by, want $SAMPLE_NS/qwen-demo-cache"
fi
log "annotation inferencecache.io/injected-by=$injected_by"

# Event assertion. Polls because the events.EventRecorder broadcasts
# asynchronously. Match on (regarding.uid, reason) — NOT just name —
# because describe-by-UID is the user-facing contract.
log "waiting for InjectedByCacheBackend Event with regarding.uid=$engine_uid"
deadline=$(($(date +%s) + 30))
seen=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  seen=$(kubectl -n "$SAMPLE_NS" get events.events.k8s.io \
    --field-selector reason=InjectedByCacheBackend \
    -o jsonpath="{range .items[?(@.regarding.uid=='$engine_uid')]}{.reason}{'\n'}{end}" \
    2>/dev/null || true)
  if [ -n "$seen" ]; then break; fi
  sleep 2
done
if [ -z "$seen" ]; then
  kubectl -n "$SAMPLE_NS" get events.events.k8s.io \
    --field-selector reason=InjectedByCacheBackend -o yaml || true
  fail "InjectedByCacheBackend Event not observed on pod uid=$engine_uid within 30s"
fi
log "InjectedByCacheBackend Event present on the engine pod (UID matches the persisted pod)"

# --- drift case: cadence-driven Matched → 0 --------------------------------
# Scale engine to 0; force-delete to avoid the (terminating) pod still
# being label-visible to the reconciler's pod List for an extended
# time when the image is unavailable. Then wait for the next
# RequeueAfter cycle (no CB-side change, no Owned watch event — pure
# cadence) to drive Matched=0.
log "scaling engine Deployment to 0 to exercise the RequeueAfter cadence"
kubectl -n "$SAMPLE_NS" scale deploy qwen-engine --replicas=0 >/dev/null
kubectl -n "$SAMPLE_NS" delete pod "$engine_pod" \
  --force --grace-period=0 >/dev/null 2>&1 || true
log "waiting up to ${SAMPLE_DRIFT_TIMEOUT}s for status.matchedEnginePods=0 via cadence"
deadline=$(($(date +%s) + SAMPLE_DRIFT_TIMEOUT))
drifted=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  drifted=$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.matchedEnginePods}' 2>/dev/null || true)
  if [ "$drifted" = "0" ]; then break; fi
  sleep 2
done
if [ "$drifted" != "0" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  kubectl -n "$SAMPLE_NS" get pod -l app=qwen-demo -o wide || true
  fail "status.matchedEnginePods=$drifted after engine scaled to 0; want 0 via cadence within ${SAMPLE_DRIFT_TIMEOUT}s"
fi
log "drift converged: status.matchedEnginePods=0 via the self-RequeueAfter cadence"

# Sample cleanup: tear down the whole dedicated namespace in one shot
# (best-effort; failure here doesn't fail the smoke). The script created
# the namespace at the start of this phase, so this leaves the cluster
# in the state the rest of the smoke produced.
kubectl delete namespace "$SAMPLE_NS" \
  --wait=false --ignore-not-found=true >/dev/null 2>&1 || true

# --- External CacheBackend end-to-end ---------------------------------------
# Exercises the External passthrough adapter on the running cluster: the
# reconciler should NOT render a Deployment/Service, status.endpoint should
# mirror spec.endpoint, Ready should be True with reason ExternalEndpointAccepted,
# and a matching engine pod should come out of admission with LMCACHE_REMOTE_URL
# pointing at the operator-supplied endpoint. Also exercises the new admission
# validation rules (non-External with endpoint and External with a bad scheme
# must be rejected at write time).
log "exercising External CacheBackend end-to-end in namespace $EXT_SMOKE_NS"
kubectl create namespace "$EXT_SMOKE_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Apply the External CR. selector label matches the engine pod below.
cat <<EOF | kubectl apply -f - >/dev/null || fail "kubectl apply CacheBackend(type:External) failed"
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: $EXT_SMOKE_CB_NAME
  namespace: $EXT_SMOKE_NS
spec:
  type: External
  endpoint: $EXT_SMOKE_ENDPOINT
  integration:
    engine: vllm
    role: ReadWrite
  engineSelector:
    matchLabels:
      ${EXT_SMOKE_LABEL%%=*}: ${EXT_SMOKE_LABEL##*=}
EOF

# Wait for the reconciler to publish status.endpoint + the Ready=True condition.
# Sub-second on a quiet cluster; the timeout covers leader-election warm-up.
log "waiting up to ${EXTERNAL_BACKEND_TIMEOUT}s for External CR to publish status + Ready=True"
deadline=$(($(date +%s) + EXTERNAL_BACKEND_TIMEOUT))
status_endpoint=""
ready_status=""
ready_reason=""
until [ "$status_endpoint" = "$EXT_SMOKE_ENDPOINT" ] && \
      [ "$ready_status" = "True" ] && \
      [ "$ready_reason" = "ExternalEndpointAccepted" ]; do
  status_endpoint="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.status.endpoint}' 2>/dev/null || true)"
  ready_status="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  ready_reason="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" -o yaml || true
    fail "External CR didn't converge: status.endpoint=$status_endpoint Ready=$ready_status/$ready_reason (want $EXT_SMOKE_ENDPOINT True/ExternalEndpointAccepted)"
  fi
  sleep 1
done
log "External CR status: endpoint=$status_endpoint Ready=$ready_status/$ready_reason"

# No Deployment, no Service should have been rendered for an External CR.
# A leading API service `kubernetes` doesn't exist in this fresh namespace,
# so a flat count of zero is the right assertion.
dep_count="$(kubectl -n "$EXT_SMOKE_NS" get deploy -o name 2>/dev/null | wc -l | tr -d ' ')"
svc_count="$(kubectl -n "$EXT_SMOKE_NS" get svc -o name 2>/dev/null | wc -l | tr -d ' ')"
if [ "$dep_count" != "0" ] || [ "$svc_count" != "0" ]; then
  kubectl -n "$EXT_SMOKE_NS" get deploy,svc
  fail "External CR rendered controller-owned workload (deploy=$dep_count svc=$svc_count, want 0/0)"
fi
log "no Deployment or Service in $EXT_SMOKE_NS (External backend skipped provisioning)"

# Apply a matching engine pod with the conventional `vllm` container name.
# `pause` keeps the pod alive long enough to inspect the injected env+args
# without pulling a real vLLM image (which would be ~5+ GB).
cat <<EOF | kubectl apply -f - >/dev/null || fail "kubectl apply engine pod failed"
apiVersion: v1
kind: Pod
metadata:
  name: $EXT_SMOKE_POD_NAME
  namespace: $EXT_SMOKE_NS
  labels:
    ${EXT_SMOKE_LABEL%%=*}: ${EXT_SMOKE_LABEL##*=}
spec:
  containers:
  - name: vllm
    image: registry.k8s.io/pause:3.10
EOF

# The pod webhook is synchronous at admission, so the env should be present
# the moment the API has the object. The retry loop here is a defensive
# circuit-breaker against a slow first-admission (cert-manager certificate
# becoming available, etc.), NOT an expected wait.
log "waiting up to ${EXTERNAL_INJECT_TIMEOUT}s for pod webhook to inject External endpoint"
deadline=$(($(date +%s) + EXTERNAL_INJECT_TIMEOUT))
injected=""
until [ -n "$injected" ]; do
  injected="$(kubectl -n "$EXT_SMOKE_NS" get pod "$EXT_SMOKE_POD_NAME" \
    -o jsonpath='{.spec.containers[?(@.name=="vllm")].env[?(@.name=="LMCACHE_REMOTE_URL")].value}' \
    2>/dev/null || true)"
  if [ -n "$injected" ]; then break; fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl -n "$EXT_SMOKE_NS" get pod "$EXT_SMOKE_POD_NAME" -o yaml || true
    fail "pod webhook did not inject LMCACHE_REMOTE_URL within ${EXTERNAL_INJECT_TIMEOUT}s"
  fi
  sleep 1
done
# Form the expected URL the same way the adapter does: preserve an
# operator-supplied `lm://` prefix (case-insensitive — admission lowers
# the scheme), otherwise prepend it. Without this an EXT_SMOKE_ENDPOINT
# of `lm://host:port` (legal per the contract) would compare against
# `lm://lm://host:port` and the smoke would fail on a valid input.
case "$(printf '%s' "$EXT_SMOKE_ENDPOINT" | tr '[:upper:]' '[:lower:]')" in
  lm://*) expected_url="lm://${EXT_SMOKE_ENDPOINT#??://}" ;;
  *)      expected_url="lm://$EXT_SMOKE_ENDPOINT" ;;
esac
if [ "$injected" != "$expected_url" ]; then
  fail "LMCACHE_REMOTE_URL=$injected, want $expected_url (pod webhook should wire to spec.endpoint via the LMCache wire format)"
fi
log "pod webhook injected LMCACHE_REMOTE_URL=$injected"

# Verify the --kv-transfer-config arg is also present — pins the full
# engine wire contract the External adapter shares with the LMCache adapter.
kv_arg="$(kubectl -n "$EXT_SMOKE_NS" get pod "$EXT_SMOKE_POD_NAME" \
  -o jsonpath='{.spec.containers[?(@.name=="vllm")].args}' 2>/dev/null || true)"
if ! grep -q -- "--kv-transfer-config" <<<"$kv_arg"; then
  fail "pod webhook did not inject --kv-transfer-config arg; got args=$kv_arg"
fi
log "pod args contain --kv-transfer-config"

# Negative-path admission checks. Each must be rejected with a specific
# message; admission error goes to stderr so we capture both streams.
log "exercising negative admission rules"

reject_output="$(kubectl apply -f - <<EOF 2>&1 || true
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: smoke-reject-https
  namespace: $EXT_SMOKE_NS
spec:
  type: External
  endpoint: https://cache.example.com:443/api
  integration: { engine: vllm }
EOF
)"
if ! grep -q 'scheme "https" is not supported' <<<"$reject_output"; then
  fail "admission did not reject External+https scheme as expected; got: $reject_output"
fi

reject_output="$(kubectl apply -f - <<EOF 2>&1 || true
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: smoke-reject-no-host
  namespace: $EXT_SMOKE_NS
spec:
  type: External
  endpoint: "lm://"
  integration: { engine: vllm }
EOF
)"
if ! grep -q "must be a non-empty host AND port" <<<"$reject_output"; then
  fail "admission did not reject External+lm:// (no host) as expected; got: $reject_output"
fi

reject_output="$(kubectl apply -f - <<EOF 2>&1 || true
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: smoke-reject-managed-endpoint
  namespace: $EXT_SMOKE_NS
spec:
  type: LMCache
  endpoint: user-supplied.example:8080
  integration: { engine: vllm }
EOF
)"
if ! grep -q "spec.endpoint is only valid when spec.type=External" <<<"$reject_output"; then
  fail "admission did not reject non-External + endpoint as expected; got: $reject_output"
fi
log "admission rejected External+https, External+empty-host, and non-External+endpoint"

# Clean up — keeps the cluster reusable for KEEP_CLUSTER=1 reruns.
kubectl delete pod -n "$EXT_SMOKE_NS" "$EXT_SMOKE_POD_NAME" --ignore-not-found --wait=false >/dev/null || true
kubectl delete cb -n "$EXT_SMOKE_NS" "$EXT_SMOKE_CB_NAME" --ignore-not-found --wait=false >/dev/null || true
kubectl delete namespace "$EXT_SMOKE_NS" --ignore-not-found --wait=false >/dev/null || true

# --- /snapshot auth assertion ---------------------------------------------
# The CacheIndex CR being populated above already proves the controller can
# scrape /snapshot with its SA token (the bearer path). The complementary
# half — that an UNAUTHENTICATED caller is rejected — is what this section
# checks, since it's the failure mode the new auth middleware is meant to
# prevent. A short-lived curl pod outside the controller's identity tries to
# GET /snapshot; the server must respond 401, OR the NetworkPolicy must drop
# the connection at L3/L4 (curl exits non-zero on timeout). Either outcome
# proves the gate works; under kind's default kindnet CNI, NetworkPolicy is
# not enforced so the 401 path is the one actually exercised.
log "asserting unauthenticated /snapshot scrape from a side pod is rejected"
SIDE_POD="ic-snapshot-probe"
# Clean any leftover probe from an interrupted prior run before creating a
# fresh one — otherwise `kubectl run` fails with AlreadyExists and the script
# silently reads stale logs from the previous attempt. --wait gates on the
# delete actually completing so the create below sees a clean namespace.
kubectl -n "$NAMESPACE" delete pod "$SIDE_POD" --ignore-not-found --wait=true >/dev/null 2>&1 || true
if ! kubectl -n "$NAMESPACE" run "$SIDE_POD" --image=curlimages/curl:8.10.1 --restart=Never \
  --command -- /bin/sh -c '
    # -w prints the HTTP status; -o /dev/null discards the (error) body so the
    # status is the only line on stdout. Timeout protects against the listener
    # being unreachable (NetworkPolicy drop), in which case curl exits non-zero.
    curl -sS -m 5 -o /dev/null -w "%{http_code}" \
      http://inference-cache-server:8081/snapshot || echo "curl_failed:$?"
  ' >/tmp/snapshot-probe-create.log 2>&1; then
  cat /tmp/snapshot-probe-create.log >&2 || true
  fail "kubectl run $SIDE_POD failed; cannot run /snapshot auth assertion"
fi

# Wait for the probe pod to finish (Succeeded or Failed) — either is fine; we
# read its logs to learn the outcome. The 90s budget covers the curlimages/curl
# image pull on a cold kind node (typical pull is ~15s, but the paired-sample
# phase that runs earlier can leave the kubelet busy reaping its own
# Terminating pods, occasionally pushing the new pod's container creation
# above the previous 30s budget).
for _ in $(seq 1 90); do
  phase="$(kubectl -n "$NAMESPACE" get pod "$SIDE_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
    break
  fi
  sleep 1
done
probe_out="$(kubectl -n "$NAMESPACE" logs "$SIDE_POD" 2>/dev/null || true)"
# If the pod never finished, capture its describe output so the failure
# message tells operators why (ImagePullBackOff vs ContainerCreating vs ...).
if [ -z "$probe_out" ]; then
  kubectl -n "$NAMESPACE" describe pod "$SIDE_POD" >&2 || true
fi
kubectl -n "$NAMESPACE" delete pod "$SIDE_POD" --grace-period=0 --force >/dev/null 2>&1 || true

# Acceptable outcomes (either gate sufficient):
#   - HTTP 401: the L7 auth middleware rejected the request (kindnet path
#     today; the only branch actually exercised under the default CNI).
#   - "curl_failed:28": curl timed out (`-m 5`), i.e. the L3/L4 NetworkPolicy
#     dropped the SYN and the kernel never saw a RST. Exit code 28 is the
#     SHAPE of a real CNI-enforced policy drop.
# Other curl exit codes are NOT accepted — they would mask a regression:
#   - 6  (couldn't resolve host) → Service rename or DNS bug
#   - 7  (failed to connect, e.g. ECONNREFUSED) → server not listening; this
#        is NOT what an enforcing CNI does (it drops packets silently, it
#        does not RST), so accepting 7 would let "listener crashed" pass
#   - 3  (malformed URL) → script regression
# Restricting the catch-all to 28 keeps the smoke a real regression detector.
# 200 (unauthenticated) is always a regression.
# A future ticket will swap kindnet for an enforcing CNI and tighten the
# accept set to require curl_failed:28 alone (i.e. reject 401), proving
# the NetworkPolicy is actually doing the work in CI.
case "$probe_out" in
  "401"|*"curl_failed:28"*)
    log "unauthenticated /snapshot probe rejected (probe output: $probe_out)"
    ;;
  *)
    fail "unauthenticated /snapshot probe was not rejected (or curl failed for an unexpected reason); got: $probe_out"
    ;;
esac

log "PASS — install bundle came up, CacheIndex poller is writing, gRPC fail-open works, CacheBackend ↔ engine-pod binding signals wire up + drift cadence converges, External backend end-to-end works, /snapshot rejects unauth"
