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
#   4. The External CacheBackend type end-to-end: applying a `type: External`
#      CR renders NO Deployment/Service in its namespace, status.endpoint
#      mirrors spec.endpoint, the CR goes Ready=True/ExternalEndpointAccepted,
#      and a matching engine pod is admitted with `LMCACHE_REMOTE_URL=lm://
#      <spec.endpoint>` injected by the pod-mutating webhook. Also exercises
#      the new admission validation rules (non-External + endpoint and
#      External + bad scheme are both rejected at write time).
#
# Distinct from the C2/C6 canaries: those exercise real engine pods + cross-pod
# cache reuse (multi-GB image, ~10+ GiB RAM, schedule-only). This smoke stops
# at "the default install bundle wires together; gRPC fail-open works" -- light
# enough to run on every PR.
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
#           CACHEINDEX_TIMEOUT, GRPC_LOCAL_PORT, LOG_DIR,
#           EXTERNAL_BACKEND_TIMEOUT, EXTERNAL_INJECT_TIMEOUT.

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

log "PASS — install bundle came up, CacheIndex poller is writing, gRPC fail-open works, External backend end-to-end works"
