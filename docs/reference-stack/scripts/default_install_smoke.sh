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
#   3. The CachePolicy PUSH path works: an applied `CachePolicy` renders its
#      operator-facing printer columns, the controller pushes it to the
#      server's `/policy` endpoint, and `LookupRoute` observes the pushed
#      minimumPrefixTokens gate without engine pods or inference traffic.
#   4. The per-CacheTenant status projection works: an applied `CacheTenant`
#      gets `.status.indexEntries=0` (observed-zero — no engine traffic in the
#      smoke) and a `Ready=True` condition written by the same poller.
#   5. The gRPC surface is reachable: a `LookupRoute` for an unknown model
#      returns the fail-open default (`reason_code: NO_HINT`).
#   6. The CacheBackend ↔ engine-pod binding surfaces operators rely on
#      actually wire up end-to-end: applying config/samples/cachebackend-
#      with-engine.yaml drives status.matchedEnginePods=1, stamps the
#      injected-by annotation on the engine pod, and surfaces the
#      InjectedByCacheBackend Event (with the persisted pod UID — the
#      regression that hides events from `kubectl describe pod`). Then
#      scaling the engine to 0 drives status.matchedEnginePods=0 via the
#      reconciler's self-RequeueAfter cadence (no CR or owned-workload
#      event needed) within ~30s, the bound on stale-Matched the
#      cadence guarantees.
#   7. The External CacheBackend type end-to-end: applying the committed
#      config/samples/cachebackend-external.yaml drives the CacheBackend
#      mutating webhook defaults (lookupTimeoutMs=50,
#      minimumPrefixTokens=256), renders NO Deployment/Service in its
#      namespace, status.endpoint mirrors spec.endpoint, observedGeneration
#      is set, the CR goes Ready=True/ExternalEndpointAccepted, and
#      `kubectl get cb` renders the CacheBackend printer columns. A matching
#      engine pod is admitted with `LMCACHE_REMOTE_URL=lm://<spec.endpoint>`
#      injected by the pod-mutating webhook. Also exercises admission
#      validation rules (External with no endpoint, External with bad
#      endpoint shape, and non-External + endpoint are rejected at write time).
#   8. The /snapshot endpoint rejects unauthenticated callers: a side curl
#      pod outside the controller's SA identity gets either an HTTP 401 (L7
#      auth middleware) or a curl timeout (L3/L4 NetworkPolicy drop).
#   9. The /policy endpoint rejects unauthenticated callers: same side-pod
#      shape against the write-side endpoint. This is the more dangerous of
#      the two — /policy is replace-on-write, so a successful unauthenticated
#      POST would override every namespace's CachePolicy state cluster-wide.
#      The probe POSTs a valid snapshot body so the rejection cannot be
#      misattributed to a 400; the only valid outcome is 401 (auth
#      middleware) or a curl timeout (NetworkPolicy drop).
#
# Distinct from the C2/C6 canaries: those exercise real engine pods + cross-pod
# cache reuse (multi-GB image, ~10+ GiB RAM, schedule-only). This smoke stops
# at "the default install bundle wires together; gRPC fail-open works; the
# CacheBackend ↔ engine-pod binding surfaces operators rely on actually
# wire up end-to-end" -- light enough to run on every PR. The paired-sample
# phase swaps the engine container's image to busybox before pod CREATE and
# uses a tiny locally built lmcache_server stand-in for the managed cache
# server, so the smoke does not pay multi-GB pulls or depend on mutable
# upstream image availability; the signals it asserts materialize from pod
# CREATE and the controller-managed Deployment readiness surface.
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
#           CACHEINDEX_TIMEOUT, POLICY_PUSH_TIMEOUT, GRPC_LOCAL_PORT, LOG_DIR,
#           POLICY_SMOKE_NS, SAMPLE_NS, SAMPLE_ENDPOINT_TIMEOUT,
#           SAMPLE_MATCH_TIMEOUT, SAMPLE_DRIFT_TIMEOUT, SAMPLE_ENGINE_IMAGE,
#           SAMPLE_CACHE_SERVER_IMAGE, EXTERNAL_BACKEND_TIMEOUT,
#           EXTERNAL_INJECT_TIMEOUT.

set -euo pipefail

TAG="${TAG:-${GITHUB_SHA:-$(git rev-parse HEAD)}}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-install-smoke}"
NAMESPACE="${NAMESPACE:-inference-cache-system}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.1}"
READY_TIMEOUT="${READY_TIMEOUT:-120s}"
CACHEINDEX_TIMEOUT="${CACHEINDEX_TIMEOUT:-90}" # seconds; ~3x the 30s refresh, absorbs leader-election + first-tick jitter
POLICY_PUSH_TIMEOUT="${POLICY_PUSH_TIMEOUT:-90}" # seconds; watch-triggered push + one 30s periodic repair tick
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
POLICY_SMOKE_NS="${POLICY_SMOKE_NS:-ic-smoke-policy}"
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
# KV-event gate: time budget for the managed cache-server Deployment to pull
# its image and reach Available, then for the gate to publish
# AwaitingFirstKVEvent. The image pull dominates on a cold node, hence the
# larger default than the other sample waits.
SAMPLE_GATE_TIMEOUT="${SAMPLE_GATE_TIMEOUT:-240}"
# Tiny stand-in for the vLLM image. The webhook injects on pod CREATE; the
# engine doesn't need to run for the operator-facing signals (Matched,
# annotation, Event) to materialize. Avoids a multi-GB pull in CI.
SAMPLE_ENGINE_IMAGE="${SAMPLE_ENGINE_IMAGE:-busybox:1.36}"
# Tiny stand-in for the managed LMCache server image. The controller still
# renders the canonical lmcache_server command/args and TCP readiness probe; the
# image only provides a local binary that listens on the requested port so the
# Deployment can become Available without pulling lmcache/standalone:latest.
SAMPLE_CACHE_SERVER_IMAGE="${SAMPLE_CACHE_SERVER_IMAGE:-install-smoke-lmcache-server:$TAG}"

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
EXT_SMOKE_CB_NAME="cachebackend-external"
EXT_SMOKE_POD_NAME="${EXT_SMOKE_POD_NAME:-smoke-engine}"

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
  kubectl get cachetenants -A -o yaml \
    >"$LOG_DIR/cachetenants.yaml" 2>&1 || true
  kubectl get cachepolicies -A -o yaml \
    >"$LOG_DIR/cachepolicies.yaml" 2>&1 || true
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
  kubectl get cb -A -o yaml \
    >"$LOG_DIR/cachebackends.yaml" 2>&1 || true
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

build_sample_cache_server_image() {
  local context
  context="$(mktemp -d "$tmpdir/lmcache-server-context.XXXXXX")"

  cat >"$context/lmcache_server" <<'EOF'
#!/bin/sh
port="${2:-65432}"
while true; do
  nc -l -p "$port" >/dev/null 2>&1 || sleep 1
done
EOF

  cat >"$context/Dockerfile" <<'EOF'
FROM busybox:1.36
COPY lmcache_server /usr/local/bin/lmcache_server
RUN chmod +x /usr/local/bin/lmcache_server
EOF

  log "building lightweight lmcache_server stand-in image=$SAMPLE_CACHE_SERVER_IMAGE"
  docker build -t "$SAMPLE_CACHE_SERVER_IMAGE" "$context"
  log "loading $SAMPLE_CACHE_SERVER_IMAGE into the kind node"
  "$KIND" load docker-image "$SAMPLE_CACHE_SERVER_IMAGE" --name "$KIND_CLUSTER"
}

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

# --- CacheBackend CRD schema-trim assertion --------------------------------
# The installed CRD must reflect the inert-field trim: the three removed fields
# are absent from the served v1alpha1 schema, and the field that replaced the
# removed status.indexEntries — status.indexParticipation.prefixCount — is
# present. Probing the live CRD in the cluster (not just the repo manifest)
# proves the trimmed schema is what actually got installed by `kubectl apply
# -k`. Each probe asks for a field's `.type`: absent fields yield empty output,
# present fields yield their OpenAPI type — unambiguous and free of
# map-formatting quirks.
crd_field_type() {
  # $1 = jsonpath under the v1alpha1 openAPIV3Schema.properties root
  kubectl get crd cachebackends.inferencecache.io \
    -o "jsonpath={.spec.versions[?(@.name=='v1alpha1')].schema.openAPIV3Schema.properties.$1.type}" \
    2>/dev/null || true
}
if [ -n "$(crd_field_type 'spec.properties.integration.properties.lookupTimeoutMs')" ]; then
  fail "CRD still serves removed spec.integration.lookupTimeoutMs (schema trim not installed)"
fi
if [ -n "$(crd_field_type 'spec.properties.integration.properties.minimumPrefixTokens')" ]; then
  fail "CRD still serves removed spec.integration.minimumPrefixTokens (schema trim not installed)"
fi
if [ -n "$(crd_field_type 'status.properties.indexEntries')" ]; then
  fail "CRD still serves removed status.indexEntries (schema trim not installed)"
fi
# status.indexParticipation.prefixCount is the authoritative count surface that
# replaced the removed status.indexEntries — assert the replacement is served.
if [ -z "$(crd_field_type 'status.properties.indexParticipation.properties.prefixCount')" ]; then
  fail "CRD is missing status.indexParticipation.prefixCount (the replacement for status.indexEntries)"
fi
log "CacheBackend CRD reflects the schema trim (lookupTimeoutMs/minimumPrefixTokens/indexEntries absent; indexParticipation.prefixCount present)"

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

# --- CachePolicy push + printer-column setup --------------------------------
# Apply a CachePolicy in a dedicated namespace and verify its operator-facing
# table columns render. The gRPC side-effect assertion below proves this CR
# was pushed through the controller's authenticated /policy bridge and adopted
# by the server; keeping the apply here gives the watch-triggered reconcile
# time to run before the port-forward opens.
log "resetting CachePolicy smoke namespace $POLICY_SMOKE_NS"
kubectl delete namespace "$POLICY_SMOKE_NS" --ignore-not-found --wait=true --timeout=60s >/dev/null \
  || fail "timed out waiting for prior CachePolicy smoke namespace $POLICY_SMOKE_NS to delete"
log "applying CachePolicy sample in namespace $POLICY_SMOKE_NS"
kubectl create namespace "$POLICY_SMOKE_NS" --dry-run=client -o yaml \
  | kubectl apply -f - >/dev/null
kubectl -n "$POLICY_SMOKE_NS" apply -f config/samples/cache_v1alpha1_cachepolicy.yaml >/dev/null

# The Eviction printer column is the operator-facing surface kept on the
# CachePolicy CRD. Verify the header and the row values so a regression to a
# default NAME/AGE-only table does not pass unnoticed.
cp_table="$(kubectl -n "$POLICY_SMOKE_NS" get cachepolicy cachepolicy-sample 2>/dev/null || true)"
cp_header="$(printf '%s\n' "$cp_table" | sed -n '1p')"
if ! grep -Eq "(^|[[:space:]])EVICTION([[:space:]]|$)" <<<"$cp_header"; then
  echo "$cp_table"
  fail "expected CachePolicy printer column EVICTION in kubectl get output"
fi
if ! grep -Fq "cachepolicy-sample" <<<"$cp_table" || \
   ! grep -Fq "LRU" <<<"$cp_table"; then
  echo "$cp_table"
  fail "expected CachePolicy printer row to include name and Eviction=LRU"
fi
log "CachePolicy printer column renders Eviction"

# --- CacheTenant status projection assertion -------------------------------
# Apply a CacheTenant and prove the poller's per-tenant projection writes its
# status. The smoke cluster has no engine pods, so the tenant holds zero
# prefixes: the projection must report indexEntries=0 (observed-zero, not nil)
# with Ready=True. This exercises the CacheTenant CRD schema, the combined
# CachePolicy+CacheTenant push to /policy, and the per-tenant status writer.
log "applying CacheTenant sample and waiting for its status projection"
kubectl apply -f config/samples/cache_v1alpha1_cachetenant.yaml
deadline=$(($(date +%s) + CACHEINDEX_TIMEOUT))
ct_entries=""
until [ -n "$ct_entries" ]; do
  ct_entries="$(kubectl get cachetenant cachetenant-sample \
    -o jsonpath='{.status.indexEntries}' 2>/dev/null || true)"
  if [ -n "$ct_entries" ]; then break; fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl get cachetenant cachetenant-sample -o yaml || true
    fail "cachetenant-sample.status.indexEntries was empty after ${CACHEINDEX_TIMEOUT}s"
  fi
  sleep 3
done
if [ "$ct_entries" != "0" ]; then
  kubectl get cachetenant cachetenant-sample -o yaml || true
  fail "expected cachetenant-sample.status.indexEntries=0 (no traffic), got: $ct_entries"
fi
ct_ready="$(kubectl get cachetenant cachetenant-sample \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
if [ "$ct_ready" != "True" ]; then
  kubectl get cachetenant cachetenant-sample -o yaml || true
  fail "expected cachetenant-sample Ready=True, got: ${ct_ready:-<unset>}"
fi

# The printer columns (Tenant / Entries / Quota) are themselves an operator-
# facing surface. Verify `kubectl get cachetenants` renders them — a default
# table with only NAME/AGE would mean the additionalPrinterColumns regressed.
ct_table="$(kubectl get cachetenant cachetenant-sample 2>/dev/null || true)"
if ! grep -q 'tenant-a' <<<"$ct_table" || ! grep -q '100000' <<<"$ct_table"; then
  echo "$ct_table"
  fail "expected CacheTenant printer columns (Tenant=tenant-a, Quota=100000) in kubectl get output"
fi
log "cachetenant-sample.status: indexEntries=$ct_entries Ready=$ct_ready (printer columns OK)"

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
# being temporarily reverted or moved).
grpcurl_lookup_route() {
  local payload="$1"
  local err_file="${2:-$LOG_DIR/grpcurl.err}"
  if grpcurl -plaintext -max-time 5 -d "$payload" \
       "localhost:$GRPC_LOCAL_PORT" \
       inferencecache.v1alpha1.InferenceCache/LookupRoute \
       2>"$err_file"; then
    return 0
  fi
  log "reflection LookupRoute probe failed; falling back to proto-file probe" >&2
  grpcurl -plaintext -max-time 5 \
    -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto \
    -d "$payload" \
    "localhost:$GRPC_LOCAL_PORT" \
    inferencecache.v1alpha1.InferenceCache/LookupRoute \
    2>>"$err_file"
}

grpcurl_report_cache_state() {
  local payload="$1"
  local err_file="${2:-$LOG_DIR/grpcurl-report-cache-state.err}"
  if printf '%s\n' "$payload" | grpcurl -plaintext -max-time 5 -d @ \
       "localhost:$GRPC_LOCAL_PORT" \
       inferencecache.v1alpha1.InferenceCache/ReportCacheState \
       2>"$err_file"; then
    return 0
  fi
  log "reflection ReportCacheState probe failed; falling back to proto-file probe" >&2
  printf '%s\n' "$payload" | grpcurl -plaintext -max-time 5 \
    -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto \
    -d @ \
    "localhost:$GRPC_LOCAL_PORT" \
    inferencecache.v1alpha1.InferenceCache/ReportCacheState \
    2>>"$err_file"
}

has_reason_code() {
  local resp="$1"
  local want="$2"
  grep -Eq "\"(reasonCode|reason_code)\"[[:space:]]*:[[:space:]]*\"$want\"" <<<"$resp"
}

probe_lookup_route() {
  local payload='{"modelId":"install-smoke-unknown"}'
  grpcurl_lookup_route "$payload" "$LOG_DIR/grpcurl.err"
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
if ! has_reason_code "$resp" "NO_HINT"; then
  fail "expected reason_code=NO_HINT for an unknown model, got: $resp"
fi

# --- CachePolicy PUSH adoption assertion -----------------------------------
# No read-back endpoint exists for /policy, by design. Prove the server adopted
# the controller-pushed CachePolicy via an existing gRPC side effect instead:
# seed one synthetic prefix metadata update, then require a lookup above the
# policy's minimumPrefixTokens to hit while the same exact prefix below the
# threshold returns NO_HINT. Without the pushed policy, the low-token lookup
# would also be a PREFIX_MATCH. This avoids engine pods/images, model traffic,
# and any new transport.
log "seeding a prefix and asserting CachePolicy minimumPrefixTokens is enforced by LookupRoute"
policy_model="install-smoke-policy"
policy_replica="policy-smoke-replica"
policy_hash_b64="cG9saWN5LXByZWZpeA==" # base64("policy-prefix")
policy_report_payload="$(cat <<EOF
{"replicaId":"$policy_replica","modelId":"$policy_model","tenantId":"$POLICY_SMOKE_NS","hashScheme":"vllm","prefixes":[{"prefixHash":"$policy_hash_b64","tokenCount":64}],"stats":{"replicaId":"$policy_replica","hitRate":1}}
EOF
)"
policy_report_resp="$(grpcurl_report_cache_state "$policy_report_payload" "$LOG_DIR/grpcurl-policy-report.err")" || {
  cat "$LOG_DIR/grpcurl-policy-report.err" >&2 || true
  fail "grpcurl ReportCacheState did not accept the CachePolicy smoke prefix"
}
log "ReportCacheState response: $policy_report_resp"

policy_high_payload="$(cat <<EOF
{"modelId":"$policy_model","tenantId":"$POLICY_SMOKE_NS","hashScheme":"vllm","prefixHash":"$policy_hash_b64","prefixTokenCount":64}
EOF
)"
policy_low_payload="$(cat <<EOF
{"modelId":"$policy_model","tenantId":"$POLICY_SMOKE_NS","hashScheme":"vllm","prefixHash":"$policy_hash_b64","prefixTokenCount":1}
EOF
)"

deadline=$(($(date +%s) + POLICY_PUSH_TIMEOUT))
policy_high_resp=""
policy_low_resp=""
until has_reason_code "$policy_high_resp" "PREFIX_MATCH" && has_reason_code "$policy_low_resp" "NO_HINT"; do
  policy_high_resp="$(grpcurl_lookup_route "$policy_high_payload" "$LOG_DIR/grpcurl-policy-high.err")" || true
  policy_low_resp="$(grpcurl_lookup_route "$policy_low_payload" "$LOG_DIR/grpcurl-policy-low.err")" || true

  if has_reason_code "$policy_high_resp" "PREFIX_MATCH" && has_reason_code "$policy_low_resp" "NO_HINT"; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl -n "$POLICY_SMOKE_NS" get cachepolicy cachepolicy-sample -o yaml || true
    echo "above-threshold LookupRoute response:" >&2
    echo "$policy_high_resp" >&2
    echo "below-threshold LookupRoute response:" >&2
    echo "$policy_low_resp" >&2
    for err_file in "$LOG_DIR/grpcurl-policy-high.err" "$LOG_DIR/grpcurl-policy-low.err"; do
      if [ -s "$err_file" ]; then
        echo "$(basename "$err_file"):" >&2
        cat "$err_file" >&2
      fi
    done
    fail "server did not adopt the pushed CachePolicy within ${POLICY_PUSH_TIMEOUT}s (want high-token PREFIX_MATCH and low-token NO_HINT)"
  fi
  sleep 2
done
log "CachePolicy push adopted: above-threshold lookup hit, below-threshold lookup returned NO_HINT"
kubectl delete namespace "$POLICY_SMOKE_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true

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

build_sample_cache_server_image
escaped_sample_cache_server_image="$(printf '%s' "$SAMPLE_CACHE_SERVER_IMAGE" | sed 's/[&|\\]/\\&/g')"
sed -i.bak "s|serverImage: lmcache/standalone:latest|serverImage: $escaped_sample_cache_server_image|g" \
  "$sample_tmp_cb"
rm -f "${sample_tmp_cb}.bak"

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
# The managed backend has an engine pod attached (matchedEnginePods=1), but the
# smoke's stub engine (busybox) emits no KV events and the controller runs with
# no kvevent-subscriber sidecar, so NO KV event will ever be observed — the
# exact demo-day failure mode the gate exists to surface (engine present,
# KV-event stream silent). We assert the gate's operator-visible surfaces end to
# end on the real install:
#   - spec.integration.firstEventTimeout defaulted to 5m (new CRD field +
#     admission defaulting);
#   - once the managed cache-server reaches Available, the gate holds the
#     backend at Ready=False / reason AwaitingFirstKVEvent — the deterministic
#     condition surface (Ready can never become True here, so without the gate
#     the backend would have been reported Ready on rollout);
#   - status.firstKVEventObservedAt stays UNSET — the durable latch is written
#     the instant a KV event is observed, so its absence is the gate-specific
#     "nothing observed" signal.
fet="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.spec.integration.firstEventTimeout}' 2>/dev/null || true)"
# Accept both "5m" (CRD-schema default, applied when the integration block is
# present) and "5m0s" (Go Duration.String(), the webhook-materialized form) —
# both decode to the same 5m duration.
if [ "$fet" != "5m" ] && [ "$fet" != "5m0s" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "spec.integration.firstEventTimeout=$fet, want 5m (CRD default / webhook defaulter not applied)"
fi

# The gate only evaluates once the managed cache-server Deployment is Available,
# so wait for that first; then the awaited state is deterministic (no events).
log "waiting up to ${SAMPLE_GATE_TIMEOUT}s for the managed cache-server Deployment to reach Available"
if ! kubectl -n "$SAMPLE_NS" wait --for=condition=Available --timeout="${SAMPLE_GATE_TIMEOUT}s" \
     deployment/qwen-demo-cache >/dev/null 2>&1; then
  kubectl -n "$SAMPLE_NS" get deployment/qwen-demo-cache -o yaml || true
  kubectl -n "$SAMPLE_NS" get pod -l app.kubernetes.io/instance=qwen-demo-cache -o wide || true
  fail "managed cache-server Deployment did not reach Available within ${SAMPLE_GATE_TIMEOUT}s; cannot exercise the KV-event gate"
fi
log "waiting up to ${SAMPLE_GATE_TIMEOUT}s for the gate to publish Ready=False / AwaitingFirstKVEvent"
deadline=$(($(date +%s) + SAMPLE_GATE_TIMEOUT))
gate_status=""
gate_reason=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  gate_status="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  gate_reason="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
  if [ "$gate_status" = "False" ] && [ "$gate_reason" = "AwaitingFirstKVEvent" ]; then break; fi
  sleep 2
done
if [ "$gate_status" != "False" ] || [ "$gate_reason" != "AwaitingFirstKVEvent" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "KV-event gate not engaged: Ready=$gate_status/$gate_reason, want False/AwaitingFirstKVEvent (engine attached, no KV events)"
fi

gate_latch="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.status.firstKVEventObservedAt}' 2>/dev/null || true)"
if [ -n "$gate_latch" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "status.firstKVEventObservedAt=$gate_latch, want unset (no KV event source exists, so the gate latch must never be written)"
fi
log "KV-event gate engaged: firstEventTimeout=$fet, Ready=False/AwaitingFirstKVEvent, firstKVEventObservedAt unset"

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
# Exercises the committed External passthrough sample on the running cluster:
# the mutating webhook should default integration lookup knobs, the reconciler
# should NOT render a Deployment/Service, status.endpoint should mirror
# spec.endpoint, observedGeneration should advance, Ready should be True with
# reason ExternalEndpointAccepted, and a matching engine pod should come out of
# admission with LMCACHE_REMOTE_URL pointing at the operator-supplied endpoint.
# Also exercises CacheBackend printer columns and the validating webhook's
# negative path.
log "exercising External CacheBackend end-to-end in namespace $EXT_SMOKE_NS"
kubectl create namespace "$EXT_SMOKE_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Apply the committed External CR sample.
kubectl -n "$EXT_SMOKE_NS" apply -f config/samples/cachebackend-external.yaml >/dev/null \
  || fail "kubectl apply config/samples/cachebackend-external.yaml failed"

external_spec_endpoint="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
  -o jsonpath='{.spec.endpoint}' 2>/dev/null || true)"
external_pod_labels="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
  -o go-template='{{range $k, $v := .spec.engineSelector.matchLabels}}{{printf "    %s: %s\n" $k $v}}{{end}}' \
  2>/dev/null || true)"
if [ -z "$external_spec_endpoint" ]; then
  kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" -o yaml || true
  fail "External sample did not create spec.endpoint on $EXT_SMOKE_CB_NAME"
fi
if [ -z "$external_pod_labels" ]; then
  kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" -o yaml || true
  fail "External sample did not create spec.engineSelector.matchLabels on $EXT_SMOKE_CB_NAME"
fi
log "External CR sample endpoint=$external_spec_endpoint"

# Wait for the reconciler to publish status.endpoint + observedGeneration +
# the Ready=True condition. Sub-second on a quiet cluster; the timeout covers
# leader-election warm-up.
log "waiting up to ${EXTERNAL_BACKEND_TIMEOUT}s for External CR to publish status + Ready=True"
deadline=$(($(date +%s) + EXTERNAL_BACKEND_TIMEOUT))
status_endpoint=""
observed_generation=""
metadata_generation=""
ready_status=""
ready_reason=""
until [ "$status_endpoint" = "$external_spec_endpoint" ] && \
      [ -n "$observed_generation" ] && \
      [ "$ready_status" = "True" ] && \
      [ "$ready_reason" = "ExternalEndpointAccepted" ]; do
  status_endpoint="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.status.endpoint}' 2>/dev/null || true)"
  observed_generation="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)"
  metadata_generation="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.metadata.generation}' 2>/dev/null || true)"
  ready_status="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  ready_reason="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" -o yaml || true
    fail "External CR didn't converge: status.endpoint=$status_endpoint observedGeneration=$observed_generation Ready=$ready_status/$ready_reason (want $external_spec_endpoint non-empty True/ExternalEndpointAccepted)"
  fi
  sleep 1
done
if [ "$observed_generation" != "$metadata_generation" ]; then
  kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" -o yaml || true
  fail "External CR status.observedGeneration=$observed_generation, want metadata.generation=$metadata_generation"
fi
log "External CR status: endpoint=$status_endpoint observedGeneration=$observed_generation Ready=$ready_status/$ready_reason"

# The CacheBackend printer columns are an operator-facing surface. Verify the
# table exposes the expected columns and row values instead of regressing to a
# default NAME/AGE-only table.
cb_table="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" 2>/dev/null || true)"
cb_header="$(printf '%s\n' "$cb_table" | sed -n '1p')"
for column in TYPE HEALTH MATCHED ENDPOINT PREFIXES LASTEVENT; do
  if ! grep -Eq "(^|[[:space:]])${column}([[:space:]]|$)" <<<"$cb_header"; then
    echo "$cb_table"
    fail "expected CacheBackend printer column $column in kubectl get cb output"
  fi
done
if ! grep -Fq "$EXT_SMOKE_CB_NAME" <<<"$cb_table" || \
   ! grep -Fq "External" <<<"$cb_table" || \
   ! grep -Fq "Ready" <<<"$cb_table" || \
   ! grep -Fq "$external_spec_endpoint" <<<"$cb_table"; then
  echo "$cb_table"
  fail "expected CacheBackend printer row to include name/type/health/endpoint"
fi
log "CacheBackend printer columns render Type/Health/Matched/Endpoint/Prefixes/LastEvent"

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
$external_pod_labels
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
# the scheme), otherwise prepend it. Without this a sample endpoint of
# `lm://host:port` (legal per the contract) would compare against
# `lm://lm://host:port` and the smoke would fail on a valid input.
case "$(printf '%s' "$external_spec_endpoint" | tr '[:upper:]' '[:lower:]')" in
  lm://*) expected_url="lm://${external_spec_endpoint#??://}" ;;
  *)      expected_url="lm://$external_spec_endpoint" ;;
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
  name: smoke-reject-no-endpoint
  namespace: $EXT_SMOKE_NS
spec:
  type: External
  integration: { engine: vllm }
EOF
)"
if ! grep -q "requires spec.endpoint" <<<"$reject_output"; then
  fail "admission did not reject External with no endpoint as expected; got: $reject_output"
fi

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
log "admission rejected External+missing-endpoint, External+https, External+empty-host, and non-External+endpoint"

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

# --- /policy auth assertion -------------------------------------------------
# The CachePolicy side-effect assertion above proves the authenticated write
# path works: the controller pushed the CR through /policy and the server
# enforced it on LookupRoute. The complementary half — that an
# UNAUTHENTICATED POST is rejected — is what this section checks, since
# /policy is replace-on-write and a successful rogue POST would override
# cluster-wide policy state with no audit trail. Mirror of the /snapshot probe
# above. Sends a valid PolicySnapshot body so a 400 (bad request) cannot be
# confused with a 401; the only valid outcomes are 401 (auth) or
# curl_failed:28 (NetworkPolicy drop under an enforcing CNI).
log "asserting unauthenticated /policy POST from a side pod is rejected"
SIDE_POD_POLICY="ic-policy-probe"
kubectl -n "$NAMESPACE" delete pod "$SIDE_POD_POLICY" --ignore-not-found --wait=true >/dev/null 2>&1 || true
if ! kubectl -n "$NAMESPACE" run "$SIDE_POD_POLICY" --image=curlimages/curl:8.10.1 --restart=Never \
  --command -- /bin/sh -c '
    # POST a minimal valid PolicySnapshot so any non-2xx response must be
    # an auth rejection, not a body-parse rejection. -d sets the body and
    # implies POST.
    curl -sS -m 5 -o /dev/null -w "%{http_code}" \
      -H "Content-Type: application/json" \
      -d "{\"version\":2,\"policies\":[]}" \
      http://inference-cache-server:8081/policy || echo "curl_failed:$?"
  ' >/tmp/policy-probe-create.log 2>&1; then
  cat /tmp/policy-probe-create.log >&2 || true
  fail "kubectl run $SIDE_POD_POLICY failed; cannot run /policy auth assertion"
fi

# 90s budget + describe-pod fallback matches the /snapshot probe above —
# the External-backend phase that runs earlier can leave the kubelet busy
# reaping its own Terminating pods, occasionally pushing the new pod's
# container creation above a 30s budget; without the diagnostics dump,
# a timeout would surface as an empty-log failure with no breadcrumb.
for _ in $(seq 1 90); do
  phase="$(kubectl -n "$NAMESPACE" get pod "$SIDE_POD_POLICY" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
    break
  fi
  sleep 1
done
policy_probe_out="$(kubectl -n "$NAMESPACE" logs "$SIDE_POD_POLICY" 2>/dev/null || true)"
# If the pod never finished, capture its describe output so the failure
# message tells operators why (ImagePullBackOff vs ContainerCreating vs ...).
if [ -z "$policy_probe_out" ]; then
  kubectl -n "$NAMESPACE" describe pod "$SIDE_POD_POLICY" >&2 || true
fi
kubectl -n "$NAMESPACE" delete pod "$SIDE_POD_POLICY" --grace-period=0 --force >/dev/null 2>&1 || true

# Acceptable outcomes (either gate sufficient) — see /snapshot probe above
# for the rationale on why 7 (ECONNREFUSED) is NOT accepted (an enforcing
# CNI drops, it does not RST; accepting 7 would let "listener crashed"
# pass). 204 (write succeeded unauthenticated) is the regression this
# whole ticket exists to prevent.
case "$policy_probe_out" in
  "401"|*"curl_failed:28"*)
    log "unauthenticated /policy probe rejected (probe output: $policy_probe_out)"
    ;;
  *)
    fail "unauthenticated /policy probe was not rejected (or curl failed for an unexpected reason); got: $policy_probe_out"
    ;;
esac

log "PASS — install bundle came up, CacheIndex + CacheTenant status writing, CachePolicy push adoption, gRPC fail-open, CacheBackend ↔ engine-pod binding signals + drift cadence, External backend end-to-end, and /snapshot + /policy unauth rejection all work"
