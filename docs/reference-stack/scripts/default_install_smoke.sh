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
#   3. The server's operator HTTP surface is wired on the installed Service:
#      `/readyz` returns 200, `/metrics` exposes `inferencecache_server_up 1`,
#      and `kubectl get ci` renders the CacheIndex Prefixes/Changed printer
#      columns.
#   4. The CachePolicy PUSH path works: an applied `CachePolicy` renders its
#      operator-facing printer columns, the controller pushes it to the
#      server's `/policy` endpoint, and `LookupRoute` observes the pushed
#      minimumPrefixTokens gate without engine pods or inference traffic. The
#      installed validating webhook also rejects a SECOND CachePolicy in the
#      namespace (one-per-namespace), proving the bundle's webhook Service +
#      cert-manager CA-injection path — not just envtest handler logic.
#   5. The per-CacheTenant status projection works: an applied `CacheTenant`
#      gets `.status.indexEntries=0` (observed-zero — no engine traffic in the
#      smoke) and a `Ready=True` condition written by the same poller. The
#      installed validating webhook also rejects a SECOND CacheTenant reusing
#      an existing tenantID in the namespace (tenantID-uniqueness).
#   6. The gRPC surface is reachable and PLAINTEXT by default: config/default
#      serves :9090 plaintext (TLS is opt-in — phase 12), so a plaintext client
#      lists services and a `LookupRoute` for an unknown model returns the
#      fail-open default (`reason_code: NO_HINT`).
#   7. The CacheBackend ↔ engine-pod binding surfaces operators rely on
#      actually wire up end-to-end: applying config/samples/cachebackend-
#      with-engine.yaml drives status.matchedEnginePods=1, stamps the
#      injected-by annotation on the engine pod, and surfaces the
#      InjectedByCacheBackend Event (with the persisted pod UID — the
#      regression that hides events from `kubectl describe pod`). Then
#      scaling the engine to 0 drives status.matchedEnginePods=0 via the
#      reconciler's self-RequeueAfter cadence (no CR or owned-workload
#      event needed) within ~30s, the bound on stale-Matched the
#      cadence guarantees.
#   8. The External CacheBackend type end-to-end: applying the committed
#      config/samples/cachebackend-external.yaml drives the CacheBackend
#      mutating webhook default (spec.replicas=1), renders NO
#      Deployment/Service in its namespace, status.endpoint mirrors
#      spec.endpoint, observedGeneration is set, the CR goes
#      Ready=True/ExternalEndpointAccepted, and
#      `kubectl get cb` renders the CacheBackend printer columns. A matching
#      engine pod is admitted with `LMCACHE_REMOTE_URL=lm://<spec.endpoint>`
#      injected by the pod-mutating webhook. Also exercises admission
#      validation rules (External with no endpoint, External with bad
#      endpoint shape, and non-External + endpoint are rejected at write time).
#   9. The /snapshot endpoint rejects unauthenticated callers: a side curl
#      pod outside the controller's SA identity gets either an HTTP 401 (L7
#      auth middleware) or a curl timeout (L3/L4 NetworkPolicy drop).
#  10. The /policy endpoint rejects unauthenticated callers: same side-pod
#      shape against the write-side endpoint. This is the more dangerous of
#      the two — /policy is replace-on-write, so a successful unauthenticated
#      POST would override every namespace's CachePolicy state cluster-wide.
#      The probe POSTs a valid snapshot body so the rejection cannot be
#      misattributed to a 400; the only valid outcome is 401 (auth
#      middleware) or a curl timeout (NetworkPolicy drop).
#  11. The audience binding holds on BOTH /snapshot and /policy: a probe
#      pod with the controller's SA + labels reads two mounted tokens
#      (audience-bound projected + default-audience apiserver automount)
#      and asserts the audience-bound token admits on both endpoints
#      while the default-audience token of the SAME SA is rejected on
#      both. The two endpoints share one middleware identity (one
#      controller SA, one audience), so any drift would surface on both
#      simultaneously. Catches a regression in the SERVER's audience-
#      enforcement half of the contract — `--controller-audience` flag
#      drift, the middleware forgetting to populate
#      `TokenReviewSpec.Audiences`, or the apiserver mis-enforcing
#      audience. Does NOT catch drift in the controller's production
#      projected-volume manifest (the probe deliberately uses its own
#      inline volume spec so it still runs when that manifest is broken);
#      that drift is caught by item 2 above — observedServer populates
#      only when the REAL controller's poller successfully scrapes
#      /snapshot.
#  12. The opt-in gRPC TLS path works: applying config/overlays/server-tls
#      (config/default + the config/server/tls component) rolls the server with
#      --tls-cert-file/--tls-key-file + the cert-manager Secret. After rollout,
#      a plaintext client is rejected and the cert-manager-issued chain +
#      Service-FQDN SAN VERIFY against the CA published in the serving Secret
#      (`grpcurl -cacert` with -authority <FQDN>; a wrong authority is
#      rejected) — proving server authentication, not just encryption, for the
#      overlay operators actually enable. Finally re-runs the SAME
#      LookupRoute(unknown model) the plaintext phase (6) ran and asserts the
#      identical fail-open NO_HINT, proving the existing call pattern is
#      unchanged over TLS (pure transport wrapper, no contract/behavior change).
#  13. Every sample manifest under config/samples/ applies cleanly against
#      the live install: a server-side dry-run apply of each *.yaml/*.yml
#      exercises CRD structural validation + the validating admission webhook
#      on the real cluster. Complements `make verify-samples` (which runs the
#      same assertion at envtest level) by catching admission-wiring failures
#      envtest masks — the webhook being unreachable/mis-wired on a real
#      cluster (cert-manager caBundle injection) and the CRDs as actually
#      installed by config/default. Mirrors verify-samples' sample set and
#      honors its `# verify-samples: skip` opt-out so the two gates stay in
#      lockstep. Admission-level only — does NOT create CRs, write status, or
#      hit /policy+/snapshot (no NetworkPolicy/RBAC coverage; the per-CRD
#      phases above cover those). No engine pods, no traffic.
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
#   - curl            (probes the installed HTTP surface)
#   - grpcurl         (probes the gRPC surface)
#   - kustomize       (optional; sed fallback handles the image rewrite if absent)
#
# Usage:    docs/reference-stack/scripts/default_install_smoke.sh
# Tunables: TAG, KIND_CLUSTER, NAMESPACE, CERT_MANAGER_VERSION, READY_TIMEOUT,
#           CACHEINDEX_TIMEOUT, POLICY_PUSH_TIMEOUT, HTTP_LOCAL_PORT,
#           GRPC_LOCAL_PORT, LOG_DIR, POLICY_SMOKE_NS, SAMPLE_NS,
#           SAMPLE_ENDPOINT_TIMEOUT,
#           SAMPLE_MATCH_TIMEOUT, SAMPLE_DRIFT_TIMEOUT, SAMPLE_ENGINE_IMAGE,
#           SAMPLE_CACHE_SERVER_IMAGE, EXTERNAL_BACKEND_TIMEOUT,
#           EXTERNAL_INJECT_TIMEOUT, SAMPLE_APPLY_NS.

set -euo pipefail

TAG="${TAG:-${GITHUB_SHA:-$(git rev-parse HEAD)}}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-install-smoke}"
NAMESPACE="${NAMESPACE:-inference-cache-system}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.1}"
READY_TIMEOUT="${READY_TIMEOUT:-120s}"
CACHEINDEX_TIMEOUT="${CACHEINDEX_TIMEOUT:-90}" # seconds; ~3x the 30s refresh, absorbs leader-election + first-tick jitter
POLICY_PUSH_TIMEOUT="${POLICY_PUSH_TIMEOUT:-90}" # seconds; watch-triggered push + one 30s periodic repair tick
HTTP_LOCAL_PORT="${HTTP_LOCAL_PORT:-18080}"
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
http_pf_pid=""
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
  [ -n "$http_pf_pid" ] && kill "$http_pf_pid" 2>/dev/null || true
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
for bin in docker kubectl curl grpcurl "$KIND"; do
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

# The CacheIndex table is the operator-facing view for the status poller. Check
# the installed CRD renders the Prefixes/Changed columns and that their JSONPath
# targets actually populate cells in the row instead of falling back to a
# header-only/default NAME/AGE table.
ci_table="$(kubectl get ci cluster-default 2>/dev/null || true)"
ci_header="$(printf '%s\n' "$ci_table" | sed -n '1p')"
ci_row="$(printf '%s\n' "$ci_table" | sed -n '2p')"
for column in PREFIXES CHANGED; do
  if ! grep -Eq "(^|[[:space:]])${column}([[:space:]]|$)" <<<"$ci_header"; then
    echo "$ci_table"
    fail "expected CacheIndex printer column ${column} in kubectl get output"
  fi
done
if ! grep -Fq "cluster-default" <<<"$ci_row"; then
  echo "$ci_table"
  fail "expected CacheIndex printer row to include cluster-default"
fi
ci_prefixes="$(awk 'NR==2 {print $2}' <<<"$ci_table")"
ci_changed="$(awk 'NR==2 {print $3}' <<<"$ci_table")"
if [ "$ci_prefixes" != "0" ]; then
  echo "$ci_table"
  fail "expected CacheIndex printer column Prefixes=0 in kubectl get output, got: ${ci_prefixes:-<empty>}"
fi
if [ -z "$ci_changed" ] || [ "$ci_changed" = "<none>" ] || [ "$ci_changed" = "<unknown>" ]; then
  echo "$ci_table"
  fail "expected CacheIndex printer column Changed to be populated in kubectl get output"
fi
log "CacheIndex printer columns render Prefixes=$ci_prefixes Changed=$ci_changed"

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
# CachePolicy CRD. Verify the header AND the row value — the sample
# intentionally omits spec.eviction so this also exercises the
# +kubebuilder:default=LRU marker (the default must fill the column).
cp_table="$(kubectl -n "$POLICY_SMOKE_NS" get cachepolicy cachepolicy-sample 2>/dev/null || true)"
cp_header="$(printf '%s\n' "$cp_table" | sed -n '1p')"
if ! grep -Eq "(^|[[:space:]])EVICTION([[:space:]]|$)" <<<"$cp_header"; then
  echo "$cp_table"
  fail "expected CachePolicy printer column EVICTION in kubectl get output"
fi
cp_eviction="$(kubectl -n "$POLICY_SMOKE_NS" get cachepolicy cachepolicy-sample \
  -o jsonpath='{.spec.eviction}' 2>/dev/null || true)"
if [ "$cp_eviction" != "LRU" ]; then
  echo "$cp_table"
  fail "expected .spec.eviction=LRU after the kubebuilder default fired; got '$cp_eviction'"
fi
if ! grep -Fq "cachepolicy-sample" <<<"$cp_table" || \
   ! grep -Fq "LRU" <<<"$cp_table"; then
  echo "$cp_table"
  fail "expected CachePolicy printer row to include name and Eviction=LRU"
fi
log "CachePolicy default eviction=LRU stamped, printer column renders Eviction"

# --- CachePolicy admission rejection (one-per-namespace webhook) ------------
# The installed validating webhook — served through the bundle's webhook
# Service with the cert-manager-injected CA bundle — must reject a SECOND
# CachePolicy in the namespace. Proving it on the real install (not just
# envtest) exercises the Service + cert + CA-injection path an operator's
# `kubectl apply` actually traverses: a broken cainjection annotation, a wrong
# Service selector, or a missing cert would fail here while envtest still
# passed. cachepolicy-sample already occupies $POLICY_SMOKE_NS, so this apply
# must be denied.
log "asserting a second CachePolicy in $POLICY_SMOKE_NS is rejected at admission"
second_cp_yaml="$(cat <<EOF
apiVersion: inferencecache.io/v1alpha1
kind: CachePolicy
metadata:
  name: cachepolicy-sample-2
  namespace: $POLICY_SMOKE_NS
spec: {}
EOF
)"
if cp_reject_out="$(printf '%s\n' "$second_cp_yaml" | kubectl apply -f - 2>&1)"; then
  echo "$cp_reject_out"
  fail "second CachePolicy in $POLICY_SMOKE_NS was admitted; the one-per-namespace webhook did not fire on the real install"
fi
if ! grep -q "already has CachePolicy" <<<"$cp_reject_out"; then
  echo "$cp_reject_out"
  fail "second CachePolicy was rejected, but not by the expected webhook rule (missing 'already has CachePolicy' message)"
fi
log "second CachePolicy rejected at admission by the installed validating webhook"

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

# --- CacheTenant admission rejection (tenantID-uniqueness webhook) ----------
# The installed validating webhook must reject a SECOND CacheTenant claiming an
# already-used tenantID in the same namespace. cachetenant-sample (tenantID
# tenant-a) was applied to the default namespace above, so a second tenant
# reusing tenant-a there must be denied — proving the in-cluster
# Service/cert/CA-injection path for this webhook too.
log "asserting a duplicate-tenantID CacheTenant in the default namespace is rejected at admission"
second_ct_yaml="$(cat <<'EOF'
apiVersion: inferencecache.io/v1alpha1
kind: CacheTenant
metadata:
  name: cachetenant-sample-2
spec:
  tenantID: tenant-a
EOF
)"
if ct_reject_out="$(printf '%s\n' "$second_ct_yaml" | kubectl apply -f - 2>&1)"; then
  echo "$ct_reject_out"
  fail "second CacheTenant reusing tenantID tenant-a was admitted; the uniqueness webhook did not fire on the real install"
fi
if ! grep -q "already claimed by CacheTenant" <<<"$ct_reject_out"; then
  echo "$ct_reject_out"
  fail "duplicate CacheTenant was rejected, but not by the expected webhook rule (missing 'already claimed by CacheTenant' message)"
fi
log "duplicate-tenantID CacheTenant rejected at admission by the installed validating webhook"

# --- gRPC fail-open assertion ----------------------------------------------
log "port-forwarding svc/inference-cache-server :9090 -> localhost:$GRPC_LOCAL_PORT"
mkdir -p "$LOG_DIR"
kubectl -n "$NAMESPACE" port-forward svc/inference-cache-server "$GRPC_LOCAL_PORT:9090" \
  >"$LOG_DIR/port-forward.log" 2>&1 &
pf_pid=$!

# Wait for the local port to accept connections. config/default serves :9090
# PLAINTEXT (TLS is opt-in — exercised separately against the
# config/overlays/server-tls overlay near the end of this smoke), so probe with
# -plaintext.
for _ in $(seq 1 30); do
  if grpcurl -plaintext -max-time 2 "localhost:$GRPC_LOCAL_PORT" list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# --- server HTTP surface assertion -----------------------------------------
# The installed Service also exposes the operator/observability HTTP surface on
# :8080. Probe it through kubectl port-forward so this covers Service wiring,
# the real process listeners, readiness, and Prometheus registration together.
log "port-forwarding svc/inference-cache-server :8080 -> localhost:$HTTP_LOCAL_PORT"
kubectl -n "$NAMESPACE" port-forward svc/inference-cache-server "$HTTP_LOCAL_PORT:8080" \
  >"$LOG_DIR/port-forward-http.log" 2>&1 &
http_pf_pid=$!

http_ready=0
for _ in $(seq 1 30); do
  if curl -fsS --max-time 2 "http://localhost:$HTTP_LOCAL_PORT/readyz" \
       >"$LOG_DIR/readyz.out" 2>"$LOG_DIR/readyz.err"; then
    http_ready=1
    break
  fi
  sleep 1
done
if [ "$http_ready" != "1" ]; then
  cat "$LOG_DIR/port-forward-http.log" >&2 || true
  cat "$LOG_DIR/readyz.err" >&2 || true
  fail "server /readyz did not return 200 on :8080 within 30s"
fi

if ! curl -fsS --max-time 5 "http://localhost:$HTTP_LOCAL_PORT/metrics" \
      >"$LOG_DIR/metrics.out" 2>"$LOG_DIR/metrics.err"; then
  cat "$LOG_DIR/port-forward-http.log" >&2 || true
  cat "$LOG_DIR/metrics.err" >&2 || true
  fail "server /metrics did not return 200 on :8080"
fi
if ! grep -Eq '^inferencecache_server_up[[:space:]]+1([[:space:]]|$)' "$LOG_DIR/metrics.out"; then
  grep -n 'inferencecache_server_up' "$LOG_DIR/metrics.out" >&2 || true
  fail "server /metrics did not expose inferencecache_server_up 1"
fi
log "server HTTP surface OK: /readyz returned 200 and /metrics exposes inferencecache_server_up 1"

# --- gRPC default posture assertion (plaintext) ----------------------------
# config/default serves :9090 plaintext. Assert a plaintext client can list the
# services. (We deliberately do NOT probe with a TLS client here: a TLS
# ClientHello against the plaintext HTTP/2 listener destabilizes the shared
# kubectl port-forward and breaks the probes that follow. The TLS-rejected-on-
# plaintext direction is covered by the unit tests; the opt-in TLS phase near
# the end proves the inverse — plaintext rejected once TLS is enabled.)
log "asserting gRPC default posture on :9090 (plaintext serving)"
if ! grpcurl -plaintext -max-time 5 "localhost:$GRPC_LOCAL_PORT" list \
     >"$LOG_DIR/grpcurl-plaintext-list.out" 2>&1; then
  cat "$LOG_DIR/grpcurl-plaintext-list.out" >&2 || true
  fail "expected plaintext 'grpcurl -plaintext list' to succeed against the default (plaintext) install"
fi
log "gRPC default posture OK: plaintext serving"

# Probe twice in priority order: server reflection first (what a real gateway
# client would do), proto-file second (survives the reflection registration
# being temporarily reverted or moved). The default install is plaintext, so
# these functional probes use -plaintext; TLS is verified in the opt-in phase.
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
  # Default install is plaintext (see grpcurl_lookup_route).
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
# the mutating webhook should stamp spec.replicas, the reconciler should NOT
# render a Deployment/Service, status.endpoint should mirror spec.endpoint,
# observedGeneration should advance, Ready should be True with reason
# ExternalEndpointAccepted, and a matching engine pod should come out of
# admission with LMCACHE_REMOTE_URL pointing at the operator-supplied
# endpoint. Also exercises CacheBackend printer columns and the validating
# webhook's negative path.
log "exercising External CacheBackend end-to-end in namespace $EXT_SMOKE_NS"
kubectl create namespace "$EXT_SMOKE_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Apply the committed External CR sample. The sample intentionally omits
# spec.replicas so the smoke drives the mutating webhook defaulter instead of
# only proving the CRD accepts already-defaulted YAML.
kubectl -n "$EXT_SMOKE_NS" apply -f config/samples/cachebackend-external.yaml >/dev/null \
  || fail "kubectl apply config/samples/cachebackend-external.yaml failed"

defaulted_replicas="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" \
  -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
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
if [ "$defaulted_replicas" != "1" ]; then
  kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" -o yaml || true
  fail "CacheBackend defaulter did not stamp spec.replicas: got=$defaulted_replicas (want 1)"
fi
log "External CR sample endpoint=$external_spec_endpoint; defaulted replicas=$defaulted_replicas"

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
# default NAME/AGE-only table. HEALTH was retired in favour of the Ready
# condition printer column (see this PR's design-doc carve-out); the row
# value is the condition's True string rather than the old enum value.
cb_table="$(kubectl -n "$EXT_SMOKE_NS" get cb "$EXT_SMOKE_CB_NAME" 2>/dev/null || true)"
cb_header="$(printf '%s\n' "$cb_table" | sed -n '1p')"
for column in TYPE READY MATCHED ENDPOINT PREFIXES LASTEVENT; do
  if ! grep -Eq "(^|[[:space:]])${column}([[:space:]]|$)" <<<"$cb_header"; then
    echo "$cb_table"
    fail "expected CacheBackend printer column $column in kubectl get cb output"
  fi
done
if ! grep -Fq "$EXT_SMOKE_CB_NAME" <<<"$cb_table" || \
   ! grep -Fq "External" <<<"$cb_table" || \
   ! grep -Fq "True" <<<"$cb_table" || \
   ! grep -Fq "$external_spec_endpoint" <<<"$cb_table"; then
  echo "$cb_table"
  fail "expected CacheBackend printer row to include name/type/Ready=True/endpoint"
fi
log "CacheBackend printer columns render Type/Ready/Matched/Endpoint/Prefixes/LastEvent"

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

# --- Audience-binding assertion (BOTH /snapshot and /policy) --------------
# Audience-binding follow-up to the bearer-token gate. The controller pod
# in production mounts TWO ServiceAccount tokens:
#   1. The default automount at /var/run/secrets/kubernetes.io/serviceaccount/token
#      — audience = the apiserver. Used by the controller-runtime client.
#   2. A projected volume at /var/run/secrets/inferencecache.io/controller-token/token
#      — audience = "inferencecache.io/controller". Used by the CacheIndex
#      poller AND the CachePolicy push side — they share the projected
#      token because /snapshot and /policy share one auth middleware.
# The server passes TokenReviewSpec.Audiences=["inferencecache.io/controller"]
# on every review for BOTH endpoints, so a default-audience token MUST
# come back 401 on BOTH even though the SA identity (controller-manager)
# would otherwise be admitted.
#
# Why a single probe pod with FOUR scrapes (audience-bound × {snapshot,
# policy} ∪ default-audience × {snapshot, policy}): the two endpoints share
# one middleware identity, so audience drift would surface on both
# simultaneously — but a regression in JUST ONE direction (e.g. the policy
# handler skipping the auth wrapper) would show up here too. Four small
# checks, one pod, one assertion: "all four outcomes match the contract."
#
# Scoping — what each smoke gate actually catches (the assertions are
# complementary, not redundant):
#   - The CacheIndex assertion earlier (cacheindex/cluster-default.status
#     .observedServer populates within ~60s) is what proves the REAL
#     controller's projected-volume manifest, the controller binary's
#     BearerTokenPath, the server's flag, and the middleware all agree
#     end-to-end. If config/manager/manager.yaml drifts (audience renamed,
#     mount path moved, expirationSeconds zeroed), the real poller's
#     scrape returns 401 and the CR's observedServer stays empty, failing
#     that earlier gate.
#   - THIS probe asserts only server-side behavior: that an audience-bound
#     token admits and a default-audience token of the same SA is rejected,
#     on BOTH endpoints. It uses an inline duplicate volume spec so it can
#     run even if the controller's manifest is broken (which would
#     otherwise mask the server-side check). It does NOT catch drift in
#     config/manager/manager.yaml; that's the earlier gate's job.
log "asserting audience binding on both /snapshot and /policy"
PROBE_POD="ic-audience-probe"
kubectl -n "$NAMESPACE" delete pod "$PROBE_POD" --ignore-not-found --wait=true >/dev/null 2>&1 || true

probe_yaml=$(cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $PROBE_POD
  namespace: $NAMESPACE
  labels:
    app.kubernetes.io/name: inference-cache
    app.kubernetes.io/component: controller
spec:
  serviceAccountName: inference-cache-controller-manager
  restartPolicy: Never
  containers:
  - name: probe
    image: curlimages/curl:8.10.1
    command: ["/bin/sh", "-c"]
    args:
    - |
      # The probe pod IS in the NetworkPolicy allowlist (component=controller
      # label), so we expect no L3/L4 drop — but a short -m timeout + explicit
      # curl-exit capture keeps a regression (DNS, Service rename, allowlist
      # drift) from looking like a silent bad outcome. If curl fails on any
      # of the four scrapes, we emit "K=curl_failed:N" so the case statement
      # below surfaces the exit code, not the empty-status default.
      controller_token=\$(cat /var/run/secrets/inferencecache.io/controller-token/token 2>/dev/null || echo "")
      default_token=\$(cat /var/run/secrets/kubernetes.io/serviceaccount/token 2>/dev/null || echo "")
      if [ -z "\$controller_token" ]; then echo "controller_token_missing"; exit 0; fi
      if [ -z "\$default_token" ]; then echo "default_token_missing"; exit 0; fi
      # GET /snapshot — controller-audience must 200, default-audience must 401.
      sa_ctrl=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$controller_token" "http://inference-cache-server:8081/snapshot" || echo "curl_failed:\$?")
      sa_def=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$default_token" "http://inference-cache-server:8081/snapshot" || echo "curl_failed:\$?")
      # POST /policy — controller-audience must 204, default-audience must 401.
      # Body is a minimal valid PolicySnapshot so any non-2xx is auth-side, not body-parse.
      pa_ctrl=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$controller_token" -H "Content-Type: application/json" -d '{"version":2,"policies":[]}' "http://inference-cache-server:8081/policy" || echo "curl_failed:\$?")
      pa_def=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$default_token" -H "Content-Type: application/json" -d '{"version":2,"policies":[]}' "http://inference-cache-server:8081/policy" || echo "curl_failed:\$?")
      echo "snapshot_ctrl=\$sa_ctrl snapshot_def=\$sa_def policy_ctrl=\$pa_ctrl policy_def=\$pa_def"
    volumeMounts:
    - name: controller-token
      mountPath: /var/run/secrets/inferencecache.io/controller-token
      readOnly: true
  volumes:
  - name: controller-token
    projected:
      sources:
      - serviceAccountToken:
          path: token
          audience: inferencecache.io/controller
          expirationSeconds: 3600
EOF
)
if ! echo "$probe_yaml" | kubectl apply -f - >/tmp/audience-probe-create.log 2>&1; then
  cat /tmp/audience-probe-create.log >&2 || true
  fail "kubectl apply for $PROBE_POD failed; cannot run audience-binding assertion"
fi

# Wait for the probe pod to finish; 90s budget + describe-pod fallback
# matches the unauth probes above for the same kubelet-busy-reaping reason.
for _ in $(seq 1 90); do
  phase="$(kubectl -n "$NAMESPACE" get pod "$PROBE_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
    break
  fi
  sleep 1
done
audience_probe="$(kubectl -n "$NAMESPACE" logs "$PROBE_POD" 2>/dev/null || true)"
if [ -z "$audience_probe" ]; then
  kubectl -n "$NAMESPACE" describe pod "$PROBE_POD" >&2 || true
fi
kubectl -n "$NAMESPACE" delete pod "$PROBE_POD" --grace-period=0 --force >/dev/null 2>&1 || true

log "audience probe output: $audience_probe"
# Expected outcome line, in order:
#   snapshot_ctrl=200 snapshot_def=401 policy_ctrl=204 policy_def=401
# Anything else is a regression. curl_failed:28 splits out so an operator
# triaging a red smoke knows whether to look at NetworkPolicy (timeout) vs
# Service/listener (other curl exit).
case "$audience_probe" in
  "snapshot_ctrl=200 snapshot_def=401 policy_ctrl=204 policy_def=401")
    log "audience binding verified on both endpoints — controller-audience token admitted, default-audience token rejected on /snapshot and /policy"
    ;;
  *controller_token_missing*)
    fail "probe pod is missing /var/run/secrets/inferencecache.io/controller-token/token — the projected volume did not mount; check the probe-pod manifest above (and config/manager/manager.yaml for the production analog)"
    ;;
  *default_token_missing*)
    fail "probe pod is missing the default automount; cannot run the audience-binding negative case"
    ;;
  *"curl_failed:28"*)
    fail "audience-binding probe timed out reaching :8081 (curl -m 5 fired). Likely NetworkPolicy regression — does the probe pod still match the controller's component=controller selector? Probe output: $audience_probe"
    ;;
  *"curl_failed:"*)
    fail "audience-binding probe could not connect to the controller-facing listener (curl exited non-zero). Check Service name 'inference-cache-server', port 8081, and that the listener is up. Probe output: $audience_probe"
    ;;
  *)
    fail "audience-binding probe got unexpected outcome: $audience_probe (want 'snapshot_ctrl=200 snapshot_def=401 policy_ctrl=204 policy_def=401')"
    ;;
esac

# --- opt-in gRPC TLS overlay verification ----------------------------------
# config/default is plaintext; Service TLS is an opt-in overlay
# (config/overlays/server-tls = config/default + the config/server/tls
# component). Apply it on top of the running install, which patches the server
# Deployment with --tls-cert-file/--tls-key-file + the cert-manager Secret
# volume and ships the Issuer + Certificate. After the rollout, verify the
# cert-manager-issued chain + Service-FQDN SAN actually authenticate the server
# (not just encrypt): pull ca.crt from the serving Secret and run
# `grpcurl -cacert <ca.crt> -authority <Service FQDN>` (grpcurl uses -authority
# as the verification name even though the port-forward terminates at
# localhost). A wrong authority must fail, and plaintext must be rejected.
log "verifying opt-in TLS overlay (config/overlays/server-tls)"
kubectl apply -k "$tmpdir/config/overlays/server-tls" >/dev/null \
  || fail "kubectl apply -k config/overlays/server-tls failed"
# The patched pod stays Pending until cert-manager mints the Secret from the
# Certificate the overlay just applied; rollout status blocks until it's served.
if ! kubectl -n "$NAMESPACE" rollout status deploy/inference-cache-server --timeout=150s; then
  kubectl -n "$NAMESPACE" get pod -l app.kubernetes.io/component=server -o wide || true
  kubectl -n "$NAMESPACE" describe certificate inference-cache-server-serving-cert || true
  fail "server Deployment did not roll out with TLS within 150s (cert-manager Secret not minted?)"
fi

# Re-establish the port-forward against the freshly rolled (TLS) pod; the old
# forward pointed at the now-terminated plaintext pod.
kill "$pf_pid" 2>/dev/null || true
TLS_LOCAL_PORT="$((GRPC_LOCAL_PORT + 1))"
kubectl -n "$NAMESPACE" port-forward svc/inference-cache-server "$TLS_LOCAL_PORT:9090" \
  >"$LOG_DIR/port-forward-tls.log" 2>&1 &
pf_pid=$!
tls_ready=0
for _ in $(seq 1 30); do
  if grpcurl -insecure -max-time 2 "localhost:$TLS_LOCAL_PORT" list >/dev/null 2>&1; then
    tls_ready=1
    break
  fi
  sleep 1
done
# Assert the TLS port-forward actually came up before the real checks run — an
# explicit failure here points at the forward / TLS listener rather than letting
# the -cacert assertion below fail with a murkier "connection refused".
if [ "$tls_ready" != "1" ]; then
  cat "$LOG_DIR/port-forward-tls.log" >&2 || true
  fail "TLS port-forward to :9090 never accepted a connection (grpcurl -insecure failed for 30s after the overlay rollout)"
fi

if grpcurl -plaintext -max-time 5 "localhost:$TLS_LOCAL_PORT" list \
     >"$LOG_DIR/grpcurl-tls-plaintext.out" 2>&1; then
  cat "$LOG_DIR/grpcurl-tls-plaintext.out" >&2 || true
  fail "expected plaintext to be REJECTED once the TLS overlay is applied"
fi
ca_file="$LOG_DIR/server-ca.crt"
kubectl -n "$NAMESPACE" get secret inference-cache-server-tls \
  -o jsonpath='{.data.ca\.crt}' 2>/dev/null | base64 -d > "$ca_file" || true
if [ ! -s "$ca_file" ]; then
  kubectl -n "$NAMESPACE" get secret inference-cache-server-tls -o yaml || true
  fail "serving Secret inference-cache-server-tls has no usable ca.crt — clients could not authenticate the server (encryption only)"
fi
server_fqdn="inference-cache-server.${NAMESPACE}.svc.cluster.local"
if ! grpcurl -cacert "$ca_file" -authority "$server_fqdn" -max-time 5 \
     "localhost:$TLS_LOCAL_PORT" list >"$LOG_DIR/grpcurl-cacert-list.out" 2>&1; then
  cat "$LOG_DIR/grpcurl-cacert-list.out" >&2 || true
  fail "expected TLS verification with the cert-manager CA against $server_fqdn to succeed"
fi
if grpcurl -cacert "$ca_file" -authority "wrong.example.invalid" -max-time 5 \
     "localhost:$TLS_LOCAL_PORT" list >"$LOG_DIR/grpcurl-cacert-badname.out" 2>&1; then
  cat "$LOG_DIR/grpcurl-cacert-badname.out" >&2 || true
  fail "expected TLS verification with a wrong authority to FAIL (the Service-FQDN SAN must be enforced)"
fi
log "opt-in TLS overlay OK: plaintext rejected; cert-manager CA verifies the server cert for $server_fqdn (wrong name rejected)"

# Backward-compatibility: the EXISTING call pattern must work UNCHANGED over
# TLS. Re-run the same LookupRoute(unknown model) the plaintext phase ran (phase
# 5) and assert the identical fail-open result (reason_code=NO_HINT) — proving
# TLS is a pure transport wrapper that does not alter the gRPC contract or
# handler behavior, so a client only swaps plaintext creds for TLS creds.
# Reflection first, proto-file fallback (same priority order as the plaintext
# probe in grpcurl_lookup_route).
tls_lookup_payload='{"modelId":"install-smoke-unknown"}'
tls_lookup_resp="$(grpcurl -cacert "$ca_file" -authority "$server_fqdn" -max-time 5 -d "$tls_lookup_payload" \
    "localhost:$TLS_LOCAL_PORT" inferencecache.v1alpha1.InferenceCache/LookupRoute 2>"$LOG_DIR/grpcurl-tls-lookup.err" \
  || grpcurl -cacert "$ca_file" -authority "$server_fqdn" -max-time 5 \
       -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto -d "$tls_lookup_payload" \
       "localhost:$TLS_LOCAL_PORT" inferencecache.v1alpha1.InferenceCache/LookupRoute 2>>"$LOG_DIR/grpcurl-tls-lookup.err")"
if [ -z "$tls_lookup_resp" ]; then
  cat "$LOG_DIR/grpcurl-tls-lookup.err" >&2 || true
  fail "LookupRoute over TLS returned no response — the existing call pattern broke once TLS was enabled"
fi
if ! has_reason_code "$tls_lookup_resp" "NO_HINT"; then
  fail "LookupRoute over TLS did not return the fail-open NO_HINT the plaintext path returns (TLS altered handler behavior): $tls_lookup_resp"
fi
log "existing call pattern intact over TLS: LookupRoute(unknown model) → NO_HINT, identical to the plaintext phase"

# --- sample-manifest apply-clean backstop ----------------------------------
# Every YAML under config/samples/ must apply cleanly against the running
# install. Operators copy these as their first-contact recipe; a sample that
# rejects at admission (names a (runtime, type) pair no shipped adapter
# supports, populates a field the schema no longer accepts) burns trust on
# first contact.
#
# Envtest-level sample validation already lives in `make verify-samples`
# (it runs every sample through admission against an in-process apiserver +
# the CacheBackend webhook). This phase is the live-kind-cluster complement:
# it exercises the SAME samples against the real default install via
# server-side dry-run, so it additionally catches admission-path failures
# that envtest's self-managed apiserver + webhook certs mask — the CacheBackend
# validating webhook being unreachable or mis-wired on a real cluster
# (cert-manager-injected caBundle, Service routing, failurePolicy), and the
# CRDs as actually installed by `kubectl apply -k config/default` rather than
# from envtest's CRDDirectoryPaths. Belt-and-suspenders against sample drift
# in the actually-deployed scenario.
#
# Scope note: --dry-run=server stops at apiserver admission. It does NOT
# create CRs, drive controllers, write status, or hit the /policy + /snapshot
# HTTP endpoints — so it exercises no NetworkPolicy or status-write RBAC (the
# earlier per-CRD behavioral phases cover those). This is a CRD + admission-
# wiring backstop, nothing more.
#
# Runs LAST: the per-CRD phases above (External backend, CachePolicy push,
# binding signals) assert behavioral regressions first; this generic
# apply-clean loop is the catch-all backstop. Server-side dry-run exercises
# CRD structural validation AND the validating admission webhook without
# persisting any CRs (nothing to clean up afterwards); the only cluster side
# effect is one transient namespace, created below and deleted at the end so
# namespaced samples have a target. Spins up no engine pods (admission-level
# only, no traffic).
#
# Honors the same opt-out as `make verify-samples`: a sample whose top-of-file
# comment block contains a line equal to `# verify-samples: skip` is reported
# as SKIP and not applied. Keeping the two gates' sample sets in lockstep
# means a sample intentionally excluded from one is excluded from both.
log "asserting every config/samples/ manifest applies cleanly against the live install (server dry-run)"
SAMPLE_APPLY_NS="${SAMPLE_APPLY_NS:-ic-sample-apply}"
kubectl create namespace "$SAMPLE_APPLY_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# sample_skip_marker / has_skip_marker mirror hack/verify-samples'
# hasSkipMarker: scan only the leading comment block (blank + '#'-prefixed
# lines), stop at the first non-comment line, and match the marker exactly
# after trimming surrounding whitespace. Defined here (not at the top) to keep
# the backstop self-contained.
sample_skip_marker="# verify-samples: skip"
has_skip_marker() {
  local f="$1" line trimmed
  while IFS= read -r line || [ -n "$line" ]; do
    trimmed="${line#"${line%%[![:space:]]*}"}"        # ltrim
    trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"  # rtrim
    [ -z "$trimmed" ] && continue
    case "$trimmed" in
      "#"*) [ "$trimmed" = "$sample_skip_marker" ] && return 0 ;;
      *) return 1 ;;  # first non-comment line: marker can no longer appear
    esac
  done < "$f"
  return 1
}

# Enumerate the same sample set as `make verify-samples` (hack/verify-samples'
# listSamples): every regular *.yaml / *.yml under config/samples, recursively,
# sorted for deterministic output. Tracking its selection keeps the two gates
# in lockstep — a future .yml or subdirectory sample can't be covered by the
# envtest gate yet silently skipped by this live-cluster one. (config/samples
# holds only regular files; symlinked samples — which `find -type f` skips and
# Go's filepath.Walk would include — are not used here, so the sets match.)
#
# Materialize the list FIRST, with an explicit error check, rather than piping
# find straight into the loop via process substitution: a process
# substitution's exit status is discarded, so `set -o pipefail` can't observe a
# find failure, and a traversal that errored after emitting some files would
# slip past the zero-match guard below as partial coverage. Failing here means
# the coverage gate never silently passes on a partial walk. (pipefail is set
# at the top of the script, so the command substitution sees find's status.)
sample_list="$(find config/samples -type f \( -name '*.yaml' -o -name '*.yml' \) | sort)" \
  || fail "could not enumerate config/samples manifests (find failed) — refusing to report partial sample coverage"
sample_ok=0
sample_skip=0
sample_fail=0
while IFS= read -r f; do
  [ -n "$f" ] || continue  # skip the lone empty line an empty here-string yields
  if has_skip_marker "$f"; then
    log "  SKIP $f (opt-out: $sample_skip_marker)"
    sample_skip=$((sample_skip + 1))
    continue
  fi
  # cacheindex is cluster-scoped; -n is a harmless no-op for it. Everything
  # else under config/samples is namespace-scoped, so a dedicated namespace
  # keeps each sample's default ObjectMeta from colliding with earlier phases'
  # fixtures. --dry-run=server persists nothing, so no teardown is needed.
  # --request-timeout bounds a hung apply (mirrors verify-samples'
  # perSampleTimeout) so a stuck apiserver/admission webhook fails THIS sample
  # fast — surfacing its filename via the rejection branch below — instead of
  # stalling CI until the workflow timeout with no in-flight breadcrumb.
  if kubectl apply --dry-run=server --request-timeout=30s -n "$SAMPLE_APPLY_NS" -f "$f" \
       >/tmp/sample-dry-run.log 2>&1; then
    log "  OK   $f"
    sample_ok=$((sample_ok + 1))
  else
    echo "[install-smoke] sample $f did not apply cleanly:" >&2
    cat /tmp/sample-dry-run.log >&2
    sample_fail=$((sample_fail + 1))
  fi
done <<< "$sample_list"
kubectl delete namespace "$SAMPLE_APPLY_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true

# Guard against the backstop silently becoming a no-op if config/samples ends
# up empty or unreadable. The recursive find above tracks `make verify-samples`,
# so layout changes (subdirs, .yml) stay covered; this only catches the
# degenerate "no samples at all" case.
if [ "$((sample_ok + sample_skip + sample_fail))" -eq 0 ]; then
  fail "no *.yaml/*.yml manifests found under config/samples — the apply-clean backstop covered nothing (is the sample directory missing or empty?)"
fi
if [ "$sample_fail" -ne 0 ]; then
  fail "$sample_fail config/samples/ manifest(s) did not apply cleanly against the live install — see the rejection output above"
fi
log "all config/samples/ manifests applied cleanly ($sample_ok ok, $sample_skip skipped; server dry-run)"

log "PASS — install bundle came up, CacheIndex + CacheTenant status writing, server HTTP surface, CachePolicy push adoption, gRPC fail-open (plaintext default), CacheBackend ↔ engine-pod binding signals + drift cadence, External backend end-to-end, /snapshot + /policy unauth rejection, audience binding on both endpoints, the opt-in gRPC TLS overlay (incl. the existing LookupRoute call pattern over TLS), and every config/samples/ manifest applies cleanly — all work"
