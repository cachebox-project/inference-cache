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
#      server's `/policy` endpoint, and `LookupRoute` observes ALL THREE
#      policy enforcement paths without engine pods or inference traffic:
#      the pushed `minimumPrefixTokens` request-side gate, the pushed
#      `minimumMatchedTokens` per-replica result-side floor, and the
#      pushed `routingFloorScore` whole-response score floor. Three
#      orthogonal lookups exercise the first two (above both, below the
#      request-side gate, sub-floor realized match); a follow-up patch +
#      re-lookup pair exercises the routingFloorScore propagation and
#      replace-on-write semantics. The installed validating webhook also
#      rejects a SECOND CachePolicy in the namespace (one-per-namespace),
#      proving the bundle's webhook Service + cert-manager CA-injection
#      path — not just envtest handler logic.
#   5. The per-CacheTenant status projection works: an applied `CacheTenant`
#      gets `.status.indexEntries=0` (observed-zero — no engine traffic in the
#      smoke) and a `Ready=True` condition written by the same poller. The
#      installed validating webhook also rejects a SECOND CacheTenant reusing
#      an existing tenantID in the namespace (tenantID-uniqueness).
#   6. PromptTemplate + PDTopology are schema-only in the default install
#      today: the manager registers their CRDs/RBAC but no status-writing
#      reconciler. The smoke applies committed samples and asserts
#      `kubectl get pt` / `kubectl get pdt` render their operator-facing
#      printer columns.
#   7. The gRPC surface is reachable and PLAINTEXT by default: config/default
#      serves :9090 plaintext (TLS is opt-in — phase 13), so a plaintext client
#      lists services and a `LookupRoute` for an unknown model returns the
#      fail-open default (`reason_code: NO_HINT`).
#   8. The CacheBackend ↔ engine-pod binding surfaces operators rely on
#      actually wire up end-to-end: applying config/samples/cachebackend-
#      with-engine.yaml drives status.matchedEnginePods=1, stamps the
#      injected-by annotation on the engine pod, and surfaces the
#      InjectedByCacheBackend Event (with the persisted pod UID — the
#      regression that hides events from `kubectl describe pod`). Then
#      the cache-server restart cascade: force-deleting the
#      cache-server pod flips status.observedServerInstance to the
#      replacement's server-instance identifier and patches the cascade-restart-trigger
#      annotation onto the engine Deployment's pod template (the
#      mechanism that drives the rolling restart). Finally scaling the
#      engine to 0 drives status.matchedEnginePods=0 via the
#      reconciler's self-RequeueAfter cadence (no CR or owned-workload
#      event needed) within ~30s, the bound on stale-Matched the
#      cadence guarantees.
#   8b. CacheBackend.spec.resources defaults + threading: the CRD-schema
#      default stamps spec.resources.limits.memory on every admitted
#      CacheBackend (so the cache-server pod is bounded by the cgroup
#      rather than node-pressure OOM-killed by the kubelet under T2
#      write load — the failure mode that surfaced in the Phase-2
#      benchmark), and the controller threads the value into the
#      rendered Deployment container. The smoke asserts BOTH ends of
#      the contract against the real kubectl-installed bundle — the
#      CR's spec carries the default AND the rendered pod template's
#      container shows the same limit — because either half-failing
#      reintroduces the regression.
#   9. The External CacheBackend type end-to-end: applying the committed
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
#   9b. The Events-only CacheBackend mode (spec.integration.mode=EventsOnly)
#      end-to-end: applying an events-only LMCache CacheBackend renders NO owned
#      Deployment/Service, keeps status.endpoint empty, latches no
#      firstKVEventObservedAt (no subscriber image wired → no KV events), and
#      parks the CR at Ready=False/AwaitingFirstKVEvent via the same KV-event
#      gate as a managed backend. The managed-only conditions (FunctionalProbeOK
#      / EngineKernelsHealthy / T2Degraded / EngineCompatibility) are absent.
#      Also exercises the validating webhook's EventsOnly+External rejection.
#  10. The /snapshot endpoint rejects unauthenticated callers: a side curl
#      pod outside the controller's SA identity gets either an HTTP 401 (L7
#      auth middleware) or a curl timeout (L3/L4 NetworkPolicy drop).
#  11. The /policy endpoint rejects unauthenticated callers: same side-pod
#      shape against the write-side endpoint. This is the more dangerous of
#      the two — /policy is replace-on-write, so a successful unauthenticated
#      POST would override every namespace's CachePolicy state cluster-wide.
#      The probe POSTs a valid snapshot body so the rejection cannot be
#      misattributed to a 400; the only valid outcome is 401 (auth
#      middleware) or a curl timeout (NetworkPolicy drop).
#  11b. The /probe endpoint rejects unauthenticated callers: same side-pod
#      shape against the functional-self-test endpoint. /probe shares the
#      controller ServiceAccount identity with /snapshot and /policy, so a regression
#      that wired /probe outside that profile would let any pod that can
#      reach :8081 drive a synthetic round-trip AND, since the CacheBackend
#      reconciler now consumes the result to publish FunctionalProbeOK and
#      downgrade Ready, observe or trigger forged Ready transitions on
#      every managed backend. Sends a valid ProbeRequest body so the
#      rejection cannot be misattributed to a 400; valid outcomes are
#      401 / NetworkPolicy drop.
#  12. The audience binding holds on /snapshot, /policy, AND /probe: a probe
#      pod with the controller's SA + labels reads three tokens
#      (controller-audience projected, policy-audience projected, and the
#      default-audience apiserver automount). It asserts the controller token
#      admits on /snapshot + /probe, the policy token admits on /policy, and
#      the default-audience token of the SAME SA is rejected everywhere; it
#      also asserts the controller token cannot push /policy. Catches a
#      regression in the SERVER's audience-enforcement half of the contract —
#      `--controller-audience` / `--policy-audience` flag drift, the
#      middleware forgetting to populate `TokenReviewSpec.Audiences`, or the
#      apiserver mis-enforcing audience. Does NOT catch drift in the
#      controller's production projected-volume manifest (the probe
#      deliberately uses its own inline volume specs so it still runs when
#      that manifest is broken); that drift is caught by item 2 above —
#      observedServer populates only when the REAL controller's poller
#      successfully scrapes /snapshot, and the CachePolicy adoption assertion
#      passes only when the REAL controller's policy pusher reaches /policy.
#  12b. The authenticated /probe handler returns the expected default
#       posture on a clean install: a controller-SA-authenticated POST
#       gets HTTP 200 AND the parsed JSON body asserts ingest=ok,
#       routing=ok, t2=skipped (no T2Prober is wired into the server
#       today, so Stage C always reports skipped). A regression where
#       the handler returns 200 with a per-stage "failed" would
#       otherwise slip past the audience-binding phase above (which only
#       checks HTTP status).
#  12c. The CacheTenant admission webhook rejects a CR claiming the
#       server-reserved probe tenantID (inferencecache.io/probe). Pairs
#       with the existing duplicate-tenantID assertion to pin BOTH
#       CacheTenant validation rules end-to-end against the real
#       installed webhook.
#  13. The opt-in gRPC TLS path works: applying config/overlays/server-tls
#      (config/default + the config/server/tls component) rolls the server with
#      --tls-cert-file/--tls-key-file + the cert-manager Secret. After rollout,
#      a plaintext client is rejected and the cert-manager-issued chain +
#      Service-FQDN SAN VERIFY against the CA published in the serving Secret
#      (`grpcurl -cacert` with -authority <FQDN>; a wrong authority is
#      rejected) — proving server authentication, not just encryption, for the
#      overlay operators actually enable. Finally re-runs the SAME
#      LookupRoute(unknown model) the plaintext phase (7) ran and asserts the
#      identical fail-open NO_HINT, proving the existing call pattern is
#      unchanged over TLS (pure transport wrapper, no contract/behavior change).
#  14. The LMCache kernel-check injection shape is correct end-to-end: a
#      GPU-requesting engine pod (labeled app=kc-inject-engine, bound to a
#      dedicated LMCache CacheBackend) is admitted and carries a
#      lmcache-kernel-check init container whose image EQUALS the engine
#      container's image (the adapter reuses it so no extra image pull
#      occurs). Exercises the mutating pod webhook's auto mode (inject
#      iff GPU requested) end-to-end on the real installed bundle.
#  15. The report-only FAIL condition path works fail-open: a dedicated
#      LMCache CacheBackend (kc-cond) is annotated report-only, and a
#      matching engine pod using python:3.11-slim runs the kernel-check
#      init container, which exits 0 (fail-open) but writes "FAIL: lmcache
#      not importable" to /dev/termination-log. The main container starts
#      normally (pod Ready), proving report-only did not block the engine.
#      The C2 reconciler reads the termination message and publishes
#      EngineKernelsHealthy=False / reason=KernelLoadFailed on the
#      CacheBackend status. The validating webhook also rejects an invalid
#      lmcache-kernel-check annotation value (a typo would otherwise silently
#      relax strict enforcement to report-only).
#  16. Every sample manifest under config/samples/ applies cleanly against
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
#  17. The operator `inferencecache doctor` CLI runs end-to-end against the live
#      install: build the binary, apply a CacheBackend, run the config-only
#      checks, and assert it emits the documented JSON envelope, surfaces a
#      CacheBackend (CB0xx) finding, and exits with a code matching the reported
#      summary.exitCode (the CI-gating contract).
#  18. The managed Mooncake backend reconciles end-to-end: a busybox
#      `mooncake_master` stand-in (accepts TCP on the RPC port so the rendered
#      readiness probe passes — the real kvcacheai/mooncake image is NOT pulled)
#      lets `CacheBackend{type: Mooncake}` reach an Available Deployment, with
#      `status.endpoint=<svc>:50051` and the Service's first port = the RPC port.
#      Proves the real installed controller selects the vLLM/Mooncake adapter and
#      renders the mooncake_master workload via ResolveCacheServer; the real
#      engine-over-mooncakestore:// path stays for the Mooncake reference stack.
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
#           PROMPT_TOPOLOGY_SMOKE_NS, SAMPLE_ENDPOINT_TIMEOUT,
#           SAMPLE_MATCH_TIMEOUT, SAMPLE_DRIFT_TIMEOUT,
#           SAMPLE_CASCADE_TIMEOUT, SAMPLE_ENGINE_IMAGE,
#           SAMPLE_CACHE_SERVER_IMAGE, EXTERNAL_BACKEND_TIMEOUT,
#           EXTERNAL_INJECT_TIMEOUT, EVENTSONLY_BACKEND_TIMEOUT,
#           EVENTSONLY_SMOKE_NS, EVENTSONLY_SMOKE_CB_NAME, SAMPLE_APPLY_NS,
#           KERNEL_CHECK_SMOKE_NS, KERNEL_CHECK_POD_TIMEOUT,
#           KERNEL_CHECK_COND_TIMEOUT, MOONCAKE_SMOKE_NS, MOONCAKE_MASTER_IMAGE.

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

# Events-only smoke tunable. An events-only backend provisions no workload, so
# the only wait is the reconciler latching status.firstAvailableAt and the
# KV-event gate publishing Ready=False/AwaitingFirstKVEvent — a sub-second
# server-less reconcile; the budget covers APIReader warm-up + leader-election.
EVENTSONLY_BACKEND_TIMEOUT="${EVENTSONLY_BACKEND_TIMEOUT:-30}" # seconds

# Kernel-check smoke tunables (assertions 14 + 15).
# KERNEL_CHECK_SMOKE_NS is a dedicated namespace created + deleted by those
# two phases so they don't leave fixtures in other namespaces.
KERNEL_CHECK_SMOKE_NS="${KERNEL_CHECK_SMOKE_NS:-ic-smoke-kernel-check}"
# Budget for the report-only engine pod to become Ready (init container runs
# python:3.11-slim; the image pull dominates on a cold node but is small).
KERNEL_CHECK_POD_TIMEOUT="${KERNEL_CHECK_POD_TIMEOUT:-120}"
# Budget for the C2 reconciler to read the init-container termination message
# and publish EngineKernelsHealthy=False. One reconcile cycle + poll buffer.
KERNEL_CHECK_COND_TIMEOUT="${KERNEL_CHECK_COND_TIMEOUT:-60}"

# Sample-smoke tunables — apply config/samples/cachebackend-with-engine.yaml,
# assert the operator-facing signals, exercise the RequeueAfter drift case.
#
# Default namespace is dedicated to this smoke so re-runs against an existing
# cluster (KEEP_CLUSTER=1) don't mutate or delete a developer's own resources
# in `default`. The script creates the namespace on entry and deletes it on
# the way out.
SAMPLE_NS="${SAMPLE_NS:-cb-engine-smoke}"
POLICY_SMOKE_NS="${POLICY_SMOKE_NS:-ic-smoke-policy}"
PROMPT_TOPOLOGY_SMOKE_NS="${PROMPT_TOPOLOGY_SMOKE_NS:-ic-smoke-prompt-topology}"
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
# Cache-server restart cascade. Each wait covers a different leg of
# the loop: the controller observing the replacement cache-server pod
# and computing its server-instance identifier
# (`<pod-uid>:<restart-sum>`), then patching the engine Deployment's
# pod template annotations. 60s absorbs the cache-server pod's
# recreate-and-Ready cycle (the busybox stand-in starts in a few
# seconds; the wait dominates on a cold node).
SAMPLE_CASCADE_TIMEOUT="${SAMPLE_CASCADE_TIMEOUT:-60}"
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
# Deployment can become Available without pulling lmcache/standalone:v0.4.7.
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

# Events-only-backend smoke fixture identifiers. Declared up front so the
# diagnostics helper can reference them even if the smoke aborts before the
# events-only section creates the objects.
EVENTSONLY_SMOKE_NS="${EVENTSONLY_SMOKE_NS:-ic-smoke-events-only}"
EVENTSONLY_SMOKE_CB_NAME="${EVENTSONLY_SMOKE_CB_NAME:-cachebackend-events-only}"

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
  kubectl get prompttemplates -A -o yaml \
    >"$LOG_DIR/prompttemplates.yaml" 2>&1 || true
  kubectl get pdtopologies -A -o yaml \
    >"$LOG_DIR/pdtopologies.yaml" 2>&1 || true
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
  # Events-only-backend smoke artefacts. Best-effort — the CR may not exist if
  # the smoke aborted before that section.
  kubectl get cb -n "$EVENTSONLY_SMOKE_NS" "$EVENTSONLY_SMOKE_CB_NAME" -o yaml \
    >"$LOG_DIR/events-only-cb.yaml" 2>&1 || true
  kubectl get deploy,svc -n "$EVENTSONLY_SMOKE_NS" \
    >"$LOG_DIR/events-only-ns-workloads.txt" 2>&1 || true
  # Kernel-check smoke artefacts. Best-effort — the objects may not
  # exist if the smoke aborted before that section.
  kubectl get cb -n "$KERNEL_CHECK_SMOKE_NS" -o yaml \
    >"$LOG_DIR/kernel-check-cachebackends.yaml" 2>&1 || true
  kubectl get pod -n "$KERNEL_CHECK_SMOKE_NS" -o yaml \
    >"$LOG_DIR/kernel-check-pods.yaml" 2>&1 || true
  kubectl get events.events.k8s.io -n "$KERNEL_CHECK_SMOKE_NS" \
    >"$LOG_DIR/kernel-check-events.txt" 2>&1 || true
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

# --- server resources sized for DefaultMaxEntries ---------------------------
# The default install MUST budget enough memory to actually hold the
# DefaultMaxEntries=1,000,000 cap; without that, the default cap is a
# meaningless number — operators would OOM well before reaching it. The
# sizing-guide measurements (docs/operations/index-sizing.md) put 1M
# entries at ~540 MiB peak RSS, so the limit lives at 1Gi. Asserting on
# the live Deployment proves the bundle still ships that resource shape —
# the smoke would catch a future refactor that "simplified" the limit
# back to its old 256Mi value, which would silently re-introduce the
# OOM-below-cap discrepancy.
server_mem_limit=$(kubectl -n "$NAMESPACE" get deployment/inference-cache-server \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="server")].resources.limits.memory}' \
  2>/dev/null || true)
# Normalize to bytes so the assertion catches semantic drift, not literal-string
# drift: K8s quantities like "1Gi" and "1024Mi" are equivalent and either is a
# valid way to express the documented 1 GiB. The minimum sized to fit the
# DefaultMaxEntries=1M cap at ~540 MiB peak RSS plus a 1.5x headroom margin is
# 1 GiB = 1073741824 bytes; we accept anything >= that.
mem_to_bytes() {
  # Strips a K8s memory quantity suffix (Ki/Mi/Gi/Ti or k/M/G/T) and emits bytes.
  # Returns 0 on unparseable input — caller treats 0 as "below threshold" and
  # fails noisily. Uses awk for the multiply so fractional quantities like
  # 1.5Gi don't trip bash integer arithmetic (which would crash the gate
  # instead of failing it cleanly).
  local v="$1" n factor
  case "$v" in
    *Ki) n=${v%Ki}; factor=1024 ;;
    *Mi) n=${v%Mi}; factor=$((1024 * 1024)) ;;
    *Gi) n=${v%Gi}; factor=$((1024 * 1024 * 1024)) ;;
    *Ti) n=${v%Ti}; factor=$((1024 * 1024 * 1024 * 1024)) ;;
    *k)  n=${v%k};  factor=1000 ;;
    *M)  n=${v%M};  factor=$((1000 * 1000)) ;;
    *G)  n=${v%G};  factor=$((1000 * 1000 * 1000)) ;;
    *T)  n=${v%T};  factor=$((1000 * 1000 * 1000 * 1000)) ;;
    *) n=$v; factor=1 ;;
  esac
  awk -v n="$n" -v f="$factor" 'BEGIN {
    # awk parses leading numerics; "garbage" becomes 0, "1.5" stays 1.5.
    # printf "%.0f" rounds the product back to an integer byte count.
    printf "%.0f\n", n * f
  }'
}
server_mem_bytes=$(mem_to_bytes "$server_mem_limit")
min_bytes=$(( 1024 * 1024 * 1024 ))   # 1 GiB
if [ "$server_mem_bytes" -lt "$min_bytes" ]; then
  fail "inference-cache-server memory limit = '$server_mem_limit' ($server_mem_bytes bytes); want >= 1Gi ($min_bytes bytes) to fit DefaultMaxEntries=1M per docs/operations/index-sizing.md"
fi
log "inference-cache-server memory limit = $server_mem_limit ($server_mem_bytes bytes; >= 1Gi → sized for DefaultMaxEntries=1M)"

# --- CacheBackend CRD schema-trim assertion --------------------------------
# The installed CRD must reflect the inert-field trim: the five removed fields
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
# status.indexParticipation.t2HitRate is the tier-2 (LMCache) offload health
# surface — assert the new status field is actually served by the installed CRD.
if [ -z "$(crd_field_type 'status.properties.indexParticipation.properties.t2HitRate')" ]; then
  fail "CRD is missing status.indexParticipation.t2HitRate (the tier-2 health surface)"
fi
# spec.storage{,.pvc} + status.capacity were removed in the storage-retirement
# trim — the lm:// server we provision is in-memory, so a local PVC cannot
# honestly back it; durability is a backend choice. Assert the installed CRD no
# longer serves them, so an operator cannot set a storage field the controller
# no longer honors (the operator-facing surface change this smoke must catch).
if [ -n "$(crd_field_type 'spec.properties.storage')" ]; then
  fail "CRD still serves removed spec.storage (storage-retirement trim not installed)"
fi
if [ -n "$(crd_field_type 'status.properties.capacity')" ]; then
  fail "CRD still serves removed status.capacity (storage-retirement trim not installed)"
fi
log "CacheBackend CRD reflects the schema trim (lookupTimeoutMs/minimumPrefixTokens/indexEntries/storage/capacity absent; indexParticipation.prefixCount + t2HitRate present)"

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

# --- CacheIndex CRD per-tenant-memory deprecation assertion ----------------
# Per-tenant memory cannot be honestly attributed on a shared, tenant-unaware
# engine (status.tenants[].memoryUsed double-counts the same bytes once per
# tenant), so the field is DEPRECATED and always 0 — but retained in the
# v1alpha1 schema for wire/shape compatibility (removal deferred to v1beta1).
# The honest per-replica engine total stays. Probing the live CRD proves both
# fields are in the installed bundle, not just the repo.
ci_field_type() {
  kubectl get crd cacheindices.inferencecache.io \
    -o "jsonpath={.spec.versions[?(@.name=='v1alpha1')].schema.openAPIV3Schema.properties.$1.type}" \
    2>/dev/null || true
}
if [ -z "$(ci_field_type 'status.properties.tenants.items.properties.memoryUsed')" ]; then
  fail "CRD is missing status.tenants[].memoryUsed (deprecated+zeroed but retained in the v1alpha1 schema for compat — must remain until v1beta1)"
fi
if [ -z "$(ci_field_type 'status.properties.replicas.items.properties.cacheMemoryBytes')" ]; then
  fail "CRD is missing status.replicas[].cacheMemoryBytes (the honest per-replica engine total)"
fi
log "CacheIndex CRD serves deprecated status.tenants[].memoryUsed (retained, always 0) and the honest status.replicas[].cacheMemoryBytes"

# --- CacheIndex harmonized-pointer status fields ---------------------------
# hitRate (status.replicas[] and status.tenants[]) and status.tenants[].indexEntries
# use the "nil = not yet reported / computed" pointer convention, aligned with
# the per-instance CacheBackend/CacheTenant surfaces. Pointer-ness itself is NOT
# visible in the OpenAPI schema (a *string still serves as type: string, a
# *int64 as type: integer), so this check only proves the fields still exist
# with their expected scalar leaf types in the installed bundle — the guard is
# against an accidental field drop/rename or a codegen change that alters the
# served type. The value-level nil-vs-observed-0 behavior is exercised by the
# envtest suite (persisted-shape assertions), not here.
if [ "$(ci_field_type 'status.properties.replicas.items.properties.hitRate')" != "string" ]; then
  fail "CacheIndex CRD status.replicas[].hitRate is not served as type string"
fi
if [ "$(ci_field_type 'status.properties.tenants.items.properties.hitRate')" != "string" ]; then
  fail "CacheIndex CRD status.tenants[].hitRate is not served as type string"
fi
if [ "$(ci_field_type 'status.properties.tenants.items.properties.indexEntries')" != "integer" ]; then
  fail "CacheIndex CRD status.tenants[].indexEntries is not served as type integer"
fi
log "CacheIndex CRD serves status.{replicas,tenants}[].hitRate (string) + status.tenants[].indexEntries (integer)"

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

# --- CacheTenant admission: reserved probe tenantID ------------------------
# The functional self-test uses tenant_id "inferencecache.io/probe" as
# server-internal state. The CacheTenant admission webhook MUST reject any
# CR claiming that id so an operator-created tenant cannot collide with the
# probe scope (which would bypass quota enforcement at the PolicyStore
# layer and share the probe's reserved replica). The rule fires on CREATE,
# and on an UPDATE that newly introduces the id — pinned end-to-end by
# this assertion against the real installed webhook (not just envtest).
log "asserting a CacheTenant claiming the reserved probe tenantID is rejected at admission"
reserved_ct_yaml="$(cat <<'EOF'
apiVersion: inferencecache.io/v1alpha1
kind: CacheTenant
metadata:
  name: cachetenant-reserved-probe
spec:
  tenantID: inferencecache.io/probe
EOF
)"
if reserved_ct_out="$(printf '%s\n' "$reserved_ct_yaml" | kubectl apply -f - 2>&1)"; then
  echo "$reserved_ct_out"
  fail "CacheTenant claiming the reserved probe tenantID was admitted; the reservation rule did not fire on the real install"
fi
if ! grep -q "reserved" <<<"$reserved_ct_out"; then
  echo "$reserved_ct_out"
  fail "reserved-probe-tenantID CacheTenant was rejected, but not by the expected rule (missing 'reserved' in the diagnostic)"
fi
log "CacheTenant claiming the reserved probe tenantID rejected by the installed validating webhook"

# --- PromptTemplate + PDTopology schema-only assertion ----------------------
# config/default installs the PromptTemplate/PDTopology CRDs and RBAC, and the
# controller manager adds their Go types to the scheme, but no PromptTemplate
# render-controller or PDTopology reconciler is started from cmd/controller/main.go.
# Their status fields are therefore future status surfaces, not live signals in
# config/default. Assert the meaningful Phase-1 contract instead: committed
# samples apply against the real CRDs, and the short-name kubectl tables expose
# their operator-facing printer columns.
log "applying PromptTemplate + PDTopology samples in namespace $PROMPT_TOPOLOGY_SMOKE_NS"
kubectl delete namespace "$PROMPT_TOPOLOGY_SMOKE_NS" --ignore-not-found --wait=true --timeout=60s >/dev/null \
  || fail "timed out waiting for prior PromptTemplate/PDTopology smoke namespace $PROMPT_TOPOLOGY_SMOKE_NS to delete"
kubectl create namespace "$PROMPT_TOPOLOGY_SMOKE_NS" --dry-run=client -o yaml \
  | kubectl apply -f - >/dev/null
kubectl -n "$PROMPT_TOPOLOGY_SMOKE_NS" apply -f config/samples/cache_v1alpha1_prompttemplate.yaml >/dev/null
kubectl -n "$PROMPT_TOPOLOGY_SMOKE_NS" apply -f config/samples/cache_v1alpha1_pdtopology.yaml >/dev/null

pt_table="$(kubectl -n "$PROMPT_TOPOLOGY_SMOKE_NS" get pt prompttemplate-sample 2>/dev/null || true)"
pt_header="$(printf '%s\n' "$pt_table" | sed -n '1p')"
pt_row="$(printf '%s\n' "$pt_table" | sed -n '2p')"
if ! grep -Eq "(^|[[:space:]])REVISION([[:space:]]|$)" <<<"$pt_header"; then
  echo "$pt_table"
  fail "expected PromptTemplate printer column REVISION in kubectl get pt output"
fi
if ! grep -Fq "prompttemplate-sample" <<<"$pt_row"; then
  echo "$pt_table"
  fail "expected PromptTemplate printer row to include prompttemplate-sample"
fi

pdt_table="$(kubectl -n "$PROMPT_TOPOLOGY_SMOKE_NS" get pdt pdtopology-sample 2>/dev/null || true)"
pdt_header="$(printf '%s\n' "$pdt_table" | sed -n '1p')"
pdt_row="$(printf '%s\n' "$pdt_table" | sed -n '2p')"
for column in PREFILL DECODE; do
  if ! grep -Eq "(^|[[:space:]])${column}([[:space:]]|$)" <<<"$pdt_header"; then
    echo "$pdt_table"
    fail "expected PDTopology printer column ${column} in kubectl get pdt output"
  fi
done
if ! grep -Fq "pdtopology-sample" <<<"$pdt_row" || \
   ! grep -Fq "prefill-a" <<<"$pdt_row" || \
   ! grep -Fq "decode-a" <<<"$pdt_row"; then
  echo "$pdt_table"
  fail "expected PDTopology printer row to include sample name and prefill/decode pool names"
fi
log "PromptTemplate/PDTopology are schema-only in default install; samples apply and printer columns render"
kubectl delete namespace "$PROMPT_TOPOLOGY_SMOKE_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true

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

# --- functional-probe gate: positive-case coverage scoped to a later phase --
# The controller-side caller publishes the FunctionalProbeOK condition on
# managed CacheBackends. This smoke asserts the NEGATIVE case directly
# (FunctionalProbeOK absent while the upstream KV-event gate holds
# Ready=False) inline in the paired-sample phase below, alongside the
# KV-event gate assertion it cascades off of — see the
# "functional-probe gate (downstream of KV-event gate)" block after the
# AwaitingFirstKVEvent assertion. The positive case (FunctionalProbeOK
# appearing once the upstream gate clears) requires an engine workload
# that actually publishes KV events, which this smoke does not stand up;
# that assertion lands with a follow-up that ships an engine-pod fixture.
# The metric inferencecache_backend_probe_result_total is similarly not
# /metrics-visible on this install because Prometheus client_golang's
# CounterVec exposes no HELP/TYPE/data lines until a WithLabelValues
# child is instantiated, and that requires a real probe call to fire —
# which only happens after the upstream gate clears. Stage 2's
# controller-side wiring is otherwise covered by 20+ unit tests and an
# envtest integration sub-test driving a real CacheBackend reconciler
# against an httptest /probe server.

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

# --- Dual-input LookupRoute assertions (server-side tokenization surface) ---
# CONTRIBUTING requires gRPC behavior changes to extend this gate. The default
# install ships the pure-Go server (no cgo tokenizer), so prove BOTH dual-input
# paths are wired at the deployment level and fail open:
#   - token_ids: the server fingerprints the supplied tokens itself (no tokenizer
#     needed); for an unknown model it fails open to NO_HINT.
#   - prompt_text: the default build has no tokenizer, so this path fails open to
#     NO_HINT (never an error on the hot path).
ti_resp="$(grpcurl_lookup_route '{"modelId":"install-smoke-unknown","hashScheme":"vllm","tokenIds":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]}' "$LOG_DIR/grpcurl-tokenids.err")" || {
  cat "$LOG_DIR/grpcurl-tokenids.err" >&2 || true
  fail "grpcurl LookupRoute(token_ids) did not return a response"
}
log "LookupRoute(token_ids) response: $ti_resp"
if ! has_reason_code "$ti_resp" "NO_HINT"; then
  fail "expected reason_code=NO_HINT for token_ids on an unknown model, got: $ti_resp"
fi

pt_resp="$(grpcurl_lookup_route '{"modelId":"install-smoke-unknown","hashScheme":"vllm","promptText":"hello world"}' "$LOG_DIR/grpcurl-prompttext.err")" || {
  cat "$LOG_DIR/grpcurl-prompttext.err" >&2 || true
  fail "grpcurl LookupRoute(prompt_text) did not return a response"
}
log "LookupRoute(prompt_text) response: $pt_resp"
if ! has_reason_code "$pt_resp" "NO_HINT"; then
  fail "expected reason_code=NO_HINT for prompt_text on the default (tokenizer-less) build, got: $pt_resp"
fi

# --- CachePolicy PUSH adoption assertion -----------------------------------
# No read-back endpoint exists for /policy, by design. Prove the server adopted
# the controller-pushed CachePolicy via existing gRPC side effects on two
# orthogonal axes (routingFloorScore is probed separately by the patch-and-
# wait block further below):
#   1. minimumPrefixTokens (request-side gate; applied BEFORE the index
#      lookup under affinityRouting=Disabled or as a post-lookup
#      result-side downgrade under affinityRouting=Enabled — the default).
#      Seed one prefix; the request below the policy threshold no longer
#      reaches the prefix-match path. With affinityRouting=Enabled (the
#      kubebuilder default carried by the sample CR) the fallback fires and
#      the response is AFFINITY_HINT — *not* PREFIX_MATCH, which is what
#      proves the gate adopted. With affinityRouting=Disabled this same path
#      would return NO_HINT; either non-PREFIX_MATCH outcome proves the gate.
#   2. minimumMatchedTokens (result-side floor, applied AFTER the lookup, against
#      the realized matched-token overlap). Seed a SECOND prefix whose stored
#      tokenCount is above minimumPrefixTokens (so it clears the request-side
#      gate) but BELOW minimumMatchedTokens (so the realized match downgrades
#      away from PREFIX_MATCH). Without this assertion the floor could be
#      silently dropped and the smoke would still pass on the request-side
#      gate alone. The sample carries minimumMatchedTokens explicitly
#      (config/samples/cache_v1alpha1_cachepolicy.yaml). The downgrade
#      again surfaces as AFFINITY_HINT (or NO_HINT under affinity Disabled).
#
# Note: with no CachePolicy at all the server-wide DefaultMinimumMatchedTokens
# (= 64) ALSO downgrades the trivial 32-token match away from PREFIX_MATCH —
# the no-policy fallback fires the same floor as the sample CR sets. The
# point of the trivial-match assertion is therefore "the pushed CR did not
# silently drop the result-side floor" rather than "without the CR this would
# have been PREFIX_MATCH". The low-prefix lookup is the standalone proof
# that policy adoption happened (its non-PREFIX_MATCH outcome IS owned by
# the pushed minimumPrefixTokens: 32 — no-policy would have ungated the
# request and returned PREFIX_MATCH on the 64-token stored prefix). Together
# they cover both policy enforcement axes end-to-end. Avoids engine
# pods/images, model traffic, and any new transport.
log "seeding two prefixes and asserting CachePolicy minimumPrefixTokens (request-side gate) AND minimumMatchedTokens (result-side floor) are both enforced by LookupRoute"
policy_model="install-smoke-policy"
policy_replica="policy-smoke-replica"
policy_hash_b64="cG9saWN5LXByZWZpeA==" # base64("policy-prefix") — stored at tokenCount=64, clears both gates
trivial_hash_b64="dHJpdmlhbC1wcmVmaXg=" # base64("trivial-prefix") — stored at tokenCount=32, clears request gate (32 >= sample's 32) but below the 64 matched-tokens floor

# Two stored prefixes in one ReportCacheState ingest: the regular 64-token
# prefix (clears both gates) and the trivial 32-token prefix (clears the
# request-side gate, fails the result-side floor — what proves the
# minimumMatchedTokens floor actually fires end-to-end).
policy_report_payload="$(cat <<EOF
{"replicaId":"$policy_replica","modelId":"$policy_model","tenantId":"$POLICY_SMOKE_NS","hashScheme":"vllm","prefixes":[{"prefixHash":"$policy_hash_b64","tokenCount":64},{"prefixHash":"$trivial_hash_b64","tokenCount":32}],"stats":{"replicaId":"$policy_replica","hitRate":1}}
EOF
)"
policy_report_resp="$(grpcurl_report_cache_state "$policy_report_payload" "$LOG_DIR/grpcurl-policy-report.err")" || {
  cat "$LOG_DIR/grpcurl-policy-report.err" >&2 || true
  fail "grpcurl ReportCacheState did not accept the CachePolicy smoke prefixes"
}
log "ReportCacheState response: $policy_report_resp"

# Three lookups exercise the three orthogonal policy-enforcement paths:
#   - policy_high: above both gates → PREFIX_MATCH (control: ingest path
#     works; affinity does NOT preempt a real match).
#   - policy_low: below the request-side gate → AFFINITY_HINT (request-side
#     gate fires; the affinity fallback then picks the only known replica).
#   - policy_trivial: above the request-side gate but matched_tokens=32 <
#     floor 64 → AFFINITY_HINT (result-side floor fires — the assertion the
#     request-side gate alone cannot make — and again the affinity fallback
#     surfaces the single known replica).
policy_high_payload="$(cat <<EOF
{"modelId":"$policy_model","tenantId":"$POLICY_SMOKE_NS","hashScheme":"vllm","prefixHash":"$policy_hash_b64","prefixTokenCount":64}
EOF
)"
policy_low_payload="$(cat <<EOF
{"modelId":"$policy_model","tenantId":"$POLICY_SMOKE_NS","hashScheme":"vllm","prefixHash":"$policy_hash_b64","prefixTokenCount":1}
EOF
)"
policy_trivial_payload="$(cat <<EOF
{"modelId":"$policy_model","tenantId":"$POLICY_SMOKE_NS","hashScheme":"vllm","prefixHash":"$trivial_hash_b64","prefixTokenCount":64}
EOF
)"

deadline=$(($(date +%s) + POLICY_PUSH_TIMEOUT))
policy_high_resp=""
policy_low_resp=""
policy_trivial_resp=""
until has_reason_code "$policy_high_resp" "PREFIX_MATCH" \
  && has_reason_code "$policy_low_resp" "AFFINITY_HINT" \
  && has_reason_code "$policy_trivial_resp" "AFFINITY_HINT"; do
  policy_high_resp="$(grpcurl_lookup_route "$policy_high_payload" "$LOG_DIR/grpcurl-policy-high.err")" || true
  policy_low_resp="$(grpcurl_lookup_route "$policy_low_payload" "$LOG_DIR/grpcurl-policy-low.err")" || true
  policy_trivial_resp="$(grpcurl_lookup_route "$policy_trivial_payload" "$LOG_DIR/grpcurl-policy-trivial.err")" || true

  if has_reason_code "$policy_high_resp" "PREFIX_MATCH" \
    && has_reason_code "$policy_low_resp" "AFFINITY_HINT" \
    && has_reason_code "$policy_trivial_resp" "AFFINITY_HINT"; then
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl -n "$POLICY_SMOKE_NS" get cachepolicy cachepolicy-sample -o yaml || true
    echo "above-threshold LookupRoute response (want PREFIX_MATCH):" >&2
    echo "$policy_high_resp" >&2
    echo "below-request-gate LookupRoute response (want AFFINITY_HINT — request gate fires; affinity fallback picks the known replica):" >&2
    echo "$policy_low_resp" >&2
    echo "trivial-match (below result floor) LookupRoute response (want AFFINITY_HINT — matched-tokens floor fires; affinity fallback picks the known replica):" >&2
    echo "$policy_trivial_resp" >&2
    for err_file in "$LOG_DIR/grpcurl-policy-high.err" "$LOG_DIR/grpcurl-policy-low.err" "$LOG_DIR/grpcurl-policy-trivial.err"; do
      if [ -s "$err_file" ]; then
        echo "$(basename "$err_file"):" >&2
        cat "$err_file" >&2
      fi
    done
    fail "server did not adopt the pushed CachePolicy within ${POLICY_PUSH_TIMEOUT}s (want above PREFIX_MATCH, below-request AFFINITY_HINT, trivial-match AFFINITY_HINT — the non-PREFIX_MATCH outcomes prove both gates fired with affinityRouting=Enabled)"
  fi
  sleep 2
done
log "CachePolicy push adopted: above-threshold lookup hit; below-request-gate lookup returned AFFINITY_HINT; trivial-match (matched_tokens<floor) lookup returned AFFINITY_HINT — both minimum-token policy knobs enforced end-to-end (the non-PREFIX_MATCH outcomes prove the gates fired; the affinity fallback then surfaces the single known replica)"

# --- routingFloorScore end-to-end probe ------------------------------------
# Proves the new field flows CR → controller flatten → /policy push → server
# resolver → buildLookupResponse downgrade. The same 64-token prefix that
# returned PREFIX_MATCH above is forced off the prefix-match path after we
# patch the live CachePolicy to routingFloorScore="1000" (well above any
# plausible score: matched_tokens (64) × freshness (~1) ×
# distinguishing_power (1.0 — only one replica was seeded, so the factor
# degenerates) = ~64, which is below the strict 1000 floor). With the
# kubebuilder-default affinityRouting=Enabled the response settles at
# AFFINITY_HINT (the floor fired and the affinity fallback picked the
# single known replica). Restoring "0.1" must flip the response back to
# PREFIX_MATCH — proving replace-on-write semantics carry the new field
# too. Without engine pods or model traffic, this is the minimum end-to-end
# exercise of the new operator-facing knob.
log "asserting CachePolicy.spec.routingFloorScore is propagated and gates LookupRoute"

apply_floor() {
  local floor="$1"
  kubectl -n "$POLICY_SMOKE_NS" patch cachepolicy cachepolicy-sample \
    --type=merge -p "{\"spec\":{\"routingFloorScore\":\"$floor\"}}" >/dev/null \
    || fail "kubectl patch cachepolicy routingFloorScore=$floor failed"
}

wait_floor_reason() {
  local payload="$1" want="$2" err_file="$3" label="$4"
  local deadline=$(($(date +%s) + POLICY_PUSH_TIMEOUT))
  local resp=""
  until has_reason_code "$resp" "$want"; do
    resp="$(grpcurl_lookup_route "$payload" "$err_file")" || true
    if has_reason_code "$resp" "$want"; then
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      kubectl -n "$POLICY_SMOKE_NS" get cachepolicy cachepolicy-sample -o yaml || true
      echo "$label response (want $want):" >&2
      echo "$resp" >&2
      if [ -s "$err_file" ]; then
        echo "$(basename "$err_file"):" >&2
        cat "$err_file" >&2
      fi
      fail "server did not adopt routingFloorScore patch within ${POLICY_PUSH_TIMEOUT}s ($label, want $want)"
    fi
    sleep 2
  done
}

apply_floor "1000"
wait_floor_reason "$policy_high_payload" "AFFINITY_HINT" "$LOG_DIR/grpcurl-policy-floor-strict.err" "routingFloorScore=1000 high-token lookup"
log "routingFloorScore=1000 enforced: same 64-token match now AFFINITY_HINT (score below floor; affinity fallback picks the known replica)"

apply_floor "0.1"
wait_floor_reason "$policy_high_payload" "PREFIX_MATCH" "$LOG_DIR/grpcurl-policy-floor-restored.err" "routingFloorScore=0.1 high-token lookup"
log "routingFloorScore=0.1 restored: same 64-token match flipped back to PREFIX_MATCH (replace-on-write OK)"

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
sed -i.bak "s|serverImage: lmcache/standalone:v0.4.7|serverImage: $escaped_sample_cache_server_image|g" \
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

# --- spec.resources defaults + thread-through ------------------------------
# The minimal paired-sample CacheBackend declares no spec.resources, so the
# CRD-schema default must stamp limits.memory=8Gi (and the matching
# requests.memory=4Gi) on the persisted CR; the controller must then thread
# that limit onto the rendered Deployment's lmcache-server container. Both
# halves must hold — half-failing reintroduces the OOM-kill cliff that
# motivated bounding the cache-server pod by the cgroup.
log "asserting spec.resources defaults stamp on the CR and thread to the rendered Deployment"
cb_lim_mem="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.spec.resources.limits.memory}' 2>/dev/null || true)"
if [ "$cb_lim_mem" != "8Gi" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "cb.spec.resources.limits.memory=$cb_lim_mem, want 8Gi (CRD schema default not applied)"
fi
cb_req_mem="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.spec.resources.requests.memory}' 2>/dev/null || true)"
if [ "$cb_req_mem" != "4Gi" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "cb.spec.resources.requests.memory=$cb_req_mem, want 4Gi (CRD schema default not applied)"
fi
dep_lim_mem="$(kubectl -n "$SAMPLE_NS" get deploy qwen-demo-cache \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="lmcache-server")].resources.limits.memory}' \
  2>/dev/null || true)"
if [ "$dep_lim_mem" != "8Gi" ]; then
  kubectl -n "$SAMPLE_NS" get deploy qwen-demo-cache -o yaml || true
  fail "deploy.lmcache-server.resources.limits.memory=$dep_lim_mem, want 8Gi (controller did not thread spec.resources)"
fi
dep_req_mem="$(kubectl -n "$SAMPLE_NS" get deploy qwen-demo-cache \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="lmcache-server")].resources.requests.memory}' \
  2>/dev/null || true)"
if [ "$dep_req_mem" != "4Gi" ]; then
  kubectl -n "$SAMPLE_NS" get deploy qwen-demo-cache -o yaml || true
  fail "deploy.lmcache-server.resources.requests.memory=$dep_req_mem, want 4Gi (controller did not thread spec.resources)"
fi
log "spec.resources defaults stamped + threaded: requests.memory=4Gi limits.memory=8Gi"

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

# --- functional-probe gate (downstream of KV-event gate) -------------------
# The functional-probe gate is cascade-prevented from running while any
# upstream gate keeps Ready=False — there is no point in driving a
# synthetic round-trip against a backend the operator has not yet
# declared "is supposed to be working." This asserts that
# operator-visible behavior on the live install:
#   - The FunctionalProbeOK condition MUST be ABSENT on a backend the
#     upstream KV-event gate is holding at Ready=False/AwaitingFirstKVEvent.
#     Its presence here would be a regression: the controller is firing
#     the probe loop on a backend that's still warming up, paging
#     operators on a known-not-ready state.
#   - The Ready condition's status+reason still reflect the upstream gate
#     (False/AwaitingFirstKVEvent), not a downstream probe verdict —
#     proving cascade-prevention is on, not just "no probe call yet."
# A positive-case assertion (FunctionalProbeOK appearing once the
# upstream gate clears) requires an engine workload that actually
# publishes KV events; not feasible from this smoke without a real GPU
# or the CPU vLLM image. That's deferred to a Stage 4 follow-up that
# lands an engine-pod fixture; this negative case still locks the
# operator-facing cascade behavior in place.
fp_status="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.status.conditions[?(@.type=="FunctionalProbeOK")].status}' 2>/dev/null || true)"
if [ -n "$fp_status" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "FunctionalProbeOK condition present (status=$fp_status) while Ready=$gate_status/$gate_reason; cascade-prevention regressed — downstream probe gate must not fire while upstream KV-event gate is False"
fi
log "functional-probe cascade-prevention holds: FunctionalProbeOK absent while Ready=False/AwaitingFirstKVEvent"

# T2Degraded (advisory tier-2 offload health) must likewise be ABSENT on a
# backend that has not exercised its tier-2 cache: the condition is derived from
# status.indexParticipation.t2HitRate, which stays nil until external lookups
# are observed. A fresh smoke backend drives no tier-2 traffic, so the operator
# must NOT see a T2Degraded breadcrumb here — a present condition (even
# False) would be a misleading "tier-2 is being tracked" signal where the
# tier was never used.
t2_status="$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
  -o jsonpath='{.status.conditions[?(@.type=="T2Degraded")].status}' 2>/dev/null || true)"
if [ -n "$t2_status" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  fail "T2Degraded condition present (status=$t2_status) on a backend that has not exercised tier-2 — it must be absent until external lookups are observed"
fi
log "T2Degraded absent until tier-2 is exercised (no-traffic steady state)"

# --- EngineCompatibility (injected-engine crash-loop) assertion -------------
# ORDERING (do not move earlier): this phase blocks up to ~300s (180s for the
# injected engine to reach CrashLoopBackOff + 120s for the advisory condition to
# publish). It MUST run AFTER the KV-event-gate AwaitingFirstKVEvent assertion
# above, NOT between matchedEnginePods=1 and that gate. The gate's
# AwaitingFirstKVEvent window is bounded by spec.integration.firstEventTimeout
# (defaulted to 5m and asserted as 5m above), anchored at the cache-server
# Deployment becoming Available; the busybox engine never emits KV events, so the
# backend deterministically flips AwaitingFirstKVEvent -> NoKVEventsObserved once
# that 5m elapses. Running this ~300s wait before the gate assertion would burn
# most of that 5m budget and race the gate to NoKVEventsObserved on slow CI
# (flaky). Placed here, the gate is already banked and EngineCompatibility is
# independent of the Ready condition, so the long wait is harmless.
#
# This asserts the controller surfaces an injected engine's CrashLoopBackOff as
# the advisory EngineCompatibility condition — it does NOT validate the
# hybrid-attention incompatibility *cause* (a real hybrid model would need a
# GPU). The condition reports the generic crash-loop observation; the root cause
# is verified out-of-band via engine logs. The busybox stand-in
# (SAMPLE_ENGINE_IMAGE) CANNOT run `vllm serve`, so its injected engine
# container lands in CrashLoopBackOff — which is all this assertion needs: it
# exercises the controller's crash-loop heuristic, not the connector-versus-
# hybrid-attention diagnosis. We must NOT mutate the qwen-engine Deployment
# here: a later phase cascade-restarts it to assert status.observedServerInstance
# advances, and a command override would stick and break that check. So we only
# wait for the natural crash-loop and assert the advisory
# EngineCompatibility=False/InjectedEngineCrashLooping condition surfaces
# (instead of a silent crash-loop). The controller does NOT watch engine pods
# (no informer, by design), so we poke the CacheBackend to drive the reconcile
# that reads them.
log "asserting EngineCompatibility surfaces on the crash-looping injected engine"
deadline=$(($(date +%s) + 180))
clbo=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  clbo=$(kubectl -n "$SAMPLE_NS" get pod -l app=qwen-demo \
    -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.state.waiting.reason} {end}{end}' 2>/dev/null || true)
  case "$clbo" in *CrashLoopBackOff*) break;; esac
  sleep 4
done
case "$clbo" in
  *CrashLoopBackOff*) : ;;
  *) kubectl -n "$SAMPLE_NS" get pod -l app=qwen-demo -o wide || true
     fail "engine pod did not reach CrashLoopBackOff within 180s (waiting reasons: $clbo)" ;;
esac
deadline=$(($(date +%s) + 120))
ec_reason=""
ec_status=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  kubectl -n "$SAMPLE_NS" annotate cb qwen-demo-cache \
    inferencecache.io/smoke-poke="$(date +%s)" --overwrite >/dev/null 2>&1 || true
  ec_reason=$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.conditions[?(@.type=="EngineCompatibility")].reason}' 2>/dev/null || true)
  ec_status=$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.conditions[?(@.type=="EngineCompatibility")].status}' 2>/dev/null || true)
  # Assert BOTH status and reason: the documented surface is
  # EngineCompatibility=False/InjectedEngineCrashLooping. Checking the reason
  # alone would let a regression to status=True (advisory condition inverted)
  # slip through while the reason string still matched.
  if [ "$ec_status" = "False" ] && [ "$ec_reason" = "InjectedEngineCrashLooping" ]; then break; fi
  sleep 4
done
if [ "$ec_status" != "False" ] || [ "$ec_reason" != "InjectedEngineCrashLooping" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o jsonpath='{.status.conditions}' || true
  fail "EngineCompatibility status=$ec_status reason=$ec_reason, want False/InjectedEngineCrashLooping after the injected engine crash-looped"
fi
log "EngineCompatibility=False/InjectedEngineCrashLooping surfaced on the crash-looping injected engine"

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

# --- binding diagnostics: unmatched selector + explicit skip ----------------
# A deliberately non-matching CacheBackend should expose the selector drift on
# the CR itself: status.engineSelectorMessage echoes the selector, and a Normal
# EngineSelectorUnmatched Event provides the push-style breadcrumb.
log "applying a non-matching CacheBackend to assert selector diagnostics"
cat <<EOF | kubectl -n "$SAMPLE_NS" apply -f - >/dev/null
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: unmatched-cache
spec:
  type: External
  endpoint: unmatched-cache.example.com:8200
  engineSelector:
    matchLabels:
      app: definitely-not-qwen
EOF

log "waiting up to ${SAMPLE_MATCH_TIMEOUT}s for unmatched selector status + Event"
deadline=$(($(date +%s) + SAMPLE_MATCH_TIMEOUT))
unmatched_msg=""
unmatched_event=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  unmatched_msg="$(kubectl -n "$SAMPLE_NS" get cb unmatched-cache \
    -o jsonpath='{.status.engineSelectorMessage}' 2>/dev/null || true)"
  unmatched_event="$(kubectl -n "$SAMPLE_NS" get events.events.k8s.io \
    --field-selector reason=EngineSelectorUnmatched \
    -o jsonpath="{range .items[?(@.regarding.name=='unmatched-cache')]}{.reason}{'\n'}{end}" \
    2>/dev/null || true)"
  case "$unmatched_msg" in
    *"app:definitely-not-qwen"*"no Pods in namespace match"*)
      [ -n "$unmatched_event" ] && break
      ;;
  esac
  sleep 2
done
case "$unmatched_msg" in
  *"app:definitely-not-qwen"*"no Pods in namespace match"*) ;;
  *)
    kubectl -n "$SAMPLE_NS" get cb unmatched-cache -o yaml || true
    fail "status.engineSelectorMessage=$unmatched_msg, want selector echo + no Pods diagnostic"
    ;;
esac
if [ -z "$unmatched_event" ]; then
  kubectl -n "$SAMPLE_NS" get events.events.k8s.io \
    --field-selector reason=EngineSelectorUnmatched -o yaml || true
  fail "EngineSelectorUnmatched Event not observed on CacheBackend/unmatched-cache within ${SAMPLE_MATCH_TIMEOUT}s"
fi
log "unmatched selector diagnostics present: $unmatched_msg"

# A pod that explicitly opts out must be distinguishable from a drifted pod:
# the webhook stamps inject-skipped, and the engine-pod-events controller turns
# that stamp into a describe-visible SkippedByOperator Event keyed to the
# persisted pod UID.
log "creating a skip-inject pod to assert opt-out visibility"
cat <<EOF | kubectl -n "$SAMPLE_NS" apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: skipped-engine
  annotations:
    inferencecache.io/skip-inject: "true"
  labels:
    app: skip-demo
spec:
  containers:
    - name: engine
      image: $SAMPLE_ENGINE_IMAGE
      command: ["sh", "-c", "sleep 3600"]
EOF
skipped_uid="$(kubectl -n "$SAMPLE_NS" get pod skipped-engine \
  -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
if [ -z "$skipped_uid" ]; then
  fail "skipped-engine pod did not persist with a UID"
fi
deadline=$(($(date +%s) + 30))
skip_reason=""
skip_event=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  skip_reason="$(kubectl -n "$SAMPLE_NS" get pod skipped-engine \
    -o jsonpath='{.metadata.annotations.inferencecache\.io/inject-skipped}' 2>/dev/null || true)"
  skip_event="$(kubectl -n "$SAMPLE_NS" get events.events.k8s.io \
    --field-selector reason=SkippedByOperator \
    -o jsonpath="{range .items[?(@.regarding.uid=='$skipped_uid')]}{.reason}{'\n'}{end}" \
    2>/dev/null || true)"
  [ "$skip_reason" = "skip-inject-annotation" ] && [ -n "$skip_event" ] && break
  sleep 2
done
if [ "$skip_reason" != "skip-inject-annotation" ]; then
  kubectl -n "$SAMPLE_NS" get pod skipped-engine -o yaml || true
  fail "annotation inferencecache.io/inject-skipped=$skip_reason, want skip-inject-annotation"
fi
skipped_injected_by="$(kubectl -n "$SAMPLE_NS" get pod skipped-engine \
  -o jsonpath='{.metadata.annotations.inferencecache\.io/injected-by}' 2>/dev/null || true)"
if [ -n "$skipped_injected_by" ]; then
  kubectl -n "$SAMPLE_NS" get pod skipped-engine -o yaml || true
  fail "skipped pod unexpectedly carries inferencecache.io/injected-by=$skipped_injected_by"
fi
if [ -z "$skip_event" ]; then
  kubectl -n "$SAMPLE_NS" get events.events.k8s.io \
    --field-selector reason=SkippedByOperator -o yaml || true
  fail "SkippedByOperator Event not observed on skipped pod uid=$skipped_uid within 30s"
fi
log "skip-inject visibility present: inject-skipped=$skip_reason and SkippedByOperator Event"

# --- cache-server restart cascade ------------------------------------------
# When the cache-server pod is replaced (OOM-kill, eviction, image roll,
# operator-initiated restart, …), every injected engine pod holds a stale
# LMCache client socket — the upstream LMServerConnector opens its TCP
# socket in __init__ only and silently fails every subsequent PUT with
# EPIPE until the engine pod itself rolls. The controller's
# observedServerInstance latch detects the cache-server UID transition
# and cascade-restarts every engine Deployment that owns pods carrying
# this backend's inferencecache.io/injected-by annotation AND the
# matching inferencecache.io/injected-by-uid (the UID half rejects
# forgeries and stale name-reuse), by patching
# AnnotationCacheServerRestartTrigger onto the Deployment's pod template
# (the same mechanism kubectl rollout restart uses).
#
# This phase asserts the end-to-end loop on the real install:
#   - status.observedServerInstance picks up the current cache-server
#     server-instance identifier (`<pod-uid>:<restart-sum>`) after the
#     initial rollout (no cascade — first observation never cascades).
#   - Forcing a cache-server pod restart flips observedServerInstance
#     to the replacement's server-instance identifier.
#   - The engine Deployment's spec.template.metadata.annotations gets
#     the cascade trigger set to that new identifier — proving the
#     loop closed against the installed RBAC + actual apiserver Patch,
#     not just envtest.
#
# Must run BEFORE the drift case below, which scales the engine to 0:
# with no engine pod present, no injected-by annotations remain in the
# namespace, so the cascade would find no Deployments to annotate.
log "waiting up to ${SAMPLE_CASCADE_TIMEOUT}s for the initial cache-server pod to publish status.observedServerInstance"
deadline=$(($(date +%s) + SAMPLE_CASCADE_TIMEOUT))
baseline_server_instance=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  baseline_server_instance=$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.observedServerInstance}' 2>/dev/null || true)
  if [ -n "$baseline_server_instance" ]; then break; fi
  sleep 2
done
if [ -z "$baseline_server_instance" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  kubectl -n "$SAMPLE_NS" get pod -l app.kubernetes.io/instance=qwen-demo-cache -o wide || true
  fail "status.observedServerInstance not populated within ${SAMPLE_CASCADE_TIMEOUT}s; cannot exercise the cache-server restart cascade"
fi
log "baseline status.observedServerInstance=$baseline_server_instance"

# Force-delete the cache-server pod to simulate the OOM / restart trigger.
# The Deployment controller recreates it with a fresh UID.
cache_pod=$(kubectl -n "$SAMPLE_NS" get pod -l app.kubernetes.io/instance=qwen-demo-cache \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -z "$cache_pod" ]; then
  fail "no cache-server pod labeled app.kubernetes.io/instance=qwen-demo-cache found in $SAMPLE_NS"
fi
log "deleting cache-server pod $cache_pod to simulate restart"
# Don't mask the delete with `|| true` — a failed trigger here would
# silently look like the cascade isn't firing, which would be
# diagnosed as a controller bug instead of a smoke-script failure.
if ! kubectl -n "$SAMPLE_NS" delete pod "$cache_pod" \
     --force --grace-period=0 >/dev/null 2>&1; then
  kubectl -n "$SAMPLE_NS" get pod -l app.kubernetes.io/instance=qwen-demo-cache -o wide || true
  fail "failed to delete cache-server pod $cache_pod to simulate restart"
fi

# Wait for the controller to observe the new pod and update the latch.
log "waiting up to ${SAMPLE_CASCADE_TIMEOUT}s for status.observedServerInstance to flip to the replacement's server-instance identifier"
deadline=$(($(date +%s) + SAMPLE_CASCADE_TIMEOUT))
new_server_instance=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  cur=$(kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache \
    -o jsonpath='{.status.observedServerInstance}' 2>/dev/null || true)
  if [ -n "$cur" ] && [ "$cur" != "$baseline_server_instance" ]; then
    new_server_instance="$cur"
    break
  fi
  sleep 2
done
if [ -z "$new_server_instance" ]; then
  kubectl -n "$SAMPLE_NS" get cb qwen-demo-cache -o yaml || true
  kubectl -n "$SAMPLE_NS" get pod -l app.kubernetes.io/instance=qwen-demo-cache -o wide || true
  fail "status.observedServerInstance did not advance past $baseline_server_instance within ${SAMPLE_CASCADE_TIMEOUT}s"
fi
log "status.observedServerInstance flipped: $baseline_server_instance → $new_server_instance"

# Assert the engine Deployment's pod template carries the cascade trigger
# annotation set to the new server-instance identifier (the same value the
# controller wrote to status.observedServerInstance). The annotation is
# the mechanism that drives the rolling restart, so its absence here is
# a missed cascade.
log "waiting up to ${SAMPLE_CASCADE_TIMEOUT}s for the engine Deployment to receive the cascade trigger annotation"
deadline=$(($(date +%s) + SAMPLE_CASCADE_TIMEOUT))
trigger=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  trigger=$(kubectl -n "$SAMPLE_NS" get deploy qwen-engine \
    -o jsonpath='{.spec.template.metadata.annotations.inferencecache\.io/cache-server-restart-trigger}' \
    2>/dev/null || true)
  if [ "$trigger" = "$new_server_instance" ]; then break; fi
  sleep 2
done
if [ "$trigger" != "$new_server_instance" ]; then
  kubectl -n "$SAMPLE_NS" get deploy qwen-engine -o yaml || true
  fail "engine Deployment cascade trigger=$trigger, want $new_server_instance (the cache-server restart did not cascade)"
fi
log "engine Deployment qwen-engine carries inferencecache.io/cache-server-restart-trigger=$trigger"

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

# --- Events-only CacheBackend end-to-end -----------------------------------
# Exercises spec.integration.mode=EventsOnly (the routing-only integration) on
# the running cluster. The operator-facing contract for an events-only backend
# is the inverse of a managed one: the reconciler provisions NO owned Deployment
# and NO owned Service, status.endpoint stays EMPTY (no server address to
# publish), and readiness runs the same KV-event gate as a managed backend.
#
# The default install wires no --kvevent-subscriber-image, so no subscriber
# sidecar is injected and no KV events ever flow. The events-only backend is
# server-less, so it is "up" the moment it exists (status.firstAvailableAt
# latches immediately) and the gate parks it at Ready=False/AwaitingFirstKVEvent
# inside the firstEventTimeout window — exactly the managed sample's
# no-KV-event-source assertion above, but with no Deployment to wait on. The
# managed-only advisory conditions (FunctionalProbeOK / EngineKernelsHealthy /
# T2Degraded / EngineCompatibility) must be ABSENT — events-only has no server
# to probe, loads no LMCache connector whose native kernels need checking, has
# no tier-2, and injects no connector that could be incompatible.
# We assert the server-less + empty-endpoint + KV-gate contract rather than a
# positive KV event, since the smoke's engine stand-in emits none.
log "exercising Events-only CacheBackend end-to-end in namespace $EVENTSONLY_SMOKE_NS"
kubectl create namespace "$EVENTSONLY_SMOKE_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Apply the COMMITTED events-only sample so it is exercised end-to-end and
# cannot silently drift — every other backend type's smoke phase applies its
# config/samples/ manifest (with-engine, external, cachepolicy, cachetenant),
# so this one must too rather than hand-typing a private inline copy. The
# sample's metadata.name is cachebackend-events-only == the default
# $EVENTSONLY_SMOKE_CB_NAME; pin the name via a tmp copy so an overridden
# tunable still resolves, and set the namespace with -n (the sample is
# namespace-less, like the other samples). type=LMCache, integration.mode=
# EventsOnly, backendConfig.model set, no spec.endpoint (rejected on
# non-External) and no spec.autoscaling (rejected for events-only). The
# sample's engineSelector is irrelevant here: events-only provisions no
# workload and the assertions below are all about the CR's own reconcile, so
# no matched engine pod is required.
eo_sample_tmp="$(mktemp "$tmpdir/sample-events-only.XXXXXX")"
sed "s|^  name: cachebackend-events-only\$|  name: $EVENTSONLY_SMOKE_CB_NAME|" \
  config/samples/cachebackend-events-only.yaml > "$eo_sample_tmp"
kubectl -n "$EVENTSONLY_SMOKE_NS" apply -f "$eo_sample_tmp" >/dev/null \
  || fail "kubectl apply events-only sample (config/samples/cachebackend-events-only.yaml) failed"

# Wait for the reconciler to take the events-only path: Ready published by the
# KV-event gate (False/AwaitingFirstKVEvent before any event), status.endpoint
# empty, observedGeneration advanced.
log "waiting up to ${EVENTSONLY_BACKEND_TIMEOUT}s for events-only CR to reach Ready=False/AwaitingFirstKVEvent"
deadline=$(($(date +%s) + EVENTSONLY_BACKEND_TIMEOUT))
eo_ready_status=""
eo_ready_reason=""
eo_observed_generation=""
until [ "$eo_ready_status" = "False" ] && \
      [ "$eo_ready_reason" = "AwaitingFirstKVEvent" ] && \
      [ -n "$eo_observed_generation" ]; do
  eo_ready_status="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  eo_ready_reason="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
  eo_observed_generation="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" \
    -o jsonpath='{.status.observedGeneration}' 2>/dev/null || true)"
  if [ "$(date +%s)" -ge "$deadline" ]; then
    kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" -o yaml || true
    fail "events-only CR didn't converge: Ready=$eo_ready_status/$eo_ready_reason observedGeneration=$eo_observed_generation (want False/AwaitingFirstKVEvent + advanced generation)"
  fi
  sleep 1
done
log "events-only CR Ready=$eo_ready_status/$eo_ready_reason observedGeneration=$eo_observed_generation"

# status.endpoint must stay EMPTY — events-only provisions no server, so there
# is no address to publish. (A managed/External backend mirrors one here.)
eo_endpoint="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" \
  -o jsonpath='{.status.endpoint}' 2>/dev/null || true)"
if [ -n "$eo_endpoint" ]; then
  kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" -o yaml || true
  fail "events-only status.endpoint=$eo_endpoint, want empty (no provisioned server)"
fi

# No owned Deployment, no owned Service. The namespace is dedicated to this
# phase and otherwise empty, so a flat count of zero is the right assertion
# (matches the External phase's reasoning).
eo_dep_count="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get deploy -o name 2>/dev/null | wc -l | tr -d ' ')"
eo_svc_count="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get svc -o name 2>/dev/null | wc -l | tr -d ' ')"
if [ "$eo_dep_count" != "0" ] || [ "$eo_svc_count" != "0" ]; then
  kubectl -n "$EVENTSONLY_SMOKE_NS" get deploy,svc
  fail "events-only CR rendered controller-owned workload (deploy=$eo_dep_count svc=$eo_svc_count, want 0/0)"
fi
log "no Deployment or Service in $EVENTSONLY_SMOKE_NS (events-only backend skipped provisioning)"

# The KV-event gate latch must be unset: no KV event source exists (no
# subscriber image wired), so the controller must never write
# status.firstKVEventObservedAt — same invariant as the managed gate phase.
eo_latch="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" \
  -o jsonpath='{.status.firstKVEventObservedAt}' 2>/dev/null || true)"
if [ -n "$eo_latch" ]; then
  kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" -o yaml || true
  fail "events-only status.firstKVEventObservedAt=$eo_latch, want unset (no KV event source exists)"
fi

# The managed-only advisory conditions must be ABSENT on an events-only backend:
# events-only publishes only Ready/Degraded/Progressing. EngineCompatibility is
# included because events-only injects no connector (nothing to be incompatible
# with) and an Offload->EventsOnly flip clears any prior verdict — this asserts
# that clear holds at install level, not just in envtest.
for eo_cond in FunctionalProbeOK EngineKernelsHealthy T2Degraded EngineCompatibility; do
  eo_present="$(kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" \
    -o jsonpath="{.status.conditions[?(@.type=='$eo_cond')].type}" 2>/dev/null || true)"
  if [ -n "$eo_present" ]; then
    kubectl -n "$EVENTSONLY_SMOKE_NS" get cb "$EVENTSONLY_SMOKE_CB_NAME" -o yaml || true
    fail "events-only CR published managed-only condition $eo_cond, want absent"
  fi
done
log "events-only CR publishes only Ready/Degraded/Progressing (FunctionalProbeOK/EngineKernelsHealthy/T2Degraded/EngineCompatibility absent)"

# Negative-path admission: the misconfiguration the events-only validator
# guards. An EventsOnly + spec.type=External pair must be rejected at admission
# (events-only wires no connector; External provisions a server one would dial).
eo_reject_output="$(kubectl apply -f - <<EOF 2>&1 || true
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: smoke-reject-events-only-external
  namespace: $EVENTSONLY_SMOKE_NS
spec:
  type: External
  endpoint: external-cache.example:8200
  integration:
    engine: vllm
    mode: EventsOnly
EOF
)"
if ! grep -q "is incompatible with spec.type" <<<"$eo_reject_output"; then
  fail "admission did not reject EventsOnly+External as expected; got: $eo_reject_output"
fi
log "admission rejected EventsOnly+External misconfiguration"

# Clean up — keeps the cluster reusable for KEEP_CLUSTER=1 reruns.
kubectl delete cb -n "$EVENTSONLY_SMOKE_NS" "$EVENTSONLY_SMOKE_CB_NAME" --ignore-not-found --wait=false >/dev/null || true
kubectl delete namespace "$EVENTSONLY_SMOKE_NS" --ignore-not-found --wait=false >/dev/null || true

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
      -d "{\"version\":3,\"policies\":[]}" \
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

# --- /probe auth assertion ------------------------------------------------
# /probe is the controller-driven functional self-test endpoint; same
# controller ServiceAccount identity as /snapshot and /policy. The complementary
# UNAUTHENTICATED-rejection half is what this section checks — a regression
# that wired /probe outside the shared auth profile would let any pod that
# can reach :8081 drive a synthetic round-trip AND, since the CacheBackend
# reconciler now consumes the result to publish FunctionalProbeOK and
# downgrade Ready, observe (or trigger forged) Ready transitions on every
# managed backend. Sends a valid ProbeRequest body so the rejection cannot
# be misattributed to a 400; the only valid outcomes are 401 (auth) or
# curl_failed:28 (NetworkPolicy drop under an enforcing CNI). Mirror of the
# /policy probe above.
log "asserting unauthenticated /probe POST from a side pod is rejected"
SIDE_POD_PROBE="ic-probe-probe"
kubectl -n "$NAMESPACE" delete pod "$SIDE_POD_PROBE" --ignore-not-found --wait=true >/dev/null 2>&1 || true
if ! kubectl -n "$NAMESPACE" run "$SIDE_POD_PROBE" --image=curlimages/curl:8.10.1 --restart=Never \
  --command -- /bin/sh -c '
    # POST a minimal valid ProbeRequest so any non-2xx response must be an
    # auth rejection, not a body-parse rejection.
    curl -sS -m 5 -o /dev/null -w "%{http_code}" \
      -H "Content-Type: application/json" \
      -d "{\"backend\":\"smoke\",\"model\":\"smoke-model\",\"hashScheme\":\"vllm\"}" \
      http://inference-cache-server:8081/probe || echo "curl_failed:$?"
  ' >/tmp/probe-probe-create.log 2>&1; then
  cat /tmp/probe-probe-create.log >&2 || true
  fail "kubectl run $SIDE_POD_PROBE failed; cannot run /probe auth assertion"
fi

# 90s budget + describe-pod fallback matches the /snapshot and /policy probes
# above — the surrounding phases (External-backend, audience-binding) can
# leave the kubelet busy reaping Terminating pods.
for _ in $(seq 1 90); do
  phase="$(kubectl -n "$NAMESPACE" get pod "$SIDE_POD_PROBE" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
    break
  fi
  sleep 1
done
probe_probe_out="$(kubectl -n "$NAMESPACE" logs "$SIDE_POD_PROBE" 2>/dev/null || true)"
if [ -z "$probe_probe_out" ]; then
  kubectl -n "$NAMESPACE" describe pod "$SIDE_POD_PROBE" >&2 || true
fi
kubectl -n "$NAMESPACE" delete pod "$SIDE_POD_PROBE" --grace-period=0 --force >/dev/null 2>&1 || true

# Acceptable outcomes (either gate sufficient) — see /snapshot probe above
# for the rationale on why 7 (ECONNREFUSED) is NOT accepted (an enforcing
# CNI drops, it does not RST; accepting 7 would let "listener crashed"
# pass). 200 (synthesis ran unauthenticated) is the regression this whole
# section exists to prevent.
case "$probe_probe_out" in
  "401"|*"curl_failed:28"*)
    log "unauthenticated /probe POST rejected (probe output: $probe_probe_out)"
    ;;
  *)
    fail "unauthenticated /probe POST was not rejected (or curl failed for an unexpected reason); got: $probe_probe_out"
    ;;
esac

# --- Audience-binding assertion (/snapshot, /policy, AND /probe) ----------
# Audience-binding follow-up to the bearer-token gate. The controller pod
# in production mounts THREE ServiceAccount tokens:
#   1. The default automount at /var/run/secrets/kubernetes.io/serviceaccount/token
#      — audience = the apiserver. Used by the controller-runtime client.
#   2. A projected volume at /var/run/secrets/inferencecache.io/controller-token/token
#      — audience = "inferencecache.io/controller". Used by the CacheIndex
#      poller and functional-probe driver (/snapshot + /probe).
#   3. A projected volume at /var/run/secrets/inferencecache.io/policy-token/token
#      — audience = "inferencecache.io/policy". Used by the CachePolicy pusher
#      (/policy).
# The server passes TokenReviewSpec.Audiences=["inferencecache.io/controller"]
# on /snapshot + /probe reviews, and ["inferencecache.io/policy"] on /policy
# reviews, so a default-audience token MUST come back 401 on all three even
# though the SA identity (controller-manager) would otherwise be admitted.
#
# Why a single probe pod with seven scrapes: it covers the positive path for
# each endpoint's intended audience, the default-audience negative path for all
# three endpoints, and the cross-endpoint "controller token cannot push policy"
# case. Seven small checks, one pod, one assertion: "all outcomes match the
# audience contract."
#
# Scoping — what each smoke gate actually catches (the assertions are
# complementary, not redundant):
#   - The CacheIndex assertion earlier (cacheindex/cluster-default.status
#     .observedServer populates within ~60s) is what proves the REAL
#     controller's controller-token projected-volume manifest, the controller
#     binary's BearerTokenPath, the server's flag, and the middleware all agree
#     end-to-end. If config/manager/manager.yaml drifts (audience renamed,
#     mount path moved, expirationSeconds zeroed), the real poller's
#     scrape returns 401 and the CR's observedServer stays empty, failing
#     that earlier gate. The CachePolicy adoption assertion earlier is the
#     equivalent real-controller check for the policy-token projection.
#   - THIS probe asserts only server-side behavior: that each endpoint's
#     intended audience-bound token admits and a default-audience token of the
#     same SA is rejected on all three endpoints. It uses inline duplicate
#     volume specs so it can run even if the controller's manifest is broken
#     (which would otherwise mask the server-side check). It does NOT catch
#     drift in
#     config/manager/manager.yaml; that's the earlier gate's job.
log "asserting audience binding on /snapshot, /policy, and /probe"
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
      # of the seven scrapes, we emit "K=curl_failed:N" so the case statement
      # below surfaces the exit code, not the empty-status default.
      controller_token=\$(cat /var/run/secrets/inferencecache.io/controller-token/token 2>/dev/null || echo "")
      policy_token=\$(cat /var/run/secrets/inferencecache.io/policy-token/token 2>/dev/null || echo "")
      default_token=\$(cat /var/run/secrets/kubernetes.io/serviceaccount/token 2>/dev/null || echo "")
      if [ -z "\$controller_token" ]; then echo "controller_token_missing"; exit 0; fi
      if [ -z "\$policy_token" ]; then echo "policy_token_missing"; exit 0; fi
      if [ -z "\$default_token" ]; then echo "default_token_missing"; exit 0; fi
      # GET /snapshot — controller-audience must 200, default-audience must 401.
      sa_ctrl=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$controller_token" "http://inference-cache-server:8081/snapshot" || echo "curl_failed:\$?")
      sa_def=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$default_token" "http://inference-cache-server:8081/snapshot" || echo "curl_failed:\$?")
      # POST /policy — policy-audience must 204; controller/default audiences must 401.
      # Body is a minimal valid PolicySnapshot so any non-2xx is auth-side, not body-parse.
      pa_policy=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$policy_token" -H "Content-Type: application/json" -d '{"version":5,"policies":[],"tenants":[]}' "http://inference-cache-server:8081/policy" || echo "curl_failed:\$?")
      pa_ctrl=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$controller_token" -H "Content-Type: application/json" -d '{"version":5,"policies":[],"tenants":[]}' "http://inference-cache-server:8081/policy" || echo "curl_failed:\$?")
      pa_def=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$default_token" -H "Content-Type: application/json" -d '{"version":5,"policies":[],"tenants":[]}' "http://inference-cache-server:8081/policy" || echo "curl_failed:\$?")
      # POST /probe — controller-audience must 200, default-audience must 401.
      # Body is a minimal valid ProbeRequest so any non-2xx is auth-side, not body-parse.
      pr_ctrl=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$controller_token" -H "Content-Type: application/json" -d '{"backend":"smoke","model":"smoke-model","hashScheme":"vllm"}' "http://inference-cache-server:8081/probe" || echo "curl_failed:\$?")
      pr_def=\$(curl -sS -m 5 -o /dev/null -w "%{http_code}" -H "Authorization: Bearer \$default_token" -H "Content-Type: application/json" -d '{"backend":"smoke","model":"smoke-model","hashScheme":"vllm"}' "http://inference-cache-server:8081/probe" || echo "curl_failed:\$?")
      echo "snapshot_ctrl=\$sa_ctrl snapshot_def=\$sa_def policy_policy=\$pa_policy policy_ctrl=\$pa_ctrl policy_def=\$pa_def probe_ctrl=\$pr_ctrl probe_def=\$pr_def"
    volumeMounts:
    - name: controller-token
      mountPath: /var/run/secrets/inferencecache.io/controller-token
      readOnly: true
    - name: policy-token
      mountPath: /var/run/secrets/inferencecache.io/policy-token
      readOnly: true
  volumes:
  - name: controller-token
    projected:
      sources:
      - serviceAccountToken:
          path: token
          audience: inferencecache.io/controller
          expirationSeconds: 3600
  - name: policy-token
    projected:
      sources:
      - serviceAccountToken:
          path: token
          audience: inferencecache.io/policy
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
#   snapshot_ctrl=200 snapshot_def=401 policy_policy=204 policy_ctrl=401 policy_def=401 probe_ctrl=200 probe_def=401
# Anything else is a regression. curl_failed:28 splits out so an operator
# triaging a red smoke knows whether to look at NetworkPolicy (timeout) vs
# Service/listener (other curl exit).
case "$audience_probe" in
  "snapshot_ctrl=200 snapshot_def=401 policy_policy=204 policy_ctrl=401 policy_def=401 probe_ctrl=200 probe_def=401")
    log "audience binding verified — controller-audience token admitted on /snapshot and /probe, policy-audience token admitted on /policy, default-audience token rejected everywhere, and controller-audience token rejected on /policy"
    ;;
  *controller_token_missing*)
    fail "probe pod is missing /var/run/secrets/inferencecache.io/controller-token/token — the projected volume did not mount; check the probe-pod manifest above (and config/manager/manager.yaml for the production analog)"
    ;;
  *policy_token_missing*)
    fail "probe pod is missing /var/run/secrets/inferencecache.io/policy-token/token — the projected volume did not mount; check the probe-pod manifest above (and config/manager/manager.yaml for the production analog)"
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
    fail "audience-binding probe got unexpected outcome: $audience_probe (want 'snapshot_ctrl=200 snapshot_def=401 policy_policy=204 policy_ctrl=401 policy_def=401 probe_ctrl=200 probe_def=401')"
    ;;
esac

# --- /probe functional-self-test result assertion -------------------------
# The audience-binding section above asserts /probe returned HTTP 200 with the
# controller-audience token, but a deployed handler can also return 200 with a
# per-stage `failed`. This section drives one authenticated /probe call,
# captures the JSON body, and asserts ingest=ok, routing=ok, t2=skipped.
# No T2Prober is wired into the server today, so Stage C always reports
# skipped on a clean install — when one is plumbed in, this assertion
# tightens to t2=ok. A regression that flips ingest or routing to failed
# on a clean install would be a clear signal that the cache-plane
# internal round-trip itself is broken — exactly the class of bug the
# probe exists to catch.
log "asserting authenticated /probe returns ingest=ok, routing=ok, t2=skipped"
PROBE_RESULT_POD="ic-probe-result"
kubectl -n "$NAMESPACE" delete pod "$PROBE_RESULT_POD" --ignore-not-found --wait=true >/dev/null 2>&1 || true
probe_result_yaml=$(cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $PROBE_RESULT_POD
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
      controller_token=\$(cat /var/run/secrets/inferencecache.io/controller-token/token 2>/dev/null || echo "")
      if [ -z "\$controller_token" ]; then echo "controller_token_missing"; exit 0; fi
      # Capture body to stdout — the smoke parses ingest/routing/t2 from it.
      curl -sS -m 5 -H "Authorization: Bearer \$controller_token" \\
        -H "Content-Type: application/json" \\
        -d '{"backend":"smoke","model":"smoke-model","hashScheme":"vllm"}' \\
        http://inference-cache-server:8081/probe || echo "curl_failed:\$?"
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
if ! echo "$probe_result_yaml" | kubectl apply -f - >/tmp/probe-result-create.log 2>&1; then
  cat /tmp/probe-result-create.log >&2 || true
  fail "kubectl apply for $PROBE_RESULT_POD failed; cannot run /probe result assertion"
fi
for _ in $(seq 1 90); do
  phase="$(kubectl -n "$NAMESPACE" get pod "$PROBE_RESULT_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
    break
  fi
  sleep 1
done
probe_result_body="$(kubectl -n "$NAMESPACE" logs "$PROBE_RESULT_POD" 2>/dev/null || true)"
if [ -z "$probe_result_body" ]; then
  kubectl -n "$NAMESPACE" describe pod "$PROBE_RESULT_POD" >&2 || true
fi
kubectl -n "$NAMESPACE" delete pod "$PROBE_RESULT_POD" --grace-period=0 --force >/dev/null 2>&1 || true
log "probe result body: $probe_result_body"
case "$probe_result_body" in
  *controller_token_missing*)
    fail "probe-result pod is missing /var/run/secrets/inferencecache.io/controller-token/token"
    ;;
  *"curl_failed:"*)
    fail "probe-result curl failed: $probe_result_body"
    ;;
esac
# Parse the three stage values; reject anything that isn't the expected
# default posture (ingest=ok, routing=ok, t2=skipped). The default-install
# CacheBackend has no engine pods reporting state, but the probe synthesizes
# its own — so Stage A + B must always pass on a clean install regardless of
# workload. Stage C is "skipped" because no T2Prober is wired in this revision.
case "$probe_result_body" in
  *'"ingest":"ok"'*'"routing":"ok"'*'"t2":"skipped"'*)
    log "probe result matches expected default posture (ingest=ok, routing=ok, t2=skipped)"
    ;;
  *)
    fail "probe result does not match expected default posture; want ingest=ok routing=ok t2=skipped, got: $probe_result_body"
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
# 7) and assert the identical fail-open result (reason_code=NO_HINT) — proving
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

# --- kernel-check init-container injection shape (assertion 14) ------------
# Block 1: prove the mutating pod webhook injects lmcache-kernel-check when
# auto mode fires (GPU-requesting container). A dedicated LMCache CacheBackend
# (kc-inject) is created in the kernel-check smoke namespace so the webhook
# can resolve a matching backend at pod admission time. The engine pod carries
# the backend's engineSelector label (app=kc-inject-engine), requests
# nvidia.com/gpu → auto mode injects the init container. The kind node has no
# GPU, so the pod stays Pending — that is expected and correct; the webhook
# runs at admission, before scheduling. Only the SPEC is inspected here (no
# status, no pod running).
log "asserting lmcache-kernel-check init-container injection shape (assertion 14)"
# Start from a clean namespace: on a rerun against a kept cluster, leftover pods
# would be UPDATED by `kubectl apply` (the mutating webhook only fires on
# CREATE), so the assertions could pass/fail on stale injected specs. Delete and
# recreate so every fixture pod is created fresh and re-admitted.
kubectl delete namespace "$KERNEL_CHECK_SMOKE_NS" --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
kubectl create namespace "$KERNEL_CHECK_SMOKE_NS" --dry-run=client -o yaml \
  | kubectl apply -f - >/dev/null

# Apply the CacheBackend first so status.endpoint is published before the
# engine pod is admitted (the webhook fail-opens when endpoint is absent).
KC_INJECT_CB="kc-inject"
KC_INJECT_POD="kc-inject-probe"
kubectl apply -f - >/dev/null <<EOF
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: $KC_INJECT_CB
  namespace: $KERNEL_CHECK_SMOKE_NS
spec:
  type: LMCache
  engineSelector:
    matchLabels:
      app: kc-inject-engine
  backendConfig:
    serverImage: $SAMPLE_CACHE_SERVER_IMAGE
EOF

# Wait for the controller to publish status.endpoint before admitting the engine
# pod. The webhook fail-opens (no injection) when endpoint is absent.
log "waiting up to 60s for kc-inject status.endpoint before admitting the engine pod"
kc_inject_deadline=$(($(date +%s) + 60))
kc_inject_endpoint=""
while [ "$(date +%s)" -lt "$kc_inject_deadline" ]; do
  kc_inject_endpoint="$(kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_INJECT_CB" \
    -o jsonpath='{.status.endpoint}' 2>/dev/null || true)"
  if [ -n "$kc_inject_endpoint" ]; then break; fi
  sleep 2
done
if [ -z "$kc_inject_endpoint" ]; then
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_INJECT_CB" -o yaml || true
  fail "$KC_INJECT_CB status.endpoint not published within 60s; cannot exercise the webhook injection (endpoint must be set before engine pod CREATE)"
fi

kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $KC_INJECT_POD
  namespace: $KERNEL_CHECK_SMOKE_NS
  labels:
    app: kc-inject-engine
spec:
  restartPolicy: Never
  # Pin to a non-existent node so the pod stays Pending on ANY cluster (incl. a
  # GPU-capable one): we only inspect the webhook-mutated spec, and busybox has
  # no python3, so it must never actually run the kernel-check init container.
  nodeSelector:
    inferencecache.io/smoke-never-schedule: "true"
  containers:
  - name: vllm
    image: busybox:1.36
    command: ["sleep", "3600"]
    resources:
      limits:
        nvidia.com/gpu: "1"
EOF

# The webhook runs synchronously at admission, so the init container is present
# immediately. Poll briefly as a defensive circuit-breaker against a slow first
# admission (cert warm-up on a cold cluster).
log "waiting up to 30s for lmcache-kernel-check init container to be present in the pod spec"
deadline=$(($(date +%s) + 30))
kc_injected_name=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  kc_injected_name="$(kubectl -n "$KERNEL_CHECK_SMOKE_NS" get pod "$KC_INJECT_POD" \
    -o jsonpath='{.spec.initContainers[?(@.name=="lmcache-kernel-check")].name}' 2>/dev/null || true)"
  if [ "$kc_injected_name" = "lmcache-kernel-check" ]; then break; fi
  sleep 2
done
if [ "$kc_injected_name" != "lmcache-kernel-check" ]; then
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get pod "$KC_INJECT_POD" -o yaml || true
  fail "lmcache-kernel-check init container not injected into GPU-requesting engine pod after 30s (webhook auto mode did not fire)"
fi

# Assert the init container's image equals the engine container's image
# (busybox:1.36). The adapter must copy the engine image so no extra pull occurs.
kc_init_image="$(kubectl -n "$KERNEL_CHECK_SMOKE_NS" get pod "$KC_INJECT_POD" \
  -o jsonpath='{.spec.initContainers[?(@.name=="lmcache-kernel-check")].image}' 2>/dev/null || true)"
if [ "$kc_init_image" != "busybox:1.36" ]; then
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get pod "$KC_INJECT_POD" -o yaml || true
  fail "lmcache-kernel-check init container image=$kc_init_image, want busybox:1.36 (adapter must reuse the engine container image)"
fi
log "lmcache-kernel-check init container injected; image=$kc_init_image (matches engine container — no extra pull)"

# --- kernel-check report-only FAIL condition path (assertion 15) -----------
# Block 2: prove the report-only FAIL path is fail-open and surfaces
# EngineKernelsHealthy=False/KernelLoadFailed. A dedicated managed LMCache
# CacheBackend (kc-cond) is annotated report-only. The matching engine pod
# uses python:3.11-slim: the init container runs the kernel-check script, which
# calls find_spec("lmcache") → None → emits "FAIL: lmcache not importable" to
# /dev/termination-log and exits 0 (fail-open; STRICT unset). The main container
# starts normally, so the pod reaches Ready — proving fail-open semantics.
# The C2 reconciler reads the termination message from
# status.initContainerStatuses and publishes EngineKernelsHealthy=False /
# reason=KernelLoadFailed on the CacheBackend.
#
# python:3.11-slim was chosen deliberately: it has python3 (so the init container
# runs successfully, producing our FAIL: message) but does NOT have lmcache
# installed (find_spec returns None → the FAIL: branch fires). Using a non-python
# image (e.g. pause) would produce a 127 exit / KernelCheckError, not
# KernelLoadFailed, which would break the condition assertion.
log "asserting report-only FAIL path: EngineKernelsHealthy=False/KernelLoadFailed (assertion 15)"
KC_COND_CB="kc-cond"
KC_COND_ENGINE_POD="kc-cond-engine"
kubectl apply -f - >/dev/null <<EOF
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: $KC_COND_CB
  namespace: $KERNEL_CHECK_SMOKE_NS
  annotations:
    inferencecache.io/lmcache-kernel-check: "report-only"
spec:
  type: LMCache
  engineSelector:
    matchLabels:
      app: kc-cond-engine
  backendConfig:
    serverImage: $SAMPLE_CACHE_SERVER_IMAGE
EOF

# Wait for status.endpoint before admitting the engine pod. The webhook
# fail-opens (no init-container injection) when the endpoint is absent; without
# the init container the FAIL: termination message is never written, and the
# reconciler cannot publish EngineKernelsHealthy. The endpoint is typically
# published within a few seconds of CB create.
log "waiting up to 60s for kc-cond status.endpoint before admitting the engine pod"
kc_cond_deadline=$(($(date +%s) + 60))
kc_cond_endpoint=""
while [ "$(date +%s)" -lt "$kc_cond_deadline" ]; do
  kc_cond_endpoint="$(kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_COND_CB" \
    -o jsonpath='{.status.endpoint}' 2>/dev/null || true)"
  if [ -n "$kc_cond_endpoint" ]; then break; fi
  sleep 2
done
if [ -z "$kc_cond_endpoint" ]; then
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_COND_CB" -o yaml || true
  fail "$KC_COND_CB status.endpoint not published within 60s; cannot exercise the report-only condition path"
fi

# Apply the engine pod. python:3.11-slim is small (~50 MB) and always present on
# Docker Hub, so this phase does not pay a multi-GB pull. No GPU request: the
# report-only mode injects regardless of GPU (the annotation overrides auto mode).
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $KC_COND_ENGINE_POD
  namespace: $KERNEL_CHECK_SMOKE_NS
  labels:
    app: kc-cond-engine
spec:
  restartPolicy: Never
  containers:
  - name: vllm
    image: python:3.11-slim
    # The engine container is named "vllm", so the mutating webhook injects the
    # LMCache engine wiring onto it — including the --kv-transfer-config ARG. A
    # bare command ["sleep","3600"] would then run as
    # `sleep 3600 --kv-transfer-config {...}` and exit 1 ("invalid time
    # interval"). Wrapping in `sh -c` makes the injected args harmless
    # positional params to the script, so the stand-in stays alive to prove
    # the init container's report-only fail-open behavior.
    command: ["/bin/sh", "-c", "sleep 3600"]
EOF

# Wait for the pod to become Ready. The init container runs the kernel-check
# script (exits 0 because report-only); then the main container starts and
# sleep 3600 runs. python:3.11-slim is ~50 MB; the budget covers a cold pull.
log "waiting up to ${KERNEL_CHECK_POD_TIMEOUT}s for $KC_COND_ENGINE_POD to become Ready (proves report-only fail-open)"
if ! kubectl -n "$KERNEL_CHECK_SMOKE_NS" wait \
     --for=condition=Ready pod/"$KC_COND_ENGINE_POD" \
     --timeout="${KERNEL_CHECK_POD_TIMEOUT}s" >/dev/null 2>&1; then
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get pod "$KC_COND_ENGINE_POD" -o yaml || true
  fail "$KC_COND_ENGINE_POD did not become Ready within ${KERNEL_CHECK_POD_TIMEOUT}s — report-only did not fail-open (init container may have exited non-zero, or the image pull stalled)"
fi
log "$KC_COND_ENGINE_POD is Ready — report-only mode did not block the engine pod"

# Poll for EngineKernelsHealthy=False on the CacheBackend. The reconciler reads
# the init container's termination message from status.initContainerStatuses on
# the matched pod; it fires on the next reconcile after the init container
# completes. The budget absorbs one RequeueAfter cycle + pod-list round-trip.
log "waiting up to ${KERNEL_CHECK_COND_TIMEOUT}s for EngineKernelsHealthy=False on $KC_COND_CB"
deadline=$(($(date +%s) + KERNEL_CHECK_COND_TIMEOUT))
kc_cond_status=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  kc_cond_status="$(kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_COND_CB" \
    -o jsonpath='{.status.conditions[?(@.type=="EngineKernelsHealthy")].status}' 2>/dev/null || true)"
  if [ "$kc_cond_status" = "False" ]; then break; fi
  sleep 3
done
if [ "$kc_cond_status" != "False" ]; then
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_COND_CB" -o yaml || true
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get pod "$KC_COND_ENGINE_POD" -o yaml || true
  fail "EngineKernelsHealthy condition status=$kc_cond_status on $KC_COND_CB after ${KERNEL_CHECK_COND_TIMEOUT}s; want False (reconciler did not read the FAIL: termination message)"
fi

kc_cond_reason="$(kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_COND_CB" \
  -o jsonpath='{.status.conditions[?(@.type=="EngineKernelsHealthy")].reason}' 2>/dev/null || true)"
if [ "$kc_cond_reason" != "KernelLoadFailed" ]; then
  kubectl -n "$KERNEL_CHECK_SMOKE_NS" get cb "$KC_COND_CB" -o yaml || true
  fail "EngineKernelsHealthy reason=$kc_cond_reason on $KC_COND_CB; want KernelLoadFailed (FAIL: termination message must map to this reason)"
fi
log "EngineKernelsHealthy=False/KernelLoadFailed on $KC_COND_CB — report-only FAIL path wired end-to-end"

# The validating webhook must reject an invalid lmcache-kernel-check annotation
# value: a typo (e.g. "strcit") would otherwise silently fall back to report-only
# and disable the fail-closed strict gate. Proves the rule on the real install.
log "asserting an invalid lmcache-kernel-check annotation is rejected at admission"
kc_badannot_yaml="$(cat <<EOF
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: kc-badannot
  namespace: $KERNEL_CHECK_SMOKE_NS
  annotations:
    inferencecache.io/lmcache-kernel-check: "strcit"
spec:
  type: LMCache
  engineSelector:
    matchLabels:
      app: kc-badannot
EOF
)"
if kc_badannot_out="$(printf '%s\n' "$kc_badannot_yaml" | kubectl apply -f - 2>&1)"; then
  echo "$kc_badannot_out"
  fail "CacheBackend with an invalid lmcache-kernel-check annotation was admitted; the validating webhook should reject it"
fi
if ! grep -q "must be one of" <<<"$kc_badannot_out"; then
  echo "$kc_badannot_out"
  fail "invalid kernel-check annotation rejected, but not by the expected rule (missing 'must be one of' message)"
fi
log "invalid lmcache-kernel-check annotation rejected at admission"

# Cleanup: drop the kernel-check smoke namespace.
kubectl delete namespace "$KERNEL_CHECK_SMOKE_NS" \
  --wait=false --ignore-not-found=true >/dev/null 2>&1 || true

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

# --- inferencecache doctor CLI assertion -----------------------------------
# The operator-facing `inferencecache doctor` CLI must run end-to-end against a
# real install, not just envtest. Build it and run the config-only checks (no
# live server probe required) against a freshly-applied CacheBackend, asserting
# it emits the documented JSON envelope, actually inspected the backend (a CB0xx
# finding), and that its process exit code matches the reported summary.exitCode
# — the CI-gating contract operators rely on.
log "asserting 'inferencecache doctor' runs against the live install"
mkdir -p "$LOG_DIR"
DOCTOR_BIN="$LOG_DIR/inferencecache"
if ! go build -o "$DOCTOR_BIN" ./cmd/inferencecache >"$LOG_DIR/doctor-build.log" 2>&1; then
  cat "$LOG_DIR/doctor-build.log" >&2 || true
  fail "could not build cmd/inferencecache for the doctor smoke assertion"
fi
DOCTOR_NS=doctor-smoke
kubectl create namespace "$DOCTOR_NS" >/dev/null 2>&1 || true
kubectl apply -n "$DOCTOR_NS" -f config/samples/cache_v1alpha1_cachebackend.yaml >/dev/null
doctor_rc=0
"$DOCTOR_BIN" doctor --config-only --namespace "$DOCTOR_NS" --output json --no-color \
  >"$LOG_DIR/doctor.json" 2>"$LOG_DIR/doctor.err" || doctor_rc=$?
kubectl delete namespace "$DOCTOR_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true
case "$doctor_rc" in
  0|1|2) : ;;
  *) cat "$LOG_DIR/doctor.json" "$LOG_DIR/doctor.err" >&2 || true
     fail "inferencecache doctor exited $doctor_rc (want a CI-gating 0/1/2)" ;;
esac
if ! grep -q '"summary"' "$LOG_DIR/doctor.json" || ! grep -q '"findings"' "$LOG_DIR/doctor.json"; then
  cat "$LOG_DIR/doctor.json" >&2 || true
  fail "inferencecache doctor did not emit the expected JSON envelope (summary + findings)"
fi
if ! grep -q '"code": "CB0' "$LOG_DIR/doctor.json"; then
  cat "$LOG_DIR/doctor.json" >&2 || true
  fail "inferencecache doctor produced no CacheBackend (CB0xx) finding despite an applied backend"
fi
if ! grep -q "\"exitCode\": $doctor_rc" "$LOG_DIR/doctor.json"; then
  cat "$LOG_DIR/doctor.json" >&2 || true
  fail "doctor process exit ($doctor_rc) does not match the reported summary.exitCode"
fi
log "inferencecache doctor ran against the live install (exit $doctor_rc; JSON envelope + CB finding present)"

# --- managed Mooncake backend smoke ----------------------------------------
# CacheBackend{type: Mooncake} is a new operator-facing surface, so it needs a
# real-install assertion, not just unit/envtest. The kvcacheai/mooncake image
# is intentionally NOT pulled here (heavy, and its entrypoint/ports are pending
# reference-stack validation); instead a busybox stand-in named `mooncake_master`
# accepts TCP on the RPC port so the controller-rendered readiness probe passes
# and the managed Deployment reaches Available. That proves the REAL installed
# controller reconciles type:Mooncake through ResolveCacheServer into a healthy
# workload + the mooncakestore:// RPC endpoint in status — the operator-visible
# contract envtest can't fully exercise (real install bundle + real controller
# image). The real engine-over-mooncakestore:// path stays for the reference stack.
MOONCAKE_SMOKE_NS="${MOONCAKE_SMOKE_NS:-mooncake-smoke}"
MOONCAKE_MASTER_IMAGE="${MOONCAKE_MASTER_IMAGE:-install-smoke-mooncake-master:$TAG}"
MOONCAKE_CB_NAME="cachebackend-mooncake"

log "building lightweight mooncake_master stand-in image=$MOONCAKE_MASTER_IMAGE"
mc_ctx="$(mktemp -d "$tmpdir/mooncake-master-context.XXXXXX")"
cat >"$mc_ctx/mooncake_master" <<'EOF'
#!/bin/sh
# Stand-in: ignore all flags (--rpc_port=..., metadata/metrics ports) and just
# accept TCP on the RPC port so the TCP-socket readiness probe passes.
while true; do
  nc -l -p 50051 >/dev/null 2>&1 || sleep 1
done
EOF
cat >"$mc_ctx/Dockerfile" <<'EOF'
FROM busybox:1.36
COPY mooncake_master /usr/local/bin/mooncake_master
RUN chmod +x /usr/local/bin/mooncake_master
EOF
docker build -t "$MOONCAKE_MASTER_IMAGE" "$mc_ctx" >/dev/null
"$KIND" load docker-image "$MOONCAKE_MASTER_IMAGE" --name "$KIND_CLUSTER" >/dev/null

log "creating namespace $MOONCAKE_SMOKE_NS"
kubectl create namespace "$MOONCAKE_SMOKE_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

mc_cb_tmp="$(mktemp "$tmpdir/mooncake-cb.XXXXXX")"
cp config/samples/cachebackend-mooncake.yaml "$mc_cb_tmp"
mc_escaped_image="$(printf '%s' "$MOONCAKE_MASTER_IMAGE" | sed 's/[&|\\]/\\&/g')"
sed -i.bak "s|serverImage: kvcacheai/mooncake:0.3.11.post1|serverImage: $mc_escaped_image|g" "$mc_cb_tmp"
rm -f "${mc_cb_tmp}.bak"

log "applying Mooncake CacheBackend"
kubectl -n "$MOONCAKE_SMOKE_NS" apply -f "$mc_cb_tmp" >/dev/null

# Reuses SAMPLE_ENDPOINT_TIMEOUT deliberately: the reconcile-to-status.endpoint
# latency is a per-managed-backend property (the reconciler publishes it from
# the live Service), identical for the LMCache and Mooncake managed paths — no
# Mooncake-specific tunable is warranted.
log "waiting up to ${SAMPLE_ENDPOINT_TIMEOUT}s for Mooncake status.endpoint"
mc_deadline=$(($(date +%s) + SAMPLE_ENDPOINT_TIMEOUT))
mc_endpoint=""
while [ "$(date +%s)" -lt "$mc_deadline" ]; do
  mc_endpoint=$(kubectl -n "$MOONCAKE_SMOKE_NS" get cb "$MOONCAKE_CB_NAME" \
    -o jsonpath='{.status.endpoint}' 2>/dev/null || true)
  if [ -n "$mc_endpoint" ]; then break; fi
  sleep 2
done
mc_want_endpoint="$MOONCAKE_CB_NAME.$MOONCAKE_SMOKE_NS.svc.cluster.local:50051"
if [ "$mc_endpoint" != "$mc_want_endpoint" ]; then
  kubectl -n "$MOONCAKE_SMOKE_NS" get cb "$MOONCAKE_CB_NAME" -o yaml || true
  fail "Mooncake status.endpoint=$mc_endpoint, want $mc_want_endpoint (master RPC host:port via ResolveCacheServer)"
fi
log "Mooncake status.endpoint=$mc_endpoint"

# The rendered Service must expose the RPC port (50051) FIRST — serviceEndpoint
# publishes Ports[0], and the engine wire dials it via mooncakestore://.
mc_svc_port="$(kubectl -n "$MOONCAKE_SMOKE_NS" get svc "$MOONCAKE_CB_NAME" \
  -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || true)"
if [ "$mc_svc_port" != "50051" ]; then
  kubectl -n "$MOONCAKE_SMOKE_NS" get svc "$MOONCAKE_CB_NAME" -o yaml || true
  fail "Mooncake Service first port=$mc_svc_port, want 50051"
fi

# The managed Deployment must reach Available: the stand-in master accepts TCP
# on 50051 so the controller-rendered readiness probe passes — proving
# ResolveCacheServer rendered a workload that actually comes up under a real
# install. (CacheBackend Ready is deliberately NOT asserted: no engine is wired
# here, so the KV-event readiness gate legitimately holds it at
# AwaitingFirstKVEvent — orthogonal to the managed-reconcile contract.)
log "waiting up to ${READY_TIMEOUT} for the Mooncake master Deployment to be Available"
if ! kubectl -n "$MOONCAKE_SMOKE_NS" wait --for=condition=Available --timeout="$READY_TIMEOUT" \
  deployment/"$MOONCAKE_CB_NAME" >/dev/null 2>&1; then
  kubectl -n "$MOONCAKE_SMOKE_NS" get deploy "$MOONCAKE_CB_NAME" -o yaml || true
  kubectl -n "$MOONCAKE_SMOKE_NS" get pods -o wide || true
  fail "Mooncake master Deployment did not reach Available within ${READY_TIMEOUT}"
fi
log "Mooncake master Deployment Available; managed type:Mooncake reconcile verified end-to-end"

kubectl delete namespace "$MOONCAKE_SMOKE_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true

log "PASS — install bundle came up, CacheIndex + CacheTenant status writing, PromptTemplate + PDTopology schema-only surfaces, server HTTP surface, CachePolicy push adoption, gRPC fail-open (plaintext default), CacheBackend ↔ engine-pod binding signals + drift cadence, spec.resources defaults + thread-through, External backend end-to-end, /snapshot + /policy + /probe unauth rejection, audience binding on all three endpoints, the opt-in gRPC TLS overlay (incl. the existing LookupRoute call pattern over TLS), kernel-check injection shape + report-only FAIL condition path (EngineKernelsHealthy=False/KernelLoadFailed), the operator 'inferencecache doctor' CLI against the live install, the managed Mooncake backend end-to-end (stand-in master reaches Available + mooncakestore:// RPC endpoint in status), and every config/samples/ manifest applies cleanly — all work"
