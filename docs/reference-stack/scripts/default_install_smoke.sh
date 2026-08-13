#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

# Per-PR smoke for the current MP-only CacheBackend contract. It installs
# config/default into a fresh kind cluster and verifies the served schema,
# managed Redis lifecycle, and real Pod admission mutation. No GPU or inference
# engine is required: engine startup remains the authoritative package/
# connector compatibility check and is covered by the GPU validation matrix.

set -euo pipefail

TAG="${TAG:-${GITHUB_SHA:-$(git rev-parse HEAD)}}"
REGISTRY="${REGISTRY:-ghcr.io/cachebox-project}"
KIND_CLUSTER="${KIND_CLUSTER:-ic-install-smoke}"
SYSTEM_NAMESPACE="${SYSTEM_NAMESPACE:-inference-cache-system}"
SMOKE_NAMESPACE="${SMOKE_NAMESPACE:-ic-mp-smoke}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.1}"
READY_TIMEOUT="${READY_TIMEOUT:-180s}"
CACHEINDEX_TIMEOUT="${CACHEINDEX_TIMEOUT:-90}"
GRPC_LOCAL_PORT="${GRPC_LOCAL_PORT:-19090}"
HTTP_LOCAL_PORT="${HTTP_LOCAL_PORT:-18080}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
LOG_DIR="${LOG_DIR:-/tmp/install-smoke-logs}"

CONTROLLER_IMG="$REGISTRY/inference-cache-controller:$TAG"
SERVER_IMG="$REGISTRY/inference-cache-server:$TAG"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

KIND="${KIND:-}"
if [ -z "$KIND" ]; then
  if [ -x "$REPO_ROOT/bin/kind" ]; then
    KIND="$REPO_ROOT/bin/kind"
  else
    KIND="kind"
  fi
fi

log() { printf '[default-install-smoke] %s\n' "$*"; }
fail() { printf '[default-install-smoke] ERROR: %s\n' "$*" >&2; exit 1; }

for binary in curl docker grpcurl kubectl "$KIND"; do
  command -v "$binary" >/dev/null 2>&1 || fail "missing required tool: $binary"
done

mkdir -p "$LOG_DIR"
created_cluster=0
tmpdir=""
grpc_pf_pid=""
http_pf_pid=""

collect_diagnostics() {
  kubectl get nodes -o wide >"$LOG_DIR/nodes.txt" 2>&1 || true
  kubectl -n "$SYSTEM_NAMESPACE" get all -o wide >"$LOG_DIR/system.txt" 2>&1 || true
  kubectl -n "$SYSTEM_NAMESPACE" get events --sort-by=.lastTimestamp >"$LOG_DIR/events.txt" 2>&1 || true
  kubectl -n "$SYSTEM_NAMESPACE" logs deployment/inference-cache-controller-manager --all-containers >"$LOG_DIR/controller.log" 2>&1 || true
  kubectl -n "$SMOKE_NAMESPACE" get all -o wide >"$LOG_DIR/smoke-system.txt" 2>&1 || true
  kubectl -n "$SMOKE_NAMESPACE" get pods -o json >"$LOG_DIR/smoke-pods.json" 2>&1 || true
  kubectl -n "$SMOKE_NAMESPACE" get events --sort-by=.lastTimestamp >"$LOG_DIR/smoke-events.txt" 2>&1 || true
}

cleanup() {
	[ -z "$grpc_pf_pid" ] || kill "$grpc_pf_pid" >/dev/null 2>&1 || true
	[ -z "$http_pf_pid" ] || kill "$http_pf_pid" >/dev/null 2>&1 || true
	if [ -n "$tmpdir" ] && [ -d "$tmpdir" ]; then
    rm -rf "$tmpdir"
  fi
  if [ "$created_cluster" = "1" ] && [ "$KEEP_CLUSTER" != "1" ]; then
    "$KIND" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
  fi
}

on_exit() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    collect_diagnostics
  fi
  cleanup
  exit "$rc"
}
trap on_exit EXIT

if "$KIND" get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  log "reusing kind cluster $KIND_CLUSTER"
else
  log "creating kind cluster $KIND_CLUSTER"
  "$KIND" create cluster --name "$KIND_CLUSTER" --wait 120s
  created_cluster=1
fi
kubectl config use-context "kind-$KIND_CLUSTER" >/dev/null

log "building and loading controller/server images"
make image-build TAG="$TAG" REGISTRY="$REGISTRY"
"$KIND" load docker-image "$CONTROLLER_IMG" --name "$KIND_CLUSTER"
"$KIND" load docker-image "$SERVER_IMG" --name "$KIND_CLUSTER"

log "installing cert-manager $CERT_MANAGER_VERSION"
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml"
kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=180s

tmpdir="$(mktemp -d)"
cp -R config "$tmpdir/config"
(
  cd "$tmpdir/config/default"
  if command -v kustomize >/dev/null 2>&1; then
    kustomize edit set image "controller=$CONTROLLER_IMG" "server=$SERVER_IMG"
  else
    sed -i.bak \
      -e "/^- name: controller$/,/^- name: server$/ { s|^  newName: .*|  newName: ${CONTROLLER_IMG%:*}|; s|^  newTag: .*|  newTag: ${CONTROLLER_IMG##*:}|; }" \
      -e "/^- name: server$/,$ { s|^  newName: .*|  newName: ${SERVER_IMG%:*}|; s|^  newTag: .*|  newTag: ${SERVER_IMG##*:}|; }" \
      kustomization.yaml
    rm -f kustomization.yaml.bak
  fi
)

log "installing config/default"
kubectl apply -k "$tmpdir/config/default"
kubectl -n "$SYSTEM_NAMESPACE" wait --for=condition=Available --timeout="$READY_TIMEOUT" \
  deployment/inference-cache-controller-manager deployment/inference-cache-server

log "checking CacheIndex polling and server HTTP/gRPC surfaces"
deadline=$(($(date +%s) + CACHEINDEX_TIMEOUT))
observed=""
until [ -n "$observed" ]; do
  observed="$(kubectl get cacheindex cluster-default -o jsonpath='{.status.observedServer}' 2>/dev/null || true)"
  [ -n "$observed" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || fail "CacheIndex poller did not publish status.observedServer"
  sleep 3
done

kubectl -n "$SYSTEM_NAMESPACE" port-forward svc/inference-cache-server "$GRPC_LOCAL_PORT:9090" >"$LOG_DIR/grpc-port-forward.log" 2>&1 &
grpc_pf_pid=$!
kubectl -n "$SYSTEM_NAMESPACE" port-forward svc/inference-cache-server "$HTTP_LOCAL_PORT:8080" >"$LOG_DIR/http-port-forward.log" 2>&1 &
http_pf_pid=$!
for _ in $(seq 1 30); do
  grpcurl -plaintext -max-time 2 "localhost:$GRPC_LOCAL_PORT" list >/dev/null 2>&1 && break
  sleep 1
done
grpcurl -plaintext -max-time 5 "localhost:$GRPC_LOCAL_PORT" list >"$LOG_DIR/grpc-services.txt" \
  || fail "default plaintext gRPC endpoint is unavailable"
lookup_response="$(grpcurl -plaintext -max-time 5 \
  -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto \
  -d '{"modelId":"install-smoke-unknown"}' "localhost:$GRPC_LOCAL_PORT" \
  inferencecache.v1alpha1.InferenceCache/LookupRoute)" \
  || fail "LookupRoute smoke request failed"
grep -Eq '"(reasonCode|reason_code)"[[:space:]]*:[[:space:]]*"NO_HINT"' <<<"$lookup_response" \
  || fail "LookupRoute did not fail open with NO_HINT: $lookup_response"
for _ in $(seq 1 30); do
  curl -fsS --max-time 2 "http://localhost:$HTTP_LOCAL_PORT/readyz" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS --max-time 5 "http://localhost:$HTTP_LOCAL_PORT/readyz" >/dev/null \
  || fail "server /readyz is unavailable"
curl -fsS --max-time 5 "http://localhost:$HTTP_LOCAL_PORT/metrics" >"$LOG_DIR/server-metrics.txt" \
  || fail "server /metrics is unavailable"
grep -Eq '^inferencecache_server_up[[:space:]]+1([[:space:]]|$)' "$LOG_DIR/server-metrics.txt" \
  || fail "server up metric is missing"

log "checking the served CacheBackend schema is MP-only"
crd_yaml="$(kubectl get crd cachebackends.inferencecache.io -o yaml)"
for retired in LMCacheServer LMCacheConnectorV1 LMCACHE_REMOTE_URL 'lm://' workerImage workerPort remoteSerde hostMemory observedServerInstance deploymentKind autoscaling; do
  if grep -Fq "$retired" <<<"$crd_yaml"; then
    fail "served CacheBackend CRD still contains retired surface: $retired"
  fi
done

kubectl create namespace "$SMOKE_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

log "creating typed PodLocal and NodeLocal MP backends"
cat <<'EOF' | kubectl -n "$SMOKE_NAMESPACE" apply -f - >/dev/null
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: host-only
spec:
  runtime: VLLM
  type: LMCache
  engineSelector:
    matchLabels:
      inferencecache.io/cache-domain: mp-engine
  integration:
    role: ReadWrite
  lmCache:
    topology: PodLocal
    chunkSizeTokens: 256
    podLocal:
      server:
        image: docker.io/lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13
        port: 5555
        l1Capacity: 1Gi
        maxWorkers: 1
        resources:
          requests:
            cpu: "1"
            memory: 2Gi
          limits:
            memory: 2Gi
---
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: managed-redis
spec:
  runtime: SGLang
  type: LMCache
  engineSelector:
    matchLabels:
      inferencecache.io/cache-domain: sglang-engine
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
          requests:
            cpu: "1"
            memory: 2Gi
          limits:
            memory: 2Gi
  remoteStorage:
    provider: Redis
    ownership: Managed
    workload:
      nodeSelector:
        kubernetes.io/os: linux
      terminationGracePeriodSeconds: 45
    redis: {}
---
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: node-local
spec:
  runtime: VLLM
  type: LMCache
  engineSelector:
    matchLabels:
      inferencecache.io/cache-domain: node-local-engine
  integration:
    role: ReadWrite
  lmCache:
    topology: NodeLocal
    chunkSizeTokens: 256
    nodeLocal:
      idleRetentionSeconds: 300
      server:
        image: docker.io/lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13
        port: 5556
        httpPort: 8081
        l1Capacity: 1Gi
        maxGPUWorkers: 2
        maxCPUWorkers: 2
        resources:
          requests:
            cpu: "1"
            memory: 2Gi
          limits:
            memory: 2Gi
EOF

log "checking duplicate same-namespace cache domains are rejected"
if cat <<'EOF' | kubectl -n "$SMOKE_NAMESPACE" apply --dry-run=server -f - >/dev/null 2>&1
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: overlapping-selector
spec:
  runtime: VLLM
  type: LMCache
  engineSelector:
    matchLabels:
      inferencecache.io/cache-domain: mp-engine
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
          requests:
            cpu: "1"
            memory: 2Gi
          limits:
            memory: 2Gi
EOF
then
  fail "duplicate cache-domain engineSelector was admitted"
fi

for _ in $(seq 1 60); do
  endpoint="$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend managed-redis -o jsonpath='{.status.remoteStorage.endpoint}' 2>/dev/null || true)"
  [ "$endpoint" = "managed-redis.$SMOKE_NAMESPACE.svc.cluster.local:6379" ] && break
  sleep 1
done
[ "${endpoint:-}" = "managed-redis.$SMOKE_NAMESPACE.svc.cluster.local:6379" ] || fail "managed Redis endpoint was not published"
kubectl -n "$SMOKE_NAMESPACE" get deployment managed-redis >/dev/null
kubectl -n "$SMOKE_NAMESPACE" get service managed-redis >/dev/null
managed_os="$(kubectl -n "$SMOKE_NAMESPACE" get deployment managed-redis -o jsonpath='{.spec.template.spec.nodeSelector.kubernetes\.io/os}')"
[ "$managed_os" = "linux" ] || fail "managed workload nodeSelector was not rendered"
managed_grace="$(kubectl -n "$SMOKE_NAMESPACE" get deployment managed-redis -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}')"
[ "$managed_grace" = "45" ] || fail "managed workload terminationGracePeriodSeconds was not rendered"

log "checking NodeLocal creates no speculative server before an engine is scheduled"
[ "$(kubectl -n "$SMOKE_NAMESPACE" get pods -l inferencecache.io/lmcache-node-server=true --no-headers 2>/dev/null | wc -l | tr -d ' ')" = "0" ] \
  || fail "NodeLocal created a server without scheduled engine demand"
if kubectl -n "$SMOKE_NAMESPACE" get service node-local >/dev/null 2>&1; then
  fail "NodeLocal MP endpoint must not have a Service"
fi

log "checking real Pod admission renders only the MP wire"
pod_json="$tmpdir/admitted-pod.json"
cat <<'EOF' | kubectl -n "$SMOKE_NAMESPACE" apply --dry-run=server -o json -f - >"$pod_json"
apiVersion: v1
kind: Pod
metadata:
  name: mp-engine
  labels:
    inferencecache.io/cache-domain: mp-engine
spec:
  nodeSelector:
    kubernetes.io/os: linux
  containers:
    - name: vllm
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
EOF

grep -Fq 'lmcache-mp-server' "$pod_json" || fail "native MP sidecar was not injected"
grep -Fq 'LMCacheMPConnector' "$pod_json" || fail "vLLM MP connector was not injected"
grep -Fq 'lmcache.mp.host' "$pod_json" || fail "MP loopback host was not injected"
for retired in LMCacheConnectorV1 LMCACHE_REMOTE_URL LMCACHE_REMOTE_SERDE 'lm://'; do
  if grep -Fq "$retired" "$pod_json"; then
    fail "admitted Pod contains retired wire: $retired"
  fi
done

log "checking real Pod admission renders the same-node NodeLocal wire and startup gate"
node_pod_json="$tmpdir/admitted-node-local-pod.json"
cat <<'EOF' | kubectl -n "$SMOKE_NAMESPACE" apply --dry-run=server -o json -f - >"$node_pod_json"
apiVersion: v1
kind: Pod
metadata:
  name: node-local-engine
  labels:
    inferencecache.io/cache-domain: node-local-engine
spec:
  nodeSelector:
    kubernetes.io/os: linux
  containers:
    - name: vllm
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
EOF
grep -Fq 'lmcache-node-local-gate' "$node_pod_json" || fail "NodeLocal ownership/health startup gate was not injected"
grep -Fq 'EXPECTED_SHM_NAME' "$node_pod_json" || fail "NodeLocal startup gate does not verify UID-scoped shared memory"
grep -Fq 'INFERENCECACHE_NODE_IP' "$node_pod_json" || fail "NodeLocal hostIP Downward API was not injected"
grep -Fq 'status.hostIP' "$node_pod_json" || fail "NodeLocal endpoint is not derived from status.hostIP"
grep -Fq 'kubernetes.io/os' "$node_pod_json" || fail "inference-owned nodeSelector was not preserved"
if grep -Fq 'podAffinity' "$node_pod_json"; then
  fail "NodeLocal injection unexpectedly added server-first PodAffinity"
fi
grep -Fq 'hostPath' "$node_pod_json" || fail "NodeLocal engine did not receive host /dev/shm"
grep -Fq 'tcp://$(INFERENCECACHE_NODE_IP)' "$node_pod_json" || fail "vLLM NodeLocal connector does not use the node-derived endpoint"
if grep -Fq '"name": "lmcache-mp-server"' "$node_pod_json"; then
  fail "NodeLocal engine unexpectedly received the PodLocal native sidecar"
fi

log "checking scheduled engine demand creates one exact-node server Pod"
kubectl -n "$SMOKE_NAMESPACE" apply -f "$node_pod_json" >/dev/null
for _ in $(seq 1 60); do
  engine_node="$(kubectl -n "$SMOKE_NAMESPACE" get pod node-local-engine -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  server_name="$(kubectl -n "$SMOKE_NAMESPACE" get pods -l inferencecache.io/lmcache-node-server=true -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [ -n "$engine_node" ] && [ -n "$server_name" ] && break
  sleep 1
done
[ -n "${engine_node:-}" ] && [ -n "${server_name:-}" ] || fail "scheduled engine did not create an on-demand NodeLocal server"
kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o json >"$LOG_DIR/node-local-server.json"
[ "$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o jsonpath='{.metadata.annotations.inferencecache\.io/node-local-target-node}')" = "$engine_node" ] \
  || fail "NodeLocal server target does not match the engine-selected node"
[ "$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o jsonpath='{.spec.hostNetwork}')" = "true" ] \
  || fail "NodeLocal server does not use hostNetwork"
node_local_host_ipc="$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o jsonpath='{.spec.hostIPC}')"
[ -z "$node_local_host_ipc" ] || [ "$node_local_host_ipc" = "false" ] \
  || fail "NodeLocal server unexpectedly uses hostIPC"
[ "$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o jsonpath='{.spec.volumes[0].hostPath.path}')" = "/dev/shm" ] \
  || fail "NodeLocal server does not mount host /dev/shm"
node_local_backend_uid="$(kubectl -n "$SMOKE_NAMESPACE" get cachebackend node-local -o jsonpath='{.metadata.uid}')"
node_local_shm_name="lmcache_l1_pool_inferencecache_${node_local_backend_uid}"
[ "$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o jsonpath='{.metadata.annotations.inferencecache\.io/node-local-shm-name}')" = "$node_local_shm_name" ] \
  || fail "NodeLocal server does not carry its UID-scoped shared-memory identity"
node_local_shm_arg="$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" \
  -o jsonpath='{range .spec.containers[?(@.name=="lmcache-mp-server")].args[*]}{@}{"\n"}{end}' | \
  awk 'previous == "--shm-name" { print; exit } { previous = $0 }')"
[ "$node_local_shm_arg" = "$node_local_shm_name" ] \
  || fail "NodeLocal server does not pass its UID-scoped --shm-name: $node_local_shm_arg"
node_local_ports="$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o jsonpath='{range .spec.containers[?(@.name=="lmcache-mp-server")].ports[*]}{.containerPort}:{.hostPort}{" "}{end}')"
[ "$node_local_ports" = "5556:5556 8081:8081 " ] || fail "NodeLocal host ports were not declared: $node_local_ports"
node_affinity_target="$(kubectl -n "$SMOKE_NAMESPACE" get pod "$server_name" -o jsonpath='{.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchFields[0].values[0]}')"
[ "$node_affinity_target" = "$engine_node" ] || fail "server does not use scheduler-bound exact-node affinity"

log "checking current samples against the live CRDs and admission webhooks"
sample_namespace="${SMOKE_NAMESPACE}-samples"
kubectl create namespace "$sample_namespace" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
sample_count=0
while IFS= read -r sample; do
  if awk '
    /^[[:space:]]*$/ { next }
    /^[[:space:]]*#/ { if ($0 ~ /#[[:space:]]*verify-samples:[[:space:]]*skip[[:space:]]*$/) found=1; next }
    { exit }
    END { exit(found ? 0 : 1) }
  ' "$sample"; then
    continue
  fi
  kubectl -n "$sample_namespace" apply --dry-run=server -f "$sample" >/dev/null \
    || fail "sample failed live server-side admission: $sample"
  sample_count=$((sample_count + 1))
done < <(find config/samples -type f \( -name '*.yaml' -o -name '*.yml' \) | sort)
[ "$sample_count" -gt 0 ] || fail "no samples were checked"

log "checking non-LMCache control-plane APIs and engine-local backends"
for sample in \
  config/samples/cache_v1alpha1_cachepolicy.yaml \
  config/samples/cache_v1alpha1_cachetenant.yaml \
  config/samples/cache_v1alpha1_prompttemplate.yaml \
  config/samples/cache_v1alpha1_pdtopology.yaml; do
  kubectl -n "$sample_namespace" apply -f "$sample" >/dev/null
done
kubectl -n "$sample_namespace" get cachepolicy cachepolicy-sample >/dev/null
kubectl -n "$sample_namespace" get cachetenant cachetenant-sample >/dev/null
kubectl -n "$sample_namespace" get prompttemplate prompttemplate-sample >/dev/null
kubectl -n "$sample_namespace" get pdtopology pdtopology-sample >/dev/null

for sample in \
  config/samples/cachebackend-events-only.yaml \
  config/samples/cachebackend-sglang-hicache.yaml; do
  kubectl -n "$sample_namespace" apply -f "$sample" >/dev/null
done
for engine_local in cachebackend-events-only sglang-hicache; do
  if kubectl -n "$sample_namespace" get deployment "$engine_local" >/dev/null 2>&1 || \
     kubectl -n "$sample_namespace" get service "$engine_local" >/dev/null 2>&1; then
    fail "$engine_local unexpectedly provisioned a backend workload"
  fi
done

log "checking the operator-facing doctor CLI against the live install"
doctor_bin="$tmpdir/inferencecache"
go build -o "$doctor_bin" ./cmd/inferencecache
doctor_rc=0
"$doctor_bin" doctor --config-only --namespace "$SMOKE_NAMESPACE" --output json --no-color \
  >"$LOG_DIR/doctor.json" 2>"$LOG_DIR/doctor.err" || doctor_rc=$?
case "$doctor_rc" in 0|1|2) ;; *) fail "doctor exited with unexpected code $doctor_rc" ;; esac
grep -Fq '"summary"' "$LOG_DIR/doctor.json" || fail "doctor JSON summary is missing"
grep -Fq '"findings"' "$LOG_DIR/doctor.json" || fail "doctor JSON findings are missing"

log "re-applying the bundle as an idempotent upgrade check"
kubectl apply -k "$tmpdir/config/default" >/dev/null
kubectl -n "$SYSTEM_NAMESPACE" wait --for=condition=Available --timeout="$READY_TIMEOUT" \
  deployment/inference-cache-controller-manager deployment/inference-cache-server
kubectl -n "$SMOKE_NAMESPACE" get cachebackend host-only managed-redis node-local >/dev/null

log "PASS: default install, control-plane APIs, server surfaces, samples, doctor, PodLocal/NodeLocal typed MP admission, engine-demanded NodeLocal server Pods, managed Redis, and idempotent re-apply"
