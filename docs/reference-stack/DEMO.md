# InferenceCache — team demo runbook

A ~10-minute live demo of the **cache-aware inference loop**: an engine publishes
KV-cache events, a subscriber reports cache state to the policy server, and the
server answers cache-aware routing hints. The thesis throughout:

> **We describe cache state; the gateway decides routing.** The server is
> status-only and hint-providing — it never routes, and it fails open.

There are two acts, and both now have a guaranteed verification path:

- **Act 1 — the inference loop** (cache-aware routing). The engine/subscriber/server
  chain answering `LookupRoute`.
- **Act 2 — the operator loop** (declarative backends). A `CacheBackend` CR that the
  controller reconciles into a Deployment + Service, Ready with an endpoint, and
  garbage-collects on delete.

Together they are the whole story: **the operator loop provisions a cache backend;
the inference loop reports that backend's cache state and turns it into routing
hints.** "Where this goes next" at the end sketches the modules beyond C2.

This runbook gives two ways to run Act 1:

| Path | Needs | Shows | Use it as |
|---|---|---|---|
| **A — Synthetic** | Go, Python (pyzmq+msgspec), grpcurl | the full chain + LookupRoute + aggregate, with a synthetic KV-event source | the **guaranteed** demo / fallback |
| **B — Live CPU engine** | + Docker, the cached vLLM CPU image | a **real** vLLM prefix-cache hit (TTFT drop) driving the same chain | the **headline** demo when the stack is healthy |

`kubectl get cacheindex` (Act 1 step *d*) is shown in its own section and works
on top of **either** path.

> **Rehearse Path A.** It needs no GPU, no Docker, no Kubernetes, and no image
> pull, so it works regardless of network/VPN. If the live stack misbehaves on
> the day, run Path A and the demo still lands.

---

## Prerequisites

```bash
go version           # 1.24+
grpcurl -version     # 1.9+
# Python deps for the synthetic publisher (one-time):
python3 -m venv docs/reference-stack/.venv
docs/reference-stack/.venv/bin/pip install -r docs/reference-stack/scripts/requirements.txt
```

Path B additionally needs Docker with a VM that has **~12 GB RAM** (the vLLM CPU
runtime baseline is ~5 GiB before any KV cache) and the vLLM CPU image. The
`kubectl get cacheindex` section additionally needs `kind` + `kubectl`.

> **VPN / image pulls.** Image pulls fail under a TLS-intercepting VPN
> (`x509: certificate signed by unknown authority`). **Disconnect the VPN before
> pulling** the vLLM image or the `kind` node image, then reconnect. Path A pulls
> nothing.

Run everything from the repo root.

---

## Path A — Synthetic loop (guaranteed, ~3 min)

```
synthetic publisher --ZMQ KV events--> kvevent-subscriber --gRPC--> policy server --> index
```

A synthetic publisher emits vLLM-shaped `BlockStored` / `BlockRemoved` events; the
real `kvevent-subscriber` decodes them and calls `ReportCacheState`; the real
server indexes them and answers `LookupRoute`. Same code path as the live engine,
minus the engine.

### Bring it up (push-button)

```bash
PYTHON=docs/reference-stack/.venv/bin/python \
  docs/reference-stack/scripts/demo_synthetic.sh up
```

This builds the binaries, starts the server (gRPC `:19090`, HTTP `:18080`), the
publisher (ZMQ `:15557`), and the subscriber, leaves them running, and prints the
commands below with the anchor hash already base64-encoded. Expected tail:

```
[demo] stack is up. ...
```

### Drive the demo

**(a/b) The index fills and keeps growing** as events flow — this is the
subscriber → server → index chain working:

```bash
curl -s http://localhost:18080/metrics | grep '^inferencecache_index_entries'
# inferencecache_index_entries{model="demo-model"} 15      # run again a few seconds later → larger
```

**(c) A cache-aware route hint** for a prefix the index holds. The publisher
re-sends a stable "anchor" prefix every tick (hash `424242` → base64
`AAAAAAAGeTI=`), so it is always present to look up:

```bash
grpcurl -plaintext -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto \
  -d '{"model_id":"demo-model","hash_scheme":"vllm","prefix_hash":"AAAAAAAGeTI="}' \
  127.0.0.1:19090 inferencecache.v1alpha1.InferenceCache/LookupRoute
```
```json
{
  "replicaScores": [
    {
      "replicaId": "demo-replica-a",
      "score": 15.999102,
      "matchedTokens": 16,
      "estimatedCacheHitProb": 0.99994385
    }
  ],
  "reasonCode": "PREFIX_MATCH",
  "lookupLatencyUs": "1"
}
```
`PREFIX_MATCH` names the warm replica (`demo-replica-a`) and how many tokens it
matched. That is the hint the gateway uses to route for a prefix cache hit.

**Fail-open** — an unknown prefix returns an empty result with `NO_HINT`, never an
error:

```bash
grpcurl -plaintext -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto \
  -d '{"model_id":"demo-model","hash_scheme":"vllm","prefix_hash":"3q2+7w=="}' \
  127.0.0.1:19090 inferencecache.v1alpha1.InferenceCache/LookupRoute
# { "reasonCode": "NO_HINT", "lookupLatencyUs": "2" }
```

**(b) The same calls show up in metrics**, split by `reason_code`:

```bash
curl -s http://localhost:18080/metrics | grep '^inferencecache_lookup_route_calls_total'
# inferencecache_lookup_route_calls_total{hint_used="true",model="demo-model",reason_code="PREFIX_MATCH"} 1
# inferencecache_lookup_route_calls_total{hint_used="false",model="demo-model",reason_code="NO_HINT"} 1
```

**(d) The cluster-wide aggregate** — the exact JSON the controller scrapes to
populate the `CacheIndex` CR status (see the next section for the `kubectl` view):

```bash
curl -s http://localhost:18080/snapshot
# {"replicas":null,"tenants":null,"totalPrefixes":46,"hotPrefixes":0}
```
> `totalPrefixes` is the live aggregate. `replicas`/`tenants` populate from
> replica-stats reports, which the KV-event subscriber does not emit (it reports
> prefixes), so they stay empty in this demo — `totalPrefixes` is the signal.

### Tear down

```bash
docs/reference-stack/scripts/demo_synthetic.sh down
```

---

## Path B — Live CPU vLLM engine (real prefix-cache hit, ~10 min)

```
vLLM (CPU) --ZMQ KV events--> kvevent-subscriber --gRPC--> policy server --> index
```

Same chain, with a real vLLM engine on CPU. No GPU needed: vLLM's v1 engine and
its KV-event publisher both run on CPU. Tiny ungated model
(`Qwen/Qwen2.5-0.5B-Instruct`).

### One-time pre-pull (VPN OFF)

```bash
# Disconnect the VPN first, then:
docker pull vllm/vllm-openai-cpu:latest-arm64     # ~2.5 GB (x86: latest-x86_64)
# The model downloads from Hugging Face on first engine start — also do this with
# the VPN off, or pre-warm it once. Reconnect the VPN afterwards if you must.
```

### Option 1 — one-shot canary (proves the chain, then exits)

```bash
docs/reference-stack/scripts/canary_e2e.sh
```
Brings up the engine, server, and subscriber; drives two requests that share a
long prefix; asserts an **engine prefix-cache hit** and that the **server index
populated**; cleans up. Expected final line:

```
[canary] prefix_cache_hits: 0 -> 1   |   index_entries{model=canary}: <N>
[canary] PASS — engine prefix-cache hit observed and the index was populated end-to-end
```

### Option 2 — interactive (leave it up to show LookupRoute live)

Use this for the live narration. Four steps; keep the engine + server + subscriber
running between them.

```bash
# 1. Engine (CPU). Wait for /health (model load is slow on CPU, ~1-2 min).
docker run -d --name vllm-cpu -p 8000:8000 -p 5557:5557 -p 5558:5558 \
  -e VLLM_CPU_KVCACHE_SPACE=4 --shm-size=4g vllm/vllm-openai-cpu:latest-arm64 \
  --model Qwen/Qwen2.5-0.5B-Instruct --dtype bfloat16 --max-model-len 8192 \
  --enforce-eager --enable-prefix-caching \
  --kv-events-config '{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","buffer_steps":10000,"topic":"kv-events"}'
until curl -sf -o /dev/null http://localhost:8000/health; do sleep 4; done; echo "engine ready"

# 2. Server + subscriber (this branch's binaries).
make build
./bin/server --grpc-bind-address=:9090 --http-bind-address=:8080 &
until curl -sf -o /dev/null http://localhost:8080/readyz; do sleep 1; done
./bin/kvevent-subscriber --engine-endpoint tcp://127.0.0.1:5557 --topic kv-events \
  --server 127.0.0.1:9090 --replica-id vllm-0 --model-id Qwen/Qwen2.5-0.5B-Instruct \
  --hash-scheme vllm --window 100ms &

# 3. Two requests sharing a long prefix → the 2nd is a prefix-cache hit (TTFT drop).
PREFIX=$(python3 -c 'print("You are a meticulous assistant. Follow the rules precisely. " * 200)')
post() { curl -s -o /dev/null -w 'HTTP %{http_code}  total=%{time_total}s\n' \
  http://localhost:8000/v1/chat/completions -H 'Content-Type: application/json' \
  -d "$(python3 -c 'import json,sys;print(json.dumps({"model":"Qwen/Qwen2.5-0.5B-Instruct","max_tokens":8,"temperature":0,"messages":[{"role":"system","content":sys.argv[1]},{"role":"user","content":sys.argv[2]}]}))' "$PREFIX" "$1")"; }
post "summarize in one word"     # cold: longer
post "summarize in two words"    # warm: shorter (prefill skipped on the shared prefix)
curl -s http://localhost:8000/metrics | grep '^vllm:prefix_cache_hits_total'   # increased

# 4. The events reached the server index, and LookupRoute hints the warm replica.
curl -s http://localhost:8080/metrics | grep '^inferencecache_index_entries'
grpcurl -plaintext -import-path proto -proto inferencecache/v1alpha1/inferencecache.proto \
  -d '{"model_id":"Qwen/Qwen2.5-0.5B-Instruct"}' \
  127.0.0.1:9090 inferencecache.v1alpha1.InferenceCache/GetCacheState
```

For a `LookupRoute` `PREFIX_MATCH` against the live engine you need a real
prefix-block hash. Capture one from the live event stream and look it up:

```bash
docs/reference-stack/.venv/bin/python docs/reference-stack/scripts/kv_events_subscriber.py \
  --endpoint tcp://localhost:5557 --topic kv-events --max 5 --json
# copy a block_hashes value, base64-encode the 8-byte big-endian form, and pass it as prefix_hash
```
(If a live capture is fiddly on the day, fall back to Path A's stable anchor to
show `PREFIX_MATCH`.)

### Tear down

```bash
docker rm -f vllm-cpu; kill %1 %2 2>/dev/null   # or pkill -f bin/server; pkill -f kvevent-subscriber
```

---

## `kubectl get cacheindex` — the live aggregate as a CR (Act 1 step *d*)

The controller scrapes the server's `/snapshot` and writes a single cluster-scoped
`CacheIndex` CR (`cluster-default`) — the controller writes, the server describes.
Works on top of **either** path; the steps below assume the server from Path A is
running on `:18080` (adjust `--server-snapshot-url` for Path B's `:8080`).

```bash
# One-time: kind node image pull needs the VPN OFF.
kind create cluster --name ic-demo
kubectl apply -f config/crd/bases/        # all CRDs (one controller serves both acts)

# Run the controller out-of-cluster, pointed at the running server's snapshot.
make build
./bin/controller \
  --server-snapshot-url=http://localhost:18080/snapshot \
  --cacheindex-refresh-interval=5s \
  --metrics-bind-address=:18090 --health-probe-bind-address=:18091 &

# Watch the aggregate land in the CR status.
kubectl get cacheindex -w
# NAME              PREFIXES   CHANGED   AGE
# cluster-default   46         3s        7s
kubectl get cacheindex cluster-default -o jsonpath='{.status.prefixes.summary.total}{"\n"}'
```

Tear down: `kill %1` (controller), `kind delete cluster --name ic-demo`.

---

## Act 2 — the operator loop (C2)

A `CacheBackend` CR is the declarative API for a managed cache backend. The
controller reconciles a managed (LMCache) backend into a **Deployment + ClusterIP
Service** it owns via owner references, publishes the in-cluster **endpoint** and a
**Ready** condition to status, and the children are **garbage-collected** when the
CR is deleted. Same controller binary as Act 1's `cacheindex` poller.

### Guaranteed path — verify the whole loop with no cluster (envtest)

Like Path A is for Act 1, this proves the operator loop against a **real Kubernetes
API server** (envtest binaries — no Docker, no kind, no image pull):

```bash
make test-env                       # one-time: fetch the envtest kube binaries
KUBEBUILDER_ASSETS="$(make test-env)" go test ./internal/controller/ -run TestIntegration -v
```
Expected — every facet of the loop passes:
```
--- PASS: TestIntegrationCacheBackendReconcile/LMCacheTemplatedWorkloadShape
--- PASS: TestIntegrationCacheBackendReconcile/StatusEndpointAndObservedGeneration
--- PASS: TestIntegrationCacheBackendReconcile/HealthTransitions
--- PASS: TestIntegrationCacheBackendReconcile/OwnerReferencesDriveGC
--- PASS: TestIntegrationCacheBackendReconcile/ServicePortDriftCorrected
... PASS
```
`OwnerReferencesDriveGC` (delete CR → children gone), `HealthTransitions`
(Deployment available → CR `Ready`), and `StatusEndpointAndObservedGeneration`
(endpoint published) are the exact beats of the live demo below.

### Live path — `kubectl` on kind

```bash
kind create cluster --name ic-demo            # one-time node image pull needs the VPN OFF
kubectl apply -f config/crd/bases/            # installs CacheBackend + CacheIndex CRDs
make build && ./bin/controller \
  --metrics-bind-address=:18090 --health-probe-bind-address=:18091 &

SEL=app.kubernetes.io/managed-by=inference-cache-controller
kubectl apply -f config/samples/cachebackend-lmcache.yaml
kubectl get cachebackend -w
# NAME                   TYPE      HEALTH   ENDPOINT                                              AGE
# cachebackend-lmcache   LMCache   Ready    cachebackend-lmcache.default.svc.cluster.local:8000   20s
kubectl get cachebackend cachebackend-lmcache -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}{"\n"}'  # True
kubectl get deploy,svc -l "$SEL"          # the owned children
kubectl delete -f config/samples/cachebackend-lmcache.yaml   # owner-ref GC removes both
kubectl get deploy,svc -l "$SEL"          # gone
```

> **`HEALTH Ready` needs the backend pod to actually run.** The sample pins the real
> LMCache image + a gated model — pull it with the VPN off (and supply an `hf-token`
> secret), ideally on a GPU cluster. On an offline/CPU kind cluster the pod sits in
> `ImagePullBackOff`, so `HEALTH` stays `Pending` — but the CR still creates and
> **GC-deletes** its Deployment + Service, which is the operator-loop point. To show
> `HEALTH Ready` offline, override `spec.backendConfig.image` to a tiny image that
> answers `GET /health` on `:8000` and `kind load docker-image` it first.

---

## Talking points (tie each step back to the thesis)

1. **"We describe cache state; the gateway decides."** `LookupRoute` returns a
   *hint* (`PREFIX_MATCH` + which replica is warm + matched tokens), not a routing
   command. The gateway stays in control of routing; we only surface where the KV
   cache already is. This is why it's a control *plane*, not a router.
2. **Fail-open, never on the hot path.** An unknown prefix, an empty index, a
   server blip — all return `NO_HINT`, never an error. The gateway treats a no-hint
   as "route as you normally would," so the cache plane can never take down serving.
   Lookups are side-effect-free apart from metrics.
3. **Soft state, additive deltas.** The index is in-memory and rebuilt from the
   engine's KV-event stream (`BlockStored` adds, `BlockRemoved`/TTL removes). A lost
   update degrades to a cache *miss*, never a wrong answer — which is exactly why we
   can run a single in-memory instance now and add HA later without changing the
   contract.
4. **Declarative backends, the controller owns the workload (Act 2).** A
   `CacheBackend` CR is the whole API; the controller templates and *owns* the
   Deployment + Service via owner references, so lifecycle is Kubernetes-native —
   `kubectl delete` GCs the children, no imperative teardown. Status (endpoint +
   `Ready`) is observed, not asserted. The operator loop *provisions* the backend
   that the inference loop then *describes*.

---

## Where this goes next (beyond C2)

The two loops in this demo are the foundation; the contract and CRDs are designed
so the rest slots in without breaking either:

- **C3 — storage + autoscaling.** `deploymentKind: StatefulSet` (PVC-backed cache)
  and cache-aware autoscaling. The spec field is already present and deferred today.
- **C5 — runtime-adapter interface.** Generalize beyond LMCache/vLLM so other
  engines and KV backends plug in behind the same `CacheBackend` API.
- **Ranking v2 + block-level longest-prefix match.** Better `LookupRoute` hints
  (pressure/SLO-aware, `TENANT_HOT`) — pure server-side, no contract change — and
  matching the longest common *block-hash chain* for far higher hit rates.
- **E-series — gateway integration.** The Java gRPC client and the Data-Plane patch
  that consume `LookupRoute` in a real gateway, plus fail-open validation.
- **F-series — observability/ship.** The standardized public metric schema,
  dashboards, and a CLI on top of the `inferencecache_*` metrics shown here.

Every step keeps the invariant from talking point 1: **we describe, the gateway
decides.**

---

## Gotchas to pre-empt

- **Ports in use.** Path A uses `:19090/:18080/:15557` to avoid the common `:8080`
  clash; Path B's interactive block uses `:9090/:8080/:5557/:5558`. If a port is
  taken, override via the env vars at the top of `demo_synthetic.sh` (or stop the
  other process).
- **VPN blocks pulls, not Path A.** The `x509: certificate signed by unknown
  authority` error means the VPN is intercepting TLS — disconnect it to pull the
  vLLM or kind images. Path A pulls nothing and is unaffected.
- **vLLM on CPU is slow to start** (~1-2 min model load) and needs ~12 GB VM RAM.
  Pre-pull the image and pre-warm the model *before* the meeting.
- **Synthetic publisher Python deps.** It needs `pyzmq` + `msgspec`; use the venv
  from Prerequisites and pass `PYTHON=.../python`. (`python3 test_kv_events.py` in
  the scripts dir is a quick sanity check.)
- **`grpcurl` uses `-proto`, not reflection** — the server does not register gRPC
  reflection, so always pass `-import-path proto -proto inferencecache/v1alpha1/inferencecache.proto`.
- **Empty `replicas` in `/snapshot` / the CR is expected** — the subscriber reports
  prefixes, not replica memory stats; `totalPrefixes` is the live aggregate.
- **Act 2 envtest assets download once.** `make test-env` fetches the kube-apiserver
  / etcd binaries (cached under `bin/k8s/`); it needs network the first time. The
  live kind/`kubectl` path additionally needs the kind node image (VPN off once).
- **`CacheBackend` `HEALTH` reflects the pod, not the reconcile.** The CR reconciles
  (children created/owned) immediately; `HEALTH` only goes `Ready` once the
  Deployment's pods are available — so it stays `Pending` until the backend image
  actually runs.
