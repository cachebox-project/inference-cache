# CacheBackend ↔ engine-Pod binding

CacheBackend uses a namespaced label selector to find the inference-engine Pods
whose cache integration it should inject. The inference system owns the engine
Deployment and image; CacheBackend owns only the cache components and
engine-specific connector wire it adds.

## Current LMCache flow

For `spec.type: LMCache`, current manifests declare
`spec.lmCache.topology: PodLocal`. At Pod CREATE, the webhook:

1. finds matching CacheBackends in the Pod's namespace;
2. selects the runtime-specific MP adapter;
3. injects one `lmcache-mp-server` native sidecar, shared `/dev/shm`, and the
   vLLM or SGLang connector launch surface;
4. optionally binds the MP server to a Redis L3; and
5. stamps `inferencecache.io/injected-by` and
   `inferencecache.io/injected-by-uid`.

The engine image is never replaced or inspected. Normal engine initialization
is the authoritative compatibility check for the required connector/package.
PodLocal native sidecars require Kubernetes 1.29 or newer.

```text
CacheBackend selector ──matches at Pod CREATE──▶ mutating webhook
                                                   │
                                                   ▼
engine Pod: engine + LMCache MP server sidecar + optional subscriber
                         │
                         └── optional RESP ──▶ Redis L3
```

Host-only PodLocal objects publish no endpoint. With external Redis, the
webhook uses `spec.remoteStorage.endpoint`; with managed Redis, it uses the
controller-resolved endpoint. Connector readiness and remote-storage readiness
are reported independently.

## Lifecycle

1. Apply the CacheBackend before creating engine Pods.
2. Create an engine Deployment whose Pod-template labels include every
   `spec.engineSelector.matchLabels` entry.
3. Admission injects the complete MP wire atomically. A collision or invalid
   Pod shape fails open without a partial mutation; inspect Pod annotations,
   Events, and engine startup logs.
4. If `--kvevent-subscriber-image` is configured and
   `spec.observation.modelID` is set, the webhook also adds the observation
   sidecar. The subscriber reports metadata-only KV events to the policy index.
5. Recreate Pods after changing a CacheBackend. Injection is evaluated only at
   Pod CREATE; relabeling or editing a running Pod does not re-run admission.

## Annotated example

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: qwen-demo-cache
spec:
  runtime: VLLM
  type: LMCache
  engineSelector:
    matchLabels:
      app: qwen-demo
  lmCache:
    topology: PodLocal
    podLocal:
      server:
        image: docker.io/lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13
        port: 5555
        l1Capacity: 4Gi
        maxWorkers: 4
        resources:
          requests:
            cpu: "1"
            memory: 5Gi
          limits:
            cpu: "2"
            memory: 6Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: qwen-engine
spec:
  selector:
    matchLabels:
      app: qwen-demo
  template:
    metadata:
      labels:
        app: qwen-demo
    spec:
      containers:
        - name: vllm
          image: example.invalid/runtime-owned-vllm-lmcache@sha256:0000000000000000000000000000000000000000000000000000000000000000
```

A fuller paired sample is
[`config/samples/cachebackend-with-engine.yaml`](../../config/samples/cachebackend-with-engine.yaml).

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `MATCHED: 0` and no injection annotation | Selector and Pod labels differ. | Align the labels and recreate the Pod. |
| A matching Pod has no injection annotation | Admission failed open because of an invalid/colliding Pod shape or an unavailable managed Redis endpoint. | Read webhook logs and Pod Events, fix the reported shape, then recreate the Pod. |
| Engine crashes after successful injection | The runtime-owned image lacks a compatible LMCache client/API, or another engine startup requirement failed. | Inspect engine logs and use a compatible pinned image; CacheBackend does not replace it. |
| Multiple CacheBackends match one Pod | Selectors overlap; the lexicographically first CacheBackend wins. | Narrow selectors so every engine Pod has one owner. |
| Pod was relabeled after creation | Admission is CREATE-only. | Recreate the Pod. |
| Pod intentionally needs no cache injection | No explicit opt-out was set. | Put `inferencecache.io/skip-inject: "true"` on the Pod template and recreate it. |

Legacy topology-less vLLM/IP binding remains implemented only for Phase 7
compatibility tests. It is not a current sample or recommended production path.
