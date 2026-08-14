# CacheBackend ↔ engine-Pod binding

CacheBackend uses a namespaced label selector to find the inference-engine Pods
whose cache integration it should inject. The inference system owns the engine
Deployment and image; CacheBackend owns only the cache components and
engine-specific connector wire it adds.

## Current LMCache flow

For `spec.type: LMCache`, current manifests declare a typed PodLocal or
NodeLocal topology. At Pod CREATE, the webhook:

1. finds the one matching CacheBackend in the Pod's namespace;
2. selects the runtime-specific MP adapter;
3. injects the vLLM or SGLang connector launch surface plus either a PodLocal
   native sidecar or a NodeLocal same-node startup gate and the backend UID's
   host SHM directory mounted as `/dev/shm`;
4. optionally binds the MP server to a Redis L3; and
5. stamps `inferencecache.io/injected-by` and
   `inferencecache.io/injected-by-uid`.

The engine image is never replaced or inspected. Normal engine initialization
is the authoritative compatibility check for the required connector/package.
PodLocal native sidecars require Kubernetes 1.29 or newer.

NodeLocal is engine-first. CacheBackend creation alone creates no server. After
the inference system schedules an injected engine Pod, the controller creates
one CacheBackend-owned server Pod for each distinct active engine node. The
server uses exact node-name affinity, so Kubernetes still evaluates taints,
resources, and declared host-port conflicts. The Downward API supplies the
engine's `status.hostIP`; no ClusterIP participates. The init gate blocks normal
engine startup until `/config` and `/healthcheck` verify the same
name/UID/generation and live server configuration. Each CacheBackend UID also
derives an explicit `lmcache_l1_pool_inferencecache_<uid>` POSIX SHM name; the
gate verifies both the declared and effective live name before starting the
engine. The server and engines mount only
`/dev/shm/inference-cache/<cacheBackendUID>` from the host as their container
`/dev/shm`, so normally behaving co-located pools do not see one another's SHM
objects. This remains ownership isolation rather than cryptographic
authentication: host root, privileged Pods, or processes mounting the parent
directory can bypass it, so the host-network/server pool still requires one
trusted tenant domain.
When the final selected engine leaves a node, the server enters the configured
`idleRetentionSeconds` window instead of being coupled to that engine Pod's
restart. Demand returning during the window clears the idle marker and reuses
the same server and L1; expiry removes the server. The default is 300 seconds,
and zero requests immediate deletion.

Every new non-empty `engineSelector` contains exactly one label:
`inferencecache.io/cache-domain`. Its value is unique within the namespace and
identifies one runtime/model/KV-layout/trust compatibility domain. Engine Pods
may carry any other application, scheduling, and observability labels, but
those labels do not participate in CacheBackend ownership. CREATE and UPDATE
both enforce this shape. As a runtime backstop for concurrent CREATE admission,
the Pod webhook denies a Pod matching more than one CacheBackend; it never
chooses one by name.

```text
CacheBackend selector ──matches at Pod CREATE──▶ mutating webhook
                                                   │
                           PodLocal: native sidecar │ NodeLocal: startup gate
                                                   ▼
                                    inference system schedules engine
                                                   │
                         NodeLocal controller observes spec.nodeName
                                                   ▼
                              one same-node server Pod per active node
```

Host-only PodLocal and NodeLocal objects publish no endpoint. With external Redis, the
webhook uses `spec.remoteStorage.endpoint`; with managed Redis, it uses the
controller-resolved endpoint. Connector readiness and remote-storage readiness
are reported independently.

## Lifecycle

1. Apply the CacheBackend before creating engine Pods.
2. Create an engine Deployment whose Pod-template labels include every
   `spec.engineSelector.matchLabels` entry.
3. Admission injects the complete MP wire atomically. Ordinary lookup or
   adapter failures fail open without a partial mutation; selector ambiguity
   is denied because choosing a cache trust domain is unsafe. Inspect Pod
   annotations, Events, and engine startup logs.
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
      inferencecache.io/cache-domain: qwen-demo
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
        inferencecache.io/cache-domain: qwen-demo
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
| Multiple CacheBackends could match one Pod | A cache-domain value was reused by concurrent creates. CacheBackend admission normally rejects the duplicate; the Pod webhook also denies an ambiguous live match rather than choosing a backend. | Give every CacheBackend a unique namespace-scoped `inferencecache.io/cache-domain` value and put that value on only the intended engine Pod templates. |
| NodeLocal engine stays in `lmcache-node-local-gate` | Its on-demand same-node server is not healthy, the host ports conflict, the effective UID-scoped SHM pool is unavailable/mismatched, or live config belongs to another backend. | Inspect the server Pod args, `/config`, scheduler events, and `status.connector.enginePodCoverage`; fix ports, host `/dev/shm` capacity, resources, or runtime configuration and recreate or reschedule. |
| Pod was relabeled after creation | Admission is CREATE-only. | Recreate the Pod. |
| Pod intentionally needs no cache injection | No explicit opt-out was set. | Put `inferencecache.io/skip-inject: "true"` on the Pod template and recreate it. |

Legacy topology-less vLLM/IP binding exists only as negative search/schema
assertions. No production adapter implements it.
