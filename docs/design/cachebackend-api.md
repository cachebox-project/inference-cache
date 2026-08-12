# Design: CacheBackend API

Status: implemented · Tracks: InferenceCache tech spec §4.1 · API group: `inferencecache.io/v1alpha1`

> **Current production contract:** LMCache uses typed
> `spec.lmCache.topology: PodLocal` multiprocess wiring for both vLLM and
> SGLang, with optional Redis selected explicitly. References to topology-less
> LMCacheServer, the former IP-wired Mooncake provider, `lm://`, and the IP
> connector are explicitly marked history for behavior physically removed in
> Phase 7. Mooncake remains a planned typed MP L2 provider; it is not available
> in the current API and is never translated to Redis.

`CacheBackend` is the namespaced CRD that describes an engine-side cache
implementation, an optional remote-storage tier, and the engine integration
policy that should use them. Provider lifecycle belongs to storage-provider
adapters; runtime adapters own engine Pod wiring only.

## Identity

| | Value |
|---|---|
| group | `inferencecache.io` |
| version | `v1alpha1` |
| kind | `CacheBackend` |
| plural | `cachebackends` |
| short name | `cb` |

The `v1alpha1` contract is pre-launch and explicitly unstable (see the carve-out paragraph below for the precise terms); after the v1beta1 promotion, new fields must be additive and tightening validation on existing fields requires a versioned migration path.

**Pre-launch carve-out (active until v1beta1).** The project is pre-launch and `v1alpha1` is explicitly unstable: where keeping an inert, unidiomatic, or operator-confusing field through to `v1beta1` would compound the cleanup work, a per-change waiver allows in-place removal during alpha. Each such removal is gated on (1) a locked design decision naming the field and the reason, (2) zero current consumers (no external operator manifests, no cross-component code), and (3) replacement of the operator-facing surface where one existed. Closed precedent: `CacheTenant.spec.quota.maxMemoryBytes` and `status.memoryUsed` removed (we cannot enforce per-tenant byte budgets on shared engines, and the underlying observation would be double-counted across tenants). The cluster-aggregate sibling `CacheIndex.status.tenants[].memoryUsed` has the same honesty problem (summing per-tenant memory across replicas on a shared engine double-counts the same bytes once per tenant), but because it is a published v1alpha1 *status* field it is **deprecated and zeroed in place** rather than removed: the controller stops populating it (always `0`) and operators are redirected to the per-replica `CacheIndex.status.replicas[].cacheMemoryBytes` (engine total per replica, honest at that altitude), while the field stays in the schema for wire/shape compatibility until its removal at v1beta1. Current applied removals: `CacheBackend.status.health` and the `CacheBackendHealth` enum removed in favour of the standard `status.conditions[Ready|Degraded|Progressing]` surface (the old `Degraded` health value is replaced by `Conditions[Degraded]`), which the new `Ready` printer column displays; and `CacheBackend.spec.storage{,.pvc}` + `status.capacity` removed. The original rationale referenced the now-legacy in-memory `lm://` server and Mooncake provider; current durability/sharing is an explicit typed MP L3 choice, normally Redis (historical rationale: `docs/design/lmcache-server-persistence.md`). Once `v1beta1` is promoted, this carve-out is closed: subsequent breaking changes require a versioned migration.

## Cache hierarchy and ownership

The canonical API assigns one architectural dimension to each field:

```yaml
spec:
  runtime: SGLang
  type: LMCache
  integration:
    role: ReadWrite
  lmCache:
    topology: PodLocal
    chunkSizeTokens: 256
    podLocal:
      server:
        image: docker.io/lmcache/standalone@sha256:...
        port: 5555
        l1Capacity: 32Gi
        maxWorkers: 4
        resources:
          requests: {cpu: "1", memory: 33Gi}
          limits: {memory: 33Gi}
  remoteStorage:
    provider: Redis
    ownership: Managed
    workload:
      nodeSelector:
        cache-tier: shared
      serviceAccountName: cache-provider
    redis:
      image: docker.io/library/redis:7.4-alpine
      resources:
        limits:
          memory: 8Gi
  observation:
    modelID: Qwen/Qwen3
```

- `runtime` selects the inference runtime.
- `type` selects only the engine-side cache implementation.
- `lmCache` and `hiCache` configure local/host cache behavior.
- `remoteStorage.provider` selects the optional remote technology.
- `remoteStorage.ownership` selects controller-managed or external lifecycle.
- Generic managed-workload scheduling and Pod security live under
  `remoteStorage.workload`; provider image, resources, and topology remain
  provider-specific.
- `observation` owns event-observation identity and timing.

Omitting `remoteStorage` is meaningful and never selects infrastructure:

```yaml
spec:
  runtime: SGLang
  type: LMCache
  lmCache:
    topology: PodLocal
    podLocal:
      server:
        image: docker.io/lmcache/standalone@sha256:...
        port: 5555
        l1Capacity: 32Gi
        maxWorkers: 4
        resources:
          requests: {cpu: "1", memory: 33Gi}
          limits: {memory: 33Gi}
```

This requests SGLang typed PodLocal MP with host-only L1. The controller creates
no remote-provider Deployment or Service; the webhook injects the MP server
native sidecar without an L3 adapter.

Capability resolution is deliberately two-dimensional:

```text
runtime + type                 provider + ownership
      |                                  |
      v                                  v
engine-wire adapter              storage-provider adapter
      |                                  |
      +--------- optional Binding -------+
```

The Redis provider adapter owns workload and Service rendering and emits a
structured RESP binding. The engine adapter declares whether it accepts RESP
or host-only operation. Admission rejects unsupported combinations before an
engine Pod is created.

### Cache type validation

`spec.type` is a closed CRD enum containing `LMCache` and `SGLangHiCache`.
Remote-provider technology and lifecycle ownership are not cache types. Redis
is selected through `remoteStorage.provider`, and externally managed
infrastructure through `remoteStorage.ownership`.

Current managed and external Redis examples are available in
[`config/samples/cachebackend-lmcache.yaml`](../../config/samples/cachebackend-lmcache.yaml)
and [`config/samples/cachebackend-external.yaml`](../../config/samples/cachebackend-external.yaml).

## Spec

| Field | Type | Purpose |
|---|---|---|
| `runtime` | enum | Required inference runtime: `VLLM` or `SGLang`. Values are case-sensitive. |
| `type` | enum | Engine-side cache implementation: `LMCache` or `SGLangHiCache`. Defaults to `LMCache`. |
| `lmCache` | object | Typed LMCache MP configuration: topology, chunk size, and PodLocal server image/port/L1/resources. |
| `remoteStorage` | object | Optional remote tier. Omitting it means host-only and provisions no provider workload. |
| `remoteStorage.provider` | enum | Current MP provider: `Redis`. |
| `remoteStorage.ownership` | enum | `Managed` or `External`. |
| `remoteStorage.endpoint` | string | Required for `External`, rejected for `Managed`; managed endpoints are controller-observed in `status.remoteStorage`. Redis requires bare `host:port`. |
| `remoteStorage.workload` | object | Pod scheduling and security for a `Managed` provider workload. Rejected for `External`; deliberately has no generic replicas/autoscaling fields. |
| `remoteStorage.redis` | object | Redis-owned image and resource configuration. |
| `observation` | object | Observation-owned `modelID` and `firstEventTimeout`. |
| `integration.mode` | enum | Which cache tiers the engine is wired for: `Offload` (default) or `EventsOnly`. `Offload` is full participation — cache-aware routing (tier-1) plus the KV-offload connector (tier-2). It may remain host-only, connect to externally owned remote storage, or provision a provider workload when `remoteStorage.ownership` is `Managed`. `EventsOnly` wires routing only: the kvevent-subscriber sidecar is injected when the controller runs with `--kvevent-subscriber-image` set and `observation.modelID` is present; otherwise the append is skipped fail-open. No KV connector or backend server is created. See [Events-only mode](#events-only-mode-specintegrationmode--eventsonly). |
| `integration.role` | enum | Engine participation mode: `ReadOnly`, `WriteOnly`, or `ReadWrite`. Defaults to `ReadWrite`. LMCache currently admits only `ReadWrite`; directional roles remain reserved for a connector that demonstrably enforces them. |
| `integration.failOpen` | boolean | Default `true`. Remote L3 failure is soft by default and the PodLocal MP server can continue L1-only. The co-scheduled `lmcache-mp-server` is part of the required connector path for both vLLM and SGLang, so `failOpen` does not turn a missing or broken PodLocal server into a cacheless engine launch. Setting `false` makes remote storage a serving dependency and is surfaced as a Warning Event. |
| `integration.engineOverrides` | object | Optional engine-injection overrides applied to the args/env the pod-mutating webhook would otherwise inject into the engine container. See [Engine-injection overrides](#engine-injection-overrides-specintegrationengineoverrides). |
| `engineSelector.matchLabels` | map | Equality-based label selector matched against engine **pod** labels (the pod template's `metadata.labels`, not Deployment, DaemonSet, or any other workload-level labels). Every key/value here must appear on the pod for it to match. `matchExpressions` is intentionally not exposed in v1alpha1 — the surface is `matchLabels` only. |
| `hiCache` | object | Typed SGLang native HiCache configuration. Required only for `type: SGLangHiCache`; see [SGLang native HiCache](#sglang-native-hicache). |
| `allowCrossNamespace` | boolean | Opt-in flag that allows `spec.remoteStorage.endpoint` to resolve to a Kubernetes Service in a different namespace from the CacheBackend itself. Without it, admission rejects cross-namespace Service-DNS endpoints. External hostnames and IPs are unaffected. Defaults to `false`. |

> **Per-namespace lookup tuning lives on CachePolicy, not CacheBackend.** The
> lookup latency budget and the minimum-prefix-token gate are configured via
> `CachePolicy.spec.lookupTimeoutMs` and `CachePolicy.spec.minimumPrefixTokens`,
> which are the surfaces actually wired into the server's `ResolvedPolicy` and
> the `LookupRoute` path.

### Resources

Current resources live with the workload owner:
`lmCache.podLocal.server.resources` for the MP native sidecar and
`remoteStorage.redis.resources` for managed Redis. The Redis renderer
deep-copies the selected block onto its managed container.

Managed provider Pod placement and security are configured independently under
`remoteStorage.workload` (`nodeSelector`, affinity, tolerations, topology spread,
image-pull secrets, ServiceAccount, Pod security context, priority/scheduler,
runtime class, and termination grace). Admission rejects this block for
`ownership: External`, because inference-cache does not own that workload.
There is intentionally no generic replica count: the current managed Redis is a
standalone singleton, while a real Redis Cluster needs provider-specific shard,
replica, discovery, and resharding semantics.

**Pass-through to the rendered container.** The provider adapter `DeepCopy`'s
the selected typed resource block onto `Container.Resources`. The deep copy is
load-bearing: the reconciler reads from an informer cache, and writing through
the spec pointer would corrupt the cached object for every subsequent reader.
An explicit empty provider `resources: {}` suppresses the provider default.

**`redis-l2`: the memory limit also sizes the L2 keyspace.** The rendered Redis
provider derives `--maxmemory` from
`remoteStorage.redis.resources.limits.memory` at roughly 80%, with
`allkeys-lru`.

**`resources.claims` is rejected at admission.** `corev1.ResourceRequirements` also exposes a `Claims` slice for Dynamic Resource Allocation (DRA), but the renderer does not plumb the matching pod-level `spec.resourceClaims` — a claim-bound `container.resources.claims` would render a pod the apiserver rejects (claim name doesn't resolve at the pod level). The validating webhook (`rejectResourceClaims`) hard-rejects non-empty `claims` until DRA is wired end-to-end; a nil/empty `claims` slice admits unchanged.

**Request/limit relationship is resource-aware.** The validating webhook (`rejectResourceLimitsBelowRequests`) enforces K8s' two-regime contract:
- **Overcommittable resources** (`cpu`, `memory`, `ephemeral-storage`): `limits[X]` must be `>= requests[X]` when both are set. Memory is the motivating case (an inverted memory limit deepens the OOM-kill cliff this field exists to close), and the rule catches CPU typos with the same diagnostic.
- **Non-overcommittable resources** (`hugepages-*` and vendor-prefixed extended resources like `"nvidia.com/gpu"`): `limits[X]` must EQUAL `requests[X]` when both are set. K8s does not allow overcommitting these — every page or device is dedicated — so request and limit must agree.

Limits-only shapes admit unchanged for any resource — K8s auto-populates `requests` from `limits`. **Requests-only is admitted only for overcommittable resources** (cpu, memory, ephemeral-storage); a non-overcommittable resource declared in `requests` without a matching `limits` entry is rejected (`rejectRequestsOnlyForNonOvercommittableResources`), because K8s requires hugepages and extended resources to declare both halves together.

**Extended-resource quantities must be integers.** Vendor-prefixed extended resources (e.g. `nvidia.com/gpu`) are allocated by whole units — K8s rejects a fractional shape like `nvidia.com/gpu: 500m` on the rendered Pod. The validating webhook (`rejectFractionalExtendedResources`) mirrors that rule at admission, so the operator sees a field-scoped error at `kubectl apply`. Standard overcommittable resources (cpu, memory, ephemeral-storage) admit fractional values — `250m` is the canonical kubelet CPU shape and is unaffected.

**Hugepage quantities must align to the page size.** The Linux kernel allocates hugepages in whole-page chunks, so K8s rejects a misaligned shape like `hugepages-2Mi: 3Mi` (3Mi isn't a multiple of 2Mi). The validating webhook (`rejectMisalignedHugepageQuantities`) parses the page size from the resource name's suffix and rejects any positive quantity that is not a whole multiple of that page size. Zero quantities admit trivially (no allocation) and negative quantities are caught upstream by `rejectNegativeResourceQuantities`.

**Quantities must be non-negative.** The CRD-schema layer treats each `requests`/`limits` entry as a `resource.Quantity` string and admits a leading `-` without complaint. The kubelet only flags the negative quantity once the child pod tries to schedule — by which time the operator is chasing it through Deployment events. The validating webhook (`rejectNegativeResourceQuantities`) rejects any strictly-negative entry at admission so the regression surfaces at `kubectl apply` instead. Zero is admitted (an operator who writes `requests.memory: "0"` is explicitly opting into "no guaranteed minimum", which matches the kubelet's `>= 0` contract).

**Resource names must match K8s container-resource rules.** `ResourceList` keys are opaque map keys at the CRD-schema layer; an invalid name like `"foo"` or `""` persists in etcd and only fails when the apiserver later rejects the child pod. The validating webhook (`rejectInvalidResourceNames`) applies the same rules the apiserver applies to a `Container.Resources` map: standard names (`cpu`, `memory`, `ephemeral-storage`) admit unconditionally; a `hugepages-<size>` name admits only when the size suffix parses as a strictly-positive `resource.Quantity` (e.g. `"hugepages-2Mi"`, `"hugepages-1Gi"` — a bare `"hugepages-"` or non-numeric `"hugepages-nope"` is rejected because the apiserver requires the size token); any other name must be **third-party vendor-prefixed** (e.g. `"nvidia.com/gpu"`) and pass `IsQualifiedName`. A bare unqualified `"foo"` is rejected even though `IsQualifiedName` alone admits it, because the apiserver's container-resource layer requires extended resources to carry a vendor identity. Names under the **K8s-reserved prefixes `kubernetes.io/` and `requests.kubernetes.io/`** are also rejected — those prefixes are reserved for native resources, so extended resources may not use them. The rejection names the offending key so multi-key errors surface together.

**Inert without a controller-managed workload.** Host-only, externally owned,
and `SGLangHiCache` configurations provision no provider Deployment or Service.
Typed PodLocal LMCache still injects its server into each matching engine Pod as
a native sidecar. HiCache host memory belongs to the user-owned engine container
and must be sized on that workload instead.

### vLLM typed PodLocal LMCache MP support

The typed shape `spec.runtime: VLLM`, `spec.type: LMCache`, and
`spec.lmCache.topology: PodLocal` selects a dedicated MP adapter; it does not
reuse the legacy `LMCacheConnectorV1` / `lm://` path. The engine image remains
owned by the inference runtime. No connector-profile annotation or image
allowlist is required: this CacheBackend shape is the only enablement switch.
The webhook validates Pod-visible topology and arguments, then injects the MP
wire. The engine's normal initialization loads the connector and fails before
serving if its image does not contain a compatible LMCache client/API;
admission does not pull, execute, or otherwise introspect the engine image.

The webhook injects a digest-pinned `lmcache-mp-server` native sidecar and adds
the following vLLM launch contract:

- `--kv-transfer-config` selects `LMCacheMPConnector` through
  `lmcache.integration.vllm.lmcache_mp_connector`, points it at
  `tcp://127.0.0.1:<podLocal.server.port>`, and sets `kv_role: kv_both` for the
  only currently admitted LMCache role, `ReadWrite`;
- `--disable-hybrid-kv-cache-manager` is required by the initial validated
  integration;
- `PYTHONHASHSEED=0` stabilizes vLLM's cross-process hash chain;
- `INFERENCECACHE_FAIL_OPEN` mirrors the API setting, although runtime-native
  failure behavior still requires GPU validation.

The typed adapter accepts host-only or RESP bindings. Redis credentials are
mounted into the MP server from `SecretKeyRef`; they are not copied into the
vLLM container. LMCache 0.5.3 TLS and logical-database selection remain rejected
because that RESP adapter cannot consume them. The initial adapter admits TP
but rejects PP/DP greater than one and external multi-process DP flags. These
checks and persisted webhook injection are covered without GPU; the pinned
vLLM image/version, KV reuse, TP determinism, and failure recovery remain Phase
4 runtime gates. Canonical examples are the three
`config/samples/cachebackend-vllm-podlocal-*.yaml` files.

For both typed vLLM and SGLang PodLocal adapters, `l1Capacity` is the usable L1
target, not the complete container budget. The common renderer creates a
memory-backed `/dev/shm` with `sizeLimit: l1Capacity + 1Gi`; admission requires
both the MP-server memory request and memory limit to be at least that value. If
the engine already mounts `/dev/shm`, the adapter reuses it only when it is a
memory-backed `emptyDir` with a `sizeLimit` at least as large as that budget.
This keeps scheduling/cgroup accounting aligned with the tmpfs and leaves room
for LMCache metadata and shared-memory allocator overhead.

### SGLang engine support

SGLang supports two peer cache integrations:

| Runtime/backend pair | Data plane | Controller-managed workload |
|---|---|---|
| `(SGLang, LMCache)` without `remoteStorage` | PodLocal LMCache MP server, host-only | Native sidecar in each selected engine Pod |
| `(SGLang, LMCache)` with Managed Redis | PodLocal LMCache MP server with a shared Redis remote tier | Native sidecar plus Redis Deployment and Service |
| `(sglang, SGLangHiCache)` | Native engine-local host cache | None |

#### SGLang LMCache MP mode

> **SGLang drives LMCache in multiprocess mode (implemented and GPU-validated).** SGLang reads the generated client config through `--lmcache-config-file` and attaches to the PodLocal `lmcache server` over loopback plus shared memory. Optional Redis is an L3 adapter selected explicitly. It does not use the legacy cluster-reachable IP server.

SGLang is the second runtime the cache plane supports (`spec.runtime: SGLang`,
`spec.type: LMCache`; adapter at `internal/adapters/builtin/runtime`). Its engine
adapter configures the node-local MP worker and accepts either no binding
(host-only) or a RESP binding. The independent Redis provider adapter creates a
Redis workload only when `spec.remoteStorage` explicitly selects
`provider: Redis`, `ownership: Managed`.

> **Cluster prerequisite — Kubernetes ≥ 1.29 (REQUIRED for typed PodLocal LMCache).** The MP server is injected as a **native sidecar** — an `initContainers` entry with `restartPolicy: Always`, which K8s only understands from 1.29 (beta, on by default; stable 1.33). On an older cluster the apiserver does not recognize that field, so a typed SGLang or vLLM PodLocal engine pod fails admission (or the server degrades to a plain init container that exits before the engine starts). There is no in-webhook version gate today; operators using typed PodLocal LMCache must run 1.29+.

> **Lookup caveat:** server-derived `LookupRoute` with raw
> `token_ids`/`prompt_text` only hits when the server's global
> `--engine-block-size` matches SGLang's page size. Gateways that send
> pre-computed hashes are unaffected. CacheBackend does not inspect or replace
> the engine image and does not add a package-verifier init container.

The webhook renders the MP data plane on the SGLang engine pod. Alongside the
engine container it adds a **PodLocal `lmcache-mp-server` native sidecar** (an
init container with `restartPolicy: Always`) that runs the supported
`lmcache server` entry point on `127.0.0.1` and writes the client configuration.
With a RESP binding it offloads to Redis; without a binding it runs host-only.
`NVIDIA_VISIBLE_DEVICES=all`
lets the GPU-less sidecar use CUDA-IPC with no device-plugin
allocation, an `exec` startup-probe on the loopback ZMQ port gates the engine's
start, and a shared `emptyDir` carries the config file. For `/dev/shm` (the L1 tier)
it reuses the engine's own volume when the engine already mounts one (a duplicate
mountPath is an invalid Pod), else adds a sized `emptyDir{medium: Memory}` — see the
reserved-names note below for the reuse/reject rules. On the engine container (name
`sglang`) it injects:

- `--enable-lmcache` — SGLang's boolean flag (an argparse `store_true`) that activates its LMCache connector. This replaces vLLM's `--kv-transfer-config` JSON.
- `--lmcache-config-file <path>` — points the engine at the MP config file the worker writes (`mp_host`/`mp_port`); MP mode aborts at startup without it.
- `LMCACHE_USE_EXPERIMENTAL=True` — gates SGLang's experimental LMCache integration; without it `--enable-lmcache` does not engage the connector.
- `INFERENCECACHE_FAIL_OPEN=<true|false>` — the `spec.integration.failOpen` mirror.

> **GPU visibility on the MP worker — an isolation trade-off to know about before running this on a shared node.** The worker sidecar carries `NVIDIA_VISIBLE_DEVICES=all`, so with the NVIDIA container runtime it can see **every GPU on the node**, not only the one its engine was allocated. It holds no device-plugin allocation (no `nvidia.com/gpu` request), so it consumes no GPU from the node's allocatable — but the visibility is real, and on a node shared with other tenants' GPU workloads the worker is not confined to its own device.
>
> **Why it cannot be narrowed.** The worker moves KV by CUDA-IPC: the engine hands it a device **UUID**, which LMCache resolves to a local device index — and that resolution fails unless the device is visible to the worker process. Revoking visibility is GPU-validated as fatal, not degraded: the worker dies with `RuntimeError: Device UUID <uuid> not found in the discovered devices` and the engine never reaches ready. Scoping to just the engine's device is not available to a mutating webhook: the device plugin assigns the UUID at kubelet time, *after* admission runs, so there is nothing to narrow to yet. Giving the worker its own `nvidia.com/gpu` request is worse — it burns a second GPU and the scheduler would hand it a *different* device than the engine's.
>
> **Scope of what the adapter adds.** This is the engine image's own posture rather than something the adapter introduces: sglang images ship `NVIDIA_VISIBLE_DEVICES=all` in their `ENV`, and the device plugin overrides it only for containers that request a GPU (the engine gets a specific UUID; a request-less sidecar keeps the image default). The adapter sets it explicitly so the wire also works on a `workerImage` that lacks that default, instead of depending on an image side effect. Operators who need hard GPU isolation between tenants should not co-schedule those tenants on one node — the same guidance that applies to any CUDA-IPC sidecar.

**Names the MP wire reserves on the engine pod.** The init container
`lmcache-mp-server`, volumes `lmcache-mp-config` and `lmcache-mp-shm`, and mount
path `/var/run/inference-cache/lmcache` are adapter-owned. A foreign collision
rejects injection, which the pod webhook reports while admitting the pod
unwired under fail-open semantics. Re-injecting an operator-rendered pod is
idempotent. An incompatible existing `/dev/shm` mount is rejected rather than
failing later inside LMCache.

The old lm:// `LMCACHE_REMOTE_URL` / serde / chunk-size / local-CPU env is
**NOT** injected — SGLang MP mode ignores it. New manifests use typed
`spec.lmCache` fields:

| Field | Default | Bounds | Purpose |
|---|---|---|---|
| `lmCache.chunkSizeTokens` | `256` | `>=1` | Server chunk size and client config. |
| `lmCache.topology` | required | `PodLocal` | `NodeLocal` is a future shape and is rejected. |
| `lmCache.podLocal.server.image` | required | digest-pinned reference | Independently owned LMCache server image; never copied from or into the engine image. |
| `lmCache.podLocal.server.port` | required | `1`–`65535` | Loopback MP port. |
| `lmCache.podLocal.server.l1Capacity` | required | positive quantity | Usable L1; `/dev/shm` and memory resources must cover this plus 1Gi. |
| `lmCache.podLocal.server.maxWorkers` | required | `>=1` | Server worker bound. |
| `lmCache.podLocal.server.resources` | required | validated K8s resources | Positive CPU request and sufficient memory request/limit. |

Deliberately **not** injected for SGLang (a real engine difference, not an omission): `VLLM_USE_V1` (a vLLM-internal codepath with no SGLang analogue) and `PYTHONHASHSEED` (vLLM pins it to stabilise its builtin-`hash()`-seeded block-hash chain across TP workers; SGLang derives its prefix hash with `hashlib.sha256` over the token-id bytes, independent of `PYTHONHASHSEED`).

**`spec.integration.role` support.** Every LMCache backend currently supports only `ReadWrite` (the default), and admission rejects `ReadOnly` / `WriteOnly` through `rejectUnsupportedLMCacheRole`. SGLang's `--enable-lmcache` path has no role split. vLLM can render `kv_consumer` / `kv_producer`, but live GPU validation found that LMCache 0.5.3 still stored in consumer mode and retrieved in producer mode. Directional roles remain in the generic API for other backends and a future validated LMCache connector, but inference-cache does not claim semantics the selected data plane cannot enforce.

**Reserved set** (`internal/adapters/builtin/runtime`): `ReservedArgs()` = `--enable-lmcache`, `--lmcache-config-file`; `ReservedEnv()` = `LMCACHE_USE_EXPERIMENTAL`, `INFERENCECACHE_FAIL_OPEN`. In MP mode the old lm:// `LMCACHE_REMOTE_URL` is neither injected nor reserved. `VLLM_USE_V1` / `PYTHONHASHSEED` are not reserved because they are never injected.

The two override surfaces are separate: `spec.lmCache` shapes the server
sidecar, while `spec.integration.engineOverrides` edits the engine container's
args/env only.

**KV-event source & `hash_scheme: "sglang"`.** SGLang adopted vLLM's KV-event wire wholesale — `--kv-events-config` drives a ZMQ `ZmqEventPublisher` emitting the same msgspec array-like `BlockStored` / `BlockRemoved` / `AllBlocksCleared` tuples (`BlockStored` carries `token_ids`, so the subscriber derives the same in-pod content fingerprint it does for vLLM). The shipped `kvevent-subscriber` binary therefore decodes SGLang's stream **unchanged**; the only difference is the adapter pins the sidecar's `--hash-scheme=sglang`. The index keys on `(tenant, model, hash_scheme, adapter, prefix_hash)`, so SGLang prefixes occupy a **domain disjoint** from vLLM's: a request hashed under one scheme never false-hits a bytewise-identical entry recorded under the other, even when both engines tokenize the same text to the same token ids and the content fingerprints collide. As with vLLM, the operator must launch the engine with `--kv-events-config '{"publisher":"zmq","endpoint":"tcp://*:5557","topic":"kv-events"}'` for the publisher to be active — the adapter wires the cache offload, not the event publisher.

> **Block-size alignment for server-derived lookups (operational note).** The subscriber-ingested path is block-size-safe by construction: the subscriber derives each prefix fingerprint in-pod using the `block_size` carried on the engine's own `BlockStored` event (SGLang's `--page-size`, often 64), so SGLang entries land in the index at SGLang's block size with no server involvement. The **server-derived** lookup path is the catch: when a gateway calls `LookupRoute` with raw `token_ids` / `prompt_text` (rather than a pre-computed `prefix_hash` / `block_hashes` chain), the server fingerprints them with its single global `--engine-block-size` (default 16, vLLM's). For SGLang those server-side hashes only line up with SGLang-ingested entries when `--engine-block-size` is set to SGLang's page size. Because the flag is **one global value**, a single server cannot serve raw-`token_ids` lookups for both a vLLM (16) and an SGLang (64) deployment at once — in a mixed-engine cluster, have gateways send the pre-computed `prefix_hash` / `block_hashes` (the subscriber/fingerprint path, which is block-size-correct per engine) for the raw-token path, or run a server per block size. Making `--engine-block-size` per-`hash_scheme` is a server change tracked as a follow-up; it is the first concrete latently-vLLM-centric assumption this second engine surfaced.

> **MP mode implemented (GPU-validated 2026-07).** The once-open wire-test question — does SGLang honour `LMCACHE_REMOTE_URL` from the env, or require a config file? — resolved to "neither the old way": SGLang ignores the `LMCACHE_*` env and reads config only from `--lmcache-config-file`, driving LMCache in **MP mode**, not the `lm://` remote-server model. The adapter now renders exactly that — config-file + node-local MP-worker sidecar + shared Redis L2 — validated end to end (store→flush→retrieve reuses KV via the worker). Full design + evidence: [`sglang-lmcache-mp-mode.md`](sglang-lmcache-mp-mode.md).

#### SGLang native HiCache

The `(sglang, SGLangHiCache)` pair configures the selected SGLang engine Pods
directly. It does not create a cache-server Deployment, Service, HPA, or
endpoint. The first implementation intentionally publishes no `Ready`
condition: Kubernetes Pod readiness proves that SGLang is serving, but does not
prove a HiCache host-tier write/read round trip. A dedicated readiness contract
is a separate follow-up.

The required integration shape is:

```yaml
spec:
  runtime: SGLang
  type: SGLangHiCache
  engineSelector:
    matchLabels:
      app: sglang
  hiCache:
    # Exactly one:
    ratio: "2.0"
    # sizeGB: 64
    # Optional typed pass-through:
    writePolicy: write_through
    ioBackend: kernel
    memoryLayout: layer_first
```

`ratio` is a string containing a finite number greater than zero; `sizeGB` is a
positive integer. The optional fields are injected only when present, leaving
defaults to the SGLang version in the engine image. Their accepted values match
the SGLang CLI:

- `writePolicy`: `write_back`, `write_through`, `write_through_selective`
- `ioBackend`: `direct`, `kernel`, `kernel_ascend`
- `memoryLayout`: `layer_first`, `page_first`, `page_first_direct`,
  `page_first_kv_split`, `page_head`

The webhook injects `--enable-hierarchical-cache` plus the corresponding
`--hicache-*` flags at Pod CREATE time. A one-container Pod may use any
container name; a multi-container Pod must name its engine container `sglang`.
Existing matching arguments are left byte-for-byte unchanged. A different
value, the opposite capacity mode, a malformed/duplicate argument, or existing
SGLang LMCache flags causes the whole injection to fail open without partial
HiCache wiring.

Changing or deleting the CacheBackend does not mutate live Pods. Roll the
SGLang workload to apply a new configuration or switch between LMCache and
HiCache. The webhook does not inspect image tags: the chosen image must support
these SGLang arguments.

HiCache host memory is charged to the engine container's cgroup. The operator
must size the engine's memory request/limit and node capacity accordingly.
Inference-cache does not derive resource changes from `sizeGB` or `ratio`, and
does not add `/dev/shm`, hugepages, memlock, hostIPC, or privileged settings.
The KV-event subscriber reads its model identity from
`spec.observation.modelID`, independently of the HiCache configuration.

### Events-only mode (`spec.integration.mode = EventsOnly`)

`spec.integration.mode` selects which cache tiers an engine is wired for. The default, `Offload`, is full participation: cache-aware routing (tier-1) PLUS KV offload (tier-2). Server-backed managed adapters include a controller-provisioned backend server; engine-local adapters such as SGLang HiCache provide tier-2 inside the engine pod and create no server workload or endpoint. `EventsOnly` wires the routing tier only.

**What events-only does and does not provision.** An events-only backend is the lighter, routing-only deployment:

- **No provisioned server.** The reconciler creates no Deployment or Service and leaves `status.connector` and `status.remoteStorage` absent.
- **No KV connector.** The pod webhook does not inject runtime connector arguments or an MP server. It may append only the observation subscriber described below.
- **Mode wins over host-tier configuration.** If `spec.lmCache` is present, `EventsOnly` still injects no LMCache connector or host-tier settings; the block is ignored for engine wiring. Operators should omit `spec.lmCache` on routing-only resources so the manifest does not imply an active host tier. `spec.remoteStorage` is rejected rather than ignored because it declares a provider that nothing would dial.
- **The kvevent-subscriber sidecar is injected — when wired.** That is the whole point of routing: once the sidecar is appended, `LookupRoute` and the per-backend `status.indexParticipation` slice behave identically to a managed backend; only the offload tier (server + connector) is absent. The append is gated exactly as for a managed backend and is skipped **fail-open** when either gate is unmet: the controller must run with `--kvevent-subscriber-image` set (unset by default, so a default install injects no subscriber) AND `spec.observation.modelID` must be present to supply `--model-id`. When skipped, the webhook leaves the engine pod untouched and stamps no `injected-by` annotation.
- **Evictions are tier-aware.** The subscriber tags each prefix with a cache tier from the block lifecycle: `BlockStored` → **T1** (resident in HBM). On a `BlockRemoved`, the two modes diverge. In `Offload` mode the paired LMCache L2 tier still holds the block after the engine evicts it from HBM, so the subscriber (`--ignore-block-removed=true`) **re-reports the evicted prefix at tier T2** (reload-able from host RAM), anchored at the eviction timestamp — the entry is *kept*, not dropped, and honestly tagged colder than HBM; a later `BlockStored` of the same content re-reports it back at T1. In `EventsOnly` mode there is no L2 retaining the block, so a `BlockRemoved` genuinely means the prefix is gone and the hint MUST be pruned — the subscriber omits the flag and forwards the eviction as `PREFIX_EVICTED`. Either way a stale/mis-tagged hint is soft state (a cache miss at worst, never a wrong answer). See `docs/design/kvevent-subscriber-wiring.md` "L2 cache tier semantics".

**Readiness is gated on the first KV event, same as managed.** An events-only backend has no workload to wait on, so it is "up" the moment it exists — the `firstEventTimeout` clock starts immediately (`status.firstAvailableAt` is latched on the first reconcile). It then runs the same [KV-event readiness gate](#kv-event-readiness-gate) as a managed backend: `Ready=False/AwaitingFirstKVEvent` until the first event, `Ready=True/KVEventsObserved` once `status.indexParticipation.lastEventAt` is observed, and `Ready=False/NoKVEventsObserved`, `Degraded=True` if the window elapses with no event. The base Ready reason is `EventsOnlyActive`. The managed-only advisory conditions `FunctionalProbeOK`, `EngineKernelsHealthy`, `T2Degraded`, and `EngineCompatibility` are never published on an events-only backend — there is no server to functionally probe, no LMCache native-kernel check (events-only loads no connector, so the `lmcache-kernel-check` init container is never injected), no tier-2 to mark degraded, and no injected KV connector that could be incompatible (events-only injects none, and an Offload→EventsOnly flip clears any prior verdict).

**Why it exists.** `EventsOnly` is the supported integration for **hybrid-attention models** that cannot take a vLLM KV connector — Qwen3.6/Next gated-DeltaNet, Mamba/Jamba, KDA, Falcon-H, Granite-hybrid, and similar. vLLM disables its hybrid KV-cache manager the moment any KV connector is loaded (KV-spec unification then fails at init), so these models cannot take the tier-2 connector; but their KV events coexist fine with the hybrid manager, so cache-aware routing still works. It is also simply a lighter deployment for routing-only users who do not want an offload tier at all.

**Admission constraints.** Because an events-only backend provisions no server, server-shaped configuration is structurally meaningless and is rejected at admission:

- `spec.remoteStorage` is forbidden — any Managed or External declaration requests an offload provider that events-only deliberately does not wire.

### LMCache MP server / client version alignment

The digest-pinned `spec.lmCache.podLocal.server.image` and the LMCache client
inside the operator-owned engine image must expose compatible MP APIs. The
controller does not own or rewrite the engine image, and it does not use an
image allowlist or verifier init container. Normal engine startup is the
authoritative connector/package compatibility check.

Pin the MP server image by digest and pin the engine image/package set through
the inference system's own release process. A connector mismatch is surfaced
through the engine Pod's startup failure and the advisory
`EngineCompatibility` observation; inference-cache cannot prove package
compatibility from image names alone.

### LMCache client kernels ↔ engine-image CUDA / vLLM alignment

The lmcache **client** compiled into the engine image ships native CUDA
kernels (`lmcache.c_ops`). They must match the engine image's CUDA runtime. A
mismatch — e.g. a `pip install lmcache` that pulls a CUDA-13-built wheel onto a
CUDA-12.9 image — fails to load (`Failed to import backend lmcache.c_ops:
libcudart.so.13`) and lmcache **silently falls back** to a single-stream torch
path that does not parallelize. T2 reload still works in isolation but
serializes under concurrency (measured ~10× slower), and the failure surfaces
only as a log WARNING. This is the same silent-degradation class as the
wire-protocol skew above, in its **local-kernel** variant.

Because `import lmcache.c_ops` is overridden to a fallback shim on load failure
(so it always succeeds and cannot be used as a health check), the control plane
detects this at **deploy time** with an injected `lmcache-kernel-check` init
container that force-loads LMCache `c_ops` from disk and imports the vLLM core
native extension shipped by the engine image (`vllm._C_stable_libtorch` in
current stable-ABI builds, falling back to legacy `vllm._C`). Checking both
LMCache and vLLM matters because their `libcudart` dependencies can differ. It
reports onto the CacheBackend
`EngineKernelsHealthy` condition (see
[Conditions](#conditions)) and is configured per-CacheBackend via the
`inferencecache.io/lmcache-kernel-check` annotation:

| Annotation value | Behavior |
|---|---|
| `auto` (default / unset) | Inject in report-only mode **only** when the engine container requests a GPU (the kernels are GPU-only; a CPU build legitimately has none). |
| `report-only` | Always inject; a native LMCache/vLLM load failure makes the detector exit 0, so it does not block the engine pod (best-effort fail-open — see the residual cases in [Boundaries](#boundaries-what-the-check-does-and-does-not-prove)). The condition surfaces the result. |
| `strict` | Always inject; on failure the engine pod stays in `Init` and never serves (fail-closed), and the managed CacheBackend `Ready` is downgraded with reason `EngineKernelDegraded`. |
| `off` | Never inject. |

The annotation value is validated at admission — an unrecognized value (e.g. a
`strcit` typo) is **rejected**, so a typo cannot silently relax strict
enforcement back to report-only. Changing the annotation affects only
**newly-admitted** engine pods; the `EngineKernelsHealthy` condition and any
strict `Ready` downgrade reflect each pod's *actual admitted mode* (read from
the pod, not the CacheBackend's current annotation), so flipping the annotation
on a live backend takes effect as its pods roll.

**Engine scope — vLLM only today.** The kernel-check init container is provided
by the runtime adapter via the private internal `InitContainerProvider` capability, which
only the vLLM+LMCache adapter implements. The SGLang+LMCache adapter does **not**
implement it yet, so `inferencecache.io/lmcache-kernel-check` has no effect on
SGLang engine pods and `EngineKernelsHealthy` is not published for them — even
though SGLang loads the same lmcache client and would benefit from the same
check. The annotation is still shape-validated for any CacheBackend (the value
rule is engine-agnostic); it simply injects nothing on the SGLang path.
Extending the check to SGLang is a follow-up.

**Boundaries (what the check does and does not prove):**

- `EngineKernelsHealthy=True` means the native kernels **loaded** — it does
  **not** mean T2 reload is fast under concurrency. The reload-serialization
  symptom is validated separately by a real-GPU concurrency canary.
- The check proves **load-time** linkage (it catches a missing/mismatched
  `libcudart`). It does **not** prove **runtime executability**: a `libcudart`
  present but paired with a too-old driver loads cleanly and fails only at
  kernel launch. That residual is caught only at runtime.
- **Strict-mode GPU cost:** a pod stuck in `Init` (failing the check in strict
  mode) still holds its `nvidia.com/gpu` reservation while serving nothing.
  Reclaim it by fixing the engine image's vLLM/LMCache/CUDA alignment or switching
  the annotation to `report-only`.
- The check runs `import torch` (the native extension links libtorch), adding a
  few seconds to GPU engine-pod startup. The engine imports torch anyway.
- **Report-only fail-open is best-effort.** The init container runs the engine
  image's own `python3`; in report-only mode the detector always exits 0, so a
  native-extension failure never blocks the pod. The init container declares small CPU/
  memory requests and no limits — the most broadly-compatible shape, but note
  that *no* resource shape is fail-open under every namespace policy: a
  `ResourceQuota`/`LimitRange` that requires per-container requests rejects a
  container with none, while a `LimitRange` per-container max can reject large
  ones. Small requests stay below any max the GBs-needing engine already
  satisfies and are subsumed by the engine's in the pod's effective request
  (so no scheduling/quota footprint increase); omitting limits avoids the
  max-limit trip. The other residual ways it could block are `python3` failing
  to start at all (which means the Python engine is itself broken — not a false
  outage caused by this check) or an OOM during `import torch` (the check sets
  no memory limit, so the import is bounded by the pod/node unless a namespace
  `LimitRange` defaults a memory limit onto it — in which case a too-small
  default could OOM the import, the same way it would constrain any unlimited
  container). The check is deliberately *not* wrapped in a shell to force exit
  0, because a minimal/distroless image could lack `/bin/sh` and reintroduce
  the very block the wrapper aimed to avoid.

`EngineKernelsHealthy` complements `FunctionalProbeOK` (which round-trips the
server-side cache path): the kernel check catches the engine-side load cause
that the round-trip probe cannot see.

### Removed IP and Mooncake history

The former standalone LMCache IP server and Mooncake-through-LMCache provider
were physically removed in Phase 7. They are not accepted by the served CRD and
must not be translated to Redis automatically because that would change
sharing and durability semantics. Historical rationale remains in
[the migration roadmap](lmcache-multiprocess-migration-roadmap.md) and
[lmcache-server-persistence.md](lmcache-server-persistence.md).

## Status

Status separates the Pod-local connector from the optional network-addressable
remote tier:

| Field | Purpose |
|---|---|
| `connector` | Effective MP mode/topology and matched, ready, covered, and uncovered engine/server counts. Pod-local loopback addresses are deliberately not published. |
| `remoteStorage` | Optional Redis provider, endpoint, and readiness. Absent for host-only and EventsOnly backends. |
| `matchedEnginePods` | Snapshot count for `spec.engineSelector`; nil means not yet computed, while zero is an observed no-match state. |
| `engineSelectorMessage` | Operator-facing selector mismatch diagnosis. |
| `failOpen` | Observed effective integration fail-open value. |
| `observedGeneration` | Latest CacheBackend generation reconciled. |
| `firstKVEventObservedAt`, `firstAvailableAt` | Monotonic anchors for the first-event readiness gate. |
| `indexParticipation` | Prefix count, last event, hit rate, and optional T2 hit rate projected by the CacheIndex poller. |
| `conditions` | Kubernetes conditions described below. |

The `kubectl get cachebackend` table displays Type, Ready, Matched, the remote
Redis endpoint, Prefixes, LastEvent, and Age. It never presents the Pod-local MP
loopback address as a cluster endpoint.

### Conditions

- `ConnectorReady` reports whether every selected engine Pod carries the
  webhook-authenticated injection record for the current generation and has a
  Ready `lmcache-mp-server` native sidecar.
- `RemoteStorageReady` reports the optional Redis tier independently. Managed
  Redis readiness comes from its Deployment and Service; External Redis is
  accepted from the validated operator endpoint.
- `Ready` always requires the connector. Redis also gates it when
  `integration.failOpen: false`; with the default fail-open behavior a degraded
  L3 does not hide a healthy Pod-local connector.
- `Progressing` and `Degraded` retain the standard convergence/stuck split.
  The first-KV-event, functional-probe, kernel-health, T2, and engine
  compatibility conditions remain advisory or gating according to their
  dedicated settings.
- EventsOnly publishes routing readiness without connector or remote-storage
  status. SGLangHiCache remains engine-local and provisions no backend
  workload.

### Supported-model matrix

The cache plane has two levers — **routing** (driven by kv-events; works on any engine) and the **tier-2 offload** (driven by a vLLM KV connector). Standard-attention models get both; **hybrid-attention** models get routing only, because vLLM's KV-connector interface is mutually exclusive with its hybrid KV-cache manager — wiring any connector triggers a fatal KV-spec-unification error at engine init. This is a vLLM-level constraint that applies to *any* connector (LMCache, Mooncake, NIXL); it is not an LMCache gap, and swapping the backend does not lift it.

| Model family | Attention | Routing (T1) | Offload (T2) | Integration |
|---|---|---|---|---|
| Llama, Qwen3 dense, Mixtral, … | standard | ✅ | ✅ | full (connector + subscriber) |
| Qwen3.6 / Qwen3-Next (gated-DeltaNet) | hybrid | ✅ (events-only) | ❌ | events-only — **supported** (`spec.integration.mode: EventsOnly`) |
| Mamba / Jamba, Falcon-H, Granite-hybrid, KDA | hybrid | ✅ (events-only) | ❌ | events-only — **supported** (as above) |

A hybrid model wired with the full (connector) integration crash-loops at engine init; the controller surfaces the crash-loop as `EngineCompatibility=False/InjectedEngineCrashLooping` (above) instead of leaving it a silent CrashLoopBackOff. The condition reports the observation, not a proven cause — confirm via the engine logs (a crash-loop can also be a bad image/command/secret/OOM). When it is the connector incompatibility, set `spec.integration.mode: EventsOnly` — the supported connector-less integration that preserves routing (kv-events, no offload; the controller provisions no server and injects no connector, but still wires the kvevent-subscriber). Avoid `inferencecache.io/skip-inject`, which opts the pod out of cache wiring entirely (subscriber included) and so drops routing rather than preserving it.

### KV-event readiness gate

`Ready` (for managed backends) means "the managed backend is up **and** the cache plane is actually receiving engine state" — not merely "the workload rolled out". The reconciler observes the **managed cache-backend Deployment** it owns (its rollout + `Available` condition); that proves the backend workload is up but says nothing about whether engine pods are attached and their ZMQ KV-event publishers are publishing. An engine can be serving HTTP while its publisher is silent (mis-configured `--kv-events-config`, a ZMQ bind failure, or an in-process publisher crash), or no engine pods may be wired to the backend at all — in either case `LookupRoute` keeps returning `NO_HINT` for that backend's prefixes while the CR claims everything is fine. The gate makes that silent degradation loud.

The signal source is `status.indexParticipation.lastEventAt` (written by the CacheIndex poller from engine-pod reports); the reconciler only reads it. Once the managed cache-backend Deployment is `Available`:

- **no event yet, within `firstEventTimeout`** → `Ready=False/AwaitingFirstKVEvent`, `Degraded=False`. The timeout clock starts when the backend becomes "up" — for a managed (`Offload`) backend when its Deployment first reports `Available`, for an `EventsOnly` backend (which has no workload) on the first reconcile it is wired — captured in `status.firstAvailableAt` (a stable anchor, so a later availability flap does not restart the window; see that field's row for the mode-transition re-anchor).
- **at least one event ever observed** → `Ready=True/KVEventsObserved`, `Degraded=False`. An event already present on the first reconcile counts — there is no required transition through `AwaitingFirstKVEvent`. "Ever observed" is durable: the first observation is latched into `status.firstKVEventObservedAt`, because the poller's `lastEventAt` is a current-view value it clears when the backend's replicas drain — without the latch a drained-but-healthy backend would wrongly fall back to `AwaitingFirstKVEvent`. The gate is a first-event startup probe, not an ongoing liveness check.
- **no event by `firstEventTimeout`** → `Ready=False/NoKVEventsObserved`, `Degraded=True/NoKVEventsObserved`. Once Degraded it stays Degraded until an event arrives, then transitions to Ready.

The gate is **on by default** and opt-out per CR with the annotation `inferencecache.io/require-kv-events: "false"` (alpha soft-rollout knob; an annotation rather than a spec field so it can be retired once the gate is trusted). External ownership is **always exempt** — the control plane does not own a provider workload to gate, so readiness is determined by accepting the provider-specific `spec.remoteStorage.endpoint` as described above.

**Operator note.** If a backend is stuck at `Ready=False/AwaitingFirstKVEvent` (and then flips to `Degraded=True/NoKVEventsObserved` after `firstEventTimeout`), either no engine pods are attached to the backend or the engine's KV-event publisher is mis-configured — check that engine pods are wired to the backend, then the engine's `--kv-events-config` and that its ZMQ socket bound. In `kubectl get cachebackend` the `Ready` column shows `False` and the `LASTEVENT` column shows `<none>`; the specific reason (`AwaitingFirstKVEvent` / `NoKVEventsObserved`) and the remediation hint live in the `Ready` / `Degraded` conditions, which `kubectl describe` surfaces along with the `NoKVEventsObserved` Warning Event.

### Functional-probe gate

The KV-event gate confirms that "at least one engine event has flowed into the index"; the functional-probe gate confirms that **the cache plane's round-trip is actually working**. Past phases have seen silent-failure modes — a tenant_id mismatch between subscriber and lookup that returns NO_HINT for every routed request; a proxy↔server hash-encoding skew that produces 0% PREFIX_MATCH despite well-formed events; a tier-2 client/server version skew that 0-hits across millions of queries — where each gate above said `Ready=True` while the cache plane was effectively a no-op. The functional probe drives a deterministic synthetic round-trip per CacheBackend and reflects the per-stage result on the CR.

Composition order on a managed CacheBackend's Reconcile is `managed-readiness → KV-event gate → functional-probe gate`. The probe gate **only fires when the upstream KV-event gate would otherwise say Ready=True** (a broken upstream can't be diagnosed by a downstream probe, and probing every not-yet-ready backend on every reconcile is pure noise). The signal source is the server-side `/probe` endpoint the controller POSTs to once per backend per rate-limit window (~30s); the reconciler reads the per-stage outcome (`ingest`, `routing`, `t2`) and translates it into `FunctionalProbeOK`:

- **all stages `ok` (or `skipped`)** → `FunctionalProbeOK=True/ProbeOK`, no Ready change. `t2` is intentionally `skipped` on every install today — no `T2Prober` is registered into the server binary yet, so Stage C reports `skipped` for every managed backend that runs the gate (the future state where a `T2Prober` is wired flips this to `ok`/`failed`). `External` backends never reach this evaluation at all — they are wholly exempt from the functional-probe gate per the per-CR exemption noted below — so the `True/ProbeOK` outcome is only ever published on managed backends.
- **stage `failed`** → `FunctionalProbeOK=False` with reason `ProbeIngestFailed` / `ProbeRoutingFailed` / `ProbeT2Failed` and the server's stage diagnostic in `.message`; `Ready` is downgraded to `False` with the same reason so the operator-visible Ready signal points at the broken layer.
- **transport / HTTP error** → behavior depends on whether a prior failure exists on this backend:
  - If `FunctionalProbeOK` is absent OR `True`, write `FunctionalProbeOK=Unknown/ProbeError` and **leave `Ready` alone**. A brief server outage should not flap every backend `Ready=False` — the snapshot poller / policy pusher use the same noise-avoidance posture, and `Unknown` is the operator's signal to investigate the probe wiring itself rather than the backends.
  - If `FunctionalProbeOK` is already `False/Probe*Failed` (a prior stage failure), **preserve the existing condition AND keep `Ready=False` with the prior failure's reason** (sticky-False). Letting an HTTP error fade a known per-stage failure to `Unknown` (and therefore back to `Ready=True`) would mask a real regression every time the server happens to be transiently unreachable. The False condition is sticky until a successful probe explicitly resolves it.
- **rate-limited reconcile** → no probe call. If the existing `FunctionalProbeOK` is already `False`, the Ready downgrade is re-applied so the status patch doesn't silently overwrite the prior failure with the upstream KV gate's `Ready=True`. The reconciler also schedules a requeue at the rate-limit window expiry so a quiet stuck backend re-probes even without external watch events.

The gate is **on by default** when the controller is wired with a `--server-probe-url`. The operator escape hatch is the **annotation `inferencecache.io/skip-functional-probe: "true"`** (alpha soft-rollout knob; annotation not spec field so it can be retired once the gate is trusted): when set, the probe call is skipped entirely and `FunctionalProbeOK=True/ProbeBypassed` is published — the Ready gate does not downgrade. Disabling the controller-side gate entirely is achieved by passing `--server-probe-url=""` to the controller binary; the CacheBackend reconciler then never calls the endpoint and clears any stale `FunctionalProbeOK` condition the next time it processes the CR. External ownership is **always exempt** — there is no managed Deployment for the gate to compose with, and the controller does not drive a cache-plane round trip for an operator-managed endpoint.

**Operator note.** If a backend is stuck at `Ready=False/Probe*Failed`, read the condition's `.message` — the server populates a stage-specific diagnostic (e.g. "synthesized event not in index — in-process index ingest path is broken"). For `ProbeIngestFailed` the cache-server's ingest path is broken; for `ProbeRoutingFailed` the index routing / key-derivation layer is broken; for `ProbeT2Failed` the configured tier-2 backend rejected the put or returned nothing on the get. `FunctionalProbeOK=Unknown/ProbeError` means the controller could not reach `/probe` at all — check that `--server-probe-url` is reachable from the controller pod (intra-cluster `inference-cache-server:8081` by default), that the projected SA token is mounted (`/var/run/secrets/inferencecache.io/controller-token/token`), and that the audience-bound TokenReview accepts the controller SA.

### Index Participation

| Field | Type | Purpose |
|---|---|---|
| `indexParticipation.prefixCount` | integer | Sum of distinct prefix entries currently attributed to this backend's replicas. `0` is a valid observed value (the backend is up but holds no warm prefixes yet); always serialized. |
| `indexParticipation.lastEventAt` | time | Most recent KV-event timestamp observed for any of this backend's replicas. Unset until the first event arrives; readiness gates must treat the absent value as "not yet observed" rather than epoch. |
| `indexParticipation.hitRate` | string | Prefix-count-weighted cache hit rate across this backend's replicas, formatted as a decimal in `[0,1]`. **Always unset today** — a missing value MUST NOT be interpreted as `0`. The per-replica snapshot now carries a stats-reported presence bit, but the per-backend view aggregates many replicas onto one backend with no defined backend-level hit-rate reduction (mean? token-weighted? over which replicas?), so backend hit-rate aggregation is deliberately deferred to a follow-up; that presence bit is consumed only by the cluster-aggregate `CacheIndex.status`. Do not expect this field to begin populating from the presence-bit change alone. |
| `indexParticipation.t2HitRate` | string | Query-weighted reload hit-rate of the **tier-2 (external offload, e.g. LMCache) cache** across this backend's replicas, as a decimal in `[0,1]`. Sourced from the engines' `vllm:external_prefix_cache_{hits,queries}_total`. **Presence is load-bearing:** unset means tier-2 has not been exercised (no external lookups across any replica) — distinct from `"0"`, which means the tier WAS queried but served zero reloads, i.e. a silently-degraded offload tier (store/connection failure, under-sized remote server, or scheduler/worker hash mismatch). A healthy reusing workload reads well above `0`. |

The poller attributes each `/snapshot.replicas[]` entry to a single owning `CacheBackend` by resolving the engine pod it came from. The subscriber sidecar runs inside the engine pod and reports `replica_id = <pod-name>`, `tenant_id = <pod-namespace>`. For each replica the poller:

1. Looks up the engine pod by `(tenant, replicaID)`.
2. If the pod carries the webhook's `inferencecache.io/injected-by` annotation (stamped as `<namespace>/<name>`), resolves the owning CacheBackend directly. This is the authoritative wiring signal — the engine container was wired to exactly that backend's endpoint.
3. Otherwise, iterates that namespace's CacheBackends sorted by `metadata.name` and picks the first whose `spec.engineSelector.matchLabels` is non-empty and is a subset of the pod's labels. This mirrors the pod webhook's first-match rule for pods that bypassed the webhook (manual sidecar attachment, opt-out).

Only ONE CacheBackend ever claims a given replica — overlapping selectors must agree on which backend owns the pod, otherwise status would disagree with what the engine was actually wired to. A CacheBackend without an EngineSelector (or with empty `MatchLabels`) is excluded from the selector fallback — otherwise a misconfigured backend would silently claim every replica in its namespace by vacuous truth — but a pod can still be attributed to it via the `injected-by` annotation. A replica whose pod can no longer be found (drained between events and now) is skipped; its data still appears in the cluster-wide `CacheIndex`. A failing scrape preserves existing state (soft-state); a successful scrape that finds no matching replicas resets `prefixCount` to `0` so stale positive values do not survive a drain.

## Contract Notes

- Lookup paths fail open by default. The co-scheduled MP server is required by
  both typed LMCache engine wires and always gates connector readiness. The
  optional Redis tier is the fail-open boundary: with the default
  `spec.integration.failOpen: true`, Redis can degrade while the Pod-local L1
  remains usable; `false` makes Redis a readiness dependency.
- The controller emits Events on the `CacheBackend` only on meaningful state changes, never on steady-state reconciles. Condition-transition-keyed Events: `BackendDegraded` (Warning) on entering `Conditions[Degraded]=True` with reason `ReplicasUnavailable` (the KV-event-gate `NoKVEventsObserved` flavor is suppressed — it carries its own event), `BackendRecovered` (Normal) on the transition back to `Ready=True` (similarly suppressed when recovering from `NoKVEventsObserved`, which carries its own `KVEventsObserved` event); the `FailClosedEnabled` / `FailOpenRestored` pair above; the KV-event readiness gate's `AwaitingFirstKVEvent` (Normal), `KVEventsObserved` (Normal), and `NoKVEventsObserved` (Warning); `EngineSelectorUnmatched` (Normal) when a configured selector first observes zero matching pods while engine pods are expected, transitions from matched to zero, or gains the diagnostic message during an upgrade from an older zero-count status. One advisory Event is recorded on the `CacheBackend` but triggered by engine-pod state rather than a CacheBackend condition transition: `InjectedEngineCrashLooping` (Warning) is emitted once when an injected engine pod's engine container is first observed in CrashLoopBackOff after connector injection — commonly a connector incompatibility (esp. a hybrid-attention model), surfaced as `EngineCompatibility=False/InjectedEngineCrashLooping`, but a crash-loop can also be a bad image/command/secret/OOM, so the cause is verified via the engine logs, not asserted by the Event. The controller does not watch engine pod status — it detects this on the next `CacheBackend` reconcile that lists the pods, so the Event reflects observation time, not the instant the container entered CrashLoopBackOff; a transient pod-list failure preserves the prior condition rather than re-firing it.
- A `Normal InjectedByCacheBackend` Event is emitted on engine pods the mutating webhook stamps with both `inferencecache.io/injected-by` AND `inferencecache.io/injected-by-uid`, where the UID annotation matches the live CacheBackend's `metadata.uid` at reconcile time. The controller deliberately skips emission when (a) the named CR cannot be looked up (NotFound), (b) the UID annotation is absent (failurePolicy=Ignore forgery shape), or (c) the UID does not match the live CR (forgery or CR was recreated under the same name). Non-NotFound lookup errors surface as reconcile errors so controller-runtime retries with backoff. A pod explicitly opted out with a truthy `inferencecache.io/skip-inject` is instead stamped with `inferencecache.io/inject-skipped: skip-inject-annotation`; the same post-create controller emits a `Normal SkippedByOperator` Event only when both the truthy opt-out annotation and the webhook's skipped marker are present. The Events are recorded by a Pod-watching controller, not by the webhook itself: at mutating-admission time the apiserver hasn't assigned `metadata.uid` to the pod yet, so an event recorded from the webhook would carry `involvedObject.uid=""` and be invisible to describe (which filters events by UID). Routing the emission through a post-create controller is what guarantees the event reaches the user-visible surface. There is no `NoMatchingCacheBackend` Event; the no-match signals are `status.matchedEnginePods == 0`, `status.engineSelectorMessage`, and `EngineSelectorUnmatched` on the CacheBackend.
- Optional nested specs are pointer fields in Go so omitted objects stay absent
  in JSON. `spec.integration` and `spec.observation` are the deliberate
  exceptions: the defaulting webhook materializes them so their nested defaults
  persist. Read-time helpers retain defensive defaults for callers that bypass
  admission.

## Admission

The controller serves two webhooks for CacheBackend, both registered as `failurePolicy: Fail` with `sideEffects: None` on CREATE and UPDATE. Webhook serving requires cert-manager (see README "Cluster Prerequisites").

### Defaulting (mutating)

Schema defaults set `spec.type=LMCache`,
`spec.integration.mode=Offload`, `spec.integration.role=ReadWrite`, and
`spec.integration.failOpen=true`. The webhook materializes omitted
`integration` and `observation` parents and sets
`observation.firstEventTimeout=5m`. It does not default workload replicas,
autoscaling, provider images, or an engine image.

### Validating

Validation aggregates field-scoped violations into one Kubernetes `Invalid`
response. The current rules enforce:

- a typed LMCache `PodLocal` topology (NodeLocal is published but rejected
  until Phase 8), a digest-pinned MP-server image, non-colliding ports, and
  sufficient CPU/memory resources;
- Redis as the only remote provider, with explicit Managed/External ownership,
  a valid External endpoint, provider/config agreement, and only RESP features
  implemented by the pinned adapter;
- Kubernetes resource request/limit, name, quantity, hugepage, extended
  resource, and unsupported-claim constraints;
- the shipping runtime/cache pairs and each adapter's accepted binding;
- EventsOnly and SGLangHiCache shape constraints;
- `ReadWrite` as the only LMCache role;
- valid kernel-check annotations; and
- protection of adapter-reserved engine arguments and environment variables.

`ValidateUpdate` validates the new object. Delete is always allowed so an
operator can remove invalid state.

### Breaking API cleanup

Inference-cache has not been formally deployed, so this version does not ship a resource conversion or compatibility reader. Manifests must use `spec.runtime`, typed `spec.lmCache`, `spec.remoteStorage.<provider>`, `spec.observation`, and provider-owned `resources`. The removed `spec.integration.engine`, `spec.integration.firstEventTimeout`, `spec.backendConfig`, and top-level `spec.resources` fields are not part of the served CRD schema.

### Engine-injection overrides (`spec.integration.engineOverrides`)

`spec.integration.engineOverrides` lets the operator amend non-reserved
engine-container args/env without forking an adapter. Current LMCache server
capacity, chunk size, port, image, and resources belong in typed
`spec.lmCache`; overrides are not a second configuration surface for those
fields. The reserved set makes this surface unsuitable for turning the
integration *off*: operators who need to skip injection entirely on a pod use
the `inferencecache.io/skip-inject` annotation instead.

Shape, in `corev1` vocabulary:

| Field | Type | Behavior |
|---|---|---|
| `args` | `[]string` | Args added to the engine container, scoped to adapter-owned flags. An entry whose leading flag token matches an adapter-owned canonical arg replaces it; an entry whose token is in neither the adapter-owned set nor the user pod-template is appended; an entry colliding with a user-template flag the adapter did not touch is a silent no-op. Order preserved. |
| `suppressArgs` | `[]string` | Leading flag names the adapter MUST NOT inject. Restricted to the adapter-owned set: a suppress entry that names a user-template flag the adapter did not inject is a silent no-op. |
| `env` | `[]corev1.EnvVar` | Env upserted by `Name`, scoped to adapter-owned canonical entries. An override of an adapter-owned name wins; a new name (not on the user template) is appended; a name colliding with a user-owned env the adapter did not touch is a silent no-op. |
| `suppressEnv` | `[]string` | Env var Names the adapter MUST NOT inject. Restricted to adapter-owned entries; user-owned env is protected. |

The "adapter-owned" set is derived by the webhook at admission time by diffing the engine container's args/env immediately before and after `InjectEngineConfig` runs. A flag/env is adapter-owned if the adapter added it OR modified an existing value. User pod-template entries the adapter does not touch are protected from CR-driven mutation — the CR can amend the engine integration, but not silently rewrite the engine pod owner's own template.

No `command` override (the entrypoint stays user-owned). No `resources` override on the engine container here — engine-pod resources are user-owned via the engine's own pod template, not this CR. Managed provider resources are configured under `spec.remoteStorage.<provider>.resources`.

The CRD field default is byte-identical to the prior behavior: a CacheBackend with no `engineOverrides` block renders the same injected patch as before.

#### Reserved declarations and admission hard-reject

Each runtime adapter declares `ReservedArgs()` and `ReservedEnv()`. Admission
rejects any override or suppression that overlaps those lists:

| Adapter | Reserved args | Reserved env |
|---|---|---|
| vLLM typed PodLocal MP | `--kv-transfer-config`, `--disable-hybrid-kv-cache-manager` | `PYTHONHASHSEED`, `INFERENCECACHE_FAIL_OPEN` |
| SGLang typed PodLocal MP | `--enable-lmcache`, `--lmcache-config-file`, `--enable-metrics` | `LMCACHE_USE_EXPERIMENTAL`, `INFERENCECACHE_FAIL_OPEN` |
| SGLangHiCache | its injected HiCache flags | none unless introduced by the adapter |

The removed IP connector environment is neither injected nor part of the
current override contract.

#### Shape rationale (A vs. B)

Two shapes were on the table:

- **A — typed K8s vocabulary** (`[]string` args, `[]corev1.EnvVar` env, plus suppression). Chosen.
- **B — free-form magic keys** (`cpuMode: "true"`, `gpuLimit: "0"`, `extraArgs: "..."`). Rejected.

A is more general: Redis bindings, both LMCache runtime adapters, and further
engine/backend pairs plug in with no per-adapter free-form schema churn. It keeps
the CRD disciplined. B is faster to ship but bakes engine-specific knobs into
the CRD, which is the trap an "engine-agnostic backend" surface is meant to avoid.

#### Residual risk

A user can still set non-reserved values that break the engine in subtle ways the validator can't catch — e.g. `--max-model-len 999999999` OOMing the engine, or env that subtly changes vLLM behavior. Mitigations shipped with this surface:

- Field godoc carries a "known-fragile" callout.
- `ReservedEnv()` mirrors `ReservedArgs()` for the worst offenders, so the canonical wiring can't be silently un-wired.
- Default samples in `config/samples/` exercise the no-override path so a future drift in the adapter's canonical injection breaks them loudly.

### Mutating Pod webhook (engine wiring)

A separate mutating admission webhook on `corev1/v1.Pod` (`name: mpod.inferencecache.io`) auto-wires user-supplied inference engine pods to the matching `CacheBackend` across managed Redis, external Redis, host-only MP, EventsOnly, and SGLang HiCache shapes. Operators do not have to hand-edit the adapter-specific args, env, sidecars, volumes, or mounts onto their pod templates. The handler lives in `internal/webhook/pod` and runs on every Pod CREATE.

| Aspect | Behavior |
|---|---|
| Selection | Lists `CacheBackend`s in the pod's namespace via the manager's **APIReader** (uncached live client; an informer-cache miss on a freshly-Ready backend would leave the pod permanently unwired since pod CREATE is a one-shot), then matches `pod.Labels` against each `Spec.EngineSelector.MatchLabels`. The first matching `CacheBackend` wins; one with a nil or empty `EngineSelector` is skipped (a "match-everything" selector would silently claim every pod in the namespace). |
| Injection | Resolves the runtime adapter via `runtime.Registry.Select(runtimeID, cache)`, resolves `spec.remoteStorage` independently, and constructs a structured provider `Binding{Protocol, Endpoint}`. Managed ownership uses `status.remoteStorage.endpoint` from the live Service; External ownership uses the trimmed, provider-validated `spec.remoteStorage.endpoint` with no fallback to stale status; omitted `remoteStorage` produces a nil host-only binding. `SupportsBinding` is part of the required runtime adapter interface, and the webhook passes the binding directly to `adapter.InjectEngineConfig`, so the adapter selects host-only MP or the RESP wire from the binding protocol instead of inferring storage from `spec.type`. A non-nil binding with a missing endpoint fails open. Events-only skips engine injection because it wires no KV connector and appends only the kvevent-subscriber sidecar. Adapters preserve existing user args/env and make repeat injection idempotent. |
| Annotations | Stamps TWO annotations on every successfully mutated pod: `inferencecache.io/injected-by: <namespace>/<name>` (operator-readable identity, shows in `kubectl describe pod`) AND `inferencecache.io/injected-by-uid: <cache.UID>` (the matched CR's metadata.uid). Successful injection also clears any stale `inferencecache.io/inject-skipped` marker. Reads `inferencecache.io/skip-inject: <truthy>` as an opt-out: the webhook returns Allowed, skips engine wiring, clears any stale injected-by/injected-by-uid pair, and stamps `inferencecache.io/inject-skipped: skip-inject-annotation` so explicit operator opt-out is distinguishable from selector drift. On all other fail-open returns after the pod is decoded (list/no match/missing endpoint/adapter errors), the webhook strips stale injected-by/injected-by-uid and inject-skipped annotations so a user cannot trick the events controller by pre-stamping a pod template. Decode failures fail open before a Pod exists to patch, so stale annotations cannot be cleared on that path. |
| Events | The webhook itself does NOT record events (the apiserver assigns `metadata.uid` after mutating admission, so a webhook-recorded event would carry `involvedObject.uid=""` and be invisible to `kubectl describe pod`). Instead, the pod-watching `engine-pod-events` controller reads the persisted decision annotations after CREATE. For injected pods, it validates `inferencecache.io/injected-by-uid` against the live CR's `metadata.uid` and records a `Normal InjectedByCacheBackend` event on the now-persisted pod. For explicitly skipped pods carrying both a truthy `inferencecache.io/skip-inject` and `inferencecache.io/inject-skipped: skip-inject-annotation`, it records a `Normal SkippedByOperator` event on that pod. The skip marker is not authenticated, and `skipInjection` treats a pre-existing correct marker as already converged; `SkippedByOperator` therefore means the persisted pod carries the explicit opt-out plus skipped marker, not proof that the webhook authored the marker. The UID match REDUCES — but does NOT eliminate — the failurePolicy=Ignore forgery surface for injected pods: a casual copy-paste of an injected pod's annotations into a fresh template won't match the live CR's UID, but `metadata.uid` is not secret, so a pod creator with `get` RBAC on CacheBackends can read it and stamp the pair correctly. The injected Event signals "the webhook claims this pod was injected and the claim is consistent with the live CR," not "the webhook was cryptographically authenticated." The controller skips the injected event when the CR is missing, the UID annotation is absent, or the UID does not match — see the controller godoc for the full skip table. controller-runtime's EventBroadcaster aggregates duplicates on the apiserver side, so a re-enqueue across controller restarts upserts the existing event rather than spamming. |
| Idempotency | The handler calls the adapter unconditionally on every admission and trusts the adapter to converge the full injected contract. For LMCache this is env plus the engine-specific required surface — `--kv-transfer-config` for vLLM; for SGLang `--enable-lmcache` + `--lmcache-config-file` **plus** the MP-worker native sidecar and the shared config / `/dev/shm` volumes + mounts. Its merge primitives (`upsertEnv` / `upsertArgPair` / `upsertFlag`, and for SGLang `adoptContainer` / `adoptVolume` / `upsertMountByName`) converge on the desired value rather than appending a duplicate. The SGLang `adopt*` pair additionally distinguishes the adapter's own prior injection (converge) from an operator's object squatting a reserved name (reject → fail-open admit) — see [Names the MP wire reserves](#sglang-engine-support). Native HiCache validates all reserved arguments against the original pod before mutation, preserves one matching or well-formed operator-supplied value, appends each missing canonical argument once, and rejects conflicts, malformed values, or duplicates without partially changing the pod. Re-admission of a fully-injected pod therefore produces an empty JSON-patch set. Trusting the adapter rather than a handler-side env-presence shortcut avoids the trap where a partially-injected pod is admitted permanently missing the rest of the contract. |
| Fail-open | Every error path (decode failure, list error, no matching backend, missing managed `status.remoteStorage.endpoint`, no registered adapter, adapter rejection, re-encode failure) returns `admission.Allowed(...)` with a reason — webhook errors MUST NOT block engine admission. `MutatingWebhookConfiguration.failurePolicy` is also pinned to `Ignore` as a belt-and-suspenders second layer. |
| Verbs | `CREATE` only. UPDATE re-admissions to a running pod don't re-inject (and the engine container can't pick up env changes without a restart anyway); UPDATEs to engine pods are rare in this fleet. |
