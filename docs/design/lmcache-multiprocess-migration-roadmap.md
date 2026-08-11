# Design Roadmap: LMCache Multiprocess Migration

Status: **Phases 0–4 complete (2026-08-11)** · Scope:
deprecate and remove this project's LMCache in-process data plane, converge vLLM
and SGLang on LMCache multiprocess (MP) mode, and model
Pod-local and node-local MP server placement without conflating either with
optional remote storage.

This roadmap is the tracking document for the migration. It supersedes the
long-term migration policy in
[`sglang-lmcache-mp-mode.md`](sglang-lmcache-mp-mode.md), while retaining that
document as implementation history and GPU-validation evidence for the current
SGLang prototype. It also drives the LMCache-related revisions to
[`cachebackend-api.md`](cachebackend-api.md).

## Executive recommendation

Do not delete the in-process (IP) implementation before a production-credible MP
replacement exists. Use a strangler migration:

1. freeze the IP path and lock the target API;
2. introduce an MP-only `CacheBackend` surface;
3. extract and harden the common MP server infrastructure using SGLang;
4. add vLLM MP behind the typed PodLocal `CacheBackend` shape;
5. migrate all repository-owned samples, tests, and manifests;
6. confirm that the Phase 0 no-consumer assumption still holds, then remove the
   IP adapter, `lm://` provider, and legacy lifecycle code; and
7. add node-local shared MP servers after the Pod-local path is stable.

The final API does **not** preserve `InProcess` as a first-class long-term mode.
For `spec.type: LMCache`, the canonical data plane is MP. IP exists only as a
temporary implementation transition path, not an external compatibility
commitment under the Phase 0 finding.

## Assumptions

- `inferencecache.io/v1alpha1` remains pre-launch and eligible for the documented
  alpha removal carve-out. The project owner confirmed on 2026-08-09 that there
  are no external `CacheBackend` consumers and no installed legacy objects that
  require migration. The selected policy is therefore an in-place alpha cleanup.
  If that fact changes before physical removal, the conditional compatibility
  stage in this roadmap becomes mandatory.
- The first complete MP milestone uses **PodLocal** placement. **NodeLocal** is a
  designed topology but does not block removal of IP when cross-Pod sharing is
  supplied by a supported remote L3.
- The initial remote L3 scope is RESP/Redis. MP + Mooncake Store, S3, NIXL, and
  other adapters are separate follow-ups; they must not be implied by accepting
  an inert provider declaration.
- Native sidecars require Kubernetes 1.29 or newer with `SidecarContainers`;
  Kubernetes 1.33 or newer is recommended.
- The inference-workload owner supplies and pins the engine image; CacheBackend
  never rewrites it. CacheBackend pins the cache components it injects or
  manages. The selected adapter renders the connector contract; normal engine
  initialization is the authoritative compatibility check. Mixed MP
  client/server versions are not assumed wire-compatible.

## Terminology and tier model

LMCache upstream calls MP server CPU memory **L1** and secondary storage **L2**.
This project describes the complete serving hierarchy beginning with GPU KV, so
the corresponding project-level names are:

| Project tier | Owner | LMCache upstream name |
|---|---|---|
| GPU KV / L1 | inference engine | engine GPU cache |
| Host-memory / L2 | IP connector or MP server | local CPU / MP L1 |
| Remote / L3 | optional storage adapter | remote backend / MP L2 |

This roadmap uses the project-level names unless quoting an upstream flag or
type. In particular:

- **MP server** is the out-of-process LMCache service reached by an engine
  connector. Existing code and documents sometimes call it an MP worker.
- **PodLocal** means one MP server native sidecar per engine Pod, reached over
  loopback and sharing that Pod's `/dev/shm`.
- **NodeLocal** means one MP server Pod per node, normally a DaemonSet member,
  shared by engine Pods on that node.
- **Remote storage** means only the optional L3 behind the IP connector or MP
  server. An MP server is never declared as `remoteStorage`.
- **Legacy LMCacheServer** means the legacy IP centralized-sharing service
  reached through `remote_url: lm://...`. It is not an MP L3 adapter.
- **MP + Mooncake Store L3** means an MP server configured with
  `--l2-adapter '{"type":"mooncake_store", ...}'`. It is distinct from the
  legacy engine-side `mooncakestore://` connector.

## Locked architectural decisions

The following decisions are the baseline for implementation. Changing one
requires updating this roadmap and recording the replacement decision before
code lands.

| ID | Decision | Rationale |
|---|---|---|
| D1 | The final LMCache data plane is MP-only. | Upstream v0.5.3 recommends MP but still documents IP. Deprecating IP is this project's decision; keeping both indefinitely doubles adapter, lifecycle, and test complexity. |
| D2 | `remoteStorage` is optional L3 only. | Local CPU capacity and MP server placement are engine-integration concerns, not remote-provider selection. |
| D3 | `LMCacheServer` is removed from the canonical `remoteStorage.provider` set. | `lm://` is a legacy IP remote connector and is absent from the MP L3 adapter catalog. |
| D4 | PodLocal is the first production candidate and migration target. | It has the smallest scheduling and ownership surface and builds on the existing SGLang proof. |
| D5 | NodeLocal means a per-`CacheBackend` DaemonSet in its first implementation. | Multiple engine Pods of one backend may share it; cross-`CacheBackend` sharing introduces unresolved config, tenancy, port, and deletion ownership. |
| D6 | A generic Deployment behind a load-balanced Service is not a valid CUDA MP topology. | CUDA IPC and shared memory require the engine to reach the MP server on its own node. |
| D7 | Connector endpoints are not published in the generic `status.endpoint`. | PodLocal uses loopback; NodeLocal is node-dependent. Only remote L3 has a globally meaningful provider endpoint. |
| D8 | Unsupported combinations are rejected at admission. | An accepted but inert cache field commonly produces silent zero-hit behavior. |
| D9 | Fail-open is rendered into runtime-native behavior and tested. | A custom environment variable without a known consumer is not an enforceable serving contract. |
| D10 | Component lifecycle ownership is capability-specific. | The Pod-local MP process is kubelet-owned while remote L3 is independently managed; connector re-registration after an MP-process restart is a post-migration enhancement, not an MVP contract. |
| D11 | Each supported vLLM integration explicitly identifies its MP connector implementation; the initial reference baseline uses the LMCache-shipped connector. | With vLLM 0.20 or newer, `LMCacheMPConnector` without a module path selects vLLM's built-in implementation. The initial adapter uses `kv_connector_module_path: lmcache.integration.vllm.lmcache_mp_connector` so the tested client tracks the pinned LMCache server protocol; a future adapter revision may validate a different implementation explicitly. |
| D12 | CacheBackend never owns or rewrites the inference engine image. Engine images in validation matrices are reproducible fixtures only; CacheBackend digest-pins only cache components it injects or manages. | The inference system owns its runtime lifecycle. The selected adapter renders its engine-specific connector contract, while normal engine initialization is the authoritative compatibility check; tested images are neither an admission allowlist nor a mutation default. |

## Current state

| Area | Current behavior | Gap to target |
|---|---|---|
| SGLang engine wire | Implicit MP; injects a Pod-local native sidecar, config file, loopback endpoint, and shared `/dev/shm`; the sidecar image defaults to the engine image. | Renderer is SGLang-private; cache-component ownership is coupled to the workload image; legacy ZMQ-only server entry point; incomplete worker health and parallelism coverage. |
| vLLM engine wire | `LMCacheConnectorV1` with optional host CPU, `lm://`, or `mooncakestore://`. | No `LMCacheMPConnector`; IP is still the only vLLM LMCache implementation. |
| CR API | MP mode is inferred from runtime. `hostMemory`, `workerImage`, `workerPort`, and `remoteSerde` are flat sibling fields. | No explicit MP topology; mode-specific fields can be accepted and ignored. |
| Remote storage | `Redis`, `LMCacheServer`, and `Mooncake` share one provider abstraction. | `LMCacheServer` is a legacy connector service, not a general MP L3; Mooncake needs a different MP binding shape. |
| Lifecycle | Every managed provider participates in the cache-server restart cascade. | Redis L3 restarts can roll engine fleets even though the engine connects to a local MP server. |
| Status | Provider readiness and engine-container crash loops are observed. | Native-sidecar health and node coverage are not represented; `status.endpoint` is ambiguous. |
| Tests | Strong Go unit coverage; SGLang single-GPU evidence; sample admission checks. | No default-install engine-Pod injection smoke; no vLLM MP; no automated GPU parallelism/Redis matrix. |

### Connector ownership

The engine-side connector and the LMCache MP server are separate components,
and both engines require code from LMCache:

| Runtime | Engine-owned integration surface | LMCache-owned dependency |
|---|---|---|
| vLLM | Generic `KVConnectorFactory` plus built-in `LMCacheConnectorV1` and `LMCacheMPConnector` registrations. An external module path can replace the registered implementation. | The built-in connectors still import the `lmcache` package; LMCache also ships its own vLLM connector implementation and the MP server. |
| SGLang | LMCache-specific `LMCRadixCache`, `--enable-lmcache`, and `--lmcache-config-file` integration. It is not selected through vLLM's generic connector registry. | SGLang imports `LMCacheMPConnector` and related adapters from `lmcache.integration.sglang`; LMCache also supplies the MP server. |

Therefore neither engine image is self-sufficient merely because it exposes an
LMCache flag or connector class. A runtime adapter renders the required
engine-specific wire, then CacheBackend injects and manages a compatible MP
server without replacing the engine image. The engine's normal initialization
is the authoritative check that its image actually contains the required
LMCache client package/API.

Source support is not the same as image support. The upstream vLLM Dockerfile
defaults `INSTALL_KV_CONNECTORS=false`, so its connector source may be present
while the `lmcache` runtime dependency is absent. SGLang likewise documents
installing `lmcache` separately; its integration raises an error when that import
is unavailable. CacheBackend cannot fix a missing Python package by injecting
flags or a server sidecar. The inference owner must therefore choose a
compatible image, but does not declare a second CacheBackend capability switch:
connector import or API incompatibility fails during normal engine startup
before the Pod serves.

## Target architecture

### PodLocal

```text
engine Pod
  +---------------------+       optional network       +------------------+
  | vLLM or SGLang      |                              | remote L3        |
  | MP connector        |                              | Redis initially  |
  |        |            |                              +---------^--------+
  |  loopback + IPC     |                                        |
  |        v            |                                        |
  | LMCache MP server   +----------------------------------------+
  | CPU L2 / MP L1      |
  +---------------------+
```

Properties:

- one MP server per engine Pod;
- engine endpoint is `127.0.0.1:<port>`;
- L2 capacity is per engine Pod;
- engine and server share a Pod network namespace and `/dev/shm`;
- no `hostNetwork` is required;
- cross-Pod sharing requires a configured remote L3;
- lifecycle is Pod-coupled; mid-flight MP-server restart and connector
  re-registration are explicitly deferred to post-migration improvements.

### NodeLocal

```text
GPU node
  +-------------------+  +-------------------+
  | engine Pod A      |  | engine Pod B      |
  | MP connector      |  | MP connector      |
  +---------+---------+  +---------+---------+
            | node-local endpoint   |
            +-----------+-----------+
                        v
              +--------------------+
              | LMCache MP server  |   one DaemonSet Pod per node
              | shared CPU L2      |
              +---------+----------+
                        |
                        v
                 optional remote L3
```

Properties:

- one MP server per eligible node per `CacheBackend`;
- multiple selected engine Pods on that node share CPU cache capacity;
- engines derive the endpoint from their node identity, not a load-balanced
  Service endpoint;
- L2 capacity is per node;
- server scheduling, host port, host shared-memory arrangement, GPU visibility,
  and node coverage become controller-owned concerns;
- cross-`CacheBackend` and cross-tenant sharing are out of scope for the first
  implementation.

## Target API direction

The exact Go type names are finalized in Phase 1. The intended operator shape is:

### PodLocal example

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: vllm-lmcache
spec:
  runtime: VLLM
  type: LMCache
  lmCache:
    topology: PodLocal
    podLocal:
      server:
        image: registry.example/lmcache-vllm@sha256:...
        port: 6555
        l1Capacity: 32Gi
        maxWorkers: 1
        resources:
          requests:
            cpu: "2"
            memory: 33Gi
          limits:
            memory: 33Gi
  remoteStorage:
    provider: Redis
    ownership: External
    endpoint: redis.example:6379
  integration:
    role: ReadWrite
    failOpen: true
  engineSelector:
    matchLabels:
      app.kubernetes.io/name: vllm
```

Omit `remoteStorage` for host-only MP operation.

### NodeLocal example

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: vllm-lmcache-node-local
spec:
  runtime: VLLM
  type: LMCache
  lmCache:
    topology: NodeLocal
    nodeLocal:
      server:
        image: registry.example/lmcache-standalone@sha256:...
        port: 6555
        l1Capacity: 128Gi
        maxGPUWorkers: 8
        maxCPUWorkers: 8
        resources:
          requests:
            cpu: "8"
            memory: 132Gi
          limits:
            memory: 132Gi
      scheduling:
        nodeSelector:
          inferencecache.io/lmcache-mp: "true"
        tolerations: []
  remoteStorage:
    provider: Redis
    ownership: External
    endpoint: redis.example:6379
  engineSelector:
    matchLabels:
      app.kubernetes.io/name: vllm
```

### Final provider matrix

| Engine | MP topology | No remote L3 | Redis/RESP | Mooncake Store | Legacy LMCacheServer |
|---|---|---:|---:|---:|---:|
| SGLang | PodLocal | required MVP | required MVP | future | rejected |
| vLLM | PodLocal | required MVP | required MVP | future | rejected |
| SGLang | NodeLocal | planned | planned | future | rejected |
| vLLM | NodeLocal | planned | planned | future | rejected |

“Required MVP” means the combination must be implemented and validated, not that
the remote L3 field itself is required.

## Status direction

The final field names are part of Phase 1 API review. Status must distinguish
connector health from remote-provider health and must not compress a node-local
endpoint set into one string.

```yaml
status:
  connector:
    mode: Multiprocess
    topology: NodeLocal
    matchedEnginePods: 8
    readyEnginePods: 8
    desiredServers: 4
    readyServers: 4
    coveredEnginePods: 8
    uncoveredEnginePods: 0
  remoteStorage:
    provider: Redis
    endpoint: redis.example:6379
    ready: "True"
  conditions:
    - type: ConnectorReady
      status: "True"
    - type: RemoteStorageReady
      status: "True"
    - type: Ready
      status: "True"
```

Required semantics:

- PodLocal `desiredServers` equals the selected engine Pod count.
- NodeLocal `desiredServers` equals the number of distinct nodes hosting selected
  engine Pods, or the explicitly managed eligible-node count when the DaemonSet
  is intentionally prewarmed.
- `coveredEnginePods` counts selected engine Pods whose required MP server is
  healthy and reachable.
- PodLocal loopback and NodeLocal node-derived connector addresses are not
  published as `status.remoteStorage.endpoint`.
- A configured but unavailable remote L3 produces an explicit degraded/remote
  condition. Whether it changes overall `Ready` depends on the effective,
  runtime-native fail-open policy.

Condition and Event contract:

| Signal | Semantics |
|---|---|
| `ConnectorReady` | `Unknown/ConnectorInjectionUnverified` until the current CacheBackend generation has been injected into every selected Pod; `False` for an unhealthy required MP server or engine Pod; `True` only when selected engines are Ready and covered by healthy MP servers. Runtime package/API incompatibility is surfaced by normal engine startup/readiness, not a capability declaration. |
| `RemoteStorageReady` | Omitted when no L3 is configured; otherwise `Unknown/RemoteStoragePending`, `False/RemoteStorageUnavailable`, or `True/RemoteStorageReady`, independently of connector health. |
| `LegacyInProcessDeprecated` | Conditional compatibility signal only if a legacy consumer appears before physical removal: condition `True` plus a Warning Event with the same reason and a migration instruction. It is never set for typed MP objects. |

Events are emitted on signal transitions or a changed observed generation, not
on every reconcile. The Phase 2 status writer implements the MP health signals;
the legacy deprecation writer is only implemented if Phase 6 is activated.

## Delivery overview

| Phase | Outcome | Depends on | Status |
|---|---|---|---|
| 0 | Design freeze, consumer audit, version/Kubernetes baseline | none | complete |
| 1 | MP-only API and admission/status contracts | Phase 0 | complete |
| 2 | Engine-neutral PodLocal MP server renderer | Phase 1 | complete |
| 3 | Production-credible SGLang PodLocal MP baseline | Phase 2 | complete |
| 4 | vLLM PodLocal MP | Phase 3 | complete |
| 5 | Repository consumer migration; migration tooling only if needed | Phase 4 | not started |
| 6 | Conditional compatibility gate if legacy consumers appear | Phase 5 | not required by Phase 0 finding |
| 7 | Remove IP, `lm://`, and LMCacheServer provider | Phase 5; Phase 6 only when applicable | not started |
| 8 | NodeLocal shared MP server topology | Phases 3–4; does not block Phase 7 | not started |

## Phase 0 — design freeze and compatibility baseline

- **Status:** Complete (2026-08-09)
- **Depends on:** None

### Objective

Freeze the MP-only target and confirm that an in-place `v1alpha1` cleanup is
safe.

### Scope

Includes architecture decisions, repository/consumer inventory, migration
policy, and validation baselines. It does not change the data plane.

### Deliverables

- [x] Approve decisions D1–D12 and freeze new work on `LMCacheConnectorV1`,
      `lm://`, and the legacy managed LMCache server.
- [x] Confirm there are no external `CacheBackend` users and no installed
      legacy objects that must remain readable.
- [x] Select an in-place alpha cleanup; activate a compatibility window only if
      a legacy consumer appears before removal.
- [x] Set Kubernetes 1.29 plus `SidecarContainers` as the minimum; recommend
      1.33 or newer.
- [x] Keep engine images runtime-owned. Validation records exact versions and
      digests, but does not create an image allowlist or universal CUDA tuple.
- [x] Keep top-level managed-provider workload fields legacy-only until Phase 7;
      they never configure PodLocal or NodeLocal MP servers.

### Validation

- [x] Baseline commit `083e916` passed `go test ./...`, `make verify-samples`
      (21 admitted, 2 intentional skips), naming checks, and internal-reference
      checks.
- [x] Repository inventory completed:

| Legacy class | Count | Disposition |
|---|---:|---|
| vLLM IP + `LMCacheServer` | 13 | Migrate repository-owned samples after Phase 4. |
| vLLM IP + engine-side Mooncake | 1 | Migrate explicitly; do not reinterpret as MP Mooncake Store. |
| vLLM IP host-only fixture | 1 | Replace with MP admission coverage. |
| SGLang flat MP fields | 2 | Convert to typed PodLocal in Phase 5. |
| EventsOnly | 1 | No LMCache data-plane migration. |

Initial runtime validation uses LMCache client/server 0.5.3. SGLang requires
TP=1; vLLM requires TP=1 and TP=2 on one node. Redis evidence is supplemental,
not a Phase 3/4 exit gate.

### Exit criteria

- [x] Migration policy and target API approved.
- [x] Consumer audit supports the in-place alpha cleanup.
- [x] Runtime and Kubernetes validation baselines recorded.

## Phase 1 — MP-only API, admission, and status contract

- **Status:** Complete
- **Depends on:** Phase 0

### Objective

Introduce the final typed MP API and reject configurations that would otherwise
be accepted but ignored.

### Scope

Includes CRD shape, defaulting, validation, compatibility rules, and status
types. It does not change the runtime data plane.

### Deliverables

- [x] Add typed `spec.lmCache` with `PodLocal`; design but reject `NodeLocal`
      until Phase 8.
- [x] Allow host-only MP by omitting `remoteStorage`.
- [x] Keep `LMCacheServer` only for topology-less legacy objects until Phase 7.
- [x] Add structured Redis bindings; support Secret-backed authentication and
      reject unsupported TLS/database fields.
- [x] Separate connector status from remote-storage status.
- [x] Make `CacheBackend.spec` the only enablement switch. Engine Pods require
      no connector-profile/version annotations, and admission never pulls or
      executes their images.
- [x] Preserve legacy behavior temporarily, but reject mixing typed topology
      with `hostMemory`, `workerImage`, `workerPort`, or `remoteSerde`.
- [x] Prevent typed MP objects from falling through to a legacy runtime adapter.

Migration mapping:

| Legacy input | Typed disposition |
|---|---|
| `hostMemory.capacity` | `podLocal.server.l1Capacity`; choose resources separately. |
| `workerImage` | `podLocal.server.image`, pinned by digest. |
| `workerPort` | `podLocal.server.port`, after collision validation. |
| `chunkSizeTokens` | Remains at `lmCache.chunkSizeTokens`. |
| `remoteSerde` | No automatic mapping. |
| `remoteStorage.provider: LMCacheServer` | Explicitly choose host-only or a supported L3. |

### Validation

- [x] Validate topology/block consistency, supported providers, positive L1
      budget, memory headroom, ports, parallel arguments, and EventsOnly rules.
- [x] CRD/defaulting, webhook table, deepcopy/round-trip, status serialization,
      and envtest CREATE/UPDATE tests pass.
- [x] Pod-visible incompatible or unclassifiable shapes fail open without a
      partial mutation and produce an actionable diagnostic.

### Exit criteria

- [x] PodLocal admits with or without Redis.
- [x] Unsupported combinations are rejected rather than ignored.
- [x] Existing legacy objects remain reconcilable until removal.
- [x] Every new field has a renderer or status consumer.

## Phase 2 — engine-neutral PodLocal MP server renderer

- **Status:** Complete
- **Depends on:** Phase 1

### Objective

Provide one engine-neutral PodLocal MP server renderer shared by SGLang and
vLLM.

### Scope

Includes sidecar rendering, shared memory, resources, health, metrics, Redis
binding, and lifecycle/status ownership. Engine launch arguments remain in each
runtime adapter.

### Deliverables

- [x] Extract a common renderer for the native sidecar, config volume,
      memory-backed `/dev/shm`, resources, probes, security context, and L3
      arguments.
- [x] Use the supported `lmcache server` entry point and a digest-pinned
      standalone image; never replace the engine image.
- [x] Preserve atomic/idempotent mutation and collision checks.
- [x] Keep SGLang config/flags separate from vLLM connector JSON and hash
      settings.
- [x] Add typed `maxWorkers`, CPU/memory/ephemeral-storage resources, and the
      `l1Capacity + 1Gi` shared-memory budget.
- [x] Expose `/healthcheck` and FastAPI `/metrics` on the server HTTP port.
- [x] Wire Redis username/password through `SecretKeyRef`; continue rejecting
      TLS/database settings unsupported by LMCache 0.5.3 RESP.
- [x] Make Kubelet own the native sidecar and Redis lifecycle independent from
      engine rollout.
- [x] Report server coverage through `ConnectorReady` and remote L3 through
      `RemoteStorageReady`.

### Validation

- [x] Renderer golden, idempotence, collision, resource, security, mount, L3,
      and status-transition tests pass.
- [x] Kubernetes envtest accepts the native-sidecar schema.
- [x] The pinned LMCache 0.5.3 standalone image starts through CPU fallback;
      `/healthcheck` is healthy and `/metrics` returns Prometheus text on port
      8080.

### Exit criteria

- [x] SGLang uses the common renderer without regression.
- [x] Health and metrics endpoints are real and observable.
- [x] Connector, engine, and remote-L3 failures are distinguishable.
- [x] Redis lifecycle does not automatically roll engine Pods.

## Phase 3 — SGLang PodLocal MP production baseline

- **Status:** Complete
- **Depends on:** Phase 2

### Objective

Establish a production-credible SGLang PodLocal baseline on the shared renderer.

### Scope

Includes one TP=1 engine Pod on one node, host-only MP as the required path,
resource pressure, events, metrics, and status. Managed Redis is supplemental.
SGLang TP>1, multi-node execution, model-specific features, external L3, and
sidecar restart recovery are outside this phase.

### Deliverables

- [x] Add typed host-only and optional-Redis samples plus a pinned validation
      fixture with SGLang and LMCache compatibility checks.
- [x] Require an explicit valid `--page-size` compatible with
      `chunkSizeTokens`; reject malformed or ambiguous values atomically.
- [x] Support ReadWrite and reject unsupported SGLang ReadOnly/WriteOnly roles.
- [x] Verify the complete mutation through unit tests, envtest, a live
      Kubernetes 1.32 webhook, default-install smoke, and repository regression
      gates.
- [x] Decode and index real SGLang KV events in a routing domain distinct from
      vLLM; bound stale affinity through removal, TTL, and capacity eviction.

### Validation

Live validation ran on 2026-08-10/11 in SJC dev:

| Item | Evidence |
|---|---|
| Environment | Kubernetes 1.31.1; one A100-SXM4-80GB; driver 550.163.01; `Qwen/Qwen2.5-0.5B-Instruct` |
| Engine | `lmsysorg/sglang@sha256:920df39109c60429b0a23eaacfd2786fcf1595c12f3ca4fc6e153b2abe34865f` (`0.5.13.post1-cu129`) |
| LMCache | Client wheel 0.5.3 CUDA 12.9, test-only runtime-owner overlay; standalone sidecar `sha256:0df30fc70a7d689e1f12823789208a0ee8ef31537316eba6a4c2fa83b0abe61b` |
| Host-only TP=1 | Stored and retrieved 1,280 tokens from MP L1 after flushing the engine GPU cache; real KV events reached `KVEventsObserved`. |
| Chunk/page compatibility | LMCache chunk 256 and SGLang page 64 initialized and served successfully. |
| L1 pressure | A 64 MiB L1 reached 86%, evicted to 68.75%, proved partial-prefix eviction, and had no OOM or restart. |
| Managed Redis | Replacement engine Pod with fresh GPU/L1 retrieved 768 tokens from retained L2 data. Redis loss/recovery changed only `RemoteStorageReady`; the engine did not restart. |
| Metrics/status | Connector and remote-storage conditions transitioned independently; real SGLang metrics and subscriber recovery were observed. |

The runtime-owner overlay is validation scaffolding, not authority for
CacheBackend to modify an engine image. Samples now use fully qualified
`docker.io/...` references because CRI-O rejected short registry names.

- [x] Host-only TP=1 store → GPU flush → MP L1 retrieve.
- [x] Bounded L1 eviction without OOM or container restart.
- [x] Supplemental cross-Pod managed-Redis retrieval and Redis recovery without
      engine rollout.
- [x] Live KV events, routing/index updates, metrics, and independent connector/
      remote-storage status.
- [x] Redis credentials remain `SecretKeyRef` values and are not copied into
      process arguments.

### Exit criteria

- [x] Required SGLang TP=1 host-only GPU correctness tests pass and exact
      validation images are recorded.
- [x] Resource pressure, events, metrics, status, and Redis lifecycle evidence
      pass.
- [x] Samples and design match the implementation.

## Phase 4 — vLLM PodLocal MP

- **Status:** Complete
- **Depends on:** Phase 3

### Objective

Provide the PodLocal MP replacement for the legacy vLLM IP path.

### Scope

Includes one vLLM engine Pod on one node at TP=1 and TP=2. Host-only MP is the
required path; managed Redis is supplemental. Multi-node/distributed execution,
MLA/model-specific behavior, external L3, and sidecar restart recovery are
outside this phase. No particular vLLM/CUDA image tuple is a production gate.

### Deliverables

- [x] Add a dedicated typed vLLM MP adapter using
      `lmcache.integration.vllm.lmcache_mp_connector`; retain the legacy adapter
      only for topology-less objects until removal.
- [x] Keep `CacheBackend.spec` as the only LMCache MP switch; no engine Pod
      capability annotation or image inspection is required.
- [x] Render loopback host/port, `kv_both`, `PYTHONHASHSEED=0`, and
      `--disable-hybrid-kv-cache-manager`; remove IP-only env/config.
- [x] Reuse the common sidecar, shared-memory, resources, probes, status, and
      optional Redis binding.
- [x] Reject PP>1, DP>1, external/multi-process DP, and malformed or duplicate
      parallel arguments; accept single-node TP.
- [x] Add host-only/Redis samples and verify unit, envtest, live Kubernetes 1.32
      admission, default-install smoke, and repository regression gates.
- [x] Repair the subscriber's observed vLLM tagged-map event decoding while
      retaining SGLang/legacy tuple compatibility.
- [x] Enforce `/dev/shm = l1Capacity + 1Gi` and verify both LMCache and vLLM
      native extensions through the existing kernel-check mechanism.

### Validation

Live validation ran on 2026-08-10/11 in SJC dev:

| Item | Evidence |
|---|---|
| Environment | Kubernetes 1.31.1; A100-SXM4-80GB; driver 550.163.01; CUDA 12.9 |
| Engine | `us-sanjose-1.ocir.io/idqj093njucb/vllm-openai@sha256:f72dd35b1efd50fd7646ebce708f173a4040fddf3f2363759c67ad732d912d0a` (vLLM 0.25.1) |
| LMCache | Client wheel 0.5.3 CUDA 12.9, test-only runtime-owner overlay; standalone sidecar `sha256:0df30fc70a7d689e1f12823789208a0ee8ef31537316eba6a4c2fa83b0abe61b` |
| Host-only TP=1 | Stored and retrieved 1,024 external tokens after clearing only vLLM's local prefix cache. |
| Host-only TP=2 | Both ranks registered and retrieved correctly on one node. |
| Events/status | Repaired tagged-map decoding passed live traffic; backend reached `Ready=True/KVEventsObserved`. |
| Shared memory | A 4 GiB L1 used a 5 GiB `/dev/shm`; accelerator serialization stayed enabled without pickle fallback. |
| Native ABI | LMCache `c_ops` and vLLM `_C_stable_libtorch` loaded; legacy `_C` fallback and missing-extension failures have regression tests. |
| Managed Redis | Fresh replacement Pods retrieved retained L2 data at TP=1 and TP=2; this is supplemental evidence. |
| Role test | `kv_consumer` still stored and `kv_producer` still retrieved. LMCache 0.5.3 does not enforce the rendered role. |

The runtime-owner wheel overlay is validation scaffolding, not authority for
CacheBackend to modify engine images. The fixture also lacked an HTTP readiness
probe, so traffic tests waited for the engine health endpoint.

- [x] Host-only TP=1/2 GPU store → local-cache reset → MP L1 retrieve.
- [x] Live KV events, index/status, shared-memory budget, and native-extension
      checks pass after their discovered defects were repaired.
- [x] ReadWrite performs both store and retrieve.
- [x] ReadOnly/WriteOnly negative testing proves LMCache 0.5.3 ignores
      directionality even though the adapter renders the requested role.
- [x] Supplemental cross-Pod Redis retrieval passes at TP=1/2.
- [x] Admission rejects unsupported parallel shapes and injects typed vLLM MP
      without capability annotations.
- [x] LMCache 0.5.3 exposes no supported public heartbeat/MQ tuning surface;
      the API intentionally relies on its internal defaults.
- [x] Reject ReadOnly/WriteOnly for every LMCache backend because the validated
      connector does not enforce them; defer directional roles to an independent
      future connector capability.

### Exit criteria

- [x] Required TP=1 and TP=2 host-only GPU paths pass.
- [x] Role semantics are safe and accurately represented by the API: LMCache
      admits only the validated ReadWrite behavior.

## Phase 5 — migration tooling and consumer migration

- **Status:** Not started
- **Depends on:** Phase 4

### Objective

Convert repository-owned consumers to MP without silently changing cross-Pod
sharing or remote-L3 semantics.

### Scope

Repository migration is required. Inventory/migration tooling is conditional
because Phase 0 found no external users or installed legacy objects.

### Deliverables

- [ ] Convert canonical samples, reference-stack manifests, support tables, CLI
      output, documentation, screenshots, and non-transition fixtures to MP.
- [ ] Remove language that presents the legacy LMCache server as a CPU profile
      or default backend.
- [ ] Reconfirm the zero-external-consumer assumption before removal.
- [ ] If external consumers appear, add inventory/doctor, dry-run conversion,
      unmappable-field reporting, deprecation Events, and rollback guidance.

Migration rules:

| Existing object | Automatic portion | Required operator choice |
|---|---|---|
| SGLang MP with flat worker fields | Move image, port, and host-memory capacity into `lmCache.podLocal.server` and set `lmCache.topology: PodLocal`. | Confirm pinned image/resources and supported Kubernetes version. |
| vLLM IP host-only | Move host-memory capacity to PodLocal MP L1; select vLLM MP wire. | Confirm sidecar resources and accept the process/topology change. |
| vLLM IP + managed/external LMCacheServer | Preserve local capacity; remove `lm://`. | Select no L3 and lose cross-Pod sharing, or explicitly select a supported Redis/other L3. Never choose automatically. |
| vLLM IP + existing engine-side Mooncake provider | Preserve local intent only. | Wait for MP + Mooncake Store L3 support or migrate explicitly to Redis; URL config is not equivalent to MP adapter config. |
| Any IP object with `remoteSerde` | None. | Remove it or map it to a future typed L3 serde only when that adapter supports and validates the same semantics. |

### Validation

- [ ] Repository search finds no repository-owned production LMCache workload
      still using IP, `lm://`, `LMCacheServer`, or flat SGLang MP fields.
- [ ] Migrated samples and reference manifests pass admission/default-install
      smoke.
- [ ] Any newly discovered legacy object has an owner and explicit disposition.

### Exit criteria

- [ ] Every repository-owned LMCache workload uses MP.
- [ ] No migration silently changes cross-Pod sharing behavior.
- [ ] Conditional tooling, if activated, reports zero unknown legacy shapes.

## Phase 6 — reject new IP objects

- **Status:** Conditional; currently not required
- **Depends on:** Phase 5

### Objective

If a legacy consumer appears, stop the legacy population from growing while it
is migrated or deleted.

### Scope

Skip directly to Phase 7 if the Phase 0 zero-consumer finding still holds.
Otherwise this phase adds a temporary compatibility gate, not new IP features.

### Deliverables

- [ ] Reject new vLLM LMCache objects without typed MP.
- [ ] Reject new `LMCacheServer` providers and reintroduced legacy fields.
- [ ] Permit existing IP objects only for read/status, deletion, and migration;
      reject scale-out or lifetime-extending updates.
- [ ] Report remaining legacy count and emit migration-linked warnings.
- [ ] Publish the physical-removal target and observation window.

### Validation

- [ ] CREATE/UPDATE admission tests cover rejection and grandfather rules.
- [ ] Observation window records zero newly created IP objects.
- [ ] Every exception has an owner and expiry.

### Exit criteria

- [ ] No supported API path can create a new IP data plane.
- [ ] Legacy count is zero or every exception is time-bounded.
- [ ] MP production health meets the agreed baseline.

## Phase 7 — remove IP and the legacy LMCache server

- **Status:** Not started
- **Depends on:** Phase 5; Phase 6 only if activated

### Objective

Delete the IP data plane and all code/schema that exists only to support it.

### Scope

Includes runtime adapters, provider protocols, controller workloads, status,
samples, tests, and legacy API fields. Historical migration documentation may
remain when clearly marked.

### Deliverables

- [ ] Remove the vLLM legacy LMCache adapter.
- [ ] Remove `LMCacheConnectorV1` rendering.
- [ ] Remove `LMCACHE_REMOTE_URL`, `LMCACHE_REMOTE_SERDE`, and other IP-only
      injected settings.
- [ ] Remove `ProtocolLMCache` and the `lm://` endpoint parser/binding.
- [ ] Remove the managed and external `LMCacheServer` provider surface.
- [ ] Remove the standalone LMCache-server workload renderer.
- [ ] Remove IP-only status fields, metrics, Events, samples, and tests.
- [ ] Remove compatibility defaulting/validation and migration-only code after
      any supported migration window closes.
- [ ] Remove legacy flat LMCache fields after their replacement is complete.
- [ ] Remove `LMCacheServer` from CRD enums and provider-specific schema.
- [ ] Remove or relocate top-level managed-provider workload fields according to
      the Phase 0 decision.
- [ ] Regenerate CRDs, deepcopy code, examples, and reference documentation.

### Validation

- [ ] `go test ./...` passes.
- [ ] `make verify-samples` passes.
- [ ] Default-install and upgrade smoke pass.
- [ ] Repository search finds no production-code references to:
  - `LMCacheConnectorV1`;
  - `LMCACHE_REMOTE_URL`;
  - `ProtocolLMCache`;
  - `lm://`;
  - the managed `LMCacheServer` provider.
- [ ] Any retained historical reference is clearly marked as removed behavior.

### Exit criteria

- [ ] Only MP adapters can be selected for `spec.type: LMCache`.
- [ ] No controller workload or engine wire implements IP.
- [ ] No supported stored object requires the legacy schema.

## Phase 8 — NodeLocal shared MP servers

- **Status:** Not started
- **Depends on:** Phases 3–4; does not block Phase 7

### Objective

Allow multiple engine Pods of one `CacheBackend` to share one MP server per
node without weakening placement, isolation, or status correctness.

### Scope

Includes same-node discovery, DaemonSet lifecycle, shared capacity, and
multi-node coverage. Cross-`CacheBackend` sharing remains out of scope.

### Deliverables

- [ ] Reconcile one DaemonSet per NodeLocal `CacheBackend`.
- [ ] Restrict it to intended GPU/engine nodes through typed scheduling fields.
- [ ] Configure host networking and host shared memory according to the pinned
      upstream deployment contract.
- [ ] Declare host ports so Kubernetes scheduling exposes conflicts.
- [ ] Authenticate ownership by CacheBackend name and UID.
- [ ] Compute desired/ready servers and engine-node coverage.
- [ ] Handle engine scheduling before the node-local server is ready without
      starting an engine against a missing required MP endpoint.
- [ ] Derive the node-local address from the engine Pod's node/host IP through a
      Downward API field or another deterministic node-scoped mechanism.
- [ ] Do not use a load-balanced ClusterIP as the CUDA MP endpoint.
- [ ] Keep SGLang and vLLM launch surfaces engine-specific.
- [ ] Validate the server's global chunk size and version against every selected
      engine Pod.
- [ ] Define port-conflict behavior for multiple NodeLocal CacheBackends on one
      node.
- [ ] Restrict the first implementation to one trust/tenant domain per
      CacheBackend server pool.
- [ ] Document that L1 capacity is per node and shared by selected engine Pods.
- [ ] Size `maxGPUWorkers` for the number of engine instances sharing a server.
- [ ] Add NetworkPolicy/firewall guidance where host networking permits it.
- [ ] Assess the security impact of host networking/shared memory and GPU
      visibility.

### Validation

- [ ] One engine Pod on one node.
- [ ] Multiple engine Pods sharing one node-local server.
- [ ] Engines spread across multiple nodes, each using only its local server.
- [ ] Node drain and engine rescheduling.
- [ ] Host-port conflict negative test.
- [ ] Redis outage/recovery with multiple node-local servers.
- [ ] No cross-node attempt to use CUDA IPC.

### Exit criteria

- [ ] Every selected engine Pod is covered by exactly one healthy same-node MP
      server.
- [ ] No load-balanced Service can route an engine to another node's server.
- [ ] Shared L1 accounting and failure blast radius are measured.
- [ ] Cross-`CacheBackend` sharing remains rejected.

## Post-migration improvements and additional features

These items are separate capability profiles. They are not Phase 3 or Phase 4
exit criteria and do not block migration away from the legacy IP data plane:

- [ ] Design and validate multi-node TP and vLLM distributed-executor profiles,
      including connector/server cardinality, endpoint discovery, failure
      domains, scheduling, and an explicit admission contract.
- [ ] Design and validate MLA and other model-specific connector profiles using
      model architecture metadata rather than image or model-name heuristics.
- [ ] Add client/server compatibility signaling or health detection before
      supporting multiple LMCache version baselines; do not generalize from an
      arbitrary mismatched-version test pair.
- [ ] Add each profile to the supported validation matrix only after its own
      GPU correctness, failure-recovery, and operability gates pass.

### Directional LMCache roles for PD separation

`ReadOnly` / `WriteOnly` remain generic CacheBackend API concepts, but all
LMCache backends currently admit only `ReadWrite`. This is an intentional safety
restriction, not a claim that producer/consumer roles are unnecessary.

| Finding | Evidence/impact |
|---|---|
| SGLang's LMCache integration has no directional role surface. | `--enable-lmcache` always participates in both store and retrieve. |
| vLLM accepts `kv_consumer`, `kv_producer`, and `kv_both`. | These are connector configuration values, not LMCache server roles. |
| LMCache 0.5.3's vLLM MP connector did not enforce the configured direction in live GPU tests. | `kv_consumer` still stored and `kv_producer` still retrieved, so exposing ReadOnly/WriteOnly would create a false API guarantee. |

Future work must treat directional access as a separately validated connector
capability:

- [ ] Define PD producer, consumer, and optional decode write-back semantics,
      including whether generated-token KV may be persisted after a request.
- [ ] Adopt a pinned connector that prevents store in consumer mode and retrieve
      in producer mode rather than relying only on configuration naming.
- [ ] Add GPU negative tests that fail on any prohibited request, plus normal
      prefill-to-decode transfer and multi-turn write-back tests where selected.
- [ ] Lift LMCache admission restrictions only for an adapter/version profile
      that passes those tests; do not infer support from engine CLI acceptance.

### LMCache connector control-plane convergence and SGLang TP>1

SGLang TP>1 is outside the migration baseline. Inference-cache neither patches
the engine-owned connector nor adds a TP=1 admission guard.

| Finding | Evidence/impact |
|---|---|
| vLLM has one scheduler-side owner for LOOKUP/status/session state. | Per-rank workers only retrieve their GPU shard. |
| SGLang 0.5.3 runs the control flow in every TP rank. | In TP=2, one rank consumed the exactly-once prefetch result; the other got `Prefetch job ... not found`, so the cross-rank minimum became zero. |
| A diagnostic rank-0-owner overlay retrieved 1,280 tokens on both ranks. | It proves a coordination direction, not a safe production patch; collective failure/cancellation remains undesigned. |

Future work must answer the architectural question before selecting a fix:

- [ ] Determine why the vLLM and SGLang integrations deliberately use different
      scheduler/worker ownership models and whether SGLang exposes a stable
      scheduler-to-worker metadata path suitable for LMCache.
- [ ] Define one owner for LOOKUP, prefetch status, sessions, and global lock
      cleanup while preserving per-rank registration and GPU RETRIEVE.
- [ ] Define bounded cross-rank error and cancellation propagation so a failed
      owner cannot hang peers in a collective.
- [ ] Add upstream TP=2 tests covering miss/store, GPU flush, host-only hit,
      Redis-backed hit, partial hit, timeout, cancellation, and lock/session
      cleanup.
- [ ] Adopt only an immutable released connector artifact, then add SGLang
      TP>1 back to the production validation matrix after the tests pass.

### LMCache MP server restart and connector re-registration

The migration guarantees steady-state MP operation and sidecar process-health
observation. It does not guarantee that a running engine continues caching after
its MP server restarts, and it does not patch LMCache 0.5.3 to add that behavior.

| Finding | Evidence/impact |
|---|---|
| Kubernetes can restart the native sidecar independently. | `ConnectorReady` follows process health, but process recovery does not prove registration recovery. |
| SGLang TP=1 did not recover registration. | The new server had no GPU context and the next request hung; LMCache 0.5.3's SGLang adapter does not register the vLLM adapter's recovery callback. |
| vLLM recovered after a 70-second outage. | Re-registration and post-recovery store/retrieve passed. |
| vLLM failed after a fast 10–15-second restart. | Heartbeat missed the outage, so the new server lost registration without triggering recovery. |

If this capability is selected later, its independent scope is:

- [ ] Decide whether the supported policy is sidecar-only recovery or complete
      engine-Pod recreation; document the availability and latency trade-off.
- [ ] If sidecar-only recovery is selected, implement or adopt a pinned LMCache
      client/server version that re-registers SGLang and vLLM GPU contexts and
      detects server replacement even when no heartbeat lands in the outage
      window.
- [ ] Invalidate pre-restart lookup/session state and prove bounded recompute
      rather than a hung or partially restored request.
- [ ] Make connector status registration-aware so an empty recovered server is
      not reported as `ConnectorReady=True`.
- [ ] Validate crash, hang, fast restart, repeated restart, callback failure,
      TP=1/TP=2, and post-recovery store/flush/retrieve for each selected engine
      profile.
- [ ] For NodeLocal, validate DaemonSet rollout and single-node server restart
      separately from the basic same-node topology.

## Required GPU validation matrix

The matrix grows by phase. A cell is complete only when it proves a cache hit
after clearing or replacing the engine GPU cache; successful process startup is
not sufficient.

| Runtime | Topology | Remote L3 | Parallelism | Required by |
|---|---|---|---|---|
| SGLang | PodLocal | none | TP=1 | Phase 3 |
| vLLM | PodLocal | none | TP=1 | Phase 4 |
| vLLM | PodLocal | none | TP=2 | Phase 4 |
| SGLang | NodeLocal | Redis | multiple engine Pods | Phase 8 |
| vLLM | NodeLocal | Redis | multiple engine Pods | Phase 8 |

Every required data test records:

- exact image digests and LMCache version;
- Kubernetes, driver, CUDA, and GPU model;
- engine args and effective MP server config;
- first-request store evidence;
- GPU-cache clear or fresh-engine proof;
- second-request retrieve/hit evidence;
- MP metrics before and after, plus L3 metrics only when an optional L3 binding
  is part of that particular test.

## Test pyramid

| Layer | Required evidence |
|---|---|
| API/unit | schema, defaulting, validation, provider matrix, deep copy, status transitions |
| Renderer/unit | exact args/env/config, resources, probes, security, volumes, idempotence, collision rejection |
| Envtest | real CREATE/UPDATE admission and status persistence; legacy grandfathering only if Phase 6 activates |
| Kubernetes smoke | live webhook injection into matching engine Pods, native-sidecar schema support, controller-owned workload shape |
| GPU functional | store/flush/retrieve and cross-Pod L3 reuse |
| GPU fault | Redis loss/recovery, engine rollout, node drain for NodeLocal |
| Upgrade/migration | Repository manifest conversion by default; old-object inventory, dry-run conversion, grandfather rules, and rollback only if Phase 6 activates |

## Security, reliability, scalability, and cost gates

### Security

- [ ] No production managed Redis profile is exposed without an explicit network
      isolation and credential/TLS posture.
- [x] Secrets are referenced, not embedded in CR status, Pod args visible to all
      readers, logs, or Events.
- [ ] PodLocal and NodeLocal GPU visibility is documented and reviewed for the
      target tenancy model.
- [ ] NodeLocal host networking/IPC is an explicit operator choice.
- [ ] Cross-namespace remote endpoints retain explicit opt-in validation.

### Reliability

- [x] MP server health affects connector status.
- [ ] Remote L3 loss follows tested fail-open/fail-closed behavior.

### Scalability and latency

- [x] PodLocal memory cost is reported per engine Pod.
- [ ] NodeLocal memory cost is reported per node.
- [ ] Worker pool sizing is tested under the expected engine count and TP shape.
- [ ] Remote L3 concurrency and connection limits are bounded.
- [x] Routing/index signals can be correlated with actual LMCache hit metrics.

### Operability

- [x] Status distinguishes connector, MP server, engine, and remote L3 health.
- [ ] Metrics expose server availability, L1/L3 store/retrieve/hit, capacity,
      and eviction.
- [ ] Events contain an actionable remediation or migration instruction.
- [ ] Samples never depend on an implicit runtime-selected connector mode.

## Risk register

| Risk | Impact | Mitigation / gate |
|---|---|---|
| A legacy consumer appears after Phase 0 | Breaking removal | Reconfirm before removal; activate Phase 6 and a grandfather period when non-zero. |
| MP client/server version skew | Permanent unhealthy or protocol failure | Pin a validated client/server baseline and record exact artifacts; automatic version negotiation/detection is a future improvement. |
| Redis restart rolls all engines | Availability blast radius | Lifecycle-specific restart policy in Phase 2. |
| `failOpen` is only a custom env | Contract not enforced | Render native runtime policy and fault-test it. |
| Sidecar sees all node GPUs | Isolation exposure | Document/review tenant model; prefer dedicated nodes where required. |
| PodLocal duplicates CPU L2 | Memory cost per replica | Explicit per-Pod capacity; NodeLocal follow-up. |
| NodeLocal port collision | DaemonSet Pods fail or bind incorrectly | Declared host port, typed port, controller condition, negative tests. |
| NodeLocal engine reaches remote node | CUDA IPC failure | Node-derived endpoint; reject load-balanced service topology. |
| Existing engine-side Mooncake config is treated as MP-equivalent | Admission succeeds but adapter cannot start | No automatic migration; separate MP + Mooncake Store implementation. |
| Index says warm while MP/L3 evicted data | Routing quality degrades silently | Correlate cache events with LMCache metrics/health; define stale-entry behavior. |

## Roadmap maintenance

Each implementation PR updates the delivery table and the affected phase's
checkboxes. A phase becomes complete only when every exit criterion is checked;
validation details stay summarized in the phase evidence table rather than in
dated closure sections or separate phase documents.

## Overall definition of done

The migration is complete only when all of the following are true:

- [ ] `spec.type: LMCache` selects only MP implementations.
- [x] Both SGLang and vLLM pass the required PodLocal GPU matrix.
- [x] Host-only MP is supported for both engines; optional L3 implementations
      are validated and versioned independently from the engine connector gate.
- [x] Current MP server health is observable and steady-state cache behavior is
      tested.
- [ ] `remoteStorage` is optional L3 and no longer contains LMCacheServer.
- [ ] No production code injects `LMCacheConnectorV1`, `lm://`, or
      `LMCACHE_REMOTE_URL`.
- [x] Remote-L3 lifecycle events do not automatically roll MP engines.
- [ ] Every old IP object has been migrated or intentionally deleted.
- [ ] Canonical samples, reference manifests, CLI output, and design documents
      describe only the implemented MP behavior.
- [x] NodeLocal, if enabled, guarantees same-node server selection and accurate
      engine coverage; otherwise it remains rejected rather than partially
      accepted.

## Upstream references

- [LMCache v0.5.3 release](https://github.com/LMCache/LMCache/releases/tag/v0.5.3)
- [LMCache MP overview](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/index.rst)
- [LMCache MP deployment guide](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/deployment.rst)
- [LMCache MP configuration](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/configuration.rst)
- [LMCache MP supported storage adapters](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/l2_storage/supported_storages.rst)
- [LMCache MP Mooncake Store adapter](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/mp/l2_storage/mooncake_store.rst)
- [Legacy IP centralized sharing](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/source/getting_started/quickstart/share_kv_cache.rst)
- [Official vLLM MP Kubernetes example](https://github.com/LMCache/LMCache/blob/v0.5.3/examples/multi_process/vllm-deployment.yaml)
- [MP worker liveness design](https://github.com/LMCache/LMCache/blob/v0.5.3/docs/design/v1/multiprocess/worker_liveness.md)
- [LMCache standalone image](https://github.com/LMCache/LMCache/blob/v0.5.3/docker/README.md#2-dockerfilestandalone---lmcache-only)
- [LMCache v0.5.3 vLLM MP connector](https://github.com/LMCache/LMCache/blob/v0.5.3/lmcache/integration/vllm/lmcache_mp_connector.py)
- [LMCache v0.5.3 SGLang MP adapter](https://github.com/LMCache/LMCache/blob/v0.5.3/lmcache/integration/sglang/multi_process_adapter.py)
- [vLLM v0.26.0 connector registry](https://github.com/vllm-project/vllm/blob/v0.26.0/vllm/distributed/kv_transfer/kv_connector/factory.py)
- [vLLM v0.26.0 built-in LMCache MP connector](https://github.com/vllm-project/vllm/blob/v0.26.0/vllm/distributed/kv_transfer/kv_connector/v1/lmcache_mp_connector.py)
- [vLLM v0.26.0 Docker connector-dependency switch](https://github.com/vllm-project/vllm/blob/v0.26.0/docker/Dockerfile)
- [SGLang v0.5.13.post1 LMCache integration](https://github.com/sgl-project/sglang/blob/v0.5.13.post1/python/sglang/srt/mem_cache/storage/lmcache/README.md)
- [SGLang v0.5.13.post1 LMCache cache implementation](https://github.com/sgl-project/sglang/blob/v0.5.13.post1/python/sglang/srt/mem_cache/storage/lmcache/lmc_radix_cache.py)
