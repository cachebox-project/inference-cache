# Design Roadmap: LMCache Multiprocess Migration

Status: **Phases 0–5, 7, and 8 complete; Phase 6 was not required (2026-08-12)** · Scope:
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
- **NodeLocal** means one on-demand MP server Pod per node that currently hosts
  selected engine Pods. The inference system schedules engines first; the
  cache controller follows that observed placement.
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
| D5 | NodeLocal means one controller-owned server Pod per active engine node per `CacheBackend`; the inference system remains the placement authority. This replaces the earlier DaemonSet/server-first decision on 2026-08-12. | A DaemonSet requires a node set before engines are scheduled and therefore inverted ownership by forcing engines onto cache-selected nodes. Engine-demanded Pods preserve arbitrary inference-system scheduling while still allowing same-node sharing. |
| D6 | A generic Deployment behind a load-balanced Service is not a valid CUDA MP topology. | CUDA IPC requires the engine to reach the MP server on its own node; a load balancer may select a server that cannot open that engine's GPU handle. |
| D7 | Connector endpoints are not published in the generic `status.endpoint`. | PodLocal uses loopback; NodeLocal is node-dependent. Only remote L3 has a globally meaningful provider endpoint. |
| D8 | Unsupported combinations are rejected at admission. | An accepted but inert cache field commonly produces silent zero-hit behavior. |
| D9 | Fail-open is rendered into runtime-native behavior and tested. | A custom environment variable without a known consumer is not an enforceable serving contract. |
| D10 | Component lifecycle ownership is capability-specific. | The Pod-local MP process is kubelet-owned while remote L3 is independently managed; connector re-registration after an MP-process restart is a post-migration enhancement, not an MVP contract. |
| D11 | Each supported vLLM integration explicitly identifies its MP connector implementation; the initial reference baseline uses the LMCache-shipped connector. | With vLLM 0.20 or newer, `LMCacheMPConnector` without a module path selects vLLM's built-in implementation. The initial adapter uses `kv_connector_module_path: lmcache.integration.vllm.lmcache_mp_connector` so the tested client tracks the pinned LMCache server protocol; a future adapter revision may validate a different implementation explicitly. |
| D12 | CacheBackend never owns or rewrites the inference engine image. Engine images in validation matrices are reproducible fixtures only; CacheBackend digest-pins only cache components it injects or manages. | The inference system owns its runtime lifecycle. The selected adapter renders its engine-specific connector contract, while normal engine initialization is the authoritative compatibility check; tested images are neither an admission allowlist nor a mutation default. |
| D13 | Selecting `NodeLocal` explicitly opts the backend into one host-networked MP server per active engine node. Server and selected engine Pods mount only `/dev/shm/inference-cache/<cacheBackendUID>` from the host as container `/dev/shm`; engine Pods themselves remain off host networking and host IPC. | LMCache 0.5.3 CUDA IPC requires node-visible networking and GPU visibility. The UID-scoped mount is retained for observed CUDA/PyTorch auxiliary IPC objects without exposing the entire node SHM namespace to normally behaving pool processes; L1 KV bytes are private pinned host memory. Keeping engine placement and networking under the inference system reduces coupling, while the topology choice and documented trust domain make the remaining host access explicit. |

## Migration baseline (before Phase 1)

This table records the implementation state that motivated the roadmap. It is
historical, not the current production contract; Phase completion and the
current MP-only contract are tracked below.

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
              | LMCache MP server  |   one on-demand Pod per active node
              | shared CPU L2      |
              +---------+----------+
                        |
                        v
                 optional remote L3
```

Properties:

- one MP server per node currently hosting selected engines for a
  `CacheBackend`;
- multiple selected engine Pods on that node share CPU cache capacity;
- engines derive the endpoint from their node identity, not a load-balanced
  Service endpoint;
- L2 capacity is per node;
- engine scheduling remains inference-system-owned; server exact-node binding,
  host port, host shared-memory arrangement, GPU visibility, and node coverage
  become controller-owned concerns;
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
      inferencecache.io/cache-domain: vllm-lmcache
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
        httpPort: 8080
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
        runtimeClassName: nvidia # optional server override; never selects nodes
  remoteStorage:
    provider: Redis
    ownership: External
    endpoint: redis.example:6379
  engineSelector:
    matchLabels:
      inferencecache.io/cache-domain: vllm-lmcache
```

### Final provider matrix

| Engine | MP topology | No remote L3 | Redis/RESP | Mooncake Store | Legacy LMCacheServer |
|---|---|---:|---:|---:|---:|
| SGLang | PodLocal | required MVP | required MVP | future | rejected |
| vLLM | PodLocal | required MVP | required MVP | future | rejected |
| SGLang | NodeLocal | implemented; GPU pending | functional GPU passed; metrics pending | future | rejected |
| vLLM | NodeLocal | implemented; functional GPU passed | functional GPU passed; metrics pending | future | rejected |

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
    enginePodCoverage:
      - name: engine-0
        nodeName: gpu-node-a
        ready: true
        covered: true
        reason: ConnectorReady
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
- NodeLocal `desiredServers` equals the number of distinct `spec.nodeName`
  values among active engine Pods carrying this CacheBackend's valid name+UID
  injection record. Unscheduled engines demand no speculative server yet.
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
| 5 | Repository consumer migration; migration tooling only if needed | Phase 4 | complete |
| 6 | Conditional compatibility gate if legacy consumers appear | Phase 5 | not required by Phase 0 and Phase 5 findings |
| 7 | Remove IP, `lm://`, and LMCacheServer provider | Phase 5; Phase 6 only when applicable | complete |
| 8 | NodeLocal shared MP server topology | Phases 3–4; does not block Phase 7 | complete |

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
| Engine | Private validation image at `sha256:f72dd35b1efd50fd7646ebce708f173a4040fddf3f2363759c67ad732d912d0a` (vLLM 0.25.1) |
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

- **Status:** Complete (2026-08-11)
- **Depends on:** Phase 4

### Objective

Convert every repository-owned LMCache consumer to typed MP without silently
changing cross-Pod sharing or remote-L3 semantics.

### Scope

Includes samples, reference manifests, support tables, CLI/docs, and ordinary
test fixtures. Legacy implementation and intentional compatibility coverage
remain until Phase 7. Physical removal is outside this phase.

The Phase 0 owner audit and Phase 5 repository inventory found zero external
consumers and zero installed legacy objects. This evidence covers this
repository and the recorded owner audit, not every organization source or
private validation cluster. Because no input population appeared, migration
tooling and Phase 6 were not activated.

LMCacheServer and Mooncake are never translated to Redis automatically; the
operator must choose host-only MP, Redis, or a future typed adapter.
`remoteSerde` has no generic replacement and is removed unless a future typed
adapter validates equivalent semantics.

### Deliverables

- [x] Classify repository references as current consumers, Phase 7 legacy
      implementation, history, or intentional compatibility coverage.
- [x] Convert current samples and manifests to typed `PodLocal` MP with explicit
      host-only or Redis semantics.
- [x] Convert current documentation, support tables, CLI guidance, and ordinary
      fixtures to typed MP.
- [x] Remove the unvalidated Helm mapping, legacy CPU-only LMCache sample, and
      Mooncake sample instead of inventing unsafe translations.
- [x] Stop presenting the legacy LMCache server as a default backend or CPU
      profile.
- [x] Retain the legacy implementation only for Phase 7 and clearly label all
      history and compatibility coverage.
- [x] Reconfirm the zero-consumer finding and leave conditional migration
      tooling inactive.

### Validation

Validation completed on 2026-08-11:

| Item | Evidence |
|---|---|
| Repository inventory | No additional repository-owned/generated consumer, migration input, legacy screenshot, or CLI recommendation was found. |
| Production search | No current manifest retained `LMCacheConnectorV1`, `lm://`, `LMCacheServer`, IP wiring, or flat SGLang MP fields. |
| Automated tests | `git diff --check`, `go test ./...`, `make verify-samples` (25 passed, 2 explicit opt-outs), reference YAML parsing, shell checks, and `make ci` passed. |
| Optional check | The Python golden-vector check skipped because `xxhash` was unavailable; `make ci` still passed. |
| Environment | No Kubernetes cluster or GPU was needed or used. The live kind workflow was not run locally in this phase. |

- [x] Every current repository consumer uses typed MP.
- [x] Ambiguous remote-storage examples were removed or require an explicit
      operator choice; none was silently mapped to Redis.
- [x] Every remaining legacy reference is Phase 7 implementation, explicit
      history, or intentional compatibility coverage.
- [x] The zero-consumer assumption was rechecked within its stated evidence
      boundary.

### Exit criteria

- [x] Repository-owned production/current consumers use typed MP only.
- [x] Migration preserves explicit host-only versus shared-L3 intent.
- [x] Conditional tooling remains inactive because the audited input population
      is zero.

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

- **Status:** Complete after QA review and revalidation (2026-08-11)
- **Depends on:** Phase 5; Phase 6 only if activated

### Objective

Delete the legacy IP data plane and every production API/code path that exists
only to support it. LMCache selects typed MP only after this phase.

### Scope

Includes runtime adapters, engine wire, provider lifecycle, API/schema, status,
metrics, Events, tests, samples, and documentation. Clearly marked history and
negative assertions may remain; compatibility implementation may not.

Redis is the only current typed remote L3. Host-only MP creates no provider
workload, and managed Redis is a fixed standalone singleton. Useful managed
provider Pod scheduling/security fields live under
`spec.remoteStorage.workload`; generic replicas, autoscaling, deployment kind,
and legacy top-level template fields are removed.

Mooncake remains future typed MP L2 work. Managed backend clusters, NodeLocal,
directional roles, SGLang TP>1, distributed execution, MLA, and robust MP-server
re-registration are outside this phase. None restores the old IP wire.
Inference-cache does not replace engine images; engine startup remains the
connector/package compatibility verdict.

### Deliverables

- [x] Delete the vLLM IP adapter, SGLang legacy wire helpers,
      `LMCacheConnectorV1`, `LMCACHE_REMOTE_URL`, `LMCACHE_REMOTE_SERDE`,
      `ProtocolLMCache`, and the `lm://` parser/binding.
- [x] Delete managed/external LMCacheServer, the IP-wired Mooncake
      implementation, standalone LMCache-server workloads, restart cascade, and
      IP endpoint lifecycle/status behavior.
- [x] Remove flat LMCache fields, legacy provider schemas/enums, IP-only
      status/metrics/Events, and compatibility defaulting/validation.
- [x] Move managed-provider scheduling/security to
      `spec.remoteStorage.workload`; reject it for External ownership and remove
      generic scaling/deployment fields.
- [x] Remove legacy canaries and compatibility fixtures; retain only explicit
      history and negative assertions.
- [x] Regenerate CRDs and deepcopy code, and update current samples and docs.
- [x] Reconfirm zero consumers within the Phase 5 evidence boundary and skip
      Phase 6/migration tooling.

### Validation

Repository and CPU-only validation completed on 2026-08-11; post-QA GPU
regression ran in SJC dev on 2026-08-11 PDT (2026-08-12 UTC):

| Item | Evidence |
|---|---|
| Repository gates | `git diff --check`, `go test ./...`, `make verify-samples` (25 passed, 1 explicit skip), generated-code checks, production searches, `make ci`, and `make cover-check` (90.1%) passed. |
| Fresh install | A kind smoke verified the MP-only schema, real Pod admission, managed Redis lifecycle/workload propagation, current samples, doctor, server surfaces, and idempotent re-apply. |
| Phase 5 upgrade | A separate kind smoke installed commit `10178558bfca308ee3a4b0d584efe4ed3b91197d`, created typed host-only and managed-Redis objects, upgraded to Phase 7, and preserved identity, topology, reconciliation, and Pod admission. |
| GPU environment | Kubernetes 1.31.1; one A100-SXM4-80GB per engine; LMCache 0.5.3 CUDA 12.9 client wheel; standalone sidecar `sha256:0df30fc70a7d689e1f12823789208a0ee8ef31537316eba6a4c2fa83b0abe61b`; temporary Phase 7 controller `sha256:cd5c4da653bc5a8581e75f9e0668a103f920bc88fd230ce47b1aea6f6f90efc5`. |
| vLLM TP=1 | Engine `sha256:f72dd35b1efd50fd7646ebce708f173a4040fddf3f2363759c67ad732d912d0a` (0.25.1). A 910-token prompt stored 768 tokens; after `/reset_prefix_cache`, the same request retrieved 768 from MP L1. `Ready`, `ConnectorReady`, and `EngineKernelsHealthy` were True. |
| SGLang TP=1 | Engine `sha256:920df39109c60429b0a23eaacfd2786fcf1595c12f3ca4fc6e153b2abe34865f` (0.5.13.post1). A 1,091-token prompt stored 1,024 tokens; after `/flush_cache`, the same request reported 1,024 host-cached tokens and the server logged a 1,024-token retrieve. `Ready` and `ConnectorReady` were True. |
| Cleanup and limits | Test objects were deleted and the SJC control plane restored. The regression covers steady-state PodLocal only, not SGLang TP>1, NodeLocal, server restart/re-registration, or remote L3. |

Both engines used a test-only init-container/shared-volume overlay for the
checksummed LMCache wheel. This is validation scaffolding, not the production
engine-image installation model. The optional Python golden-vector check
skipped because `xxhash` was unavailable; `make ci` still passed.

- [x] Production search finds no current `LMCacheConnectorV1`,
      `LMCACHE_REMOTE_URL`, `ProtocolLMCache`, `lm://`, or managed LMCacheServer
      implementation.
- [x] Fresh-install and real Phase 5 typed-object upgrade smokes pass.
- [x] vLLM and SGLang TP=1 store → engine-cache reset → MP L1 retrieve pass on
      GPU.
- [x] Retained legacy terms are clearly marked history or negative assertions.

### Exit criteria

- [x] `spec.type: LMCache` selects typed MP adapters only.
- [x] No controller workload, engine wire, served schema, or current manifest
      implements the legacy IP path.
- [x] No supported stored object requires the removed schema.
- [x] Repository, upgrade, and required PodLocal GPU regressions pass.

## Phase 8 — NodeLocal shared MP servers

- **Status:** Implementation revised (2026-08-22); focused GPU revalidation of
  the explicit non-lazy/private-pinned-L1 profile is pending.
- **Depends on:** Phases 3–4; does not block Phase 7

### Objective

Allow multiple engine Pods of one `CacheBackend` to share one MP server per
node without weakening placement, isolation, or status correctness.

### Scope

Includes engine-first same-node discovery, on-demand per-node server lifecycle,
shared capacity, multi-node coverage, host-port conflict handling, and status.
PodLocal remains supported. SGLang TP>1, multi-node TP, distributed executors,
MLA, directional PD roles, typed MP Mooncake L2, managed Redis clustering,
generic MP-server restart/re-registration, and cross-`CacheBackend` server
sharing remain out of scope.

The final contract is:

- **API:** `nodeLocal.server` explicitly requires a digest-pinned image,
  distinct MP and FastAPI host ports, per-node L1 capacity, GPU/CPU worker
  limits, and resources covering `l1Capacity + 1Gi`. The FastAPI listener also
  serves `/metrics`; no third metrics port is created. `nodeLocal.scheduling`
  exposes only server operational overrides and cannot select nodes.
  `nodeLocal.idleRetentionSeconds` defaults to 300, accepts 0–86400, and owns
  warm server/L1 retention independently from engine-Pod lifetime. Host-bound
  and security-relevant values have no implicit defaults.
- **Lifecycle and placement:** CacheBackend creation alone creates no MP server.
  The inference system schedules engines first; the controller then owns one
  direct server Pod per distinct active engine node. Required node affinity to
  `engine.spec.nodeName` preserves engine placement while keeping normal
  host-port, taint, resource, and scheduler checks active. After the last
  selected engine leaves a node, typed `idleRetentionSeconds` retains that
  server and L1 for reuse; expiry removes it, while zero requests immediate
  deletion. No Deployment, ReplicaSet, or DaemonSet owns these Pods.
- **Host boundary:** Server Pods use `hostNetwork`, `ClusterFirstWithHostNet`,
  the selected NVIDIA runtime without reserving allocatable GPUs, and a
  restrictive container security context. The controller-created server and
  injected startup gate disable privilege escalation, drop all Linux
  capabilities, and use the runtime-default seccomp profile; the server also
  disables host IPC and service-account token automount. Servers and selected
  engines mount only the backend's `/dev/shm/inference-cache/<uid>` host
  directory as their container `/dev/shm` for CUDA/PyTorch auxiliary IPC
  objects. The injector does not enable or otherwise govern engine-owned
  Pod/container settings such as host networking, host IPC, PID namespace,
  privileged mode, capabilities, or seccomp policy; those remain
  inference-system responsibilities. L1 itself is private pinned host memory.
- **Endpoint and gate:** Engines derive the same-node address from Downward API
  `status.hostIP`. A blocking init gate requires healthy `/healthcheck` plus an
  exact `/config` match for namespace/name/UID/generation, ports, and chunk size
  before the engine starts. It also verifies `supported_transfer_mode` is
  `lmcache_driven`, lazy L1 allocation is disabled, and both declared and
  effective `shm_name` are empty. SGLang writes its
  engine-specific client YAML; vLLM retains its connector JSON. No Service or
  ClusterIP participates in CUDA MP traffic.
- **Ownership and isolation:** One CacheBackend name/UID/runtime and its sole
  namespace-unique `inferencecache.io/cache-domain` value own one server pool.
  CREATE and UPDATE reject non-canonical or duplicate ownership; Pod admission
  denies concurrent ambiguity. Every server explicitly selects
  `lmcache_driven`, non-lazy allocation, and empty `shm_name`, so no named
  POSIX SHM-backed L1 pool exists to collide. The UID host directory is stable
  across same-UID generation/server replacement and distinct after
  CacheBackend delete/recreate. Disjoint port pairs prevent network bind
  conflicts, while the UID-scoped mount separates normally created auxiliary
  IPC files. Idle-retained servers continue reserving their ports, pinned L1,
  and IPC directory until expiry. UID matching and mount scoping are
  routing/ownership identities, not authentication. The supported contract
  assumes that co-located workloads belong to one mutually trusted node
  domain; controlling unrelated or privileged Pods, host root, namespace
  security policy, node separation, and host firewall policy belongs to the
  inference/cluster platform and is not enforced by this controller.
- **Runtime consistency:** A pool cannot mix vLLM and SGLang. CacheBackend
  supplies one server image, chunk size, port tuple, generation, and runtime for
  the pool. The inference-system owner remains responsible for engine
  image/package/model compatibility; normal engine initialization is
  authoritative. Different prompts within one compatibility domain may share
  content-addressed KV entries, while another runtime/model/layout/tenant
  domain requires a separate CacheBackend.
- **Status:** `desiredServers` is the distinct active scheduled-engine node
  count. `readyServers` counts current-generation, name/UID-verified Ready
  servers carrying the exact CUDA/non-lazy/empty-`shm_name` runtime profile and
  UID-scoped host IPC directory on those nodes. An engine is covered only by
  exactly one healthy current server on its own node; unscheduled,
  stale-generation, runtime-profile-mismatched, ambiguous, or serverless
  engines are uncovered. Connector
  readiness requires all desired servers and all matched engines to be Ready
  and covered.
- **Capacity:** `l1Capacity` is one eagerly allocated private pinned-memory
  budget per active node, not per engine Pod. `maxGPUWorkers` must cover the
  maximum engine instances expected on one node.

### Deliverables

- [x] Reconcile one controller-owned server Pod per distinct active scheduled
      engine node, retain it for the typed idle window after final demand, and
      delete it on expiry.
- [x] Preserve inference-system engine placement and bind the server through
      exact-node affinity so Kubernetes still checks ports and resources.
- [x] Configure the required host-network, host-shared-memory, GPU-visibility,
      probe, resource, and restrictive security surfaces.
- [x] Declare both host ports and surface same-node conflicts without accepting
      another backend's listener.
- [x] Require one namespace-unique `inferencecache.io/cache-domain` selector on
      CREATE and UPDATE; deny ambiguous Pod injection and cross-backend sharing.
- [x] Gate engine startup on the healthy same-node server's exact
      name/UID/generation/port/chunk-size identity.
- [x] Explicitly select `lmcache_driven`, non-lazy allocation, and empty
      `shm_name` on every server; verify declared and effective live
      configuration, and replace or un-cover servers missing that profile.
- [x] Mount only `/dev/shm/inference-cache/<cacheBackendUID>` as `/dev/shm` in
      each NodeLocal server and selected engine; replace or un-cover a server
      whose declared hostPath does not match its owner UID.
- [x] Derive the endpoint from Downward API node data and render no MP Service
      or load-balanced ClusterIP.
- [x] Keep vLLM and SGLang launch/configuration surfaces separate while sharing
      the common server renderer.
- [x] Compute desired/ready servers and per-engine same-node coverage.
- [x] Enforce one trust/tenant/runtime/model/layout domain per server pool and
      leave engine package compatibility to normal engine initialization.
- [x] Define L1 as a per-node shared budget and require `maxGPUWorkers` sizing
      for all engines expected on that node.
- [x] Document the host-network, shared-memory, GPU, firewall, isolation, and
      failure-domain boundaries.

### Validation

An initial DaemonSet/server-first prototype was tested and then rejected during
architecture review on 2026-08-12 because it made CacheBackend placement
authoritative over the inference system. None of that prototype's results count
toward Phase 8. The evidence below is for the replacement engine-first,
on-demand server-Pod implementation only.

Local and repository validation completed on 2026-08-12 PDT:

The CPU collision and UID-named rows below are retained as historical evidence
for the former `auto`/engine-driven POSIX-SHM profile. They do not validate the
current CUDA-only profile. The 2026-08-22 implementation now pins
`lmcache_driven`, non-lazy allocation, and empty `shm_name`; repository
validation and focused GPU requalification are tracked separately.

| Check | Result |
|---|---|
| Generated API artifacts | `make generate manifests` passed after the final engine-first/idle-retention API change; deepcopy, served CRD, Pod create/patch/delete RBAC, and webhook manifests are synchronized. No DaemonSet RBAC remains. |
| Unit/envtest | `git diff --check` and `go test ./...` passed. Tests cover zero-server-without-engine, one server per distinct scheduled node, multiple same-node engines, idle marking/reuse/expiry and zero-retention cleanup, generation replacement, foreign-name collision, cross-CacheBackend ownership rejection, exact-node affinity, Pod watch mapping, same-node status coverage, optional scheduling overrides, strict canonical one-label cache-domain validation on CREATE and UPDATE, duplicate-domain rejection, ambiguous-Pod denial, doctor ambiguity reporting, and vLLM/SGLang placement-preserving injection. The real Kubernetes 1.31 admission envtest also passed an API-server CREATE rejection for a duplicate cache domain. |
| Samples | `make verify-samples` passed: 27 admitted, one pre-existing explicit skip, zero failures. Both engine-first NodeLocal samples passed real admission. |
| Coverage | `make cover-check` passed. |
| CI | The baseline was amended as `628194e` with a matching `Signed-off-by`; `make verify-dco` and the complete `make ci` target passed. The optional Python golden-vector regeneration explicitly skipped because `xxhash` is unavailable. |
| Explicit CUDA allocation profile (2026-08-22) | `make generate manifests`, `git diff --check`, gate-script Python syntax parsing, `bash -n` for the reference smoke, `go test ./...`, `make verify-samples` (27 passed, one explicit skip), `make cover-check` (90.7%), and complete `make ci` passed. Tests require `lmcache_driven`, non-lazy allocation, and empty `shm_name` in rendered server args, the live-config gate, status/lifecycle identity, and vLLM connector JSON. Focused GPU revalidation remains pending and is not implied by these repository results. |
| Fresh install | Dedicated Kubernetes 1.32 kind clusters passed the engine-first CRD/controller/webhook installation, real duplicate cache-domain rejection, zero speculative server Pods, placement-preserving NodeLocal engine admission, scheduler-selected engine node followed by one exact-node-affinity direct server Pod, host boundary/host ports, no MP Service, PodLocal admission, current samples, doctor, idempotent re-apply, served idle-retention default/bounds, Pod patch RBAC, idle marking, and same-UID reuse. VPN-safe temporary self-signed webhook TLS replaced only the cert-manager download; both clusters were deleted. The later UID-scoped `--shm-name` delta updated this smoke with exact annotation/argument checks but was not rerun end-to-end because no kind node image/cluster was cached and the standard cert-manager URL remained unavailable through the VPN. That delta instead passed real envtest admission plus the SJC current-controller live tests recorded below. |
| Legacy production search | Production Go/manifests contain no `LMCacheConnectorV1`, `LMCACHE_REMOTE_URL`, `LMCACHE_REMOTE_SERDE`, `ProtocolLMCache`, `lm://`, or LMCacheServer provider path. Remaining LMCacheServer matches are sample comments explicitly describing its removal. |
| Confirmed historical SHM collision root cause | A focused SJC CPU dev test placed two independent LMCache 0.5.3 standalone Pods on node `10.0.103.182`, with disjoint ports and instance IDs but no `--shm-name`. Both ran as PID 1: A created `/dev/shm/lmcache_l1_pool_1` at inode `14092`; B unlinked that name and recreated inode `14097`; A retained a mapping to deleted inode `14092`. This proves the collision in the former CPU/engine-driven profile, not in the current CUDA-only profile. The dedicated namespace was deleted and no control-plane object changed. |
| UID-scoped SHM implementation | `git diff --check`, gate-script Python syntax parsing, `go test ./...`, `make verify-samples` (27 passed, one explicit skip), `make cover-check`, and complete `make ci` passed. Tests cover deterministic full-UID naming, distinct UIDs, unsafe/oversized UID rejection, exact server args/annotation, declared and effective startup-gate checks, status exclusion, automatic replacement of an existing server missing the managed SHM identity, and PodLocal regression. Fresh-install could not run locally because no kind node image/cluster is cached and the standard cert-manager bootstrap requires the known-unavailable GitHub path; real envtest API-server admission did run. |
| UID-directory mount hardening | Local validation passed `git diff --check`, `go test ./...`, `make verify-samples` (27 passed, one explicit skip), `make cover-check` at 90.0%, complete `make ci`, and a fresh Kubernetes 1.32 kind install smoke. Tests cover stable and distinct full-UID host paths, exact `DirectoryOrCreate` mounts in server and engine Pods, rejection of the whole host `/dev/shm` and another backend's directory, status exclusion, automatic stale-server replacement, PodLocal regression, current samples, and idempotent re-apply. The live kind node physically created `/dev/shm/inference-cache/<uid>` as `root:root 0755`; focused SJC GPU evidence for the pinned root engine/server identities is recorded below. The temporary kind cluster was deleted. Arbitrary non-root runtime compatibility remains outside this validation. |
| Focused live SHM remediation | On SJC Kubernetes 1.31.1, two raw LMCache 0.5.3 servers with distinct explicit UID-style names ran together on CPU node `10.0.103.182`: A remained at inode `14166`, while B used inode `14176` and then `14181` after replacement. A's mapping remained named and unchanged throughout. The current controller then created two independent NodeLocal pools on the same node with real CacheBackend UIDs `61a98028-653a-4cfb-83ef-2dc3a9321b50` and `47205c02-6d7d-45aa-bf40-0e1882346309`: their effective names and inodes were respectively `14196` and `14200`; replacing only B moved it to `14207` while A stayed `14196`; deleting and recreating B's engine demand inside idle retention reused B's same server Pod UID and inode `14207`. Both pools reported server/engine coverage `1/1/1/1`. All CPU test resources were deleted. |

SJC engine-first GPU validation ran on 2026-08-12 PDT:

The dedicated `inference-cache-gpu-test` namespace ran Kubernetes 1.31.1 on
two BM.GPU.A100-v2.8 nodes with A100-SXM4-80GB GPUs, NVIDIA driver 550.163.01,
and CUDA 12.9 client artifacts. The server image was
`docker.io/lmcache/standalone@sha256:0df30fc70a7d689e1f12823789208a0ee8ef31537316eba6a4c2fa83b0abe61b`.
The vLLM 0.25.1 engine image was
`sha256:f72dd35b1efd50fd7646ebce708f173a4040fddf3f2363759c67ad732d912d0a`;
the SGLang 0.5.13.post1 engine image was
`sha256:920df39109c60429b0a23eaacfd2786fcf1595c12f3ca4fc6e153b2abe34865f`.
Both engine manifests used the LMCache 0.5.3 CUDA 12.9 wheel with SHA-256
`3587d26a23e942b774589c88ea4cfb019af53474aba84dc772dae5900f9ad2cb`
as test-only runtime-owner scaffolding. It is not a production installation
mechanism and was not supplied by CacheBackend.

| Check | Evidence/result |
|---|---|
| Engine-first lifecycle and gating | Each CacheBackend initially created only its managed Redis Deployment/Service and zero MP servers. After the inference-system-owned engine Pod was scheduled, the controller created the same-node direct server Pod. All gates first observed connection refusal, then admitted the engine only after `/config` and `/healthcheck` matched the exact CacheBackend namespace/name/UID/generation and typed port/worker/chunk configuration. The test Pods had no application readiness probe, so data-plane testing additionally waited for the engine's `Application startup complete`; Kubernetes Pod Ready alone was not treated as service readiness. |
| Effective vLLM configuration | TP=1, Qwen2.5-0.5B-Instruct, MP/HTTP host ports 15555/39080, chunk size 256, shared 8 GiB L1, three GPU/CPU worker slots, and managed Redis L2. Two engines on `10.0.121.10` shared one server and a third engine on `10.0.75.171` used a second server. Status converged to `desiredServers=readyServers=2` and matched/ready/covered engines `3/3/3`. |
| vLLM store/reset/retrieve | Engine A's 750-token request caused its local server to log `Stored 512`. After `/reset_prefix_cache`, fresh engine B's first matching request caused the same `.121` server to log `Retrieved 512`; that server registered two distinct GPU IDs and affinity keys. Engine C's first matching request caused only its `.75` server to retrieve 512 through Redis L2 before local GPU transfer. |
| Effective SGLang configuration | TP=1, TinyLlama-1.1B-Chat-v1.0, MP/HTTP host ports 15556/39081, chunk size 256, shared 8 GiB L1, two GPU/CPU worker slots, and managed Redis L2. Two engines on `.121` shared one server and a third engine on `.75` used a second server. Status converged to servers `2/2` and engines `3/3/3` ready/covered. |
| SGLang store/flush/retrieve | Engine A's 831-token request caused `Stored 768`; after `/flush_cache`, its response reported `cached_tokens=768` and `host=768` while the server logged `Retrieved 768`. Fresh engine B's first request retrieved the same 768-token entry through the shared `.121` server. Engine C's first matching request retrieved 768 through only its local `.75` server. |
| Same-node and no cross-node CUDA IPC | Every engine endpoint came from its own Downward API `status.hostIP`; each direct server used exact node affinity for that engine node, and no MP Service existed. The `.121` servers registered/transferred only for `.121` engines, while the `.75` servers registered/transferred only for `.75` engines. Cross-node reuse occurred through Redis L2, then terminated in the engine's same-node server; no CUDA IPC endpoint crossed nodes. |
| Host-port conflict | A second backend demanded the same 15555/39080 host ports on `.121`. Its exact-node server remained Pending with scheduler `didn't have free ports`; status reported `NodeLocalHostPortConflict`, desired one server, zero ready servers, and the selected engine uncovered/gated. It did not accept the first backend's listener. |
| Redis outage/recovery | SGLang and vLLM were fault-tested separately with two engine-demanded node-local servers. Deleting each managed Redis Pod changed `RemoteStorageReady` to `False/RemoteStorageUnavailable` while `ConnectorReady` and both local servers remained healthy. The replacement Redis restored the condition to True; all engine/server UIDs and restart counts remained unchanged/zero, and post-recovery reset/flush requests retrieved 512 vLLM or 768 SGLang tokens from shared L1. |
| Engine lifecycle | The initial GPU run proved node-scoped deletion/recreation and gating. Architecture review then selected warm idle retention instead of immediate final-engine deletion. The final controller unit/envtest contract marks the server idle, reuses the same Pod when demand returns within the typed window, deletes it after expiry, and supports explicit zero-retention cleanup. The focused metrics GPU run left the server unchanged across normal engine traffic; it did not repeat the full engine-deletion matrix. |
| Canonical selector follow-up | The latest amd64 controller `sha256:9d15ece6a854655d77c558b30005345190fd9cd5966fc4bedb1385b72cb95a70` rejected a non-canonical `app:` selector and separately rejected a second CacheBackend claiming the existing namespace-local `inferencecache.io/cache-domain`. A canonical vLLM engine matched successfully. The backend had zero server Pods before engine scheduling, then created one exact-node server on `10.0.121.10`; status converged to desired/ready servers `1/1` and matched/ready/covered engines `1/1/1`. |
| Representative common-MP metrics | A focused vLLM TP=1 follow-up used the same pinned vLLM and standalone-server images and checksummed LMCache 0.5.3 wheel. Before traffic, `/metrics` reported L1 usage `0`. A 1,521-token request logged `Stored 1280 tokens`, raised `lmcache_mp_l1_write_chunks_total` to `5`, and raised L1 usage to `15,728,640` bytes. After successful `/reset_prefix_cache`, the identical request logged `Retrieved 1280 tokens in 0.002 seconds`; requested/hit counters became `2560/1280`, and vLLM reported a 42.1% external prefix-cache hit rate. vLLM and SGLang data-plane correctness had already passed separately above; the FastAPI `/metrics` endpoint and counters belong to their common standalone MP server, so this focused follow-up did not repeat the full runtime/node/Redis matrix. |
| Post-rebase engine-metrics merge | A focused current-controller test used controller `sha256:a318ea5e96bbcd0ea10f33394beb8fdaa74ce8a93424f6a38f8df75edb4e6889` and subscriber `sha256:ed8a2ad680d248be3e737adf4ba09cd289f6b99909cec580d1cf240d877b3d88`. Live vLLM admission rendered `--hash-scheme=vllm` and the default `http://127.0.0.1:8000/metrics`. A real SGLang 0.5.13.post1 TP=1 Pod instead used its explicitly configured port 8000 and rendered `--hash-scheme=sglang`, `--engine-metrics-url=http://127.0.0.1:8000/metrics`, and `--enable-metrics`. Its endpoint exposed the expected `sglang:token_usage`, `sglang:cache_hit_rate`, `sglang:num_running_reqs`, and `sglang:num_queue_reqs` families; an active request produced `num_running_reqs=1`. Before engine startup the subscriber reported connection failures and `load_signal_stale`; after the endpoint came up it logged `load_signal_recovered`. The authenticated server snapshot then reported `statsReported=true`, `pressure=0.00390625`, one prefix, and a current update timestamp for the SGLang replica. This validates the rebased per-engine profile selection and custom-port plumbing without repeating the already-completed runtime/node/Redis matrix. |
| UID-scoped two-pool GPU isolation | A final vLLM TP=1 run placed two CacheBackends and two engines on A100 node `10.0.75.171`, using disjoint ports `15655/39180` and `15656/39181`, 8 GiB L1, chunk size 256, and one GPU/CPU worker per pool. The tested controller was `sha256:0320ba07bae7bf5158ca1120e96c8e31275bf0b2e879a89cde46a48d0f8edc9b`; the server was the pinned standalone digest above; the vLLM digest was `sha256:f72dd35b1efd50fd7646ebce708f173a4040fddf3f2363759c67ad732d912d0a`; and the checksummed wheel carrier was `sha256:81b6767d1435f41832d3494eee47f93d08998cba99f50e9b019d6a7ba7ea1e33`. Both gate checks verified the exact full-UID name in declared and effective `/config`. A stored 1,536 tokens; B's first request for the identical prompt still missed and independently stored 1,536, proving it did not retrieve A's object. After each engine's `/reset_prefix_cache`, each server independently retrieved 1,536 tokens. Metrics for each pool were write/read chunks `6/6`, lookup requested/hit tokens `3072/1536`, L1 usage `18,874,368` bytes, and zero L2 adapters. The servers registered different GPUs (`GPU-4d9375ae-f17c-3721-2417-3af8a961c530` and `GPU-eba663df-3529-f1ea-0da8-6fbe522719d9`) and neither registered the other's worker. Recreating B changed its engine/worker identity but preserved B's server Pod UID and L1; its first request retrieved 1,536 from retained B L1 while A remained unchanged. LMCache did not retain a visible UID-named file in `/dev/shm` during this GPU-worker run, so named-inode lifetime is established by the focused CPU tests above; the GPU test establishes effective-config, endpoint, worker-registration, and behavioral data isolation. |
| UID-directory GPU isolation | A focused vLLM TP=1 follow-up used controller `sha256:e9c760a3942de447080f9d4373adfef8dd165c4d3ce432311bb9314baecd29bd`, the same pinned standalone/vLLM/wheel artifacts, Kubernetes 1.31.1, A100-SXM4-80GB, driver 550.163.01, and CUDA 12.9 on node `10.0.75.171`. Backend A UID `78346202-561d-4ce4-94dc-081fa7ecbaf5` mounted host `/dev/shm/inference-cache/<A-UID>` as `/dev/shm` in one server and two engines; both engines saw the same tmpfs mount root and inode `262725037`. A1 stored 1,536 of a 1,681-token prompt; fresh A2 retrieved those 1,536 tokens through the same server. A used ports `15755/39280`, 8 GiB L1, chunk size 256, and two GPU workers; status reached servers `1/1` and engines matched/ready/covered `2/2/2`. After A2 was removed, backend B UID `1632dcc4-9c77-40d6-9583-045c4e33be32` started beside A on disjoint ports `15756/39281` and mounted only `/dev/shm/inference-cache/<B-UID>` with distinct inode `262168102`. Both kubelet-created mounts were `root:root 0755`, and the pinned server and engine ran as UID 0 and successfully used them. B's first request for A's identical prompt had zero hits and independently stored 1,536 tokens while every A metric stayed unchanged; after B's `/reset_prefix_cache`, B retrieved its own 1,536 tokens. Final B metrics were write/read chunks `6/6`, requested/hit tokens `3072/1536`, L1 usage `18,874,368` bytes, and zero L2 adapters. Final A/B status was independently `desired=1`, `ready=1`, `matched=1`, `readyEngine=1`, and `covered=1`. The engine Pods did not use host networking or host IPC; each host-networked server declared its exact host-port pair and full UID `--shm-name`. |
| Cleanup/control-plane restore | All test objects were removed. The original validation restored from `/private/tmp/inference-cache-phase8-engine-first-sjc-backup-20260812`; the focused metrics run restored from `/private/tmp/inference-cache-phase8-metrics-backup-20260812`; the UID-scoped run restored from `/private/tmp/inference-cache-phase8-shm-backup-20260812`; the post-rebase metrics run restored from `/private/tmp/inference-cache-phase8-rebase-metrics-backup-20260812`; and the UID-directory run restored from `/private/tmp/inference-cache-phase8-uid-dir-backup-20260812`. The final run also removed only its two exact UID host directories after confirming that deleted engine/server Pods had left CUDA, torch, and semaphore files behind. Semantic comparisons of the CRD spec, ClusterRole rules, ClusterRoleBinding role/subjects, controller Deployment spec, and both webhook lists returned no differences after the final run. The original controller digest `sha256:6dcab2344027ef8ac3db2ab22352cdaa77d80202ec11df49dddeeefe08095b18` returned `1/1` Ready, and the test namespace had no remaining workload or CacheBackend. |

- [x] Zero servers before an engine is scheduled; one healthy same-node server
      after the first vLLM or SGLang TP=1 engine is placed.
- [x] Multiple same-node engines share one server for both runtimes.
- [x] Engines distributed across two nodes use only their respective same-node
      servers; cross-node reuse terminates through Redis L2 without cross-node
      CUDA IPC.
- [x] vLLM store → local reset → retrieve and SGLang store → flush → retrieve.
- [x] Desired/ready server counts and per-engine same-node coverage converge.
- [x] Host-port conflict leaves the second server Pending and its engine gated.
- [x] Redis outage/recovery with multiple node-local servers does not restart
      engines or healthy local servers.
- [x] Engine lifecycle is node-scoped; typed idle retention reuses the server
      before expiry and removes it only after expiry (or immediately at zero).
- [x] Before/after MP `/metrics` snapshots on the representative common server
      data path, in addition to the separate vLLM and SGLang functional runs.
- [x] Historical CPU-profile test: two CacheBackends with disjoint ports retain
      distinct UID-scoped SHM names and inodes on one live node through server
      startup/replacement and idle-retention reuse.
- [x] Two GPU engines independently pass store → engine GPU/local KV clear → L1
      retrieve and worker restart/reconnect; the former exact effective SHM identities,
      disjoint worker registrations, and an identical-prompt cold miss prove
      that neither pool retrieved or registered the other pool's object/worker.
- [ ] Re-run NodeLocal vLLM and SGLang store/reset-or-flush/retrieve with the
      explicit `lmcache_driven` + non-lazy + empty-`shm_name` profile; verify
      startup fails before health when the full pinned allocation is unavailable.

### Exit criteria

- [x] Repository, envtest, samples, coverage, and fresh-install validation pass.
- [x] Every selected engine Pod is covered by exactly one healthy same-node MP
      server under the engine-first lifecycle.
- [x] No load-balanced Service can route an engine to another node's server.
- [x] Shared L1 accounting and Redis failure blast radius are revalidated with
      engine-demanded servers.
- [x] Cross-`CacheBackend` sharing remains rejected by the name+UID demand filter.
- [x] Required vLLM and SGLang functional matrix evidence is supplemented by
      before/after metrics from their common standalone MP-server data path.
- [x] The current implementation no longer creates or treats a UID-scoped
      POSIX SHM L1 name as identity; status and the startup gate instead require
      the explicit CUDA/non-lazy/empty-`shm_name` profile.
- [ ] Focused GPU validation passes for that revised allocation profile.
- [x] UID-directory mounts pass focused SJC GPU validation for one pool, two
      same-node engines, and two co-located CacheBackends; the pinned root
      engine/server identities successfully use the kubelet-created
      `root:root 0755` UID directories.

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

## Overall definition of done

The migration is complete only when all of the following are true:

- [x] `spec.type: LMCache` selects only MP implementations.
- [x] Both SGLang and vLLM pass the required PodLocal GPU matrix.
- [x] Host-only MP is supported for both engines; optional L3 implementations
      are validated and versioned independently from the engine connector gate.
- [x] Current MP server health is observable and steady-state cache behavior is
      tested.
- [x] `remoteStorage` is optional L3 and no longer contains LMCacheServer.
- [x] No production code injects `LMCacheConnectorV1`, `lm://`, or
      `LMCACHE_REMOTE_URL`.
- [x] Remote-L3 lifecycle events do not automatically roll MP engines.
- [x] Every old IP object has been migrated or intentionally deleted.
- [x] Canonical samples, reference manifests, CLI output, and design documents
      describe only the implemented MP behavior.
- [x] NodeLocal, if enabled, guarantees same-node server selection and accurate
      engine coverage; otherwise it remains rejected rather than partially
      accepted.
- [x] The UID-directory NodeLocal data path passes focused GPU validation,
      including LMCache access through the kubelet-created directory and
      isolation between two co-located CacheBackends.

Phase 8 remains the final phase of the migration. Its engine-first topology and
UID-directory evidence are complete; the revised explicit CUDA allocation
profile remains pending focused GPU revalidation. The
future capability profiles below are independent backlog items rather than
additional phases.

## Known limitations and future work

These are independent known limitations and future capability profiles, not
additional migration phases. They do not restore the legacy IP data plane and
must not introduce `LMCacheConnectorV1`, `lm://`, or an LMCacheServer provider.
A profile enters the supported matrix only after its API contract, GPU
correctness, failure-recovery, security, and operability gates pass against
immutable artifacts. The numbered items below are the future-work backlog.

### 1. NodeLocal shared `/dev/shm` and UID-directory reclamation

- [x] Use `lmcache_driven`, non-lazy allocation, and empty `shm_name`, keeping
      `l1Capacity` in private pinned host memory rather than POSIX SHM.
- [x] Retain the exact `/dev/shm/inference-cache/<uid>` hostPath for
      PyTorch/CUDA IPC lifetime objects. It is an IPC correctness boundary, not
      L1 storage or capacity control.
- [x] Limit inference-cache ownership to its rendered Server/helper containers,
      startup gate, and exact LMCache configuration and UID-volume wiring. The
      Engine Pod and unrelated workloads retain their existing policy owners.
- [x] Disable privilege escalation, drop all capabilities, and use
      `RuntimeDefault` seccomp for managed containers. The NodeLocal Server also
      uses `hostIPC: false`, disables service-account token automount, declares
      only its MP/HTTP host ports, and is the only renderer enabling
      `hostNetwork`.
- [x] Install no namespace security exemption or cluster-wide Pod policy.
      Co-located pools remain within one mutually trusted node domain; UID
      directories isolate managed pools but are not hostile-process boundaries.
- [x] Use declared memory requests plus non-lazy startup as the capacity
      contract. Allocation failure keeps the Server unhealthy and the Engine
      gate closed; no aggregate pinned-memory prediction, SHM quota, or separate
      capacity API is planned.
- [x] Reclaim an idle/deleted pool with a gated, one-shot cleanup Pod after no
      Pod uses its exact UID hostPath. Cleanup mounts only that UID, clears its
      contents without removing the directory, and blocks Server recreation
      until completion; a finalizer covers CacheBackend deletion.

### 2. MP client/server compatibility signaling

- [ ] Add client/server compatibility signaling or health detection before
      supporting multiple LMCache version baselines.
- [ ] Define the admitted version/digest relationship and surface actionable
      mismatch status rather than inferring compatibility from process health.
- [ ] Do not generalize support from an arbitrary mismatched-version test pair;
      qualify each selected combination with GPU store/clear/retrieve and
      recovery evidence.

### 3. SGLang TP>1 control-plane convergence

SGLang TP>1 is outside the migration baseline. Inference-cache neither patches
the engine-owned connector nor adds a TP=1 admission guard.

| Finding | Evidence/impact |
|---|---|
| vLLM has one scheduler-side owner for LOOKUP/status/session state. | Per-rank workers only retrieve their GPU shard. |
| SGLang 0.5.3 runs the control flow in every TP rank. | In TP=2, one rank consumed the exactly-once prefetch result; the other got `Prefetch job ... not found`, so the cross-rank minimum became zero. |
| A diagnostic rank-0-owner overlay retrieved 1,280 tokens on both ranks. | It proves a coordination direction, not a safe production patch; collective failure/cancellation remains undesigned. |

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
      TP>1 to the production validation matrix after the tests pass.

### 4. Multi-node TP and distributed executors

- [ ] Define connector and MP-server cardinality for vLLM distributed executors
      and multi-node TP without allowing cross-node CUDA IPC.
- [ ] Specify deterministic endpoint discovery, engine/server placement,
      scheduling ownership, failure domains, and admission behavior.
- [ ] Validate node loss, rank loss, partial registration, replacement, and
      store/clear/retrieve across all supported ranks before adding a profile to
      the matrix.

### 5. MLA and architecture-specific connectors

- [ ] Define an explicit typed capability contract for MLA and other
      model-specific connector behavior.
- [ ] Use authoritative model architecture metadata; never infer support from
      image names, model-name strings, or runtime labels.
- [ ] Add architecture-specific correctness and incompatibility tests before
      admission accepts the profile.

### 6. Directional LMCache roles for PD separation

`ReadOnly` / `WriteOnly` remain generic CacheBackend API concepts, but all
LMCache backends currently admit only `ReadWrite`. This is an intentional safety
restriction, not a claim that producer/consumer roles are unnecessary.

| Finding | Evidence/impact |
|---|---|
| SGLang's LMCache integration has no directional role surface. | `--enable-lmcache` always participates in both store and retrieve. |
| vLLM accepts `kv_consumer`, `kv_producer`, and `kv_both`. | These are connector configuration values, not LMCache server roles. |
| LMCache 0.5.3's vLLM MP connector did not enforce the configured direction in live GPU tests. | `kv_consumer` still stored and `kv_producer` still retrieved, so exposing ReadOnly/WriteOnly would create a false API guarantee. |

- [ ] Define PD producer, consumer, and optional decode write-back semantics,
      including whether generated-token KV may be persisted after a request.
- [ ] Adopt a pinned connector that prevents store in consumer mode and retrieve
      in producer mode rather than relying only on configuration naming.
- [ ] Add GPU negative tests that fail on any prohibited request, plus normal
      prefill-to-decode transfer and multi-turn write-back tests where selected.
- [ ] Lift LMCache admission restrictions only for an adapter/version profile
      that passes those tests; do not infer support from engine CLI acceptance.

### 7. Typed MP Mooncake L2 adapter

Mooncake remains a supported provider direction, but its removed implementation
was coupled to the legacy IP connector and is not safe to restore. Future work
must add a new typed MP binding using LMCache's `mooncake_store` L2 adapter:

- [ ] Add a provider-specific typed configuration for Mooncake metadata/master
      addresses, protocol, segment sizing, local buffer sizing, credentials,
      networking, and managed-versus-external lifecycle.
- [ ] Render `--l2-adapter` configuration through the common MP server without
      exposing `lm://` or `LMCacheConnectorV1`.
- [ ] Define provider-scoped host-network/RDMA placement and security; do not
      reuse an engine-global hostNetwork toggle.
- [ ] Validate cross-Pod sharing, restart/re-registration, failure isolation,
      and both vLLM and SGLang client paths against pinned released artifacts.
- [ ] Never translate a legacy Mooncake object to Redis or infer typed adapter
      settings from its old URL; migration requires an explicit operator choice.

### 8. Managed backend topology and security

The current managed Redis renderer intentionally creates one standalone Redis
Pod. Multiple replicas behind its Service would be independent keyspaces, not a
cluster. A future managed backend-cluster capability must therefore be
provider-specific. Its current security boundary is also limited: password
authentication is optional, no backend NetworkPolicy is controller-owned, and
the pinned LMCache 0.5.3 RESP adapter does not support TLS. The existing managed
Redis profile is therefore suitable only for an explicitly trusted development
or private network, not as a secure multi-tenant production profile.

- [ ] Define Redis topology explicitly (for example standalone versus cluster),
      including shard count, replicas per shard, stable identity, discovery,
      failover, resharding, persistence, and readiness semantics.
- [ ] Decide whether inference-cache owns those resources directly or composes
      with a dedicated Redis operator; keep the core runtime/provider boundary
      inference-system-neutral.
- [ ] Verify that the selected LMCache RESP adapter or proxy endpoint supports
      the advertised cluster behavior before exposing it in the support matrix.
- [ ] Keep generic `remoteStorage.workload` limited to Pod scheduling/security;
      do not add replicas or autoscaling that silently changes provider semantics.
- [ ] Define whether authentication is mandatory for a production profile and
      keep all credentials in namespace-local Secret references.
- [ ] Define NetworkPolicy ownership and ingress/egress selectors for managed
      Redis; do not assume that a ClusterIP is a tenancy boundary.
- [ ] Require a pinned TLS-capable LMCache RESP client, a verified TLS proxy, or
      an equivalently explicit encrypted transport before advertising Redis
      across an untrusted network. Continue rejecting inert TLS configuration.
- [ ] Validate unauthorized access denial, credential rotation, network-policy
      isolation, and selected encrypted-transport failure/recovery behavior.

### 9. LMCache MP server restart and connector re-registration

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
- [ ] For NodeLocal, validate per-node server-Pod replacement and single-node
      server restart separately from the basic same-node topology.

### 10. LMCache fail-open and fail-closed semantics

`spec.integration.failOpen` currently drives status, Events, and the injected
`INFERENCECACHE_FAIL_OPEN` environment variable. The pinned vLLM and SGLang
LMCache MP connectors have not been shown to consume that project-specific
variable or enforce request behavior. The SJC Redis outage tests validated the
default fail-open status path while L1 remained available; they did not validate
an L1+L2 outage or a fail-closed request failure. Consequently, accepting
`failOpen: false` is not yet proof of a data-plane fail-closed guarantee.

- [ ] Decide whether LMCache admission must temporarily accept only
      `failOpen: true` until a connector-native fail-closed surface exists.
- [ ] Map the API to a pinned connector feature that actually controls request
      behavior; do not treat a project-specific environment mirror as
      enforcement.
- [ ] Validate vLLM and SGLang with L1 available/L2 unavailable, L1
      unavailable/L2 available, and complete L1+L2 loss.
- [ ] Prove that fail-open recomputes locally without failing the request and
      that fail-closed fails within a bounded timeout with accurate status and
      Events.
- [ ] Test recovery without engine restart and remove the restriction only for
      runtime/version profiles that pass the same fault matrix.

### 11. MP worker-pool saturation and sizing

Phase 8 proved that multiple TP=1 engine Pods can share one NodeLocal server and
requires `maxGPUWorkers` to cover their count. It did not establish saturation,
queueing, backpressure, or latency behavior at and beyond the configured worker
limits, nor how a future TP profile contributes workers.

- [ ] Define whether each engine instance, process, or TP rank consumes a GPU
      worker and document the corresponding `maxGPUWorkers` formula.
- [ ] Define the purpose and sizing rule for `maxCPUWorkers`, including its
      interaction with GPU workers and L2 operations.
- [ ] Load-test below, at, and above both worker limits and record queueing,
      rejection, timeout, throughput, and tail-latency behavior.
- [ ] Surface an actionable signal when registered demand reaches or exceeds a
      worker limit; silent request hangs are not an acceptable capacity policy.
- [ ] Qualify each future TP/distributed profile independently rather than
      extrapolating from TP=1 engine counts.

### 12. Remote L3 concurrency and connection limits

The current Redis/RESP profile has no typed or validated bound for connections
and concurrent store/retrieve work as the number of engine Pods, NodeLocal
servers, and nodes grows. This is independent of whether Redis remains a
singleton or later becomes a managed cluster.

- [ ] Measure and document Redis connections per MP server and per registered
      engine under idle, store, retrieve, and reconnect behavior.
- [ ] Define client-side connection, concurrency, queue, and timeout limits and
      their relationship to Redis `maxclients` and server resources.
- [ ] Validate connection exhaustion, slow Redis, concurrent store/retrieve,
      reconnect storms, and recovery without engine rollout.
- [ ] Add capacity guidance and actionable status/metrics for connection or
      concurrency exhaustion before advertising a production scale envelope.

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
