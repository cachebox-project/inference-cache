# Design: LMCache server-side disk persistence — entry-point decision

Status: **Accepted** · Decision: **Retire `spec.storage.pvc`; express durability via backend choice (remote store / Mooncake)** · Pairs with: `docs/design/cachebackend-api.md`

> This is a decision record produced by a research pass. It captures *which*
> LMCache server entry point (if any) can make the cache-server we provision
> persist KV to a mounted PersistentVolumeClaim, the evidence behind each option,
> and the recommendation. The implementation follow-up that would wire the chosen
> mechanism is re-scoped off this doc.

> **Accuracy note (after primary-source verification).** An earlier draft framed
> local-PVC persistence as *architecturally impossible* on our topology — that
> over-claims. LMCache's L2 `fs` adapter is **topology-neutral** (works on any
> POSIX filesystem; a PVC mount satisfies it), so persistence itself is
> achievable. The real constraint is the **transfer mechanism**: MP mode's
> preferred path is CUDA IPC (needs `hostIPC` + same-node → DaemonSet), and
> whether its `--supported-transfer-mode non_gpu` path works **end-to-end over a
> plain Service without `hostIPC`** is **unverified** — that single fact is what
> would make a Deployment+Service MP-mode server with an `fs` adapter on a PVC
> viable. This decision **retires `spec.storage.pvc` as a deliberate product
> choice — not because it is impossible**, but because the value proposition
> (survive-restart on a single RWO replica, under a fail-open posture where a
> cold cache is a few misses, not an outage) does not justify resolving that open
> question, and durable/shared cache is better delivered by the planned Mooncake
> backend. Where the options below say "rejected/incompatible" for MP mode, read
> "**not pursued, pending an unverified transfer-mode fact**."

## Context

The persistent-storage wire-up shipped the **Kubernetes-side plumbing** for
`CacheBackend.spec.storage.pvc`:

- the runtime adapter declares a data-volume mount path,
- the reconciler provisions a `PersistentVolumeClaim` (owner-referenced to the
  `CacheBackend`, `ReadWriteOnce`, adopt-and-keep on spec removal),
- the PVC is mounted into the cache-server pod,
- `status.capacity` reports the bound size,
- multi-replica + RWO is gated `Ready=False/InvalidStorageConfiguration`,
- RBAC + watch are in place.

What it did **not** do: make the LMCache server actually **write KV to the
mounted volume**. The server we render still runs its in-memory device, so the
PVC is provisioned and attached but not written to — KV does **not** survive a
pod restart. The original plan was to "flip the server's storage device from
`cpu` to `disk` via a CLI arg." This doc resolves whether that is possible.

**The question:** how do we make the LMCache server *we provision* persist KV to
a mounted PVC, given our deployment model — **one managed cache-server
Deployment per `CacheBackend`, fronted by a ClusterIP Service, that vLLM engine
pods anywhere in the cluster connect to over the network** (today via
`LMCACHE_REMOTE_URL=lm://<service>:65432`)?

## What we provision today

`pkg/adapters/runtime/vllm_lmcache.go` renders a standalone server:

```
lmcache_server 0.0.0.0 65432 cpu        # command + args
```

behind a ClusterIP Service on `:65432`. The mutating Pod webhook injects
`LMCACHE_REMOTE_URL=lm://<service-dns>:65432` + the `--kv-transfer-config`
connector into matching engine pods. This is the `lm://` **RemoteBackend** wire,
and it is **network-addressable**: engine and cache-server are independent pods
that may land on different nodes.

## LMCache's storage mechanisms (what's actually available)

LMCache exposes three distinct storage paths. Only one persists to local disk on
a *central* server, and it does not fit our topology.

| Mechanism | Where the bytes live | Engine-side wire | Network-addressable? | On-server local disk (PVC)? |
|---|---|---|---|---|
| **`lm://` RemoteBackend** (the `lmcache_server` we run) | **CPU RAM, in-memory** | `LMCACHE_REMOTE_URL=lm://host:port` | **Yes** (ClusterIP today) | **No** — the `lm://` server is an in-memory KV store; "remote disk" means a *network* store (Redis / Mooncake / InfiniStore), not a local volume |
| **Client-side LocalDisk** (`local_disk: file://…`, `max_local_disk_size`) | local disk on the **engine pod** | engine config only; no separate server | n/a | No — disk lives on the user-owned engine pod, not on a server we provision; not a central PVC |
| **MP-mode** (`LMCacheMPConnector`, ZMQ `:5555` + HTTP `:8080`) | L1 RAM + **L2 NIXL POSIX → local disk** ✅ | `kv_connector_extra_config.lmcache.mp.{host,port}` (a *different* connector) | **No** — see below | Yes, but only co-located |

## Options evaluated

### Option A — keep `lmcache_server`, add a disk `<path>` positional — REJECTED

Premise: a tagged `lmcache_server <host> <port> <path>` form turns on disk at
`<path>`. **Not supported.** The `lm://` standalone server is an in-memory KV
store; its CLI takes `<host> <port>` and there is no device/path argument that
makes it spill to local disk. Disk on the RemoteBackend side is only available by
switching to a *network* store connector (`redis://`, `mooncakestore://`,
`infinistore://`) — not a local volume. There is no version where a positional
path makes the `lm://` server persist locally.

### Option B — migrate the server to MP mode (L2 POSIX → PVC) — REJECTED for our topology

MP mode is the only LMCache mechanism where a central server persists KV to
**local disk**: `--l1-size-gb <N>` (RAM hot tier) plus an L2 adapter

```json
{"type":"nixl_store","backend":"POSIX",
 "backend_params":{"file_path":"/var/lib/lmcache","use_direct_io":"false"},
 "pool_size":64}
```

passed as `--l2-adapter`. `file_path` would be the mounted PVC dir. So the
*storage* half maps. The problem is the **transport and deployment topology**:

- **The data plane is host-shared-memory, not the network.** ZMQ (`:5555`,
  DEALER/ROUTER) is **control-only** (STORE/RETRIEVE/LOOKUP routing); the KV
  tensor bytes move via `GPUTransferModule` (CUDA IPC) or `NonGPUTransferModule`.
  `--supported-transfer-mode` is `{gpu, non_gpu, auto}`; **`non_gpu` does not use
  TCP — it uses POSIX shared memory** (`--shm-name`, `/dev/shm`). So both data
  paths require the engine and the server to share host memory.
- **The documented deployment is node-local.** LMCache's MP deployment guide
  uses a **DaemonSet (one server per node)** with `hostNetwork: true`; the vLLM
  pod discovers the server via `status.hostIP` (same-node), and **both the server
  and engine pods mount the host's `/dev/shm`** ("Required for CUDA IPC shared
  memory transfers between containers"). Verbatim: *"No example exists for vLLM
  connecting to MP server in different pods purely over a TCP Service. All
  examples require co-location on the same node with shared `/dev/shm` access."*

Consequence for us: a server reachable **only via a ClusterIP Service** (no
shared `/dev/shm`, possibly a different node) has a control channel but **no data
plane** — STORE/RETRIEVE cannot transfer. MP mode therefore cannot back our
network-addressable, per-`CacheBackend` Deployment model. Adopting it would mean
re-architecting the substrate from "one cache-server Deployment per
`CacheBackend` behind a ClusterIP" to "one node-local DaemonSet server with
`hostNetwork` + shared host `/dev/shm`," **and** rewriting the engine-side wire
from the `lm://` RemoteBackend connector to `LMCacheMPConnector` (which also
forks the wire away from the External-backend path that reuses `lm://`). That is
an OEP-level substrate change, not a device-arg flip.

> Narrow exception, noted for completeness: a **same-pod** sidecar (vLLM +
> MP-server containers in one pod sharing a `/dev/shm` `emptyDir{medium:Memory}`
> + ZMQ over localhost) works for `non_gpu` without `hostIPC`. That is a
> per-engine-pod sidecar, not a per-`CacheBackend` server we provision and front
> with a Service, so it does not fit the managed-backend abstraction either.

### Option C — client-side LocalDisk — REJECTED

`LMCACHE_LOCAL_DISK=file://…` makes the **engine pod** spill to its own disk.
Engine pods are user-owned and ephemeral; this is neither a server we provision
nor a central PVC, and it sits outside the cache-plane enforcement boundary (we
own the cache-server, not the engine). Out of scope.

## The actual decision (since none of A/B/C delivers local-PVC on a network server)

### Option D1 — Defer (keep the PVC plumbing, mark it upstream-blocked)

Keep the shipped Kubernetes-side plumbing exactly as-is (PVC provisioned +
mounted + `status.capacity`, multi-replica gate, adopt-and-keep). Record that
**no LMCache mechanism makes a network-addressable central server persist KV to a
local PVC today**, and revisit if/when LMCache adds local-disk support to the
`lm://` RemoteBackend server.

- **Pros:** zero churn; the PVC plumbing is reusable the day upstream offers it.
- **Cons:** `spec.storage.pvc` stays an attached-but-not-written-to volume — a
  permanently half-kept persistence promise with no near-term path to closure
  (upstream is unlikely to add local disk to the network server; the disk-capable
  server is deliberately node-local). It is the inert-field anti-pattern dressed
  in a disclaimer, and it encodes a **category error**: local block storage is
  not how these KV layers persist (see below). Not recommended.

### Option D2 — Express durability via backend choice (remote store / Mooncake) — RECOMMENDED

Stop trying to make persistence a generic per-backend *volume* knob. Durability
and cross-replica sharing are **properties of the backend you select**, not of a
PVC bolted onto an in-memory backend. LMCache itself models this: the
RemoteBackend connector scheme (`redis://`, `mooncakestore://`, …) determines the
storage characteristics, and the in-memory `lm://` server is just the simplest
one.

Concretely:

- The in-memory `lm://` LMCache backend stays the **simple/default** option
  (ephemeral, fastest to stand up).
- For **durable + shared + scalable** cache, the operator selects a backend
  designed for it — the planned **Mooncake** backend type (a managed Mooncake
  store, or an operator-supplied one via the RemoteBackend connector). Mooncake
  is **network-addressable** (fits our per-`CacheBackend` Deployment + ClusterIP
  model and the existing `lm://`-style engine wire, unlike MP mode), handles its
  own DRAM/SSD tiering and durability, and is explicitly within the project's
  "orchestrate existing KV-cache tech" charter.
- `spec.storage.pvc` (a local volume) is **retired** — it cannot be delivered on
  our topology and expresses persistence at the wrong layer. Retirement is
  permitted at pre-launch `v1alpha1` (see the schema-evolution policy). If a
  store-endpoint surface is later wanted, it returns as a `spec.storage.remote`-
  style field on the relevant backend, not as a PVC.

- **Pros:** delivers real persistence + cross-replica sharing on a topology that
  actually fits; aligns with the cache-plane charter and the already-roadmapped
  Mooncake backend adapter; removes a category-error field rather than carrying
  it forward into `v1beta1`. Durability becomes a first-class backend choice that
  also raises cache-hit rate across replicas (the real KPI), not just
  survive-restart on a single RWO replica.
- **Cons:** Mooncake is a different feature, not a drop-in — the operator runs/
  operates a Mooncake store (its own HA/ops), and we own the connector + config
  wiring. The connector's production-readiness against the vLLM path should be
  verified as due diligence when the Mooncake adapter is built (it is not a
  blocker for *retiring* PVC). A genuinely single-node / air-gapped operator who
  wants local durability with no external store has no clean path here (the
  same-pod MP sidecar is the only option, and it is niche) — a separate,
  deferrable conversation.

## Recommendation

**Adopt Option D2: retire `spec.storage.pvc` and let durability be a backend
choice (Mooncake / remote store).** The shipped K8s-side plumbing was correct
engineering against an incorrect premise — local-PVC persistence does not map to
how LMCache (or these KV layers generally) persist, and it cannot be delivered on
our network-addressable, per-`CacheBackend` model. Its value proposition
(survive-restart on a single RWO replica) is weak under the fail-open posture,
where a cold cache after restart is a few misses, not an outage. Carrying it
forward as an attached-but-unwritten volume (Option D1) preserves a half-kept
promise and a category error into `v1beta1`; forcing Option B re-architects the
substrate for a feature LMCache does not serve over a network Service. Removing
the field now (pre-launch `v1alpha1`, removal permitted) keeps the surface honest
and lets the planned Mooncake backend be the durable/shared/scalable path.

## Impact on the implementation follow-up (re-scope)

The implementation follow-up was pre-scoped at 1–3 points ("flip a device arg /
swap the entry point"). That estimate is **void** — there is no device arg, the
entry-point swap is an OEP-level substrate change, and the recommendation is to
not pursue local-PVC persistence at all. The device-arg follow-up is **closed as
won't-do**, replaced by two independent threads:

1. **Retire `spec.storage.pvc`** (its own follow-up). Remove the field and its
   K8s-side plumbing — the storage spec/status surface, the reconciler's PVC
   provisioning / mount / multi-replica gate / adopt-and-keep / capacity, the
   PVC RBAC + watch, the admission size/name rules, the sample + recipe + smoke
   phase, and the design-doc Persistent-storage section — at `v1alpha1`, and
   regenerate the CRD/RBAC. Small, mechanical, well-bounded.
2. **Mooncake backend adapter** (already on the roadmap). The durable / shared /
   scalable path. Includes a due-diligence pass on the LMCache MooncakeStore
   remote connector's production-readiness against the vLLM path. Independently
   scoped; not a continuation of the device-arg work.

## Open items / confidence

Topology and transport findings are taken from LMCache's own configuration,
architecture, and deployment guides and are considered authoritative for the
**documented/supported** surface. Two points are worth a maintainer confirmation
before this is marked Accepted (neither changes the recommendation):

1. **`lm://` RemoteBackend local disk:** confirmed in-memory in the docs; worth a
   one-line maintainer confirmation that no local-disk tier is planned for the
   network server.
2. **`non_gpu` vs `gpu` functional delta** beyond the GPU→`/dev/shm` staging copy:
   not load-bearing here (both require shared memory, ruling out the Service-only
   case) but not fully closed from docs.

## References

- LMCache — Multiprocess Mode (overview, configuration, architecture, deployment):
  <https://docs.lmcache.ai/mp/index.html>,
  <https://docs.lmcache.ai/mp/configuration.html>,
  <https://docs.lmcache.ai/mp/architecture.html>
- LMCache — storage architecture / RemoteBackend connector scheme:
  <https://docs.lmcache.ai/developer_guide/architecture.html>
- LMCache — local disk (client-side `LMCACHE_LOCAL_DISK`):
  <https://docs.lmcache.ai/kv_cache/storage_backends/local_storage.html>
- vLLM — LMCache connector examples:
  <https://docs.vllm.ai/en/latest/examples/others/lmcache.html>
