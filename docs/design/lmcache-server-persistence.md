# Design: LMCache server persistence — durability is a backend choice

Status: locked · Scope: managed-backend durability (`CacheBackend`)

> **Historical legacy-IP decision record.** This document explains why the old
> generic PVC surface was removed. Its LMCacheServer/Mooncake recommendation is
> superseded: current LMCache uses typed PodLocal MP with optional explicit
> Redis. Do not treat the providers below as current defaults and do not map
> them automatically to Redis; the operator must choose the desired L3 semantics.

## Decision

`CacheBackend.spec.storage` — and the nested `storage.pvc.*` plus the
`status.capacity` field — is **retired at `v1alpha1`**. Durability of a managed
cache backend is expressed as a **remote-provider choice**, not as a generic
per-`CacheBackend` volume knob:

- Omitting canonical `spec.remoteStorage` selects an engine-local host tier and
  provisions no provider workload.
- Historically, the managed **in-memory `lm://` LMCache server**
  (`spec.remoteStorage.provider: LMCacheServer`) is the simple shared tier. It
  keeps KV in process memory; it is not durable and does not persist across pod
  restarts.
- Historically, the managed **Mooncake provider**
  (`spec.remoteStorage.provider: Mooncake`) is the durable / shared / scalable
  path: a network-addressable store the engine reaches over the
  `mooncakestore://` remote wire. See
  [Mooncake provider configuration](cachebackend-api.md#mooncake-provider-configuration).

## Why a local PVC cannot honestly back the `lm://` server

An investigation into LMCache's storage model found **no mechanism by which a
network-addressable, per-`CacheBackend` LMCache server persists KV to a local
PVC**:

1. **The standalone server we render is in-memory only.** The
   `lmcache_server <host> <port> <storage>` process behind `spec.type: LMCache`
   holds KV in memory. On the LMCache *RemoteBackend* side, "disk" / durability
   means a **network store** (e.g. `redis://`, `mooncakestore://`) — not a local
   volume mounted on the server pod. Provisioning a PVC for that server would
   mount storage nothing writes KV to.
2. **LMCache's only on-server local-disk path is node-local.** Its MP-mode (the
   L2 NIXL POSIX backend writing to a `file_path`) requires `hostNetwork` and a
   shared host `/dev/shm`, where the control socket is ZMQ-only and KV bytes move
   over CUDA-IPC or POSIX shared memory. A server reachable only through a
   ClusterIP Service therefore has **no data plane** in that mode. The current
   implementation creates one directly scheduled server Pod per active engine
   node and CacheBackend; multiple pools on one node require disjoint host ports.

MP-mode is thus incompatible with this project's per-backend Deployment +
ClusterIP, engines-anywhere model.

## Consequences

- `spec.storage{,.pvc}` + `status.capacity` were removed as a category error:
  the Kubernetes-side PVC plumbing could be provisioned, but could never
  honestly back the in-memory server.
- The historical durable/shared recommendation was the **Mooncake backend**. Its
  managed workload lifecycle lives in the provider adapter
  (`internal/adapters/builtin/storage/mooncake.go`), while the vLLM runtime adapter
  (`internal/adapters/builtin/runtime/vllm_lmcache.go`) owns engine wiring.
- **Generalizable rule:** surface a `max*` / storage / quota field on a CRD only
  when the cache plane **authoritatively owns** the resource being limited. When
  it does not, omit the field or express the capability as a backend choice
  (as here) rather than a generic knob that cannot be honestly honored.
