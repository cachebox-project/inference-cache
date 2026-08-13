# Design: LMCache server persistence — durability is a backend choice

Status: locked · Scope: managed-backend durability (`CacheBackend`)

> **Historical legacy-IP decision record.** This document explains why the old
> generic PVC surface was removed. Its LMCacheServer/Mooncake recommendation is
> superseded: current LMCache uses typed PodLocal MP with optional explicit
> Redis. Do not treat the providers below as current defaults and do not map
> them automatically to Redis; the operator must choose the desired L3 semantics.

## Current decision

`CacheBackend.spec.storage` — and the nested `storage.pvc.*` plus the
`status.capacity` field — is **retired at `v1alpha1`**. Durability of a managed
cache backend is expressed as a **remote-provider choice**, not as a generic
per-`CacheBackend` volume knob:

- Omitting canonical `spec.remoteStorage` selects an engine-local host tier and
  provisions no provider workload.
- `spec.remoteStorage.provider: Redis` is the only current remote tier. Managed
  ownership renders a single Redis Deployment and Service; external ownership
  binds the declared endpoint without creating a workload.
- Removed `LMCacheServer` and Mooncake objects are not translated to Redis.
  Reintroducing either technology requires a separately validated typed MP
  adapter and an explicit operator migration choice.

## Historical rationale: why a local PVC could not back the removed IP server

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

That finding invalidated the old per-backend Deployment + ClusterIP model. The
current typed MP design instead uses a PodLocal native sidecar or one directly
scheduled NodeLocal server per active engine node; neither exposes the MP CUDA
data plane through a load-balanced Service.

## Consequences

- `spec.storage{,.pvc}` + `status.capacity` were removed as a category error:
  the Kubernetes-side PVC plumbing could be provisioned, but could never
  honestly back the in-memory server.
- Current managed Redis lifecycle lives in
  `internal/adapters/builtin/storage/redis.go`. Common typed MP rendering lives
  in `internal/adapters/builtin/runtime/lmcache_mp_renderer.go` and
  `lmcache_mp_nodelocal.go`; the SGLang and vLLM adapters own only their
  engine-specific launch surfaces.
- **Generalizable rule:** surface a `max*` / storage / quota field on a CRD only
  when the cache plane **authoritatively owns** the resource being limited. When
  it does not, omit the field or express the capability as a backend choice
  (as here) rather than a generic knob that cannot be honestly honored.
