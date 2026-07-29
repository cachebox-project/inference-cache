---
title: "Architecture"
linkTitle: "Architecture"
weight: 1
description: >
  The two binaries, the observation sidecar, the controller↔server bridge, and the
  invariants that shape them.
---

## One operator, two binaries

inference-cache is a single operator split across two control-plane binaries, plus a CLI
and an in-cluster sidecar. The split follows the responsibilities: the **controller** owns
the Kubernetes reconcile loop and admission; the **server** owns the routing decision and
the cache-state index.

### `inferencecache-controller`

The controller-runtime manager. It:

- **Reconciles the CRDs** — provisions the managed cache-server workload and Service for a
  `CacheBackend`, computes readiness, and writes status.
- **Serves six CR admission webhook entries** (over TLS, via cert-manager): defaulting +
  validation for `CacheBackend`, `CachePolicy`, and `CacheTenant`.
- **Serves the seventh entry, a mutating Pod webhook** — the *linker*. When an engine pod
  matches a `CacheBackend`'s `spec.engineSelector`, this webhook injects the KV-connector
  configuration and (optionally) the `kvevent-subscriber` sidecar into the pod. One webhook
  does both engine-config injection and sidecar injection.
- **Runs the bridge** to the server (see below).

Owns the `pkg/adapters/runtime` adapters that render the cache-server pod/Service and the
engine-side pod configuration.

### `inferencecache-server`

The gRPC + HTTP server. It:

- Serves the **`InferenceCache` gRPC API** on `:9090` (plus `grpc.health.v1`).
- Holds the **in-memory cache-state index** — a soft-state aggregate keyed by
  `(tenant, model, hash_scheme, prefix_hash)`.
- Exposes HTTP on `:8080` — `/healthz` (liveness), `/readyz` (readiness → index ready),
  `/metrics` (Prometheus `inferencecache_*`).
- Exposes a **controller-facing listener** on `:8081` — `/snapshot` (controller reads the
  aggregate), `/policy` (controller writes resolved policy), `/probe` (functional
  self-test). All three are gated by ServiceAccount bearer auth + a `NetworkPolicy`.

Owns the index (`pkg/index`), the mutable-slot render pipeline (`pkg/render`), and the
deterministic content fingerprint (`pkg/fingerprint`).

The server **fails closed**: without `--allowed-controller-sa` or
`--insecure-disable-auth` it exits rather than silently shipping unauthenticated endpoints.

### `kvevent-subscriber`

A sidecar injected next to each engine pod. It subscribes to the engine's ZMQ KV-cache
event stream, computes the content fingerprint in-pod, and calls the server's
`ReportCacheState`. It sets `replica_id = <pod-name>` and also runs a stats reporter that
derives `cache_memory_bytes` from a scraped usage percentage. Auto-injection is opt-in —
the controller injects it only when started with a `--kvevent-subscriber-image`.
The subscriber binary owns the engine event adapters in `pkg/adapters/engine`.

### `inferencecache` CLI

`inferencecache doctor` runs a read-only pre-flight diagnostic across the install — see
[Troubleshooting]({{< relref "/docs/administration/troubleshooting/" >}}).

## The controller ↔ server bridge

The controller and server use **two directional HTTP flows under the same ServiceAccount
identity.** Both are intra-cluster and soft state:

- **PULL — `GET :8081/snapshot`.** The controller's CacheIndex poller scrapes the server's
  aggregate roughly every 25–30 seconds and writes both the cluster-wide `CacheIndex.status`
  and the per-backend `CacheBackend.status.indexParticipation`. Write-only-on-change, to
  avoid resource-version churn.
- **PUSH — `POST :8081/policy`.** The CachePolicy reconciler watches `CachePolicy` (and
  `CacheTenant`) resources cluster-wide, flattens them to a resolved-policy snapshot, and
  POSTs the full snapshot. The server adopts **replace-on-write** — deleting a `CachePolicy`
  reverts its namespace to server defaults. A periodic re-push keeps a restarted server in
  sync.

The controller also calls `POST :8081/probe` for the functional self-test; it is not a
state-replication flow.

Pulled state (CacheIndex) reflects what the server has heard from the substrate; pushed
state (policy) is operator intent flowing the other way. Operators reason about both at the
`CacheBackend.status` + `CachePolicy.spec` layer; the bridge is the implementation detail
that connects them.

Auth posture: `/snapshot` and `/probe` carry a controller-audience projected ServiceAccount
token; `/policy` carries a distinct policy-audience token. A leaked token for one audience
is useless against the other, and useless against the apiserver. Auth outcomes are counted
by `inferencecache_{snapshot,policy,probe}_auth_total`.

## Invariants that shape everything

These principles recur across every design decision — internalize them and the API stops
surprising you.

### Fail-open

The cache is an **optimization, never a serving dependency.** If the server is unreachable,
if a lookup times out, or if a prefix is unknown, the answer is an empty hint (`NO_HINT`)
and the gateway round-robins. The hot path never errors. (The one exception is an explicit
`integration.failOpen: false` on a `CacheBackend`, which asks the engine to fail-closed
instead.)

### Soft state

The index is in-memory and lossy by design. Losing state produces a **cache miss, never a
wrong answer.** A stale entry that points at a replica which no longer holds the prefix
simply yields a miss when the gateway routes there. Malformed inputs (e.g. mismatched
block-hash chains) are dropped on ingest rather than trusted.

### "We hint, the gateway decides"

The server owns *all* routing intelligence — scoring, tie-breaks, threshold filtering,
freshness ranking. The gateway does not walk chains, hold per-replica state, or implement
tie-break logic. `LookupRoute` returns a ranked list and a reason code; the gateway routes
to the top replica or round-robins on no hint. This keeps ranking evolvable in one place
and consistent across every client.

### Enforcement boundary

inference-cache **describes** cache state; it does not **control** engine memory. It
surfaces a quota (`max*`) field only for a resource it authoritatively owns — the index
entry table (`CacheTenant.spec.quota.maxIndexEntries`). It deliberately does *not* offer a
per-tenant memory quota, because engine KV memory is a single shared, tenant-unaware pool
that the plane can neither enforce nor honestly attribute per tenant. For true per-tenant
byte isolation, run separate engine Deployments per tenant and let pod memory limits
enforce at the cgroup layer.
