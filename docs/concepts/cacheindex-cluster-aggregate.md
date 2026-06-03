# CacheIndex — the cluster cache "world map"

`CacheIndex` is a cluster-scoped, **status-only** CR (shortName `ci`) that reflects what the cache plane actually knows right now: how many distinct prefixes are indexed, broken down per tenant and per engine replica. It is the operator's first stop when chasing "is tenant X getting cache hits?" or "is replica Y reporting state?" — a read-only mirror, **not** a routing substrate. It is metadata-only: it never stores KV tensors or prompt text.

There is exactly one object, a controller-maintained singleton named `cluster-default`. The spec is intentionally empty — there is nothing to configure. You observe it; you never `kubectl apply` data into it.

## How it gets populated — the PULL bridge

CacheIndex is the mirror image of the CachePolicy **push**. Where the CachePolicy reconciler pushes operator intent *into* the server, the CacheIndex poller pulls observed state *out* of it:

| Direction | Surface | Mechanism |
|---|---|---|
| PULL (this doc) | `CacheIndex.status` | Controller polls the server's internal `GET /snapshot` (on the `:8081` listener) roughly every ~30s and writes the status **write-only-on-change**. |
| PUSH (contrast) | server policy state | CachePolicy reconciler posts resolved policy to the server. |

Because the write happens **only when the data changes**, a steady-state poll that sees no change does **not** advance `status.lastUpdated`. So `lastUpdated` marks the last *data* change, not the last poll. Poller liveness lives in controller metrics, not in this field — do not read a stale `lastUpdated` as "the poller died." See [`docs/design/policy-propagation.md`](../design/policy-propagation.md) for the bridge in full.

## The three counts

The status carries three nested views of the same index, each answering a different question:

| Surface | Status path | Meaning |
|---|---|---|
| Cluster total | `status.prefixes.summary.total` | Distinct prefixes across the whole index. (`status.prefixes.summary.hot` is reserved and always `0` today — per-prefix access counting is not implemented yet.) |
| Per-tenant | `status.tenants[]` — `{id, indexEntries, memoryUsed, hitRate}` | Per-tenant footprint. `indexEntries` summed across all tenant rows equals the cluster total by construction (it is the per-tenant breakdown of `prefixes.summary.total`). The empty-string `id` is the untenanted bucket. |
| Per-replica | `status.replicas[]` — `{id, tenant, cacheMemoryBytes, hitRate, pressure, lastUpdate}` | Per-replica cache health. Only replicas that **reported stats** appear here; prefix-only replicas show up in `CacheBackend.status.indexParticipation` instead, not on this surface. |

Two top-level fields round out the object: `status.observedServer` (which server endpoint the aggregate was scraped from) and `status.lastUpdated` (last data change, per the note above).

`hitRate` and `pressure` are decimal strings in `[0,1]` (e.g. `"0.78"`) — CRDs avoid floats for cross-language portability, so they render as quoted strings.

### Printer columns

```text
$ kubectl get cacheindex
NAME              PREFIXES   CHANGED   AGE
cluster-default   1428       30s       6d
```

| Column | JSONPath |
|---|---|
| `PREFIXES` | `.status.prefixes.summary.total` |
| `CHANGED` | `.status.lastUpdated` |
| `AGE` | `.metadata.creationTimestamp` |

## Operator debugging flow

Start wide, then drill in:

1. **Headline count.** `kubectl get cacheindex cluster-default` — the `PREFIXES` column is the whole-cluster distinct-prefix total at a glance, and `CHANGED` tells you how fresh that number is.
2. **Per-tenant / per-replica breakdown.** `kubectl get cacheindex cluster-default -o yaml` — read `status.tenants[]` to answer "why is tenant X not getting cache hits?" (low `indexEntries` or `hitRate` for that tenant), and `status.replicas[]` to answer "is replica Y reporting state?" — **presence of its `id` row** is the signal that it has reported stats. Do **not** read `lastUpdate` as a liveness clock: it is write-only-on-change, so a replica reporting identical stats keeps a steady `lastUpdate` while still being perfectly alive. For reporter liveness, use the server's `/metrics`, not this field.
3. **Per-backend detail.** For one CacheBackend's participation, read `CacheBackend.status.indexParticipation` instead — that surface includes prefix-only replicas that never reported stats and so are absent from `CacheIndex.status.replicas[]`.

Illustrative `-o yaml` output:

```yaml
status:
  observedServer: http://inference-cache-server.inference-cache-system.svc:8081
  lastUpdated: "2026-06-02T14:21:08Z"
  prefixes:
    summary:
      total: 1428
      hot: 0                      # reserved; always 0 today
  tenants:
    - id: team-search             # indexEntries across tenants sums to prefixes.summary.total
      indexEntries: 902
      memoryUsed: 734003200
      hitRate: "0.81"
    - id: team-rag
      indexEntries: 526
      memoryUsed: 411041792
      hitRate: "0.64"
  replicas:
    - id: qwen-engine-7d9c5-abcde
      tenant: team-search
      cacheMemoryBytes: 536870912
      hitRate: "0.78"
      pressure: "0.42"
      lastUpdate: "2026-06-02T14:21:08Z"
```

> Status is server-populated. The block above is *output* you read, not something you can apply — a normal `kubectl apply` writes only `spec`, and the status subresource ignores any `status` you include.

## Edge case — same pod name across namespaces

`status.replicas[]` is a map-list keyed on `id`, and the replica `id` **is** the engine pod's `metadata.name`. Two engine pods that share a name across **different** namespaces would therefore collide on `id` on this surface.

The mitigation is the optional `tenant` field on each replica row: the subscriber derives the tenant from the pod's namespace, so it disambiguates the source even when `id` repeats. If two stats-reporting replicas ever collide on `id` in a single poll tick, the controller resolves it deterministically — it picks the **lexicographically-later** `tenant`, and the published row's `tenant` field identifies which one was chosen. (The internal `/snapshot` keeps the two separate, keyed by `(tenant, replicaId)`; only the v1alpha1 CR surface keeps `id` as the sole map key, for backward compatibility.)

## The minimal object

The singleton ships as a kubebuilder sample — name `cluster-default`, empty spec, no status (the controller writes the status):

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheIndex
metadata:
  labels:
    app.kubernetes.io/name: inference-cache
  name: cluster-default
```

A copy ships at [`config/samples/cache_v1alpha1_cacheindex.yaml`](../../config/samples/cache_v1alpha1_cacheindex.yaml).

## When NOT to use it

- **It is not a routing input.** Gateways do not read `CacheIndex` to make routing decisions — that is what the `LookupRoute` gRPC hint is for. CacheIndex is observability only.
- **You never write data into it.** The spec is empty by design; `kubectl apply` writes only `spec`, and any `status` you include is ignored by the status subresource. If you find yourself wanting to set a value here, you want a different CR.
- **`lastUpdated` is not a liveness probe.** It marks the last data change, not the last successful poll. Use controller metrics to confirm the poller is alive.
- **Per-backend questions belong elsewhere.** For one CacheBackend's prefix count and last-event time — including prefix-only replicas — read `CacheBackend.status.indexParticipation`, not this aggregate.

## See also

- [`docs/design/policy-propagation.md`](../design/policy-propagation.md) — the controller↔server bridge (pull vs push) that populates this status.
- [`docs/design/policy-crds.md`](../design/policy-crds.md) — the CacheIndex CRD contract and status shape.
- [`docs/design/crd-contract.md`](../design/crd-contract.md) — design rationale and cross-CRD invariants.
- `cachebackend-engine-binding.md` — how engine pods become the replicas that report into this index.
