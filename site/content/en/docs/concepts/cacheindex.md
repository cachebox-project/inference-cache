---
title: "CacheIndex"
linkTitle: "CacheIndex"
weight: 5
description: >
  A cluster-scoped, status-only mirror of the server's in-memory aggregate — the cache
  "world map".
---

## What is a CacheIndex?

`CacheIndex` is a cluster-scoped, **status-only** singleton named `cluster-default`. It
mirrors the server's in-memory cache-state aggregate into the Kubernetes API so you can
observe cluster-wide cache state with `kubectl get cacheindex`. It is for **observability**,
not routing — the live routing substrate is the server's in-memory index, and `LookupRoute`
never reads the CR.

`CacheIndex` is the only cluster-scoped inference-cache CRD, short name `ci`. Its `spec` is
intentionally empty; the controller maintains its `status` by polling the server's
`/snapshot` endpoint (see [the controller↔server bridge]({{< relref "/docs/concepts/architecture/#the-controller--server-bridge" >}})).

```bash
kubectl get cacheindex cluster-default -o yaml
```

## Status shape

| Field | Meaning |
|---|---|
| `replicas[]` | Per-replica rows — `id`, `tenant`, `cacheMemoryBytes`, `hitRate`, `pressure`, `lastUpdate`. Only replicas that report stats appear here (a prefix-only replica does not). Map-list keyed on `id`. |
| `tenants[]` | Per-tenant rows — `id`, `indexEntries`, `hitRate`. |
| `prefixes.summary` | `total` distinct prefix keys (and a reserved `hot` counter, always 0 today). |
| `observedServer` | The snapshot URL the data came from. |
| `lastUpdated` | The last time the **data changed** — not the last poll. Status is written only on change. |

Printer columns: `Prefixes`, `Changed`, `Age`.

{{% alert title="Metadata only" color="info" %}}
Like the whole system, the aggregate carries **hashes and statistics, never KV tensors or
prompt text.**
{{% /alert %}}

### Two conventions to know

- **Pointer = "not yet observed".** `hitRate` (`*string`) and per-tenant `indexEntries`
  (`*int64`) are pointers so that `nil` (not yet reported) is distinct from a real zero. The
  controller emits the pointer only once the server's snapshot carries a presence bit — a
  fabricated `"0"` would read as a genuine observation. The same pointer convention is
  applied consistently on the per-backend `CacheBackend.status.indexParticipation`.
- **`tenants[].memoryUsed` is deprecated.** It is a published v1alpha1 status field, so
  rather than remove it, the controller stops populating it (always `0`) — the same
  double-counting argument that removed the per-tenant *memory quota* applies here. The
  honest per-replica `replicas[].cacheMemoryBytes` (engine total per replica) stays.

## Relationship to CacheBackend

`CacheIndex` is the **cluster-wide** view; `CacheBackend.status.indexParticipation` is the
**per-backend slice** of the same data. Per-replica *prefix* counts live on the backend's
`indexParticipation` (prefix-only replicas that don't report stats are absent from
`CacheIndex.status.replicas`). Both are written by the same controller poller from the same
`/snapshot` pull.

`CacheIndex` (pulled from the server) is the mirror image of `CachePolicy` (pushed to the
server): pulled state is what the server has heard from the substrate; pushed state is
operator intent flowing the other way.
