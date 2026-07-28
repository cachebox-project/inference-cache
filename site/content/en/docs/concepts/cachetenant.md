---
title: "CacheTenant"
linkTitle: "CacheTenant"
weight: 4
description: >
  Give an external tenant a stable identity the index isolates on, plus an optional
  entry-count quota.
---

## What is a CacheTenant?

A `CacheTenant` gives an external tenant two things:

1. A stable **identity** (`spec.tenantID`) that the cache-state index isolates on, so one
   tenant's cache hints can never collide with another's.
2. An optional **entry-count quota** that caps how many distinct prefix keys the tenant may
   occupy in the index.

`CacheTenant` is namespaced, short name `ct`.

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheTenant
metadata:
  name: team-search
  namespace: serving
spec:
  tenantID: search-prod        # external identity carried on the wire — NOT the CR name
  quota:
    maxIndexEntries: 200000
  isolationMode: Fairness
```

## Identity isolation is structural

The index is keyed by `(tenant, model, hash_scheme, prefix_hash)`. Because `tenant` is part
of the key, tenant A's hint records physically cannot collide with tenant B's, and
`LookupRoute` is tenant-scoped. This isolation is real regardless of the underlying engine
shape.

- `spec.tenantID` is the identity carried in the gRPC wire (`CacheStateUpdate.tenant_id`),
  **not** the Kubernetes object name.
- `tenantID` is **namespace-blind** in the index: reusing the same `tenantID` across
  namespaces is intentionally permitted (surfaced at runtime via a `DuplicateTenantID`
  status condition on the `CacheIndex`), while a duplicate within one namespace is rejected
  at admission.
- The `tenantID` value `inferencecache.io/probe` is **reserved** for the server's functional
  self-test and is rejected on any `CacheTenant`.

## Quota is entry-count only

`spec.quota.maxIndexEntries` caps the number of distinct prefix keys the tenant may hold.
When a tenant exceeds its budget, the oldest entries are **evicted** (Fairness) — a lookup
never rejects. An unset quota, or a tenant with no `CacheTenant` at all, is unbounded.
`spec.isolationMode` is `Fairness` (the only mode today).

### Why there is no memory quota

This is the [enforcement boundary](/docs/concepts/architecture/#enforcement-boundary) in
action. The index entry table is a data structure inference-cache **owns**, so an entry-count
quota is enforceable. Engine KV memory is **not** ours — vLLM's KV cache is a single shared
LRU pool indexed by block hash, with no tenant awareness. On a shared engine the plane could
neither enforce a per-tenant byte budget nor honestly attribute bytes per tenant (the numbers
would be double-counted across tenants sharing the engine). So `maxMemoryBytes` and
`status.memoryUsed` were deliberately left out of the v1alpha1 shape.

**For true per-tenant byte isolation today:** run separate engine Deployments per tenant and
let `Pod.spec.containers[].resources.limits.memory` enforce at the pod-cgroup layer.

## Status

| Field | Meaning |
|---|---|
| `indexEntries` | Pointer — `nil` (not yet computed) vs the tenant's observed distinct-key count. |
| `conditions` | e.g. `DuplicateTenantID` when a `tenantID` collides across namespaces. |
| `observedGeneration` | Standard reconcile bookkeeping. |

Printer columns: `Tenant`, `Entries`, `Quota`, `Isolation`, `Age`.

## Related pages

- [Isolate tenants](/docs/tasks/isolate-tenants/) — the recommended isolation patterns.
- [Index sizing](/docs/administration/index-sizing/) — choosing a `maxIndexEntries` budget.
