---
title: "Isolate tenants"
linkTitle: "Isolate tenants"
weight: 4
description: >
  Tenant identity, entry-count quota, and the pattern for true per-tenant byte isolation.
---

Multi-tenancy in inference-cache comes in two layers: **structural identity isolation**
(always on) and an optional **entry-count quota**. For byte-level isolation you use
Kubernetes, not a cache field — this page explains why and how.

See [CacheTenant](/docs/concepts/cachetenant/) for the field reference.

## Identity isolation is automatic

The index is keyed by `(tenant, model, hash_scheme, prefix_hash)`. Because `tenant` is part
of the key, one tenant's hints can never collide with another's, and `LookupRoute` is
tenant-scoped. You get this the moment your producers report a `tenant_id` — you do not need
a `CacheTenant` for isolation to hold.

You create a `CacheTenant` to give a tenant a **named, quota-bearing** identity:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheTenant
metadata:
  name: team-search
  namespace: serving
spec:
  tenantID: search-prod        # the identity on the wire — NOT the object name
  quota:
    maxIndexEntries: 200000
  isolationMode: Fairness
```

{{% alert title="tenantID is the wire identity" color="info" %}}
`spec.tenantID` — not `metadata.name` — is what producers send in
`CacheStateUpdate.tenant_id` and what `LookupRoute` scopes on. Point your gateway and
subscriber at the same `tenantID`.
{{% /alert %}}

## Bounding a tenant's index footprint

`spec.quota.maxIndexEntries` caps the number of distinct prefix keys a tenant may hold. When
a tenant exceeds it, the **oldest entries are evicted** (Fairness) — a lookup is never
rejected. Unset means unbounded.

Pick a budget from your cluster index cap and how many tenants share it — see
[Index sizing](/docs/administration/index-sizing/) for the sizing math. A bounded, non-zero
`inferencecache_tenant_evictions_total` for a tenant is normal Fairness behavior, not an
error.

## The tenantID uniqueness rules

- A duplicate `tenantID` **within one namespace** is rejected at admission.
- The same `tenantID` **across namespaces** is intentionally permitted (the index is
  namespace-blind on tenant). When it happens, the `CacheIndex` surfaces a
  `DuplicateTenantID` status condition and the controller picks a deterministic effective
  owner. Reuse across namespaces only when you truly mean the same logical tenant.
- `tenantID: inferencecache.io/probe` is **reserved** for the server's self-test and is
  rejected.

## True per-tenant byte isolation

There is deliberately **no** per-tenant memory quota. Engine KV memory is a single shared,
tenant-unaware LRU pool — the cache plane can neither enforce a byte budget on it nor
honestly attribute bytes per tenant on a shared engine (they would double-count). See the
[enforcement boundary](/docs/concepts/architecture/#enforcement-boundary).

**The supported pattern for hard byte isolation is Kubernetes-native:** run a separate engine
Deployment (and its own `CacheBackend`) per tenant, and let pod memory limits enforce at the
cgroup layer.

```yaml
# tenant A's engine
apiVersion: apps/v1
kind: Deployment
metadata:
  name: engine-tenant-a
spec:
  template:
    metadata:
      labels:
        app: engine-a
        tenant: a
    spec:
      containers:
        - name: vllm
          resources:
            limits:
              memory: 40Gi      # hard per-tenant ceiling, enforced by the kubelet cgroup
---
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: cache-tenant-a
spec:
  type: LMCache
  engineSelector:
    matchLabels:
      app: engine-a             # binds only tenant A's engine
```

Each tenant gets its own engine, its own cache backend, and a kubelet-enforced memory
ceiling — the isolation the cache plane cannot provide on a shared engine.

## Related pages

- [CacheTenant](/docs/concepts/cachetenant/) — the field reference.
- [Index sizing](/docs/administration/index-sizing/) — quota budgeting.
