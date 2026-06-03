# CacheTenant — identity and quota

`CacheTenant` is the namespaced CR (shortName `ct`) that gives an external
tenant two things at once: a stable **identity** the index isolates on, and an
optional **entry-count quota** that caps how much of the index that tenant may
hold. Reach for it when you want per-tenant isolation of cache hints and a
ceiling on a tenant's index growth. Without a matching CacheTenant, an ingest
still keys under whatever `tenant_id` it carries — it simply runs **unbounded**
(no quota is applied). Only ingests carrying *no* `tenant_id` fall into the
empty-tenant bucket; a non-empty `tenant_id` always gets its own bucket, CR or not.

## Spec fields

These are the only spec fields — do not expect others.

| Field | Type | Default | Notes |
|---|---|---|---|
| `tenantID` | string (required, non-empty) | — | The external tenant identifier carried by gateway/engine traffic (`CacheStateUpdate.tenant_id`). This is **not** the CR name. |
| `quota.maxIndexEntries` | int64 (min `0`) | unset = unbounded | Max **distinct prefixes** the tenant may hold in the index. Omit `quota` for no cap. `0` is a valid cap — the tenant settles at zero retained entries (the index evicts down to the cap; it is not a rejection of ingests). |
| `isolationMode` | enum (`Fairness`) | `Fairness` | Only value implemented today. |
| `crypto` | object | empty | Reserved for future cryptographic isolation; carries no fields today. |

The quota unit is the **distinct key** `(tenant, model, hash_scheme, prefix_hash)`.
A prefix held warm on several replicas counts **once**, not once per replica.

## Status and printer columns

| Status field | Meaning |
|---|---|
| `indexEntries` | Observed distinct-prefix count for this tenant. A `nil` pointer means "not yet computed" (no snapshot observed), distinct from an observed `0`. |
| `observedGeneration` | The `spec` generation the **status projection** has seen — this status is written by the CacheIndex snapshot poller, so `observedGeneration` advancing means the poller observed this generation, **not** that the tenant's quota intent was pushed to or enforced by the server. That propagation runs on the separate `/policy` path. |
| `conditions` | Standard condition list (e.g. the duplicate-`tenantID` signal below). |

```text
$ kubectl get ct
NAME              TENANT     ENTRIES   QUOTA    ISOLATION   AGE
cachetenant-...   tenant-a   42        100000   Fairness    5m
```

Columns: **Tenant** (`spec.tenantID`), **Entries** (`status.indexEntries`),
**Quota** (`spec.quota.maxIndexEntries`), **Isolation** (`spec.isolationMode`),
**Age**.

## Identity isolation is structural

The index keys every entry by `(tenant, model, hash_scheme, prefix_hash)`, so
tenant A's hint records can never collide with tenant B's, and `LookupRoute` is
tenant-scoped: a lookup carrying `tenant_id = A` only ever sees A's prefixes.
This isolation is a property of **our** data structure, not of the engine — it
holds regardless of the underlying engine shape, even when tenants A and B share
one engine pod.

## Quota is entry-count, and it evicts rather than rejects

`maxIndexEntries` is enforced **at ingest**. When a tenant's distinct-prefix
count exceeds its budget, the index **evicts that tenant's oldest prefixes**
(Fairness) down to the cap. It does **not** reject the ingest, and it never
touches another tenant's entries.

Enforcement is **fail-open**: an ingest whose `tenant_id` matches no
`CacheTenant` (or a CacheTenant with no `quota`) is unbounded — a missing or
quota-less tenant is never starved, only an over-budget one is trimmed.

There is deliberately **no memory quota** (no `maxMemoryBytes`). The only `max*`
field is the one the cache plane authoritatively owns: the index entry table.

## Enforcement boundary — why there is no memory quota

There is intentionally no `spec.quota.maxMemoryBytes` and no
`status.memoryUsed`. The cache plane surfaces a `max*` quota only for a resource
it **authoritatively owns** — the index entry table. Engine KV memory is a
shared, tenant-unaware LRU pool: vLLM and LMCache key blocks by block hash, not
by tenant. On a shared engine the control plane can therefore neither enforce a
per-tenant byte budget nor honestly attribute bytes per tenant (the engine's
`cache_memory_bytes` is a single total that would be double-counted across
tenants sharing it).

The recommended pattern for true **per-tenant byte isolation today**: run a
separate engine Deployment per tenant and let Kubernetes pod memory limits
(`resources.limits.memory`) enforce at the cgroup layer.

The design rationale lives in
[`docs/design/policy-crds.md`](../design/policy-crds.md#cachetenant) and the
ingest-time enforcement detail in
[`docs/design/policy-propagation.md`](../design/policy-propagation.md#enforcement-what-the-server-does-with-each-field).

## Cross-namespace `tenantID` reuse

`tenantID` identity is **namespace-blind** in the index — the index keys tenants
by the bare `tenantID` string.

| Scenario | Outcome |
|---|---|
| Two CacheTenants with the same `tenantID` in **one** namespace | Rejected at admission (a duplicate within a namespace is an unambiguous mistake) — but the check is *best-effort* (list-then-admit, raceable by concurrent creates), with the controller's deterministic dedup as the authoritative backstop. |
| Same `tenantID` across **different** namespaces | Permitted — can be deliberate (e.g. a migration). |

When cross-namespace duplicates collide, the controller deterministically picks
one effective owner — among the **quota-bearing** CacheTenants for that
`tenantID`, the first by `(namespace, name)` ascending wins. (A duplicate
*without* `quota.maxIndexEntries` carries no budget to enforce, so it never
becomes the enforced owner.) The shadowed CR surfaces a `DuplicateTenantID` /
not-effective status condition rather than silently advertising a budget that
isn't enforced. See
[`docs/design/policy-propagation.md`](../design/policy-propagation.md#wire-schema-v3)
for the tie-break.

## Example

A CacheTenant declaring identity `tenant-a`, Fairness isolation, and a
100 000-prefix index cap. A copy-pasteable version ships at
[`config/samples/cache_v1alpha1_cachetenant.yaml`](../../config/samples/cache_v1alpha1_cachetenant.yaml).

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheTenant
metadata:
  name: cachetenant-sample
spec:
  tenantID: tenant-a            # <-- external identity carried in CacheStateUpdate.tenant_id; NOT the CR name
  isolationMode: Fairness
  quota:
    maxIndexEntries: 100000     # <-- distinct-prefix cap; omit `quota` entirely for unbounded
```

## When NOT to use it

- If you don't need per-tenant identity or an index-growth cap, skip it —
  untenanted ingests already work (empty-tenant bucket, unbounded).
- Do not reach for CacheTenant to bound **memory**: it caps index entries, not
  bytes. For per-tenant byte isolation, separate the engine Deployments and use
  pod memory limits (see the enforcement-boundary section above).

True per-tenant engine-**memory** partitioning on a shared engine is deferred to
a future cross-project effort — it needs upstream vLLM/LMCache tenant-awareness
plus a new server→engine eviction direction that does not exist today.

## See also

- [`docs/design/crd-contract.md`](../design/crd-contract.md) — CRD design rationale and
  the cross-CRD invariants, including the enforcement-boundary principle.
- [`docs/design/policy-crds.md`](../design/policy-crds.md#cachetenant) — the
  CacheTenant field contract and the enforcement-boundary rationale.
- [`docs/design/policy-propagation.md`](../design/policy-propagation.md#enforcement-what-the-server-does-with-each-field)
  — how `maxIndexEntries` is propagated to the server and enforced at ingest.
- [`config/samples/cache_v1alpha1_cachetenant.yaml`](../../config/samples/cache_v1alpha1_cachetenant.yaml)
  — a runnable minimal CacheTenant.
