# Concept docs

Operator-facing guides for the CRDs that make up the cache plane: what each
resource does, when to reach for it, and the gotchas that bite in practice.
Start here, then drop into [`docs/design/crd-contract.md`](../design/crd-contract.md)
when you want the design rationale behind the surface.

| Area | Doc |
|---|---|
| Engine-pod binding | [`cachebackend-engine-binding.md`](cachebackend-engine-binding.md) |
| Engine overrides | [`cachebackend-engine-overrides.md`](cachebackend-engine-overrides.md) |
| Per-namespace tuning | [`cachepolicy-tuning.md`](cachepolicy-tuning.md) |
| Tenant identity + quota | [`cachetenant-identity-and-quota.md`](cachetenant-identity-and-quota.md) |
| Cluster aggregate / debugging | [`cacheindex-cluster-aggregate.md`](cacheindex-cluster-aggregate.md) |
| Design rationale + invariants | [`../design/crd-contract.md`](../design/crd-contract.md) |

These cover the high-leverage operator scenarios for the actively-reconciled
CRDs. The render-layer (`PromptTemplate`) and phase-disaggregation
(`PDTopology`) CRDs get their own concept docs when their controllers ship;
`kubectl explain <crd>` serves the per-field API reference in the meantime.
