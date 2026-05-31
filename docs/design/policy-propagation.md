# Design: CachePolicy propagation

Status: implemented · Owners: controller (push) + server (apply)

`CachePolicy` is a namespaced CRD reconciled by the controller, but its
enforcement (eviction TTL, lookup threshold, lookup deadline) lives in the
policy server. This document describes how the controller PROPAGATES the
declarative CRs into the server's runtime so the configuration surface
actually changes server behavior.

## Direction

The controller PUSHES; the server APPLIES.

Mirror image of the `CacheIndex` status path: the server publishes
in-memory cache aggregate at `GET /snapshot` and the controller polls it,
because the server owns the data. Here, the controller owns the data (the
set of `CachePolicy` CRs), so it publishes and the server consumes.

| Channel | Direction | Endpoint | Trigger |
|---|---|---|---|
| `/snapshot` | controller ← server | `GET` | controller tick |
| `/policy`   | controller → server | `POST` | watch event + tick |

Both sit on the server's HTTP port (`:8080`), alongside `/healthz`,
`/readyz`, `/metrics`. The endpoints are internal — securing them
(NetworkPolicy + authn) is tracked separately.

## Snapshot semantics

The controller always sends a FULL snapshot of one resolved policy per
namespace (see "Multiple CachePolicies in one namespace" below). The
server adopts **replace-on-write**: the snapshot becomes the new state,
and any namespace not present reverts to server defaults. A CR delete
therefore propagates as "next snapshot omits this namespace."

The server's policy store is purely in-memory (soft state, like the cache
index). If the server restarts and loses everything, the controller's
periodic re-push (default 30s) brings it back into sync without operator
intervention.

## Wire schema (v1)

```json
{
  "version": 1,
  "policies": [
    {
      "namespace": "team-a",
      "evictionTTL": 900000000000,
      "minimumPrefixTokens": 32,
      "lookupTimeoutMs": 25
    },
    {
      "namespace": "team-b",
      "evictionTTL": 3600000000000
    }
  ]
}
```

- `version` — schema version. Bumped on a breaking change. The server
  rejects any value it does not recognize (HTTP 400).
- `policies[]` — full snapshot of all `CachePolicy` CRs in the cluster.
  Sorted by `namespace` for deterministic bodies (and for easier diffing
  in tests).
- `policies[].namespace` — the CR's namespace, used by the server as the
  tenant key (see *Tenant mapping* below).
- `policies[].evictionTTL` — Go `time.Duration` (nanoseconds, JSON
  number). Optional. `<=0` ⇒ "use server default" (`DefaultTTL = 30m`).
- `policies[].minimumPrefixTokens` — int32. Optional. `<=0` ⇒ "no
  threshold".
- `policies[].lookupTimeoutMs` — int32 milliseconds. Optional. `<=0` ⇒
  "no deadline".

The server's `policyHandler` decodes with `DisallowUnknownFields` so an
unknown field surfaces as HTTP 400 rather than silently dropping. Request
body is capped at 1 MiB.

Successful PUSH returns `HTTP 204 No Content` with an empty body.

## Multiple CachePolicies in one namespace

The `CachePolicy` CRD does **not** enforce a singleton per namespace.
When the controller observes more than one CachePolicy in a single
namespace it deduplicates deterministically: the entries are sorted by
`(namespace, name)` ascending and the FIRST entry per namespace wins
(i.e. the lexicographically smallest `metadata.name`). The losing
policies do not appear in the wire snapshot.

This rule:

- Keeps the effective policy independent of apiserver list ordering.
- Is observable from `kubectl get cachepolicies`, so an operator can
  always predict which CR is in effect.
- Is enforced by the controller, not the CRD: an admission webhook
  enforcing one policy per namespace (singleton) would let us drop this
  rule, and is a candidate for a future webhook addition.

## Tenant mapping (phase-1)

A `CachePolicy` lives in a namespace; a `LookupRoute` carries a
`tenant_id`. Phase-1 treats them as equivalent: a policy in namespace
`team-a` applies to lookups with `tenant_id = "team-a"`.

`CacheTenant` introduces explicit tenant identifiers (`spec.tenantID`)
separate from Kubernetes namespaces. Tenant **quotas** are propagated on
the same `/policy` snapshot but keyed by `tenantID` (the value an ingest
carries in `CacheStateUpdate.tenant_id`), a different axis from the
namespace key `CachePolicy` uses — see the tenant-quota row below.

## Enforcement (what the server does with each field)

| Field | Where it lands |
|---|---|
| `evictionTTL` | `pkg/index` `TTLResolver` — per-tenant `freshness()` decay + `evictExpired()` cutoff. |
| `minimumPrefixTokens` | Pre-lookup gate on `LookupRouteRequest.prefix_token_count`. A request shorter than the threshold short-circuits to `NO_HINT` without touching the index. Matches the CRD's "minimum prefix token count before lookup" semantics. |
| `lookupTimeoutMs` | `LookupRoute` derives a `context.WithTimeout`. A breach yields `reason_code: TIMEOUT` (still fail-open: empty scores). |
| `CacheTenant.spec.quota.maxIndexEntries` | `pkg/index` `TenantQuotaResolver`. Pushed as a `ResolvedTenant{tenantID, maxIndexEntries, isolationMode}` slice alongside the policies. At ingest, if the tenant's distinct-prefix count exceeds the budget, the index evicts that tenant's oldest prefixes (Fairness) down to budget. Fail-open when no `CacheTenant` matches the ingest's `tenant_id`. |

`failOpen` and `tenantScoped` are part of the CRD but not enforced by
this propagation path: the server is already fail-open by construction
(no error on the hot path), and `tenantScoped` is reserved for future
multi-tenant lookup scoping.

The `/policy` snapshot carries both `[]ResolvedPolicy` (keyed by namespace)
and `[]ResolvedTenant` (keyed by `tenantID`). A single controller reconciler
watches **both** `CachePolicy` and `CacheTenant` and pushes one combined
snapshot — two reconcilers would race on the replace-on-write store. A
`CacheTenant` that disappears between snapshots reverts that tenant to
unbounded (no enforcement).

## Failure modes

- **Server unreachable.** Controller logs and returns a reconcile error;
  controller-runtime backs off. The periodic tick keeps retrying until
  the server is back.
- **Server returns non-2xx.** Same — reconcile error + retry.
- **CRD not installed.** `list CachePolicies` returns `IsNotFound`; the
  controller treats it as "nothing to push" rather than logging an error.
  This keeps the initial startup quiet in a half-installed cluster.
- **Server restart.** The server starts with an empty store (server
  defaults everywhere). The next periodic tick re-pushes the full
  snapshot; in steady state this is ≤ 30s.

## Versioning and forward-compat

The wire schema's `version` is integer-valued and explicit. New fields
that the server can ignore safely (additive, non-load-bearing) ship at
the same `version`; load-bearing or semantically breaking changes bump
`version` and gate decode on the new value. The controller pushes the
constant in `pkg/server.PolicyPropagationVersion` on every request.

`version` is `2`: it was bumped from `1` when the `tenants` slice was
added. The server decodes with `DisallowUnknownFields`, so a stale (v1)
controller's push is rejected with a clear "unsupported version" error
rather than silently dropping the tenants; controller and server roll
out together and the periodic re-push reconciles any transient skew.

## Out of scope

- Webhook validation of CRD fields (admission) — see the CRD admission
  webhook work.
- Per-tenant **memory** budgets — out of scope by design (engine KV
  memory is tenant-unaware; see [policy-crds.md](policy-crds.md)). Only
  the index entry-count quota (`maxIndexEntries`) is enforced.
- `LookupRoute` ranking v2 (pressure / SLO scoring, `TENANT_HOT`
  fallback) — that strategy work consumes the same policy store but
  layers on top of the threshold/deadline enforcement shipped here.
- Endpoint hardening (NetworkPolicy, authn) for `/snapshot` and
  `/policy` — tracked separately.
