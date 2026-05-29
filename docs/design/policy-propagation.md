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

The controller always sends a FULL snapshot. The server adopts
**replace-on-write**: the snapshot becomes the new state, and any
namespace not present reverts to server defaults. A CR delete therefore
propagates as "next snapshot omits this namespace."

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

## Tenant mapping (phase-1)

A `CachePolicy` lives in a namespace; a `LookupRoute` carries a
`tenant_id`. Phase-1 treats them as equivalent: a policy in namespace
`team-a` applies to lookups with `tenant_id = "team-a"`.

The forthcoming `CacheTenant` CRD will eventually introduce explicit
tenant identifiers separate from Kubernetes namespaces; at that point
the controller can map `CacheTenant.spec.tenantID → namespace` in the
resolver step before pushing, without any schema change.

## Enforcement (what the server does with each field)

| Field | Where it lands |
|---|---|
| `evictionTTL` | `pkg/index` `TTLResolver` — per-tenant `freshness()` decay + `evictExpired()` cutoff. |
| `minimumPrefixTokens` | `LookupRoute` drops candidates below the threshold; if none survive, response is `NO_HINT`. |
| `lookupTimeoutMs` | `LookupRoute` derives a `context.WithTimeout`. A breach yields `reason_code: TIMEOUT` (still fail-open: empty scores). |

`failOpen` and `tenantScoped` are part of the CRD but not enforced by
this propagation path: the server is already fail-open by construction
(no error on the hot path), and `tenantScoped` is reserved for future
multi-tenant lookup scoping.

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

## Out of scope

- Webhook validation of CRD fields (admission) — see the CRD admission
  webhook work.
- Tenant-quota enforcement — that's the `CacheTenant` propagation path.
- `LookupRoute` ranking v2 (pressure / SLO scoring, `TENANT_HOT`
  fallback) — that strategy work consumes the same policy store but
  layers on top of the threshold/deadline enforcement shipped here.
- Endpoint hardening (NetworkPolicy, authn) for `/snapshot` and
  `/policy` — tracked separately.
