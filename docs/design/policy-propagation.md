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

`/policy` sits on the server's open HTTP port (`:8080`), alongside
`/healthz`, `/readyz`, `/metrics`. `/snapshot` is on a separate internal
listener (`:8081`) gated by TokenReview-backed bearer auth and a
NetworkPolicy that restricts ingress to the controller's pod selector —
both endpoints are still internal but the snapshot path now has two
independent gates (L7 authn + L3/L4 isolation), because it leaks
per-tenant cache metadata under the cluster's trust boundary while
`/policy` is a push the controller authors anyway.

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

The forthcoming `CacheTenant` CRD will eventually introduce explicit
tenant identifiers separate from Kubernetes namespaces; at that point
the controller can map `CacheTenant.spec.tenantID → namespace` in the
resolver step before pushing, without any schema change.

## Enforcement (what the server does with each field)

| Field | Where it lands |
|---|---|
| `evictionTTL` | `pkg/index` `TTLResolver` — per-tenant `freshness()` decay + `evictExpired()` cutoff. |
| `minimumPrefixTokens` | Pre-lookup gate on `LookupRouteRequest.prefix_token_count`. A request shorter than the threshold short-circuits to `NO_HINT` without touching the index. Matches the CRD's "minimum prefix token count before lookup" semantics. |
| `lookupTimeoutMs` | `LookupRoute` derives a `context.WithTimeout`. A breach yields `reason_code: TIMEOUT` (still fail-open: empty scores). |

The server is fail-open by construction on the hot path (no error returned to the gateway); a `CachePolicy`-level fail-open knob is not part of the propagated wire format.

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
- Endpoint hardening for `/policy` — out of scope here; the controller
  is the only authorized writer and pushes over in-cluster cleartext.
  `/snapshot` hardening (bearer auth + NetworkPolicy) ships alongside
  this work (see the snapshot listener at `:8081` above).
