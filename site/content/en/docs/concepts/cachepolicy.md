---
title: "CachePolicy"
linkTitle: "CachePolicy"
weight: 3
description: >
  Opt-in, per-namespace tuning of cache lookup and eviction.
---

## What is a CachePolicy?

A `CachePolicy` tunes how the server filters and ranks lookups, and how it evicts, for a
single namespace. It is **entirely opt-in**: the server ships sane defaults, so most
namespaces need no `CachePolicy` at all. It is **purely declarative** — the controller
flattens all policies and pushes the resolved values to the server; the reconciler never
writes `CachePolicy.status`.

`CachePolicy` is namespaced, short name `cpol`. **At most one per namespace** — a second is
rejected at admission (best-effort), and the controller's deterministic dedup
(lexicographically-smallest name wins) is the authoritative backstop.

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CachePolicy
metadata:
  name: default
  namespace: serving
spec:
  eviction: LRU
  evictionTTL: 30m
  minimumMatchedTokens: 64
  routingFloorScore: "0.1"
  rankerOverrides:
    pressureWeight: 1
    tenantHotMaxAge: 5m
  strategy:
    enableChainMatching: true
    enableTenantHot: true
  affinityRouting: Enabled
```

## The lookup-filter knobs

Three orthogonal filters act at different stages of a lookup:

| Field | Default | Stage | Effect |
|---|---|---|---|
| `minimumPrefixTokens` | unset (no gate) | **Request-side, pre-lookup** | Skip the lookup entirely if the request's prefix is shorter than this many tokens. |
| `minimumMatchedTokens` | **64** (= 4 KV blocks) | **Result-side, per-replica** | A replica must have at least this many matched tokens to be offered as a `PREFIX_MATCH`. `0` opts out. |
| `routingFloorScore` | **`"0.1"`** | **Result-side, per-response** | The top-ranked replica's score must clear this floor, or the response degrades to no hint. Stringified float; `"0"` opts out; negatives clamp to 0. |

The `minimumMatchedTokens` floor exists because chat templates frame every prompt with a
shared system-prompt prefix; without a floor, that shared framing would match every replica
and produce a useless hint. The `routingFloorScore` gate catches the mirror case — a prefix
held by *every* replica has zero distinguishing power (see
[LookupRoute & ranking]({{< relref "/docs/concepts/lookuproute/" >}}).

## Ranker overrides

`spec.rankerOverrides` optionally tunes ranking-v2 for one namespace. Every nested field
is optional and inherits the server baseline when omitted; an explicit zero keeps its
kill-switch meaning.

| Field | Range | Baseline | Zero behavior |
|---|---|---|---|
| `pressureWeight` | `0..4` | `1.0` | disables pressure penalty |
| `sloTightTTFTMs` | `>=0` | `200` | SLO bias never fires |
| `sloTightBias` | `0..8` | `1.0` | disables freshness boost |
| `tenantHotMinHitRate` | `0..1` | `0.1` | every non-negative hit rate qualifies |
| `tenantHotMaxAge` | duration `>=0` | `5m` | disables `TENANT_HOT` |

## Eviction

| Field | Default | Meaning |
|---|---|---|
| `eviction` | `LRU` | Cap-based eviction ordering — `LRU` or `LFU`. |
| `evictionTTL` | server default 30m | An entry ages out this long after it was last *seen* (reported). Lookups do **not** refresh `lastSeen`. Must be strictly positive when set. |

{{% alert title="Gotcha — lookupTimeoutMs" color="warning" %}}
`lookupTimeoutMs` bounds how long the server spends on a single lookup before returning
`TIMEOUT` (fail-open). A value of **0 or less means *unbounded*, not instant** — do not set
it to 0 expecting immediate timeouts.
{{% /alert %}}

## Strategy gates

`spec.strategy` gates the ranking strategies:

| Field | Default | Effect |
|---|---|---|
| `enableChainMatching` | `true` | Enable longest-prefix block-chain matching. |
| `requireChain` | `false` | If `true`, a request without a valid block-hash chain returns `POLICY_REQUIRES_CHAIN` immediately (before touching the index). |
| `enableTenantHot` | `true` | Enable the `TENANT_HOT` fallback on a prefix miss. |

`spec.affinityRouting` (`Enabled` by default, `Disabled` to turn off) decides the final
fallback: when the ranker would otherwise return `NO_HINT`, affinity routing returns a
single stable replica chosen by a consistent hash — useful for diffuse single-turn
workloads. See [reason codes]({{< relref "/docs/reference/reason-codes/" >}}).

## Status

`status.conditions` and `status.observedGeneration` exist but are **reserved** — the
reconciler does not write them today. The policy takes effect by being pushed to the server,
not through a status handshake.

## Related pages

- [LookupRoute & ranking]({{< relref "/docs/concepts/lookuproute/" >}}) — how these knobs feed the ranker.
- [Tune lookup and eviction]({{< relref "/docs/tasks/tune-lookup-and-eviction/" >}}) — worked examples.
- [Index sizing]({{< relref "/docs/administration/index-sizing/" >}}) — how `evictionTTL` and `eviction`
  affect memory.
