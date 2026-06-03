# CachePolicy — per-namespace tuning

`CachePolicy` is the opt-in knob for tuning cache lookup and eviction behavior in a
single namespace. The policy server ships with sane defaults (30m TTL, LRU eviction,
no prefix-length gate, no lookup deadline), so **most namespaces need no CachePolicy at
all** — you reach for one only when a specific namespace has a measured reason to deviate
(a hot-prefix workload, a latency SLO, an unusually short or long prefix distribution).

`CachePolicy` is **namespaced** (shortName `cpol`), and **at most one per namespace** — a
second one is rejected at admission. The CR is purely declarative: the controller pushes
its resolved fields to the server, which is where enforcement actually happens (see
[Propagation](#propagation-controller--server) below).

## The four spec knobs

| Field | Type | Default | When to tune |
|---|---|---|---|
| `eviction` | enum `LRU` \| `LFU` | `LRU` | Cap-based eviction algorithm — which entries get dropped when the index exceeds its entry cap. `LRU` drops oldest-by-`lastSeen`; `LFU` drops the lowest access-count entry (ties broken on oldest `lastSeen`). Switch to `LFU` when a few prefixes are hot and you want them to survive cap pressure. Access counts do **not** age. |
| `evictionTTL` | duration | server default `30m` | Maximum usable lifetime of a cache entry. The freshness sweep removes entries older than this regardless of eviction algorithm. Must be strictly positive when set (admission rejects `0`/negative). |
| `minimumPrefixTokens` | int32 (min `0`) | unset = no threshold | Minimum prefix token count before a lookup is attempted. A request shorter than this short-circuits to `NO_HINT` without touching the index — saves work when short prompts can't usefully hit the cache. |
| `lookupTimeoutMs` | int32 (min `0`) | unset = no deadline | Per-lookup latency budget in milliseconds. A breach returns reason code `TIMEOUT` (still fail-open — empty result, never an error to the gateway). See the foot-gun in [Gotchas](#two-gotchas). |

`status` carries only `observedGeneration` + `conditions`, and both are **reserved** — the
reconciler does not write `CachePolicy.status` today.

Printer columns (`kubectl get cpol`): `Eviction` (`.spec.eviction`) and `Age`.

## Example

A namespace with a hot-prefix workload (LFU) and a real latency SLO (20ms deadline),
declining lookups on prefixes shorter than 32 tokens:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CachePolicy
metadata:
  name: cachepolicy-sample
spec:
  eviction: LFU            # keep frequently-used prefixes under cap pressure
  evictionTTL: 30m         # drop entries older than 30m since last REPORT
  minimumPrefixTokens: 32  # short prompts short-circuit to NO_HINT
  lookupTimeoutMs: 20      # positive => a real 20ms deadline (NOT 0 — see Gotchas)
```

A copy-pasteable starting point ships at
[`config/samples/cache_v1alpha1_cachepolicy.yaml`](../../config/samples/cache_v1alpha1_cachepolicy.yaml)
(it omits `eviction` to exercise the `LRU` default).

## Two gotchas

These two behaviors are the high-value content — both are easy to get backwards.

### 1. `lookupTimeoutMs: 0` means UNBOUNDED, not "fail instantly"

Any value `<= 0` (including `0`) is treated by the server as **no deadline** — lookups run
without a latency budget. It does **not** mean "zero milliseconds, time out immediately."
If you set `lookupTimeoutMs: 0` expecting an instant timeout, you get the opposite: no
bound at all.

To actually bound lookup latency, set a **positive** value (the sample uses `20`). Leave
the field unset if you have no measured SLO — that is also "no deadline," and it is the
honest way to express it.

### 2. `evictionTTL` ages from `lastSeen`, and lookups do NOT refresh `lastSeen`

`evictionTTL` measures age from an entry's `lastSeen` timestamp, and **only ingest**
(`ReportCacheState` from an engine subscriber) advances `lastSeen`. Serving a hint does
not. A prefix that is looked up constantly but is no longer re-reported by any engine will
still expire at `evictionTTL` after its **last REPORT** — heavy lookup traffic does not
keep it warm in the index.

The only write the lookup path makes is an `LFU` access-count bump on a *delivered* hint
(a `TIMEOUT`'d lookup credits nothing). That bump feeds cap-eviction ordering only; it
never touches `lastSeen` and never affects TTL.

## Propagation (controller → server)

The controller watches every `CachePolicy` cluster-wide and **pushes** a full snapshot to
the server's internal `POST :8081/policy` endpoint on every reconcile plus a periodic tick
(~30s). The server adopts **replace-on-write**: deleting a `CachePolicy` reverts its
namespace to server defaults on the next snapshot. The server's policy store is in-memory
soft state, so the periodic re-push re-syncs a restarted server without operator action.

For the wire schema, auth posture, and the one-policy-per-namespace dedup backstop, see
[`docs/design/policy-propagation.md`](../design/policy-propagation.md).

## When NOT to use it

- **Empty or default-happy cluster.** Server defaults (TTL 30m, LRU, no token threshold,
  no lookup deadline) are deliberately sane. A fresh cluster needs no `CachePolicy`.
- **No measured SLO.** Don't set `lookupTimeoutMs` unless you have a real latency budget
  — and never set it to `0` expecting a timeout (see [Gotcha 1](#1-lookuptimeoutms-0-means-unbounded-not-fail-instantly)).
- **Per-tenant quotas.** `CachePolicy` tunes lookup/eviction behavior per namespace; it
  does not carry tenant index-entry budgets — that's `CacheTenant`'s
  `spec.quota.maxIndexEntries`.

## See also

- [`docs/design/crd-contract.md`](../design/crd-contract.md) — CRD design rationale and
  the cross-CRD invariants (status surface, reconciler ownership, enforcement boundary).
- [`docs/design/policy-propagation.md`](../design/policy-propagation.md) — how the
  controller pushes the snapshot and how the server applies each field.
- [`docs/design/policy-crds.md`](../design/policy-crds.md) — design rationale for the
  policy CRD shape and the enforcement boundary.
