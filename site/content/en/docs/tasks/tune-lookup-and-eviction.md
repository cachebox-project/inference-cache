---
title: "Tune lookup and eviction"
linkTitle: "Tune lookup and eviction"
weight: 3
description: >
  Worked CachePolicy examples for match floors, score floors, timeouts, and eviction.
---

Most namespaces need no `CachePolicy` — the server ships sane defaults. Reach for one when
you want to change how lookups are filtered/ranked, or how the index evicts, in a specific
namespace. There is **at most one `CachePolicy` per namespace**.

See [CachePolicy]({{< relref "/docs/concepts/cachepolicy/" >}}) for the field-by-field reference; this page
is worked examples.

## A production-shaped policy

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CachePolicy
metadata:
  name: default
  namespace: serving
spec:
  eviction: LRU
  evictionTTL: 30m
  minimumPrefixTokens: 16      # skip lookups for very short prompts
  minimumMatchedTokens: 64     # a replica needs 4 KV blocks of overlap to be offered
  routingFloorScore: "0.1"     # the top replica must clear this score
  lookupTimeoutMs: 5           # server-side time budget per lookup (fail-open TIMEOUT)
  strategy:
    enableChainMatching: true
    enableTenantHot: true
  affinityRouting: Enabled
```

## Recipes

### Reduce noisy hints from shared system prompts

Chat templates give every request the same long system-prompt prefix, which would otherwise
match every replica. The default floors already handle this, but you can tighten them:

```yaml
spec:
  minimumMatchedTokens: 128    # require more unique overlap before offering a replica
  routingFloorScore: "0.2"     # demand a clearer winner
```

Raising `minimumMatchedTokens` filters replicas whose only overlap is the shared framing;
raising `routingFloorScore` rejects responses where no replica stands out.

### Turn off a filter entirely

Both floors opt out with a zero value:

```yaml
spec:
  minimumMatchedTokens: 0      # offer any non-empty overlap
  routingFloorScore: "0"       # never reject on score
```

### Keep hints fresher (or hold them longer)

`evictionTTL` controls how long an entry survives after it was last *seen* (lookups do not
refresh it). Shorten it for fast-moving workloads, lengthen it for stable ones:

```yaml
spec:
  evictionTTL: 10m             # aggressive: hints go stale faster, index stays smaller
```

Shortening `evictionTTL` is also the cheapest lever when the index is over its memory budget
— see [Index sizing]({{< relref "/docs/administration/index-sizing/" >}}).

### Require a block-hash chain

If your gateway always supplies a proper block-hash chain and you want to reject anything
else outright:

```yaml
spec:
  strategy:
    requireChain: true         # a request without a valid chain returns POLICY_REQUIRES_CHAIN
```

### Pin diffuse single-turn traffic

For workloads with little prefix reuse, affinity routing still helps by pinning a prompt to
a stable replica (consistent hash), improving the *engine's own* prefix cache hit rate:

```yaml
spec:
  affinityRouting: Enabled     # (this is the default) yields AFFINITY_HINT instead of NO_HINT
```

Set it to `Disabled` to force `NO_HINT` (pure round-robin) when there is no real match.

## Gotchas

- **`lookupTimeoutMs: 0` means *unbounded*, not instant.** Leave it unset (or positive) for a
  real budget.
- **`evictionTTL` must be strictly positive** — `0` or negative is rejected at admission.
- **A second `CachePolicy` in the namespace is rejected** at admission; if two somehow
  exist, the lexicographically-smallest name wins deterministically.

## Related pages

- [CachePolicy]({{< relref "/docs/concepts/cachepolicy/" >}}) — every field.
- [LookupRoute & ranking]({{< relref "/docs/concepts/lookuproute/" >}}) — how these knobs feed the ranker.
- [Reason codes]({{< relref "/docs/reference/reason-codes/" >}}) — what each outcome means.
