# LookupRoute ranking strategies

How the cache plane answers the gateway's "where should I send this request?"
question — what the score formula means, which strategies layer on top, and
when each one fires. This is the *design* explanation; the
[`reason-codes.md`](../reference/reason-codes.md) reference is the wire-level
"what each code means today" table.

## 1. What `LookupRoute` is for

`LookupRoute` is the single gRPC the gateway calls on the hot path of every
inference request. It carries the prompt's prefix hash (or an ordered chain
of per-block hashes — see §2.5), the tenant/model the request is for, the
engine `hash_scheme`, and an optional `SLO` (TTFT / TBT budget). The response
is a list of ranked replica hints plus a `reason_code` that names which
ranking strategy produced them.

The cache plane never routes. It only describes cache state:

> **We hint, the gateway decides.**

The gateway is always free to use a hint or ignore it. Empty hints
(`NO_HINT`) are valid; they tell the gateway "fall back to your default
policy, the cache plane is invisible to this request." No `LookupRoute`
response is ever an error on the hot path. Fail open.

## 2. The baseline strategy — `PREFIX_MATCH`

The index keys cached prefixes by `(tenant, model, hash_scheme, prefix_hash)`
→ the set of replicas that hold it, with per-replica `token_count` and a
`last_seen` timestamp. A baseline lookup:

1. Look up the request's `(tenant, model, hash_scheme, prefix_hash)`.
2. For each replica that holds it, compute

   ```
   score = matched_tokens × freshness
   ```

   where `freshness = max(0, 1 − age / TTL)` — a linear decay from 1 (just
   reported) to 0 (older than the TTL).
3. Return them ranked best-first, ties broken by replica ID. `reason_code: PREFIX_MATCH`. (The handler currently returns every qualifying candidate — there is no top-K limit today; the gateway typically uses the top entry.)

The intuition: more matched tokens → bigger TTFT win from the prefix-cache
hit, and a fresher report is stronger evidence the replica still holds the
state. If no replica holds the prefix, the result is empty; the
accompanying `reason_code` is `NO_HINT` when the miss is a same-key novel
prefix (or when the cold-start / empty-key carve-outs apply), or one of
the diagnostic codes (`UNKNOWN_TENANT` / `UNKNOWN_MODEL` /
`UNKNOWN_HASH_SCHEME`) when the miss-classifier identifies a
contract-key mismatch — see
[`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md). Either way
the gateway falls back to its default routing policy.

This is the cache plane's job at its simplest. Everything else in this doc
*layers on top of this formula* — it never replaces it. When the new
strategies' inputs are absent, every factor reduces to 1 and the score
collapses back to `matched_tokens × freshness`.

## 2.5 Longest-prefix block-level matching — richer `PREFIX_MATCH`

### Why the exact-full-hash path leaves hits on the table

§2 looks up the request's `(tenant, model, hash_scheme, prefix_hash)` as
**one opaque blob**. Two requests that share the first N KV blocks of a
prefix but diverge after — common with a shared system prompt plus
per-request RAG context — hash to different `prefix_hash` values and miss
each other entirely. The replica that already holds 80% of the prefix
gets `NO_HINT` and the gateway routes blind.

vLLM and SGLang already chain their KV blocks: each block's hash is
computed from its parent's hash plus the block's tokens, so holding
`h_i` implies the engine computed `h_0..h_{i-1}` too. Expressing the
prefix as that **ordered chain of block hashes** lets the cache plane
match the longest common leading run instead of insisting on full
equality.

This is still `reason_code: PREFIX_MATCH` — same response shape, same
ranker factors (pressure, SLO, freshness) compose on top unchanged. The
only difference is *how* `matched_tokens` is computed.

### Contract shape (additive on `v1alpha1`)

Two new fields on `PrefixEntry` and `LookupRouteRequest` — paired
parallel arrays, same length, same order:

```
PrefixEntry {
  // ... existing legacy fields kept verbatim
  repeated bytes block_hashes = 3;          // one engine block-hash per element
  repeated int32 block_token_counts = 4;    // tokens covered by each block
}

LookupRouteRequest {
  // ... existing fields including the legacy prefix_hash
  repeated bytes block_hashes = 7;
  repeated int32 block_token_counts = 8;
}
```

Legacy `prefix_hash` / `token_count` stay in place at their original
field numbers; old engines and old gateway clients keep working
unchanged. `buf breaking --against main` is part of CI to catch any
slip into a non-additive change.

### How the lookup walks the chain

For each block `block_hashes[i]` the request carries, the index already
holds a `(tenant, model, hash_scheme, block_hash)` → `{replicaID →
{tokenCount, lastSeen}}` entry — populated either from a single chain
`PrefixEntry` (the index *expands* the chain into N per-block entries)
or from N legacy single-blob `PrefixEntry` reports (one per block, the
shape the vLLM subscriber already emits). The chain walk is one
intersection per block:

1. **At block 0**, seed a running set with every replica that holds
   `block_hashes[0]` (filtered against TTL — stale blocks are skipped).
   Each entry records `matched_tokens = block_token_counts[0]` and
   `oldest_last_seen = entry.lastSeen`.
2. **At block i > 0**, intersect the running set with the holders of
   `block_hashes[i]`. Replicas that fall out had their match end at
   block i−1 — freeze their score into a finalized map. Replicas that
   stay update `matched_tokens += block_token_counts[i]` and pull
   `oldest_last_seen` down to `min(running, entry.lastSeen)`.
3. When the running set is empty, stop walking; the rest of the chain
   would yield no further matches.
4. Replicas still running at the end of the chain matched the full
   request chain.
5. Score each finalized replica:

   ```
   freshness        = freshnessAt(now, oldest_last_seen, ttl_for_tenant)
   score (chain)    = matched_tokens × freshness × pressure_factor × slo_bias
   ```

   The pressure and SLO factors from §3–§5 layer on identically — the
   chain walk only changes how the per-replica `matched_tokens` and
   `freshness` inputs get derived.

A few details that matter for correctness:

- **Freshness is the weakest link.** A replica fresh on blocks 0–3 but
  stale on block 4 cannot ride its early blocks' freshness; the run's
  `oldest_last_seen` reflects the staler block. Stops a partially-aged
  hold from looking better than it really is.
- **Dropping out is final.** Once a replica fails to hold `block_hashes[i]`,
  it can't re-enter at block i+1 — leading-run semantics, not "any
  contiguous run of matches."
- **Seeding is at block 0 only.** A replica that holds `block_hashes[5]`
  of some unrelated chain (because the bytes happen to collide) doesn't
  show up as a partial match against this request. The leading-run rule
  is enforced by where we initialize, not by separate position tracking.
- **`reason_code` is unchanged on the match side.** Any non-empty match
  — one block or the whole chain — is `PREFIX_MATCH`. On a miss, the
  request goes through the same miss-classifier as the exact-match
  path: a same-key first-block miss yields `NO_HINT`; a contract-key
  mismatch yields the matching `UNKNOWN_*` code (see
  [`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md)). The
  engine-opaque guards (empty `hash_scheme`, mismatched parallel-array
  lengths) and the cold-start carve-out continue to surface as
  `NO_HINT`.

### Worked example — 5 blocks, 3 deep

Request asks for the chain `[h1, h2, h3, h4, h5]` with per-block counts
`[16, 16, 16, 16, 16]`. Index state for the requested
`(tenant, model, scheme)`:

| Block hash | Holders → `{tokenCount, lastSeen age}`                                   |
|------------|---------------------------------------------------------------------------|
| `h1`       | `replica-A: {16, 1m}, replica-B: {16, 2m}, replica-C: {16, 4m}`           |
| `h2`       | `replica-A: {32, 1m}, replica-B: {32, 2m}`                                |
| `h3`       | `replica-A: {48, 1m}`                                                     |
| `h4`       | *(nobody)*                                                                |
| `h5`       | *(nobody)*                                                                |

Walking the chain:

| Step      | Running set after this block                       | Finalized                                  |
|-----------|----------------------------------------------------|--------------------------------------------|
| seed `h1` | `A: {tok=16, oldest=1m}, B: {tok=16, oldest=2m}, C: {tok=16, oldest=4m}` | —                                          |
| at `h2`   | `A: {tok=32, oldest=1m}, B: {tok=32, oldest=2m}`   | `C: {tok=16, oldest=4m}` *(dropped at h2)* |
| at `h3`   | `A: {tok=48, oldest=1m}`                           | `+ B: {tok=32, oldest=2m}` *(dropped at h3)*|
| at `h4`   | *(empty — A doesn't hold h4)* → stop walking       | `+ A: {tok=48, oldest=1m}`                 |

Scoring each finalized replica (no pressure/SLO, TTL = 30 min, so
`freshness = 1 − age/30m`):

| Replica | matched_tokens | freshness | score | rank |
|---------|----------------|-----------|-------|------|
| `A`     | 48             | 0.97      | 46.4  | 1    |
| `B`     | 32             | 0.93      | 29.8  | 2    |
| `C`     | 16             | 0.87      | 13.9  | 3    |

Response: `reason_code: PREFIX_MATCH`, scores `[A (46), B (30), C (14)]`
— the gateway routes to A for the deepest cache hit, with B as a hot
backup if A is overloaded.

### Backward-compat — what an unmigrated producer or client sees

- **Legacy producer, chain-aware client.** A producer that still emits
  one `PrefixEntry` per block (the existing vLLM subscriber) populates
  the per-block index keys directly. The chain lookup walks them
  end-to-end without any engine-side change. *Switching the producer
  to a single chain `PrefixEntry` is a future optimization, not a
  requirement for the value to land.*
- **Chain producer, legacy client.** A producer that sets both the
  chain and the legacy `prefix_hash` on the same `PrefixEntry` gets
  **both representations indexed** — the chain enables longest-prefix
  matching for new clients; the legacy single-blob key keeps
  unmigrated `LookupRoute` callers hitting via exact-match on
  `prefix_hash`. The chain wins precedence for chain-aware lookups;
  legacy is only added when the producer explicitly sets it alongside.
- **Chain producer, chain-aware client, missing per-block counts.**
  Mismatched parallel-array lengths (including the one-sided cases —
  hashes without counts or counts without hashes) are treated as
  malformed: dropped on ingest, `NO_HINT` on lookup. The handler does
  not silently downgrade to legacy exact-match — a stale hint is fine,
  a wrong hint is not.
- **Chain-only or chain-plus-legacy request with the `minimumPrefixTokens`
  gate active.** The handler uses `effectivePrefixTokens(req)`, which
  gives the chain precedence: when `block_token_counts` is set the
  gate uses `sum(block_token_counts)`; only when the chain is empty
  does it fall back to the legacy `prefix_token_count`. A chain-bearing
  request is therefore gated on what the chain actually reports — a
  co-set stale legacy count cannot let it through nor zero it out.

### Engine-opaque + parent-chain assumption

Block hashes are still engine-defined opaque bytes; the cache plane
matches them by exact byte equality within a `hash_scheme` and never
interprets them. The longest-leading-run rank is **only meaningful when
the engine's block hashes are parent-chained** — i.e. holding `h_i`
implies having computed `h_0..h_{i-1}`. vLLM and SGLang both satisfy
this. A future engine that emits position-blind block hashes (same
bytes for a "middle" block as for a "leading" block) would violate the
assumption and should not be ingested with the chain form. The contract
doesn't enforce that — it's a producer-side discipline.

## 3. Pressure-aware scoring — locality vs. load

Replicas also report `ReplicaStats.pressure`, a 0–1 saturation signal
(higher = more loaded). The baseline ignores it, so a token-rich but
saturated replica blindly wins over a fresher, idle peer. The hint then
just piles more traffic onto an already-overloaded server.

The pressure-aware factor demotes the score by load:

```
pressure_factor = max(0, 1 − PressureWeight × pressure)
score          = matched_tokens × freshness × pressure_factor
```

Worked example with `PressureWeight = 1`:

| Replica       | tokens | freshness | pressure | baseline score | new score |
|---|---|---|---|---|---|
| `big-but-hot` | 100    | 1.0       | 0.9      | 100            | **10**    |
| `small-cool`  |  50    | 1.0       | 0.0      |  50            | **50**    |

Under the baseline `big-but-hot` wins; with pressure folded in, the smaller
but idle peer overtakes it. **The bias is against load**, not toward it —
high pressure *lowers* the score.

`pressure = 0` (no stats reported, or genuinely idle) ⇒ factor `1.0` ⇒
baseline behavior. `PressureWeight = 0` disables the penalty entirely
(useful kill switch).

Two correctness rules apply to *which* pressure value the ranker reads:

- **Stats must come with a fresh payload.** A `REPLICA_UPDATED` liveness
  event refreshes "I'm alive" without supplying new stat numbers. The
  ranker tracks a separate `statsReported` timestamp that *only* a real
  `ReportCacheState` ingest with stats payloads updates, so a stale
  high-pressure reading kept artificially "alive" by liveness events can't
  keep demoting a perfectly fresh prefix score.
- **Stats must be within the global TTL.** Otherwise we'd be applying a
  pressure reading from before the sweeper had a chance to clear it.

## 4. `TENANT_HOT` fallback — soft locality on a prefix miss

The baseline gives up with `NO_HINT` whenever no replica has the *exact*
prefix cached. But that's often pessimistic. Even without an exact
overlap, the gateway can often still benefit from sending the request to
a replica that's already serving cache hits for this tenant: its KV
allocator is warm, its tokenizer is loaded, its block manager already
holds related state, and its tenant context is hot.

When the prefix-match path returns empty, the ranker runs a second
strategy:

1. Find replicas under `(tenant, model)` whose stats are recent
   (`statsReported` within `TenantHotMaxAge`, default 5 min) AND whose
   `hit_rate` is at least a floor (default `0.1`). These are "warm."
2. Restrict to replicas that *actually serve* the requested
   `hash_scheme` — i.e. they hold at least one prefix entry in the
   request's engine domain. Without this guard, a stats-only update with
   an unrelated `hash_scheme` could leak into a hint for a different
   engine.
3. Score them as

   ```
   score = hit_rate × recency × pressure_factor × slo_bias
   ```

   where `recency = 1 − age / TenantHotMaxAge`. There's no prefix overlap,
   so `MatchedTokens = 0` on each returned `ReplicaScore`. **The gateway
   must rely on `reason_code`, not `matched_tokens`, to recognize this
   branch.**
4. Return them with `reason_code: TENANT_HOT`. If nothing qualifies, the
   miss-classifier runs (see [`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md)):
   a same-key novel-prefix miss falls through to `NO_HINT`, but a
   mismatched contract key (`tenant_id`, `model_id`, or `hash_scheme`)
   surfaces as the matching `UNKNOWN_*` code.

This is a deliberately *softer* hint than `PREFIX_MATCH`. The gateway is
free to honor it or fall back to its default policy. Setting
`TenantHotMaxAge = 0` disables the fallback entirely.

A few subtleties worth knowing:

- The "warm" check on a candidate replica uses an O(1) secondary index
  keyed by `(tenant, model, hash_scheme)` — no full scan of the
  cache-state map per lookup, even when the index is large.
- A prefix entry that's been swept (passed the global TTL) decrements
  that secondary index in lockstep, so a long-stale replica naturally
  drops out of the `TENANT_HOT` candidate set.

## 5. SLO-aware bias — freshness matters more when latency is tight

### SLO is a field on `LookupRouteRequest`, not a separate RPC

Worth pinning before the formulas: **there is no "SLO" RPC.** `SLO` is a
nested message tucked into the existing `LookupRouteRequest`:

```
LookupRouteRequest {
  string model_id;
  string tenant_id;
  bytes  prefix_hash;
  int32  prefix_token_count;
  string hash_scheme;
  SLO    slo;            // ← TTFT / TBT budget the gateway is targeting
}

SLO {
  int32 ttft_ms;         // target time-to-first-token (ms)
  int32 tbt_ms;          // target time-between-tokens (ms)
}
```

It has been on the wire since the contract was first defined; what changes
in this layer is that the server now **reads** the field instead of
ignoring it. There is still one RPC, one response envelope, and the same
reason-code vocabulary. SLO is a knob that reshapes the existing
`PREFIX_MATCH` and `TENANT_HOT` ranking — not a new strategy.

It is also **not enforcement**: the cache plane does not refuse to answer
when SLO is tight, does not time the response out against `ttft_ms`, and
does not promise to meet the SLO. It uses the budget purely as a hint
about what the gateway cares about. "We hint, the gateway decides" still
holds.

### The bias

Under a tight TTFT budget, the cost of routing to a stale replica (it
still has to rebuild context) is higher relative to the cost of slightly
less prefix overlap. So when TTFT is tight, freshness should weigh more
than matched-token count.

The bias is a single multiplicative factor on top of everything else:

```
slo_bias = 1 + freshness × SLOTightBias    if ttft_ms < SLOTightTTFTMs
         = 1                                otherwise
```

For example with `SLOTightBias = 1`, a perfectly fresh candidate
(`freshness = 1`) gets a 2× boost; a stale one (`freshness = 0.1`) only
1.1×. Effect: under tight SLO, fresher candidates pull ahead of
token-richer-but-staler ones more aggressively.

The same factor composes into both shipped strategies — it multiplies
into the `PREFIX_MATCH` score and into the `TENANT_HOT` fallback score
identically. There is no "SLO strategy" or "SLO reason code"; a tight-SLO
response still comes back as `PREFIX_MATCH` (or `TENANT_HOT`, or
`NO_HINT`), just with a different internal ranking.

`SLOTightBias = 0` disables the bias; an unset (zero) TTFT budget skips
it; a TTFT budget above the threshold skips it. So baseline behavior is
preserved when no SLO is supplied.

### `tbt_ms` — plumbed but not yet used

`SLO.tbt_ms` (time-between-tokens budget) is threaded all the way through
from the proto request into the index's `LookupRequest.TBTBudgetMs`, but
the current scoring formula does not reference it. It is a placeholder
for a future tuning hook — e.g. a tight TBT budget might bias toward
replicas with low pressure so the decode loop isn't queued behind other
work. Wiring is in place; the factor is currently a no-op.

### Where the SLO budget comes from

`SLO` is **set by the gateway, per request** — there is no `SLO` field
on any cache-plane CRD. The cache plane consumes whatever budget the
gateway sends in `LookupRouteRequest.slo` and uses it to rank the
response. How the gateway derives that budget is its own decision:

- From the inference request itself (e.g. a header or parameter on the
  inbound API call).
- From the gateway's own per-tenant or per-model routing policy.
- From a global deployment configuration.

A request with no SLO message (or `ttft_ms = 0`) is treated as
"unspecified" and the SLO bias factor collapses to `1` — the cache plane
never invents a budget on its own.

> **Not the same as `CachePolicy.lookupTimeoutMs`.** That field (in the
> `CachePolicy` CRD, set per tenant by a cluster operator) is the budget
> the **server** spends on its own lookup before giving up — "if I can't
> rank in N milliseconds, answer `TIMEOUT`." It governs the server's
> internal time. The `SLO.ttft_ms` on `LookupRouteRequest` is the
> **gateway's end-to-end TTFT target for the inference request itself**
> and is used purely as a *ranking signal*, never as enforcement. Two
> different things that both sound like "SLO."

## 6. Putting the factors together

A single score expression covers both shipped scoring paths:

```
score (PREFIX_MATCH path) = matched_tokens × freshness × pressure_factor × slo_bias
score (TENANT_HOT  path) = hit_rate       × recency    × pressure_factor × slo_bias
```

with

```
pressure_factor = max(0, 1 − PressureWeight × pressure)
slo_bias        = 1 + decay × SLOTightBias           (when TTFT is tight)
                = 1                                  (otherwise)

where `decay` is:
  - `freshness` in the PREFIX_MATCH path (prefix entry's lastSeen vs. TTL)
  - `recency`   in the TENANT_HOT  path (stats entry's statsReported vs. TenantHotMaxAge)
```

And the strategies compose into a single orchestrator:

```
LookupRoute(req):
   if hash_scheme is empty            → NO_HINT             (engine domain unknown — fail open)
   if there is an exact prefix match  → PREFIX_MATCH        (ranked by the full score)
   else if any tenant-warm replicas   → TENANT_HOT          (ranked by the full score, soft hint)
   else (miss-classifier runs):
       if index is globally empty     → NO_HINT             (cold start — no data to compare against)
       if tenant_id not in index      → UNKNOWN_TENANT      (configuration drift)
       if (tenant, model) not in index → UNKNOWN_MODEL       (configuration drift)
       if scope not in index          → UNKNOWN_HASH_SCHEME (configuration drift)
       else                            → NO_HINT             (genuine novel prefix — fail open)
```

See [`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md) for the
full design behind the `UNKNOWN_*` codes.

Every factor and threshold is tunable through a `RankerConfig`. Defaults
are set so that:

- A deployment with **no stats reported** behaves identically to the
  pre-PR baseline (pressure factor 1, no `TENANT_HOT` candidates qualify).
- A request with **no SLO hint** sees no freshness boost (slo bias 1).
- Setting `PressureWeight = 0`, `SLOTightBias = 0`, and
  `TenantHotMaxAge = 0` simultaneously is equivalent to the original B6
  `matched_tokens × freshness` ranker.

## 7. The reason-code summary

| Code                  | When it fires                                                                                                                                                                                                                       | What the gateway treats it as           |
|---|---|---|
| `PREFIX_MATCH`        | At least one replica holds the exact prefix — or the leading block-hash run (§2.5) — in the requested engine domain                                                                                                                  | Strongest hint — route to top-ranked    |
| `TENANT_HOT`          | Prefix miss, but the tenant has recently warm replicas serving this `hash_scheme`                                                                                                                                                    | Softer hint — use or fall back          |
| `NO_HINT`             | Empty `hash_scheme`, malformed chain, no prefix match with no diagnosable contract-key mismatch (a genuine novel prefix or a cold-start window where the index holds no data yet), or any other unspecified outcome                  | Default routing; cache plane invisible  |
| `UNKNOWN_TENANT`      | Prefix miss + `TENANT_HOT` miss + the supplied `tenant_id` has zero prefix entries while some other tenant in the index does                                                                                                         | Likely SDK/producer mismatch — fail-open; surface as configuration error |
| `UNKNOWN_MODEL`       | Prefix miss + `TENANT_HOT` miss + the tenant has entries but the requested `(tenant, model)` does not                                                                                                                                | Same — fail-open; configuration error   |
| `UNKNOWN_HASH_SCHEME` | Prefix miss + `TENANT_HOT` miss + `(tenant, model)` has entries but the requested `hash_scheme` is absent for it                                                                                                                     | Same — fail-open; configuration error   |

`TIMEOUT` is reserved in the contract vocabulary for a per-tenant lookup
deadline breach and is handled by the policy-server propagation path — not
by this ranking layer. See
[`lookuproute-diagnostics.md`](./lookuproute-diagnostics.md) for the rule
behind the `UNKNOWN_*` codes (including the empty-key and cold-start
carve-outs that keep them on `NO_HINT`).

## 8. Worked examples

Six concrete scenarios that exercise the strategies in §2–5 end-to-end.
Each shows the relevant index state, the request, the score
computation, and the response. All examples use the default
`RankerConfig`: `PressureWeight = 1`, `SLOTightTTFTMs = 200 ms`,
`SLOTightBias = 1`, `TenantHotMaxAge = 5 min`,
`TenantHotMinHitRate = 0.1`.

### 8.1. Baseline: one replica holds the prefix

Index state — tenant `team-a`, model `m`, scheme `vllm`:

| Replica | Prefix | Tokens | Freshness | Pressure |
|---|---|---|---|---|
| `r1`    | `p`    | 100    | 1.0       | 0.0      |

Request: `{tenant=team-a, model=m, hash_scheme=vllm, prefix_hash=p}`, no SLO.

Computation: `100 × 1.0 × (1 − 1 × 0.0) × 1 = 100`.

Response: `reason_code=PREFIX_MATCH`, scores `[{r1, score=100, matched_tokens=100}]`.

This is the exact pre-PR baseline: with no stats and no SLO, every new
factor collapses to 1 and the score is `matched_tokens × freshness`.

### 8.2. Pressure breaks a tie the baseline didn't see

Index state:

| Replica       | Prefix | Tokens | Freshness | Pressure |
|---|---|---|---|---|
| `big-but-hot` | `p`    | 100    | 1.0       | 0.9      |
| `small-cool`  | `p`    |  50    | 1.0       | 0.0      |

Request: same as §8.1, no SLO.

Computation:
- `big-but-hot`: `100 × 1.0 × (1 − 1 × 0.9) × 1 = 100 × 0.1 = 10`
- `small-cool`:  `50 × 1.0 × (1 − 1 × 0.0) × 1 = 50`

Response: `PREFIX_MATCH`, ranked `[small-cool (50), big-but-hot (10)]`.

The pure baseline (`tokens × freshness`) would have given `big-but-hot`
a score of `100` vs `small-cool`'s `50` and routed traffic to the
already-saturated replica. The pressure factor flips it: locality
weighed against load.

### 8.3. Tight TTFT bias reshapes ordering

Index state:

| Replica       | Prefix | Tokens | Freshness | Pressure |
|---|---|---|---|---|
| `big-old`     | `p`    | 100    | 0.6       | 0.0      |
| `small-fresh` | `p`    |  50    | 1.0       | 0.0      |

**Request A — no SLO**:
- `big-old`:     `100 × 0.6 × 1 × 1 = 60`
- `small-fresh`: `50 × 1.0 × 1 × 1 = 50`
- Response: `PREFIX_MATCH`, `[big-old (60), small-fresh (50)]`.

**Request B — `slo.ttft_ms = 50`** (below the 200 ms threshold → tight):
- `big-old`:     `100 × 0.6 × 1 × (1 + 0.6 × 1) = 100 × 0.6 × 1.6 = 96`
- `small-fresh`: `50 × 1.0 × 1 × (1 + 1.0 × 1) = 50 × 1.0 × 2.0 = 100`
- Response: `PREFIX_MATCH`, `[small-fresh (100), big-old (96)]`.

Under loose latency the token-rich older replica wins; under a tight
TTFT budget the fresher one does. Both responses still say
`PREFIX_MATCH` — the shape doesn't change, the internal ranking does.

### 8.4. Prefix miss + warm tenant replica → `TENANT_HOT`

Index state (no replica holds the *requested* prefix; one replica is
"warm" for the tenant in the requested engine domain):

| Replica  | Scheme | Prefix held | Hit rate | Pressure | Stats age |
|---|---|---|---|---|---|
| `r-warm` | vllm   | `other`     | 0.8      | 0.1      | 30 s      |

Request: `{tenant=team-a, model=m, hash_scheme=vllm, prefix_hash=novel}`,
no SLO.

Prefix-match path: empty (no replica holds `novel`).

Tenant-hot fallback (defaults `TenantHotMaxAge = 5 min`,
`TenantHotMinHitRate = 0.1`):
- `r-warm` reported 30 s ago (well under 5 min), `hit_rate = 0.8 ≥ 0.1`,
  and it holds at least one prefix in `vllm` (`other`) — qualifies.
- `recency = 1 − 30 s / 5 min = 0.9`
- `pressure_factor = 1 − 1 × 0.1 = 0.9`
- `slo_bias = 1`
- `score = 0.8 × 0.9 × 0.9 × 1 = 0.648`

Response: `TENANT_HOT`, scores `[{r-warm, score=0.648, matched_tokens=0}]`.

`matched_tokens` is `0` because there's no prefix overlap — the gateway
must rely on `reason_code` (not `matched_tokens`) to tell `TENANT_HOT`
apart from a real prefix hit.

### 8.5. Engine-domain guard rejects a warm replica from a different scheme

Index state — both replicas warm, but for different engines:

| Replica   | Scheme | Prefix held | Hit rate | Pressure | Stats age |
|---|---|---|---|---|---|
| `r-vllm`  | vllm   | `p1`        | 0.7      | 0.0      | 1 min     |
| `r-sgl`   | sglang | `p2`        | 0.9      | 0.0      | 1 min     |

Request: `{model=m, hash_scheme=vllm, prefix_hash=novel}`, no SLO.

Prefix-match path: empty.

Tenant-hot fallback:
- `r-vllm`: warm AND holds at least one prefix under `vllm` → qualifies.
- `r-sgl`:  warm AND has a higher hit rate, but its only prefix entry
  is in `sglang` — it does not serve the requested engine domain.
  **Rejected**: hinting to it would route across engines, which the
  contract forbids.

Response: `TENANT_HOT`, scores only `[{r-vllm, ...}]`. `r-sgl` does not
appear.

This is the bug the engine-domain guard prevents: stats are
scheme-independent in the index (a `ReplicaStats` applies across
engines), so without this check a stats-only update — or an update
under a different scheme — could leak into a hint for the wrong engine.

### 8.6. Empty `hash_scheme` → unconditional `NO_HINT`

Index state: any.

Request: `{model=m, hash_scheme="", prefix_hash=<any bytes>}`.

The cache plane can't safely match an opaque `prefix_hash` without
knowing which engine domain it belongs to (a vLLM hash and an SGLang
hash of the same bytes are unrelated). Both strategies short-circuit
on the empty `hash_scheme` and the response is unconditionally
`NO_HINT` with empty scores — fail open.

This is also the response shape whenever the prefix-match path is empty
AND no replica qualifies for `TENANT_HOT`. The gateway treats all three
"no useful hint" outcomes identically: route per its default policy.

## 9. Where the code lives

- Scoring + strategy orchestration: [`pkg/index/index.go`](../../pkg/index/index.go)
  — see `Lookup`, `lookupExact`, `lookupChain` (§2.5 chain walk),
  `LookupRoute`, `tenantHotCandidates`, `RankerConfig`.
- Handler glue (proto ↔ index, `Strategy` → `reason_code`):
  [`pkg/server/inferencecache_service.go`](../../pkg/server/inferencecache_service.go).
- Tests covering each strategy and the baseline-preservation invariant:
  [`pkg/index/index_test.go`](../../pkg/index/index_test.go) and
  [`pkg/server/server_test.go`](../../pkg/server/server_test.go).
