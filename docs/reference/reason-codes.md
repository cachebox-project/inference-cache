# Reason codes

A living reference of every `reason_code` string the cache plane emits on the
wire. The tech spec (§4.2) and the gRPC contract design doc
([../design/grpc-contract.md](../design/grpc-contract.md)) cover the *why*; this
file is the *what is exposed today*.

**Update this file in the same change that introduces or retires a code.** New
codes don't break old clients (see "Forward compatibility" below), but an
undocumented code in production is a real friction point for the gateway team
and for ops dashboards.

## Forward compatibility — the load-bearing rule

- `reason_code` is a **`string`, not a proto `enum`**. New codes are an additive
  server-side change with **no client recompile**.
- **Clients MUST treat any unrecognized code as the no-hint default** for that
  RPC (`NO_HINT` for `LookupRoute` / `LookupPDRoute`, `RENDER_ERROR` for
  `RenderTemplate`). This is what lets the server roll out new codes without
  coordinating with every gateway.
- The server emits codes only from the *Status* column "shipped" below.
  "Spec'd" codes are reserved in the proto comments but not yet returned;
  promoting one to "shipped" is the trigger to update this doc.

---

## `LookupRoute` / `LookupPDRoute`

`LookupPDRoute` remains a fail-open stub today (Phase 2 — prefill/decode
routing) and unconditionally emits `NO_HINT`. The codes in the table below
that are marked **LookupRoute only** are emitted by `LookupRoute`'s handler
and will surface from `LookupPDRoute` when its handler is wired in a later
module — until then, treat `NO_HINT` as the only `LookupPDRoute` answer.

| Code | Status | RPC | When the server emits it | Response shape | What the gateway does |
|---|---|---|---|---|---|
| `PREFIX_MATCH` | **shipped** | LookupRoute only | The index has at least one replica holding `(tenant, model, hash_scheme, prefix_hash)`, the ranker returned a non-empty set, AND at least one replica's realized `matched_tokens` cleared the per-namespace `minimumMatchedTokens` floor (default 64). Sub-floor replicas are filtered from the response; if none clear the floor, the response downgrades to `NO_HINT`. | `replica_scores` non-empty, ranked best-first by `matched_tokens × freshness × pressure_factor × slo_bias`. The last two factors collapse to 1 when no replica stats are reported and no SLO hint is set, so the baseline is `matched_tokens × freshness`. Every qualifying replica is returned today (no top-K limit); the gateway typically uses the top entry. | Route to the top-ranked replica → prefix-cache hit; lower TTFT. |
| `NO_HINT` | **shipped** | both | The fail-open default. **`LookupPDRoute`**: every call (the handler is a stub). **`LookupRoute`**: the prefix is novel under matching contract keys; the ranker found nothing; any of `tenant_id`, `model_id`, or `hash_scheme` was unspecified (a contract violation — set-but-wrong values surface as the matching `UNKNOWN_*` code instead); the index is globally empty (cold-start carve-out); the request was policy-gated below `minimumPrefixTokens` (request-side); **every replica that held the prefix matched fewer tokens than `minimumMatchedTokens` (result-side floor, default 64)**; or an index-disabled state. | `replica_scores` **empty**. Not an error. | Route per the gateway's default policy (round-robin, least-loaded, …). The cache plane is invisible to this request. |
| `TENANT_HOT` | **shipped** | LookupRoute only | No exact prefix match for `(tenant, model, hash_scheme, prefix_hash)`, but the tenant has at least one replica that (a) has reported stats recently (within ~5 minutes by default), (b) has a `hit_rate` above a small floor (default 0.1), AND (c) currently has **at least one prefix entry in the requested `(tenant, model, hash_scheme)` in the index** — proving the replica serves the requested engine domain. The "in the index" check is sweep-driven (an entry past TTL stays counted until the next sweep removes it), so for at most one sweep interval a recently-stale entry can briefly still satisfy the check; per soft-state semantics that yields at worst a soft hint that turns into a cache miss, never a wrong answer. A coarser locality signal than `PREFIX_MATCH` — useful when the prefix is novel but the tenant already has servers warm in the cache rotation. | `replica_scores` non-empty (tenant-hot ranked); `matched_tokens` is **0** because there is no prefix overlap (the gateway must rely on `reason_code`, not `matched_tokens`, to recognize this branch). Shape otherwise unchanged. | Treat as a softer hint than `PREFIX_MATCH`; gateway free to use or ignore. |
| `TIMEOUT` | **shipped** | LookupRoute only | The lookup deadline expired before the index could rank — either the caller's context was already past its deadline on arrival or the per-tenant `CachePolicy.spec.lookupTimeoutMs` budget elapsed during the lookup. Gateway clients also synthesize this locally when *they* cancel a slow `LookupRoute` RPC. | Server: empty `replica_scores`. Client-side synth: same. | Treat as `NO_HINT`. |
| `UNKNOWN_TENANT` | **shipped** | LookupRoute only | After a prefix miss (and the `TENANT_HOT` fallback when it applies — non-chain requests; chain requests skip `TENANT_HOT` by design and classify directly), AND the index is **not globally empty**: the request supplied a non-empty `tenant_id` and the index has **zero prefix entries for that tenant** across every model and hash scheme. **Cold-start carve-out:** a globally empty index (server just started, no `ReportCacheState` yet) stays on `NO_HINT` so a fresh deployment does not flood gateways with `UNKNOWN_TENANT`; the diagnostic resumes the moment any replica has reported state. Canonical asymmetric shape: a gateway-SDK querying with `tenant_id="default"` while the producer (kvevent-subscriber sidecar) is publishing under `tenant_id=$(POD_NAMESPACE)`. | Empty `replica_scores`. | Treat as `NO_HINT` for routing (still fail-open — the cache plane is hint-only); surface as a configuration error (log line / metric / SDK warning). **Do not retry under a different key** — the cache plane will not change between calls. |
| `UNKNOWN_MODEL` | **shipped** | LookupRoute only | Same precondition as `UNKNOWN_TENANT` above. The tenant is known but the `(tenant_id, model_id)` pair has **zero entries**. The model has never served traffic in this tenant, or the model identifier disagrees between producer and consumer. | Empty `replica_scores`. | Same as `UNKNOWN_TENANT`: fail-open, surface as configuration error. |
| `UNKNOWN_HASH_SCHEME` | **shipped** | LookupRoute only | Same precondition as `UNKNOWN_TENANT` above. `(tenant_id, model_id)` has entries, but **none under the request's `hash_scheme`**. Canonical shape: ingest under `"vllm"`, lookup under `"vllm-v1"`. An empty `hash_scheme` is a contract violation (not a mismatch) and stays on `NO_HINT`. | Empty `replica_scores`. | Same as `UNKNOWN_TENANT`: fail-open, surface as configuration error. |

**Constants in code:** `reasonPrefixMatch`, `reasonTenantHot`, `reasonNoHint`, `reasonTimeout`,
`reasonUnknownTenant`, `reasonUnknownModel`, `reasonUnknownHashScheme` in
`pkg/server/inferencecache_service.go`. See also [`../design/lookuproute-diagnostics.md`](../design/lookuproute-diagnostics.md) for the design rule and gateway-SDK guidance.

### Ranking inputs beyond `matched_tokens × freshness`

The server-side ranker (`pkg/index`) is configurable via `RankerConfig`. Every
knob defaults to a value that reduces the score to the baseline when its
supporting signal is absent — so a deployment without replica stats or SLO
hints behaves exactly like the original B6 ranker.

| Knob | What it does | Default | Off switch |
|---|---|---|---|
| `PressureWeight` | Penalty applied to a replica's score from `ReplicaStats.pressure`: `pressure_factor = max(0, 1 - PressureWeight × pressure)`. Avoids blindly preferring a saturated cache holder over a fresher, lower-pressure peer. | `1.0` | `0` → no penalty |
| `SLOTightTTFTMs` | TTFT budget (ms) below which the request is "tight" and the SLO bias kicks in. Uses `LookupRouteRequest.slo.ttft_ms`. | `200` | `0` → bias never fires |
| `SLOTightBias` | Coefficient in the freshness boost: `slo_bias = 1 + freshness × SLOTightBias` when the request is tight. Higher → fresher candidates are favored more aggressively. | `1.0` | `0` → no boost |
| `TenantHotMinHitRate` | Minimum `hit_rate` for a replica to count as "warm" for the `TENANT_HOT` fallback. | `0.1` | n/a (use `TenantHotMaxAge = 0` to disable the fallback) |
| `TenantHotMaxAge` | Maximum stats age for a replica to count as "warm". | `5m` | `0` → fallback disabled (a prefix miss whose contract keys all populate the index lands at `NO_HINT`; mismatched-key misses still diagnose as `UNKNOWN_*` via the miss-classifier) |

---

## `RenderTemplate`

| Code | Status | When the server emits it | Notes |
|---|---|---|---|
| `OK` | **shipped (stub)** | The handler currently returns `OK` unconditionally — the rendering pipeline (Wedge D, D1–D5) isn't wired yet. | Becomes "real" when D2 (render pipeline) lands. |
| `TEMPLATE_NOT_FOUND` | spec'd, not emitted | The referenced `template_ref` doesn't exist. | Promoted when D5 (`RenderTemplate` handler) lands. |
| `RENDER_ERROR` | spec'd, not emitted | Template was found but rendering failed (missing/typed-wrong variables, runtime DSL error). | Promoted with D5. |

**Constants in code:** `reasonOK` in `pkg/server/inferencecache_service.go`.

---

## `Ack` (`ReportCacheState`, `PublishEvent`)

The `Ack` proto carries an optional `reason_code` for future use (e.g. partial
acceptance, throttling signals). **Today the server returns
`Ack{Accepted: true}` with `reason_code` unset on every code path** — there are
no `Ack` reason codes in use.

When the first `Ack` code ships (e.g. `THROTTLED`, `SCHEMA_DROPPED`), add it to
the table below and update this paragraph.

| Code | Status | When the server emits it | Notes |
|---|---|---|---|
| *(none)* | — | — | Field reserved for future structured acknowledgments. |

---

## How reason codes show up in metrics

`reason_code` is a **label** on `inferencecache_lookup_route_calls_total`,
alongside `model` and `hint_used`. The cardinality of values is therefore bounded
by the table above — adding a new code adds one new label value (cheap), but it
*does* show up as a new time series, so prefer reusing existing codes if the
semantic fits.

```text
inferencecache_lookup_route_calls_total{model="...", reason_code="PREFIX_MATCH", hint_used="true"}  42
inferencecache_lookup_route_calls_total{model="...", reason_code="NO_HINT",      hint_used="false"} 318
```

`hint_used="true"` ⇔ `replica_scores` was non-empty in the response. It
correlates with either `PREFIX_MATCH` or `TENANT_HOT` (both shipped codes
return non-empty scores).

See [metrics.md](metrics.md) for the full metric surface.

---

## How to add a new reason code

1. **Reserve the string in the proto comment** on the response message in
   [`proto/inferencecache/v1alpha1/inferencecache.proto`](../../proto/inferencecache/v1alpha1/inferencecache.proto)
   (already done for `TENANT_HOT`, `TIMEOUT`, `TEMPLATE_NOT_FOUND`,
   `RENDER_ERROR`). Run `make proto-gen` if the comment touched the schema.
2. **Add a constant** in `pkg/server/inferencecache_service.go` next to
   `reasonPrefixMatch` / `reasonNoHint` / `reasonOK`. Keep the constant name
   `reason<CamelCase>`.
3. **Emit it** from the handler at the relevant decision point. Keep handlers
   side-effect-free apart from metrics; `reason_code` is the *only* way the
   server communicates "what kind of answer is this."
4. **Update the table above** — move the row from "spec'd" → "shipped",
   describe the trigger condition, the response shape, and the gateway action.
5. **Document the metric expectation.** If the new code is for `LookupRoute`,
   a new `reason_code` label value will appear automatically in
   `inferencecache_lookup_route_calls_total`. Mention this in the PR description
   so dashboards can be updated.
6. **Backward-compat check.** Confirm that an existing client *not* updated for
   the new code degrades to its no-hint default (`NO_HINT` / `RENDER_ERROR`).
   This is the contract; verify by reading the client adapter, not by guessing.
