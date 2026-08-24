# Ranker calibration

This package replays routing observations through the production
`internal/index.LookupRoute` implementation and searches a Cartesian grid of
the five `RankerConfig` knobs. It reports two outcome rates:

- `prefix_hit_rate_pct`: the selected top replica produced an observed cache
  hit for a `prefix` observation.
- `tenant_hot_hit_rate_pct`: the selected top replica produced an observed
  cache hit after a `tenant_hot` fallback observation.

`macro_hit_rate_pct` gives both observation classes equal weight, regardless
of how many rows each class contributes. The deterministic tie-break prefers
gentler pressure/SLO multipliers and shorter fallback windows.

## Trace shape

Each observation is a self-contained point-in-time view. `reported_prefix`
records whether the cache plane believed that replica held the requested
prefix; `prefix_reported_at_ms` records that prefix observation's freshness,
while `stats_reported_at_ms` independently records when `hit_rate` and
`pressure` were reported. `matched_tokens` and those timestamps are the signals
visible to the ranker. `prefix_hash` is standard base64 JSON for the engine's
opaque bytes, not a human-readable identifier. Replicas without the requested
prefix still receive a collision-free serving-only entry during replay so
`TENANT_HOT` can apply its real engine-domain membership guard.

Every replica row must set `outcome_available: true`; `observed_hit` is the
ground-truth result of routing that request to that replica. Captured traces
must measure that outcome experimentally for every candidate under an
equivalent cache snapshot; synthetic traces may define it by construction. A
normal production request observes only its selected replica and is therefore
not sufficient calibration input by itself. The harness rejects incomplete
rows so an unavailable outcome cannot silently turn into a miss. Zero values
for `slo_tight_ttft_ms` and
`tenant_hot_max_age_ms` are valid sweep points and exercise the production kill
switches.

Captured data should contain opaque or one-way prefix hashes only. Do not put
prompt text, token IDs, customer identifiers, or other request content in a
trace. Use stable pseudonyms for tenants, models, and replicas. Set
`provenance.kind` to `captured` only when the observations came from a real
run; generated and hand-constructed fixtures must say `synthetic`.

The checked-in fixture is intentionally synthetic because no production C1
trace is available in this repository. It provides deterministic boundary
coverage and proves the calibration pipeline, but it should be replaced or
supplemented with a sanitized captured trace before treating the coefficients
as a production benchmark conclusion. Its selected tuple is therefore a
candidate only and does not change `DefaultRankerConfig`; production defaults
remain stable until representative captured data supports a retune.

## Reproduce

```bash
make ranker-calibration
make verify-ranker-calibration
```

Override `RANKER_CALIBRATION_TRACE` and `RANKER_CALIBRATION_RESULT` to replay a
different trace without changing the tool.
