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
visible to the ranker. `observed_hit` is the later ground-truth outcome for
routing to that replica. Replicas without the requested prefix still receive a
unique serving-prefix entry during replay so `TENANT_HOT` can apply its real
engine-domain membership guard.

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
