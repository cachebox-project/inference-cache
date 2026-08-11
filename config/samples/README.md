# Sample manifests

This directory holds three flavors of sample manifests (the recipes bundle
`inferencecache.io/v1alpha1` CRs together with their engine Deployments and, for
multi-tenant, Namespaces):

- **`recipe-*.yaml`** — the curated **recipe catalog**: named scenarios (cache
  backend + engine + policy/tenant as needed), each a single `kubectl apply -f`
  that wires the engine to the cache. Pick the one that matches your situation.
  Start here. See [the catalog](#recipe-catalog) below and the
  [quickstart](../../docs/quickstart.md).
- **`cache_v1alpha1_*.yaml`** — kubebuilder-generated minimum-viable samples,
  one per CRD kind. Useful as a starting point or for the first
  `kubectl apply` after a fresh install.
- **`cachebackend-*.yaml`** — focused hand-curated canonical CacheBackend
  examples, including the
  [`cachebackend-sglang-hicache.yaml`](cachebackend-sglang-hicache.yaml)
  engine-local example and the typed SGLang PodLocal LMCache examples for
  [host-only](cachebackend-sglang-podlocal-host-only.yaml),
  [managed Redis](cachebackend-sglang-podlocal-managed-redis.yaml), and
  [external Redis](cachebackend-sglang-podlocal-external-redis.yaml), plus the
  equivalent typed vLLM PodLocal profiles for
  [host-only](cachebackend-vllm-podlocal-host-only.yaml),
  [managed Redis](cachebackend-vllm-podlocal-managed-redis.yaml), and
  [external Redis](cachebackend-vllm-podlocal-external-redis.yaml). All LMCache
  offload samples use the typed PodLocal MP API; `EventsOnly` intentionally
  carries no LMCache data plane.

## Recipe catalog

Each recipe is a single file with a top-of-file comment explaining the scenario
and the apply steps. Most are self-contained; see "Prerequisites per recipe"
below for the two that aren't (external cache, multi-tenant), and note
`recipe-gpu-production` is a shape template whose engine image you pin before
applying. Admission and sample validation require no GPU; actual LMCache MP
startup requires a compatible engine connector/package and the selected
runtime hardware.

| Recipe | Use case |
| --- | --- |
| [`recipe-cpu-dev.yaml`](recipe-cpu-dev.yaml) | Small single-replica typed-MP binding shape; engine startup still requires a connector-compatible image. |
| [`recipe-gpu-production.yaml`](recipe-gpu-production.yaml) | Production shape — GPU engine Pods, per-Pod MP L1, explicit managed Redis L3, and a production CachePolicy. |
| [`recipe-external-cache.yaml`](recipe-external-cache.yaml) | Typed MP with external Redis L3; the controller provisions no remote provider. |
| [`recipe-multi-tenant.yaml`](recipe-multi-tenant.yaml) | Two CacheTenants + two CacheBackends across two namespaces — isolated cache identity and entry-count quotas; separate engines for per-tenant memory isolation. |
| [`recipe-tuning.yaml`](recipe-tuning.yaml) | Small typed-MP shape: typed `chunkSizeTokens` plus an `engineOverrides` log-level addition. |

**Prerequisites per recipe.** Most recipes are self-contained. One has an
external dependency: `recipe-external-cache.yaml` needs Redis already
running at the endpoint you supply (replace the placeholder).
`recipe-multi-tenant.yaml` has no external dependency but creates and deploys
into two namespaces of its own.

**Apply + observability.** Each recipe's `kubectl apply` wires matching engine
pods to the cache. Host-only PodLocal wiring needs no provider endpoint. A
managed Redis L3 is controller-resolved, so apply the CacheBackend before
creating engine Pods; externally owned Redis uses the declared endpoint. KV
reuse then works when the runtime-owned image is compatible, but a managed
backend only reaches `Ready=True`
and reports index entries once the `kvevent-subscriber` sidecar is auto-attached,
which requires the controller to run with `--kvevent-subscriber-image` set
(empty by default); otherwise it holds at `AwaitingFirstKVEvent` and then
degrades to `NoKVEventsObserved`. Externally owned backends are exempt from that gate —
they go `Ready` as soon as admission accepts the endpoint. See the
[quickstart](../../docs/quickstart.md).

`SGLangHiCache` is endpoint-free and has no endpoint publication race. Its
first implementation intentionally publishes no `Ready` condition; the
matching Pod's injection annotations are the available wiring signal until the
separate HiCache readiness contract ships.

`recipe-multi-tenant.yaml` spans two namespaces, so it carries a
`# verify-samples: skip` marker — server-side dry-run can't create the
namespaces it depends on, so the gate can't cover it. `kubectl apply` orders
namespace creation ahead of namespaced objects, so it is intended to apply in
one pass on a real cluster; validate it manually against a kind cluster. A
cache-aware-routing recipe (full gateway integration) is deferred until the
gateway-side client ships.

## Apply-clean is enforced

Every non-skipped sample under this directory MUST apply cleanly
against a cluster running the current CRD schema and the CacheBackend,
CachePolicy, and CacheTenant admission webhooks. (See the opt-out section
below for the narrow escape hatch.) CI enforces this via:

```bash
make verify-samples
```

The target spins up an envtest apiserver, installs the CRDs from
`config/crd/bases/` and the webhook configuration from
`config/webhook/manifests.yaml`, registers the CacheBackend defaulter +
validator (with the shipping adapter registry) plus the CachePolicy and
CacheTenant validators in-process, then runs
`kubectl apply --dry-run=server -f <file>` for every YAML in this
directory.

If admission rejects any sample (unknown engine value, removed CRD field,
unsupported runtime/backend pair, reserved-arg/env conflict, …) the gate
fails the PR. This is the same admission validation a real cluster runs
on `kubectl apply`, so it doubles as a fast-feedback check that the
samples teach operator-correct semantics.

The gate is wired into `make pre-pr` (the local gate contributors run
before opening a PR) and the `test` CI job (headless, no real cluster),
so it runs both before `gh pr create` locally and on every PR in CI.
It is **not** part of `make ci` or the `pre-push` hook — running envtest
on every push would slow down the inner loop more than it's worth.

### Adding a new sample

1. Drop the YAML here (any `*.yaml` / `*.yml` under this tree is picked
   up — no allowlist).
2. Run `make verify-samples` locally to confirm admission accepts it.
3. Commit.

### Opt-out

If a sample is intentionally illustrative and is expected to be rejected
by the current schema (rare — almost always a sign the sample should be
fixed instead), add this line as a top-of-file comment, **before** any
non-comment line:

```yaml
# verify-samples: skip
```

The parser trims surrounding whitespace, so leading/trailing spaces on
the comment line are tolerated; everything else (extra punctuation,
trailing tokens, a different prefix) is NOT a match and the sample will
still be applied.

The gate reports such files as `SKIP` and does not apply them. Use this
sparingly — every skipped sample is a class of drift that no longer has
coverage. Prefer fixing the sample over opting it out.

### Running just the gate locally

```bash
make verify-samples
```

The target installs `setup-envtest` if needed, fetches the envtest
binaries, and prints a per-file `OK` / `SKIP` / `FAIL` line. A non-zero
exit means at least one sample was rejected — the `FAIL` block contains
the verbatim admission error you'd see in production.
