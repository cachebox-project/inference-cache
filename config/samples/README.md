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
- **`cachebackend-*.yaml`** — earlier hand-curated CacheBackend shapes, retained
  for back-compat. The `recipe-*.yaml` catalog is the maintained entry point.

## Recipe catalog

Each recipe is a single file with a top-of-file comment explaining the scenario
and the apply steps. Most are self-contained; see "Prerequisites per recipe"
below for the two that aren't (external cache, multi-tenant), and note
`recipe-gpu-production` is a shape template whose placeholder images you pin
before applying. All but `recipe-gpu-production` run without a GPU.

| Recipe | Use case |
| --- | --- |
| [`recipe-cpu-dev.yaml`](recipe-cpu-dev.yaml) | Fastest path on a laptop / kind — tiny ungated model, no GPU, single replica, no quotas. |
| [`recipe-gpu-production.yaml`](recipe-gpu-production.yaml) | Typical production — real model on GPU engine pods, managed-backend autoscaling, a CachePolicy with production TTLs. |
| [`recipe-external-cache.yaml`](recipe-external-cache.yaml) | `type: External` — point the operator at a cache server you manage yourself; the controller provisions nothing. |
| [`recipe-multi-tenant.yaml`](recipe-multi-tenant.yaml) | Two CacheTenants + two CacheBackends across two namespaces — isolated cache identity and entry-count quotas; separate engines for per-tenant memory isolation. |
| [`recipe-tuning.yaml`](recipe-tuning.yaml) | CPU-dev shape plus a meaningful `engineOverrides` block (tune `LMCACHE_CHUNK_SIZE`, add `LMCACHE_LOG_LEVEL=DEBUG`). |
| [`recipe-persistent-cache.yaml`](recipe-persistent-cache.yaml) | `spec.storage.pvc` on `deploymentKind: Deployment` — provisions one owner-referenced ReadWriteOnce PVC and mounts it into the cache-server pod. Use `deploymentKind: StatefulSet` for per-replica PVCs. (Server-side disk-backed KV is a separate follow-up; the volume is mounted but the server still keeps KV in memory.) |

**Prerequisites per recipe.** Most recipes are self-contained. One has an
external dependency: `recipe-external-cache.yaml` needs a cache server already
running at the endpoint you supply (replace the placeholder).
`recipe-multi-tenant.yaml` has no external dependency but creates and deploys
into two namespaces of its own. `recipe-persistent-cache.yaml` needs a usable
default StorageClass for its PVC to bind (kind ships one) — on a cluster without
one, set `spec.storage.pvc.storageClassName` to an existing class (or
pre-provision a matching PV), otherwise the PVC stays `Pending` and
`status.capacity` never populates.

**Apply + observability.** Each recipe's `kubectl apply` wires matching engine
pods to the cache. For *managed* backends the wiring becomes available once the
controller publishes `status.endpoint`, so a pod admitted before then races past
injection and runs unwired until recreated (see each recipe's header); `External`
backends wire straight from the `spec.endpoint` you provide and have no such
race. KV reuse then works, but a *managed* backend only reaches `Ready=True`
and reports index entries once the `kvevent-subscriber` sidecar is auto-attached,
which requires the controller to run with `--kvevent-subscriber-image` set
(empty by default); otherwise it holds at `AwaitingFirstKVEvent` and then
degrades to `NoKVEventsObserved`. `External` backends are exempt from that gate —
they go `Ready` as soon as admission accepts the endpoint. See the
[quickstart](../../docs/quickstart.md).

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
