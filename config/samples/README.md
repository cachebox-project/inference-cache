# Sample manifests

This directory holds two flavors of `inferencecache.io/v1alpha1` sample CRs:

- **`cache_v1alpha1_*.yaml`** — kubebuilder-generated minimum-viable samples,
  one per CRD kind. Useful as a starting point or for the first
  `kubectl apply` after a fresh install.
- **`cachebackend-*.yaml`** — hand-curated recipes that show the common
  CacheBackend shapes operators are expected to deploy (LMCache, external
  cache, engine + override patterns, etc.).

## Apply-clean is enforced

Every sample under this directory MUST apply cleanly against a cluster
running the current CRD schema and the CacheBackend admission webhook. CI
enforces this via:

```bash
make verify-samples
```

The target spins up an envtest apiserver, installs the CRDs from
`config/crd/bases/` and the webhook configuration from
`config/webhook/manifests.yaml`, registers the CacheBackend defaulter +
validator in-process with the shipping adapter registry, then runs
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
fixed instead), add this exact line as a top-of-file comment, **before**
any non-comment line:

```yaml
# verify-samples: skip
```

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
