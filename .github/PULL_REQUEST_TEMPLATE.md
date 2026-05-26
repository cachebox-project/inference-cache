<!-- Title: imperative, concise. Prefix the branch with the Linear key (e.g. cac-15-...) so it auto-links. -->

## Summary

<!-- What does this PR do, and why? -->

## Linear

<!-- e.g. Closes CAC-123 -->

## Checklist

### Vendor-neutral naming (required — see CLAUDE.md)
- [ ] No `oci` / `oracle` / `*.oci.com` / `oraclecloud.com` in any API group, CRD group, proto package, gRPC service/package, Kubernetes namespace, image registry, Helm chart, or Go module path.
- [ ] Any cloud-specific (incl. OCI) integration lives in an isolated, optional adapter (`pkg/adapters/.../`) — never in core controllers, CRD types, the proto contract, or default config.
- [ ] No Oracle/OCI domain or namespace in sample manifests, README, or default values.
- [ ] Pre-commit naming guard passed (`make install-hooks` once, then it runs on every commit).

### Quality
- [ ] `make build` and `make test` pass locally.
- [ ] `make lint` clean (gofmt + go vet).
- [ ] `make manifests generate` produces **no drift** (generated code committed).
- [ ] New/changed behavior has unit tests.
- [ ] CI is green.

### Contracts (only if touching CRDs or proto)
- [ ] Change matches the tech spec (or the spec is updated in the same PR).
- [ ] Backward compatibility considered for `v1alpha1` consumers (engines, gateway clients).
