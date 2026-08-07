<!-- Title: imperative, concise. -->

## Summary

<!-- What does this PR do, and why? -->

## Linked issues

<!-- e.g. Closes #123 -->

## Checklist

### Vendor-neutral naming (required — see CONTRIBUTING.md)
- [ ] No `oci` / `oracle` / `*.oci.com` / `oraclecloud.com` in any API group, CRD group, proto package, gRPC service/package, Kubernetes namespace, image registry, Helm chart, or Go module path.
- [ ] Any cloud-specific (incl. OCI) integration lives in an isolated, optional adapter (`pkg/adapters/.../`) — never in core controllers, CRD types, the proto contract, or default config.
- [ ] No Oracle/OCI domain or namespace in sample manifests, README, or default values.
- [ ] Pre-commit naming guard passed (`make install-hooks` once, then it runs on every commit).

### Quality
- [ ] Every human-authored commit includes a matching DCO `Signed-off-by:` trailer (`git commit --signoff`).
- [ ] `make reuse-lint` passes (SPDX headers and licensing metadata are complete).
- [ ] `make build` and `make test` pass locally.
- [ ] `make lint` clean (gofmt + go vet).
- [ ] `make manifests generate` produces **no drift** (generated code committed).
- [ ] New/changed behavior has unit tests.
- [ ] Operator-facing change (CRD columns/fields, `.status`, CLI, gRPC/HTTP, install bundle/RBAC, samples)? If so, the install-smoke gate asserts it (see CONTRIBUTING.md).
- [ ] CI is green.

### Contracts (only if touching CRDs or proto)
- [ ] Change matches the tech spec (or the spec is updated in the same PR).
- [ ] Backward compatibility considered for `v1alpha1` consumers (engines, gateway clients).
- [ ] If `proto/` changed, `docs/design/grpc-contract.md` is updated to match (the pre-commit hook enforces this).
- [ ] If CRD API types (`api/v1alpha1/*_types.go`) or the proto contract changed, the **documentation is updated to match** — the docs site (`site/`) and/or the design docs (`docs/`). CI enforces this (`make verify-docs-sync`); add the `no-docs-needed` label to waive a genuinely doc-exempt change.
