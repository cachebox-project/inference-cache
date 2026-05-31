# Contributing

Thanks for helping build inference-cache. This repository follows kubebuilder-style controller conventions and keeps generated code checked in.

## Local Setup

Requirements:

- Go 1.26.3 or newer
- Make
- Docker
- kind
- protoc

Run the baseline checks before sending a PR:

```bash
make proto-gen
make proto-lint
make lint
make test-race
make build
make cover-check # fail if logic-package coverage drops below COVER_MIN (85%)
make vulncheck   # vulnerability scan (needs network); blocking in CI
```

`make test-race` runs the unit tests under the race detector — it's what the
pre-push gate and CI use; `make test` is the faster, non-race variant for quick
local iteration. `make cover-check` enforces a coverage floor (`COVER_MIN`, 85%)
over the hand-written logic packages — generated code, `cmd/` entrypoints, and
test helpers are excluded; `make cover` prints the per-function report. The
floor is a ratchet: raise it as coverage improves. `make ci-lint` runs the
golangci-lint configuration used by CI.
`make proto-lint` lints the gRPC contract with [buf](https://buf.build) (configured in `buf.yaml`);
buf is used for linting only — code generation stays on `protoc` (`make proto-gen`).

## Optional coding-agent tooling

These are optional and personal — use them with whatever editor or coding agent you prefer. None of it is required to build or contribute, and no agent-specific configuration is committed (keep any such config local and ignored).

### Serena (semantic code navigation)

[Serena](https://github.com/oraios/serena) is a language-server-backed [MCP](https://modelcontextprotocol.io) server that gives a coding agent symbol-level navigation and editing (find symbol, find references, rename) instead of plain-text search. It needs [`uv`](https://github.com/astral-sh/uv) installed; `uvx` then runs it without a separate install:

```bash
uvx --from git+https://github.com/oraios/serena serena start-mcp-server \
  --context ide-assistant --project "$(pwd)"
```

Register that server in your agent's MCP configuration (each agent has its own mechanism and config location). Serena writes a per-project cache under `.serena/`, which is local and git-ignored.

### Superpowers (development-workflow skills)

[Superpowers](https://github.com/obra/superpowers) is a set of composable agent skills (brainstorming, TDD, planning, code review). It installs per user and per harness, so there is no shared or committed install — follow the per-harness instructions in the [Superpowers quickstart](https://github.com/obra/superpowers#quickstart) for your tool.

## Development Cluster

Create a local kind cluster:

```bash
make dev-cluster
```

The default cluster name is `inference-cache`. Override it with `KIND_CLUSTER=<name>`.

## Generated Code

After changing API types or controller RBAC markers, run:

```bash
make generate
make manifests
```

After changing protobuf files, run:

```bash
make proto-gen
make proto-lint
```

Commit generated artifacts with the source changes.

## Vendor-neutral naming (required)

This is a vendor-neutral open-source project. **No cloud-vendor-specific domain or namespace may appear in any public or core identifier** — applies to both writing code and reviewing PRs.

Banned in core/public identity: `oci` / `oracle` tokens, and `*.oci.com` / `oraclecloud.com` domains, used as an API group, CRD group, kubebuilder domain, proto package, gRPC service/package, Go module path segment, default Kubernetes namespace, Helm chart name, or container image registry.

Canonical identity (use these everywhere):

| Identifier | Value |
|---|---|
| API group / CRD group / domain | `inferencecache.io` |
| proto package | `inferencecache.v1alpha1` |
| gRPC service | `InferenceCache` |
| Go module | `github.com/cachebox-project/inference-cache` |

Cloud-specific integration (including OCI) **is** allowed, but only as an isolated, optional adapter under `pkg/adapters/.../` — never in core API / CRD / proto / controller identity or default config. The rule bans vendors from the project's *identity and defaults*, not from *integration capability*.

### Enforcement

```bash
make install-hooks    # one-time per clone: installs the pre-commit naming guard (core.hooksPath)
make verify-naming    # run the same check on demand (also wire into CI)
```

The pre-commit hook (`.githooks/pre-commit`) blocks any commit that introduces a banned token into a core-identity path. Run `make install-hooks` after cloning regardless of which editor or AI assistant you use.

## No internal issue-tracker references (required)

This is a public repository. **Tracked files must not reference an internal issue tracker** — neither ticket IDs nor tracker URLs. Internal planning belongs in the tracker itself (or in local-only, untracked files), not in the codebase.

- To link work from a PR or commit, use **GitHub issues** (e.g. `Closes #123`).
- Keep internal ticket IDs, module/epic codes, and tracker links out of code comments, docs, manifests, and PR/issue templates.

### Enforcement

```bash
make verify-no-internal-refs    # scans tracked files; also runs in CI and the pre-push gate
```

The check (Makefile + `.githooks/pre-commit` + CI) scans every tracked file except the few that define or document this rule, and fails on any internal ticket ID or tracker URL. Emergency override (discouraged): `git commit --no-verify`.

## Before pushing / opening a PR

Run `make install-hooks` once per clone. Thereafter:

- **On every push**, the `pre-push` hook runs `make ci` (naming + internal-refs + format + vet + golangci-lint + race tests + build) and blocks the push if anything fails. Reproduce it anytime with `make ci`. The same set of checks runs in CI.
- **Before opening a PR**, run `make pre-pr` — it runs `make ci`, then a generated-code drift check, then prints the review checklist. Review the diff against the tech spec before submitting.

Emergency override for the push gate: `git push --no-verify` (discouraged).

### Operator-facing changes must extend the install-smoke gate

The per-PR install-smoke gate — [`docs/reference-stack/scripts/default_install_smoke.sh`](docs/reference-stack/scripts/default_install_smoke.sh), wired via `.github/workflows/default-install-smoke.yml` — spins up a kind cluster, installs `config/default`, and asserts the bundle actually comes up. **When your change alters an operator-facing surface, extend that script with an assertion that drives the surface end-to-end in the cluster.** Operator-facing surfaces include:

- CRD fields and `additionalPrinterColumns` (the `kubectl get` output operators read);
- CR `.status` surfaces;
- CLI output;
- gRPC / HTTP behavior;
- the default-install bundle or its RBAC;
- sample manifests.

A good assertion drives the surface the way an operator would: apply the relevant CR, wait for the controller to write status, then assert the observable (a `.status` field, a printer column, a gRPC response). The smoke runs with **no engine traffic**, so assert the no-traffic / "observed-zero" steady state — e.g. the cluster `CacheIndex` reports a populated `.status.observedServer` with zero prefixes — rather than anything that needs real cache hits.

Worked example: the CacheIndex poller assertion in that script applies nothing exotic but waits for the controller to populate `cacheindex/cluster-default`'s `.status.observedServer` and fails if it stays empty — the same shape every per-feature assertion should follow.

## Repository layout — where new code goes

See the README's "Repository layout" for the full map. In short:

| You're adding… | Put it in |
|---|---|
| A CRD field / new API type | `api/v1alpha1/` → then `make manifests generate` |
| Controller / reconciler logic | `internal/controller/` |
| gRPC handlers, server wiring | `pkg/server/` |
| Cache-state index logic | `pkg/index/` |
| Mutable-slot rendering (the wedge) | `pkg/render/` |
| Engine / runtime adapters | `pkg/adapters/{engine,runtime}/` |
| The gRPC contract | `proto/` → then `make proto-gen` |

Each package's `doc.go` states which binary (`inferencecache-controller` or `inferencecache-server`) it belongs to.

**Generated code** — `config/crd/`, `config/rbac/role.yaml`, `api/**/zz_generated*.go`, `pkg/server/proto/` — is committed but never hand-edited. Regenerate and commit it with the source change (`make pre-pr` verifies there's no drift).

**gRPC contract:** when you change `proto/`, update [`docs/design/grpc-contract.md`](docs/design/grpc-contract.md) in the same commit so the design doc stays accurate. The pre-commit hook blocks a commit that touches a `.proto` without touching that doc (override with `--no-verify` only if the change truly doesn't affect the contract).
