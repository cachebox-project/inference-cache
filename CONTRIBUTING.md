# Contributing

Thanks for helping build inference-cache. This repository follows kubebuilder-style controller conventions and keeps generated code checked in.

## Local Setup

Requirements:

- Go 1.26.6 or newer
- Make
- Python 3 with `venv` support (`python3-venv` on Debian/Ubuntu)
- Docker
- kind
- protoc

Run the baseline checks before sending a PR:

```bash
make proto-gen
make proto-lint
make verify-dco test-dco
make reuse-lint
make lint
make python-lint
make test-race
make build
make cover-check # fail if logic-package coverage drops below COVER_MIN (90%)
make vulncheck   # vulnerability scan (needs network); blocking in CI
make verify-minimal-base test-minimal-images
```

`make test-race` runs the unit tests under the race detector — it's what the
pre-push gate and CI use; `make test` is the faster, non-race variant for quick
local iteration. `make cover-check` enforces a coverage floor (`COVER_MIN`, 90%)
over the hand-written logic packages — generated code, `cmd/` entrypoints,
tooling under `hack/`, and test helpers are excluded; `make cover` prints the
per-function report. Coverage is measured with `-coverpkg=./...` so a helper
exercised by tests in a CONSUMING package counts as covered — this matches
how integration coverage actually exists in this repo (adapter helpers tested
through the adapter, predicates tested through the controller). The floor is
a ratchet: raise it as coverage improves.
`make ci-lint` runs the golangci-lint configuration used by CI.
`make python-lint` installs the pinned Ruff release into the ignored `bin/`
directory and lints Python scripts under `docs/reference-stack/scripts/`.
`make proto-lint` lints the gRPC contract with [buf](https://buf.build) (configured in `buf.yaml`);
buf is used for linting only — code generation stays on `protoc` (`make proto-gen`).
`make verify-minimal-base` and `make test-minimal-images` enforce the shipped
Distroless runtime-stage policy without requiring Docker. After `make
image-build`, `make verify-minimal-images` also inspects each image's effective
user, entrypoint, and filesystem; see
[`docs/operations/container-images.md`](docs/operations/container-images.md).

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

## Developer Certificate of Origin (required)

Every human-authored commit in a pull request must include a `Signed-off-by:`
trailer whose name and email match that commit's author or committer. The
trailer certifies the contribution under the
[Developer Certificate of Origin](https://developercertificate.org/); it is
not a GPG or SSH signature.

Add the trailer when creating a commit:

```bash
git commit --signoff
# Short form:
git commit -s
```

After reviewing the latest commit, add a missing trailer with:

```bash
git commit --amend --no-edit --signoff
```

For an older commit, use an interactive rebase and amend only commits you can
personally certify. Do not add another contributor's sign-off on their behalf.
The `DCO` pull-request check verifies every non-merge commit. Individual bot
commits are exempt only when the GitHub API identifies their author as a bot;
human commits in a bot-opened pull request are still checked. The local gate
has no trusted GitHub identity metadata, so it checks every commit. Run it with
`make verify-dco` (and its self-contained test suite with `make test-dco`).

## Licensing metadata

The repository follows the [REUSE Specification](https://reuse.software/spec/).
Authored source and configuration files carry SPDX comment headers. Generated
artifacts, documentation, and non-commentable fixtures are covered by narrow
entries in `REUSE.toml`; canonical license text lives under `LICENSES/`.

Run the pinned compliance tool locally with:

```bash
make reuse-lint
```

The target installs REUSE in an isolated virtual environment under the ignored
`bin/` directory. New authored source files should include:

```text
SPDX-FileCopyrightText: 2026 The inference-cache Authors
SPDX-License-Identifier: Apache-2.0
```

## Before pushing / opening a PR

Run `make install-hooks` once per clone. Thereafter:

- **On every push**, the `pre-push` hook runs `make ci` (naming + internal-refs + DCO/REUSE compliance + format + vet + Go/Python lint + Prometheus rules + golden vectors + race tests + build) and blocks the push if anything fails. Reproduce it anytime with `make ci`. CI also runs heavier gates that are not part of the local push hook, including the Rust/network-backed `make tokenize-cgo-test` job for the optional `smgcgo` tokenizer build tag.
- **Before opening a PR**, run `make pre-pr` — it runs `make ci`, then a generated-code drift check, then `make verify-samples` (server-side dry-run of every YAML under `config/samples/` against an envtest apiserver + the CacheBackend admission webhook), then prints the review checklist. Review the diff against the tech spec before submitting.

Emergency override for the push gate: `git push --no-verify` (discouraged).

### Operator-facing changes must extend the install-smoke gate

The per-PR install-smoke gate — [`docs/reference-stack/scripts/default_install_smoke.sh`](docs/reference-stack/scripts/default_install_smoke.sh), wired via `.github/workflows/default-install-smoke.yml` — spins up a kind cluster, installs `config/default`, and asserts the bundle actually comes up. **When your change alters an operator-facing surface, extend that script with an assertion that drives the surface end-to-end in the cluster.** Operator-facing surfaces include:

- CRD fields and `additionalPrinterColumns` (the `kubectl get` output operators read);
- CR `.status` surfaces;
- CLI output;
- gRPC / HTTP behavior;
- the default-install bundle or its RBAC;
- sample manifests.

A good assertion drives the surface the way an operator would: apply the relevant CR, wait for the controller to write status, then assert the observable (a `.status` field, a printer column, a gRPC response). The smoke runs with **no engine traffic**, so assert the no-traffic / "observed-zero" steady state — e.g. the cluster `CacheIndex` reports a populated `.status.observedServer` — rather than anything that needs real cache hits.

Worked example: the CacheIndex poller assertion in that script applies nothing exotic but waits for the controller to populate `cacheindex/cluster-default`'s `.status.observedServer` and fails if it stays empty — the same shape every per-feature assertion should follow.

## Repository layout — where new code goes

See the README's "Repository layout" for the full map. In short:

| You're adding… | Put it in |
|---|---|
| A CRD field / new API type | `api/v1alpha1/` → then `make manifests generate` |
| Controller / reconciler logic | `internal/controller/` |
| Controller ↔ server HTTP wire type | `internal/controlplaneapi/` |
| Pod-binding annotation / metadata contract | `internal/enginebinding/` |
| gRPC handlers, server wiring | `internal/server/` |
| Cache-state index logic | `internal/index/` |
| Planned reusable rendering API (reserved; not implemented) | `pkg/render/` |
| Stable adapter extension contract | `pkg/adapters/{backend,runtime}/` |
| Shipping adapter implementation / registration | `internal/adapters/builtin/` |
| Engine KV-event ingest implementation | `internal/subscriber/` |
| Engine egress client (pre-tokenized request → engine; harness / benchmark, no binary owner) | `pkg/engineclient/` |
| The gRPC contract | `proto/` → then `make proto-gen` |

Each package's `doc.go` (or package comment) states which binary owns it or why
it is a supported external Go API. Follow
[`docs/design/repository-boundaries.md`](docs/design/repository-boundaries.md)
for dependency direction and the staged internal-package migration.

**Generated code** — `config/crd/`, `config/rbac/role.yaml`, `api/**/zz_generated*.go`, `gen/` — is committed but never hand-edited. Regenerate and commit it with the source change (`make pre-pr` verifies there's no drift).

**gRPC contract:** when you change `proto/`, update [`docs/design/grpc-contract.md`](docs/design/grpc-contract.md) in the same commit so the design doc stays accurate. The pre-commit hook blocks a commit that touches a `.proto` without touching that doc (override with `--no-verify` only if the change truly doesn't affect the contract).

## Documentation stays in sync (required)

The public surfaces have user-facing documentation, and it must not drift behind the code. **A change to the CRD API types ([`api/v1alpha1/*_types.go`](api/v1alpha1)) or the gRPC contract ([`proto/`](proto)) must also update the documentation in the same PR** — the docs site under [`site/`](site) and/or the design docs under [`docs/`](docs). For example, a `proto/` change updates `docs/design/grpc-contract.md` (and the site's gRPC reference); a CRD field change updates the matching concept and reference pages under `site/content/`.

### Enforcement

```bash
make verify-docs-sync    # checks the current branch's diff vs origin/main; also runs in CI on every PR
```

CI runs this check on each pull request as the lightweight **Docs Sync** job. Label changes rerun only that job, so adding or removing the **`no-docs-needed`** waiver does not restart the full CI suite.
