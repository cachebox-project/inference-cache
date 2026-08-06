# Repository boundaries and refactor plan

Status: Phase 0 contract cleanup complete; staged structural migration in progress.

This document is the single source of truth for repository ownership,
dependency direction, and the remaining structure refactor. It is intentionally
an implementation checklist rather than a flag-day redesign. Each commit should
be independently reviewable, preserve behavior unless explicitly marked as a
contract change, and leave the repository buildable and testable.

## Goals

The repository structure should make the following questions easy to answer:

1. Which binaries does the project ship?
2. Which packages are supported external Go APIs?
3. Which packages are private implementations owned by a repository binary?
4. Where do Kubernetes, HTTP, and gRPC contracts live?
5. Where should a new runtime adapter, storage provider, controller, or server
   feature be added?

The default rule for new code is:

> Put code under the `internal/` component that owns it. Promote it to `pkg/`
> only after a concrete external consumer exists and the project is prepared to
> maintain its Go API compatibility.

## Top-level directory responsibilities

| Directory | Responsibility |
|---|---|
| `cmd/` | Thin executable entry points: flags, dependency construction, startup, and shutdown |
| `api/` | Public Kubernetes API and CRD Go types |
| `proto/` | Hand-written protobuf sources and wire contract |
| `gen/` | Generated public protocol bindings |
| `pkg/` | Deliberately supported external Go contracts and reusable libraries |
| `internal/` | Repository-private implementations and private cross-binary contracts |
| `config/` | Kubernetes installation, RBAC, CRDs, samples, and overlays |
| `test/` | Cross-component test programs, stacks, fixtures, and end-to-end assets |
| `docs/` | Canonical user, operator, reference, and design documentation |
| `site/` | Documentation-site rendering and presentation |
| `hack/` | Repository-maintainer tools and verification commands |

`cmd/` and `pkg/` are Go conventions. `internal/` is also enforced by the Go
compiler: code outside this module cannot import packages below the repository's
top-level `internal/` directory.

## Dependency direction

The desired high-level dependency direction is:

```text
cmd/
  -> internal/
       -> pkg/ + api/ + gen/
            -> third-party dependencies
```

The rules are:

1. `cmd/` packages are composition roots. They may select concrete built-in
   implementations and inject them into private application packages.
2. `internal/` implementations may depend on public contracts.
3. Public `pkg/` contracts must not depend on controller, webhook, server, or
   built-in implementation packages.
4. `api/v1alpha1` owns the Kubernetes API. The CRDs remain one Go package even
   when their definitions are split across cohesive files.
5. `gen/inferencecache/v1alpha1` owns generated public gRPC bindings. The server
   implements this API but does not own it.
6. `internal/controlplaneapi` owns private HTTP DTOs shared by the controller
   and server. Neither binary imports the other's implementation for wire
   types.
7. `internal/enginebinding` owns generic engine-pod metadata shared by admission
   and controllers. Controllers do not import webhook packages.
8. `pkg/adapters` contains the build-time extension contracts. Shipping
   implementations and registration belong under `internal/adapters`.
9. `internal/adapters/builtin.New` owns the complete registry composition shipped
   by the controller binary. Lower layers do not construct fallback registries.

## Extension model

The current adapter seam is a build-time Go extension point, not a dynamic
plugin system:

- an external project can implement the contracts under `pkg/adapters` and
  build a custom controller binary;
- the shipping controller registers the curated built-ins from
  `internal/adapters/builtin`;
- the CRD currently uses enums for runtime, cache, and remote-storage provider
  identifiers, so adding a new identifier also requires an intentional API and
  CRD change;
- the repository does not load Go plugins or discover adapter implementations at
  runtime.

This is the default extension model until a concrete consumer requires runtime
plugin loading. Do not introduce dynamic loading speculatively.

## Current completed boundaries

The following work is complete at the baseline for this plan:

- [x] Remove the deprecated provider-rendering seams from the runtime adapter
  package.
- [x] Remove the obsolete `CacheBackendType` values that no longer describe the
  canonical engine-cache API.
- [x] Move shipping remote-storage providers to
  `internal/adapters/builtin/storage`.
- [x] Move shipping vLLM and SGLang runtime implementations to
  `internal/adapters/builtin/runtime`.
- [x] Use consistent combination-based runtime filenames:
  `vllm_lmcache.go`, `sglang_lmcache.go`, and `sglang_hicache.go`.
- [x] Keep the LMCache kernel-check implementation with the built-in runtime
  implementation in `internal/adapters/builtin/runtime/lmcachecheck.go`.
- [x] Centralize the shipping runtime and storage registries in
  `internal/adapters/builtin.New`.
- [x] Make `pkg/adapters/runtime.NewRegistry` construct an empty public registry.
- [x] Add `internal/controlplaneapi` for the private `/policy` and `/probe` JSON
  contracts.
- [x] Add `internal/enginebinding` for generic pod-binding annotations and
  parsing.
- [x] Remove the unshipped CacheBackend legacy fields, compatibility readers,
  conversion/defaulting branches, and canonical-versus-legacy behavior.
- [x] Make structured remote-storage `Binding` support and injection a required
  part of the public runtime adapter contract.

The baseline passes `go test ./...` and `git diff --check`.

## Current package classification

| Current package | Classification | Owner / rationale | Planned target |
|---|---|---|---|
| `pkg/adapters/backend` | Supported | Remote-storage provider and binding extension contracts | Keep, narrow to contract-only code |
| `internal/adapters/builtin/storage` | Internal | Shipping provider implementations | Keep |
| `pkg/adapters/runtime` | Supported | Runtime extension interfaces and registry | Keep, narrow to contract-only code |
| `internal/adapters/builtin/runtime` | Internal | Shipping vLLM/SGLang implementations and engine wire rendering | Keep |
| `pkg/adapters/engine` | Internalize | Subscriber-side event ingest, metrics, and reporting | `internal/subscriber` |
| `pkg/adapters/engineclient` | Internalize by default | Canary/harness client with no current external consumer | `internal/engineclient` |
| `pkg/fingerprint` | Supported | Language-neutral fingerprint contract used across integrations | Keep |
| `pkg/tokenize` | Supported | Optional tokenizer boundary, including the tagged cgo implementation | Keep |
| `pkg/index` | Internalize | Server-owned mutable cache-state implementation | `internal/index` |
| `pkg/server` | Internalize | Server binary implementation | `internal/server` |
| `pkg/server/auth` | Internalize | Server-owned HTTP authentication | `internal/server/auth` |
| `pkg/server/proto/...` | Migrate | Generated public gRPC API under a server-owned path | `gen/inferencecache/v1alpha1` |
| `pkg/cli/doctor/...` | Internalize | `cmd/inferencecache` implementation | `internal/cli/doctor` |
| `pkg/render` | Remove placeholder | Empty server-owned placeholder with no implementation | Delete; create `internal/server/render` when implemented |
| `pkg/testing` | Internalize | In-repository envtest helpers | `internal/testutil` |
| `pkg/version` | Internalize | Repository binary build metadata | `internal/version` |

Every completed migration must update package documentation and repository docs
in the same commit. A remaining `pkg/` package must explicitly document its
external consumer or supported extension contract.

## Target source layout

```text
api/
└── v1alpha1/

cmd/
├── controller/
├── inferencecache/
├── kvevent-subscriber/
└── server/

gen/
└── inferencecache/
    └── v1alpha1/

internal/
├── adapters/
│   └── builtin/
│       ├── runtime/
│       │   ├── vllm_lmcache.go
│       │   ├── vllm_lmcache_wire.go
│       │   ├── sglang_lmcache.go
│       │   ├── sglang_lmcache_wire.go
│       │   ├── sglang_hicache.go
│       │   ├── subscriber.go
│       │   └── lmcachecheck.go
│       └── storage/
│           ├── redis.go
│           ├── lmcache_server.go
│           └── mooncake.go
├── cli/
│   └── doctor/
│       ├── checks/
│       └── output/
├── controller/
├── controlplaneapi/
│   ├── policy.go
│   ├── probe.go
│   └── snapshot.go
├── enginebinding/
├── engineclient/
├── index/
├── server/
│   └── auth/
├── subscriber/
├── testutil/
├── version/
└── webhook/
    ├── pod/
    └── v1alpha1/

pkg/
├── adapters/
│   ├── backend/
│   └── runtime/
├── fingerprint/
└── tokenize/

proto/
└── inferencecache/
    └── v1alpha1/

test/
├── fake-engine/
└── reference-stack/
```

`internal/adapters/builtin/runtime` intentionally remains one flat Go package
for now. The combination-based filenames make ownership clear, and the current
production implementation is not large enough to justify engine-specific
subpackages. Split it only when a real dependency boundary appears, not merely
because more files are added.

## Phase 0: complete contract cleanup

Phase 0 was intentionally completed before the structural file moves. These
were visible contract changes, not refactors, and inference-cache had not been
formally deployed, so no resource conversion or compatibility forwarding was
added.

### 0.1 Remove unshipped CacheBackend compatibility

- [x] Make `spec.runtime` required and keep the served CRD on the typed runtime,
  cache, remote-storage, observation, and provider-resource hierarchy.
- [x] Remove `spec.integration.engine`, `spec.backendConfig`, top-level
  `spec.resources`, and `spec.integration.firstEventTimeout` from the Go API and
  generated CRD.
- [x] Remove `UsesCanonicalCacheHierarchy` and all read-time legacy fallbacks,
  conversion/defaulting branches, and canonical-versus-legacy controller and
  adapter paths.
- [x] Move the first-event timeout default to
  `spec.observation.firstEventTimeout` and update samples, smoke checks, and
  operator documentation.
- [x] Regenerate deepcopy and CRD output and run the full Go test suite.

### 0.2 Make structured binding the runtime adapter contract

- [x] Require every `KVCacheRuntimeAdapter` to implement
  `SupportsBinding(*backend.Binding)`.
- [x] Pass `*backend.Binding` directly to `InjectEngineConfig` and
  `InjectRouterConfig`; a nil binding has the single meaning “host-only.”
- [x] Remove the optional `RemoteBindingAdapter` and `EndpointRequirement`
  capabilities and the endpoint-string fallback helpers.
- [x] Update the reference adapter, all shipping adapters, admission,
  reconciliation, pod injection, tests, and extension documentation together.
- [x] Keep provider lifecycle independent: backend providers render storage and
  its protocol, while runtime adapters only accept and consume the resulting
  binding.

Phase 0 does not alter protobuf compatibility, legacy vLLM metric names,
Kubernetes Event API reading, or the LookupRoute wire paths. Those are separate
released or ecosystem-facing contracts and are outside this repository
structure plan.

## Phase A: finish ownership boundaries

Complete this phase before splitting large implementation files. This prevents
the same code from being repeatedly moved and rewritten.

### A1. Narrow the public adapter surface

Proposed commit:

```text
refactor(adapters): narrow public adapter contracts
```

- [ ] Move shipping `Options`, subscriber image/server configuration, and
  subscriber sidecar rendering from `pkg/adapters/runtime` into
  `internal/adapters/builtin` or its runtime implementation package.
- [ ] Keep the runtime interfaces, registry, runtime identifiers,
  supported-pair types, required structured-binding contract, and required
  cross-component wire contracts public.
- [ ] Convert the concrete reference adapter into a Go example or test fixture
  so it documents the extension contract without expanding the production API.
- [ ] Keep the LMCache kernel-check implementation in
  `internal/adapters/builtin/runtime/lmcachecheck.go`.
- [ ] Preserve registry selection, pod mutation, and admission behavior.
- [ ] Update documentation that still references the pre-refactor adapter paths.

This commit narrows source-level API exposure but must not redesign the adapter
contract.

### A2. Move generated gRPC bindings

Proposed commit:

```text
refactor(proto): move generated grpc API under gen
```

- [ ] Change `go_package` to
  `github.com/cachebox-project/inference-cache/gen/inferencecache/v1alpha1`.
- [ ] Regenerate the Go protobuf and gRPC bindings under `gen/`.
- [ ] Update all server, subscriber, test, and tool imports in the same commit.
- [ ] Update generation and generated-drift checks to treat `gen/` as the only
  Go output target.
- [ ] Remove `pkg/server/proto`.
- [ ] Preserve the protobuf package, service names, field numbers, and wire
  behavior.

Because inference-cache has not been formally deployed, the default plan is not
to retain an old-import forwarding package. This is still a Go source import
change and must be called out in release notes if published externally before
the migration lands.

### A3. Extract the snapshot HTTP contract

Proposed commit:

```text
refactor(controlplane): extract snapshot wire contract
```

- [ ] Add `internal/controlplaneapi/snapshot.go` for the `/snapshot` JSON DTOs.
- [ ] Keep mutable index domain types separate from the HTTP representation.
- [ ] Map index state to the HTTP DTO in the server boundary.
- [ ] Update the controller poller to import `internal/controlplaneapi`, not the
  index implementation.
- [ ] Add JSON wire-shape tests before moving the index package.

### A4. Internalize the mutable cache index

Proposed commit:

```text
refactor(index): internalize the cache index
```

- [ ] Move `pkg/index` to `internal/index`.
- [ ] Update the server, tests, and `hack/index-sizing` imports.
- [ ] Preserve ingest, lookup, ranking, quota, TTL, eviction, and soft-state
  behavior.
- [ ] Do not split `index.go` in this commit.

### A5. Internalize the server implementation

Proposed commit:

```text
refactor(server): internalize the server implementation
```

- [ ] Move `pkg/server` to `internal/server`.
- [ ] Move `pkg/server/auth` to `internal/server/auth` and retain it as a
  cohesive security-focused subpackage.
- [ ] Update `cmd/server` and integration-test imports.
- [ ] Remove temporary `internal/controlplaneapi` type and constant aliases from
  the server package after all callers use the neutral owner directly.
- [ ] Preserve HTTP routes, gRPC methods, metrics, TLS, authentication, and
  fail-open behavior.
- [ ] Do not split server implementation files in this commit.

### A6. Internalize the KV-event subscriber implementation

Proposed commit:

```text
refactor(subscriber): internalize kv event ingestion
```

- [ ] Move `pkg/adapters/engine` to `internal/subscriber`.
- [ ] Keep `cmd/kvevent-subscriber` as a thin composition and lifecycle layer.
- [ ] Preserve ZMQ decoding, positional fingerprinting, metrics scraping,
  batching, reconnect, gRPC reporting, and fail-soft behavior.
- [ ] Keep tests and testdata beside the implementation.

### A7. Internalize the doctor CLI implementation

Proposed commit:

```text
refactor(cli): internalize doctor implementation
```

- [ ] Move `pkg/cli/doctor` to `internal/cli/doctor`.
- [ ] Preserve the existing `checks` and `output` subpackages.
- [ ] Preserve CLI flags, finding codes, JSON field names, output formats, and
  exit-code behavior.

The CLI output is a user-facing contract even though the Go package is private.

### A8. Internalize repository support packages

Proposed commit:

```text
refactor(repo): internalize repository support packages
```

- [ ] Move `pkg/testing` to `internal/testutil`.
- [ ] Move `pkg/version` to `internal/version`.
- [ ] Update Makefile `-ldflags` package paths.
- [ ] Delete the empty `pkg/render` placeholder.
- [ ] Create `internal/server/render` only when `RenderTemplate` receives a real
  implementation.

### A9. Reclassify the engine egress client

Proposed commit:

```text
refactor(engineclient): internalize the canary engine client
```

- [ ] Confirm that there is still no external SDK consumer.
- [ ] Move `pkg/adapters/engineclient` to `internal/engineclient` by default.
- [ ] Remove or explicitly isolate the unimplemented gRPC placeholder.
- [ ] Retain the OpenAI-compatible canary/harness behavior and tests.

If a concrete external gateway consumer exists before this step, stop and
define the supported SDK contract instead of performing the move automatically.

## Phase B: split large files without creating new package boundaries

This phase is a readability refactor. Keep the existing Go packages and avoid
new interfaces unless a real dependency cycle requires one.

### B1. Split the index implementation

Proposed commit:

```text
refactor(index): split implementation by responsibility
```

Target files:

```text
internal/index/
├── types.go
├── index.go
├── ingest.go
├── lookup.go
├── ranking.go
├── eviction.go
├── snapshot.go
└── accounting.go
```

- [ ] Move tests beside the responsibility they exercise.
- [ ] Keep all types and methods in package `index`.
- [ ] Do not change algorithms, locking, clock behavior, or metrics.

### B2. Split the server gRPC implementation

Proposed commit:

```text
refactor(server): split grpc handlers by responsibility
```

Target files:

```text
internal/server/
├── server.go
├── lookup.go
├── ingest.go
├── proto_mapping.go
├── policy.go
├── probe.go
├── metrics.go
└── auth/
```

- [ ] Separate RPC handlers from protobuf/domain mapping helpers.
- [ ] Keep route policy and lookup response construction cohesive.
- [ ] Preserve gRPC and HTTP wire behavior.

### B3. Split the CacheBackend reconciler

Proposed commit:

```text
refactor(controller): split cachebackend reconciliation flow
```

Target files:

```text
internal/controller/
├── cachebackend_reconciler.go
├── cachebackend_dispatch.go
├── cachebackend_managed.go
├── cachebackend_serverless.go
├── cachebackend_workload.go
└── cachebackend_status.go
```

Existing cohesive files such as `cachebackend_probe.go`,
`cachebackend_kernelcheck.go`, and `cachebackend_server_restart.go` remain
separate.

- [ ] Keep one `controller` package.
- [ ] Preserve reconcile ordering, ownership, status patching, events, and
  requeue behavior.
- [ ] Split large tests along the same responsibilities.

### B4. Split CacheBackend admission rules

Proposed commit:

```text
refactor(webhook): split cachebackend admission rules
```

Target files:

```text
internal/webhook/v1alpha1/
├── cachebackend_defaulter.go
├── cachebackend_validator.go
├── cachebackend_storage_validation.go
├── cachebackend_integration_validation.go
└── cachebackend_override_validation.go
```

- [ ] Keep one `v1alpha1` webhook package.
- [ ] Preserve validation ordering, field paths, error messages, and defaults.
- [ ] Split the large webhook test file by rule family.

### B5. Split CacheBackend API definitions

Proposed commit:

```text
refactor(api): split cachebackend types by concern
```

Target files:

```text
api/v1alpha1/
├── cachebackend_types.go
├── cachebackend_cache_types.go
├── cachebackend_storage_types.go
├── cachebackend_integration_types.go
└── cachebackend_status_types.go
```

- [ ] Keep one `api/v1alpha1` Go package.
- [ ] Preserve all JSON names, kubebuilder markers, schema, defaults, printer
  columns, and generated deepcopy behavior.
- [ ] Regenerate and verify the CRD after moving markers and types.

## Phase C: organize samples, tests, docs, and build assets

Complete the source ownership work first. These moves affect CI and user-facing
paths and should not obscure source-package reviews.

### C1. Classify samples

- [ ] Separate canonical minimal samples from recipes and invalid fixtures.
- [ ] Keep invalid admission fixtures clearly under a test-only directory.
- [ ] Update sample verification to discover the intended categories.
- [ ] Keep user documentation pointed at canonical examples.

### C2. Move executable reference-stack assets under `test/`

- [ ] Move CI-executed scripts, manifests, fake engines, and fixtures from
  `docs/reference-stack` and `cmd/kvevent-fake-engine` under `test/`.
- [ ] Keep explanatory runbooks in `docs/` and link to the executable assets.
- [ ] Update GitHub Actions and Make targets without renaming the public Make
  targets.

### C3. Make documentation canonical

- [ ] Decide which content under `docs/` is canonical.
- [ ] Make `site/` render or consume that source instead of maintaining
  independent copies.
- [ ] Fix stale source-path references as part of every earlier move rather than
  deferring all path updates to this step.

### C4. Split build logic last

- [ ] Split the large Makefile by concern only after source and test paths are
  stable.
- [ ] Candidate includes: `make/tools.mk`, `make/build.mk`, `make/test.mk`, and
  `make/release.mk`.
- [ ] Preserve public targets such as `build`, `test`, `ci`, `pre-pr`, image
  targets, and verification targets.

## Contract decisions outside structural phases

Contract or behavior changes that are not already recorded in Phase 0 must be
reviewed separately and must not be hidden inside file moves.

### Do not introduce runtime plugin loading without a concrete requirement

The CRD enums and built-in composition intentionally make supported integrations
explicit. If a future requirement demands third-party adapters without a custom
controller build, it will require a broader design covering API extensibility,
configuration schema, discovery, trust, versioning, and failure isolation. That
is not part of this refactor.

## Verification requirements

Every commit must run the checks appropriate to its scope and report the actual
results.

Minimum for a pure Go move or file split:

```text
gofmt on changed Go files
go test ./...
git diff --check
```

Additional checks by change type:

| Change | Required verification |
|---|---|
| CRD types or markers | Regenerate deepcopy/CRDs, verify generated diff, run API and webhook tests |
| Protobuf source or output path | Regenerate bindings, protobuf lint, generated-drift checks, full tests |
| Server/index/subscriber move | Package tests, integration tests, full tests |
| CLI move | Doctor output/exit-code tests and full tests |
| Test/reference-stack move | Affected Make target and GitHub workflow command paths |
| End of each phase | `go test -race ./...` or the repository `make ci` gate as practical |

Add a lightweight repository-boundary verification before completing Phase A:

- [ ] Maintain an explicit allow-list of supported `pkg/` packages.
- [ ] Reject production imports from `pkg/` into `internal/`.
- [ ] Reject imports from public adapter contracts into built-in adapters.
- [ ] Reject controller imports of server or mutable-index implementations.
- [ ] Require generated public Go protobuf code to live only under `gen/`.

## Review discipline

For every proposed commit:

1. State whether it is a pure refactor, a source import-path change, or a runtime
   contract/behavior change.
2. Keep unrelated cleanup out of the diff.
3. Preserve existing tests during moves; split tests only in the corresponding
   Phase B readability commit.
4. Update documentation paths in the same commit as code moves.
5. Do not create forwarding packages or compatibility aliases unless a concrete
   released consumer requires them.
6. Stop and reassess if a file move requires new runtime branching, schema
   migration, or a new public dependency.

This sequence keeps the repository continuously working while converging on a
small public API surface, explicit binary ownership, and a structure that can
grow without turning `pkg/` or `internal/` into undifferentiated collections.
