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

> Put repository-private code under the `internal/` component that owns it. Use
> `pkg/` only for APIs intentionally exposed as extension contracts, SDKs, or
> reusable libraries.

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
7. `internal/enginebinding` owns private engine-pod metadata and coordination
   contracts shared by built-in adapters, admission, and controllers.
   Controllers do not import webhook or built-in adapter packages.
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

The seam is an implementation boundary, not a complete third-party integration
platform. An adapter can extend engine wiring within the API and validation
model the repository already exposes. Adding a new runtime, cache, or provider
identifier still requires a custom controller build plus the corresponding API,
CRD, admission, and, where applicable, reconciliation changes. Adding new
configuration semantics likewise remains a core repository or custom-fork
change; implementing `KVCacheRuntimeAdapter` alone does not bypass schema or
admission validation.

This build-time seam is the designated extension point, but its Go source
contract is pre-stable. The Phase 0 audit found no real out-of-tree adapter
consumer, so the structured-binding change intentionally did not retain a
second compatibility interface. A custom controller fork upgrading from an
earlier revision must implement `SupportsBinding(*backend.Binding)` and update
both injection methods to accept `*backend.Binding`; custom forks must pin the
repository revision they build against. Once the project supports its first
external adapter consumer, later source-breaking interface changes require an
explicit versioned contract or migration layer.

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
| `pkg/adapters/engineclient` | Supported, narrow and reclassify | Public inference-engine request client for gateways, benchmarks, and canaries | `pkg/engineclient` |
| `pkg/fingerprint` | Supported | Language-neutral fingerprint contract used across integrations | Keep |
| `pkg/tokenize` | Supported | Optional tokenizer boundary, including the tagged cgo implementation | Keep |
| `pkg/index` | Internalize | Server-owned mutable cache-state implementation | `internal/index` |
| `pkg/server` | Internalize | Server binary implementation | `internal/server` |
| `pkg/server/auth` | Internalize | Server-owned HTTP authentication | `internal/server/auth` |
| `pkg/server/proto/...` | Migrate | Generated public gRPC API under a server-owned path | `gen/inferencecache/v1alpha1` |
| `pkg/cli/doctor/...` | Internalize | `cmd/inferencecache` implementation | `internal/cli/doctor` |
| `pkg/render` | Reserved public API | Planned reusable `RenderTemplate` library; no production implementation or stable Go API yet | Keep and clarify package status |
| `pkg/testing` | Internalize | In-repository envtest helpers | `internal/testutil` |
| `pkg/version` | Internalize | Repository binary build metadata | `internal/version` |

Every completed migration must update package documentation and repository docs
in the same commit. A remaining `pkg/` package must explicitly document its
external consumer, supported extension/SDK contract, or approved reserved-public
status. A reserved package must state that no implementation or compatibility
guarantee exists yet and must not accumulate speculative placeholder APIs.

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
│       ├── options.go
│       ├── registry.go
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
├── canary/
├── enginebinding/
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
├── engineclient/
├── fingerprint/
├── render/
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
- [x] Confirm no real out-of-tree adapter consumer exists and document the
  pre-stable source contract plus the custom-fork migration requirement.
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

- [x] Replace the shipping functional options with a plain
  `internal/adapters/builtin.Options` value containing `SubscriberImage` and
  `PolicyServerGRPCAddress`; `cmd/controller` passes that value to
  `internal/adapters/builtin.New` rather than configuring built-ins through the
  public runtime API.
- [x] In `internal/adapters/builtin.New`, map the composition-level `Options`
  explicitly to a narrower `internal/adapters/builtin/runtime.SubscriberConfig`
  value accepted by each shipping runtime constructor. The runtime subpackage
  must not import its parent `builtin` package.
- [x] Remove the shipping `Option`, `WithSubscriberImage`, and
  `WithPolicyServerGRPCAddress` functional-option API. Move subscriber
  image/server defaults, config normalization, and sidecar rendering to
  `internal/adapters/builtin/runtime/subscriber.go`; preserve existing zero-value
  behavior.
- [x] Keep only the core extension contract public under
  `pkg/adapters/runtime`: `KVCacheRuntimeAdapter`, `RuntimeID`, `Registry`,
  `SupportedPair`, `PairLister`, `ResolveRuntimeID`, and the required
  structured-binding methods. The public contract does not promise that an
  out-of-tree adapter participates in shipping subscriber or kernel-health
  status behavior.
- [x] Move the private `InitContainerProvider` capability,
  `SubscriberContainerName`, LMCache kernel-check container/annotation/mode/
  message/env contracts, mode validation, and shared engine-host-network helper
  to `internal/enginebinding`. Built-in adapters, admission, and controllers
  import that neutral private owner rather than one another's implementation.
- [x] Move built-in vLLM, SGLang, LMCache, and HiCache environment names,
  defaults, endpoint rendering, and other implementation helpers to
  `internal/adapters/builtin/runtime`.
- [x] Keep `Binding`, `Protocol`, `RenderedStorage`, `Provider`, and their
  registries public under `pkg/adapters/backend`. Move provider-owned endpoint
  and binding validation, including the existing external and LMCache endpoint
  validators, from the runtime package to this backend contract package.
- [x] Convert the concrete reference adapter into a Go example or test fixture
  so it documents the extension contract without expanding the production API.
- [x] Replace webhook and built-in tests that import the production reference
  adapter with local test fixtures, and remove migration-only contract aliases.
- [x] Keep the LMCache kernel-check implementation in
  `internal/adapters/builtin/runtime/lmcachecheck.go`.
- [x] Preserve registry selection, pod mutation, and admission behavior.
- [x] Update documentation that still references the pre-refactor adapter paths.

This commit narrows source-level API exposure but must not redesign the adapter
contract. The public seam supports custom build-time implementations within the
existing API and validation model; a new runtime, cache, provider, or config
shape can still require API, CRD, webhook, and controller changes in the custom
fork. It is not a dynamic or schema-extensible plugin mechanism.

### A2. Move generated gRPC bindings

Proposed commit:

```text
refactor(proto): move generated grpc API under gen
```

- [x] Change `go_package` to
  `github.com/cachebox-project/inference-cache/gen/inferencecache/v1alpha1`.
- [x] Regenerate the Go protobuf and gRPC bindings under `gen/`.
- [x] Update all server, subscriber, test, and tool imports in the same commit.
- [x] Move the package documentation and generated-contract tests to the new
  neutral owner.
- [x] Update Makefile generation, coverage exclusions, generated-drift checks,
  CI generated-code checks, review-tool generated-path configuration, and
  gRPC documentation to treat `gen/` as the only Go output target.
- [x] Remove `pkg/server/proto`.
- [x] Preserve the protobuf package, service names, field numbers, and wire
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

- [x] Add `internal/controlplaneapi/snapshot.go` with `Snapshot`,
  `ReplicaSnapshot`, and `TenantSnapshot` as the owners of the `/snapshot` JSON
  field names, optional-field behavior, and controller/server skew contract.
- [x] Keep mutable index domain types separate from the HTTP representation;
  do not use aliases between index state and `controlplaneapi` DTOs.
- [x] Keep `Index.Snapshot()` returning an index-owned domain snapshot and add
  an explicit field-by-field mapping to the HTTP DTO at the server boundary.
- [x] Update the controller poller to import `internal/controlplaneapi`, not the
  index implementation.
- [x] Move exact-key, `omitempty`, and presence-bit/skew tests to
  `internal/controlplaneapi`; keep aggregation, sorting, and accounting
  invariants with the index implementation.
- [x] Add an endpoint-level mapping test before moving the index package.

### A4. Internalize the mutable cache index

Proposed commit:

```text
refactor(index): internalize the cache index
```

- [x] Move `pkg/index` to `internal/index`.
- [x] Update the server, tests, and `hack/index-sizing` imports.
- [x] Preserve ingest, lookup, ranking, quota, TTL, eviction, and soft-state
  behavior.
- [x] Do not split `index.go` in this commit.

### A5. Internalize the server implementation

Proposed commit:

```text
refactor(server): internalize the server implementation
```

- [x] Move `pkg/server` to `internal/server`.
- [x] Move `pkg/server/auth` to `internal/server/auth` and retain it as a
  cohesive security-focused subpackage.
- [x] Update `cmd/server` and integration-test imports.
- [x] Remove temporary `internal/controlplaneapi` type and constant aliases from
  the server package after all callers use the neutral owner directly.
- [x] Preserve HTTP routes, gRPC methods, metrics, TLS, authentication, and
  fail-open behavior.
- [x] Do not split server implementation files in this commit.

### A6. Internalize the KV-event subscriber implementation

Proposed commit:

```text
refactor(subscriber): internalize kv event ingestion
```

- [x] Move `pkg/adapters/engine` to `internal/subscriber`.
- [x] Keep `cmd/kvevent-subscriber` as a thin composition and lifecycle layer.
- [x] Preserve ZMQ decoding, positional fingerprinting, metrics scraping,
  batching, reconnect, gRPC reporting, and fail-soft behavior.
- [x] Keep tests and testdata beside the implementation.

### A7. Internalize the doctor CLI implementation

Proposed commit:

```text
refactor(cli): internalize doctor implementation
```

- [x] Move `pkg/cli/doctor` to `internal/cli/doctor`.
- [x] Preserve the existing `checks` and `output` subpackages.
- [x] Preserve CLI flags, finding codes, JSON field names, output formats, and
  exit-code behavior.

The CLI output is a user-facing contract even though the Go package is private.

### A8. Reclassify repository support packages

Proposed commit:

```text
refactor(repo): reclassify repository support packages
```

- [x] Move `pkg/testing` to `internal/testutil`.
- [x] Move `pkg/version` to `internal/version`.
- [x] Update Makefile `-ldflags` package paths.
- [x] Keep `pkg/render` as the reserved public package for the planned reusable
  `RenderTemplate` implementation.
- [x] Update `pkg/render` documentation to state that no production
  implementation or stable Go API exists yet and that the current server does
  not depend on it.
- [x] Do not add speculative render interfaces or placeholder behavior before
  the implementation requirement is defined.

### A9. Narrow and reclassify the public engine client

Proposed commit:

```text
refactor(engineclient): establish public engine client boundary
```

- [x] Move `pkg/adapters/engineclient` to the neutral public path
  `pkg/engineclient`; this is a Go source import-path change.
- [x] Keep the public package focused on `EngineClient`, `CompletionParams`,
  `Completion`, `OpenAIClient`, `NewOpenAI`, and the supported pre-tokenized
  OpenAI-compatible `/v1/completions` request/response mapping.
- [x] Move `PrefixCacheProbe`, Prometheus scraping helpers, and live canary tests
  to `internal/canary` when they remain reference-stack infrastructure rather
  than general engine-client behavior.
- [x] Delete the unimplemented gRPC client and `ErrNotImplemented` when it has no
  remaining caller. Add a gRPC transport only with a concrete engine consumer
  and validated protocol.
- [x] Preserve token-ID request encoding, zero-temperature semantics, response
  size limits, status/error handling, and completion/usage parsing.
- [x] Document the current boundary explicitly: it does not yet promise
  authentication, retries, endpoint discovery, load balancing, streaming,
  tracing, or a complete OpenAI API SDK.
- [x] Update reference-stack scripts, package documentation, and tests for the
  new public import path and internal canary location.

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

Keep repository-boundary verification lightweight before completing Phase A:

- [x] Require every package under `pkg/` to have package documentation that
  states its intended public role.
- [x] Reject imports of module `internal/` packages from non-test Go code under
  `api/`, `gen/`, or `pkg/`.
- [x] Require generated public Go protobuf code to live only under `gen/`.

Do not add component-by-component import bans or a hard-coded `pkg/` allow-list
in this phase. Use normal code review for the finer-grained dependency rules.

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
