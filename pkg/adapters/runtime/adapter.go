// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

// RuntimeID identifies an inference-engine family that a runtime adapter
// handles. Values are resolved from CacheBackend.Spec.Runtime; this project
// deliberately does not model a ServingRuntime CRD (cf. OEP-0010's
// *v1beta1.ServingRuntimeSpec).
type RuntimeID string

// Canonical runtime identifiers. Adapters are free to support additional
// values; these constants exist so callers (reconciler, admission) share a
// single spelling for the engines we ship with.
const (
	RuntimeVLLM   RuntimeID = "vllm"
	RuntimeSGLang RuntimeID = "sglang"
)

// KVCacheRuntimeAdapter is the controller-side plug-point for wiring an
// inference engine to a cache backend. The interface mirrors OEP-0010
// (KVCacheRuntimeAdapter), with parameters adapted to this repo's types: the
// CacheBackend CR replaces OEP-0010's KVCacheSpec, and the engine family is
// identified by a [RuntimeID] instead of a ServingRuntimeSpec.
//
// Adapters MUST merge into the pod specs they receive — never clobber
// user-provided containers, env vars, or volumes — so an InferenceService
// owner's pod template survives the injection step intact.
type KVCacheRuntimeAdapter interface {
	// Supports reports whether this adapter can wire runtime together with
	// cache. The [Registry] consults Supports to pick an adapter for a
	// (runtime, backend) pair; cache is never nil at the call site.
	Supports(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool

	// SupportsBinding reports whether the adapter accepts the structured remote
	// storage binding. A nil binding means host-only operation. Admission and
	// reconciliation call this before injection so unsupported runtime/provider
	// combinations fail at the contract boundary.
	SupportsBinding(binding *backendadapter.Binding) bool

	// InjectEngineConfig mutates pod so the engine uses binding. Implementations
	// MUST merge: preserve existing containers, env, args, and volumes; only add
	// or update what they own. Safe to call repeatedly on the same pod. A nil
	// binding means host-only operation.
	InjectEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error

	// InjectRouterConfig mutates a router pod so it can route cache-aware
	// requests through binding. Same merge contract as InjectEngineConfig.
	// Backends without a router component should return nil without
	// touching pod.
	InjectRouterConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error

	// ObservationSidecar returns the container that observes the engine pod
	// for the cache plane (the KV-event subscriber for vLLM/LMCache), or
	// (nil, nil) when no sidecar is needed for this (engine, backend) pair
	// — for example, a future backend that exports observation data some other
	// way. Returning a
	// container does not by itself mutate pod;
	// the Pod webhook appends it after [InjectEngineConfig] (idempotent: if
	// a container with the same Name is already present, the caller skips
	// the append). Identity flags MUST be derived from cache + pod so the
	// CR is the single source of truth — no operator-supplied flags.
	ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error)

	// ReservedArgs returns the leading flag tokens (e.g.
	// "--kv-transfer-config") this adapter will inject as part of
	// [InjectEngineConfig] and that the integration cannot function
	// without. The validating webhook for CacheBackend rejects any
	// spec.integration.engineOverrides.{args,suppressArgs} entry whose
	// leading flag token overlaps this list, so the operator cannot
	// silently un-wire the integration by adding or suppressing one of
	// these.
	//
	// Tunable flags the operator may legitimately want to change MUST NOT
	// appear here unless they have a dedicated typed CacheBackend field that
	// is the integration's single source of truth. The list is exact: matching
	// is per-leading-flag-token, so equality is the contract (no
	// prefix/wildcards).
	ReservedArgs() []string

	// ReservedEnv returns the env var Names this adapter will inject as
	// part of [InjectEngineConfig] and that the integration cannot
	// function without. The validating webhook for CacheBackend rejects
	// any spec.integration.engineOverrides.{env,suppressEnv} entry whose
	// Name overlaps this list.
	//
	// Tunables the operator may legitimately want to change (perf knobs,
	// mode toggles, etc.) MUST NOT appear here. Matching is per-Name
	// (exact, case-sensitive).
	ReservedEnv() []string

	// EngineContainerName returns the canonical container Name this
	// adapter targets when mutating an engine pod. The pod webhook uses
	// it to find the container [InjectEngineConfig] modified so it can
	// apply spec.integration.engineOverrides to the same Args / Env.
	// Empty ("") signals that the adapter has no fixed engine-container
	// name (e.g. the reference adapter writes to every container) — the
	// webhook skips override application in that case.
	EngineContainerName() string
}

const (
	// AnnotationLMCacheConnectorProfile is the runtime-owner declaration of the
	// engine-side LMCache connector API implemented by the workload image.
	AnnotationLMCacheConnectorProfile = "inferencecache.io/lmcache-connector-profile"
	// AnnotationLMCacheClientVersion declares the LMCache client package version
	// validated by the runtime owner's image pipeline.
	AnnotationLMCacheClientVersion = "inferencecache.io/lmcache-client-version"
)

// LMCacheConnectorRequirement is the capability contract an MP runtime adapter
// requires from an inference workload. It names an interface profile rather
// than an image, so any inference system can provide a compatible image without
// CacheBackend owning or allowlisting that image.
type LMCacheConnectorRequirement struct {
	Profile       string
	ClientVersion string
}

// LMCacheMPRuntimeAdapter is the Phase-1 gate for adapters that understand the
// final typed LMCache topology. Legacy adapters intentionally do not implement
// it: the Pod webhook then admits a new MP Pod unmodified instead of silently
// applying the legacy in-process/flat-field wire. Phases 2-4 implement this
// interface as the shared renderer and runtime-specific MP adapters land.
type LMCacheMPRuntimeAdapter interface {
	KVCacheRuntimeAdapter

	// ConnectorRequirement returns the workload-owned capability declaration
	// required by this adapter.
	ConnectorRequirement(*cachev1alpha1.CacheBackend) LMCacheConnectorRequirement

	// ValidateMPEnginePod validates version/parallelism/command/resource
	// constraints visible only on the concrete engine Pod. An unclassifiable
	// Pod returns an error and is never silently injected.
	ValidateMPEnginePod(*corev1.Pod, *cachev1alpha1.CacheBackend) error
}

// ValidateConnectorDeclaration compares the runtime owner's Pod annotations
// with an adapter's required connector contract. This is deliberately a
// declaration check, not registry/image introspection; build-time probes and
// digest pinning bind the claim to image contents outside the admission path.
func ValidateConnectorDeclaration(pod *corev1.Pod, requirement LMCacheConnectorRequirement) error {
	if pod == nil {
		return fmt.Errorf("engine pod is nil")
	}
	annotations := pod.GetAnnotations()
	if got := annotations[AnnotationLMCacheConnectorProfile]; got != requirement.Profile {
		return fmt.Errorf("annotation %s=%q, want %q", AnnotationLMCacheConnectorProfile, got, requirement.Profile)
	}
	if got := annotations[AnnotationLMCacheClientVersion]; got != requirement.ClientVersion {
		return fmt.Errorf("annotation %s=%q, want %q", AnnotationLMCacheClientVersion, got, requirement.ClientVersion)
	}
	return nil
}

// ErrNoAdapter is returned by [Registry.Select] when no registered adapter
// supports a given (runtime, CacheBackend) pair. An admission validator can
// translate this into a user-visible rejection; the reconciler logs and skips.
var ErrNoAdapter = errors.New("no runtime adapter supports the runtime/backend pair")

// Registry holds the set of known [KVCacheRuntimeAdapter] implementations and
// resolves one for a given (runtime, backend) pair. The zero value is ready
// to use; adapters are consulted in registration order and the first match
// wins, so callers can layer specific adapters before generic ones.
type Registry struct {
	adapters []KVCacheRuntimeAdapter
}

// NewRegistry returns an empty Registry. Equivalent to the zero value;
// provided for readability at call sites.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds adapter to the Registry. Adapters are consulted in the order
// they were registered when [Registry.Select] iterates. Registering nil is a
// no-op so callers building a registry from optional inputs need not branch.
func (r *Registry) Register(adapter KVCacheRuntimeAdapter) {
	if adapter == nil {
		return
	}
	r.adapters = append(r.adapters, adapter)
}

// Select returns the first registered adapter that Supports the given runtime
// and CacheBackend, or [ErrNoAdapter] if none does. cache must be non-nil;
// passing nil yields [ErrNoAdapter] (cleanly rejected rather than panicking,
// since admission paths may surface this error).
func (r *Registry) Select(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) (KVCacheRuntimeAdapter, error) {
	if cache == nil {
		return nil, fmt.Errorf("%w: runtime=%q backend=<nil>", ErrNoAdapter, runtime)
	}
	for _, a := range r.adapters {
		if a.Supports(runtime, cache) {
			return a, nil
		}
	}
	return nil, fmt.Errorf("%w: runtime=%q backend=%q", ErrNoAdapter, runtime, cache.Spec.Type)
}

// Len reports the number of registered adapters. Mostly useful in tests.
func (r *Registry) Len() int { return len(r.adapters) }

// SupportedPair names an (engine runtime, cache backend type) combination that
// at least one registered adapter accepts. It is returned by
// [Registry.SupportedPairs] so admission validators can list the user's
// options when they ask for an unsupported pair.
type SupportedPair struct {
	Runtime RuntimeID
	Backend cachev1alpha1.CacheBackendType
}

// String renders a SupportedPair in the "<runtime>/<backend>" form used in
// user-facing admission messages.
func (p SupportedPair) String() string {
	return fmt.Sprintf("%s/%s", p.Runtime, p.Backend)
}

// PairLister is the optional interface a [KVCacheRuntimeAdapter] implements
// when it can enumerate the concrete (runtime, backend) pairs it accepts.
// Adapters that match a single canonical pair (the vLLM+LMCache adapter, the
// future SGLang HiCache adapter) implement it; permissive adapters that
// accept arbitrary backends can leave it
// off and simply do not contribute to [Registry.SupportedPairs].
type PairLister interface {
	SupportedPairs() []SupportedPair
}

// SupportedPairs returns the union of pairs reported by every registered
// adapter that implements [PairLister], in registration order, deduplicated.
// Adapters without the optional method are skipped (they do not contribute to
// the user-facing list). The result is intended for admission error messages,
// not for routing decisions — callers that need to test a specific pair must
// still go through [Registry.Select].
func (r *Registry) SupportedPairs() []SupportedPair {
	seen := map[SupportedPair]struct{}{}
	var out []SupportedPair
	for _, a := range r.adapters {
		lister, ok := a.(PairLister)
		if !ok {
			continue
		}
		for _, p := range lister.SupportedPairs() {
			if _, dup := seen[p]; dup {
				continue
			}
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}

// ResolveRuntimeID picks the [RuntimeID] every layer (admission validator,
// reconciler, pod-mutating webhook) consults the [Registry] with for a given
// CacheBackend. Centralising the rule here keeps the three layers from
// drifting: whatever pair admission admits must be the pair the reconciler
// renders and the pod webhook injects, so the three callers must read the
// CR identically.
//
// The CR carries the runtime identity in spec.runtime. The schema restricts
// persisted values to the supported case-sensitive enum.
func ResolveRuntimeID(cache *cachev1alpha1.CacheBackend) RuntimeID {
	if cache == nil {
		return ""
	}
	return RuntimeID(strings.ToLower(string(cache.Spec.Runtime)))
}
