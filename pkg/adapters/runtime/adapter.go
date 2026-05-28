package runtime

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// RuntimeID identifies an inference-engine family that a runtime adapter
// handles. Values mirror the free-form string carried in
// CacheBackend.Spec.Integration.Engine — this project deliberately does not
// model a ServingRuntime CRD (cf. OEP-0010's *v1beta1.ServingRuntimeSpec), so
// engine identity flows as a plain identifier the reconciler can pass through.
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

	// ResolveCacheServer renders the desired cache-server pod and service for
	// this backend, when one is required. Returning (nil, nil, nil) is valid
	// for backends that colocate with the engine (e.g. an in-process
	// connector) and need no separate cache-server.
	//
	// The split between adapter and reconciler is deliberate:
	//   - The adapter fills PodSpec.Containers / PodSpec.Volumes and
	//     Service.Spec.Ports / Service.Spec.Type — the backend-specific
	//     details that don't depend on the CacheBackend's identity.
	//   - The reconciler fills ObjectMeta (name, namespace, labels, owner
	//     refs), Service.Spec.Selector, replicas, and the workload kind
	//     (Deployment vs StatefulSet) — the identity-dependent fields.
	// An adapter rendering the same containers for two CacheBackends in
	// different namespaces should not have to learn about names.
	ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error)

	// InjectEngineConfig mutates pod so the engine talks to the cache at
	// endpoint. Implementations MUST merge: preserve existing containers,
	// env, args, and volumes; only add or update what they own. Safe to call
	// repeatedly on the same pod.
	InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error

	// InjectRouterConfig mutates a router pod so it can route cache-aware
	// requests through endpoint. Same merge contract as InjectEngineConfig.
	// Backends without a router component should return nil without
	// touching pod.
	InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error
}

// ErrNoAdapter is returned by [Registry.Select] when no registered adapter
// supports a given (runtime, CacheBackend) pair. Admission (cf. C7) translates
// this into a user-visible rejection; the reconciler logs and skips.
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

// DefaultRegistry returns a Registry pre-populated with the runtime adapters
// the controller ships with — currently the vLLM+LMCache adapter. The
// reconciler builds one of these at startup; tests that need a specific
// adapter set should construct their own [Registry] via [NewRegistry] +
// [Registry.Register].
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register(NewVLLMLMCacheAdapter())
	return r
}
