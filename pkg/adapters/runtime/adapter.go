package runtime

import (
	"errors"
	"fmt"
	"strings"

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

	// ObservationSidecar returns the container that observes the engine pod
	// for the cache plane (the KV-event subscriber for vLLM/LMCache), or
	// (nil, nil) when no sidecar is needed for this (engine, backend) pair
	// — for example, an External backend whose lifecycle the controller
	// does not manage, or a future backend that exports observation data
	// some other way. Returning a container does not by itself mutate pod;
	// the Pod webhook appends it after [InjectEngineConfig] (idempotent: if
	// a container with the same Name is already present, the caller skips
	// the append). Identity flags MUST be derived from cache + pod so the
	// CR is the single source of truth — no operator-supplied flags.
	ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error)
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
// accept arbitrary backends (e.g. the in-tree reference adapter) leave it
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
// The CR carries the engine name in Spec.Integration.Engine. When it is
// unset, vLLM is the Phase-1 default — the only engine the shipping
// adapters target — so a CacheBackend that omits the field is treated the
// same way the reconciler used to treat it before C7 landed. Engine values
// are normalised to lower case so common spellings ("vLLM", "VLLM",
// "SGLang") route to the canonical [RuntimeID] constants ([RuntimeVLLM]
// etc.).
func ResolveRuntimeID(cache *cachev1alpha1.CacheBackend) RuntimeID {
	if cache == nil || cache.Spec.Integration == nil || cache.Spec.Integration.Engine == "" {
		return RuntimeVLLM
	}
	return RuntimeID(strings.ToLower(cache.Spec.Integration.Engine))
}

// Options configures the runtime adapters [DefaultRegistry] constructs.
// Zero values are valid: empty PolicyServerGRPCAddress falls back to the
// package default, and empty SubscriberImage disables sidecar auto-attach
// (see the field doc for why).
type Options struct {
	// SubscriberImage is the image reference the vLLM/LMCache adapter
	// uses for the kvevent-subscriber sidecar. Empty (the zero value)
	// **disables** sidecar auto-attach — the adapter returns no sidecar
	// at all. Auto-attach is opt-in by design: a nonexistent default
	// image would put the sidecar container into ImagePullBackOff and
	// keep the engine pod from going Ready. See [DefaultSubscriberImage]
	// for the build-tag operators pin to (or a digest-pinned production
	// image), passed through the controller's --kvevent-subscriber-image
	// flag.
	SubscriberImage string

	// PolicyServerGRPCAddress overrides the host:port the kvevent-
	// subscriber sidecar dials to ReportCacheState. Empty selects the
	// package default ([DefaultPolicyServerGRPCAddress]), which assumes the
	// in-cluster Service produced by config/server installed into the
	// inference-cache-system namespace.
	PolicyServerGRPCAddress string
}

// Option mutates [Options] for callers that prefer the functional-option
// style. Either Options{...} or a chain of Option helpers work.
type Option func(*Options)

// WithSubscriberImage sets [Options.SubscriberImage].
func WithSubscriberImage(image string) Option {
	return func(o *Options) { o.SubscriberImage = image }
}

// WithPolicyServerGRPCAddress sets [Options.PolicyServerGRPCAddress].
func WithPolicyServerGRPCAddress(addr string) Option {
	return func(o *Options) { o.PolicyServerGRPCAddress = addr }
}

// DefaultRegistry returns a Registry pre-populated with the runtime adapters
// this package can install without an import cycle — currently the
// vLLM+LMCache adapter. It deliberately does NOT include the External
// passthrough adapter under pkg/adapters/runtime/external/: that package
// imports this one (for the [KVCacheRuntimeAdapter] interface and the
// [RuntimeID] constants), so registering it here would cycle. The
// production wiring in cmd/controller and both webhook handlers'
// nil-Registry fallbacks explicitly add the External adapter on top, so
// the shipping admission/injection paths agree on the full supported
// set; only direct uses of DefaultRegistry (e.g. some hermetic unit
// tests) see the LMCache-only view.
//
// Options the controller cares about (subscriber sidecar image, policy-server
// address) are passed in via the variadic [Option] helpers; the no-arg form
// preserves the original Phase-1 behaviour and is still used by the
// reconciler/webhook nil-Registry fallback paths.
func DefaultRegistry(opts ...Option) *Registry {
	r := NewRegistry()
	r.Register(NewVLLMLMCacheAdapter(opts...))
	return r
}
