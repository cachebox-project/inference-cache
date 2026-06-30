package external

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// adapter wires engine pods to a pre-existing remote cache the operator
// manages themselves. CacheBackend{type: External} carries the address in
// spec.endpoint; the controller never creates a cache-server Deployment for
// it. The engine wire format matches the managed-LMCache path — same env
// vars and --kv-transfer-config arg — so an engine pod cannot tell whether
// the cache it talks to was provisioned by the controller or by the
// operator out-of-band.
//
// The Supports gate is `runtime == vLLM && type == External`: a Mooncake-
// or SGLang-shaped External cache speaks a different engine wire and will
// land as a separate adapter, the same way managed Mooncake / SGLang
// adapters will live alongside the managed LMCache adapter.
type adapter struct{}

// NewAdapter returns the runtime adapter for CacheBackend{type: External}.
// Wire it into the shared [runtime.Registry] in cmd/controller alongside
// the managed-LMCache adapter so the pod-mutating webhook picks it up for
// engine pods that match an External CR's spec.engineSelector.
func NewAdapter() runtimeadapter.KVCacheRuntimeAdapter {
	return adapter{}
}

// Supports matches vLLM engines against External CacheBackends. Other
// runtime / backend combinations are left for the per-engine managed
// adapter (e.g. vllm+LMCache) or for a future runtime-specific External
// adapter — the External wire is LMCache-compatible today.
func (adapter) Supports(runtime runtimeadapter.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == runtimeadapter.RuntimeVLLM && cache.Spec.Type == cachev1alpha1.CacheBackendTypeExternal
}

// SupportedPairs lets the registry surface this adapter's canonical pair in
// the "no adapter supports the (engine, backend) pair" admission error so
// an operator who mistypes the backend type sees External as a candidate.
func (adapter) SupportedPairs() []runtimeadapter.SupportedPair {
	return []runtimeadapter.SupportedPair{
		{Runtime: runtimeadapter.RuntimeVLLM, Backend: cachev1alpha1.CacheBackendTypeExternal},
	}
}

// ResolveCacheServer returns (nil, nil, nil): the cache server is operator-
// managed and pre-exists, so the controller renders neither a pod nor a
// Service for it. The C2 reconciler already short-circuits on
// type==External before even consulting an adapter (see
// CacheBackendReconciler.dispatch), so this method is a safety net for any
// future code path that goes through Registry.Select for an External CR —
// it must never accidentally provision a placeholder cache-server.
func (adapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("resolve cache server: cache is nil")
	}
	return nil, nil, nil
}

// InjectEngineConfig wires the engine pod to the operator-supplied
// spec.endpoint via the same LMCache engine wire format the managed
// adapter uses (see enginewire.InjectVLLMLMCache). The pod-mutating
// webhook resolves the endpoint type-scoped: for External CRs it
// passes the trimmed cache.Spec.Endpoint (operator-authoritative;
// preferred over status.endpoint so a pod admitting between a
// spec.endpoint update and the reconciler's mirror is wired to the
// fresh address, not the stale one). The adapter itself doesn't
// know which field the caller pulled from — both paths land at the
// same wire — so this method just delegates to the shared helper.
func (adapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	return enginewire.InjectVLLMLMCache(pod, endpoint, cache)
}

// InjectRouterConfig is a no-op for External: the External topology has no
// router component the controller needs to wire. Returning nil keeps the
// interface contract satisfied so a Registry caller can blindly invoke both
// Inject* paths without branching on backend type — per
// [runtimeadapter.KVCacheRuntimeAdapter.InjectRouterConfig]: "backends
// without a router component should return nil without touching pod."
func (adapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = endpoint
	_ = cache
	return nil
}

// ObservationSidecar returns (nil, nil): there is no controller-owned pod
// whose KV events we can subscribe to, and we deliberately do NOT inject a
// subscriber into the engine pod here — the engine talks to an operator-
// managed cache the controller has no observability seam into. A future
// follow-up could surface a scrape-only observation path for External
// caches; until then, [CacheBackend.Status.IndexParticipation] for an
// External backend stays nil unless a separate side-channel populates it.
func (adapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	if cache == nil {
		return nil, fmt.Errorf("observation sidecar: cache is nil")
	}
	if pod == nil {
		return nil, fmt.Errorf("observation sidecar: pod is nil")
	}
	return nil, nil
}

// ReservedArgs returns the engine args the External adapter injects that
// the integration cannot function without. The wire format is identical
// to the managed vLLM+LMCache adapter (both call
// enginewire.InjectVLLMLMCache), so the reserved set must be identical
// too — an operator suppressing `--kv-transfer-config` on an External CR
// would un-wire the LMCache connector exactly the way it would on a
// managed CR, and the cache plane would silently stop routing through
// the operator's pre-existing cache.
func (adapter) ReservedArgs() []string {
	return []string{"--kv-transfer-config"}
}

// ReservedEnv returns the env var names the External adapter injects that
// the integration cannot function without. Same set as the managed
// vLLM+LMCache adapter; the rationale is identical (the engine must
// find the cache at the operator-supplied endpoint, run on the
// LMCache-targeting vLLM codepath, honor the fail-open contract, and pin
// the deterministic NONE_HASH so LMCache reload matches under TP>1).
func (adapter) ReservedEnv() []string {
	return []string{
		enginewire.EnvLMCacheRemoteURL,
		enginewire.EnvVLLMUseV1,
		enginewire.EnvInferenceCacheFailOpen,
		enginewire.EnvPythonHashSeed,
	}
}

// EngineContainerName returns the canonical name of the vLLM engine
// container the adapter mutates. The pod webhook uses this to scope
// engineOverrides edits to the same container InjectEngineConfig writes
// to — overrides land on the engine, not on user-attached sidecars.
func (adapter) EngineContainerName() string { return enginewire.EngineContainerName }

// Compile-time assertion: the adapter implements the full C5 interface.
var _ runtimeadapter.KVCacheRuntimeAdapter = adapter{}
