package runtime

import (
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	provideradapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend/provider"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// vllmMooncakeAdapter wires vLLM engine pods to the Mooncake store that
// a provider adapter resolves. InjectEngineConfig adds the --kv-transfer-config
// arg and LMCACHE_* env vars to the vLLM container via the shared
// LMCache-connector wire (merging, never clobbering);
// ObservationSidecar returns the same kvevent-subscriber container the LMCache
// adapter does (the KV-event stream is engine-side, so the sidecar shape is
// identical) so the engine pod auto-attaches to the policy server with no
// out-of-band steps.
//
// Why the engine wire is the LMCache connector and not vLLM's native
// MooncakeStoreConnector: the native connector is configured exclusively
// through a MOONCAKE_CONFIG_PATH JSON file (it has no env-var surface for the
// master address), and the pod-mutating webhook can only inject env + args —
// it cannot write a file into a user-owned engine container. Routing the
// controller-resolved master endpoint through LMCACHE_REMOTE_URL=
// mooncakestore://… is the only path that lets status.endpoint reach the engine
// via injection alone, and it matches the locked design decision that Mooncake
// "fits the lm://-style RemoteBackend wire" (docs/design/lmcache-server-persistence.md).
// The native connector
// remains available to operators who pre-bake their own config file; this
// adapter targets the auto-wired path.
type vllmMooncakeAdapter struct {
	// subscriberImage is the image the kvevent-subscriber sidecar runs.
	// Empty (the default) disables sidecar auto-attach — ObservationSidecar
	// returns nil — so an unconfigured controller install doesn't push
	// engine pods into ImagePullBackOff on a nonexistent default image.
	subscriberImage string
	// policyServerGRPCAddress overrides the default in-cluster Service DNS
	// the sidecar dials to ReportCacheState. Empty falls back to
	// [DefaultPolicyServerGRPCAddress].
	policyServerGRPCAddress string
}

// NewVLLMMooncakeAdapter returns the adapter that wires vLLM engine pods to a
// Mooncake CacheBackend. The optional [Option] helpers let the controller pin
// the subscriber sidecar's image + policy-server target (shared with the
// vLLM+LMCache adapter via [NewCoreRegistry]); the no-arg form reproduces the
// package defaults and keeps tests + the nil-Registry fallback paths working.
func NewVLLMMooncakeAdapter(opts ...Option) KVCacheRuntimeAdapter {
	var cfg Options
	for _, o := range opts {
		o(&cfg)
	}
	return vllmMooncakeAdapter{
		subscriberImage:         cfg.SubscriberImage,
		policyServerGRPCAddress: cfg.PolicyServerGRPCAddress,
	}
}

// Supports matches vLLM runtimes against a Mooncake CacheBackend. Any other
// (runtime, backend) combination is left for another adapter; admission
// surfaces unsupported pairs as ErrNoAdapter.
func (vllmMooncakeAdapter) Supports(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == RuntimeVLLM && cache.Spec.Type == cachev1alpha1.CacheBackendTypeMooncake
}

// SupportedPairs lets the registry expose this adapter's canonical pair to
// admission error messages so a user who asked for an unsupported pair can
// see what they could have asked for instead.
func (vllmMooncakeAdapter) SupportedPairs() []SupportedPair {
	return []SupportedPair{{Runtime: RuntimeVLLM, Backend: cachev1alpha1.CacheBackendTypeMooncake}}
}

// ReservedArgs returns the leading flag tokens this adapter injects and that
// the integration cannot function without. Mooncake speaks the LMCache
// connector, so the reserved arg is the same as the vLLM+LMCache adapter's:
//
//   - "--kv-transfer-config" is the LMCache connector configuration the engine
//     reads at startup; suppressing it means no Mooncake wiring at all.
func (vllmMooncakeAdapter) ReservedArgs() []string {
	return []string{defaultEngineKVTransferConfigArg}
}

// ReservedEnv returns the env var names this adapter injects and that the
// integration cannot function without. Identical to the vLLM+LMCache adapter's
// set because Mooncake reuses the LMCache connector wire:
//
//   - LMCACHE_REMOTE_URL is the mooncakestore:// address of the rendered
//     Mooncake master; an override re-points the engine at a different store
//     than the CR resolved to.
//   - VLLM_USE_V1 selects the vLLM v1 codepath the LMCache connector targets.
//   - INFERENCECACHE_FAIL_OPEN mirrors spec.integration.failOpen onto the pod;
//     allowing an override would silently desync the pod from the CR contract.
//   - PYTHONHASHSEED pins the deterministic NONE_HASH that seeds vLLM's
//     prefix-cache block-hash chain across the scheduler + TP worker processes;
//     an override re-randomizes it under TP>1 and reload silently 0-hits.
//
// Tunables (LMCACHE_CHUNK_SIZE / LMCACHE_REMOTE_SERDE / LMCACHE_LOCAL_CPU /
// LMCACHE_MAX_LOCAL_CPU_SIZE) are perf/mode knobs the operator may legitimately
// want to change and are deliberately NOT reserved.
func (vllmMooncakeAdapter) ReservedEnv() []string {
	return []string{
		EnvLMCacheRemoteURL,
		EnvVLLMUseV1,
		EnvInferenceCacheFailOpen,
		EnvPythonHashSeed,
	}
}

// EngineContainerName returns [EngineContainerName] — the canonical name the
// vLLM engine container carries on a pod the adapter mutates. The pod webhook
// resolves the override target via this method so admission overrides land on
// the same container [vllmMooncakeAdapter.InjectEngineConfig] modified.
func (vllmMooncakeAdapter) EngineContainerName() string { return EngineContainerName }

// ResolveCacheServer is the pre-separation compatibility renderer. Production
// provider lifecycle resolves through pkg/adapters/backend/provider.
func (vllmMooncakeAdapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return provideradapter.ResolveMooncakeServer(cache)
}

// InjectEngineConfig adds the LMCache connector arg and LMCACHE_* env to the
// vLLM container in pod, delegating to the shared engine-wire helper with the
// mooncakestore:// remote-URL scheme. The merge contract (preserve existing
// args/env, idempotent, sidecars untouched) is identical to the vLLM+LMCache
// path — see [enginewire.InjectVLLMMooncake].
//
// spec.integration.role maps onto LMCache's kv_role exactly as for the LMCache
// adapter: ReadOnly → kv_consumer, WriteOnly → kv_producer, ReadWrite (and
// unset / unknown) → kv_both.
//
// When spec.integration.engineHostNetwork is set, the engine pod is also moved
// onto the host network. Mooncake's transfer engine is a peer-to-peer mesh: the
// master returns a directory pointer and the engine then dials a real node IP on
// a dynamically negotiated port, which a CNI overlay pod IP cannot reach. That
// move is gated on the operator's explicit opt-in rather than applied by default:
// hostNetwork is a privilege, and because mutating webhooks run BEFORE Pod
// Security validation, injecting it unasked would turn a working engine pod into
// one a "restricted" namespace rejects — with an error naming Pod Security rather
// than this controller. Until the operator opts in, admission warns that the
// backend will report Ready while transferring nothing.
func (vllmMooncakeAdapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	if err := enginewire.InjectVLLMMooncake(pod, endpoint, cache); err != nil {
		return err
	}
	injectMooncakeEngineHostNetwork(pod, cache)
	return nil
}

func injectMooncakeEngineHostNetwork(pod *corev1.PodSpec, cache *cachev1alpha1.CacheBackend) {
	if EngineHostNetworkRequested(cache) {
		pod.HostNetwork = true
		// A hostNetwork pod otherwise inherits the node's resolver; keep cluster DNS
		// so the master's Service name still resolves from the engine.
		pod.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
}

// EngineHostNetworkRequested reports whether the operator opted engine pods bound
// to this backend into host networking. Nil-safe: spec.integration is optional.
// Exported so admission can enforce that the opt-in only appears on a backend
// whose data plane actually needs it, rather than sitting inert.
func EngineHostNetworkRequested(cache *cachev1alpha1.CacheBackend) bool {
	return cache != nil && cache.Spec.Integration != nil && cache.Spec.Integration.EngineHostNetwork
}

// InjectRouterConfig is a no-op for Mooncake: the topology has no router
// component the controller needs to wire. Returning nil keeps the interface
// contract satisfied so a Registry caller can blindly invoke both Inject* paths
// per-pod without branching on backend type — per
// [KVCacheRuntimeAdapter.InjectRouterConfig]: "backends without a router
// component should return nil without touching pod."
func (vllmMooncakeAdapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = endpoint
	_ = cache
	return nil
}

// ObservationSidecar returns the kvevent-subscriber container the Pod webhook
// appends to a vLLM engine pod. The subscriber observes vLLM's own ZMQ
// KV-event stream, which is independent of the L2 store, so the container is
// byte-identical to the vLLM+LMCache adapter's — both delegate to the shared
// [RenderSubscriberSidecar] with the vLLM engine dialect (--hash-scheme=vllm,
// the vLLM ZMQ PUB port). See that helper for the full contract (opt-in image
// gate, required model id, downward-API identity, --ignore-block-removed
// rationale for L2 tiers).
func (a vllmMooncakeAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return RenderSubscriberSidecar(SubscriberSidecarParams{
		Image:            a.subscriberImage,
		ServerAddr:       a.policyServerGRPCAddress,
		Cache:            cache,
		Pod:              pod,
		HashScheme:       subscriberHashScheme,
		EngineZMQPortStr: defaultEngineZMQPortStr,
	})
}

// Compile-time assertion: the adapter implements the full C5 interface.
var _ KVCacheRuntimeAdapter = vllmMooncakeAdapter{}
