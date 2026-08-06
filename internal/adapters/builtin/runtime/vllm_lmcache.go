package runtime

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// vLLM-specific kvevent-subscriber wiring. The subscriber image and
// policy-server address defaults live in lmcache_shared.go.
const (
	// vLLM engine convention: the KV-event ZMQ PUB endpoint binds on :5557 by
	// default (the reference stack's --kv-events-config sets
	// endpoint=tcp://*:5557). Parameterising via the adapter (not hardcoding in
	// the webhook) lets SGLang or another engine adapter pick a different port
	// without touching the webhook.
	vllmDefaultEngineZMQPortStr = "5557"

	// subscriberHashScheme is the canonical hash-scheme tag the vLLM subscriber
	// carries. Hard-coded for this adapter (vLLM's block-hash scheme is distinct
	// from SGLang's, and the cache plane keys on the scheme to keep them from
	// collapsing).
	vllmSubscriberHashScheme = "vllm"
)

// vllmLMCacheAdapter wires vLLM engine pods to an LMCache engine cache and an
// optional remote binding resolved independently by a provider adapter.
// InjectEngineConfig adds the --kv-transfer-config arg and LMCACHE_* env vars
// to the vLLM container, merging with what the pod template already carries;
// ObservationSidecar returns the kvevent-subscriber container the webhook
// appends so the engine pod auto-attaches to the policy server.
//
// This adapter wires vLLM+LMCache, including Mooncake remote bindings via the
// mooncakestore:// protocol. SGLang+LMCache shares
// the observation sidecar but uses its own MP engine wire and a Redis provider
// binding rather than the standalone lmcache-server.
type vllmLMCacheAdapter struct {
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

// NewVLLMLMCacheAdapter returns the adapter that wires vLLM engine pods to an
// LMCache CacheBackend. The optional [adapterruntime.Option] helpers let the controller pin
// the subscriber sidecar's image + policy-server target; the no-arg form
// reproduces the package defaults and keeps tests + the nil-Registry
// fallback paths working.
func NewVLLMLMCacheAdapter(opts ...adapterruntime.Option) adapterruntime.KVCacheRuntimeAdapter {
	var cfg adapterruntime.Options
	for _, o := range opts {
		o(&cfg)
	}
	return vllmLMCacheAdapter{
		subscriberImage:         cfg.SubscriberImage,
		policyServerGRPCAddress: cfg.PolicyServerGRPCAddress,
	}
}

// Supports matches vLLM runtimes against an LMCache CacheBackend. Any other
// (runtime, backend) combination is left for another adapter — a future
// admission validator surfaces unsupported pairs as ErrNoAdapter.
func (vllmLMCacheAdapter) Supports(runtime adapterruntime.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == adapterruntime.RuntimeVLLM &&
		cache.Spec.EffectiveCacheType() == cachev1alpha1.CacheBackendTypeLMCache
}

// SupportedPairs lets the registry expose this adapter's canonical pair to
// admission error messages so a user who asked for an unsupported pair can
// see what they could have asked for instead.
func (vllmLMCacheAdapter) SupportedPairs() []adapterruntime.SupportedPair {
	return []adapterruntime.SupportedPair{{Runtime: adapterruntime.RuntimeVLLM, Backend: cachev1alpha1.CacheBackendTypeLMCache}}
}

// ReservedArgs returns the leading flag tokens this adapter injects and that
// the LMCache integration cannot function without. The validating webhook
// blocks an spec.integration.engineOverrides entry that tries to override or
// suppress any of these so the operator cannot silently un-wire the connector.
//
//   - "--kv-transfer-config" is the LMCache connector configuration the engine
//     reads at startup; suppressing it means no LMCache wiring at all.
//
// Other tunables the operator may legitimately want to change (e.g. perf
// connector-tuning knobs are deliberately NOT reserved.
func (vllmLMCacheAdapter) ReservedArgs() []string {
	return []string{defaultEngineKVTransferConfigArg}
}

// EngineContainerName returns [EngineContainerName] — the canonical name the
// vLLM engine container carries on a pod the adapter mutates. The pod
// webhook resolves the override target via this method so admission overrides
// land on the same container [InjectEngineConfig] modified.
func (vllmLMCacheAdapter) EngineContainerName() string { return EngineContainerName }

// ReservedEnv returns the env var names this adapter injects and that the
// LMCache integration cannot function without:
//
//   - LMCACHE_REMOTE_URL is the address of the rendered cache server; an
//     override re-points the engine at a different cache than the CR
//     resolved to.
//   - VLLM_USE_V1 selects the vLLM v1 codepath the LMCache connector targets.
//   - INFERENCECACHE_FAIL_OPEN mirrors spec.integration.failOpen onto the
//     pod; allowing an override would silently desync the pod from the CR
//     contract and from status.failOpen.
//   - PYTHONHASHSEED pins the deterministic NONE_HASH that seeds vLLM's
//     prefix-cache block-hash chain across the scheduler + TP worker
//     processes; an override re-randomizes it under TP>1 and LMCache reload
//     silently 0-hits (full recompute, no crash, no error). The failure mode
//     is invisible, so the operator must not be able to suppress it.
//
// Tunables (LMCACHE_CHUNK_SIZE / LMCACHE_REMOTE_SERDE / LMCACHE_LOCAL_CPU /
// LMCACHE_MAX_LOCAL_CPU_SIZE) are perf/mode knobs the operator may legitimately
// want to change and are deliberately NOT reserved.
func (vllmLMCacheAdapter) ReservedEnv() []string {
	return []string{
		EnvLMCacheRemoteURL,
		EnvVLLMUseV1,
		EnvInferenceCacheFailOpen,
		EnvPythonHashSeed,
	}
}

// InjectEngineConfig adds the LMCache connector arg and LMCACHE_* env to the
// vLLM container in pod from the structured remote-storage binding.
//
// spec.integration.role maps onto LMCache's kv_role in the connector
// config: ReadOnly → kv_consumer, WriteOnly → kv_producer, ReadWrite
// (and unset / unknown) → kv_both.
func (vllmLMCacheAdapter) SupportsBinding(binding *backendadapter.Binding) bool {
	return binding == nil ||
		binding.Protocol == backendadapter.ProtocolLMCache ||
		binding.Protocol == backendadapter.ProtocolMooncakeStore
}

func (vllmLMCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	if cache != nil && cache.Spec.IsEventsOnly() {
		return nil
	}
	if binding == nil {
		return InjectVLLMLMCacheHostOnly(pod, cache)
	}
	switch binding.Protocol {
	case backendadapter.ProtocolLMCache:
		return InjectVLLMLMCache(pod, binding.Endpoint, cache)
	case backendadapter.ProtocolMooncakeStore:
		if err := InjectVLLMMooncake(pod, binding.Endpoint, cache); err != nil {
			return err
		}
		injectMooncakeEngineHostNetwork(pod, cache)
		return nil
	default:
		return fmt.Errorf("vLLM LMCache adapter does not support remote binding protocol %q", binding.Protocol)
	}
}

func injectMooncakeEngineHostNetwork(pod *corev1.PodSpec, cache *cachev1alpha1.CacheBackend) {
	if EngineHostNetworkRequested(cache) {
		pod.HostNetwork = true
		pod.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
}

// EngineHostNetworkRequested reports whether the operator opted engine pods
// using a Mooncake remote binding into host networking.
func EngineHostNetworkRequested(cache *cachev1alpha1.CacheBackend) bool {
	return adapterruntime.EngineHostNetworkRequested(cache)
}

// InjectRouterConfig is a no-op for LMCache: the LMCache topology has no
// router component the controller needs to wire. Returning nil keeps the
// interface contract satisfied so a Registry caller can blindly invoke both
// Inject* paths on a per-pod basis without branching on backend type — per
// [adapterruntime.KVCacheRuntimeAdapter.InjectRouterConfig]: "backends without a router
// component should return nil without touching pod." Input validation is
// intentionally skipped so a router-less backend never forces callers to
// special-case it.
func (vllmLMCacheAdapter) InjectRouterConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = binding
	_ = cache
	return nil
}

// ObservationSidecar returns the kvevent-subscriber container the Pod webhook
// appends to a vLLM engine pod so its KV-cache events flow to the policy
// server. It delegates to the shared [adapterruntime.RenderSubscriberSidecar], pinning the
// vLLM-specific knobs: --hash-scheme=vllm and the vLLM ZMQ PUB port. The
// eviction-forwarding policy (--ignore-block-removed) is mode-dependent and
// computed by the shared builder (suppressed in Offload where the L2 tier
// retains evicted blocks; forwarded in EventsOnly where there is no L2). The
// subscriber shape is identical for every vLLM-engine L2 backend (LMCache,
// Mooncake) because the KV-event stream comes from vLLM itself, not the L2 store.
func (a vllmLMCacheAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return adapterruntime.RenderSubscriberSidecar(adapterruntime.SubscriberSidecarParams{
		Image:            a.subscriberImage,
		ServerAddr:       a.policyServerGRPCAddress,
		Cache:            cache,
		Pod:              pod,
		HashScheme:       vllmSubscriberHashScheme,
		EngineZMQPortStr: vllmDefaultEngineZMQPortStr,
	})
}

// Package-local aliases to the engine-wire helpers. Kept so the in-place
// unit tests in vllm_lmcache_test.go continue to assert on the wire format
// through the canonical adapter API surface. New tests for the shared wire
// (LMCache, Mooncake, and External all speak the LMCache connector) belong in
// pkg/adapters/runtime/internal/
const defaultEngineKVTransferConfigArg = "--kv-transfer-config"

var (
	kvTransferConfig = KVTransferConfig
	upsertArgPair    = UpsertArgPair
)

// ValidateExternalEndpoint is the shared canonical endpoint seam used by
// admission, reconciliation, and pod injection. It validates an
// operator-supplied endpoint against the selected remote provider's wire
// protocol. Bare host:port is portable across providers; explicit schemes are
// accepted only when the provider's engine wire consumes them.
func ValidateExternalEndpoint(provider cachev1alpha1.CacheBackendRemoteStorageProvider, endpoint string) error {
	return adapterruntime.ValidateExternalEndpoint(provider, endpoint)
}
