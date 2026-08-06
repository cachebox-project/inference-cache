package runtime

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// Engine env var names. Re-exported from the internal enginewire package so
// downstream callers (admission validators, integration tests, future
// adapter authors) can assert on the wire contract without importing an
// internal/ path. The constants live in enginewire so adapters that speak
// the same wire (vLLM+LMCache and the External passthrough today) share a
// single source of truth.
const (
	EnvLMCacheRemoteURL       = enginewire.EnvLMCacheRemoteURL
	EnvLMCacheRemoteSerde     = enginewire.EnvLMCacheRemoteSerde
	EnvLMCacheChunkSize       = enginewire.EnvLMCacheChunkSize
	EnvLMCacheLocalCPU        = enginewire.EnvLMCacheLocalCPU
	EnvLMCacheMaxLocalCPU     = enginewire.EnvLMCacheMaxLocalCPU
	EnvVLLMUseV1              = enginewire.EnvVLLMUseV1
	EnvInferenceCacheFailOpen = enginewire.EnvInferenceCacheFailOpen
	EnvPythonHashSeed         = enginewire.EnvPythonHashSeed
	// EngineContainerName is the conventional name for the vLLM container in
	// an engine pod the adapter mutates. When no container with this name is
	// present, a single-container pod is treated as the engine; a multi-
	// container pod is rejected.
	EngineContainerName = enginewire.EngineContainerName
)

// vLLM-specific kvevent-subscriber wiring. The subscriber image and
// policy-server address defaults live in lmcache_shared.go.
const (
	// vLLM engine convention: the KV-event ZMQ PUB endpoint binds on :5557 by
	// default (the reference stack's --kv-events-config sets
	// endpoint=tcp://*:5557). Parameterising via the adapter (not hardcoding in
	// the webhook) lets SGLang or another engine adapter pick a different port
	// without touching the webhook.
	defaultEngineZMQPortStr = "5557"

	// subscriberHashScheme is the canonical hash-scheme tag the vLLM subscriber
	// carries. Hard-coded for this adapter (vLLM's block-hash scheme is distinct
	// from SGLang's, and the cache plane keys on the scheme to keep them from
	// collapsing).
	subscriberHashScheme = "vllm"
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
// LMCache CacheBackend. The optional [Option] helpers let the controller pin
// the subscriber sidecar's image + policy-server target; the no-arg form
// reproduces the package defaults and keeps tests + the nil-Registry
// fallback paths working.
func NewVLLMLMCacheAdapter(opts ...Option) KVCacheRuntimeAdapter {
	var cfg Options
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
func (vllmLMCacheAdapter) Supports(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == RuntimeVLLM &&
		cache.Spec.EffectiveCacheType() == cachev1alpha1.CacheBackendTypeLMCache &&
		(cache.Spec.UsesCanonicalCacheHierarchy() || cache.Spec.Type == cachev1alpha1.CacheBackendTypeLMCache)
}

// SupportedPairs lets the registry expose this adapter's canonical pair to
// admission error messages so a user who asked for an unsupported pair can
// see what they could have asked for instead.
func (vllmLMCacheAdapter) SupportedPairs() []SupportedPair {
	return []SupportedPair{{Runtime: RuntimeVLLM, Backend: cachev1alpha1.CacheBackendTypeLMCache}}
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
// knobs surfaced as backendConfig keys) are deliberately NOT reserved.
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
// vLLM container in pod, delegating to the shared engine-wire helper. The
// External backend adapter calls the same helper with an operator-supplied
// endpoint, keeping the rendered engine wiring byte-identical regardless of
// who owns the cache lifecycle.
//
// spec.integration.role maps onto LMCache's kv_role in the connector
// config: ReadOnly → kv_consumer, WriteOnly → kv_producer, ReadWrite
// (and unset / unknown) → kv_both.
func (vllmLMCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	// Events-only (tier-1 routing) wires NO KV connector: the engine container
	// is left unmodified so a hybrid-attention model's KV-cache manager is not
	// disabled by a connector it cannot load. The engine's own (operator-
	// configured) kv-events publisher is all the observation sidecar needs, and
	// nothing dials a cache server, so no endpoint is required either. The
	// subscriber is still appended by the webhook via ObservationSidecar.
	if cache != nil && cache.Spec.IsEventsOnly() {
		return nil
	}
	return enginewire.InjectVLLMLMCache(pod, endpoint, cache)
}

func (vllmLMCacheAdapter) SupportsRemoteBinding(binding *backendadapter.Binding) bool {
	return binding == nil ||
		binding.Protocol == backendadapter.ProtocolLMCache ||
		binding.Protocol == backendadapter.ProtocolMooncakeStore
}

func (vllmLMCacheAdapter) InjectEngineConfigWithBinding(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	if cache != nil && cache.Spec.IsEventsOnly() {
		return nil
	}
	if binding == nil {
		return enginewire.InjectVLLMLMCacheHostOnly(pod, cache)
	}
	switch binding.Protocol {
	case backendadapter.ProtocolLMCache:
		return enginewire.InjectVLLMLMCache(pod, binding.Endpoint, cache)
	case backendadapter.ProtocolMooncakeStore:
		if err := enginewire.InjectVLLMMooncake(pod, binding.Endpoint, cache); err != nil {
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
	return cache != nil && cache.Spec.Integration != nil && cache.Spec.Integration.EngineHostNetwork
}

// InjectRouterConfig is a no-op for LMCache: the LMCache topology has no
// router component the controller needs to wire. Returning nil keeps the
// interface contract satisfied so a Registry caller can blindly invoke both
// Inject* paths on a per-pod basis without branching on backend type — per
// [KVCacheRuntimeAdapter.InjectRouterConfig]: "backends without a router
// component should return nil without touching pod." Input validation is
// intentionally skipped so a router-less backend never forces callers to
// special-case it.
func (vllmLMCacheAdapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = endpoint
	_ = cache
	return nil
}

// ObservationSidecar returns the kvevent-subscriber container the Pod webhook
// appends to a vLLM engine pod so its KV-cache events flow to the policy
// server. It delegates to the shared [RenderSubscriberSidecar], pinning the
// vLLM-specific knobs: --hash-scheme=vllm and the vLLM ZMQ PUB port. The
// eviction-forwarding policy (--ignore-block-removed) is mode-dependent and
// computed by the shared builder (suppressed in Offload where the L2 tier
// retains evicted blocks; forwarded in EventsOnly where there is no L2). The
// subscriber shape is identical for every vLLM-engine L2 backend (LMCache,
// Mooncake) because the KV-event stream comes from vLLM itself, not the L2 store.
func (a vllmLMCacheAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return RenderSubscriberSidecar(SubscriberSidecarParams{
		Image:            a.subscriberImage,
		ServerAddr:       a.policyServerGRPCAddress,
		Cache:            cache,
		Pod:              pod,
		HashScheme:       subscriberHashScheme,
		EngineZMQPortStr: defaultEngineZMQPortStr,
	})
}

// Package-local aliases to the engine-wire helpers. Kept so the in-place
// unit tests in vllm_lmcache_test.go continue to assert on the wire format
// through the canonical adapter API surface. New tests for the shared wire
// (LMCache, Mooncake, and External all speak the LMCache connector) belong in
// pkg/adapters/runtime/internal/enginewire.
const defaultEngineKVTransferConfigArg = "--kv-transfer-config"

var (
	kvTransferConfig = enginewire.KVTransferConfig
	upsertArgPair    = enginewire.UpsertArgPair
)

// ValidateLMCacheEndpoint re-exports [enginewire.ValidateLMCacheEndpoint] for
// LMCache-specific callers. External remoteStorage callers use
// [ValidateExternalEndpoint], which dispatches this
// same host/port shape check according to the selected provider.
func ValidateLMCacheEndpoint(s string) error {
	return enginewire.ValidateLMCacheEndpoint(s)
}

// ValidateExternalEndpoint is the shared canonical endpoint seam used by
// admission, reconciliation, and pod injection. It validates an
// operator-supplied endpoint against the selected remote provider's wire
// protocol. Bare host:port is portable across providers; explicit schemes are
// accepted only when the provider's engine wire consumes them.
func ValidateExternalEndpoint(provider cachev1alpha1.CacheBackendRemoteStorageProvider, endpoint string) error {
	trimmed := strings.TrimSpace(endpoint)
	switch provider {
	case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
		return enginewire.ValidateLMCacheEndpoint(trimmed)
	case cachev1alpha1.CacheBackendRemoteStorageProviderRedis:
		if scheme, _, ok := strings.Cut(trimmed, "://"); ok {
			return fmt.Errorf("scheme %q is not supported for remoteStorage.provider=%s; use bare host:port",
				scheme, provider)
		}
		if err := enginewire.ValidateLMCacheEndpoint(trimmed); err != nil {
			return err
		}
		_, port, err := net.SplitHostPort(trimmed)
		if err != nil {
			return fmt.Errorf("Redis endpoint must be a bare host:port: %w", err)
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("Redis endpoint port %q must be an integer in 1-65535", port)
		}
		return nil
	case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
		if scheme, address, ok := strings.Cut(trimmed, "://"); ok {
			if !strings.EqualFold(scheme, "mooncakestore") {
				return fmt.Errorf("scheme %q is not supported for remoteStorage.provider=%s; use bare host:port or mooncakestore://host:port",
					scheme, provider)
			}
			if strings.Contains(address, "://") {
				return fmt.Errorf("nested endpoint schemes are not supported for remoteStorage.provider=%s; use mooncakestore://host:port",
					provider)
			}
			trimmed = address
		}
		return enginewire.ValidateLMCacheEndpoint(trimmed)
	default:
		return fmt.Errorf("remote-storage provider %q has no endpoint protocol", provider)
	}
}
