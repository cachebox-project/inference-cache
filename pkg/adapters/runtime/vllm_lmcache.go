package runtime

import (
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
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

// vLLM-specific kvevent-subscriber wiring. The subscriber image + policy-server
// address defaults and the shared sidecar/LMCache-server rendering live in
// lmcache_shared.go (engine-agnostic, also used by the SGLang+LMCache adapter).
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

// vllmLMCacheAdapter wires vLLM engine pods to the LMCache backend that
// CacheBackend (type=LMCache) provisions. ResolveCacheServer renders a
// standalone lmcache-server pod + Service (the engine connects to it via
// LMCACHE_REMOTE_URL=lm://<svc>:65432); InjectEngineConfig adds the
// --kv-transfer-config arg and the LMCACHE_* env vars to the vLLM container,
// merging with what the pod template already carries; ObservationSidecar
// returns the kvevent-subscriber container the webhook appends so the engine
// pod auto-attaches to the policy server with no out-of-band steps.
//
// This adapter wires vLLM+LMCache. It has two siblings that reuse the shared
// helpers here: the vLLM+Mooncake adapter (vllm_mooncake.go) reuses the same
// LMCache connector wire via a mooncakestore:// remote, and the SGLang+LMCache
// adapter (pkg/adapters/runtime/sglang) reuses the lmcache-server rendering
// (ResolveLMCacheServer) + the subscriber sidecar (RenderSubscriberSidecar) and
// differs only in the engine-side wire.
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
	return runtime == RuntimeVLLM && cache.Spec.Type == cachev1alpha1.CacheBackendTypeLMCache
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

// ResolveCacheServer renders the standalone LMCache server's container set
// and the Service's port set, delegating to the engine-agnostic
// [ResolveLMCacheServer] shared with the SGLang+LMCache adapter (the
// lmcache-server is the same regardless of which engine connects).
func (vllmLMCacheAdapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return ResolveLMCacheServer(cache)
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

// ValidateLMCacheEndpoint re-exports [enginewire.ValidateLMCacheEndpoint]
// so consumers outside pkg/adapters/runtime — the validating webhook in
// internal/webhook/v1alpha1, the C2 reconciler in internal/controller,
// and the pod webhook in internal/webhook/pod — can call the same
// endpoint-shape check that admission uses. Go's internal-package rule
// keeps the enginewire subpackage adapter-scoped; this re-export is the
// public seam those layers reach for. Returns nil when s is a valid
// LMCache endpoint, otherwise an error whose message describes the
// shape problem (kubectl admission paths wrap it in field.Invalid;
// reconciler/pod-webhook paths surface the message in a status reason
// or fail-open log).
func ValidateLMCacheEndpoint(s string) error {
	return enginewire.ValidateLMCacheEndpoint(s)
}
