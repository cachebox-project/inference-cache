package sglang

import (
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

const (
	// subscriberHashScheme is the canonical hash-scheme tag the SGLang
	// subscriber carries. Kept distinct from the runtime id and from vLLM's
	// "vllm" tag: the cache plane keys the index on (tenant, model,
	// hash_scheme, prefix_hash), so tagging SGLang prefixes "sglang" keeps
	// them in a disjoint domain from vLLM's — a request hashed under one
	// scheme can never false-hit a bytewise-identical entry recorded under the
	// other. The prefix_hash the index stores is the cache plane's OWN
	// content fingerprint (derived in-pod from the engine's token_ids by the
	// subscriber — the same scheme-independent algorithm for both engines, NOT
	// the engine's native block hash), so the disjointness guarantee rides
	// entirely on this tag, not on vLLM's and SGLang's native hashes differing.
	subscriberHashScheme = "sglang"

	// defaultEngineZMQPortStr is the port SGLang's KV-event ZMQ PUB endpoint
	// binds by default (SGLang's KVEventsConfig defaults to tcp://*:5557, the
	// same port vLLM uses). The operator enables the publisher with
	// --kv-events-config on the engine; the subscriber sidecar dials it over
	// 127.0.0.1 since it shares the engine pod's network namespace.
	defaultEngineZMQPortStr = "5557"
)

// adapter wires SGLang engine pods to the managed LMCache backend a
// CacheBackend{type: LMCache} provisions, for the (sglang, LMCache) pair. Unlike
// the vLLM+LMCache adapter, SGLang drives LMCache in MULTIPROCESS (MP) mode, so the
// two adapters diverge on both halves of the data plane:
//
//   - ResolveCacheServer provisions a shared Redis L2 store, NOT the vLLM lm://
//     lmcache-server — lm:// is not a valid MP --l2-adapter type; and
//   - InjectEngineConfig renders the MP engine wire — a node-local MP-worker
//     native sidecar + a config-file (mp_host/mp_port) the engine reads via
//     --lmcache-config-file, offloading to that Redis L2. It turns LMCache on with
//     --enable-lmcache + LMCACHE_USE_EXPERIMENTAL (not vLLM's --kv-transfer-config)
//     and does NOT inject the lm:// LMCACHE_REMOTE_URL env, which MP mode ignores.
//     See enginewire.InjectSGLangLMCache.
//
// GPU-validated end-to-end; full design: docs/design/sglang-lmcache-mp-mode.md. The
// kvevent-subscriber sidecar rendering is still shared engine-agnostically.
type adapter struct {
	// subscriberImage is the image the kvevent-subscriber sidecar runs.
	// Empty (the default) disables sidecar auto-attach — ObservationSidecar
	// returns nil — so an unconfigured controller install doesn't push engine
	// pods into ImagePullBackOff on a nonexistent default image.
	subscriberImage string
	// policyServerGRPCAddress overrides the default in-cluster Service DNS the
	// sidecar dials to ReportCacheState. Empty falls back to
	// [runtimeadapter.DefaultPolicyServerGRPCAddress].
	policyServerGRPCAddress string
}

// NewAdapter returns the runtime adapter for the (sglang, LMCache) pair. The
// optional [runtimeadapter.Option] helpers let the controller pin the
// subscriber sidecar's image + policy-server target — the same options
// cmd/controller passes to runtime.DefaultRegistry for the vLLM adapter, so
// the SGLang subscriber sidecar auto-attaches with identical operator wiring.
//
// Wire it into the shared [runtimeadapter.Registry] in cmd/controller and both
// webhook handlers' nil-Registry fallbacks alongside the vLLM+LMCache and
// External adapters: this package imports its parent pkg/adapters/runtime, so
// it cannot live inside runtime.DefaultRegistry without an import cycle (same
// reason as the External adapter).
func NewAdapter(opts ...runtimeadapter.Option) runtimeadapter.KVCacheRuntimeAdapter {
	var cfg runtimeadapter.Options
	for _, o := range opts {
		o(&cfg)
	}
	return adapter{
		subscriberImage:         cfg.SubscriberImage,
		policyServerGRPCAddress: cfg.PolicyServerGRPCAddress,
	}
}

// Supports matches SGLang engines against an LMCache CacheBackend. Every other
// (runtime, backend) combination is left for another adapter — vLLM+LMCache,
// the External passthrough, or a future SGLang+Mooncake adapter — and an
// unsupported pair surfaces as ErrNoAdapter at admission.
func (adapter) Supports(runtime runtimeadapter.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == runtimeadapter.RuntimeSGLang && cache.Spec.Type == cachev1alpha1.CacheBackendTypeLMCache
}

// SupportedPairs lets the registry surface this adapter's canonical pair in the
// "no adapter supports the (engine, backend) pair" admission error so an
// operator who mistypes the engine or backend sees sglang/LMCache as a
// candidate.
func (adapter) SupportedPairs() []runtimeadapter.SupportedPair {
	return []runtimeadapter.SupportedPair{
		{Runtime: runtimeadapter.RuntimeSGLang, Backend: cachev1alpha1.CacheBackendTypeLMCache},
	}
}

// ResolveCacheServer renders the shared Redis L2 store the SGLang MP worker
// offloads to via its resp --l2-adapter, delegating to
// [runtimeadapter.ResolveRedisL2Server]. This replaces the standalone lm://
// lmcache-server the adapter previously (mis)rendered for SGLang: lm:// is not a
// valid MP --l2-adapter type, so SGLang cannot reuse it. The reconciler wraps the
// returned pod + service into the managed Deployment + Service and publishes its
// address as status.endpoint; the engine-side wire ([InjectEngineConfig]) points a
// per-pod node-local MP worker's L2 at that Redis. See
// docs/design/sglang-lmcache-mp-mode.md.
func (adapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return runtimeadapter.ResolveRedisL2Server(cache)
}

// InjectEngineConfig renders SGLang's LMCache MP-mode launch surface, merging with
// the pod template's existing args/env: it adds the node-local MP-worker native
// sidecar + shared /dev/shm/config volumes, and turns the connector on via
// --enable-lmcache + --lmcache-config-file + LMCACHE_USE_EXPERIMENTAL (no
// VLLM_USE_V1 / PYTHONHASHSEED, and no lm:// LMCACHE_REMOTE_URL — MP mode ignores
// it). endpoint is the managed Redis L2 address the worker offloads to. See
// enginewire.InjectSGLangLMCache for the full wire.
func (adapter) InjectEngineConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	return enginewire.InjectSGLangLMCache(pod, endpoint, cache)
}

// InjectRouterConfig is a no-op for LMCache: the topology has no router
// component the controller wires. Returning nil keeps the interface contract
// satisfied so a Registry caller can blindly invoke both Inject* paths without
// branching on backend type — per
// [runtimeadapter.KVCacheRuntimeAdapter.InjectRouterConfig]: "backends without
// a router component should return nil without touching pod."
func (adapter) InjectRouterConfig(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = endpoint
	_ = cache
	return nil
}

// ObservationSidecar returns the kvevent-subscriber container the Pod webhook
// appends to an SGLang engine pod so its KV-cache events flow to the policy
// server. It delegates to the shared [runtimeadapter.RenderSubscriberSidecar],
// pinning the SGLang-specific knobs: --hash-scheme=sglang (so the index keeps
// SGLang prefixes disjoint from vLLM's) and SGLang's ZMQ PUB port. The
// eviction-forwarding policy (--ignore-block-removed) is mode-dependent and
// computed by the shared builder — suppressed in Offload (LMCache L2 retains
// evicted blocks) and forwarded in EventsOnly (no L2). The shipped subscriber
// binary decodes SGLang's KV-event stream unchanged because SGLang emits the
// same msgspec BlockStored/BlockRemoved/AllBlocksCleared wire vLLM does.
func (a adapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return runtimeadapter.RenderSubscriberSidecar(runtimeadapter.SubscriberSidecarParams{
		Image:            a.subscriberImage,
		ServerAddr:       a.policyServerGRPCAddress,
		Cache:            cache,
		Pod:              pod,
		HashScheme:       subscriberHashScheme,
		EngineZMQPortStr: defaultEngineZMQPortStr,
	})
}

// ReservedArgs returns the engine args this adapter injects that the LMCache MP
// integration cannot function without. The validating webhook blocks an
// spec.integration.engineOverrides entry that overrides or suppresses any of
// these so the operator cannot silently un-wire the connector.
//
//   - "--enable-lmcache" turns the LMCache connector on at startup; suppressing it
//     means no LMCache wiring at all.
//   - "--lmcache-config-file" points the engine at the MP config file the worker
//     sidecar writes (mp_host/mp_port); without it SGLang's MP mode aborts at
//     startup, so suppressing it breaks the engine, not just the cache.
//
// (Distinct from the vLLM adapter, which reserves --kv-transfer-config — the
// two engines turn LMCache on through different launch surfaces.)
func (adapter) ReservedArgs() []string {
	return []string{enginewire.SGLangEnableLMCacheArg, enginewire.SGLangConfigFileArg}
}

// ReservedEnv returns the env var names this adapter injects and blocks
// engineOverrides from touching. SGLang drives LMCache in MP mode (config-file +
// node-local worker), so — unlike the old lm:// wire — LMCACHE_REMOTE_URL and the
// serde/local-CPU tunables are NOT injected and NOT reserved. What remains:
//
//   - LMCACHE_USE_EXPERIMENTAL (set to "True") gates SGLang's experimental LMCache
//     path; without it, --enable-lmcache does not engage the connector at all.
//   - INFERENCECACHE_FAIL_OPEN mirrors spec.integration.failOpen onto the pod;
//     an override would silently desync the pod from the CR contract.
//
// Unlike the vLLM adapter, VLLM_USE_V1 and PYTHONHASHSEED are NOT reserved — they
// are not injected for SGLang at all (no vLLM v1 codepath; SGLang's sha256-based
// prefix hashing does not depend on PYTHONHASHSEED).
func (adapter) ReservedEnv() []string {
	return []string{
		enginewire.EnvLMCacheUseExperimental,
		enginewire.EnvInferenceCacheFailOpen,
	}
}

// EngineContainerName returns the canonical name of the SGLang engine container
// the adapter mutates. The pod webhook uses this to scope engineOverrides edits
// to the same container InjectEngineConfig writes to — overrides land on the
// engine, not on user-attached sidecars.
func (adapter) EngineContainerName() string { return enginewire.SGLangEngineContainerName }

// Compile-time assertion: the adapter implements the full C5 interface.
var _ runtimeadapter.KVCacheRuntimeAdapter = adapter{}
