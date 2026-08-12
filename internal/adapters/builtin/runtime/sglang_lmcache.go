// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

const (
	EnvLMCacheUseExperimental = "LMCACHE_USE_EXPERIMENTAL"
	lmcacheUseExperimentalVal = "True"
	SGLangEngineContainerName = "sglang"
	SGLangEnableLMCacheArg    = "--enable-lmcache"
	SGLangEnableMetricsArg    = "--enable-metrics"
	SGLangConfigFileArg       = "--lmcache-config-file"

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
	sglangSubscriberHashScheme = "sglang"

	// defaultEngineZMQPortStr is the port SGLang's KV-event ZMQ PUB endpoint
	// binds by default (SGLang's KVEventsConfig defaults to tcp://*:5557, the
	// same port vLLM uses). The operator enables the publisher with
	// --kv-events-config on the engine; the subscriber sidecar dials it over
	// 127.0.0.1 since it shares the engine pod's network namespace.
	sglangDefaultEngineZMQPortStr = "5557"

	// sglangDefaultMetricsPortStr is the port SGLang serves Prometheus /metrics
	// on by default (:30000, distinct from vLLM's :8000). Requires the engine to
	// be launched with --enable-metrics; fed to --engine-metrics-url so the stats
	// scraper reads sglang:* names off the right endpoint. Shared by the SGLang
	// HiCache adapter.
	sglangDefaultMetricsPortStr = "30000"
)

// sglangLMCacheAdapter wires SGLang engine pods to LMCache for the (SGLang, LMCache)
// pair. SGLang drives LMCache in MULTIPROCESS (MP) mode:
//
//   - Typed PodLocal objects use the shared CacheBackend-configured MP-server native
//     sidecar + a config file (mp_host/mp_port) the engine reads via
//     --lmcache-config-file. A nil binding is L1-only; an optional RESP binding
//     offloads to independently selected Redis storage.
//   - It turns LMCache on with
//     --enable-lmcache + LMCACHE_USE_EXPERIMENTAL (not vLLM's --kv-transfer-config)
//     and does not inject any IP-connector environment.
//     See InjectSGLangLMCache.
//
// The typed common-renderer path has been GPU-validated for the supported TP=1
// SGLang configuration.
// The kvevent-subscriber sidecar rendering remains engine-agnostic.
type sglangLMCacheAdapter struct {
	subscriber SubscriberConfig
}

// NewSGLangLMCacheAdapter returns the runtime adapter for the (sglang, LMCache) pair.
func NewSGLangLMCacheAdapter(subscriber SubscriberConfig) runtimeadapter.KVCacheRuntimeAdapter {
	return sglangLMCacheAdapter{subscriber: subscriber}
}

// Supports matches SGLang engines against an LMCache CacheBackend. Every other
// (runtime, backend) combination is left for another adapter — vLLM+LMCache,
// an externally owned Redis binding — and an
// unsupported pair surfaces as ErrNoAdapter at admission.
func (sglangLMCacheAdapter) Supports(runtime runtimeadapter.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	if runtime != runtimeadapter.RuntimeSGLang ||
		cache.Spec.EffectiveCacheType() != cachev1alpha1.CacheBackendTypeLMCache {
		return false
	}
	return cache.Spec.IsEventsOnly() ||
		(cache.Spec.LMCache != nil && cache.Spec.LMCache.Topology == cachev1alpha1.LMCacheTopologyPodLocal)
}

// SupportedPairs lets the registry surface this adapter's canonical pair in the
// "no adapter supports the (engine, backend) pair" admission error so an
// operator who mistypes the engine or backend sees sglang/LMCache as a
// candidate.
func (sglangLMCacheAdapter) SupportedPairs() []runtimeadapter.SupportedPair {
	return []runtimeadapter.SupportedPair{
		{Runtime: runtimeadapter.RuntimeSGLang, Backend: cachev1alpha1.CacheBackendTypeLMCache},
	}
}

func (sglangLMCacheAdapter) SupportsBinding(binding *backendadapter.Binding) bool {
	return binding == nil || binding.Protocol == backendadapter.ProtocolRESP
}

// InjectEngineConfig renders SGLang's LMCache MP-mode launch surface from a
// host-only nil binding or a RESP binding for Redis L2 storage.
func (a sglangLMCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	if err := injectSGLangLMCachePodLocal(pod, binding, cache); err != nil {
		return err
	}
	return ensureSGLangMetricsForSubscriber(pod, cache, a.subscriber)
}

// ensureSGLangMetricsForSubscriber makes the subscriber contract complete:
// SGLang does not expose /metrics unless --enable-metrics is present. Only add
// the flag when this CacheBackend will actually receive an observation sidecar.
func ensureSGLangMetricsForSubscriber(pod *corev1.PodSpec, cache *cachev1alpha1.CacheBackend, subscriber SubscriberConfig) error {
	if subscriber.Image == "" || cache == nil || cache.Spec.EffectiveObservationModelID() == "" {
		return nil
	}
	engineIndex, err := EngineContainerIndexNamed(pod, SGLangEngineContainerName)
	if err != nil {
		return err
	}
	pod.Containers[engineIndex].Args = UpsertFlag(pod.Containers[engineIndex].Args, SGLangEnableMetricsArg)
	return nil
}

// ValidateMPEnginePod checks the concrete Pod constraints needed before the
// common renderer runs. Topology and server resource validation remain at the
// CacheBackend admission boundary; this method owns runtime-visible shape.
func (sglangLMCacheAdapter) ValidateMPEnginePod(pod *corev1.Pod, cache *cachev1alpha1.CacheBackend) error {
	if pod == nil {
		return fmt.Errorf("SGLang LMCache MP engine pod is nil")
	}
	if cache == nil || cache.Spec.LMCache == nil {
		return fmt.Errorf("SGLang LMCache MP CacheBackend configuration is missing")
	}
	if cache.Spec.LMCache.Topology != cachev1alpha1.LMCacheTopologyPodLocal {
		return fmt.Errorf("SGLang LMCache MP topology %q is not implemented; want %q",
			cache.Spec.LMCache.Topology, cachev1alpha1.LMCacheTopologyPodLocal)
	}
	if cache.Spec.LMCache.PodLocal == nil || cache.Spec.LMCache.PodLocal.Server == nil {
		return fmt.Errorf("SGLang LMCache PodLocal server configuration is missing")
	}
	engineIndex, err := EngineContainerIndexNamed(&pod.Spec, SGLangEngineContainerName)
	if err != nil {
		return err
	}
	if err := validateSGLangMPPageSize(
		pod.Spec.Containers[engineIndex].Args,
		effectiveLMCacheChunkSize(cache.Spec.LMCache),
	); err != nil {
		return err
	}
	return nil
}

// validateSGLangMPPageSize catches a launch-time incompatibility before the
// webhook renders the MP wire. LMCache 0.5.3 also checks the effective page
// size after SGLang has resolved model/backend-specific defaults; that runtime
// check remains authoritative when SGLang rewrites an explicitly declared
// value. Requiring the Pod template to declare --page-size makes the admission
// preflight deterministic instead of guessing from an image tag or a moving
// SGLang default.
func validateSGLangMPPageSize(args []string, chunkSize int32) error {
	const pageSizeFlag = "--page-size"
	values, malformed := argValues(args, pageSizeFlag)
	if malformed {
		return fmt.Errorf("SGLang LMCache MP %s is malformed; declare one positive integer value", pageSizeFlag)
	}
	if len(values) == 0 {
		return fmt.Errorf("SGLang LMCache MP engine must explicitly declare %s so chunk-size compatibility can be verified", pageSizeFlag)
	}
	if len(values) > 1 {
		return fmt.Errorf("SGLang LMCache MP %s is duplicated", pageSizeFlag)
	}
	pageSize, err := strconv.ParseInt(values[0], 10, 32)
	if err != nil || pageSize < 1 {
		return fmt.Errorf("SGLang LMCache MP %s=%q must be a positive integer", pageSizeFlag, values[0])
	}
	if int64(chunkSize)%pageSize != 0 {
		return fmt.Errorf("LMCache chunk size %d must be a multiple of SGLang page size %d", chunkSize, pageSize)
	}
	return nil
}

func effectiveLMCacheChunkSize(spec *cachev1alpha1.LMCacheEngineSpec) int32 {
	if spec != nil && spec.ChunkSizeTokens != nil {
		return *spec.ChunkSizeTokens
	}
	return 256
}

func injectSGLangLMCachePodLocal(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	if err := validateInjectPodCacheInputs(pod, cache, "engine"); err != nil {
		return err
	}
	lm := cache.Spec.LMCache
	if lm == nil || lm.Topology != cachev1alpha1.LMCacheTopologyPodLocal || lm.PodLocal == nil || lm.PodLocal.Server == nil {
		return fmt.Errorf("inject SGLang LMCache MP: typed PodLocal server configuration is required")
	}
	server := lm.PodLocal.Server
	chunkSize := effectiveLMCacheChunkSize(lm)

	// Compose the common server and SGLang launch surface on one copy. Although
	// the post-render SGLang upserts cannot fail, keeping one commit point makes
	// the adapter's atomicity contract explicit and future-proof.
	work := pod.DeepCopy()
	configPath, err := renderLMCachePodLocalServer(work, SGLangEngineContainerName, lmCacheMPServerConfig{
		Image:             server.Image,
		Port:              server.Port,
		ChunkSizeTokens:   chunkSize,
		L1Capacity:        server.L1Capacity,
		MaxWorkers:        server.MaxWorkers,
		Resources:         server.Resources,
		Binding:           binding,
		WriteClientConfig: true,
	})
	if err != nil {
		return err
	}
	engineIndex, err := EngineContainerIndexNamed(work, SGLangEngineContainerName)
	if err != nil {
		return err
	}
	engine := &work.Containers[engineIndex]
	engine.Args = UpsertFlag(engine.Args, SGLangEnableLMCacheArg)
	engine.Args = UpsertArgPair(engine.Args, SGLangConfigFileArg, configPath)
	engine.Env = UpsertEnv(engine.Env, corev1.EnvVar{Name: EnvLMCacheUseExperimental, Value: lmcacheUseExperimentalVal})
	engine.Env = UpsertEnv(engine.Env, corev1.EnvVar{Name: EnvInferenceCacheFailOpen, Value: FailOpenString(cache)})

	*pod = *work
	return nil
}

// InjectRouterConfig is a no-op for LMCache: the topology has no router
// component the controller wires. Returning nil keeps the interface contract
// satisfied so a Registry caller can blindly invoke both Inject* paths without
// branching on backend type — per
// [runtimeadapter.KVCacheRuntimeAdapter.InjectRouterConfig]: "backends without
// a router component should return nil without touching pod."
func (sglangLMCacheAdapter) InjectRouterConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	_ = pod
	_ = binding
	_ = cache
	return nil
}

// ObservationSidecar returns the kvevent-subscriber container the Pod webhook
// appends to an SGLang engine pod so its KV-cache events flow to the policy
// server. It delegates to the shared internal subscriber renderer,
// pinning the SGLang-specific knobs: --hash-scheme=sglang (so the index keeps
// SGLang prefixes disjoint from vLLM's) and SGLang's ZMQ PUB port. The
// eviction-forwarding policy (--ignore-block-removed) is mode-dependent and
// computed by the shared builder — suppressed in Offload (LMCache L2 retains
// evicted blocks) and forwarded in EventsOnly (no L2). The shipped subscriber
// binary decodes SGLang's KV-event stream unchanged because SGLang emits the
// same msgspec BlockStored/BlockRemoved/AllBlocksCleared wire vLLM does.
func (a sglangLMCacheAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return renderSubscriberSidecar(subscriberSidecarParams{
		Config:               a.subscriber,
		Cache:                cache,
		Pod:                  pod,
		HashScheme:           sglangSubscriberHashScheme,
		EngineZMQPortStr:     sglangDefaultEngineZMQPortStr,
		EngineMetricsPortStr: sglangDefaultMetricsPortStr,
		EngineContainerName:  a.EngineContainerName(),
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
//   - "--enable-metrics" exposes SGLang's /metrics endpoint (off by default);
//     this adapter injects it, and suppressing it would silently defeat the
//     load-aware stats path without breaking the engine.
//
// (Distinct from the vLLM adapter, which reserves --kv-transfer-config — the
// two engines turn LMCache on through different launch surfaces.)
func (sglangLMCacheAdapter) ReservedArgs() []string {
	return []string{SGLangEnableLMCacheArg, SGLangConfigFileArg, SGLangEnableMetricsArg}
}

// ReservedEnv returns the env var names this adapter injects and blocks
// engineOverrides from touching. SGLang drives LMCache in MP mode (config-file +
// node-local worker), so legacy IP-connector environment and the
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
func (sglangLMCacheAdapter) ReservedEnv() []string {
	return []string{
		EnvLMCacheUseExperimental,
		EnvInferenceCacheFailOpen,
	}
}

// EngineContainerName returns the canonical name of the SGLang engine container
// the adapter mutates. The pod webhook uses this to scope engineOverrides edits
// to the same container InjectEngineConfig writes to — overrides land on the
// engine, not on user-attached sidecars.
func (sglangLMCacheAdapter) EngineContainerName() string { return SGLangEngineContainerName }

// Compile-time assertion: the adapter implements the full C5 interface.
var _ runtimeadapter.KVCacheRuntimeAdapter = sglangLMCacheAdapter{}
var _ runtimeadapter.LMCacheMPRuntimeAdapter = sglangLMCacheAdapter{}
