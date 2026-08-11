// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
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
//   - InjectEngineConfig renders a node-local MP-worker
//     native sidecar + a config-file (mp_host/mp_port) the engine reads via
//     --lmcache-config-file. A nil binding is host-only; an optional RESP
//     binding offloads to independently selected Redis storage.
//   - It turns LMCache on with
//     --enable-lmcache + LMCACHE_USE_EXPERIMENTAL (not vLLM's --kv-transfer-config)
//     and does NOT inject the lm:// LMCACHE_REMOTE_URL env, which MP mode ignores.
//     See InjectSGLangLMCache.
//
// GPU-validated end-to-end; full design: docs/design/sglang-lmcache-mp-mode.md. The
// kvevent-subscriber sidecar rendering is still shared engine-agnostically.
type sglangLMCacheAdapter struct {
	subscriber SubscriberConfig
}

// NewSGLangLMCacheAdapter returns the runtime adapter for the (sglang, LMCache) pair.
func NewSGLangLMCacheAdapter(subscriber SubscriberConfig) runtimeadapter.KVCacheRuntimeAdapter {
	return sglangLMCacheAdapter{subscriber: subscriber}
}

// Supports matches SGLang engines against an LMCache CacheBackend. Every other
// (runtime, backend) combination is left for another adapter — vLLM+LMCache,
// an externally owned remote binding, or a future SGLang+Mooncake binding — and an
// unsupported pair surfaces as ErrNoAdapter at admission.
func (sglangLMCacheAdapter) Supports(runtime runtimeadapter.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	if cache == nil {
		return false
	}
	return runtime == runtimeadapter.RuntimeSGLang &&
		cache.Spec.EffectiveCacheType() == cachev1alpha1.CacheBackendTypeLMCache
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
func (sglangLMCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	endpoint := ""
	if binding != nil {
		if binding.Protocol != backendadapter.ProtocolRESP {
			return fmt.Errorf("SGLang LMCache adapter does not support remote binding protocol %q", binding.Protocol)
		}
		endpoint = binding.Endpoint
	}
	return InjectSGLangLMCache(pod, endpoint, cache)
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
