package v1alpha1

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UsesCanonicalCacheHierarchy reports whether the resource uses the separated
// runtime/cache/storage API. Legacy resources are detected by the absence of
// these fields and retain their historical implicit provider mapping.
func (s *CacheBackendSpec) UsesCanonicalCacheHierarchy() bool {
	return s.Runtime != "" || s.LMCache != nil || s.RemoteStorage != nil
}

// EffectiveRuntime returns the canonical inference runtime while preserving
// integration.engine as a read-time compatibility input.
func (s *CacheBackendSpec) EffectiveRuntime() CacheBackendRuntime {
	if s.Runtime != "" {
		return normalizeRuntime(s.Runtime)
	}
	if s.Integration != nil {
		if runtime := normalizeRuntime(CacheBackendRuntime(s.Integration.Engine)); runtime != "" {
			return runtime
		}
	}
	return CacheBackendRuntimeVLLM
}

// EffectiveCacheType returns the engine-side cache implementation, defaulting
// an omitted value to LMCache for callers that do not pass through admission.
func (s *CacheBackendSpec) EffectiveCacheType() CacheBackendType {
	if s.Type == "" {
		return CacheBackendTypeLMCache
	}
	return s.Type
}

// EffectiveRemoteStorage returns the explicit remote-storage declaration. A
// nil remoteStorage remains nil: host-only caching must never select a provider
// as an adapter side effect.
func (s *CacheBackendSpec) EffectiveRemoteStorage() *CacheBackendRemoteStorageSpec {
	return s.RemoteStorage
}

// EffectiveObservationModelID returns the independently-owned observation
// model id while retaining backendConfig.model for legacy resources.
func (s *CacheBackendSpec) EffectiveObservationModelID() string {
	if s.Observation != nil {
		return s.Observation.ModelID
	}
	return s.BackendConfig["model"]
}

// EffectiveFirstEventTimeout returns the observation-owned timeout, falling
// back to the deprecated integration field for compatibility.
func (s *CacheBackendSpec) EffectiveFirstEventTimeout() *metav1.Duration {
	if s.Observation != nil && s.Observation.FirstEventTimeout != nil {
		return s.Observation.FirstEventTimeout
	}
	if s.Integration != nil {
		return s.Integration.FirstEventTimeout
	}
	return nil
}

func normalizeRuntime(value CacheBackendRuntime) CacheBackendRuntime {
	switch strings.ToLower(string(value)) {
	case "vllm":
		return CacheBackendRuntimeVLLM
	case "sglang":
		return CacheBackendRuntimeSGLang
	default:
		return value
	}
}
