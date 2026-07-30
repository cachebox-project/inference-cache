package v1alpha1

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UsesCanonicalCacheHierarchy reports whether the resource uses the separated
// runtime/cache/storage API. Legacy resources are detected by the absence of
// these fields and retain their historical implicit provider mapping.
func (s *CacheBackendSpec) UsesCanonicalCacheHierarchy() bool {
	return s.Runtime != "" || s.LMCache != nil || s.RemoteStorage != nil || s.Observation != nil
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

// EffectiveCacheType returns the engine-side cache implementation. Legacy
// Mooncake and External values represented remote-provider concerns in
// spec.type; both use LMCache engine wiring and normalize to LMCache here.
func (s *CacheBackendSpec) EffectiveCacheType() CacheBackendType {
	switch s.Type {
	case CacheBackendTypeMooncake, CacheBackendTypeExternal:
		return CacheBackendTypeLMCache
	case "":
		return CacheBackendTypeLMCache
	default:
		return s.Type
	}
}

// EffectiveRemoteStorage returns the explicit remote-storage declaration, or a
// synthesized declaration for a legacy resource. In the canonical API a nil
// remoteStorage remains nil: host-only caching must never select a provider as
// an adapter side effect.
func (s *CacheBackendSpec) EffectiveRemoteStorage() *CacheBackendRemoteStorageSpec {
	if s.RemoteStorage != nil {
		return s.RemoteStorage
	}
	if s.UsesCanonicalCacheHierarchy() {
		return nil
	}

	switch s.Type {
	case CacheBackendTypeExternal:
		return &CacheBackendRemoteStorageSpec{
			Provider:  CacheBackendRemoteStorageProviderLMCacheServer,
			Ownership: CacheBackendRemoteStorageOwnershipExternal,
			Endpoint:  s.Endpoint,
		}
	case CacheBackendTypeMooncake:
		return &CacheBackendRemoteStorageSpec{
			Provider:  CacheBackendRemoteStorageProviderMooncake,
			Ownership: CacheBackendRemoteStorageOwnershipManaged,
			Mooncake: &MooncakeRemoteStorageSpec{
				Image:     s.BackendConfig["serverImage"],
				Resources: s.Resources,
			},
		}
	case CacheBackendTypeLMCache, "":
		if s.EffectiveRuntime() == CacheBackendRuntimeSGLang {
			return &CacheBackendRemoteStorageSpec{
				Provider:  CacheBackendRemoteStorageProviderRedis,
				Ownership: CacheBackendRemoteStorageOwnershipManaged,
				Redis: &RedisRemoteStorageSpec{
					Image:     s.BackendConfig["redisImage"],
					Resources: s.Resources,
				},
			}
		}
		return &CacheBackendRemoteStorageSpec{
			Provider:  CacheBackendRemoteStorageProviderLMCacheServer,
			Ownership: CacheBackendRemoteStorageOwnershipManaged,
			LMCacheServer: &LMCacheServerRemoteStorageSpec{
				Image:     s.BackendConfig["serverImage"],
				Resources: s.Resources,
			},
		}
	default:
		return nil
	}
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
