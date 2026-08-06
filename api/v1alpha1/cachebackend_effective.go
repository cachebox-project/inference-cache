package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EffectiveRuntime returns the configured inference runtime.
func (s *CacheBackendSpec) EffectiveRuntime() CacheBackendRuntime {
	return s.Runtime
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
// model id.
func (s *CacheBackendSpec) EffectiveObservationModelID() string {
	if s.Observation != nil {
		return s.Observation.ModelID
	}
	return ""
}

// EffectiveFirstEventTimeout returns the observation-owned timeout.
func (s *CacheBackendSpec) EffectiveFirstEventTimeout() *metav1.Duration {
	if s.Observation != nil && s.Observation.FirstEventTimeout != nil {
		return s.Observation.FirstEventTimeout
	}
	return nil
}
