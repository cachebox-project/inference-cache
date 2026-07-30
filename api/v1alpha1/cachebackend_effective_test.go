package v1alpha1

import "testing"

func TestEffectiveRemoteStorageSeparatesCanonicalAndLegacyHierarchy(t *testing.T) {
	t.Run("canonical omission is host-only", func(t *testing.T) {
		spec := CacheBackendSpec{
			Runtime: CacheBackendRuntimeSGLang,
			Type:    CacheBackendTypeLMCache,
		}
		if got := spec.EffectiveRemoteStorage(); got != nil {
			t.Fatalf("EffectiveRemoteStorage() = %+v, want nil", got)
		}
	})

	t.Run("legacy sglang lmcache keeps redis compatibility", func(t *testing.T) {
		spec := CacheBackendSpec{
			Type: CacheBackendTypeLMCache,
			Integration: &CacheBackendIntegrationSpec{
				Engine: "sglang",
			},
		}
		got := spec.EffectiveRemoteStorage()
		if got == nil ||
			got.Provider != CacheBackendRemoteStorageProviderRedis ||
			got.Ownership != CacheBackendRemoteStorageOwnershipManaged {
			t.Fatalf("EffectiveRemoteStorage() = %+v, want Managed Redis", got)
		}
	})

	t.Run("legacy mooncake normalizes engine cache separately", func(t *testing.T) {
		spec := CacheBackendSpec{Type: CacheBackendTypeMooncake}
		if got := spec.EffectiveCacheType(); got != CacheBackendTypeLMCache {
			t.Fatalf("EffectiveCacheType() = %q, want LMCache", got)
		}
		storage := spec.EffectiveRemoteStorage()
		if storage == nil || storage.Provider != CacheBackendRemoteStorageProviderMooncake {
			t.Fatalf("EffectiveRemoteStorage() = %+v, want Mooncake", storage)
		}
	})
}

func TestEffectiveRuntimePrefersCanonicalField(t *testing.T) {
	spec := CacheBackendSpec{
		Runtime: CacheBackendRuntimeSGLang,
		Integration: &CacheBackendIntegrationSpec{
			Engine: "vllm",
		},
	}
	if got := spec.EffectiveRuntime(); got != CacheBackendRuntimeSGLang {
		t.Fatalf("EffectiveRuntime() = %q, want SGLang", got)
	}
}

func TestObservationSelectsCanonicalHierarchy(t *testing.T) {
	spec := CacheBackendSpec{
		Type:        CacheBackendTypeLMCache,
		Observation: &CacheBackendObservationSpec{ModelID: "model-a"},
	}
	if !spec.UsesCanonicalCacheHierarchy() {
		t.Fatal("typed observation must select the canonical hierarchy")
	}
	if got := spec.EffectiveRemoteStorage(); got != nil {
		t.Fatalf("EffectiveRemoteStorage() = %+v, want nil host-only hierarchy", got)
	}
}
