package v1alpha1

import "testing"

func TestEffectiveRemoteStorageUsesOnlyExplicitDeclaration(t *testing.T) {
	spec := CacheBackendSpec{Type: CacheBackendTypeLMCache}
	if got := spec.EffectiveRemoteStorage(); got != nil {
		t.Fatalf("EffectiveRemoteStorage() = %+v, want nil", got)
	}

	want := &CacheBackendRemoteStorageSpec{
		Provider:  CacheBackendRemoteStorageProviderMooncake,
		Ownership: CacheBackendRemoteStorageOwnershipManaged,
	}
	spec.RemoteStorage = want
	if got := spec.EffectiveRemoteStorage(); got != want {
		t.Fatalf("EffectiveRemoteStorage() = %+v, want explicit declaration %+v", got, want)
	}
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

func TestObservationDoesNotSynthesizeRemoteStorage(t *testing.T) {
	spec := CacheBackendSpec{
		Type:        CacheBackendTypeLMCache,
		Observation: &CacheBackendObservationSpec{ModelID: "model-a"},
	}
	if spec.UsesCanonicalCacheHierarchy() {
		t.Fatal("typed observation must remain independent from cache/provider hierarchy selection")
	}
	if got := spec.EffectiveRemoteStorage(); got != nil {
		t.Fatalf("EffectiveRemoteStorage() = %+v, want nil", got)
	}
}
