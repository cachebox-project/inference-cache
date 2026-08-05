package builtin

import (
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func TestNewIncludesEveryShippingRuntimeAdapter(t *testing.T) {
	t.Parallel()

	registry := New().Runtime
	for _, tc := range []struct {
		name        string
		runtime     adapterruntime.RuntimeID
		backend     cachev1alpha1.CacheBackendType
		integration *cachev1alpha1.CacheBackendIntegrationSpec
	}{
		{name: "vllm lmcache", runtime: adapterruntime.RuntimeVLLM, backend: cachev1alpha1.CacheBackendTypeLMCache},
		{name: "vllm mooncake legacy", runtime: adapterruntime.RuntimeVLLM, backend: cachev1alpha1.CacheBackendTypeMooncake},
		{name: "vllm external legacy", runtime: adapterruntime.RuntimeVLLM, backend: cachev1alpha1.CacheBackendTypeExternal},
		{name: "sglang lmcache", runtime: adapterruntime.RuntimeSGLang, backend: cachev1alpha1.CacheBackendTypeLMCache},
		{name: "sglang hicache", runtime: adapterruntime.RuntimeSGLang, backend: cachev1alpha1.CacheBackendTypeSGLangHiCache},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cache := &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{
				Type: tc.backend, Integration: tc.integration,
			}}
			if _, err := registry.Select(tc.runtime, cache); err != nil {
				t.Fatalf("Select(%q, %q): %v", tc.runtime, tc.backend, err)
			}
		})
	}
}

func TestNewIncludesShippingStorageProviders(t *testing.T) {
	t.Parallel()

	registry := New().Storage
	for _, provider := range []cachev1alpha1.CacheBackendRemoteStorageProvider{
		cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
	} {
		for _, ownership := range []cachev1alpha1.CacheBackendRemoteStorageOwnership{
			cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		} {
			storage := &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider: provider, Ownership: ownership,
			}
			if _, err := registry.Select(storage); err != nil {
				t.Fatalf("Select(%q, %q): %v", provider, ownership, err)
			}
		}
	}
}
