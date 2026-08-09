// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package builtin

import (
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func TestNewIncludesEveryShippingRuntimeAdapter(t *testing.T) {
	t.Parallel()

	registry := New(Options{}).Runtime
	for _, tc := range []struct {
		name        string
		runtime     adapterruntime.RuntimeID
		backend     cachev1alpha1.CacheBackendType
		integration *cachev1alpha1.CacheBackendIntegrationSpec
	}{
		{name: "vllm lmcache", runtime: adapterruntime.RuntimeVLLM, backend: cachev1alpha1.CacheBackendTypeLMCache},
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

	registry := New(Options{}).Storage
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

func TestNewPassesLMCacheServerImageToStorageRegistry(t *testing.T) {
	t.Parallel()

	const image = "registry.example/lmcache:operator-default"
	cache := &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{
		Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
		Type:    cachev1alpha1.CacheBackendTypeLMCache,
		RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:      cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
			Ownership:     cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			LMCacheServer: &cachev1alpha1.LMCacheServerRemoteStorageSpec{},
		},
	}}

	registry := New(Options{LMCacheServerImage: image}).Storage
	provider, err := registry.Select(cache.Spec.RemoteStorage)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	rendered, err := provider.Render(cache)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := rendered.PodSpec.Containers[0].Image; got != image {
		t.Fatalf("container image = %q, want %q", got, image)
	}
}
