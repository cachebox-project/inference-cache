// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package builtin

import (
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func TestNewRegistersCurrentRuntimeAndStorageAdapters(t *testing.T) {
	registries := New(Options{})
	if got := registries.Runtime.Len(); got != 3 {
		t.Fatalf("runtime registry length = %d, want 3", got)
	}
	cache := &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{
		Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
		Type:    cachev1alpha1.CacheBackendTypeLMCache,
		LMCache: &cachev1alpha1.LMCacheEngineSpec{Topology: cachev1alpha1.LMCacheTopologyPodLocal},
	}}
	if _, err := registries.Runtime.Select(adapterruntime.RuntimeVLLM, cache); err != nil {
		t.Fatalf("select vLLM LMCache MP adapter: %v", err)
	}
	if _, err := registries.Storage.Select(&cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
	}); err != nil {
		t.Fatalf("select managed Redis provider: %v", err)
	}
}
