// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestBindingForKeepsResolvedExternalEndpoint(t *testing.T) {
	storage := &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  "  cache.example:65432  ",
	}

	got := BindingFor(storage, ProtocolLMCache, "cache.example:65432")
	if got == nil {
		t.Fatal("BindingFor returned nil")
	}
	if got.Endpoint != "cache.example:65432" {
		t.Fatalf("binding endpoint = %q, want caller-resolved endpoint", got.Endpoint)
	}
}
