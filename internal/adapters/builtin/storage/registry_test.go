// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"errors"
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

func TestDefaultRegistrySupportsRedisOwnershipModes(t *testing.T) {
	registry := DefaultRegistry()
	for _, ownership := range []cachev1alpha1.CacheBackendRemoteStorageOwnership{
		cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
	} {
		storage := &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
			Ownership: ownership,
			Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
		}
		if _, err := registry.Select(storage); err != nil {
			t.Fatalf("Select(Redis, %s): %v", ownership, err)
		}
	}
}

func TestDefaultRegistryRejectsUnknownProvider(t *testing.T) {
	_, err := DefaultRegistry().Select(&cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  "removed",
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
	})
	if !errors.Is(err, backendadapter.ErrNoProvider) {
		t.Fatalf("Select() error = %v, want ErrNoProvider", err)
	}
}
