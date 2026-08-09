// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
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

func TestBindingForCarriesOnlyTypedRedisConnectionSettings(t *testing.T) {
	database := int32(3)
	storage := &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  "redis.example:6379",
		Redis: &cachev1alpha1.RedisRemoteStorageSpec{
			Image:    "must-not-be-part-of-runtime-binding",
			Database: &database,
			Authentication: &cachev1alpha1.RedisAuthenticationSpec{
				Password: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"},
					Key:                  "password",
				},
			},
		},
	}

	got := BindingFor(storage, ProtocolRESP, "redis.example:6379")
	if got == nil || got.Redis == nil {
		t.Fatalf("BindingFor = %+v, want typed Redis binding", got)
	}
	if got.Redis.Authentication == nil || got.Redis.Authentication.Password.Name != "redis-auth" {
		t.Fatalf("Redis authentication = %+v, want secret selector", got.Redis.Authentication)
	}
	if got.Redis.Database == nil || *got.Redis.Database != 3 {
		t.Fatalf("Redis database = %v, want 3", got.Redis.Database)
	}
	*storage.Redis.Database = 9
	storage.Redis.Authentication.Password.Name = "changed"
	if *got.Redis.Database != 3 || got.Redis.Authentication.Password.Name != "redis-auth" {
		t.Fatalf("binding aliases mutable spec data: %+v", got.Redis)
	}
}
