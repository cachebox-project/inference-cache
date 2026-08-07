package storage

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func defaultProviderResources() *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
	}
}

func effectiveProviderResources(cache *cachev1alpha1.CacheBackend) *corev1.ResourceRequirements {
	if cache == nil {
		return nil
	}
	storage := cache.Spec.EffectiveRemoteStorage()
	if storage != nil {
		switch storage.Provider {
		case cachev1alpha1.CacheBackendRemoteStorageProviderRedis:
			if storage.Redis != nil && storage.Redis.Resources != nil {
				return storage.Redis.Resources
			}
		case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
			if storage.LMCacheServer != nil && storage.LMCacheServer.Resources != nil {
				return storage.LMCacheServer.Resources
			}
		case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
			if storage.Mooncake != nil && storage.Mooncake.Resources != nil {
				return storage.Mooncake.Resources
			}
		}
	}
	return defaultProviderResources()
}

func effectiveProviderImage(cache *cachev1alpha1.CacheBackend, provider cachev1alpha1.CacheBackendRemoteStorageProvider, fallback string) string {
	if cache == nil {
		return fallback
	}
	storage := cache.Spec.EffectiveRemoteStorage()
	if storage != nil && storage.Provider == provider {
		switch provider {
		case cachev1alpha1.CacheBackendRemoteStorageProviderRedis:
			if storage.Redis != nil && storage.Redis.Image != "" {
				return storage.Redis.Image
			}
		case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
			if storage.LMCacheServer != nil && storage.LMCacheServer.Image != "" {
				return storage.LMCacheServer.Image
			}
		case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
			if storage.Mooncake != nil && storage.Mooncake.Image != "" {
				return storage.Mooncake.Image
			}
		}
	}
	return fallback
}

func effectiveProviderCommand(cache *cachev1alpha1.CacheBackend, provider cachev1alpha1.CacheBackendRemoteStorageProvider) []string {
	if cache == nil {
		return nil
	}
	storage := cache.Spec.EffectiveRemoteStorage()
	if storage == nil || storage.Provider != provider {
		return nil
	}
	switch provider {
	case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
		if storage.LMCacheServer != nil {
			return storage.LMCacheServer.Command
		}
	case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
		if storage.Mooncake != nil {
			return storage.Mooncake.Command
		}
	}
	return nil
}
