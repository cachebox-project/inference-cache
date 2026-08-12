// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

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
		if storage.Provider == cachev1alpha1.CacheBackendRemoteStorageProviderRedis &&
			storage.Redis != nil && storage.Redis.Resources != nil {
			return storage.Redis.Resources
		}
	}
	return defaultProviderResources()
}

func defaultServerResources(cache *cachev1alpha1.CacheBackend) corev1.ResourceRequirements {
	if resources := effectiveProviderResources(cache); resources != nil {
		return *resources.DeepCopy()
	}
	return corev1.ResourceRequirements{}
}

func effectiveProviderImage(cache *cachev1alpha1.CacheBackend, provider cachev1alpha1.CacheBackendRemoteStorageProvider, fallback string) string {
	if cache == nil {
		return fallback
	}
	storage := cache.Spec.EffectiveRemoteStorage()
	if storage != nil && storage.Provider == provider &&
		provider == cachev1alpha1.CacheBackendRemoteStorageProviderRedis &&
		storage.Redis != nil && storage.Redis.Image != "" {
		return storage.Redis.Image
	}
	return fallback
}
