// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestManagedRedisProviderOwnsTypedWorkloadConfig(t *testing.T) {
	memory := resource.MustParse("2Gi")
	cache := &cachev1alpha1.CacheBackend{
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
				Redis: &cachev1alpha1.RedisRemoteStorageSpec{
					Image: "registry.example/redis:test",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{corev1.ResourceMemory: memory},
					},
				},
			},
		},
	}

	provider, err := DefaultRegistry().Select(cache.Spec.RemoteStorage)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	rendered, err := provider.Render(cache)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if rendered.Protocol != "resp" {
		t.Fatalf("protocol = %q, want resp", rendered.Protocol)
	}
	container := rendered.PodSpec.Containers[0]
	if container.Image != "registry.example/redis:test" {
		t.Fatalf("image = %q, want typed Redis image", container.Image)
	}
	if got := container.Resources.Limits[corev1.ResourceMemory]; got.Cmp(memory) != 0 {
		t.Fatalf("memory limit = %s, want %s", got.String(), memory.String())
	}
}

func TestCanonicalProviderUsesBoundedDefaults(t *testing.T) {
	cache := &cachev1alpha1.CacheBackend{
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
				Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
			},
		},
	}

	selected, err := DefaultRegistry().Select(cache.Spec.RemoteStorage)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	rendered, err := selected.Render(cache)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	container := rendered.PodSpec.Containers[0]
	wantMemory := resource.MustParse("8Gi")
	if got := container.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantMemory) != 0 {
		t.Fatalf("canonical default memory limit = %s, want %s", got.String(), wantMemory.String())
	}
}

func TestProviderRetainsBoundedResourcesWithoutDefaulter(t *testing.T) {
	cache := &cachev1alpha1.CacheBackend{
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			},
		},
	}
	rendered, _, err := ResolveLMCacheServer(cache)
	if err != nil {
		t.Fatalf("ResolveLMCacheServer: %v", err)
	}
	wantLimit := resource.MustParse("8Gi")
	wantRequest := resource.MustParse("4Gi")
	resources := rendered.Containers[0].Resources
	if got := resources.Limits[corev1.ResourceMemory]; got.Cmp(wantLimit) != 0 {
		t.Fatalf("fallback memory limit = %s, want %s", got.String(), wantLimit.String())
	}
	if got := resources.Requests[corev1.ResourceMemory]; got.Cmp(wantRequest) != 0 {
		t.Fatalf("fallback memory request = %s, want %s", got.String(), wantRequest.String())
	}
}
