package provider

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

func TestExternalNFSProviderHasFileProtocolAndNoWorkload(t *testing.T) {
	cache := &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{
		RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderNFS,
			Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
			NFS: &cachev1alpha1.NFSRemoteStorageSpec{
				Server: "10.0.0.25", Path: "/hicache", MountPath: "/mnt/hicache",
			},
		},
	}}
	selected, err := DefaultRegistry().Select(cache.Spec.RemoteStorage)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	rendered, err := selected.Render(cache)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if rendered.Protocol != "file" || rendered.PodSpec != nil || rendered.Service != nil {
		t.Fatalf("rendered NFS storage = %+v, want file protocol without workload", rendered)
	}
}

func TestCanonicalProviderDoesNotInheritLegacyWorkloadConfig(t *testing.T) {
	cache := &cachev1alpha1.CacheBackend{
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			RemoteStorage: &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
				Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
			},
			BackendConfig: map[string]string{"redisImage": "legacy.example/redis:wrong"},
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
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
	if container.Image == "legacy.example/redis:wrong" {
		t.Fatal("canonical provider inherited legacy backendConfig.redisImage")
	}
	wantMemory := resource.MustParse("8Gi")
	if got := container.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantMemory) != 0 {
		t.Fatalf("canonical default memory limit = %s, want %s", got.String(), wantMemory.String())
	}
}

func TestLegacyProviderRetainsBoundedResourcesWithoutDefaulter(t *testing.T) {
	cache := &cachev1alpha1.CacheBackend{
		Spec: cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeLMCache},
	}
	rendered, _, err := ResolveLMCacheServer(cache)
	if err != nil {
		t.Fatalf("ResolveLMCacheServer: %v", err)
	}
	wantLimit := resource.MustParse("8Gi")
	wantRequest := resource.MustParse("4Gi")
	resources := rendered.Containers[0].Resources
	if got := resources.Limits[corev1.ResourceMemory]; got.Cmp(wantLimit) != 0 {
		t.Fatalf("legacy fallback memory limit = %s, want %s", got.String(), wantLimit.String())
	}
	if got := resources.Requests[corev1.ResourceMemory]; got.Cmp(wantRequest) != 0 {
		t.Fatalf("legacy fallback memory request = %s, want %s", got.String(), wantRequest.String())
	}
}
