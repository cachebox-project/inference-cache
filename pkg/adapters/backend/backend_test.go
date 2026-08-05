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

func TestBindingForNFSKeepsStructuredMountWithoutEndpoint(t *testing.T) {
	storage := &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderNFS,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		NFS: &cachev1alpha1.NFSRemoteStorageSpec{
			Server:    "10.0.0.25",
			Path:      "/hicache",
			MountPath: "/mnt/hicache",
		},
	}
	protocol, err := ProtocolFor(storage)
	if err != nil {
		t.Fatalf("ProtocolFor: %v", err)
	}
	got := BindingFor(storage, protocol, "")
	if got == nil || got.Protocol != ProtocolFile || got.NFS == nil {
		t.Fatalf("BindingFor = %+v, want structured file/NFS binding", got)
	}
	if got.NFS.Server != "10.0.0.25" || got.NFS.Path != "/hicache" || got.NFS.MountPath != "/mnt/hicache" {
		t.Fatalf("NFS binding = %+v", got.NFS)
	}
	if BindingRequiresEndpoint(got) {
		t.Fatal("file/NFS binding unexpectedly requires an endpoint")
	}
}
