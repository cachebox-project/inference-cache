package backend

import (
	"strings"
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestValidateNFSServerIPv6UsesKubeletLiteralContract(t *testing.T) {
	// corev1.NFSVolumeSource.Server carries the raw literal. Kubelet's in-tree
	// NFS plugin recognizes it with netutil.IsIPv6String, adds brackets in
	// getServerFromSource, and only then builds "[server]:path" for mount.nfs.
	if err := ValidateNFSServer("2001:db8::25"); err != nil {
		t.Fatalf("raw IPv6 literal rejected: %v", err)
	}
	if err := ValidateNFSServer("[2001:db8::25]"); err == nil {
		t.Fatal("bracketed IPv6 must be rejected at the NFSVolumeSource.Server boundary")
	}
}

func TestValidateInlineNFSBinding(t *testing.T) {
	valid := func() *cachev1alpha1.CacheBackendRemoteStorageSpec {
		return &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderNFS,
			Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
			NFS: &cachev1alpha1.NFSRemoteStorageSpec{
				Server:    "10.0.0.25",
				Path:      "/hicache",
				MountPath: "/mnt/hicache",
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*cachev1alpha1.CacheBackendRemoteStorageSpec)
		wantErr string
	}{
		{name: "valid"},
		{name: "managed ownership", mutate: func(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) {
			storage.Ownership = cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged
		}, wantErr: "ownership must be External"},
		{name: "endpoint", mutate: func(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) {
			storage.Endpoint = "nfs.example.com:2049"
		}, wantErr: "endpoint must be empty"},
		{name: "missing NFS", mutate: func(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) {
			storage.NFS = nil
		}, wantErr: "remoteStorage.nfs is required"},
		{name: "invalid server", mutate: func(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) {
			storage.NFS.Server = "@"
		}, wantErr: "remoteStorage.nfs.server"},
		{name: "relative export path", mutate: func(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) {
			storage.NFS.Path = "hicache"
		}, wantErr: "remoteStorage.nfs.path"},
		{name: "root mount", mutate: func(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) {
			storage.NFS.MountPath = "/"
		}, wantErr: "must not mount remote storage over the container root"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := valid()
			if tt.mutate != nil {
				tt.mutate(storage)
			}
			err := ValidateInlineNFSBinding(storage)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateInlineNFSBinding: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateInlineNFSBinding error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

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
