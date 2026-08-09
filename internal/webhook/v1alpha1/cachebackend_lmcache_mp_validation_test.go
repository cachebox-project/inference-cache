// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"strings"
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const testMPServerImage = "registry.example/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func validPodLocalMPBackend() *cachev1alpha1.CacheBackend {
	l1 := resource.MustParse("1Gi")
	return &cachev1alpha1.CacheBackend{
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			LMCache: &cachev1alpha1.LMCacheEngineSpec{
				Topology: cachev1alpha1.LMCacheTopologyPodLocal,
				PodLocal: &cachev1alpha1.LMCachePodLocalSpec{
					Server: &cachev1alpha1.LMCachePodLocalServerSpec{
						Image:      testMPServerImage,
						Port:       6555,
						L1Capacity: l1,
						MaxWorkers: 1,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("3Gi"),
							},
						},
					},
				},
			},
		},
	}
}

func TestValidatorMPProviderMatrix(t *testing.T) {
	tests := []struct {
		name     string
		runtime  cachev1alpha1.CacheBackendRuntime
		provider cachev1alpha1.CacheBackendRemoteStorageProvider
		wantErr  bool
	}{
		{name: "vLLM host-only", runtime: cachev1alpha1.CacheBackendRuntimeVLLM},
		{name: "SGLang host-only", runtime: cachev1alpha1.CacheBackendRuntimeSGLang},
		{name: "vLLM Redis", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis},
		{name: "SGLang Redis", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis},
		{name: "vLLM legacy LMCacheServer", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, wantErr: true},
		{name: "SGLang legacy LMCacheServer", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, wantErr: true},
		{name: "vLLM Mooncake future", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, wantErr: true},
		{name: "SGLang Mooncake future", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, wantErr: true},
	}

	validator := shippingValidator()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cb := validPodLocalMPBackend()
			cb.Name = "mp"
			cb.Namespace = "default"
			cb.Spec.Runtime = tc.runtime
			if tc.provider != "" {
				cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
					Provider:  tc.provider,
					Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
					Endpoint:  "storage.example:6379",
				}
			}
			_, err := validator.ValidateCreate(context.Background(), cb)
			if tc.wantErr && err == nil {
				t.Fatal("expected admission rejection")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected admission rejection: %v", err)
			}
		})
	}
}

func TestValidateLMCacheTopology(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*cachev1alpha1.CacheBackend)
		wantField string
	}{
		{name: "PodLocal host-only"},
		{
			name: "PodLocal Redis",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
					Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
					Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
					Endpoint:  "redis.example:6379",
				}
			},
		},
		{
			name: "block without topology",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.Topology = ""
			},
			wantField: "spec.lmCache.topology",
		},
		{
			name: "PodLocal missing block",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.PodLocal = nil
			},
			wantField: "spec.lmCache.podLocal",
		},
		{
			name: "PodLocal and NodeLocal mixed",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{}
			},
			wantField: "spec.lmCache.nodeLocal",
		},
		{
			name: "NodeLocal reserved",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				server := cb.Spec.LMCache.PodLocal.Server
				cb.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
				cb.Spec.LMCache.PodLocal = nil
				cb.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{
					Server: &cachev1alpha1.LMCacheNodeLocalServerSpec{
						Image:         server.Image,
						Port:          server.Port,
						L1Capacity:    server.L1Capacity,
						MaxGPUWorkers: 1,
						MaxCPUWorkers: 1,
						Resources:     server.Resources,
					},
				}
			},
			wantField: "spec.lmCache.topology",
		},
		{
			name: "legacy host memory mixed",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				capacity := resource.MustParse("1Gi")
				cb.Spec.LMCache.HostMemory = &cachev1alpha1.CacheBackendHostMemorySpec{Capacity: &capacity}
			},
			wantField: "spec.lmCache.hostMemory",
		},
		{
			name: "legacy worker image mixed",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.WorkerImage = "legacy:latest"
			},
			wantField: "spec.lmCache.workerImage",
		},
		{
			name: "legacy serde mixed",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.RemoteSerde = "cachegen"
			},
			wantField: "spec.lmCache.remoteSerde",
		},
		{
			name: "legacy LMCacheServer L3",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
					Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
					Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
					Endpoint:  "cache.example:8200",
				}
			},
			wantField: "spec.remoteStorage.provider",
		},
		{
			name: "Mooncake L3 not implemented",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
					Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
					Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
					Endpoint:  "mooncake.example:50051",
				}
			},
			wantField: "spec.remoteStorage.provider",
		},
		{
			name: "image tag is not immutable",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.PodLocal.Server.Image = "lmcache/standalone:v0.5.3"
			},
			wantField: "spec.lmCache.podLocal.server.image",
		},
		{
			name: "digest without image name",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.PodLocal.Server.Image = "@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			},
			wantField: "spec.lmCache.podLocal.server.image",
		},
		{
			name: "event port collision",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.PodLocal.Server.Port = lmcacheKVEventPort
			},
			wantField: "spec.lmCache.podLocal.server.port",
		},
		{
			name: "memory request has no headroom",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.PodLocal.Server.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1Gi")
			},
			wantField: "spec.lmCache.podLocal.server.resources.requests[memory]",
		},
		{
			name: "fractional extended resource",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.LMCache.PodLocal.Server.Resources.Requests[corev1.ResourceName("example.com/device")] = resource.MustParse("500m")
				cb.Spec.LMCache.PodLocal.Server.Resources.Limits[corev1.ResourceName("example.com/device")] = resource.MustParse("500m")
			},
			wantField: "spec.lmCache.podLocal.server.resources.requests[example.com/device]",
		},
		{
			name: "EventsOnly cannot carry MP",
			mutate: func(cb *cachev1alpha1.CacheBackend) {
				cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly}
			},
			wantField: "spec.lmCache",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cb := validPodLocalMPBackend()
			if tc.mutate != nil {
				tc.mutate(cb)
			}
			errs := validateLMCacheTopology(cb)
			if tc.wantField == "" {
				if len(errs) != 0 {
					t.Fatalf("unexpected errors: %v", errs)
				}
				return
			}
			for _, err := range errs {
				if err.Field == tc.wantField {
					return
				}
			}
			t.Fatalf("errors %v do not contain field %q", errs, tc.wantField)
		})
	}
}

func TestValidateLMCacheTopologyLeavesLegacyShapeUntouched(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.LMCache.Topology = ""
	cb.Spec.LMCache.PodLocal = nil
	cb.Spec.LMCache.WorkerImage = "legacy-worker:test"
	if errs := validateLMCacheTopology(cb); len(errs) != 0 {
		t.Fatalf("legacy topology-less shape should be left to compatibility rules: %v", errs)
	}
}

func TestRejectUnimplementedRedisBindingFeatures(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  "redis.example:6379",
		Redis: &cachev1alpha1.RedisRemoteStorageSpec{
			Database: func() *int32 { v := int32(1); return &v }(),
		},
	}
	errs := rejectUnimplementedRedisBindingFeatures(cb)
	if len(errs) != 1 || !strings.Contains(errs[0].Field, "database") {
		t.Fatalf("errors = %v, want database rejection", errs)
	}
	_, err := shippingValidator().ValidateCreate(context.Background(), cb)
	if err == nil || !strings.Contains(err.Error(), "not rendered until Phase 2") {
		t.Fatalf("ValidateCreate error = %v, want inert-binding rejection", err)
	}
	if strings.Contains(err.Error(), "provider workload configuration") {
		t.Fatalf("external Redis connection settings were misclassified as managed workload config: %v", err)
	}
}
