// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func validPodLocalMPBackend() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			LMCache: &cachev1alpha1.LMCacheEngineSpec{
				Topology: cachev1alpha1.LMCacheTopologyPodLocal,
				PodLocal: &cachev1alpha1.LMCachePodLocalSpec{Server: &cachev1alpha1.LMCachePodLocalServerSpec{
					Image:      "registry.example/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Port:       6500,
					L1Capacity: resource.MustParse("4Gi"),
					MaxWorkers: 4,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("5Gi")},
						Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("5Gi")},
					},
				}},
			},
			Integration:    &cachev1alpha1.CacheBackendIntegrationSpec{Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "engine"}},
		},
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
	if err == nil || !strings.Contains(err.Error(), "does not support database selection") {
		t.Fatalf("ValidateCreate error = %v, want pinned-adapter capability rejection", err)
	}
	if strings.Contains(err.Error(), "provider workload configuration") {
		t.Fatalf("external Redis connection settings were misclassified as managed workload config: %v", err)
	}
}

func TestValidateRedisAuthenticationForTypedPodLocal(t *testing.T) {
	newAuthBackend := func(ownership cachev1alpha1.CacheBackendRemoteStorageOwnership) *cachev1alpha1.CacheBackend {
		cb := validPodLocalMPBackend()
		cb.Name = "mp"
		cb.Namespace = "default"
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
			Ownership: ownership,
			Redis: &cachev1alpha1.RedisRemoteStorageSpec{Authentication: &cachev1alpha1.RedisAuthenticationSpec{
				Password: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"},
					Key:                  "password",
				},
			}},
		}
		if ownership == cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal {
			cb.Spec.RemoteStorage.Endpoint = "redis.example:6379"
		} else {
			cb.Spec.RemoteStorage.Redis.Image = "redis:7.4-alpine"
		}
		return cb
	}

	for _, ownership := range []cachev1alpha1.CacheBackendRemoteStorageOwnership{
		cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
	} {
		t.Run(string(ownership)+" password", func(t *testing.T) {
			cb := newAuthBackend(ownership)
			if errs := rejectUnimplementedRedisBindingFeatures(cb); len(errs) != 0 {
				t.Fatalf("authentication errors = %v", errs)
			}
			if _, err := shippingValidator().ValidateCreate(context.Background(), cb); err != nil {
				t.Fatalf("ValidateCreate: %v", err)
			}
		})
	}

	t.Run("external ACL username", func(t *testing.T) {
		cb := newAuthBackend(cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal)
		cb.Spec.RemoteStorage.Redis.Authentication.Username = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"}, Key: "username",
		}
		if errs := rejectUnimplementedRedisBindingFeatures(cb); len(errs) != 0 {
			t.Fatalf("external ACL authentication errors = %v", errs)
		}
	})

	t.Run("managed ACL username rejected", func(t *testing.T) {
		cb := newAuthBackend(cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged)
		cb.Spec.RemoteStorage.Redis.Authentication.Username = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"}, Key: "username",
		}
		errs := rejectUnimplementedRedisBindingFeatures(cb)
		if len(errs) != 1 || !strings.Contains(errs[0].Field, "username") {
			t.Fatalf("errors = %v, want managed username rejection", errs)
		}
	})

	t.Run("optional password rejected", func(t *testing.T) {
		cb := newAuthBackend(cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal)
		optional := true
		cb.Spec.RemoteStorage.Redis.Authentication.Password.Optional = &optional
		errs := rejectUnimplementedRedisBindingFeatures(cb)
		if len(errs) != 1 || !strings.Contains(errs[0].Field, "optional") {
			t.Fatalf("errors = %v, want optional rejection", errs)
		}
	})

	t.Run("empty selector rejected", func(t *testing.T) {
		cb := newAuthBackend(cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal)
		cb.Spec.RemoteStorage.Redis.Authentication.Password = corev1.SecretKeySelector{}
		errs := rejectUnimplementedRedisBindingFeatures(cb)
		if len(errs) != 2 {
			t.Fatalf("errors = %v, want name and key rejections", errs)
		}
	})

	t.Run("vLLM PodLocal admitted", func(t *testing.T) {
		cb := newAuthBackend(cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal)
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		if errs := rejectUnimplementedRedisBindingFeatures(cb); len(errs) != 0 {
			t.Fatalf("authentication errors = %v", errs)
		}
		if _, err := shippingValidator().ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("ValidateCreate: %v", err)
		}
	})

	t.Run("legacy vLLM remains rejected", func(t *testing.T) {
		cb := newAuthBackend(cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal)
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		cb.Spec.LMCache.Topology = ""
		cb.Spec.LMCache.PodLocal = nil
		errs := rejectUnimplementedRedisBindingFeatures(cb)
		if len(errs) != 1 || !strings.Contains(errs[0].Error(), "typed PodLocal LMCache MP") {
			t.Fatalf("errors = %v, want topology-scoped rejection", errs)
		}
	})

	t.Run("non-LMCache type remains rejected", func(t *testing.T) {
		cb := newAuthBackend(cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal)
		cb.Spec.Type = cachev1alpha1.CacheBackendTypeSGLangHiCache
		errs := rejectUnimplementedRedisBindingFeatures(cb)
		if len(errs) != 1 || !strings.Contains(errs[0].Error(), "typed PodLocal LMCache MP") {
			t.Fatalf("errors = %v, want cache-type-scoped rejection", errs)
		}
	})
}

func TestValidateLMCacheTopologyRequiresTypedPodLocal(t *testing.T) {
	cb := validPodLocalMPBackend()
	if errs := validateLMCacheTopology(cb); len(errs) != 0 {
		t.Fatalf("valid typed PodLocal errors: %v", errs)
	}

	cb.Spec.LMCache.Topology = ""
	if errs := validateLMCacheTopology(cb); len(errs) == 0 {
		t.Fatal("topology-less LMCache was accepted")
	}
}

func TestValidateLMCacheTopologyRejectsNodeLocalUntilImplemented(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
	cb.Spec.LMCache.PodLocal = nil
	cb.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{}
	if errs := validateLMCacheTopology(cb); len(errs) == 0 {
		t.Fatal("NodeLocal was accepted before Phase 8")
	}
}

func TestValidateLMCacheTopologyRejectsUnpinnedImage(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.LMCache.PodLocal.Server.Image = "registry.example/lmcache:latest"
	if errs := validateLMCacheTopology(cb); len(errs) == 0 {
		t.Fatal("mutable MP server image was accepted")
	}
}

func TestValidateLMCacheTopologyRejectsInsufficientMemory(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.LMCache.PodLocal.Server.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("4Gi")
	if errs := validateLMCacheTopology(cb); len(errs) == 0 {
		t.Fatal("MP server memory below L1 plus headroom was accepted")
	}
}

func TestValidateLMCacheTopologyCurrentMatrix(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*cachev1alpha1.CacheBackend)
		wantField string
	}{
		{name: "host only"},
		{name: "external Redis", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
				Endpoint: "redis.example:6379", Redis: &cachev1alpha1.RedisRemoteStorageSpec{},
			}
		}},
		{name: "missing PodLocal block", mutate: func(cb *cachev1alpha1.CacheBackend) { cb.Spec.LMCache.PodLocal = nil }, wantField: "spec.lmCache.podLocal"},
		{name: "mixed NodeLocal block", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{}
		}, wantField: "spec.lmCache.nodeLocal"},
		{name: "NodeLocal reserved", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
			cb.Spec.LMCache.PodLocal = nil
			cb.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{}
		}, wantField: "spec.lmCache.topology"},
		{name: "digest without repository", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.LMCache.PodLocal.Server.Image = "@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}, wantField: "spec.lmCache.podLocal.server.image"},
		{name: "KV event port collision", mutate: func(cb *cachev1alpha1.CacheBackend) { cb.Spec.LMCache.PodLocal.Server.Port = lmcacheKVEventPort }, wantField: "spec.lmCache.podLocal.server.port"},
		{name: "HTTP health port collision", mutate: func(cb *cachev1alpha1.CacheBackend) { cb.Spec.LMCache.PodLocal.Server.Port = lmcacheMPHTTPPort }, wantField: "spec.lmCache.podLocal.server.port"},
		{name: "memory request below shm", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.LMCache.PodLocal.Server.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("4Gi")
		}, wantField: "spec.lmCache.podLocal.server.resources.requests[memory]"},
		{name: "memory limit below shm", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.LMCache.PodLocal.Server.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("4Gi")
		}, wantField: "spec.lmCache.podLocal.server.resources.limits[memory]"},
		{name: "fractional extended resource", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.LMCache.PodLocal.Server.Resources.Requests[corev1.ResourceName("example.com/device")] = resource.MustParse("500m")
			cb.Spec.LMCache.PodLocal.Server.Resources.Limits[corev1.ResourceName("example.com/device")] = resource.MustParse("500m")
		}, wantField: "spec.lmCache.podLocal.server.resources.requests[example.com/device]"},
		{name: "EventsOnly cannot carry MP", mutate: func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
		}, wantField: "spec.lmCache"},
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
