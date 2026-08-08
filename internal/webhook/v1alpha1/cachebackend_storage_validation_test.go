// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"
	"testing"
)

func TestValidator_CanonicalCacheHierarchy(t *testing.T) {
	validator := shippingValidator()

	t.Run("sglang host-only", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
		if _, err := validator.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("ValidateCreate: %v", err)
		}
	})

	t.Run("host-only rejects autoscaling", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
		requireInvalidWithCause(t, validator, cb, "spec.autoscaling",
			"host-only backends")
	})

	t.Run("host memory capacity must be positive", func(t *testing.T) {
		for _, capacity := range []string{"0", "-1Gi"} {
			t.Run(capacity, func(t *testing.T) {
				cb := newBackend()
				cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
				quantity := resource.MustParse(capacity)
				cb.Spec.LMCache = &cachev1alpha1.LMCacheEngineSpec{
					HostMemory: &cachev1alpha1.CacheBackendHostMemorySpec{Capacity: &quantity},
				}
				requireInvalidWithCause(t, validator, cb, "spec.lmCache.hostMemory.capacity",
					"must be greater than zero")
			})
		}
	})

	t.Run("positive host memory capacity is admitted", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		quantity := resource.MustParse("1Gi")
		cb.Spec.LMCache = &cachev1alpha1.LMCacheEngineSpec{
			HostMemory: &cachev1alpha1.CacheBackendHostMemorySpec{Capacity: &quantity},
		}
		if _, err := validator.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("ValidateCreate: %v", err)
		}
	})

	t.Run("sglang managed redis", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
			Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			Redis:     &cachev1alpha1.RedisRemoteStorageSpec{Image: "redis:test"},
		}
		if _, err := validator.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("ValidateCreate: %v", err)
		}
	})

	t.Run("vllm rejects resp binding", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
			Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		}
		_, err := validator.ValidateCreate(context.Background(), cb)
		if err == nil || !strings.Contains(err.Error(), "does not accept") {
			t.Fatalf("ValidateCreate error = %v, want binding compatibility rejection", err)
		}
	})

	t.Run("external requires endpoint", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
			Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		}
		_, err := validator.ValidateCreate(context.Background(), cb)
		if err == nil || !strings.Contains(err.Error(), "remoteStorage.endpoint") {
			t.Fatalf("ValidateCreate error = %v, want endpoint rejection", err)
		}
	})

	t.Run("external endpoint scheme follows provider protocol", func(t *testing.T) {
		tests := []struct {
			name     string
			runtime  cachev1alpha1.CacheBackendRuntime
			provider cachev1alpha1.CacheBackendRemoteStorageProvider
			endpoint string
			wantErr  bool
		}{
			{name: "redis bare", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, endpoint: "redis.example:6379"},
			{name: "redis rejects lm scheme", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, endpoint: "lm://redis.example:6379", wantErr: true},
			{name: "redis rejects named port", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, endpoint: "redis.example:redis", wantErr: true},
			{name: "redis rejects zero port", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, endpoint: "redis.example:0", wantErr: true},
			{name: "redis rejects out-of-range port", runtime: cachev1alpha1.CacheBackendRuntimeSGLang, provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, endpoint: "redis.example:70000", wantErr: true},
			{name: "lmcache bare", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, endpoint: "cache.example:8200"},
			{name: "lmcache explicit scheme", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, endpoint: "lm://cache.example:8200"},
			{name: "lmcache rejects mooncake scheme", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, endpoint: "mooncakestore://cache.example:50051", wantErr: true},
			{name: "lmcache rejects named port", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, endpoint: "cache.example:not-a-port", wantErr: true},
			{name: "lmcache rejects zero port", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, endpoint: "cache.example:0", wantErr: true},
			{name: "lmcache rejects out-of-range port", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, endpoint: "cache.example:70000", wantErr: true},
			{name: "mooncake bare", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, endpoint: "mooncake.example:50051"},
			{name: "mooncake explicit scheme", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, endpoint: "mooncakestore://mooncake.example:50051"},
			{name: "mooncake rejects lm scheme", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, endpoint: "lm://mooncake.example:50051", wantErr: true},
			{name: "mooncake rejects named port", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, endpoint: "mooncakestore://mooncake.example:not-a-port", wantErr: true},
			{name: "mooncake rejects zero port", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, endpoint: "mooncakestore://mooncake.example:0", wantErr: true},
			{name: "mooncake rejects out-of-range port", runtime: cachev1alpha1.CacheBackendRuntimeVLLM, provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, endpoint: "mooncakestore://mooncake.example:70000", wantErr: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cb := newBackend()
				cb.Spec.Runtime = tt.runtime
				cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
					Provider:  tt.provider,
					Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
					Endpoint:  tt.endpoint,
				}
				if tt.wantErr {
					requireInvalidWithCause(t, validator, cb, "spec.remoteStorage.endpoint", "")
					return
				}
				if _, err := validator.ValidateCreate(context.Background(), cb); err != nil {
					t.Fatalf("ValidateCreate: %v", err)
				}
			})
		}
	})

	t.Run("managed provider resources are validated at their typed path", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
			Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			Redis: &cachev1alpha1.RedisRemoteStorageSpec{
				Resources: &corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("-1Gi"),
					},
				},
			},
		}
		_, err := validator.ValidateCreate(context.Background(), cb)
		if err == nil || !strings.Contains(err.Error(), "spec.remoteStorage.redis.resources.limits[memory]") {
			t.Fatalf("ValidateCreate error = %v, want typed provider resource path", err)
		}
	})

	t.Run("managed provider command requires non-empty entries", func(t *testing.T) {
		tests := []struct {
			name    string
			command []string
			path    string
		}{
			{name: "empty command", command: []string{}, path: "spec.remoteStorage.lmCacheServer.command"},
			{name: "empty executable", command: []string{""}, path: "spec.remoteStorage.lmCacheServer.command[0]"},
			{name: "blank argument", command: []string{"cache-server", " "}, path: "spec.remoteStorage.lmCacheServer.command[1]"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cb := newBackend()
				cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
				cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
					Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
					Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
					LMCacheServer: &cachev1alpha1.LMCacheServerRemoteStorageSpec{
						Command: tt.command,
					},
				}
				requireInvalidWithCause(t, validator, cb, tt.path, "must")
			})
		}

		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
			Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			Mooncake: &cachev1alpha1.MooncakeRemoteStorageSpec{
				Command: []string{""},
			},
		}
		requireInvalidWithCause(t, validator, cb, "spec.remoteStorage.mooncake.command[0]", "must not be empty")
	})

	t.Run("typed observation does not synthesize provider storage", func(t *testing.T) {
		cb := newBackend()
		cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "model-a"}
		if _, err := validator.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("ValidateCreate: %v", err)
		}
		storage := cb.Spec.EffectiveRemoteStorage()
		if storage != nil {
			t.Fatalf("EffectiveRemoteStorage() = %+v, want nil", storage)
		}
	})

}

func TestValidator_ResourcesLimitsBelowRequestsRejected(t *testing.T) {
	// limits.memory < requests.memory makes the operator's intent
	// impossible to satisfy at scheduling time and is the canonical
	// misconfiguration the rule exists to catch. Reject loudly at
	// admission with a field-scoped error rather than admit a CR the
	// pod will refuse later (and that the operator would have to
	// diagnose through downstream kubectl-describe spelunking).
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.lmCacheServer.resources.limits[memory]",
		"must be greater than or equal to spec.remoteStorage.lmCacheServer.resources.requests[memory]")
}

func TestValidator_ResourcesLimitsEqualRequestsAdmitted(t *testing.T) {
	// limits == requests is the canonical "exact size" intent and must
	// admit. The rule only rejects strict-less-than.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("limits==requests rejected: %v", err)
	}
}

func TestValidator_ResourcesRequestsOnlyAdmitted(t *testing.T) {
	// Requests-only is a valid shape (no upper bound declared); the
	// rule MUST NOT synthesise a phantom limit to compare against.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("requests-only rejected: %v", err)
	}
}

func TestValidator_ResourcesLimitsOnlyAdmitted(t *testing.T) {
	// Limits-only is also valid (scheduler treats limit as the request
	// when no request is given); no comparison is meaningful, so the
	// rule must not fire.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("limits-only rejected: %v", err)
	}
}

func TestValidator_ResourcesFractionalExtendedRejected(t *testing.T) {
	// K8s requires extended-resource quantities to be integers — a
	// fractional value like "nvidia.com/gpu: 500m" is rejected by the
	// apiserver on the rendered Pod. Mirror that rule at admission so
	// the operator sees a field-scoped error at `kubectl apply`.
	// Standard overcommittable resources (cpu, memory, ephemeral-
	// storage) allow fractional values and are not affected.
	for _, side := range []string{"requests", "limits"} {
		t.Run(side, func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{}
			entry := corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("500m"),
			}
			matching := corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("500m"),
			}
			if side == "requests" {
				managedLMCacheServer(cb).Resources.Requests = entry
				managedLMCacheServer(cb).Resources.Limits = matching
			} else {
				managedLMCacheServer(cb).Resources.Limits = entry
				managedLMCacheServer(cb).Resources.Requests = matching
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.lmCacheServer.resources.%s[nvidia.com/gpu]", side),
				"must be an integer quantity")
		})
	}
}

func TestValidator_ResourcesIntegerExtendedAdmitted(t *testing.T) {
	// Integer extended-resource quantities (e.g. nvidia.com/gpu: 1)
	// admit — the rule fires only on fractional values.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
		},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("integer extended resource rejected: %v", err)
	}
}

func TestValidator_ResourcesFractionalCPUAdmitted(t *testing.T) {
	// Standard overcommittable CPU MUST still accept fractional values
	// (250m is the canonical kubelet shape) — the integer rule applies
	// only to vendor-prefixed extended resources.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("fractional CPU rejected: %v", err)
	}
}

func TestValidator_ResourcesRequestsOnlyNonOvercommittableRejected(t *testing.T) {
	// K8s' container-resource contract requires non-overcommittable
	// resources (hugepages-*, vendor-prefixed extended resources) to
	// declare BOTH requests and limits — they cannot be specified in
	// requests alone. Reject at admission so the operator sees a
	// field-scoped error at `kubectl apply` rather than discovering
	// the gap downstream.
	for _, name := range []corev1.ResourceName{
		"hugepages-2Mi",
		"nvidia.com/gpu",
	} {
		t.Run(string(name), func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			qty := "1"
			if name == "hugepages-2Mi" {
				qty = "2Mi"
			}
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{name: resource.MustParse(qty)},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.lmCacheServer.resources.requests[%s]", name),
				"must also be set in spec.remoteStorage.lmCacheServer.resources.limits")
		})
	}
}

func TestValidator_ResourcesLimitsOnlyNonOvercommittableAdmitted(t *testing.T) {
	// Limits-only IS admitted for non-overcommittable resources —
	// K8s auto-populates requests from limits when only limits is set.
	// The rule we add fires only on the requests-only direction.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
		},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("limits-only extended resource rejected: %v", err)
	}
}

func TestValidator_ResourcesRequestsOnlyOvercommittableAdmitted(t *testing.T) {
	// The new rule MUST NOT touch overcommittable resources — a
	// requests-only cpu / memory shape is the canonical kubelet
	// "no upper bound" pattern and admits today.
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		t.Run(string(name), func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{name: resource.MustParse("1")},
			}
			if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
				t.Fatalf("requests-only overcommittable %q rejected: %v", name, err)
			}
		})
	}
}

func TestValidator_ResourcesNonOvercommittableMismatchRejected(t *testing.T) {
	// K8s requires limits==requests for non-overcommittable resources
	// (hugepages-* and any extended resource). Only the standard
	// overcommittable resources (cpu, memory, ephemeral-storage)
	// permit limits > requests. Mirror that rule at admission so a
	// CR with limits!=requests on, e.g., hugepages-2Mi or
	// nvidia.com/gpu is rejected at `kubectl apply` instead of
	// crashing the rendered Pod.
	for _, tc := range []struct {
		name     string
		resource corev1.ResourceName
		req      string
		lim      string
	}{
		{"hugepages mismatch", corev1.ResourceName("hugepages-2Mi"), "2Mi", "4Mi"},
		{"nvidia gpu mismatch", corev1.ResourceName("nvidia.com/gpu"), "1", "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{tc.resource: resource.MustParse(tc.req)},
				Limits:   corev1.ResourceList{tc.resource: resource.MustParse(tc.lim)},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.lmCacheServer.resources.limits[%s]", tc.resource),
				"must equal spec.remoteStorage.lmCacheServer.resources.requests")
		})
	}
}

func TestValidator_ResourcesNonOvercommittableEqualAdmitted(t *testing.T) {
	// The same non-overcommittable resources admit when limits == requests.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1")},
		Limits:   corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("non-overcommittable equal request/limit rejected: %v", err)
	}
}

func TestValidator_ResourcesReservedPrefixesRejected(t *testing.T) {
	// K8s reserves "kubernetes.io/" and "requests.kubernetes.io/" for
	// native resources — they cannot be used as vendor prefixes for
	// extended resources. The webhook must reject them so the rendered
	// pod doesn't fail apiserver validation later.
	for _, name := range []string{
		"kubernetes.io/myresource",
		"requests.kubernetes.io/myresource",
	} {
		t.Run(name, func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(name): resource.MustParse("1"),
				},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.lmCacheServer.resources.requests[%s]", name),
				"not a valid container resource name")
		})
	}
}

func TestValidator_ResourcesInvalidNameRejected(t *testing.T) {
	// ResourceList keys are opaque map keys at the CRD-schema layer:
	// a CR can be admitted with structurally-malformed names ("memory!",
	// empty string), and the kubelet rejects the pod later. Reject at
	// admission so the regression surfaces at `kubectl apply`.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceName("memory!"): resource.MustParse("4Gi"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.lmCacheServer.resources.requests[memory!]",
		"not a valid container resource name")
}

func TestValidator_ResourcesUnqualifiedNonStandardNameRejected(t *testing.T) {
	// K8s container-resource rules are stricter than IsQualifiedName:
	// a bare name like "foo" (no "/" prefix) is admitted by the schema
	// AND by IsQualifiedName, but the apiserver rejects the rendered
	// pod because non-standard container resources MUST be vendor-
	// prefixed (e.g. "nvidia.com/gpu"). Reject at admission so the
	// operator sees a field-scoped error at `kubectl apply` rather
	// than chasing it through a child Deployment apply.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceName("foo"): resource.MustParse("1"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.lmCacheServer.resources.requests[foo]",
		"not a valid container resource name")
}

func TestValidator_ResourcesMalformedHugepagesRejected(t *testing.T) {
	// `hugepages-<size>` is K8s-reserved, but the suffix MUST parse as
	// a positive resource.Quantity (e.g. "2Mi", "1Gi"). A bare
	// "hugepages-" or a non-numeric suffix like "hugepages-nope" is
	// rejected by the apiserver downstream, so admission rejects the
	// same shapes at write time.
	for _, name := range []string{"hugepages-", "hugepages-nope", "hugepages-0"} {
		t.Run(name, func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(name): resource.MustParse("1"),
				},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.lmCacheServer.resources.requests[%s]", name),
				"not a valid container resource name")
		})
	}
}

func TestValidator_ResourcesStandardContainerResourceNamesAdmitted(t *testing.T) {
	// The full set of standard container resource names — cpu, memory,
	// ephemeral-storage, and well-formed hugepages-<size> variants —
	// MUST admit. Pin the contract so a future tightening doesn't
	// accidentally exclude one of them. The hugepage quantity is chosen
	// to be a multiple of the page size advertised in the suffix
	// (K8s rejects hugepages-2Mi: 1 because 1 byte is not divisible by
	// 2Mi — see TestValidator_ResourcesHugepagesQuantityMustBeDivisible).
	for _, tc := range []struct {
		name corev1.ResourceName
		qty  string
	}{
		{corev1.ResourceCPU, "1"},
		{corev1.ResourceMemory, "1"},
		{corev1.ResourceEphemeralStorage, "1"},
		{corev1.ResourceName("hugepages-2Mi"), "4Mi"},
		{corev1.ResourceName("hugepages-1Gi"), "2Gi"},
	} {
		t.Run(string(tc.name), func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{tc.name: resource.MustParse(tc.qty)},
				Limits:   corev1.ResourceList{tc.name: resource.MustParse(tc.qty)},
			}
			if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
				t.Fatalf("standard container resource %q (qty=%s) rejected: %v", tc.name, tc.qty, err)
			}
		})
	}
}

func TestValidator_ResourcesHugepagesQuantityMustBeDivisible(t *testing.T) {
	// K8s rejects hugepages-<size>: <amount> when amount is not a
	// multiple of the page size — the kernel allocates whole pages.
	// Mirror that rule at admission so the operator sees a field-
	// scoped error at `kubectl apply` rather than the apiserver's
	// downstream rejection on the rendered Pod.
	for _, tc := range []struct {
		page string
		qty  string
	}{
		{"hugepages-2Mi", "3Mi"},   // 3Mi is not a multiple of 2Mi
		{"hugepages-2Mi", "1"},     // 1 byte not a multiple of 2Mi
		{"hugepages-1Gi", "512Mi"}, // 512Mi is not a multiple of 1Gi
	} {
		t.Run(tc.page+"/"+tc.qty, func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(tc.page): resource.MustParse(tc.qty),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceName(tc.page): resource.MustParse(tc.qty),
				},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.lmCacheServer.resources.requests[%s]", tc.page),
				"must be a multiple of the page size")
		})
	}
}

func TestValidator_ResourcesHugepagesAlignedQuantityAdmitted(t *testing.T) {
	// Aligned hugepage quantities admit — the rule fires only on
	// non-multiples of the page size.
	for _, tc := range []struct {
		page string
		qty  string
	}{
		{"hugepages-2Mi", "2Mi"},
		{"hugepages-2Mi", "4Mi"},
		{"hugepages-2Mi", "0"},
		{"hugepages-1Gi", "1Gi"},
		{"hugepages-1Gi", "4Gi"},
	} {
		t.Run(tc.page+"/"+tc.qty, func(t *testing.T) {
			v := shippingValidator()
			cb := newBackend()
			managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(tc.page): resource.MustParse(tc.qty),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceName(tc.page): resource.MustParse(tc.qty),
				},
			}
			if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
				t.Fatalf("aligned hugepage %s qty=%s rejected: %v", tc.page, tc.qty, err)
			}
		})
	}
}

func TestValidator_ResourcesValidExtendedNameAdmitted(t *testing.T) {
	// Vendor-prefixed extended resources are valid K8s ResourceNames;
	// the rule MUST admit them so operators can declare e.g.
	// nvidia.com/gpu on the cache-server container (rare but not
	// structurally forbidden).
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
		},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("vendor-prefixed extended resource rejected: %v", err)
	}
}

func TestValidator_ResourcesNegativeRequestRejected(t *testing.T) {
	// The CRD schema serialises requests/limits as resource.Quantity
	// strings — it admits a leading "-" without complaint, and the
	// kubelet rejects the negative quantity only when the pod tries
	// to schedule. Reject at admission with a field-scoped error so
	// the regression surfaces at `kubectl apply`.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1Gi")},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.lmCacheServer.resources.requests[memory]",
		"must be a non-negative quantity")
}

func TestValidator_ResourcesNegativeLimitRejected(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-100m")},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.lmCacheServer.resources.limits[cpu]",
		"must be a non-negative quantity")
}

func TestValidator_ResourcesZeroQuantityAdmitted(t *testing.T) {
	// Zero is permitted (matches the kubelet's >=0 contract): an
	// operator who writes `requests.memory: "0"` is explicitly opting
	// into "no guaranteed minimum", which is a valid (if unusual)
	// shape. Only strictly-negative quantities are rejected.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("0")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("zero-quantity request rejected: %v", err)
	}
}

func TestValidator_ResourcesClaimsRejected(t *testing.T) {
	// corev1.ResourceRequirements exposes Claims for the Dynamic Resource
	// Allocation (DRA) feature, but the renderer only copies Container.
	// Resources — it does NOT plumb the matching pod.spec.resourceClaims
	// the claims field references. Admitting a CR with non-empty Claims
	// would render a Deployment the apiserver rejects because the claim
	// names don't resolve at the pod level. Reject at admission until the
	// renderer learns to thread resourceClaims onto the PodSpec.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Claims: []corev1.ResourceClaim{{Name: "gpu-claim"}},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.lmCacheServer.resources.claims",
		"spec.remoteStorage.lmCacheServer.resources.claims is not supported")
}

func TestValidator_ResourcesEmptyClaimsAdmitted(t *testing.T) {
	// A nil/empty Claims slice MUST admit — the rule only fires on
	// operator-supplied entries, never on the absence of the field.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("nil-Claims rejected: %v", err)
	}
}

func TestValidator_ResourcesCPULimitsBelowRequestsRejected(t *testing.T) {
	// The rule generalises across every resource present in BOTH the
	// Requests and Limits maps — it's not specific to memory. CPU is
	// the obvious second case worth pinning so future contributors don't
	// silently narrow the rule back to memory-only.
	v := shippingValidator()
	cb := newBackend()
	managedLMCacheServer(cb).Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.lmCacheServer.resources.limits[cpu]",
		"must be greater than or equal to spec.remoteStorage.lmCacheServer.resources.requests[cpu]")
}

func TestValidator_CrossNamespaceEndpointWithoutOptInRejected(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	setCanonicalExternalStorage(cb, "shared-cache.team-b.svc.cluster.local:9000")
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"references namespace \"team-b\"")
}

func TestValidator_CrossNamespaceEndpointWithOptInAdmitted(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	// Carry a port — the new shape rule requires host:port; the
	// cross-namespace assertion below is unaffected by the port suffix.
	setCanonicalExternalStorage(cb, "shared-cache.team-b.svc.cluster.local:9000")
	cb.Spec.AllowCrossNamespace = true
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External cross-namespace endpoint with opt-in rejected: %v", err)
	}
}

func TestValidator_CanonicalCrossNamespaceEndpointWithoutOptInRejected(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  "shared-cache.team-b.svc.cluster.local:9000",
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"references namespace \"team-b\"")
}

func TestValidator_CanonicalCrossNamespaceEndpointWithOptInAdmitted(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  "shared-cache.team-b.svc.cluster.local:9000",
	}
	cb.Spec.AllowCrossNamespace = true
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("canonical cross-namespace endpoint with opt-in rejected: %v", err)
	}
}

func TestValidator_ExternalEndpoint_LMSchemeAdmitted(t *testing.T) {
	// Operators who prefer to be explicit can pre-fix the endpoint with
	// the LMCache lm:// scheme; the adapter passes it through unchanged.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "lm://cache.example.com:8200")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External with lm:// scheme rejected: %v", err)
	}
}

func TestValidator_ExternalEndpoint_HTTPSchemeRejected(t *testing.T) {
	// A non-lm:// scheme would concatenate to LMCACHE_REMOTE_URL=lm://
	// https://... at injection time, which the LMCache connector
	// rejects. Catch the misconfiguration at admission instead of in
	// engine-pod crash logs.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "https://cache.example.com:443/api")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		`scheme "https" is not supported`)
}

func TestValidator_ExternalEndpoint_PathRejected(t *testing.T) {
	// LMCache is a TCP-level protocol — paths/queries/fragments don't
	// belong on the wire and would be silently dropped at the engine
	// connector. Reject them at admission so the rejection message
	// surfaces the problem.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "cache.example.com:8200/path")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be host:port (optionally prefixed lm://)")
}

func TestValidator_ExternalEndpoint_LMSchemeOnlyRejected(t *testing.T) {
	// `lm://` alone is just the scheme — no host. Without this check
	// the CR admits, goes Ready=True, and the pod webhook injects
	// LMCACHE_REMOTE_URL=lm:// (the exact broken value the validation
	// exists to prevent).
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "lm://")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortOnlyRejected(t *testing.T) {
	// `:8200` is a port with no host — same broken-injection risk.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, ":8200")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_LMSchemePortOnlyRejected(t *testing.T) {
	// Scheme + port with no host.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "lm://:8200")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortlessHostRejected(t *testing.T) {
	// Bare host with no port is rejected: the LMCache connector dials a
	// specific TCP target, so spec.remoteStorage.endpoint must carry both
	// halves.
	// Without this check the CR admits and the engine boots with
	// LMCACHE_REMOTE_URL=lm://cache.example.com — the connector then
	// either picks an undocumented default or crashes; either way the
	// failure surfaces at the engine, not at admission where it belongs.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "cache.example.com")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_EmptyPortRejected(t *testing.T) {
	// Trailing colon with no port (`host:`) is the failure mode of an
	// operator who started typing the port and saved. Same broken
	// LMCACHE_REMOTE_URL=lm://cache.example.com: at injection.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "cache.example.com:")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortlessLMSchemeRejected(t *testing.T) {
	// Same rule applies when the scheme is explicit.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "lm://cache.example.com")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortlessIPv6Rejected(t *testing.T) {
	// Bracket-only IPv6 (`[::1]`) has no port either — reject for the
	// same reason. Validates that the bracket-aware path enforces the
	// port-required rule.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "[2001:db8::1]")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_EmbeddedWhitespaceRejected(t *testing.T) {
	// Leading/trailing whitespace is already trimmed for friendliness;
	// whitespace *inside* the address is not. `cache example:8200`
	// would otherwise pass the host:port split (host="cache example",
	// port="8200") and inject a malformed LMCACHE_REMOTE_URL — the
	// LMCache connector refuses to dial it at engine startup. Catch
	// the misconfiguration loudly at write time.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "cache example.com:8200")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must not contain whitespace or control characters")
}

func TestValidator_ExternalEndpoint_EmbeddedWhitespaceInPortRejected(t *testing.T) {
	// Same rule applies to the port half — `cache.example:82 00`
	// would split host="cache.example", port="82 00" and inject a
	// broken URL.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "cache.example:82 00")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must not contain whitespace or control characters")
}

func TestValidator_ExternalEndpoint_ControlCharRejected(t *testing.T) {
	// Embedded control chars (newline, tab, etc.) are rejected even
	// though they're "whitespace": same broken-URL injection risk,
	// plus a defence-in-depth against header injection if a future
	// consumer ever templates the endpoint into a text format.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "cache.example.com:8200\nLMCACHE_LOG_LEVEL=debug")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must not contain whitespace or control characters")
}

func TestValidator_ExternalEndpoint_BracketedIPv6ExtraColonRejected(t *testing.T) {
	// The bracketed form `[::1]:8200:bad` would otherwise pass with
	// host="::1" port="8200:bad" — the bracket strips the IPv6 colons
	// out of the host/port boundary calculation, but the naive port
	// half still contains the trailing `:bad`. Reject: the brackets are
	// the contract that makes the boundary unambiguous; sneaking an
	// extra colon past them produces an invalid
	// LMCACHE_REMOTE_URL=lm://[::1]:8200:bad at injection.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "[::1]:8200:bad")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_BracketedIPv6ExtraColonWithSchemeRejected(t *testing.T) {
	// Same bug surface with the explicit scheme — the scheme strip
	// shouldn't change the host:port shape check that follows.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "lm://[::1]:8200:bad")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_UnbracketedIPv6Rejected(t *testing.T) {
	// RFC 3986 requires brackets for IPv6 in URI authority components,
	// and there is no unambiguous host:port boundary without them. A
	// naive LastIndex(":") split would treat `2001:db8::1` as host=
	// "2001:db8:" port="1" — admission would pass and the engine pod
	// would inject LMCACHE_REMOTE_URL=lm://2001:db8::1, which the
	// LMCache connector cannot parse. Refuse at write time.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "2001:db8::1")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_IPv6Admitted(t *testing.T) {
	// IPv6 literals require brackets in host:port form; the validator
	// must accept them rather than mistaking the inner colons for
	// scheme/port separators.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "[2001:db8::1]:8200")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("IPv6 endpoint rejected: %v", err)
	}
}

func TestValidator_ExternalEndpoint_LMSchemeWithPathRejected(t *testing.T) {
	// Same concern as the bare-host case; the path-after-scheme variant
	// is just as broken.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "lm://cache.example.com:8200/path")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.endpoint",
		"must be host:port (optionally prefixed lm://)")
}

func TestServiceDNSNamespace(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		wantNS   string
		wantOK   bool
	}{
		{"bare svc DNS", "cache.team-a.svc", "team-a", true},
		{"cluster.local svc DNS", "cache.team-b.svc.cluster.local", "team-b", true},
		{"svc DNS with port", "cache.team-d.svc.cluster.local:9000", "team-d", true},
		{"https scheme + path", "https://cache.team-e.svc.cluster.local/api", "team-e", true},
		{"grpc scheme", "grpc://cache.team-f.svc:9090", "team-f", true},
		{"pod FQDN bare", "cache-0.cache.team-g.svc", "team-g", true},
		{"pod FQDN cluster.local", "cache-0.cache.team-h.svc.cluster.local", "team-h", true},
		{"pod FQDN with port", "cache-1.cache.team-i.svc.cluster.local:9000", "team-i", true},
		{"FQDN trailing dot", "cache.team-j.svc.cluster.local.", "team-j", true},
		{"FQDN trailing dot bare svc", "cache.team-k.svc.", "team-k", true},
		{"uppercase svc DNS", "Cache.TEAM-L.SVC.cluster.local", "team-l", true},
		{"uppercase pod FQDN trailing dot", "CACHE-0.cache.team-m.SVC.cluster.local.", "team-m", true},
		{"external hostname", "cache.example.com", "", false},
		{"external hostname with port", "cache.example.com:443", "", false},
		{"external with svc-shaped label", "cache.team-b.svc.example.com", "", false},
		{"external svc-shaped label with port", "cache.team-b.svc.example.com:443", "", false},
		{"non-default cluster domain", "cache.team-c.svc.private", "", false},
		{"bare hostname", "cache", "", false},
		{"two-label hostname", "cache.team-a", "", false},
		{"third label not svc", "cache.team-a.cluster", "", false},
		{"ipv4", "10.0.0.5:9000", "", false},
		{"empty", "", "", false},
		{"whitespace", "   ", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns, ok := serviceDNSNamespace(tc.endpoint)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ns != tc.wantNS {
				t.Fatalf("ns = %q, want %q", ns, tc.wantNS)
			}
		})
	}
}
