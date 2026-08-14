// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"fmt"
	"strings"
	"testing"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestValidateCacheHierarchyRedisOwnership(t *testing.T) {
	tests := []struct {
		name      string
		ownership cachev1alpha1.CacheBackendRemoteStorageOwnership
		endpoint  string
		image     string
		workload  bool
		wantErr   string
	}{
		{name: "managed", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged},
		{name: "managed endpoint", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged, endpoint: "redis.example:6379", wantErr: "managed providers"},
		{name: "external", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal, endpoint: "redis.example:6379"},
		{name: "external missing endpoint", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal, wantErr: "required"},
		{name: "external scheme", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal, endpoint: "redis://redis.example:6379", wantErr: "schemes are not supported"},
		{name: "external managed image", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal, endpoint: "redis.example:6379", image: "redis:7", wantErr: "Managed ownership"},
		{name: "managed workload", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged, workload: true},
		{name: "external managed workload", ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal, endpoint: "redis.example:6379", workload: true, wantErr: "Managed ownership"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cb := validPodLocalMPBackend()
			cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Ownership: tc.ownership,
				Endpoint:  tc.endpoint,
				Redis:     &cachev1alpha1.RedisRemoteStorageSpec{Image: tc.image},
			}
			if tc.workload {
				cb.Spec.RemoteStorage.Workload = &cachev1alpha1.CacheBackendManagedWorkloadSpec{
					NodeSelector: map[string]string{"pool": "cache"},
				}
			}
			errs := validateCacheHierarchy(cb)
			if tc.wantErr == "" && len(errs) != 0 {
				t.Fatalf("unexpected errors: %v", errs)
			}
			if tc.wantErr != "" && !strings.Contains(errs.ToAggregate().Error(), tc.wantErr) {
				t.Fatalf("errors = %v, want %q", errs, tc.wantErr)
			}
		})
	}
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.redis.resources.limits[memory]",
		"must be greater than or equal to spec.remoteStorage.redis.resources.requests[memory]")
}

func TestValidator_ResourcesLimitsEqualRequestsAdmitted(t *testing.T) {
	// limits == requests is the canonical "exact size" intent and must
	// admit. The rule only rejects strict-less-than.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{}
			entry := corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("500m"),
			}
			matching := corev1.ResourceList{
				corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("500m"),
			}
			if side == "requests" {
				cb.Spec.RemoteStorage.Redis.Resources.Requests = entry
				cb.Spec.RemoteStorage.Redis.Resources.Limits = matching
			} else {
				cb.Spec.RemoteStorage.Redis.Resources.Limits = entry
				cb.Spec.RemoteStorage.Redis.Resources.Requests = matching
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.redis.resources.%s[nvidia.com/gpu]", side),
				"must be an integer quantity")
		})
	}
}

func TestValidator_ResourcesIntegerExtendedAdmitted(t *testing.T) {
	// Integer extended-resource quantities (e.g. nvidia.com/gpu: 1)
	// admit — the rule fires only on fractional values.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{name: resource.MustParse(qty)},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.redis.resources.requests[%s]", name),
				"must also be set in spec.remoteStorage.redis.resources.limits")
		})
	}
}

func TestValidator_ResourcesLimitsOnlyNonOvercommittableAdmitted(t *testing.T) {
	// Limits-only IS admitted for non-overcommittable resources —
	// K8s auto-populates requests from limits when only limits is set.
	// The rule we add fires only on the requests-only direction.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{tc.resource: resource.MustParse(tc.req)},
				Limits:   corev1.ResourceList{tc.resource: resource.MustParse(tc.lim)},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.redis.resources.limits[%s]", tc.resource),
				"must equal spec.remoteStorage.redis.resources.requests")
		})
	}
}

func TestValidator_ResourcesNonOvercommittableEqualAdmitted(t *testing.T) {
	// The same non-overcommittable resources admit when limits == requests.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(name): resource.MustParse("1"),
				},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.redis.resources.requests[%s]", name),
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceName("memory!"): resource.MustParse("4Gi"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.redis.resources.requests[memory!]",
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceName("foo"): resource.MustParse("1"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.redis.resources.requests[foo]",
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(name): resource.MustParse("1"),
				},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.redis.resources.requests[%s]", name),
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(tc.page): resource.MustParse(tc.qty),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceName(tc.page): resource.MustParse(tc.qty),
				},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.remoteStorage.redis.resources.requests[%s]", tc.page),
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
			cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1Gi")},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.redis.resources.requests[memory]",
		"must be a non-negative quantity")
}

func TestValidator_ResourcesNegativeLimitRejected(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-100m")},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.redis.resources.limits[cpu]",
		"must be a non-negative quantity")
}

func TestValidator_ResourcesZeroQuantityAdmitted(t *testing.T) {
	// Zero is permitted (matches the kubelet's >=0 contract): an
	// operator who writes `requests.memory: "0"` is explicitly opting
	// into "no guaranteed minimum", which is a valid (if unusual)
	// shape. Only strictly-negative quantities are rejected.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
		Claims: []corev1.ResourceClaim{{Name: "gpu-claim"}},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.redis.resources.claims",
		"spec.remoteStorage.redis.resources.claims is not supported")
}

func TestValidator_ResourcesEmptyClaimsAdmitted(t *testing.T) {
	// A nil/empty Claims slice MUST admit — the rule only fires on
	// operator-supplied entries, never on the absence of the field.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
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
	cb.Spec.RemoteStorage.Redis.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage.redis.resources.limits[cpu]",
		"must be greater than or equal to spec.remoteStorage.redis.resources.requests[cpu]")
}

func TestValidateCacheHierarchyRejectsProviderConfigMismatch(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  "removed",
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	if errs := validateCacheHierarchy(cb); len(errs) == 0 {
		t.Fatal("provider/config mismatch was accepted")
	}
}
