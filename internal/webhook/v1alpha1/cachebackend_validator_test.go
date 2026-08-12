// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func i32p(v int32) *int32 { return &v }

func newBackend() *cachev1alpha1.CacheBackend {
	backend := validPodLocalMPBackend()
	backend.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	return backend
}

func TestValidator_HappyPath_LMCacheAdmitted(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("happy-path LMCache rejected: %v", err)
	}
}

func TestValidator_Update_NewObjectChecked(t *testing.T) {
	// ValidateUpdate validates the new object just as create does.
	v := shippingValidator()
	old := newBackend()
	newCB := newBackend()
	newCB.Spec.Type = cachev1alpha1.CacheBackendType("unsupported")
	_, err := v.ValidateUpdate(context.Background(), old, newCB)
	if err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid on update, got %v", err)
	}
}

func TestValidator_Delete_AlwaysAllowed(t *testing.T) {
	// Even a structurally-broken backend must be deletable so operators can
	// clean up bad state — the validator must never block delete.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendType("unsupported")
	if _, err := v.ValidateDelete(context.Background(), cb); err != nil {
		t.Fatalf("ValidateDelete rejected: %v", err)
	}
}

func TestValidator_PluggableRuleAppendable(t *testing.T) {
	// The whole point of the Rules slice is that a future module (M6's
	// runtime/backend compatibility check) can plug in a new admission rule
	// as a one-line append without editing the handler — this test pins
	// that contract.
	rejectAll := func(cb *cachev1alpha1.CacheBackend) field.ErrorList {
		return field.ErrorList{field.Invalid(field.NewPath("spec"), cb.Spec, "synthetic")}
	}
	v := &CacheBackendValidator{Rules: append(DefaultValidationRules, rejectAll)}
	cb := newBackend()
	requireInvalidWithCause(t, v, cb, "spec", "synthetic")
}

func TestSetupCacheBackendWebhookRequiresRegistry(t *testing.T) {
	if err := SetupCacheBackendWebhookWithManager(nil, nil); err == nil || !strings.Contains(err.Error(), "registry is required") {
		t.Fatalf("SetupCacheBackendWebhookWithManager error = %v, want missing-registry error", err)
	}
}

func TestValidatorRejectsOverlappingEngineSelectorsInNamespace(t *testing.T) {
	existing := newBackend()
	existing.Name = "broad"
	existing.Namespace = "team-a"
	existing.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}

	tests := []struct {
		name     string
		selector map[string]string
		wantErr  bool
	}{
		{name: "same domain", selector: map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}, wantErr: true},
		{name: "different domain is disjoint", selector: map[string]string{cachev1alpha1.CacheBackendDomainLabel: "sglang"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := newBackend()
			candidate.Name = "candidate"
			candidate.Namespace = "team-a"
			candidate.Spec.EngineSelector.MatchLabels = tc.selector
			_, err := selectorValidator(t, existing).ValidateCreate(context.Background(), candidate)
			if tc.wantErr {
				if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "already owned") {
					t.Fatalf("ValidateCreate error = %v, want overlapping-selector Invalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateCreate rejected disjoint selector: %v", err)
			}
		})
	}
}

func TestValidatorEngineSelectorOverlapIsNamespaceScoped(t *testing.T) {
	existing := newBackend()
	existing.Name = "other-namespace"
	existing.Namespace = "team-b"
	existing.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}
	candidate := newBackend()
	candidate.Name = "candidate"
	candidate.Namespace = "team-a"
	candidate.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}
	if _, err := selectorValidator(t, existing).ValidateCreate(context.Background(), candidate); err != nil {
		t.Fatalf("same selector in another namespace rejected: %v", err)
	}
}

func TestValidatorRejectsUpdateThatIntroducesSelectorOverlap(t *testing.T) {
	existing := newBackend()
	existing.Name = "existing"
	existing.Namespace = "team-a"
	existing.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}
	oldCB := newBackend()
	oldCB.Name = "candidate"
	oldCB.Namespace = "team-a"
	oldCB.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "sglang"}
	newCB := oldCB.DeepCopy()
	newCB.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}

	_, err := selectorValidator(t, existing).ValidateUpdate(context.Background(), oldCB, newCB)
	if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "already owned") {
		t.Fatalf("ValidateUpdate error = %v, want newly-overlapping selector Invalid", err)
	}
}

func TestValidatorRejectsUnrelatedUpdateWhileSelectorOverlapExists(t *testing.T) {
	existing := newBackend()
	existing.Name = "existing"
	existing.Namespace = "team-a"
	existing.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}
	oldCB := newBackend()
	oldCB.Name = "candidate"
	oldCB.Namespace = "team-a"
	oldCB.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}
	newCB := oldCB.DeepCopy()
	newCB.Labels = map[string]string{"maintenance": "requested"}

	_, err := selectorValidator(t, existing).ValidateUpdate(context.Background(), oldCB, newCB)
	if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "already owned") {
		t.Fatalf("unrelated update error = %v, want existing overlap to remain blocked", err)
	}
}

func TestValidatorRequiresSoleCacheDomainSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector map[string]string
		want     string
	}{
		{name: "app selector is not an ownership domain", selector: map[string]string{"app": "vllm"}, want: cachev1alpha1.CacheBackendDomainLabel},
		{name: "extra ownership key", selector: map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm", "app": "vllm"}, want: "must contain only"},
		{name: "invalid domain value", selector: map[string]string{cachev1alpha1.CacheBackendDomainLabel: "Qwen/Qwen"}, want: "Invalid value"},
		{name: "canonical domain", selector: map[string]string{cachev1alpha1.CacheBackendDomainLabel: "vllm"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cb := newBackend()
			cb.Spec.EngineSelector.MatchLabels = tc.selector
			_, err := shippingValidator().ValidateCreate(context.Background(), cb)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("canonical selector rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateCreate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidatorRejectsNonCanonicalSelectorOnEveryUpdate(t *testing.T) {
	oldCB := newBackend()
	oldCB.Spec.EngineSelector.MatchLabels = map[string]string{"app": "legacy"}

	unrelated := oldCB.DeepCopy()
	unrelated.Labels = map[string]string{"maintenance": "requested"}
	if _, err := shippingValidator().ValidateUpdate(context.Background(), oldCB, unrelated); err == nil || !strings.Contains(err.Error(), cachev1alpha1.CacheBackendDomainLabel) {
		t.Fatalf("non-canonical selector update error = %v, want strict domain requirement", err)
	}

	canonical := oldCB.DeepCopy()
	canonical.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "canonical"}
	if _, err := shippingValidator().ValidateUpdate(context.Background(), oldCB, canonical); err != nil {
		t.Fatalf("canonical selector update rejected: %v", err)
	}
}

func TestValidator_VLLMRoleReadOnlyRejected(t *testing.T) {
	// vLLM renders ReadOnly as kv_consumer, but LMCache 0.5.3 does not enforce
	// that directionality. Admission must reject the unsupported API promise.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := newBackend()
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Role: cachev1alpha1.CacheBackendIntegrationRoleReadOnly,
	}
	requireInvalidWithCause(t, v, cb, "spec.integration.role", "directional cache access")
}

func defaultShippingRegistry() *adapterruntime.Registry {
	registry := adapterruntime.NewRegistry()
	registry.Register(builtinruntime.NewVLLMLMCacheMPAdapter(builtinruntime.SubscriberConfig{}))
	registry.Register(builtinruntime.NewSGLangLMCacheAdapter(builtinruntime.SubscriberConfig{}))
	registry.Register(builtinruntime.NewSGLangHiCacheAdapter(builtinruntime.SubscriberConfig{}))
	return registry
}

func shippingValidator() *CacheBackendValidator {
	return &CacheBackendValidator{Registry: defaultShippingRegistry()}
}

func selectorValidator(t *testing.T, existing ...*cachev1alpha1.CacheBackend) *CacheBackendValidator {
	t.Helper()
	objects := make([]runtime.Object, len(existing))
	for i := range existing {
		objects[i] = existing[i]
	}
	return &CacheBackendValidator{
		Registry: defaultShippingRegistry(),
		Reader:   fake.NewClientBuilder().WithScheme(newCacheScheme(t)).WithRuntimeObjects(objects...).Build(),
	}
}

type rejectingBindingAdapter struct {
	adapterruntime.KVCacheRuntimeAdapter
}

func (rejectingBindingAdapter) SupportsBinding(*backendadapter.Binding) bool { return false }

func TestValidatorTypedLMCacheChecksRuntimeBindingCapability(t *testing.T) {
	cb := newBackend()
	registry := adapterruntime.NewRegistry()
	registry.Register(rejectingBindingAdapter{
		KVCacheRuntimeAdapter: builtinruntime.NewVLLMLMCacheMPAdapter(builtinruntime.SubscriberConfig{}),
	})

	requireInvalidWithCause(t, &CacheBackendValidator{Registry: registry}, cb,
		"spec.remoteStorage.provider", "does not accept remote-storage protocol")
}

func newHiCacheBackend() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "hicache", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeSGLangHiCache,
			HiCache: &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"},
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeOffload,
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{cachev1alpha1.CacheBackendDomainLabel: "sglang"}},
		},
	}
}

func requireInvalidWithCause(t *testing.T, validator *CacheBackendValidator, cb *cachev1alpha1.CacheBackend, wantField, wantMsg string) {
	t.Helper()
	_, err := validator.ValidateCreate(context.Background(), cb)
	if err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("ValidateCreate() error = %v, want Invalid", err)
	}
	statusErr := err.(*apierrors.StatusError)
	for _, cause := range statusErr.Status().Details.Causes {
		if cause.Field == wantField && strings.Contains(cause.Message, wantMsg) {
			return
		}
	}
	t.Fatalf("no cause on field %q containing %q: %+v", wantField, wantMsg, statusErr.Status().Details.Causes)
}

func requireUpdateInvalidWithCause(t *testing.T, validator *CacheBackendValidator, oldCB, newCB *cachev1alpha1.CacheBackend, wantField, wantMsg string) {
	t.Helper()
	_, err := validator.ValidateUpdate(context.Background(), oldCB, newCB)
	if err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("ValidateUpdate() error = %v, want Invalid", err)
	}
	statusErr := err.(*apierrors.StatusError)
	for _, cause := range statusErr.Status().Details.Causes {
		if cause.Field == wantField && strings.Contains(cause.Message, wantMsg) {
			return
		}
	}
	t.Fatalf("no cause on field %q containing %q: %+v", wantField, wantMsg, statusErr.Status().Details.Causes)
}

func TestValidatorAdmitsTypedPodLocal(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.LMCache.PodLocal.Server.L1Capacity = resource.MustParse("4Gi")
	if _, err := shippingValidator().ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("typed PodLocal rejected: %v", err)
	}
}

func TestValidatorRejectsTopologyLessLMCache(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.LMCache.Topology = ""
	requireInvalidWithCause(t, shippingValidator(), cb, "spec.lmCache.topology", "required")
}

func TestValidatorSGLangHiCacheCurrentContract(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		if _, err := shippingValidator().ValidateCreate(context.Background(), newHiCacheBackend()); err != nil {
			t.Fatalf("valid SGLangHiCache rejected: %v", err)
		}
	})

	t.Run("remote storage rejected", func(t *testing.T) {
		cb := newHiCacheBackend()
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			Redis: &cachev1alpha1.RedisRemoteStorageSpec{},
		}
		requireInvalidWithCause(t, shippingValidator(), cb, "spec.remoteStorage.provider", "does not accept")
	})

	t.Run("hiCache block rejected for LMCache", func(t *testing.T) {
		cb := validPodLocalMPBackend()
		cb.Spec.HiCache = &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"}
		requireInvalidWithCause(t, shippingValidator(), cb, "spec.hiCache", "only valid")
	})

	t.Run("invalid ratio rejected", func(t *testing.T) {
		cb := newHiCacheBackend()
		cb.Spec.HiCache.Ratio = "NaN"
		requireInvalidWithCause(t, shippingValidator(), cb, "spec.hiCache.ratio", "finite number")
	})
}

func TestValidatorRejectsInvalidKernelCheckAnnotation(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Annotations = map[string]string{enginebinding.AnnotationLMCacheKernelCheck: "strcit"}
	requireInvalidWithCause(t, shippingValidator(), cb,
		"metadata.annotations[inferencecache.io/lmcache-kernel-check]", "must be one of")
}

func TestValidatorRejectsEventsOnlyRemoteStorage(t *testing.T) {
	cb := newBackend()
	cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
	requireInvalidWithCause(t, shippingValidator(), cb, "spec.remoteStorage", "remove spec.remoteStorage")
}

func TestValidatorRejectsUnknownRuntime(t *testing.T) {
	cb := validPodLocalMPBackend()
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntime("UnknownRuntime")
	requireInvalidWithCause(t, shippingValidator(), cb, "spec.runtime", "no runtime adapter supports")
}
