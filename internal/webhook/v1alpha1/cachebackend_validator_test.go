// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"strings"
	"testing"
)

// newBackend returns a minimally-valid managed CacheBackend the test cases
// derive from. Tests deep-copy and mutate the relevant fields rather than
// re-declaring the whole spec each time.
func newBackend() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
		},
	}
}

func managedLMCacheServer(cb *cachev1alpha1.CacheBackend) *cachev1alpha1.LMCacheServerRemoteStorageSpec {
	if cb.Spec.RemoteStorage == nil {
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{}
	}
	cb.Spec.RemoteStorage.Provider = cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer
	cb.Spec.RemoteStorage.Ownership = cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged
	if cb.Spec.RemoteStorage.LMCacheServer == nil {
		cb.Spec.RemoteStorage.LMCacheServer = &cachev1alpha1.LMCacheServerRemoteStorageSpec{}
	}
	return cb.Spec.RemoteStorage.LMCacheServer
}

func i32p(v int32) *int32 { return &v }

func defaultShippingRegistry() *adapterruntime.Registry {
	registry := adapterruntime.NewRegistry()
	registry.Register(builtinruntime.NewVLLMLMCacheAdapter(builtinruntime.SubscriberConfig{}))
	registry.Register(builtinruntime.NewSGLangLMCacheAdapter(builtinruntime.SubscriberConfig{}))
	registry.Register(builtinruntime.NewSGLangHiCacheAdapter(builtinruntime.SubscriberConfig{}))
	return registry
}

func shippingValidator() *CacheBackendValidator {
	return &CacheBackendValidator{Registry: defaultShippingRegistry()}
}

func newHiCacheBackend() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "hicache", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeSGLangHiCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Mode: cachev1alpha1.CacheBackendIntegrationModeOffload,
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "sglang"},
			},
			HiCache: &cachev1alpha1.SGLangHiCacheSpec{
				Ratio:        "2.0",
				WritePolicy:  cachev1alpha1.SGLangHiCacheWriteThroughSelective,
				IOBackend:    cachev1alpha1.SGLangHiCacheIOKernel,
				MemoryLayout: cachev1alpha1.SGLangHiCacheMemoryPageFirst,
			},
			Observation: &cachev1alpha1.CacheBackendObservationSpec{ModelID: "model-a"},
		},
	}
}

// requireInvalidWithCause runs v against cb and asserts the response is an
// aggregated Invalid status whose causes contain the substring wantMsg on
// field wantField. Centralising the assertion keeps the per-rule tests one
// line and the error-shape contract pinned in one place.
func requireInvalidWithCause(t *testing.T, v *CacheBackendValidator, cb *cachev1alpha1.CacheBackend, wantField, wantMsg string) {
	t.Helper()
	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok {
		t.Fatalf("expected *apierrors.StatusError, got %T: %v", err, err)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid status, got %v", statusErr.Status())
	}
	if statusErr.Status().Details == nil {
		t.Fatalf("Invalid status has no details: %v", statusErr.Status())
	}
	for _, c := range statusErr.Status().Details.Causes {
		if c.Field == wantField && strings.Contains(c.Message, wantMsg) {
			return
		}
	}
	t.Fatalf("no cause on field %q containing %q; got causes: %+v",
		wantField, wantMsg, statusErr.Status().Details.Causes)
}

// requireUpdateInvalidWithCause is the ValidateUpdate-equivalent of
// requireInvalidWithCause. Asserts the (old, new) pair fails validation
// with an Invalid status carrying the named field + message substring.
func requireUpdateInvalidWithCause(t *testing.T, v *CacheBackendValidator, oldCB, newCB *cachev1alpha1.CacheBackend, wantField, wantMsg string) {
	t.Helper()
	_, err := v.ValidateUpdate(context.Background(), oldCB, newCB)
	if err == nil {
		t.Fatalf("expected validation error on update, got nil")
	}
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok {
		t.Fatalf("expected *apierrors.StatusError, got %T: %v", err, err)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid status, got %v", statusErr.Status())
	}
	if statusErr.Status().Details == nil {
		t.Fatalf("Invalid status has no details: %v", statusErr.Status())
	}
	for _, c := range statusErr.Status().Details.Causes {
		if c.Field == wantField && strings.Contains(c.Message, wantMsg) {
			return
		}
	}
	t.Fatalf("no cause on field %q containing %q; got causes: %+v",
		wantField, wantMsg, statusErr.Status().Details.Causes)
}

func TestValidator_HappyPath_LMCacheAdmitted(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("happy-path LMCache rejected: %v", err)
	}
}

func mooncakeBackendWithEngineHostNetwork(optIn bool) *cachev1alpha1.CacheBackend {
	cb := newBackend()
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Role:              cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
		EngineHostNetwork: optIn,
	}
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
	}
	return cb
}

func setCanonicalExternalStorage(cb *cachev1alpha1.CacheBackend, endpoint string) {
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  endpoint,
	}
}

func setCanonicalMooncakeStorage(cb *cachev1alpha1.CacheBackend) {
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
	}
}

func TestValidator_WarningTextStaysConcise(t *testing.T) {
	// The Kubernetes API conventions ask for warnings within a concise budget so
	// clients render them reliably. A warning that gets truncated — or dropped — is
	// exactly the silent failure this warning exists to prevent, so guard the budget
	// here rather than trusting review to catch a future edit that pads it out.
	for _, w := range []string{mooncakeEngineHostNetworkWarning} {
		if got := len(w); got > maxWarningLen {
			t.Fatalf("warning is %d chars, want <= %d — put the detail in the docs, not the warning:\n%q",
				got, maxWarningLen, w)
		}
	}
}

func sglangLMCacheBackend() *cachev1alpha1.CacheBackend {
	cb := newBackend() // Type=LMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	return cb
}

func TestValidator_SameNamespaceEndpointAdmitted(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "team-a-cache.team-a.svc.cluster.local:9000")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("same-namespace endpoint rejected: %v", err)
	}
}

func TestValidator_ExternalHostnamePassesThrough(t *testing.T) {
	// External hostnames are not in-cluster Service DNS — the cross-namespace
	// rule has no namespace to compare against and must let them through.
	// Use a bare host:port (the canonical External shape; the LMCache
	// adapter prepends the lm:// scheme on injection).
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "cache.example.com:8200")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("external hostname rejected: %v", err)
	}
}

func TestValidator_AggregatesMultipleViolations(t *testing.T) {
	// Two independent violations on a single CR must both appear in the
	// rejection's status.details.causes, so kubectl prints them together.
	// Here: non-positive host memory plus scale-to-zero with autoscaling and no
	// explicit minReplicas. Both rules should fire on
	// the same spec.
	v := shippingValidator()
	cb := newBackend()
	zeroQuantity := resource.MustParse("0")
	cb.Spec.LMCache = &cachev1alpha1.LMCacheEngineSpec{
		HostMemory: &cachev1alpha1.CacheBackendHostMemorySpec{Capacity: &zeroQuantity},
	}
	zero := int32(0)
	cb.Spec.Replicas = &zero
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}

	_, err := v.ValidateCreate(context.Background(), cb)
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok {
		t.Fatalf("expected *apierrors.StatusError, got %T: %v", err, err)
	}
	if statusErr.Status().Details == nil || len(statusErr.Status().Details.Causes) < 2 {
		t.Fatalf("expected >=2 causes, got %+v", statusErr.Status().Details)
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

// stubVLLMLMCacheAdapter is a hermetic stand-in for the production
// vLLM+LMCache adapter that exercises the validator without dragging in the
// reference-stack adapter wiring. It supports exactly the vLLM/LMCache pair
// and exposes it via PairLister so the registry surfaces the same option to
// admission error messages.
type stubVLLMLMCacheAdapter struct{}

func (stubVLLMLMCacheAdapter) Supports(rt adapterruntime.RuntimeID, cb *cachev1alpha1.CacheBackend) bool {
	if cb == nil {
		return false
	}
	return rt == adapterruntime.RuntimeVLLM && cb.Spec.Type == cachev1alpha1.CacheBackendTypeLMCache
}

func (stubVLLMLMCacheAdapter) SupportsBinding(binding *backendadapter.Binding) bool {
	return binding == nil ||
		binding.Protocol == backendadapter.ProtocolLMCache ||
		binding.Protocol == backendadapter.ProtocolMooncakeStore
}

func (stubVLLMLMCacheAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return nil, nil, nil
}
func (stubVLLMLMCacheAdapter) InjectEngineConfig(*corev1.PodSpec, *backendadapter.Binding, *cachev1alpha1.CacheBackend) error {
	return nil
}
func (stubVLLMLMCacheAdapter) InjectRouterConfig(*corev1.PodSpec, *backendadapter.Binding, *cachev1alpha1.CacheBackend) error {
	return nil
}
func (stubVLLMLMCacheAdapter) ObservationSidecar(*cachev1alpha1.CacheBackend, *corev1.Pod) (*corev1.Container, error) {
	return nil, nil
}
func (stubVLLMLMCacheAdapter) SupportedPairs() []adapterruntime.SupportedPair {
	return []adapterruntime.SupportedPair{{
		Runtime: adapterruntime.RuntimeVLLM,
		Backend: cachev1alpha1.CacheBackendTypeLMCache,
	}}
}
func (stubVLLMLMCacheAdapter) ReservedArgs() []string {
	return []string{"--kv-transfer-config"}
}
func (stubVLLMLMCacheAdapter) ReservedEnv() []string {
	return []string{"VLLM_USE_V1", "LMCACHE_REMOTE_URL", "INFERENCECACHE_FAIL_OPEN", "PYTHONHASHSEED"}
}
func (stubVLLMLMCacheAdapter) EngineContainerName() string { return "vllm" }

// stubRegistry returns a Registry with the stub vLLM+LMCache adapter
// installed. Hermetic — tests don't depend on the in-tree
// builtin adapter composition, so they keep passing if a future adapter joins
// or leaves the default set. External ownership uses this same runtime adapter;
// it is a remote-storage binding property, not a separate cache type.
func stubRegistry() *adapterruntime.Registry {
	r := adapterruntime.NewRegistry()
	r.Register(builtinruntime.NewVLLMLMCacheAdapter(builtinruntime.SubscriberConfig{}))
	return r
}

// stubRegistryWithExternal uses the same LMCache runtime adapter because
// external ownership is a remote-storage property, not a cache type.
func stubRegistryWithExternal() *adapterruntime.Registry {
	return stubRegistry()
}

func TestSetupCacheBackendWebhookRequiresRegistry(t *testing.T) {
	if err := SetupCacheBackendWebhookWithManager(nil, nil); err == nil || !strings.Contains(err.Error(), "registry is required") {
		t.Fatalf("SetupCacheBackendWebhookWithManager error = %v, want missing-registry error", err)
	}
}

// withVLLMOverrides returns a fresh stub-LMCache backend whose integration
// declares vLLM and carries the supplied EngineInjectionOverrides — the
// admission-test backing for the engine-overrides rule.
func withVLLMOverrides(o cachev1alpha1.EngineInjectionOverrides) *cachev1alpha1.CacheBackend {
	cb := newBackend()
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		EngineOverrides: &o,
	}
	return cb
}

// withSGLangOverrides builds a (sglang, LMCache) CacheBackend carrying the
// given engineOverrides. The SGLang override tests use the real
// defaultShippingRegistry (not stubRegistry) so the SGLang adapter is the one
// the reserved-args/env check consults — proving the check is keyed on the
// SELECTED adapter, not a hardcoded vLLM list.
func withSGLangOverrides(o cachev1alpha1.EngineInjectionOverrides) *cachev1alpha1.CacheBackend {
	cb := newBackend() // type=LMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		EngineOverrides: &o,
	}
	return cb
}

func TestValidator_VLLMRoleReadOnlyStillAdmitted(t *testing.T) {
	// The SGLang role rule must not bleed onto vLLM: vLLM maps ReadOnly onto
	// its connector (kv_consumer), so a (vllm, LMCache) backend with ReadOnly
	// must still admit.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := newBackend()
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Role: cachev1alpha1.CacheBackendIntegrationRoleReadOnly,
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("vllm role=ReadOnly rejected by the sglang role rule: %v", err)
	}
}

// eventsOnlyIntegration returns an integration spec wired for the events-only
// (tier-1 routing) mode. Centralised so the events-only tests set the mode the
// one way the implementation reads it (via spec.integration.mode).
func eventsOnlyIntegration() *cachev1alpha1.CacheBackendIntegrationSpec {
	return &cachev1alpha1.CacheBackendIntegrationSpec{
		Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
	}
}

// Sanity check on the package-level wiring: SetupCacheBackendWebhookWithManager
// is exercised by manager start-up; the runtime.Object interface is the only
// thing we can sanity-check here without a controller manager.
var _ runtime.Object = (*cachev1alpha1.CacheBackend)(nil)
