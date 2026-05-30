package v1alpha1

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// newBackend returns a minimally-valid managed CacheBackend the test cases
// derive from. Tests deep-copy and mutate the relevant fields rather than
// re-declaring the whole spec each time.
func newBackend() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
		},
	}
}

func i32p(v int32) *int32 { return &v }

func TestDefaulter_StampsAllPhase1DefaultsWhenUnset(t *testing.T) {
	d := &CacheBackendDefaulter{}
	cb := newBackend()

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Replicas == nil || *cb.Spec.Replicas != defaultReplicas {
		t.Errorf("replicas = %v, want %d", cb.Spec.Replicas, defaultReplicas)
	}
	if cb.Spec.Integration == nil {
		t.Fatal("integration block not created")
	}
	if cb.Spec.Integration.LookupTimeoutMs == nil || *cb.Spec.Integration.LookupTimeoutMs != defaultLookupTimeoutMs {
		t.Errorf("lookupTimeoutMs = %v, want %d", cb.Spec.Integration.LookupTimeoutMs, defaultLookupTimeoutMs)
	}
	if cb.Spec.Integration.MinimumPrefixTokens == nil || *cb.Spec.Integration.MinimumPrefixTokens != defaultMinimumPrefixTokens {
		t.Errorf("minimumPrefixTokens = %v, want %d", cb.Spec.Integration.MinimumPrefixTokens, defaultMinimumPrefixTokens)
	}
}

func TestDefaulter_DoesNotClobberOperatorValues(t *testing.T) {
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(7)
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		LookupTimeoutMs:     i32p(123),
		MinimumPrefixTokens: i32p(999),
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if *cb.Spec.Replicas != 7 {
		t.Errorf("replicas clobbered: got %d, want 7", *cb.Spec.Replicas)
	}
	if *cb.Spec.Integration.LookupTimeoutMs != 123 {
		t.Errorf("lookupTimeoutMs clobbered: got %d, want 123", *cb.Spec.Integration.LookupTimeoutMs)
	}
	if *cb.Spec.Integration.MinimumPrefixTokens != 999 {
		t.Errorf("minimumPrefixTokens clobbered: got %d, want 999", *cb.Spec.Integration.MinimumPrefixTokens)
	}
}

func TestDefaulter_PreservesPartiallySetIntegration(t *testing.T) {
	// Operator pinned the timeout but left minimumPrefixTokens unset —
	// defaulter should fill in only the holes.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		LookupTimeoutMs: i32p(25),
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if *cb.Spec.Integration.LookupTimeoutMs != 25 {
		t.Errorf("operator timeout clobbered: %d", *cb.Spec.Integration.LookupTimeoutMs)
	}
	if cb.Spec.Integration.MinimumPrefixTokens == nil || *cb.Spec.Integration.MinimumPrefixTokens != defaultMinimumPrefixTokens {
		t.Errorf("minimumPrefixTokens not defaulted")
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("happy-path LMCache rejected: %v", err)
	}
}

func TestValidator_External_WithEndpointAdmitted(t *testing.T) {
	// External now flows through the runtime-adapter check; use a registry
	// that includes the External adapter (matching production cmd/controller
	// wiring) so the (vllm, External) pair is supported.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "team-a-cache.team-a.svc.cluster.local:9000"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External with endpoint rejected: %v", err)
	}
}

func TestValidator_External_WithoutEndpointRejected(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"spec.type=External requires spec.endpoint")
}

func TestValidator_External_BlankEndpointRejected(t *testing.T) {
	// Whitespace-only is the same as unset: a whitespace string is not a valid
	// network address, but a naïve != "" check would accept it.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "   "
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"spec.type=External requires spec.endpoint")
}

func TestValidator_EndpointOnManagedTypeRejected(t *testing.T) {
	// spec.endpoint is the External-passthrough field; setting it on a
	// managed type silently does nothing today (the reconciler overwrites
	// status.endpoint from the live Service it provisions), so admission
	// hard-rejects to make the misconfiguration visible at write time.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.Endpoint = "user-supplied.example:8080"
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"spec.endpoint is only valid when spec.type=External")
}

func TestValidator_EndpointOnManagedType_PreExistingUpdateAllowed(t *testing.T) {
	// v1alpha1 backward-compat: a CR that was admitted before
	// rejectEndpointOnNonExternal landed (e.g. LMCache with a stale
	// spec.endpoint) must remain editable for unrelated changes — the
	// new rule only rejects updates that *introduce* a new violation.
	// Without this property, every existing CR with the legacy
	// combination becomes un-updatable the moment an operator runs
	// `kubectl annotate`.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	old := newBackend()
	old.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	old.Spec.Endpoint = "legacy.example:9000"
	old.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}

	// Same offending fields; only an unrelated label change.
	newCB := old.DeepCopy()
	if newCB.Labels == nil {
		newCB.Labels = map[string]string{}
	}
	newCB.Labels["edited"] = "true"

	if _, err := v.ValidateUpdate(context.Background(), old, newCB); err != nil {
		t.Fatalf("unrelated update on pre-existing CR rejected: %v", err)
	}
}

func TestValidator_EndpointOnManagedType_UpdateThatWorsensIsRejected(t *testing.T) {
	// The diff-only semantics must not turn into a loophole: if the
	// update *changes* the bad value (different invalid endpoint), the
	// error key differs and the new violation is rejected. The locked
	// rule still bites when the operator actively edits the bad field.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	old := newBackend()
	old.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	old.Spec.Endpoint = "legacy.example:9000"
	old.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}

	newCB := old.DeepCopy()
	newCB.Spec.Endpoint = "freshly-typed.example:8080" // different bad value

	requireUpdateInvalidWithCause(t, v, old, newCB, "spec.endpoint",
		"spec.endpoint is only valid when spec.type=External")
}

func TestValidator_EndpointOnManagedType_UpdateThatIntroducesViolationIsRejected(t *testing.T) {
	// If the old CR was clean (no spec.endpoint on a managed type) and
	// the update adds one, the violation is freshly introduced — reject.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	old := newBackend()
	old.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	old.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}

	newCB := old.DeepCopy()
	newCB.Spec.Endpoint = "user-supplied.example:8080"

	requireUpdateInvalidWithCause(t, v, old, newCB, "spec.endpoint",
		"spec.endpoint is only valid when spec.type=External")
}

func TestValidator_EndpointOnManagedTypeBlankAdmitted(t *testing.T) {
	// Whitespace-only spec.endpoint on a managed type passes — same
	// leniency the External-required rule applies; the field is treated
	// as empty.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.Endpoint = "   "
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("LMCache with whitespace endpoint rejected: %v", err)
	}
}

func TestValidator_PersistentStorageOnMemoryOnlyBackendRejected(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeNIXL
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse("10Gi")},
	}
	requireInvalidWithCause(t, v, cb, "spec.storage.pvc",
		"memory-only and cannot declare spec.storage.pvc")
}

func TestValidator_PersistentStorageOnHierarchicalBackendAdmitted(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse("100Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("LMCache with PVC rejected: %v", err)
	}
}

func TestValidator_CrossNamespaceEndpointWithoutOptInRejected(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "shared-cache.team-b.svc.cluster.local:9000"
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"references namespace \"team-b\"")
}

func TestValidator_CrossNamespaceEndpointWithOptInAdmitted(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "shared-cache.team-b.svc.cluster.local"
	cb.Spec.AllowCrossNamespace = true
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External cross-namespace endpoint with opt-in rejected: %v", err)
	}
}

func TestValidator_SameNamespaceEndpointAdmitted(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "team-a-cache.team-a.svc.cluster.local:9000"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
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
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "cache.example.com:8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("external hostname rejected: %v", err)
	}
}

func TestValidator_ExternalEndpoint_LMSchemeAdmitted(t *testing.T) {
	// Operators who prefer to be explicit can pre-fix the endpoint with
	// the LMCache lm:// scheme; the adapter passes it through unchanged.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "lm://cache.example.com:8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
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
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "https://cache.example.com:443/api"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		`scheme "https" is not supported for spec.type=External`)
}

func TestValidator_ExternalEndpoint_PathRejected(t *testing.T) {
	// LMCache is a TCP-level protocol — paths/queries/fragments don't
	// belong on the wire and would be silently dropped at the engine
	// connector. Reject them at admission so the rejection message
	// surfaces the problem.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "cache.example.com:8200/path"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a bare host[:port]")
}

func TestValidator_ExternalEndpoint_LMSchemeOnlyRejected(t *testing.T) {
	// `lm://` alone is just the scheme — no host. Without this check
	// the CR admits, goes Ready=True, and the pod webhook injects
	// LMCACHE_REMOTE_URL=lm:// (the exact broken value the validation
	// exists to prevent).
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "lm://"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must include a non-empty host")
}

func TestValidator_ExternalEndpoint_PortOnlyRejected(t *testing.T) {
	// `:8200` is a port with no host — same broken-injection risk.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = ":8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must include a non-empty host")
}

func TestValidator_ExternalEndpoint_LMSchemePortOnlyRejected(t *testing.T) {
	// Scheme + port with no host.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "lm://:8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must include a non-empty host")
}

func TestValidator_ExternalEndpoint_IPv6Admitted(t *testing.T) {
	// IPv6 literals require brackets in host:port form; the validator
	// must accept them rather than mistaking the inner colons for
	// scheme/port separators.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "[2001:db8::1]:8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("IPv6 endpoint rejected: %v", err)
	}
}

func TestValidator_ExternalEndpoint_LMSchemeWithPathRejected(t *testing.T) {
	// Same concern as the bare-host case; the path-after-scheme variant
	// is just as broken.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "lm://cache.example.com:8200/path"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a bare host[:port]")
}

func TestValidator_AggregatesMultipleViolations(t *testing.T) {
	// Two independent violations on a single CR must both appear in the
	// rejection's status.details.causes, so kubectl prints them together.
	// Here: PVC on a memory-only backend (NIXL) plus a cross-namespace
	// endpoint without opt-in. Both rules should fire on the same spec.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeNIXL
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse("1Gi")},
	}
	cb.Spec.Endpoint = "x.other-ns.svc"

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
	// ValidateUpdate validates the *new* object — flipping spec.type to
	// External on update must fail just as it would on create.
	v := &CacheBackendValidator{}
	old := newBackend()
	newCB := newBackend()
	newCB.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	_, err := v.ValidateUpdate(context.Background(), old, newCB)
	if err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid on update, got %v", err)
	}
}

func TestValidator_Delete_AlwaysAllowed(t *testing.T) {
	// Even a structurally-broken backend must be deletable so operators can
	// clean up bad state — the validator must never block delete.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal // no endpoint
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

func (stubVLLMLMCacheAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return nil, nil, nil
}
func (stubVLLMLMCacheAdapter) InjectEngineConfig(*corev1.PodSpec, string, *cachev1alpha1.CacheBackend) error {
	return nil
}
func (stubVLLMLMCacheAdapter) InjectRouterConfig(*corev1.PodSpec, string, *cachev1alpha1.CacheBackend) error {
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

// stubExternalAdapter mirrors the real External adapter's Supports gate
// (vllm-only). Used by validator tests so admission of an External CR
// runs through the registry the same way production does — without
// dragging in the real adapter package and its enginewire dependency
// from a unit-test file.
type stubExternalAdapter struct{}

func (stubExternalAdapter) Supports(rt adapterruntime.RuntimeID, cb *cachev1alpha1.CacheBackend) bool {
	if cb == nil {
		return false
	}
	return rt == adapterruntime.RuntimeVLLM && cb.Spec.Type == cachev1alpha1.CacheBackendTypeExternal
}

func (stubExternalAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return nil, nil, nil
}
func (stubExternalAdapter) InjectEngineConfig(*corev1.PodSpec, string, *cachev1alpha1.CacheBackend) error {
	return nil
}
func (stubExternalAdapter) InjectRouterConfig(*corev1.PodSpec, string, *cachev1alpha1.CacheBackend) error {
	return nil
}
func (stubExternalAdapter) ObservationSidecar(*cachev1alpha1.CacheBackend, *corev1.Pod) (*corev1.Container, error) {
	return nil, nil
}
func (stubExternalAdapter) SupportedPairs() []adapterruntime.SupportedPair {
	return []adapterruntime.SupportedPair{{
		Runtime: adapterruntime.RuntimeVLLM,
		Backend: cachev1alpha1.CacheBackendTypeExternal,
	}}
}

// stubRegistry returns a Registry with the stub vLLM+LMCache adapter
// installed. Hermetic — tests don't depend on the in-tree
// adapterruntime.DefaultRegistry() composition, so they keep passing if a
// future adapter joins or leaves the default set. An External-specific
// runtime adapter is added by stubRegistryWithExternal so tests that
// exercise admission of External CRs run against both adapters the
// production wiring registers.
func stubRegistry() *adapterruntime.Registry {
	r := adapterruntime.NewRegistry()
	r.Register(stubVLLMLMCacheAdapter{})
	return r
}

// stubRegistryWithExternal mirrors the production cmd/controller wiring:
// the stub managed-LMCache adapter PLUS a stub External adapter that
// supports the same (vllm, External) pair the real External adapter
// supports. Tests of admission's External-with-supported-engine and
// External-with-unsupported-engine branches use this so they assert
// against the registry composition the running controller actually
// sees, rather than the bare stubRegistry that omits External.
func stubRegistryWithExternal() *adapterruntime.Registry {
	r := stubRegistry()
	r.Register(stubExternalAdapter{})
	return r
}

func TestValidator_RuntimeAdapter_VLLMPlusLMCacheAdmitted(t *testing.T) {
	// Happy path: an explicit (vLLM, LMCache) pair the stub registry
	// supports must be admitted. Pins the C7 check's positive side so a
	// regression doesn't silently start rejecting the only currently
	// shipping combination.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("vLLM+LMCache rejected: %v", err)
	}
}

func TestValidator_RuntimeAdapter_VLLMPlusMooncakeRejected(t *testing.T) {
	// Rejection path: a (vLLM, Mooncake) pair no installed adapter
	// supports must be rejected with a message that names BOTH sides of
	// the offending pair and lists the supported pairs so the user has
	// an actionable next step.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeMooncake
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}

	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("expected vLLM+Mooncake to be rejected")
	}
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok {
		t.Fatalf("expected *apierrors.StatusError, got %T: %v", err, err)
	}
	if statusErr.Status().Details == nil || len(statusErr.Status().Details.Causes) == 0 {
		t.Fatalf("Invalid status carried no causes: %v", statusErr.Status())
	}
	var match *metav1.StatusCause
	causes := statusErr.Status().Details.Causes
	for i := range causes {
		if causes[i].Field == "spec.integration.engine" {
			match = &causes[i]
			break
		}
	}
	if match == nil {
		t.Fatalf("no cause on spec.integration.engine; got: %+v", causes)
	}
	for _, want := range []string{"vllm", "Mooncake", "vllm/LMCache"} {
		if !strings.Contains(match.Message, want) {
			t.Errorf("rejection message missing %q; got %q", want, match.Message)
		}
	}
}

func TestValidator_RuntimeAdapter_UnknownEngineRejected(t *testing.T) {
	// An engine name no adapter handles must also be rejected — guards
	// against a typo (`engin: vllmm`) silently riding through admission
	// and only failing at reconcile.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllmm"}
	requireInvalidWithCause(t, v, cb, "spec.integration.engine",
		"engine=\"vllmm\"")
}

func TestValidator_RuntimeAdapter_EngineNormalisedToLowerCase(t *testing.T) {
	// The reconciler downcases the engine string before looking up an
	// adapter; admission must do the same so a CR that spells "VLLM" is
	// not admitted by one layer and rejected by the other.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "VLLM"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("VLLM (uppercase) + LMCache rejected: %v", err)
	}
}

func TestValidator_RuntimeAdapter_EmptyEngineDefaultsToVLLM(t *testing.T) {
	// Engine is optional on the CRD; the reconciler and pod webhook
	// default it to vLLM via adapterruntime.ResolveRuntimeID, so
	// admission must use the same defaulting or pairs like
	// "type: Mooncake with no engine" slip past the webhook and only
	// fail at reconcile (the exact gap C7 closes).
	//
	// With LMCache the default vLLM pair is supported → admit.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache, no Integration block
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("LMCache + defaulted vLLM engine rejected: %v", err)
	}
}

func TestValidator_RuntimeAdapter_EmptyEngineWithUnsupportedTypeRejected(t *testing.T) {
	// Counterpart to the previous test: the default vLLM resolution
	// must also fire C7 — type: Mooncake with no engine must be
	// rejected at admission, since the reconciler would otherwise try
	// vllm+Mooncake and fall back to unmanaged.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeMooncake
	requireInvalidWithCause(t, v, cb, "spec.integration.engine",
		"backend=\"Mooncake\"")
}

func TestValidator_RuntimeAdapter_EmptyTypeSkipsCheck(t *testing.T) {
	// Mirror edge case: an empty type must not trigger C7 either, for
	// the same "defer to required-field validation" reason.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = ""
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("empty type must not trigger C7; got %v", err)
	}
}

func TestValidator_RuntimeAdapter_ExternalWithSupportedEngineAdmitted(t *testing.T) {
	// External flows through the adapter registry the same way managed
	// types do (the pod webhook needs to find an adapter to wire engine
	// pods to the operator-supplied endpoint). vLLM is the engine the
	// External adapter supports today, so the pair must admit.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "team-a-cache.team-a.svc.cluster.local:9000"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External with engine=vllm rejected by C7: %v", err)
	}
}

func TestValidator_RuntimeAdapter_ExternalWithUnsupportedEngineRejected(t *testing.T) {
	// External + sglang is admittable on shape (endpoint present, type set)
	// but no adapter in the registry handles that pair, so the pod webhook
	// would fail-open and never inject — the engine boots un-wired to the
	// external cache. Reject at admission with a useful error instead of
	// letting the silent miss happen.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "team-a-cache.team-a.svc.cluster.local:9000"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "sglang"}
	requireInvalidWithCause(t, v, cb, "spec.integration.engine",
		"backend=\"External\"")
}

func TestValidator_RuntimeAdapter_UpdateAlsoChecks(t *testing.T) {
	// ValidateUpdate runs the same check as ValidateCreate — a kubectl
	// edit that flips engine to something the registry doesn't support
	// must be rejected just as it would on create.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	old := newBackend()
	old.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	newCB := old.DeepCopy()
	newCB.Spec.Type = cachev1alpha1.CacheBackendTypeMooncake

	_, err := v.ValidateUpdate(context.Background(), old, newCB)
	if err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid on update with unsupported pair, got %v", err)
	}
}

func TestValidator_RuntimeAdapter_DeleteSkipsCheck(t *testing.T) {
	// Deletion of a CR that would now be rejected (e.g. registry shrank
	// since admission) must still be allowed so operators can clean up.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeMooncake
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateDelete(context.Background(), cb); err != nil {
		t.Fatalf("ValidateDelete rejected unsupported pair: %v", err)
	}
}

func TestValidator_RuntimeAdapter_NilRegistry_AdmitsExternal(t *testing.T) {
	// The nil-Registry fallback mirrors production cmd/controller wiring
	// — DefaultRegistry PLUS the External adapter — so a bare
	// `CacheBackendValidator{}` admits the same set the running
	// controller does. Without the explicit External registration in
	// the fallback, this CR would be rejected for "no adapter supports
	// (vllm, External)" even though the production webhook wires it
	// just fine.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "ext.example.com:8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("nil-Registry fallback rejected vLLM+External: %v", err)
	}
}

func TestValidator_RuntimeAdapter_NilRegistryFallsBackToDefault(t *testing.T) {
	// A zero-value validator (Registry nil) must still run the C7 check
	// against [adapterruntime.DefaultRegistry] — the production safety
	// net for cmd/controller wiring drift. The default registry ships
	// the vLLM+LMCache adapter, so the happy pair admits.
	v := &CacheBackendValidator{}
	cb := newBackend() // type=LMCache
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("nil-registry fallback rejected vLLM+LMCache: %v", err)
	}
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

// Sanity check on the package-level wiring: SetupCacheBackendWebhookWithManager
// is exercised by manager start-up; the runtime.Object interface is the only
// thing we can sanity-check here without a controller manager.
var _ runtime.Object = (*cachev1alpha1.CacheBackend)(nil)
