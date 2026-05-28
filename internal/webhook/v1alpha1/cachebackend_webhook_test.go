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

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
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
func bp(v bool) *bool     { return &v }

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
	if cb.Spec.Integration.FailOpen == nil || *cb.Spec.Integration.FailOpen != defaultFailOpen {
		t.Errorf("failOpen = %v, want %v", cb.Spec.Integration.FailOpen, defaultFailOpen)
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
		FailOpen:            bp(false),
		LookupTimeoutMs:     i32p(123),
		MinimumPrefixTokens: i32p(999),
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if *cb.Spec.Replicas != 7 {
		t.Errorf("replicas clobbered: got %d, want 7", *cb.Spec.Replicas)
	}
	if *cb.Spec.Integration.FailOpen {
		t.Errorf("failOpen clobbered: got true, want false")
	}
	if *cb.Spec.Integration.LookupTimeoutMs != 123 {
		t.Errorf("lookupTimeoutMs clobbered: got %d, want 123", *cb.Spec.Integration.LookupTimeoutMs)
	}
	if *cb.Spec.Integration.MinimumPrefixTokens != 999 {
		t.Errorf("minimumPrefixTokens clobbered: got %d, want 999", *cb.Spec.Integration.MinimumPrefixTokens)
	}
}

func TestDefaulter_PreservesPartiallySetIntegration(t *testing.T) {
	// Operator pinned the timeout but left failOpen and minimumPrefixTokens
	// unset — defaulter should fill in only the holes.
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
	if cb.Spec.Integration.FailOpen == nil || *cb.Spec.Integration.FailOpen != defaultFailOpen {
		t.Errorf("failOpen not defaulted")
	}
	if cb.Spec.Integration.MinimumPrefixTokens == nil || *cb.Spec.Integration.MinimumPrefixTokens != defaultMinimumPrefixTokens {
		t.Errorf("minimumPrefixTokens not defaulted")
	}
}

func TestDefaulter_RejectsWrongType(t *testing.T) {
	// A misregistered webhook would hand the handler a different runtime.Object;
	// it should surface as a typed BadRequest, not a panic.
	d := &CacheBackendDefaulter{}
	err := d.Default(context.Background(), &cachev1alpha1.CacheBackendList{})
	if !apierrors.IsBadRequest(err) {
		t.Fatalf("expected BadRequest, got %v", err)
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

func TestValidator_HappyPath_LMCacheAdmitted(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("happy-path LMCache rejected: %v", err)
	}
}

func TestValidator_External_WithEndpointAdmitted(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "team-a-cache.team-a.svc.cluster.local:9000"
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "shared-cache.team-b.svc.cluster.local"
	cb.Spec.AllowCrossNamespace = true
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External cross-namespace endpoint with opt-in rejected: %v", err)
	}
}

func TestValidator_SameNamespaceEndpointAdmitted(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "team-a-cache.team-a.svc.cluster.local:9000"
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("same-namespace endpoint rejected: %v", err)
	}
}

func TestValidator_ExternalHostnamePassesThrough(t *testing.T) {
	// External hostnames are not in-cluster Service DNS — the cross-namespace
	// rule has no namespace to compare against and must let them through.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "https://cache.example.com:443/api"
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("external hostname rejected: %v", err)
	}
}

func TestValidator_AggregatesMultipleViolations(t *testing.T) {
	// External backend with no endpoint AND a PVC on a memory-only-by-spec
	// type — both rules should fire and both causes should land in the same
	// rejection. (We use NIXL here so the storage rule applies; the missing
	// endpoint rule fires regardless because spec.type=External.)
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeNIXL
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse("1Gi")},
	}
	// Add a second violation: cross-namespace endpoint without opt-in.
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

func TestValidator_RejectsWrongType(t *testing.T) {
	v := &CacheBackendValidator{}
	_, err := v.ValidateCreate(context.Background(), &cachev1alpha1.CacheBackendList{})
	if !apierrors.IsBadRequest(err) {
		t.Fatalf("expected BadRequest, got %v", err)
	}
	_, err = v.ValidateUpdate(context.Background(), nil, &cachev1alpha1.CacheBackendList{})
	if !apierrors.IsBadRequest(err) {
		t.Fatalf("expected BadRequest, got %v", err)
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

func TestServiceDNSNamespace(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		wantNS   string
		wantOK   bool
	}{
		{"bare svc DNS", "cache.team-a.svc", "team-a", true},
		{"cluster.local svc DNS", "cache.team-b.svc.cluster.local", "team-b", true},
		{"alt cluster suffix", "cache.team-c.svc.private", "team-c", true},
		{"svc DNS with port", "cache.team-d.svc.cluster.local:9000", "team-d", true},
		{"https scheme + path", "https://cache.team-e.svc.cluster.local/api", "team-e", true},
		{"grpc scheme", "grpc://cache.team-f.svc:9090", "team-f", true},
		{"external hostname", "cache.example.com", "", false},
		{"external hostname with port", "cache.example.com:443", "", false},
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
