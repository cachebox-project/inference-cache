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

func TestDefaulter_StampsReplicasDefaultWhenUnset(t *testing.T) {
	d := &CacheBackendDefaulter{}
	cb := newBackend()

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Replicas == nil || *cb.Spec.Replicas != defaultReplicas {
		t.Errorf("replicas = %v, want %d", cb.Spec.Replicas, defaultReplicas)
	}
	// The defaulter no longer materialises an integration block: lookup
	// tuning lives on CachePolicy, and failOpen's effective default is applied
	// at read time by IntegrationFailOpen (the CRD +kubebuilder:default only
	// persists when the parent integration object is present). An unset
	// integration spec is therefore left nil.
	if cb.Spec.Integration != nil {
		t.Errorf("integration block = %+v, want nil", cb.Spec.Integration)
	}
}

func TestDefaulter_DoesNotClobberOperatorValues(t *testing.T) {
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(7)

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if *cb.Spec.Replicas != 7 {
		t.Errorf("replicas clobbered: got %d, want 7", *cb.Spec.Replicas)
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
func (stubVLLMLMCacheAdapter) ReservedArgs() []string {
	return []string{"--kv-transfer-config"}
}
func (stubVLLMLMCacheAdapter) ReservedEnv() []string {
	return []string{"VLLM_USE_V1", "LMCACHE_REMOTE_URL", "INFERENCECACHE_FAIL_OPEN"}
}
func (stubVLLMLMCacheAdapter) EngineContainerName() string { return "vllm" }

// stubRegistry returns a Registry with only the stub vLLM+LMCache adapter
// installed. Hermetic — tests don't depend on the in-tree
// adapterruntime.DefaultRegistry() composition, so they keep passing if a
// future adapter joins or leaves the default set.
func stubRegistry() *adapterruntime.Registry {
	r := adapterruntime.NewRegistry()
	r.Register(stubVLLMLMCacheAdapter{})
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

func TestValidator_RuntimeAdapter_ExternalSkipsCheck(t *testing.T) {
	// External backends are pre-existing services the controller mirrors
	// to status — they never reach the adapter registry at reconcile, so
	// admission must not reject them for "no adapter". The endpoint rule
	// (and the cross-namespace rule) still apply.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "team-a-cache.team-a.svc.cluster.local:9000"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External with engine=vllm rejected by C7: %v", err)
	}
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

// withVLLMOverrides returns a fresh stub-LMCache backend whose integration
// declares vLLM and carries the supplied EngineInjectionOverrides — the
// admission-test backing for the engine-overrides rule.
func withVLLMOverrides(o cachev1alpha1.EngineInjectionOverrides) *cachev1alpha1.CacheBackend {
	cb := newBackend()
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Engine:          "vllm",
		EngineOverrides: &o,
	}
	return cb
}

func TestValidator_EngineOverrides_NoOverrideAdmitted(t *testing.T) {
	// Sanity baseline: a CacheBackend whose integration is set but carries
	// no engineOverrides block must admit unchanged. Locked decision #7
	// (byte-identical default) hinges on this.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("no-override CR rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_SuppressReservedArgRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	requireInvalidWithCause(t, v,
		cb,
		"spec.integration.engineOverrides.suppressArgs[0]",
		"--kv-transfer-config")
	// And the rejection MUST name the offending adapter so the operator
	// can trace the contract back.
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.suppressArgs[0]", "\"vllm\"")
}

func TestValidator_EngineOverrides_OverrideReservedArgRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	// Two forms: bare flag and equals form. Both must trip the rule, since
	// both express the same leading flag token.
	for _, form := range []string{"--kv-transfer-config", "--kv-transfer-config=alt"} {
		cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
			Args: []string{form},
		})
		requireInvalidWithCause(t, v, cb,
			"spec.integration.engineOverrides.args[0]",
			"--kv-transfer-config")
	}
}

func TestValidator_EngineOverrides_SuppressReservedEnvRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressEnv: []string{"VLLM_USE_V1"},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.suppressEnv[0]",
		"VLLM_USE_V1")
}

func TestValidator_EngineOverrides_OverrideReservedEnvRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "INFERENCECACHE_FAIL_OPEN", Value: "false"}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"INFERENCECACHE_FAIL_OPEN")
}

func TestValidator_EngineOverrides_NonReservedAdmitted(t *testing.T) {
	// Positive case: a non-reserved arg + env + suppression combination
	// must pass admission. This is the CPU-vLLM use case — the operator
	// suppresses a flag the adapter wouldn't inject anyway (no-op) and
	// adds a perf knob. We pin the happy path here so a future tightening
	// of the rule doesn't accidentally reject legitimate overrides.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Args:         []string{"--max-model-len", "8192"},
		SuppressArgs: []string{"--enforce-eager"},
		Env: []corev1.EnvVar{
			{Name: "LMCACHE_CHUNK_SIZE", Value: "512"},
		},
		SuppressEnv: []string{"VLLM_LOG_LEVEL"},
	})
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("non-reserved overrides rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_PositionalArgIgnored(t *testing.T) {
	// Positionals (no leading "-") cannot overlap a reserved flag name
	// because the merge classifies them differently. Admission must treat
	// them the same way and not surface a spurious rejection — the engine
	// would happily accept the positional, so admission must too.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Args: []string{"some-positional"},
	})
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("positional arg rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_RejectsEmptyEnvName(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "", Value: "x"}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"must declare a Name")
}

func TestValidator_EngineOverrides_RejectsInvalidEnvName(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		// "=" is forbidden in K8s env var names.
		Env: []corev1.EnvVar{{Name: "FOO=BAR", Value: "x"}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"invalid env var name")
}

func TestValidator_EngineOverrides_RejectsValueAndValueFrom(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name:  "BOTH",
			Value: "literal",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0]",
		"value OR valueFrom, not both")
}

func TestValidator_EngineOverrides_RejectsEmptyValueFrom(t *testing.T) {
	// valueFrom with zero sources fails K8s Pod validation; admission
	// must catch it before it reaches engine pods.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name:      "BAD",
			ValueFrom: &corev1.EnvVarSource{},
		}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].valueFrom",
		"exactly one source")
}

func TestValidator_EngineOverrides_RejectsMultipleValueFromSources(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name: "BAD",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
					Key:                  "k",
				},
			},
		}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].valueFrom",
		"multiple set")
}

func TestEnvVarSourceCount_CountsAllNonNilPointerFields(t *testing.T) {
	// Pin the reflection-based count's contract: it walks pointer fields
	// on EnvVarSource and counts non-nil ones. This is the future-proof
	// path for new source kinds upstream adds (e.g. fileKeyRef) — the
	// generated CRD already embeds them from the upstream OpenAPI, and the
	// validator's one-of check needs to stay aligned without a code
	// change for each new field.
	cases := []struct {
		name string
		src  *corev1.EnvVarSource
		want int
	}{
		{"nil", nil, 0},
		{"empty", &corev1.EnvVarSource{}, 0},
		{
			"one source",
			&corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
			1,
		},
		{
			"two sources",
			&corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
					Key:                  "k",
				},
			},
			2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := envVarSourceCount(tc.src)
			if got != tc.want {
				t.Fatalf("envVarSourceCount = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestValidator_EngineOverrides_ValueFromAloneAdmitted(t *testing.T) {
	// Positive case: a ValueFrom-only entry (no Value) is a valid K8s env
	// shape and must pass.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		}},
	})
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("ValueFrom-only env rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_ExternalBackendSkipsCheck(t *testing.T) {
	// External backends never reach an adapter — the reconciler routes
	// them through reconcileExternal — so engineOverrides on an External
	// CR is structurally meaningless. The check is bypassed; the
	// structural rules (endpoint required) still fire.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "shared.team-a.svc.cluster.local:9000"
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External CR with engineOverrides rejected: %v", err)
	}
}

// Sanity check on the package-level wiring: SetupCacheBackendWebhookWithManager
// is exercised by manager start-up; the runtime.Object interface is the only
// thing we can sanity-check here without a controller manager.
var _ runtime.Object = (*cachev1alpha1.CacheBackend)(nil)
