package v1alpha1

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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

func TestDefaulter_MaterialisesIntegrationForFirstEventTimeout(t *testing.T) {
	// The webhook materialises spec.integration solely to persist
	// firstEventTimeout: the CRD-schema default for firstEventTimeout only
	// applies when spec.integration is present in the submitted object, so the
	// common CR that omits integration entirely relies on the webhook stamping
	// it here.
	//
	// Other Phase-1 literal defaults (spec.replicas=1, spec.type=LMCache,
	// spec.deploymentKind=Deployment, spec.integration.engine=vllm,
	// spec.integration.role=ReadWrite) ride on `+kubebuilder:default=` markers
	// stamped by the apiserver before this handler runs — they are NOT this
	// defaulter's job, and a unit-level call to Default() on a raw struct
	// will not see them. The persisted-CR shape is asserted end-to-end in the
	// envtest below (TestDefaulter_MinimumViableYAMLGetsFullyDefaulted).
	d := &CacheBackendDefaulter{}
	cb := newBackend()

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Integration == nil {
		t.Fatal("integration block not materialised")
	}
	if cb.Spec.Integration.FirstEventTimeout == nil || cb.Spec.Integration.FirstEventTimeout.Duration != defaultFirstEventTimeout {
		t.Errorf("firstEventTimeout = %v, want %s", cb.Spec.Integration.FirstEventTimeout, defaultFirstEventTimeout)
	}
}

func TestDefaulter_DoesNotClobberOperatorValues(t *testing.T) {
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(7)
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		FirstEventTimeout: &metav1.Duration{Duration: 90 * time.Second},
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	// Replicas now defaults via a CRD-schema marker (not the webhook), but the
	// non-clobber contract still holds: an operator-set value must survive the
	// webhook regardless of which layer applied the default.
	if *cb.Spec.Replicas != 7 {
		t.Errorf("replicas clobbered: got %d, want 7", *cb.Spec.Replicas)
	}
	if cb.Spec.Integration.FirstEventTimeout == nil || cb.Spec.Integration.FirstEventTimeout.Duration != 90*time.Second {
		t.Errorf("firstEventTimeout clobbered: got %v, want 90s", cb.Spec.Integration.FirstEventTimeout)
	}
}

func TestDefaulter_PreservesPartiallySetIntegration(t *testing.T) {
	// Operator pinned firstEventTimeout on an otherwise-empty integration
	// block — the defaulter should leave the pinned value alone (and there are
	// no other integration fields left for it to fill in).
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		FirstEventTimeout: &metav1.Duration{Duration: 30 * time.Second},
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Integration.FirstEventTimeout == nil || cb.Spec.Integration.FirstEventTimeout.Duration != 30*time.Second {
		t.Errorf("operator firstEventTimeout clobbered: got %v, want 30s", cb.Spec.Integration.FirstEventTimeout)
	}
}

func TestDefaulter_AutoscalingMinReplicasComputedFromReplicas(t *testing.T) {
	// When the operator opts into autoscaling without pinning the floor, the
	// defaulter computes minReplicas from spec.replicas so the HPA's lower
	// bound follows the baseline declaration rather than a hard-coded
	// constant. (spec.replicas itself is stamped by the apiserver from its
	// `+kubebuilder:default=1` marker; the unit test seeds it explicitly so
	// the assertion does not depend on the marker firing.)
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(3)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling.MinReplicas == nil || *cb.Spec.Autoscaling.MinReplicas != 3 {
		t.Errorf("autoscaling.minReplicas = %v, want 3 (= spec.replicas)", cb.Spec.Autoscaling.MinReplicas)
	}
}

func TestDefaulter_AutoscalingMinReplicasNotClobbered(t *testing.T) {
	// An operator-set minReplicas survives the defaulter. The non-clobber
	// contract extends to every default this handler stamps.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(3)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MinReplicas: i32p(2),
		MaxReplicas: 10,
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if *cb.Spec.Autoscaling.MinReplicas != 2 {
		t.Errorf("autoscaling.minReplicas clobbered: got %d, want 2", *cb.Spec.Autoscaling.MinReplicas)
	}
}

func TestDefaulter_AutoscalingMinReplicasSkippedWhenAutoscalingOff(t *testing.T) {
	// No autoscaling = no defaulting. The reconciler's autoscalingFloor
	// helper handles the nil-Autoscaling case at runtime; the defaulter
	// must not synthesise an autoscaling object operators did not request.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(3)

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling != nil {
		t.Errorf("autoscaling materialised unexpectedly: %+v", cb.Spec.Autoscaling)
	}
}

func TestDefaulter_AutoscalingMinReplicasSkippedWhenReplicasNil(t *testing.T) {
	// Defence-in-depth: when a test calls Default() on a raw struct without
	// the apiserver in the loop, spec.replicas may still be nil (the schema
	// default did not get a chance to fire). The defaulter must leave
	// minReplicas alone in that case rather than dereference a nil pointer.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = nil
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling.MinReplicas != nil {
		t.Errorf("autoscaling.minReplicas should stay nil when spec.replicas is nil; got %v", *cb.Spec.Autoscaling.MinReplicas)
	}
}

func TestDefaulter_AutoscalingMinReplicasSkippedWhenReplicasZero(t *testing.T) {
	// spec.replicas permits 0 (scale-to-zero is a valid operator choice),
	// but autoscaling.minReplicas carries `+kubebuilder:validation:Minimum=1`
	// in the CRD schema. If the defaulter copied a 0 spec.replicas into
	// minReplicas the apiserver would then reject the persisted object
	// against the schema — a webhook-introduced validation failure on a CR
	// the operator did NOT explicitly misconfigure. Refusing to default in
	// that case leaves the field unset so the operator's combination of
	// `replicas: 0` + opted-in autoscaling surfaces as a missing-required
	// field violation against autoscaling itself, which is the actual
	// problem.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling.MinReplicas != nil {
		t.Errorf("autoscaling.minReplicas should stay nil when spec.replicas is 0 (would violate schema Minimum=1); got %v", *cb.Spec.Autoscaling.MinReplicas)
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

func TestValidator_NonPositivePVCSizeRejected(t *testing.T) {
	// size drives a real PVC request, so a non-positive value must be a
	// field-scoped rejection at admission, not a late child-PVC failure.
	for _, sz := range []string{"0", "0Gi"} {
		v := &CacheBackendValidator{}
		cb := newBackend()
		cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
		cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
			PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse(sz)},
		}
		requireInvalidWithCause(t, v, cb, "spec.storage.pvc.size",
			"must be a positive storage quantity")
	}
}

func TestValidator_StorageWithOverlongNameRejected(t *testing.T) {
	// The derived PVC name (<name>-data) must fit the 253-char k8s name limit;
	// a near-max CacheBackend name + the suffix would otherwise make the backend
	// silently unreconcilable. 250 + len("-data")=5 = 255 > 253.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Name = strings.Repeat("a", 250)
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse("10Gi")},
	}
	requireInvalidWithCause(t, v, cb, "metadata.name", "would exceed the 253-character limit")

	// A normal-length name with the same storage spec is accepted.
	ok := newBackend()
	ok.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	ok.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{
		PVC: &cachev1alpha1.CacheBackendPVCSpec{Size: resource.MustParse("10Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), ok); err != nil {
		t.Fatalf("normal-length persistent backend rejected: %v", err)
	}
}

func TestValidator_ResourcesLimitsBelowRequestsRejected(t *testing.T) {
	// limits.memory < requests.memory makes the operator's intent
	// impossible to satisfy at scheduling time and is the canonical
	// misconfiguration the rule exists to catch. Reject loudly at
	// admission with a field-scoped error rather than admit a CR the
	// pod will refuse later (and that the operator would have to
	// diagnose through downstream kubectl-describe spelunking).
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.resources.limits[memory]",
		"must be greater than or equal to spec.resources.requests[memory]")
}

func TestValidator_ResourcesLimitsEqualRequestsAdmitted(t *testing.T) {
	// limits == requests is the canonical "exact size" intent and must
	// admit. The rule only rejects strict-less-than.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("limits-only rejected: %v", err)
	}
}

func TestValidator_ResourcesInvalidNameRejected(t *testing.T) {
	// ResourceList keys are opaque map keys at the CRD-schema layer:
	// a CR can be admitted with structurally-malformed names ("memory!",
	// empty string), and the kubelet rejects the pod later. Reject at
	// admission so the regression surfaces at `kubectl apply`.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceName("memory!"): resource.MustParse("4Gi"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.resources.requests[memory!]",
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceName("foo"): resource.MustParse("1"),
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.resources.requests[foo]",
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
			v := &CacheBackendValidator{}
			cb := newBackend()
			cb.Spec.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(name): resource.MustParse("1"),
				},
			}
			requireInvalidWithCause(t, v, cb,
				fmt.Sprintf("spec.resources.requests[%s]", name),
				"not a valid container resource name")
		})
	}
}

func TestValidator_ResourcesStandardContainerResourceNamesAdmitted(t *testing.T) {
	// The full set of standard container resource names — cpu, memory,
	// ephemeral-storage, and any hugepages-* variant — MUST admit. Pin
	// the contract so a future tightening doesn't accidentally exclude
	// one of them.
	for _, name := range []corev1.ResourceName{
		corev1.ResourceCPU,
		corev1.ResourceMemory,
		corev1.ResourceEphemeralStorage,
		corev1.ResourceName("hugepages-2Mi"),
		corev1.ResourceName("hugepages-1Gi"),
	} {
		t.Run(string(name), func(t *testing.T) {
			v := &CacheBackendValidator{}
			cb := newBackend()
			cb.Spec.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{name: resource.MustParse("1")},
			}
			if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
				t.Fatalf("standard container resource %q rejected: %v", name, err)
			}
		})
	}
}

func TestValidator_ResourcesValidExtendedNameAdmitted(t *testing.T) {
	// Vendor-prefixed extended resources are valid K8s ResourceNames;
	// the rule MUST admit them so operators can declare e.g.
	// nvidia.com/gpu on the cache-server container (rare but not
	// structurally forbidden).
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1Gi")},
	}
	requireInvalidWithCause(t, v, cb, "spec.resources.requests[memory]",
		"must be a non-negative quantity")
}

func TestValidator_ResourcesNegativeLimitRejected(t *testing.T) {
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-100m")},
	}
	requireInvalidWithCause(t, v, cb, "spec.resources.limits[cpu]",
		"must be a non-negative quantity")
}

func TestValidator_ResourcesZeroQuantityAdmitted(t *testing.T) {
	// Zero is permitted (matches the kubelet's >=0 contract): an
	// operator who writes `requests.memory: "0"` is explicitly opting
	// into "no guaranteed minimum", which is a valid (if unusual)
	// shape. Only strictly-negative quantities are rejected.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Claims: []corev1.ResourceClaim{{Name: "gpu-claim"}},
	}
	requireInvalidWithCause(t, v, cb, "spec.resources.claims",
		"spec.resources.claims is not supported")
}

func TestValidator_ResourcesEmptyClaimsAdmitted(t *testing.T) {
	// A nil/empty Claims slice MUST admit — the rule only fires on
	// operator-supplied entries, never on the absence of the field.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
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
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")},
	}
	requireInvalidWithCause(t, v, cb, "spec.resources.limits[cpu]",
		"must be greater than or equal to spec.resources.requests[cpu]")
}

func TestValidator_ReplicasZeroWithAutoscalingAndNilMinReplicasRejected(t *testing.T) {
	// spec.replicas=0 + spec.autoscaling enabled + nil minReplicas is the
	// silent-HPA-fallback-to-1 trap: the defaulter declines to default
	// minReplicas (a 0 value would violate the schema's Minimum=1), the
	// apiserver accepts the CR with minReplicas unset, and the reconciler's
	// HPA fallback picks defaultHPAMinReplicas=1 — overriding the operator's
	// "scale to zero" intent without notification. Admission must reject
	// the combination so the operator either sets the floor explicitly or
	// removes the autoscaling block to truly scale to zero.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}
	requireInvalidWithCause(t, v, cb, "spec.autoscaling.minReplicas",
		"spec.replicas=0 with spec.autoscaling enabled requires spec.autoscaling.minReplicas")
}

func TestValidator_ReplicasZeroWithAutoscalingAndExplicitMinReplicasAdmitted(t *testing.T) {
	// Operator who pairs replicas=0 with autoscaling sets minReplicas
	// explicitly to declare the intended HPA floor. With minReplicas=1 the
	// HPA scales the workload back up to 1 immediately (minReplicas=1 means
	// "never below one"); the test pins that the admission rule fires only
	// on the nil-minReplicas trap, not on the explicit-floor case. (CRD
	// schema enforces Minimum=1 on minReplicas, so the smallest legal
	// explicit value here is 1; true scale-to-zero requires removing the
	// autoscaling block entirely, which the next test covers.)
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MinReplicas: i32p(1),
		MaxReplicas: 10,
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("replicas=0 + autoscaling + explicit minReplicas rejected: %v", err)
	}
}

func TestValidator_ReplicasZeroWithoutAutoscalingAdmitted(t *testing.T) {
	// Pure scale-to-zero (no autoscaling block) is allowed. The HPA-fallback
	// trap only applies when autoscaling is opted into.
	v := &CacheBackendValidator{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("replicas=0 without autoscaling rejected: %v", err)
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
	// Carry a port — the new shape rule requires host:port; the
	// cross-namespace assertion below is unaffected by the port suffix.
	cb.Spec.Endpoint = "shared-cache.team-b.svc.cluster.local:9000"
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
		`scheme "https" is not supported`)
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
		"must be host:port (optionally prefixed lm://)")
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
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortOnlyRejected(t *testing.T) {
	// `:8200` is a port with no host — same broken-injection risk.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = ":8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_LMSchemePortOnlyRejected(t *testing.T) {
	// Scheme + port with no host.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "lm://:8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortlessHostRejected(t *testing.T) {
	// Bare host with no port is rejected: the LMCache connector dials a
	// specific TCP target, so spec.endpoint must carry both halves.
	// Without this check the CR admits and the engine boots with
	// LMCACHE_REMOTE_URL=lm://cache.example.com — the connector then
	// either picks an undocumented default or crashes; either way the
	// failure surfaces at the engine, not at admission where it belongs.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "cache.example.com"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_EmptyPortRejected(t *testing.T) {
	// Trailing colon with no port (`host:`) is the failure mode of an
	// operator who started typing the port and saved. Same broken
	// LMCACHE_REMOTE_URL=lm://cache.example.com: at injection.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "cache.example.com:"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortlessLMSchemeRejected(t *testing.T) {
	// Same rule applies when the scheme is explicit.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "lm://cache.example.com"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_PortlessIPv6Rejected(t *testing.T) {
	// Bracket-only IPv6 (`[::1]`) has no port either — reject for the
	// same reason. Validates that the bracket-aware path enforces the
	// port-required rule.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "[2001:db8::1]"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
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
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "cache example.com:8200"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must not contain whitespace or control characters")
}

func TestValidator_ExternalEndpoint_EmbeddedWhitespaceInPortRejected(t *testing.T) {
	// Same rule applies to the port half — `cache.example:82 00`
	// would split host="cache.example", port="82 00" and inject a
	// broken URL.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "cache.example:82 00"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must not contain whitespace or control characters")
}

func TestValidator_ExternalEndpoint_ControlCharRejected(t *testing.T) {
	// Embedded control chars (newline, tab, etc.) are rejected even
	// though they're "whitespace": same broken-URL injection risk,
	// plus a defence-in-depth against header injection if a future
	// consumer ever templates the endpoint into a text format.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "cache.example.com:8200\nLMCACHE_LOG_LEVEL=debug"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
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
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "[::1]:8200:bad"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a non-empty host AND port")
}

func TestValidator_ExternalEndpoint_BracketedIPv6ExtraColonWithSchemeRejected(t *testing.T) {
	// Same bug surface with the explicit scheme — the scheme strip
	// shouldn't change the host:port shape check that follows.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "lm://[::1]:8200:bad"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
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
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "2001:db8::1"
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
	requireInvalidWithCause(t, v, cb, "spec.endpoint",
		"must be a non-empty host AND port")
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
		"must be host:port (optionally prefixed lm://)")
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

func (stubVLLMLMCacheAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*adapterruntime.ResolvedCacheServer, error) {
	return nil, nil
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

func (stubExternalAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*adapterruntime.ResolvedCacheServer, error) {
	return nil, nil
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

// stubExternalAdapter's reserved set mirrors the production adapter at
// pkg/adapters/runtime/external — same load-bearing LMCache wire, so the
// same flag/env entries are reserved. Keeping them aligned here means the
// reserved-args/env admission check exercises the External adapter
// realistically rather than against an artificially-empty surface.
func (stubExternalAdapter) ReservedArgs() []string { return []string{"--kv-transfer-config"} }
func (stubExternalAdapter) ReservedEnv() []string {
	return []string{"LMCACHE_REMOTE_URL", "VLLM_USE_V1", "INFERENCECACHE_FAIL_OPEN"}
}
func (stubExternalAdapter) EngineContainerName() string { return "vllm" }
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

func TestValidator_EngineOverrides_ExternalBackendChecksReservedSet(t *testing.T) {
	// External now flows through the runtime-adapter check (it has its
	// own adapter with its own ReservedArgs/ReservedEnv). engineOverrides
	// on an External CR is structurally meaningful — the same canonical
	// LMCache wire reaches the engine pod whether the cache is managed
	// or operator-supplied, so suppressing `--kv-transfer-config` would
	// silently un-wire the integration in both cases. The
	// reserved-args/env check must therefore fire on External just like
	// on managed, and the registry the validator consults must include
	// the External adapter so its declared reserved set is consulted.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "shared.team-a.svc.cluster.local:9000"
	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("External CR suppressing --kv-transfer-config admitted; reserved-arg check must fire on External too")
	}
	if !strings.Contains(err.Error(), "--kv-transfer-config") {
		t.Fatalf("reserved-arg rejection should name the offending flag; got %v", err)
	}
}

func TestValidator_EngineOverrides_NilRegistry_FallsBackToShippingSet(t *testing.T) {
	// A zero-value validator (Registry: nil) must consult the SAME
	// shipping adapter set in BOTH checkRuntimeAdapter and
	// checkEngineOverrides — otherwise External admits the (vllm, External)
	// pair via the External adapter in defaultShippingRegistry but then
	// silently bypasses its reserved-arg enforcement here, letting an
	// operator un-wire the cache at the engine pod. Pin both halves of
	// the contract: nil-registry rejects External + suppressed
	// --kv-transfer-config with a field-scoped error.
	v := &CacheBackendValidator{}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "shared.team-a.svc.cluster.local:9000"
	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("nil-registry validator admitted External + suppressed --kv-transfer-config; reserved-arg check must fire via the shipping-set fallback")
	}
	if !strings.Contains(err.Error(), "--kv-transfer-config") {
		t.Fatalf("expected rejection naming the offending flag; got %v", err)
	}
}

func TestValidator_EngineOverrides_ExternalBackendAdmittedWhenSafe(t *testing.T) {
	// An External CR carrying engineOverrides that DON'T touch the
	// adapter's reserved set must still admit — the surface is engine-
	// agnostic and the External adapter's reserved set is identical to
	// the managed adapter's (LMCache wire is shared). LMCACHE_CHUNK_SIZE
	// is a perf knob, not reserved; suppressing or amending it is fine.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
	})
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	cb.Spec.Endpoint = "shared.team-a.svc.cluster.local:9000"
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External CR with non-reserved override rejected: %v", err)
	}
}

// Sanity check on the package-level wiring: SetupCacheBackendWebhookWithManager
// is exercised by manager start-up; the runtime.Object interface is the only
// thing we can sanity-check here without a controller manager.
var _ runtime.Object = (*cachev1alpha1.CacheBackend)(nil)
