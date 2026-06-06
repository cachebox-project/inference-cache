package v1alpha1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// CacheTenantDefaulter is a no-op defaulter for CacheTenant. Its only default
// (spec.isolationMode=Fairness) is applied by the apiserver from the
// `+kubebuilder:default=` marker before this webhook runs, so there is
// nothing to stamp here. It exists as the registered mutating handler so the
// seam is ready for a future cluster-state-dependent default. It implements
// [admission.Defaulter] over CacheTenant.
type CacheTenantDefaulter struct{}

// CacheTenantValidator enforces the operator-trust invariant CRD-schema
// markers cannot: tenantID uniqueness within a namespace. Two CacheTenants in
// one namespace claiming the same spec.tenantID would key into the same
// (tenant, model, hash_scheme, prefix_hash) index slots, so their cache state
// would structurally collide; admission rejection surfaces the clash at the
// point of mistake. The cross-CR check is why the validator holds a
// [client.Reader]. It implements [admission.Validator] over CacheTenant.
//
// Uniqueness is scoped to the namespace by design. The index keys tenants by
// the bare tenantID string (no namespace), so two CacheTenants in DIFFERENT
// namespaces that share a tenantID do resolve to the same index/quota slot —
// but the controller already handles that case deliberately and visibly: the
// CacheIndex status writer picks a deterministic owner (effectiveTenantOwners,
// first by (namespace, name)) and stamps Ready=False/DuplicateTenantID +
// QuotaExceeded=False/NotEffective on the shadowed CR. Cross-namespace reuse is
// therefore left to that runtime signal (it can be intentional, e.g. a
// migration), while the same-namespace collision — an unambiguous operator
// mistake with no legitimate reading — is hard-rejected here for immediate
// feedback. See docs/design/policy-propagation.md ("Duplicate tenantID
// tie-break").
//
// Like the CachePolicy single-per-namespace check, this is BEST-EFFORT: it
// lists then admits, so concurrent CREATEs can race. The controller's
// deterministic resolveTenants dedup is the authoritative backstop; the webhook
// is fast feedback on the common sequential mistake, not a hard guarantee.
type CacheTenantValidator struct {
	// Reader lists sibling CacheTenants for the tenantID-uniqueness check.
	// It MUST be a live (uncached) reader — production wiring passes
	// mgr.GetAPIReader() — so a CacheTenant created microseconds earlier is
	// visible and a duplicate can't slip through a cold informer cache.
	Reader client.Reader

	// Rules is the ordered list of spec-only admission checks applied to
	// every admitted CacheTenant. When nil/empty,
	// [DefaultCacheTenantValidationRules] is used. The tenantID-uniqueness
	// check is NOT a rule here because it needs cluster state (the Reader),
	// not just the spec; it runs separately.
	Rules []CacheTenantValidationRule
}

// CacheTenantValidationRule is the seam plugged-in spec-only admission rules
// implement, mirroring the CachePolicy/CacheBackend pattern.
type CacheTenantValidationRule func(ct *cachev1alpha1.CacheTenant) field.ErrorList

// reservedProbeTenantID is the tenant id the server uses for its own
// functional self-test (see ProbeTenantID in pkg/server/probe.go). A real
// CacheTenant claiming this id would (a) bypass quota enforcement at the
// PolicyStore layer — the server unconditionally exempts the probe tenant
// to defend the probe from operator-configured quotas — and (b) share the
// reserved scope with the probe's synthetic state. Reject at admission so
// the reservation lives at both layers (server policy + admission).
// MUST stay in lockstep with pkg/server.ProbeTenantID; the constant is
// duplicated rather than imported to keep internal/webhook free of a
// pkg/server dependency.
const reservedProbeTenantID = "inferencecache.io/probe"

// rejectReservedProbeTenantID rejects any CacheTenant whose spec.tenantID
// equals the server-reserved probe tenant id. The probe scope is
// server-internal state; operator-supplied CacheTenants must not govern it.
var rejectReservedProbeTenantID = func(ct *cachev1alpha1.CacheTenant) field.ErrorList {
	if ct.Spec.TenantID != reservedProbeTenantID {
		return nil
	}
	return field.ErrorList{field.Invalid(
		field.NewPath("spec", "tenantID"),
		ct.Spec.TenantID,
		"tenantID is reserved for the server's functional self-test (see pkg/server/probe.go); choose a different tenantID",
	)}
}

// DefaultCacheTenantValidationRules is the spec-only rule set every admitted
// CacheTenant is checked against. Today: reject the reserved probe tenant id
// (kubebuilder markers cover the structural rules — tenantID non-empty,
// quota.maxIndexEntries >= 0); append here or via [CacheTenantValidator.Rules]
// to extend admission.
var DefaultCacheTenantValidationRules = []CacheTenantValidationRule{
	rejectReservedProbeTenantID,
}

// SetupCacheTenantWebhookWithManager registers the defaulting and validating
// webhooks for CacheTenant with mgr. The kubebuilder markers below are the
// single source of truth for the generated webhook configurations; do not
// hand-edit config/webhook/manifests.yaml.
//
// The validator reads sibling CacheTenants through mgr.GetAPIReader() — the
// uncached live client — so the tenantID-uniqueness check sees the true
// current state rather than a lagging informer view.
func SetupCacheTenantWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &cachev1alpha1.CacheTenant{}).
		WithDefaulter(&CacheTenantDefaulter{}).
		WithValidator(&CacheTenantValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-inferencecache-io-v1alpha1-cachetenant,mutating=true,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachetenants,verbs=create;update,versions=v1alpha1,name=mcachetenant.inferencecache.io,admissionReviewVersions=v1

// Default implements [admission.Defaulter]. No-op beyond logging:
// spec.isolationMode defaults via its kubebuilder marker.
func (d *CacheTenantDefaulter) Default(ctx context.Context, ct *cachev1alpha1.CacheTenant) error {
	logf.FromContext(ctx).V(1).Info("defaulting CacheTenant (no-op; kubebuilder markers apply defaults)",
		"namespace", ct.Namespace, "name", ct.Name)
	return nil
}

// +kubebuilder:webhook:path=/validate-inferencecache-io-v1alpha1-cachetenant,mutating=false,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachetenants,verbs=create;update,versions=v1alpha1,name=vcachetenant.inferencecache.io,admissionReviewVersions=v1

// ValidateCreate implements [admission.Validator]. A new CacheTenant must
// pass the spec-only rule set AND own a tenantID no other CacheTenant in the
// namespace already claims.
func (v *CacheTenantValidator) ValidateCreate(ctx context.Context, ct *cachev1alpha1.CacheTenant) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CacheTenant create",
		"namespace", ct.Namespace, "name", ct.Name, "tenantID", ct.Spec.TenantID)

	errs := collectCacheTenantSpecErrors(v.Rules, ct)
	dupErrs, err := v.checkTenantIDUniqueness(ctx, ct)
	if err != nil {
		return nil, err
	}
	errs = append(errs, dupErrs...)
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CacheTenant"},
		ct.Name,
		errs,
	)
}

// ValidateUpdate implements [admission.Validator]. Spec-only violations are
// rejected only when the update newly introduces them (v1alpha1 tightening
// seam). The tenantID-uniqueness check re-runs only when the update actually
// changes tenantID — an unchanged tenantID cannot newly collide (the only
// sibling holding it would be the object itself, which is excluded), so an
// unrelated edit on a CR that predates the webhook isn't trapped; flipping
// tenantID onto a sibling's value is rejected.
func (v *CacheTenantValidator) ValidateUpdate(ctx context.Context, oldCT, newCT *cachev1alpha1.CacheTenant) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CacheTenant update",
		"namespace", newCT.Namespace, "name", newCT.Name, "tenantID", newCT.Spec.TenantID)

	newErrs := collectCacheTenantSpecErrors(v.Rules, newCT)
	oldErrs := collectCacheTenantSpecErrors(v.Rules, oldCT)
	errs := filterIntroducedErrors(oldErrs, newErrs)

	if newCT.Spec.TenantID != oldCT.Spec.TenantID {
		dupErrs, err := v.checkTenantIDUniqueness(ctx, newCT)
		if err != nil {
			return nil, err
		}
		errs = append(errs, dupErrs...)
	}

	if len(errs) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CacheTenant"},
		newCT.Name,
		errs,
	)
}

// ValidateDelete implements [admission.Validator]. Deletion is always
// allowed so operators can clear bad state.
func (v *CacheTenantValidator) ValidateDelete(_ context.Context, _ *cachev1alpha1.CacheTenant) (admission.Warnings, error) {
	return nil, nil
}

// collectCacheTenantSpecErrors runs the configured spec-only rule set against
// ct. Shared by ValidateCreate and ValidateUpdate.
func collectCacheTenantSpecErrors(rules []CacheTenantValidationRule, ct *cachev1alpha1.CacheTenant) field.ErrorList {
	if len(rules) == 0 {
		rules = DefaultCacheTenantValidationRules
	}
	var errs field.ErrorList
	for _, rule := range rules {
		errs = append(errs, rule(ct)...)
	}
	return errs
}

// checkTenantIDUniqueness rejects a CacheTenant whose spec.tenantID is already
// claimed by another CacheTenant in the same namespace. It lists siblings
// through the live Reader and excludes the object itself by name. The returned
// error names the conflicting tenant and the contested tenantID so the
// operator knows exactly what collides.
//
// A nil Reader FAILS CLOSED (returns an error) rather than skipping: this is a
// hard-reject invariant, so a miswired validator must surface loudly instead of
// silently admitting duplicates. Production always wires mgr.GetAPIReader().
//
// When more than one sibling already holds the tenantID, the error names the
// lexicographically smallest by metadata.name so the message is deterministic
// regardless of apiserver list order.
func (v *CacheTenantValidator) checkTenantIDUniqueness(ctx context.Context, ct *cachev1alpha1.CacheTenant) (field.ErrorList, error) {
	if v.Reader == nil {
		return nil, fmt.Errorf("CacheTenant validator misconfigured: nil Reader, cannot enforce tenantID uniqueness")
	}
	var existing cachev1alpha1.CacheTenantList
	if err := v.Reader.List(ctx, &existing, client.InNamespace(ct.Namespace)); err != nil {
		// Fail closed under failurePolicy=fail: a list error is a transient
		// apiserver problem, not license to admit a possible duplicate.
		return nil, fmt.Errorf("listing existing CacheTenants in namespace %q: %w", ct.Namespace, err)
	}
	conflict := ""
	for i := range existing.Items {
		other := &existing.Items[i]
		if other.Name == ct.Name {
			continue
		}
		if other.Spec.TenantID == ct.Spec.TenantID && (conflict == "" || other.Name < conflict) {
			conflict = other.Name
		}
	}
	if conflict == "" {
		return nil, nil
	}
	// field.Invalid (not field.Duplicate) so the detail — which names the
	// conflicting tenant — renders verbatim; Duplicate %q-escapes its value and
	// would mangle the embedded CR name.
	return field.ErrorList{
		field.Invalid(
			field.NewPath("spec", "tenantID"),
			ct.Spec.TenantID,
			fmt.Sprintf("tenantID is already claimed by CacheTenant %q in namespace %q; pick a unique tenantID or edit the existing tenant.",
				conflict, ct.Namespace),
		),
	}, nil
}
