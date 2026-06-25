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

// CachePolicyDefaulter is a no-op defaulter for CachePolicy. Every Phase-1
// default the type needs is expressed by `+kubebuilder:default=` markers
// (spec.eviction=LRU), which the apiserver applies at admission before this
// webhook runs — so there is nothing left to stamp here. It exists as the
// registered mutating handler so the seam is in place the moment a
// cluster-state-dependent default emerges (the kind kubebuilder markers
// cannot express); until then Default only logs. It implements
// [admission.Defaulter] over CachePolicy.
type CachePolicyDefaulter struct{}

// CachePolicyValidator enforces the operator-trust invariants CRD-schema
// markers cannot: at most one CachePolicy per namespace, and a strictly
// positive evictionTTL when set. The single-policy rule is why the validator
// holds a [client.Reader] — it must list sibling CachePolicies in the
// namespace at admission time. It implements [admission.Validator] over
// CachePolicy.
//
// The single-namespace invariant exists because the CachePolicy reconciler
// flattens to one ResolvedPolicy per namespace and picks deterministically
// when several CRs exist; operators have no signal which one won. Rejecting
// the second CR at CREATE makes the "one policy, one namespace" contract
// explicit at the point of mistake instead of silently dropping intent.
//
// This admission check is BEST-EFFORT, not a hard guarantee: it lists then
// admits, so two concurrent CREATEs can both observe an empty namespace before
// either persists and both slip through. The AUTHORITATIVE backstop remains
// the controller's deterministic dedup (resolvePolicies sorts by
// (namespace, name) and the first entry wins regardless of apiserver ordering)
// — the webhook turns the common, sequential mistake into immediate kubectl
// feedback rather than replacing that backstop. See docs/design/
// policy-propagation.md ("Multiple CachePolicies in one namespace").
type CachePolicyValidator struct {
	// Reader lists sibling CachePolicies for the one-per-namespace check.
	// It MUST be a live (uncached) reader — production wiring passes
	// mgr.GetAPIReader() — so a CachePolicy created microseconds earlier is
	// visible and two CRs can't both slip through a cold informer cache.
	Reader client.Reader

	// Rules is the ordered list of spec-only admission checks applied to
	// every admitted CachePolicy. When nil/empty,
	// [DefaultCachePolicyValidationRules] is used. The one-per-namespace
	// check is NOT a rule here because it needs cluster state (the Reader),
	// not just the spec; it runs separately in ValidateCreate.
	Rules []CachePolicyValidationRule
}

// CachePolicyValidationRule is the seam plugged-in spec-only admission rules
// implement. It inspects a single CachePolicy and returns field-scoped
// violations, or nil when the rule accepts the spec. Mirrors the
// CacheBackend webhook's ValidationRule, scoped to CachePolicy so the two
// rule sets stay type-distinct in the shared package.
type CachePolicyValidationRule func(cp *cachev1alpha1.CachePolicy) field.ErrorList

// DefaultCachePolicyValidationRules is the spec-only rule set every admitted
// CachePolicy is checked against. Append a new rule here (or via
// [CachePolicyValidator.Rules]) to extend admission; no other handler code
// changes.
var DefaultCachePolicyValidationRules = []CachePolicyValidationRule{
	rejectNonPositiveEvictionTTL,
	rejectIncoherentStrategy,
}

// SetupCachePolicyWebhookWithManager registers the defaulting and validating
// webhooks for CachePolicy with mgr. The kubebuilder markers below are the
// single source of truth for the generated webhook configurations; do not
// hand-edit config/webhook/manifests.yaml.
//
// The validator reads sibling CachePolicies through mgr.GetAPIReader() — the
// uncached live client, matching the pod-mutating webhook's posture — so the
// one-per-namespace check sees the true current state rather than a lagging
// informer view.
func SetupCachePolicyWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &cachev1alpha1.CachePolicy{}).
		WithDefaulter(&CachePolicyDefaulter{}).
		WithValidator(&CachePolicyValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-inferencecache-io-v1alpha1-cachepolicy,mutating=true,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachepolicies,verbs=create;update,versions=v1alpha1,name=mcachepolicy.inferencecache.io,admissionReviewVersions=v1

// Default implements [admission.Defaulter]. It is intentionally a no-op
// beyond logging: CachePolicy defaults are applied by the apiserver from
// `+kubebuilder:default=` markers before this handler runs.
func (d *CachePolicyDefaulter) Default(ctx context.Context, cp *cachev1alpha1.CachePolicy) error {
	logf.FromContext(ctx).V(1).Info("defaulting CachePolicy (no-op; kubebuilder markers apply defaults)",
		"namespace", cp.Namespace, "name", cp.Name)
	return nil
}

// +kubebuilder:webhook:path=/validate-inferencecache-io-v1alpha1-cachepolicy,mutating=false,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachepolicies,verbs=create;update,versions=v1alpha1,name=vcachepolicy.inferencecache.io,admissionReviewVersions=v1

// ValidateCreate implements [admission.Validator]. A new CachePolicy must
// pass the spec-only rule set AND be the only CachePolicy in its namespace.
// Violations from both checks are aggregated into one Invalid status so
// kubectl prints them together.
func (v *CachePolicyValidator) ValidateCreate(ctx context.Context, cp *cachev1alpha1.CachePolicy) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CachePolicy create",
		"namespace", cp.Namespace, "name", cp.Name)

	errs := collectCachePolicySpecErrors(v.Rules, cp)
	siblingErrs, err := v.checkSinglePolicyPerNamespace(ctx, cp)
	if err != nil {
		return nil, err
	}
	errs = append(errs, siblingErrs...)
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CachePolicy"},
		cp.Name,
		errs,
	)
}

// ValidateUpdate implements [admission.Validator]. The one-per-namespace
// rule is deliberately NOT re-checked on update: editing the namespace's
// single policy is the intended workflow, and a CR already in etcd must stay
// editable. Only spec-only violations the update newly introduces are
// rejected (the same v1alpha1 tightening seam the CacheBackend webhook uses),
// so an unrelated label edit on a CR admitted under a laxer rule set isn't
// suddenly un-updatable, while flipping evictionTTL negative is.
func (v *CachePolicyValidator) ValidateUpdate(ctx context.Context, oldCP, newCP *cachev1alpha1.CachePolicy) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CachePolicy update",
		"namespace", newCP.Namespace, "name", newCP.Name)

	newErrs := collectCachePolicySpecErrors(v.Rules, newCP)
	if len(newErrs) == 0 {
		return nil, nil
	}
	oldErrs := collectCachePolicySpecErrors(v.Rules, oldCP)
	introduced := filterIntroducedErrors(oldErrs, newErrs)
	if len(introduced) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CachePolicy"},
		newCP.Name,
		introduced,
	)
}

// ValidateDelete implements [admission.Validator]. Deletion is always
// allowed so operators can clear bad state.
func (v *CachePolicyValidator) ValidateDelete(_ context.Context, _ *cachev1alpha1.CachePolicy) (admission.Warnings, error) {
	return nil, nil
}

// collectCachePolicySpecErrors runs the configured spec-only rule set against
// cp. Shared by ValidateCreate and ValidateUpdate so both evaluate the same
// rules; the cross-CR namespace check is layered on separately by
// ValidateCreate because it needs the Reader.
func collectCachePolicySpecErrors(rules []CachePolicyValidationRule, cp *cachev1alpha1.CachePolicy) field.ErrorList {
	if len(rules) == 0 {
		rules = DefaultCachePolicyValidationRules
	}
	var errs field.ErrorList
	for _, rule := range rules {
		errs = append(errs, rule(cp)...)
	}
	return errs
}

// checkSinglePolicyPerNamespace rejects a CachePolicy when another already
// exists in the same namespace. It lists siblings through the live Reader and
// excludes the object itself by name (so a re-admitted CR — e.g. a dry-run
// followed by the real CREATE — doesn't collide with its own record).
//
// A nil Reader FAILS CLOSED (returns an error): a validator wired without a
// Reader cannot enforce a hard-reject invariant, and silently admitting would
// disable the rule on a future miswiring. Production always wires
// mgr.GetAPIReader() via SetupCachePolicyWebhookWithManager.
//
// When several siblings exist (e.g. CRs that predate this webhook), the error
// names the lexicographically smallest by metadata.name — which is also the CR
// the controller's resolvePolicies picks as effective — so the message is
// deterministic regardless of apiserver list order AND points at the policy
// actually in force.
func (v *CachePolicyValidator) checkSinglePolicyPerNamespace(ctx context.Context, cp *cachev1alpha1.CachePolicy) (field.ErrorList, error) {
	if v.Reader == nil {
		return nil, fmt.Errorf("CachePolicy validator misconfigured: nil Reader, cannot enforce one-policy-per-namespace")
	}
	var existing cachev1alpha1.CachePolicyList
	if err := v.Reader.List(ctx, &existing, client.InNamespace(cp.Namespace)); err != nil {
		// A failed list is a transient apiserver problem, not an admission
		// verdict — surface it so the request fails closed (failurePolicy=fail)
		// instead of silently admitting a second policy.
		return nil, fmt.Errorf("listing existing CachePolicies in namespace %q: %w", cp.Namespace, err)
	}
	conflict := ""
	for i := range existing.Items {
		other := existing.Items[i].Name
		if other == cp.Name {
			continue
		}
		if conflict == "" || other < conflict {
			conflict = other
		}
	}
	if conflict == "" {
		return nil, nil
	}
	return field.ErrorList{
		field.Forbidden(
			field.NewPath("metadata", "name"),
			fmt.Sprintf("namespace %q already has CachePolicy %q; at most one CachePolicy is allowed per namespace. Edit the existing policy instead of creating a second one.",
				cp.Namespace, conflict),
		),
	}, nil
}

// rejectNonPositiveEvictionTTL enforces the "evictionTTL > 0 when set"
// invariant. The field is a metav1.Duration whose `+kubebuilder` markers
// don't constrain sign, so a zero or negative TTL — which the index would
// silently clamp to its default, hiding the operator's typo — is caught here
// at admission. An unset (nil) TTL is fine: the index applies its own
// DefaultTTL.
func rejectNonPositiveEvictionTTL(cp *cachev1alpha1.CachePolicy) field.ErrorList {
	if cp.Spec.EvictionTTL == nil {
		return nil
	}
	if cp.Spec.EvictionTTL.Duration > 0 {
		return nil
	}
	return field.ErrorList{
		field.Invalid(
			field.NewPath("spec", "evictionTTL"),
			cp.Spec.EvictionTTL.Duration.String(),
			"evictionTTL must be greater than zero when set (a non-positive TTL would be silently clamped to the index default)",
		),
	}
}

// rejectIncoherentStrategy rejects a policy that requires chain-form lookups
// while also disabling the chain matcher. That shape would reject legacy
// exact-prefix callers and make chain callers unmatchable, so surface it at
// admission instead of letting the server produce surprising misses.
func rejectIncoherentStrategy(cp *cachev1alpha1.CachePolicy) field.ErrorList {
	strategy := cp.Spec.Strategy
	if strategy == nil || strategy.EnableChainMatching == nil || strategy.RequireChain == nil {
		return nil
	}
	if *strategy.EnableChainMatching || !*strategy.RequireChain {
		return nil
	}
	return field.ErrorList{
		field.Invalid(
			field.NewPath("spec", "strategy", "requireChain"),
			*strategy.RequireChain,
			"requireChain requires enableChainMatching to be true",
		),
	}
}
