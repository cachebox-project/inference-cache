// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// CacheBackendValidator rejects CacheBackend specs that are structurally
// broken — External without an endpoint, cross-namespace endpoints without
// explicit opt-in, runtime/backend pairs no installed adapter supports —
// before the reconciler ever sees them. It implements [admission.Validator]
// over CacheBackend.
//
// The structural rule set is ordered and pluggable; the runtime-adapter
// compatibility check runs separately because it needs to consult the
// shared [adapterruntime.Registry] rather than just the spec.
type CacheBackendValidator struct {
	// Rules is the ordered list of structural admission-time checks the
	// validator applies to every admitted CacheBackend. When nil/empty,
	// [DefaultValidationRules] is used.
	Rules []ValidationRule

	// Registry resolves the runtime adapter for a (runtime, backend) pair at
	// admission time. The composition root must inject it.
	Registry *adapterruntime.Registry
}

// ValidationRule is the seam plugged-in admission rules implement. It
// inspects a single CacheBackend and returns one or more field-scoped
// violations, or nil when the rule accepts the spec. Returning
// field-scoped errors lets the framework aggregate violations from every
// rule into a single rejection rather than failing on the first one.
type ValidationRule func(cb *cachev1alpha1.CacheBackend) field.ErrorList

// DefaultValidationRules is the rule set every admitted CacheBackend is
// checked against. Append a new ValidationRule here (or via
// [CacheBackendValidator.Rules]) to extend admission; no other code in the
// handler changes.
var DefaultValidationRules = []ValidationRule{
	validateCacheHierarchy,
	validateLMCacheTopology,
	rejectUnimplementedRedisBindingFeatures,
	rejectCrossNamespaceEndpointWithoutOptIn,
	requireExplicitMinReplicasOnScaleToZeroWithAutoscaling,
	rejectMooncakeMasterScaleOut,
	rejectEngineHostNetworkOnBackendThatDoesNotNeedIt,
	rejectResourceLimitsBelowRequests,
	rejectRequestsOnlyForNonOvercommittableResources,
	rejectResourceClaims,
	rejectNegativeResourceQuantities,
	rejectNonPositiveHostMemoryCapacity,
	rejectInvalidResourceNames,
	rejectFractionalExtendedResources,
	rejectMisalignedHugepageQuantities,
	rejectEventsOnlyMisconfiguration,
	validateSGLangHiCache,
	rejectInvalidKernelCheckAnnotation,
	rejectUnsupportedSGLangRole,
	rejectSGLangRedisL2ScaleOut,
}

// SetupCacheBackendWebhookWithManager registers the defaulting and
// validating webhooks for CacheBackend with mgr. The kubebuilder markers
// below are the single source of truth for the generated webhook
// configurations; do not hand-edit config/webhook/manifests.yaml.
//
// registry is the runtime-adapter [adapterruntime.Registry] the validator
// consults for the (engine, backend) compatibility check AND for the
// engineOverrides reserved-args/env check. cmd/controller threads the same
// non-nil instance the reconciler + pod webhook receive so all three layers
// agree on what's supported.
func SetupCacheBackendWebhookWithManager(mgr ctrl.Manager, registry *adapterruntime.Registry) error {
	if registry == nil {
		return fmt.Errorf("runtime adapter registry is required")
	}
	return ctrl.NewWebhookManagedBy(mgr, &cachev1alpha1.CacheBackend{}).
		WithDefaulter(&CacheBackendDefaulter{}).
		WithValidator(&CacheBackendValidator{Registry: registry}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-inferencecache-io-v1alpha1-cachebackend,mutating=false,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachebackends,verbs=create;update,versions=v1alpha1,name=vcachebackend.inferencecache.io,admissionReviewVersions=v1

// ValidateCreate implements [admission.Validator]. Every admitted
// CacheBackend runs the full rule set; aggregated violations come back as
// one Invalid status so kubectl prints them all in a single rejection.
func (v *CacheBackendValidator) ValidateCreate(ctx context.Context, cb *cachev1alpha1.CacheBackend) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CacheBackend create",
		"namespace", cb.Namespace, "name", cb.Name, "type", cb.Spec.Type)
	return collectWarnings(cb), v.validate(cb)
}

// collectWarnings returns non-blocking advisories surfaced to the operator at
// admission time (kubectl prints them on apply). A warning is for a gap the object
// itself cannot express and the controller cannot close on the operator's behalf —
// never for something a validation rule could simply reject.
func collectWarnings(cb *cachev1alpha1.CacheBackend) admission.Warnings {
	var w admission.Warnings
	w = append(w, warnMooncakeEngineHostNetwork(cb)...)
	return w
}

// ValidateUpdate implements [admission.Validator]. Updates only reject
// violations the new object *introduces* — an error that already existed
// on oldCB is filtered out so an unrelated update (a label tweak, a
// status-subresource-adjacent edit) on a CR that was admitted under a
// laxer rule set isn't suddenly un-updatable. A kubectl edit that flips a
// previously-valid field into an invalid one is still rejected, because
// the violation is then new to the diff.
//
// This is the standard pattern for tightening admission rules on a
// v1alpha1 CRD: create-time is strict; update-time only rejects fresh
// violations so existing CRs aren't trapped. Without it, adding a new
// field-level rule would break every existing CR that happens to violate it
// the moment an operator runs `kubectl annotate` on it.
func (v *CacheBackendValidator) ValidateUpdate(ctx context.Context, oldCB, newCB *cachev1alpha1.CacheBackend) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CacheBackend update",
		"namespace", newCB.Namespace, "name", newCB.Name, "type", newCB.Spec.Type)
	warnings := collectWarnings(newCB)
	newErrs := v.collectErrors(newCB)
	if len(newErrs) == 0 {
		return warnings, nil
	}
	oldErrs := v.collectErrors(oldCB)
	introduced := filterIntroducedErrors(oldErrs, newErrs)
	if len(introduced) == 0 {
		return warnings, nil
	}
	return warnings, apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CacheBackend"},
		newCB.Name,
		introduced,
	)
}

// ValidateDelete implements [admission.Validator]. Deletion is always
// allowed: removing a CacheBackend that was previously admitted under a
// stricter rule must still succeed so operators can clear bad state.
func (v *CacheBackendValidator) ValidateDelete(_ context.Context, _ *cachev1alpha1.CacheBackend) (admission.Warnings, error) {
	return nil, nil
}

// validate runs the configured rule set against cb and returns a single
// aggregated Invalid status, or nil when every rule accepts. Used by
// ValidateCreate (every rule applies); ValidateUpdate calls
// collectErrors directly so it can diff old vs new and only reject
// newly introduced violations.
func (v *CacheBackendValidator) validate(cb *cachev1alpha1.CacheBackend) error {
	errs := v.collectErrors(cb)
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CacheBackend"},
		cb.Name,
		errs,
	)
}

// collectErrors returns the field-scoped violations every configured
// rule produced for cb, including the runtime-adapter compatibility
// check. Centralised so ValidateCreate and ValidateUpdate share the
// rule-evaluation path; the runtime-adapter check runs last so a
// missing required field surfaces as a single field-level error
// instead of stacking an unsupported-pair complaint on top of it.
func (v *CacheBackendValidator) collectErrors(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	rules := v.Rules
	if len(rules) == 0 {
		rules = DefaultValidationRules
	}
	var errs field.ErrorList
	for _, rule := range rules {
		errs = append(errs, rule(cb)...)
	}
	errs = append(errs, v.checkRuntimeAdapter(cb)...)
	errs = append(errs, v.checkEngineOverrides(cb)...)
	return errs
}

// filterIntroducedErrors returns the subset of newErrs that does NOT
// appear in oldErrs — the violations the update actually introduced.
// Errors are compared by (Type, Field, BadValue, Detail); two errors
// are "the same" only if all four match, so a different message or a
// different bad value on the same field counts as a fresh violation.
//
// This is the v1alpha1 backward-compat seam: tightening admission
// rules is always allowed at create time, and at update time only
// rejects edits that newly trip the rule. A CR already in etcd that
// happens to violate a newly-added rule can still be edited (labels,
// annotations, unrelated spec fields) — the operator just can't
// introduce more violations and can't make the bad field worse.
func filterIntroducedErrors(oldErrs, newErrs field.ErrorList) field.ErrorList {
	if len(newErrs) == 0 {
		return nil
	}
	type key struct {
		Type     field.ErrorType
		Field    string
		BadValue string
		Detail   string
	}
	keyOf := func(e *field.Error) key {
		return key{
			Type:     e.Type,
			Field:    e.Field,
			BadValue: fmt.Sprintf("%v", e.BadValue),
			Detail:   e.Detail,
		}
	}
	seen := make(map[key]struct{}, len(oldErrs))
	for _, e := range oldErrs {
		seen[keyOf(e)] = struct{}{}
	}
	out := make(field.ErrorList, 0, len(newErrs))
	for _, e := range newErrs {
		if _, dup := seen[keyOf(e)]; dup {
			continue
		}
		out = append(out, e)
	}
	return out
}

// Compile-time assertions: the handlers implement the controller-runtime
// webhook interfaces. A breaking change in those interfaces fails the
// build here instead of at manager start-up.
var (
	_ admission.Defaulter[*cachev1alpha1.CacheBackend] = (*CacheBackendDefaulter)(nil)
	_ admission.Validator[*cachev1alpha1.CacheBackend] = (*CacheBackendValidator)(nil)
)
