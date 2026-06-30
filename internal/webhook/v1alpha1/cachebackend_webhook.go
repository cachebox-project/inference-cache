package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	externaladapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime/external"
)

// Phase-1 defaults applied by the mutating webhook. Centralised here so the
// tests pin the same constants the handler uses.
//
// Literal-value defaults (spec.type=LMCache, spec.deploymentKind=Deployment,
// spec.replicas=1, spec.integration.engine=vllm, spec.integration.role=
// ReadWrite, spec.integration.failOpen=true, spec.resources={requests:
// {memory:4Gi}, limits:{memory:8Gi}}) are expressed via `+kubebuilder:default=`
// markers on the API types and stamped by the apiserver before this webhook
// runs. The webhook only handles defaults the schema cannot express:
//
//   - spec.integration.firstEventTimeout: the CRD-schema default only fires
//     when spec.integration is present in the submitted object; when the
//     operator omits integration entirely the webhook materialises it here
//     so the persisted CR carries the readiness-gate deadline rather than
//     relying on the controller's runtime fallback.
//   - spec.autoscaling.minReplicas: cluster-context default computed from
//     spec.replicas at admission so the HPA's floor matches the operator's
//     baseline declaration rather than a hard-coded constant.
//
// Per-field rationale lives in the godoc on each spec field; this comment
// is the index for the webhook-stamped defaults specifically.
const (
	// defaultFirstEventTimeout mirrors the +kubebuilder:default on
	// spec.integration.firstEventTimeout. The CRD-schema default only applies
	// when spec.integration is present in the submitted object; when the
	// operator omits integration entirely the webhook materialises it here, so
	// stamping the timeout too keeps the persisted CR consistent (rather than
	// relying on the controller's runtime fallback).
	defaultFirstEventTimeout = 5 * time.Minute
)

// CacheBackendDefaulter applies the Phase-1 defaults that CRD-schema
// `+kubebuilder:default=` markers cannot express at admission time. Literal
// defaults (spec.type, deploymentKind, replicas, integration.engine,
// integration.role, integration.failOpen, resources) ride on schema
// markers and are stamped by the apiserver before this handler runs;
// the webhook only handles the schema-inexpressible ones:
//
//   - Materialises spec.integration solely to persist
//     spec.integration.firstEventTimeout when the operator omits the
//     integration block entirely (a CRD-schema default only applies when
//     the parent object is present).
//   - Computes spec.autoscaling.minReplicas from spec.replicas when
//     autoscaling is opted into and minReplicas is left unset — the HPA
//     floor needs to follow the workload's baseline declaration, which is
//     cluster-context the schema cannot encode.
//
// It does NOT stamp spec.integration.failOpen explicitly — once the
// defaulter materialises spec.integration above, the apiserver applies
// the `+kubebuilder:default=true` marker on the now-present failOpen
// field (alongside engine, role, firstEventTimeout) before persisting,
// so an admitted CR with no integration block ends up with failOpen
// populated in etcd. The read-time fallback in [IntegrationFailOpen]
// covers callers that bypass the apiserver (raw-struct test invocation,
// partial deserialization). It implements [admission.Defaulter] over
// CacheBackend.
type CacheBackendDefaulter struct{}

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

	// Registry resolves the runtime adapter for a (runtime, backend) pair
	// at admission time. A nil Registry falls back to
	// [defaultShippingRegistry], which mirrors the production cmd/controller
	// wiring: [adapterruntime.DefaultRegistry] plus the External adapter
	// (registered explicitly because the External package lives in a
	// subpackage that DefaultRegistry can't import without a cycle). The
	// bare zero value (`&CacheBackendValidator{}`) therefore admits every
	// (engine, backend) pair the running controller supports, including
	// External — so admission doesn't silently reject an otherwise-valid
	// External CR just because the caller forgot to pass a registry.
	Registry *adapterruntime.Registry
}

// defaultShippingRegistry returns a Registry with every adapter the
// production cmd/controller wiring installs: the in-package vLLM+LMCache
// adapter (via [adapterruntime.DefaultRegistry]) and the subpackage
// External adapter. Centralised here so the validator's nil-Registry
// fallback admits the same set the running controller does.
func defaultShippingRegistry() *adapterruntime.Registry {
	r := adapterruntime.DefaultRegistry()
	r.Register(externaladapter.NewAdapter())
	return r
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
	requireEndpointForExternal,
	rejectEndpointOnNonExternal,
	rejectInvalidExternalEndpoint,
	rejectCrossNamespaceEndpointWithoutOptIn,
	requireExplicitMinReplicasOnScaleToZeroWithAutoscaling,
	rejectResourceLimitsBelowRequests,
	rejectRequestsOnlyForNonOvercommittableResources,
	rejectResourceClaims,
	rejectNegativeResourceQuantities,
	rejectInvalidResourceNames,
	rejectFractionalExtendedResources,
	rejectMisalignedHugepageQuantities,
}

// SetupCacheBackendWebhookWithManager registers the defaulting and
// validating webhooks for CacheBackend with mgr. The kubebuilder markers
// below are the single source of truth for the generated webhook
// configurations; do not hand-edit config/webhook/manifests.yaml.
//
// registry is the runtime-adapter [adapterruntime.Registry] the validator
// consults for the (engine, backend) compatibility check AND for the
// engineOverrides reserved-args/env check; passing nil falls back to
// [defaultShippingRegistry] (DefaultRegistry plus the External adapter),
// mirroring cmd/controller's production wiring so a zero-value validator
// sees the same adapter set the running controller does. cmd/controller
// threads the same instance the reconciler + pod webhook receive so all
// three layers agree on what's supported.
func SetupCacheBackendWebhookWithManager(mgr ctrl.Manager, registry *adapterruntime.Registry) error {
	return ctrl.NewWebhookManagedBy(mgr, &cachev1alpha1.CacheBackend{}).
		WithDefaulter(&CacheBackendDefaulter{}).
		WithValidator(&CacheBackendValidator{Registry: registry}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-inferencecache-io-v1alpha1-cachebackend,mutating=true,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachebackends,verbs=create;update,versions=v1alpha1,name=mcachebackend.inferencecache.io,admissionReviewVersions=v1

// Default implements [admission.Defaulter]. It applies the defaults the
// CRD-schema markers cannot express:
//
//   - Materialises spec.integration when omitted so spec.integration.
//     firstEventTimeout carries the readiness-gate deadline (the
//     `+kubebuilder:default` only fires when the parent object is present).
//   - Computes spec.autoscaling.minReplicas from spec.replicas when
//     autoscaling is opted in and minReplicas is left unset.
//
// Every other Phase-1 default (spec.type=LMCache, deploymentKind=Deployment,
// replicas=1, integration.engine=vllm, integration.role=ReadWrite,
// integration.failOpen=true, resources={requests:{memory:4Gi},
// limits:{memory:8Gi}}) rides on a `+kubebuilder:default=` marker and
// is stamped by the apiserver before this handler runs. Note that the nested
// integration.* markers only fire when spec.integration is already present
// in the submitted object — when the operator omits the integration block
// entirely the apiserver has nothing to apply nested defaults to, which is
// why the webhook materialises the parent below (and the read-time
// helpers in [adapterruntime.ResolveRuntimeID] / [enginewire.IntegrationRole]
// / [IntegrationFailOpen] provide the same effective default at read time
// for callers that don't go through admission).
//
// A non-nil pointer or non-empty value is treated as an explicit operator
// choice and left alone, preserving the established "defaulter never
// clobbers" contract.
func (d *CacheBackendDefaulter) Default(ctx context.Context, cb *cachev1alpha1.CacheBackend) error {
	logf.FromContext(ctx).V(1).Info("defaulting CacheBackend",
		"namespace", cb.Namespace, "name", cb.Name)

	if cb.Spec.Integration == nil {
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	}
	if cb.Spec.Integration.FirstEventTimeout == nil {
		cb.Spec.Integration.FirstEventTimeout = &metav1.Duration{Duration: defaultFirstEventTimeout}
	}

	// autoscaling.minReplicas defaults to spec.replicas when autoscaling is
	// opted into and the operator left the floor unset. The literal
	// spec.replicas default (=1) is applied by the apiserver from the
	// `+kubebuilder:default` marker before this handler runs, so reading
	// cb.Spec.Replicas here sees either the operator's explicit value or
	// the schema default — never nil for a CR that came through admission.
	// The nil guard is defence-in-depth for tests that construct a
	// CacheBackend directly and call Default without the apiserver in the
	// loop; we leave minReplicas alone in that case rather than dereference
	// a nil pointer.
	//
	// The `>= 1` guard mirrors the CRD schema's `minimum: 1` on
	// autoscaling.minReplicas: spec.replicas allows 0 (scale-to-zero), so a
	// CR with `replicas: 0` + opted-in autoscaling would otherwise have the
	// defaulter stamp `minReplicas: 0`, which the apiserver then rejects
	// against the schema's minimum. Refusing to default in that case leaves
	// the field unset so the operator's misconfiguration surfaces as a
	// missing-required-field validation error against autoscaling rather
	// than a webhook-introduced schema violation.
	if cb.Spec.Autoscaling != nil && cb.Spec.Autoscaling.MinReplicas == nil &&
		cb.Spec.Replicas != nil && *cb.Spec.Replicas >= 1 {
		v := *cb.Spec.Replicas
		cb.Spec.Autoscaling.MinReplicas = &v
	}

	return nil
}

// +kubebuilder:webhook:path=/validate-inferencecache-io-v1alpha1-cachebackend,mutating=false,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachebackends,verbs=create;update,versions=v1alpha1,name=vcachebackend.inferencecache.io,admissionReviewVersions=v1

// ValidateCreate implements [admission.Validator]. Every admitted
// CacheBackend runs the full rule set; aggregated violations come back as
// one Invalid status so kubectl prints them all in a single rejection.
func (v *CacheBackendValidator) ValidateCreate(ctx context.Context, cb *cachev1alpha1.CacheBackend) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CacheBackend create",
		"namespace", cb.Namespace, "name", cb.Name, "type", cb.Spec.Type)
	return nil, v.validate(cb)
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
// rule (e.g. rejectEndpointOnNonExternal) would break every existing CR
// that happens to violate it the moment an operator runs `kubectl
// annotate` on it.
func (v *CacheBackendValidator) ValidateUpdate(ctx context.Context, oldCB, newCB *cachev1alpha1.CacheBackend) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CacheBackend update",
		"namespace", newCB.Namespace, "name", newCB.Name, "type", newCB.Spec.Type)
	newErrs := v.collectErrors(newCB)
	if len(newErrs) == 0 {
		return nil, nil
	}
	oldErrs := v.collectErrors(oldCB)
	introduced := filterIntroducedErrors(oldErrs, newErrs)
	if len(introduced) == 0 {
		return nil, nil
	}
	return nil, apierrors.NewInvalid(
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

// checkRuntimeAdapter rejects a CacheBackend whose effective (engine, type)
// pair no installed runtime adapter supports. The effective engine is
// resolved through [adapterruntime.ResolveRuntimeID] — the same helper the
// reconciler and pod-mutating webhook consult — so admission, reconcile,
// and pod injection agree on which adapter the registry should pick. In
// particular, an unset engine defaults to vLLM here just as it does at
// reconcile, so a CR with `type: Mooncake` and no engine no longer slips
// past admission only to fail downstream.
//
// External backends flow through this check the same way managed types
// do: they have a real runtime adapter (vllm-only today, see
// pkg/adapters/runtime/external), and the pod-mutating webhook calls
// it to wire engine pods. A CR with `type: External, engine: sglang`
// would be admitted into a state the pod webhook can't realise — the
// engine pod would silently boot un-wired to the external cache —
// without this check. Admission reject is the right surface: the
// reconciler still short-circuits External via reconcileExternal before
// any adapter lookup, so the only consumer of the (engine, External)
// pair is the pod webhook, and admission rejecting upstream of it gives
// the operator a useful error instead of a silent miss.
//
// The check is bypassed only when Spec.Type is empty: a CR that came
// through admission carries `+kubebuilder:default=LMCache` stamped by
// the apiserver before this handler runs, so an empty Type here means
// the caller bypassed the apiserver (raw-struct unit-test invocation).
// In that case the missing-type rejection is owned by CRD-level /
// future field-level validation; piling an "adapter for backend=\"\""
// cause on top would not help the user.
func (v *CacheBackendValidator) checkRuntimeAdapter(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Type == "" {
		return nil
	}
	registry := v.Registry
	if registry == nil {
		registry = defaultShippingRegistry()
	}
	runtimeID := adapterruntime.ResolveRuntimeID(cb)
	if _, err := registry.Select(runtimeID, cb); err != nil {
		if !errors.Is(err, adapterruntime.ErrNoAdapter) {
			// Registry currently only returns ErrNoAdapter from Select; a
			// future error class should surface as-is rather than be
			// rewritten as an unsupported-pair message.
			return field.ErrorList{
				field.InternalError(field.NewPath("spec", "integration", "engine"), err),
			}
		}
		// Field path points at spec.integration.engine even when the user
		// did not set it — the offending knob is "which runtime should we
		// wire to this backend", which the resolver answered for them
		// using the default. Reporting the resolved value (not "") in the
		// message gives the user the literal pair to fix.
		shownValue := ""
		if cb.Spec.Integration != nil {
			shownValue = cb.Spec.Integration.Engine
		}
		return field.ErrorList{
			field.Invalid(
				field.NewPath("spec", "integration", "engine"),
				shownValue,
				unsupportedPairMessage(runtimeID, cb.Spec.Type, registry),
			),
		}
	}
	return nil
}

// checkEngineOverrides rejects spec.integration.engineOverrides entries
// that would touch the adapter's reserved args/env — the args/env strictly
// required for the integration to function. Catching the overlap here
// gives the operator a field-scoped error naming the offending flag/env
// and the adapter, instead of a crashed engine later.
//
// The adapter is resolved through the same registry the reconciler and
// pod webhook consult; an empty spec.type or a missing adapter is left
// alone (the structural rules above already cover those). The validator
// strips the value half of two-arg args from the operator's input so a
// CR carrying `args: ["--kv-transfer-config", "{json}"]` rejects against
// the reserved leading-flag token rather than treating the JSON value as
// an unrelated entry.
func (v *CacheBackendValidator) checkEngineOverrides(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Integration == nil || cb.Spec.Integration.EngineOverrides == nil {
		return nil
	}
	overrides := cb.Spec.Integration.EngineOverrides
	basePath := field.NewPath("spec", "integration", "engineOverrides")
	var errs field.ErrorList

	// Structural env-shape checks run regardless of whether an adapter
	// matches. The pod webhook copies these EnvVar entries onto engine
	// pods at admission; if any one is shaped in a way the apiserver
	// rejects (empty name, invalid name, both value and valueFrom set),
	// the mutated pod fails Kubernetes Pod validation, blocking workload
	// admission. That would turn a cache misconfiguration into a serving
	// outage — the exact failure mode the fail-open posture exists to
	// avoid — so we reject the CR up front.
	errs = append(errs, checkEngineOverrideEnvShape(overrides.Env, basePath.Child("env"))...)

	// External backends flow through this check the same way managed
	// types do: the External adapter declares its own ReservedArgs /
	// ReservedEnv (mirroring the managed-LMCache wire it shares), and
	// the pod webhook calls the adapter for engine pods that match an
	// External CR's spec.engineSelector. Suppressing
	// `--kv-transfer-config` or overriding `LMCACHE_REMOTE_URL` on an
	// External CR would silently un-wire the cache exactly the way it
	// would on a managed CR — admission must catch it at write time,
	// not let the engine crash later. The earlier in-place External
	// skip was load-bearing only when External had no adapter; it
	// became a backdoor the moment the adapter shipped.
	//
	// Bypassed only for an empty spec.type — the structural rules
	// already reject that, and piling an "adapter for backend=\"\""
	// cause on top would not help the user.
	if cb.Spec.Type == "" {
		return errs
	}
	registry := v.Registry
	if registry == nil {
		// Mirror checkRuntimeAdapter's fallback exactly: a nil-registry
		// validator must see the same adapter set in BOTH checks, or
		// External admits in checkRuntimeAdapter (via the External adapter
		// in defaultShippingRegistry) and then silently skips its
		// reserved-arg/env enforcement here. That would let an External
		// CR suppress `--kv-transfer-config` or override
		// `LMCACHE_REMOTE_URL` and un-wire the cache at the engine pod.
		registry = defaultShippingRegistry()
	}
	runtimeID := adapterruntime.ResolveRuntimeID(cb)
	adapter, err := registry.Select(runtimeID, cb)
	if err != nil {
		// checkRuntimeAdapter already reports the unsupported pair; piling
		// a derived "engineOverrides vs (unknown adapter)" complaint on
		// top would not help the user.
		return errs
	}

	reservedArgs := stringSet(adapter.ReservedArgs())
	reservedEnv := stringSet(adapter.ReservedEnv())

	if len(reservedArgs) > 0 {
		argsPath := basePath.Child("args")
		for i, entry := range overrides.Args {
			token := overrideArgFlagToken(entry)
			if token == "" {
				continue
			}
			if reservedArgs[token] {
				errs = append(errs, field.Forbidden(
					argsPath.Index(i),
					reservedArgMessage(token, runtimeID),
				))
			}
		}
		suppressPath := basePath.Child("suppressArgs")
		for i, entry := range overrides.SuppressArgs {
			if reservedArgs[entry] {
				errs = append(errs, field.Forbidden(
					suppressPath.Index(i),
					reservedArgMessage(entry, runtimeID),
				))
			}
		}
	}

	if len(reservedEnv) > 0 {
		envPath := basePath.Child("env")
		for i, entry := range overrides.Env {
			if reservedEnv[entry.Name] {
				errs = append(errs, field.Forbidden(
					envPath.Index(i).Child("name"),
					reservedEnvMessage(entry.Name, runtimeID),
				))
			}
		}
		suppressPath := basePath.Child("suppressEnv")
		for i, entry := range overrides.SuppressEnv {
			if reservedEnv[entry] {
				errs = append(errs, field.Forbidden(
					suppressPath.Index(i),
					reservedEnvMessage(entry, runtimeID),
				))
			}
		}
	}
	return errs
}

// checkEngineOverrideEnvShape rejects EnvVar entries the apiserver itself
// would reject on the engine pod after the webhook copies them in. Mirrors
// the upstream validation rules in k8s.io/kubernetes core validation:
//
//   - Name must be set and conform to [validation.IsEnvVarName].
//   - Value and ValueFrom are mutually exclusive at the K8s API level.
//   - When ValueFrom is set, it must select EXACTLY ONE source (FieldRef,
//     ResourceFieldRef, ConfigMapKeyRef, or SecretKeyRef). Zero sources or
//     multiple sources both fail K8s Pod validation.
//
// Without these checks an operator could admit a CacheBackend that then
// makes every selected engine pod fail K8s pod validation after the
// webhook mutation — a cache misconfiguration cascading into a workload
// outage, which violates the fail-open posture.
func checkEngineOverrideEnvShape(env []corev1.EnvVar, basePath *field.Path) field.ErrorList {
	var errs field.ErrorList
	for i := range env {
		entry := &env[i]
		entryPath := basePath.Index(i)
		if entry.Name == "" {
			errs = append(errs, field.Required(entryPath.Child("name"),
				"engineOverrides env entries must declare a Name"))
		} else {
			for _, msg := range validation.IsEnvVarName(entry.Name) {
				errs = append(errs, field.Invalid(entryPath.Child("name"),
					entry.Name, "invalid env var name: "+msg))
			}
		}
		if entry.Value != "" && entry.ValueFrom != nil {
			errs = append(errs, field.Invalid(entryPath, entry.Name,
				"env entry may set value OR valueFrom, not both — engine pods carrying this entry would fail Kubernetes Pod validation"))
		}
		if entry.ValueFrom != nil {
			errs = append(errs, checkEnvVarSource(entry.ValueFrom, entryPath.Child("valueFrom"))...)
		}
	}
	return errs
}

// checkEnvVarSource enforces the K8s one-of constraint on EnvVarSource:
// exactly one source selector must be set. Both an empty valueFrom and a
// valueFrom with multiple selectors fail K8s Pod validation; admitting
// either would cascade a cache misconfiguration into engine-pod admission
// failure.
//
// Source detection uses reflection over the pointer fields of
// [corev1.EnvVarSource] so the count is future-proof: when a newer
// k8s.io/api adds a new selector (e.g. fileKeyRef, which controller-gen
// already embeds in the generated CRD schema from the upstream OpenAPI),
// the validator picks it up without code changes. Counting a hard-coded
// list of fields would silently undercount and let a multi-source shape
// slip past admission only to be rejected at engine-pod CREATE.
func checkEnvVarSource(src *corev1.EnvVarSource, path *field.Path) field.ErrorList {
	n := envVarSourceCount(src)
	switch {
	case n == 0:
		return field.ErrorList{field.Required(path,
			"valueFrom must select exactly one source (e.g. fieldRef, resourceFieldRef, configMapKeyRef, or secretKeyRef)")}
	case n > 1:
		return field.ErrorList{field.Invalid(path, n,
			"valueFrom must select exactly one source — multiple set")}
	}
	return nil
}

// envVarSourceCount returns the number of non-nil pointer fields on src.
// Each pointer field on [corev1.EnvVarSource] models one selectable
// source; the K8s API rule is exactly one of them. Reflection keeps the
// count aligned with k8s.io/api as it evolves.
func envVarSourceCount(src *corev1.EnvVarSource) int {
	if src == nil {
		return 0
	}
	v := reflect.ValueOf(*src)
	n := 0
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() == reflect.Pointer && !f.IsNil() {
			n++
		}
	}
	return n
}

// reservedArgMessage formats the rejection a user sees when their
// engineOverrides try to override or suppress an arg the adapter declares
// as reserved. Naming both the flag and the adapter lets the operator
// trace the contract back to its source without digging into adapter code.
func reservedArgMessage(flag string, runtimeID adapterruntime.RuntimeID) string {
	return fmt.Sprintf(
		"arg %q is reserved by the %q runtime adapter and cannot be overridden or suppressed via spec.integration.engineOverrides; "+
			"the adapter strictly requires this flag for the integration to function",
		flag, runtimeID,
	)
}

// reservedEnvMessage formats the rejection a user sees when their
// engineOverrides try to override or suppress an env var the adapter
// declares as reserved.
func reservedEnvMessage(name string, runtimeID adapterruntime.RuntimeID) string {
	return fmt.Sprintf(
		"env %q is reserved by the %q runtime adapter and cannot be overridden or suppressed via spec.integration.engineOverrides; "+
			"the adapter strictly requires this env for the integration to function",
		name, runtimeID,
	)
}

// overrideArgFlagToken returns the leading flag token of an arg entry,
// matching the parser the pod-webhook merge uses so admission rejects
// what the merge would otherwise treat as a flag. "--flag=value" returns
// "--flag"; "--flag" returns "--flag"; a positional (no leading "-")
// returns "" so admission ignores it (positionals never overlap a
// reserved flag name).
func overrideArgFlagToken(arg string) string {
	if !strings.HasPrefix(arg, "-") {
		return ""
	}
	if i := strings.IndexByte(arg, '='); i >= 0 {
		return arg[:i]
	}
	return arg
}

// stringSet returns a set keyed by xs. nil/empty input returns nil so
// callers can use len()/lookup-from-nil semantics without branching.
func stringSet(xs []string) map[string]bool {
	if len(xs) == 0 {
		return nil
	}
	out := make(map[string]bool, len(xs))
	for _, s := range xs {
		out[s] = true
	}
	return out
}

// unsupportedPairMessage formats the admission rejection a user sees when
// their CacheBackend asks for a runtime/backend pair no installed adapter
// supports. The message names both sides of the offending pair and lists
// the supported pairs the controller's registered adapters expose so the
// user has an actionable list of alternatives. When the registry can
// enumerate no pairs (e.g. only the permissive reference adapter is
// installed) we surface that as "(none)" rather than printing a
// misleading empty list.
func unsupportedPairMessage(engine adapterruntime.RuntimeID, backend cachev1alpha1.CacheBackendType, registry *adapterruntime.Registry) string {
	pairs := registry.SupportedPairs()
	pretty := make([]string, 0, len(pairs))
	for _, p := range pairs {
		pretty = append(pretty, p.String())
	}
	sort.Strings(pretty)
	list := "(none)"
	if len(pretty) > 0 {
		list = strings.Join(pretty, ", ")
	}
	return fmt.Sprintf(
		"no runtime adapter supports the (engine=%q, backend=%q) pair; supported pairs in this build: %s",
		engine, backend, list,
	)
}

// requireEndpointForExternal rejects an External backend that has no
// Endpoint set. An External backend is a pre-existing service the
// controller only mirrors to status — without an address there is nothing
// to mirror and the spec is structurally incomplete.
func requireEndpointForExternal(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Type != cachev1alpha1.CacheBackendTypeExternal {
		return nil
	}
	if strings.TrimSpace(cb.Spec.Endpoint) != "" {
		return nil
	}
	return field.ErrorList{
		field.Required(
			field.NewPath("spec", "endpoint"),
			"CacheBackend with spec.type=External requires spec.endpoint to be set to the address of the pre-existing backend",
		),
	}
}

// rejectEndpointOnNonExternal rejects a non-External backend that carries a
// non-empty spec.endpoint. The field is meaningful only for the External
// passthrough adapter — for managed types the controller overwrites
// status.endpoint from the live Service it provisions, so a user-supplied
// spec.endpoint would be silently ignored. Hard-rejecting at admission
// makes the misconfiguration visible at write time instead of leaving the
// operator wondering why their endpoint never took effect.
//
// An empty spec.type is left to the External-required rule and CRD-level
// validation; piling a "remove spec.endpoint" cause on top of a missing-
// type rejection would not help the user.
func rejectEndpointOnNonExternal(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Type == "" || cb.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		return nil
	}
	if strings.TrimSpace(cb.Spec.Endpoint) == "" {
		return nil
	}
	// field.Invalid (not field.Forbidden) so the bad endpoint flows into
	// the error's BadValue. ValidateUpdate's diff-vs-old logic keys on
	// (Type, Field, BadValue, Detail); using Invalid lets it distinguish
	// "operator edited the bad endpoint to a different bad endpoint"
	// (newly-introduced violation, reject) from "operator left the same
	// bad endpoint in place and changed only an unrelated field" (no
	// fresh violation, allow). field.Forbidden has BadValue="forbidden"
	// regardless of the actual value and would collapse the two cases.
	return field.ErrorList{
		field.Invalid(
			field.NewPath("spec", "endpoint"),
			cb.Spec.Endpoint,
			fmt.Sprintf("spec.endpoint is only valid when spec.type=External; got spec.type=%q with non-empty spec.endpoint. Managed backends learn their endpoint from the controller-rendered Service.", cb.Spec.Type),
		),
	}
}

// rejectInvalidExternalEndpoint rejects an External CacheBackend whose
// spec.endpoint fails the shared LMCache endpoint shape check —
// unsupported scheme, missing port, embedded whitespace, unbracketed
// IPv6, path/query/fragment components, or any other shape that would
// produce an LMCACHE_REMOTE_URL the engine connector refuses at startup.
// Catches the misconfiguration loudly at write time instead of leaving
// the operator to discover it from engine-pod crash logs.
//
// Allowed forms (see [adapterruntime.ValidateLMCacheEndpoint] for the
// full shape contract — admission, the C2 reconciler, and the pod
// webhook all call the same helper so the three layers agree):
//   - bare `host:port` (the canonical shape — the helper adds the
//     `lm://` scheme on injection)
//   - `lm://host:port` (operators who prefer to be explicit)
//   - bracketed IPv6 (`[::1]:8200`)
//
// Empty endpoint is left to [requireEndpointForExternal]; non-External
// types are left to [rejectEndpointOnNonExternal].
//
// A future SGLang-shaped External adapter (different engine wire) will
// have its own shape rules; this rule narrows on `Type == External`
// only because the vLLM wire is the only one we ship today.
func rejectInvalidExternalEndpoint(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Type != cachev1alpha1.CacheBackendTypeExternal {
		return nil
	}
	if strings.TrimSpace(cb.Spec.Endpoint) == "" {
		return nil // requireEndpointForExternal handles this
	}
	if err := adapterruntime.ValidateLMCacheEndpoint(cb.Spec.Endpoint); err != nil {
		// Wrap the helper's plain error in a field-scoped Invalid so
		// kubectl prints the field path alongside the message. The
		// reconciler and pod webhook call the same helper and act on
		// the raw error (degrade Ready, fail-open).
		return field.ErrorList{
			field.Invalid(
				field.NewPath("spec", "endpoint"),
				cb.Spec.Endpoint,
				"spec."+err.Error(),
			),
		}
	}
	return nil
}

// rejectCrossNamespaceEndpointWithoutOptIn rejects an Endpoint that
// resolves into a Service in a namespace other than the CacheBackend's
// own, unless spec.allowCrossNamespace is true. Crossing a namespace is
// a tenancy boundary the operator should explicitly acknowledge; the
// rule fires only when the Endpoint is a recognisable in-cluster Service
// DNS — external hostnames and IPs pass through (we have no namespace to
// compare against).
func rejectCrossNamespaceEndpointWithoutOptIn(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	ns, ok := serviceDNSNamespace(cb.Spec.Endpoint)
	if !ok {
		return nil
	}
	if ns == cb.Namespace {
		return nil
	}
	if cb.Spec.AllowCrossNamespace {
		return nil
	}
	return field.ErrorList{
		field.Forbidden(
			field.NewPath("spec", "endpoint"),
			fmt.Sprintf("spec.endpoint references namespace %q but CacheBackend is in namespace %q; "+
				"set spec.allowCrossNamespace=true to opt in to the cross-namespace reference",
				ns, cb.Namespace),
		),
	}
}

// requireExplicitMinReplicasOnScaleToZeroWithAutoscaling rejects the
// combination spec.replicas=0 + spec.autoscaling != nil +
// spec.autoscaling.minReplicas == nil. Without this rule the defaulter
// declines to compute minReplicas (a 0 value would violate the schema's
// Minimum=1), the apiserver accepts the CR with minReplicas left unset,
// and the reconciler's HPA fallback silently picks defaultHPAMinReplicas
// (=1) — so an operator who wrote "scale to zero" gets "scale 1-N" with
// no notification. Forcing the operator to either set the floor
// explicitly or remove the autoscaling block keeps the scale-to-zero
// intent loud at write time.
//
// Bypassed when spec.replicas is nil: the apiserver applies the
// `+kubebuilder:default=1` marker on spec.replicas before this rule
// runs for a CR that came through admission, so a nil here means the
// caller bypassed the apiserver (raw-struct unit-test invocation) and
// the rule has no replicas value to interpret.
func requireExplicitMinReplicasOnScaleToZeroWithAutoscaling(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Replicas == nil || *cb.Spec.Replicas != 0 {
		return nil
	}
	if cb.Spec.Autoscaling == nil || cb.Spec.Autoscaling.MinReplicas != nil {
		return nil
	}
	return field.ErrorList{
		field.Required(
			field.NewPath("spec", "autoscaling", "minReplicas"),
			"spec.replicas=0 with spec.autoscaling enabled requires spec.autoscaling.minReplicas to be set explicitly (must be >=1). "+
				"Set minReplicas to make the autoscaling floor explicit, or remove spec.autoscaling to scale to zero unconditionally.",
		),
	}
}

// rejectResourceLimitsBelowRequests rejects spec.resources where the
// request/limit relationship is invalid for the named resource. K8s
// distinguishes two regimes:
//
//   - Overcommittable resources (cpu, memory, ephemeral-storage):
//     limits[X] MUST be >= requests[X] when both are set. The reverse
//     is unsatisfiable at scheduling time.
//   - Non-overcommittable resources (hugepages-*, vendor-prefixed
//     extended resources like "nvidia.com/gpu"): limits[X] MUST EQUAL
//     requests[X] when both are set. Overcommitting these resources is
//     not a meaningful kubelet concept — every page or device is
//     dedicated, so request and limit must agree.
//
// The CRD-schema layer treats Requests/Limits as opaque maps, so an
// inverted or mismatched shape is silently accepted by the apiserver
// at write time and only fails when the rendered Pod tries to schedule.
// Catching it at admission turns the failure into a field-scoped error
// at `kubectl apply`. Missing Request OR missing Limit has no
// comparison to make and admits.
func rejectResourceLimitsBelowRequests(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Resources == nil {
		return nil
	}
	var errs field.ErrorList
	for name, req := range cb.Spec.Resources.Requests {
		lim, ok := cb.Spec.Resources.Limits[name]
		if !ok {
			continue
		}
		path := field.NewPath("spec", "resources", "limits").Key(string(name))
		if isOvercommittableResource(name) {
			if lim.Cmp(req) >= 0 {
				continue
			}
			errs = append(errs, field.Invalid(
				path,
				lim.String(),
				fmt.Sprintf("must be greater than or equal to spec.resources.requests[%s] (%s)", name, req.String()),
			))
			continue
		}
		// Non-overcommittable: request and limit must be exactly equal.
		if lim.Cmp(req) == 0 {
			continue
		}
		errs = append(errs, field.Invalid(
			path,
			lim.String(),
			fmt.Sprintf("must equal spec.resources.requests[%s] (%s) — %q is a non-overcommittable resource (hugepages and extended resources require request == limit)", name, req.String(), name),
		))
	}
	return errs
}

// isOvercommittableResource reports whether the resource name is one of
// the three standard overcommittable container resources for which K8s
// permits limits > requests. Every other resource (hugepages, vendor-
// prefixed extended resources) is non-overcommittable and requires
// request == limit when both are set.
func isOvercommittableResource(name corev1.ResourceName) bool {
	switch name {
	case corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
		return true
	}
	return false
}

// rejectMisalignedHugepageQuantities rejects hugepages-<size> quantities
// that are not whole multiples of the page size encoded in the resource
// name. The Linux kernel allocates hugepages in page-sized chunks, so
// K8s rejects "hugepages-2Mi: 3Mi" (3Mi isn't divisible by 2Mi) on the
// rendered Pod. Mirror that rule at admission so the operator sees a
// field-scoped error at `kubectl apply`.
//
// The page size comes from the suffix the operator wrote, which
// rejectInvalidResourceNames has already validated as a positive
// quantity. A zero quantity admits — it means "no allocation" and
// is trivially aligned to any page size.
//
// Non-hugepage resources are skipped — cpu/memory/ephemeral-storage
// take any kubelet-valid quantity, and vendor-prefixed extended
// resources are integer-checked by rejectFractionalExtendedResources.
func rejectMisalignedHugepageQuantities(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Resources == nil {
		return nil
	}
	const hugePagesPrefix = "hugepages-"
	var errs field.ErrorList
	check := func(list corev1.ResourceList, kind string) {
		for name, qty := range list {
			s := string(name)
			if !strings.HasPrefix(s, hugePagesPrefix) {
				continue
			}
			suffix := strings.TrimPrefix(s, hugePagesPrefix)
			pageSize, err := resource.ParseQuantity(suffix)
			if err != nil || pageSize.Sign() <= 0 {
				// rejectInvalidResourceNames already produced the
				// malformed-name error; don't pile on with a redundant
				// divisibility error against an undefined page size.
				continue
			}
			pageVal := pageSize.Value()
			qtyVal := qty.Value()
			// Zero is trivially aligned; negative quantities are
			// rejected by rejectNegativeResourceQuantities; we only
			// gate on a positive, mis-multiple quantity.
			if qtyVal <= 0 {
				continue
			}
			if qtyVal%pageVal != 0 {
				errs = append(errs, field.Invalid(
					field.NewPath("spec", "resources", kind).Key(s),
					qty.String(),
					fmt.Sprintf("must be a multiple of the page size %s — the Linux kernel allocates hugepages in whole-page chunks", suffix),
				))
			}
		}
	}
	check(cb.Spec.Resources.Requests, "requests")
	check(cb.Spec.Resources.Limits, "limits")
	return errs
}

// rejectFractionalExtendedResources rejects vendor-prefixed extended-
// resource quantities (e.g. nvidia.com/gpu) that carry a fractional
// value. K8s allocates extended resources by whole units (a GPU is
// either claimed or not — no "half a GPU"), so the apiserver rejects
// fractional shapes on the rendered Pod. Mirror that rule at admission
// so the operator sees a field-scoped error at `kubectl apply` rather
// than later in a child-Deployment apply.
//
// Standard overcommittable resources (cpu, memory, ephemeral-storage)
// admit fractional values — "250m" is the canonical kubelet CPU
// shape and the rule MUST NOT touch them. Hugepages-* are checked
// elsewhere (rejectInvalidResourceNames validates the suffix); their
// quantity is also non-fractional by construction but we don't gate
// on quantity here.
func rejectFractionalExtendedResources(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Resources == nil {
		return nil
	}
	var errs field.ErrorList
	check := func(list corev1.ResourceList, kind string) {
		for name, qty := range list {
			if isOvercommittableResource(name) {
				continue
			}
			if strings.HasPrefix(string(name), "hugepages-") {
				continue
			}
			if _, ok := qty.AsInt64(); !ok {
				errs = append(errs, field.Invalid(
					field.NewPath("spec", "resources", kind).Key(string(name)),
					qty.String(),
					fmt.Sprintf("%q is an extended resource and must be an integer quantity — K8s allocates extended resources by whole units", name),
				))
			}
		}
	}
	check(cb.Spec.Resources.Requests, "requests")
	check(cb.Spec.Resources.Limits, "limits")
	return errs
}

// rejectInvalidResourceNames rejects any spec.resources.requests or
// spec.resources.limits key that fails the K8s container-resource-name
// rules. The CRD schema treats ResourceList keys as opaque strings, so
// an invalid name persists in etcd and only fails when the apiserver
// rejects the rendered child pod. Rejecting at admission turns that
// latent failure into a field-scoped error at `kubectl apply`.
//
// K8s container-resource rules are stricter than the bare
// IsQualifiedName check: a valid container resource name is one of
//   - the standard scheduled resources (cpu, memory, ephemeral-storage),
//   - a hugepages-* variant (the prefix is K8s-reserved), or
//   - a vendor-prefixed extended resource ("vendor.com/foo") that also
//     satisfies IsQualifiedName.
//
// A bare unqualified name like "foo" is admitted by IsQualifiedName but
// is NOT a valid container resource: the apiserver requires extended
// resources to carry a "/" — the prefix is the vendor identity. We
// apply the same rule here so the rejection is consistent with what
// the rendered Pod would face downstream.
func rejectInvalidResourceNames(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Resources == nil {
		return nil
	}
	var errs field.ErrorList
	check := func(list corev1.ResourceList, kind string) {
		for name := range list {
			if msg, ok := validateContainerResourceName(name); !ok {
				errs = append(errs, field.Invalid(
					field.NewPath("spec", "resources", kind).Key(string(name)),
					string(name),
					msg,
				))
			}
		}
	}
	check(cb.Spec.Resources.Requests, "requests")
	check(cb.Spec.Resources.Limits, "limits")
	return errs
}

// validateContainerResourceName mirrors the K8s container-resource-name
// contract: standard names (cpu, memory, ephemeral-storage) admit
// unconditionally; a `hugepages-<size>` name admits only when the size
// suffix parses as a strictly-positive resource.Quantity (matching what
// the apiserver requires of Container.Resources entries); any other
// name must be vendor-prefixed (contain a "/") and satisfy
// IsQualifiedName. Returns ("", true) on accept; ("…reason…", false) on
// reject — the reason is surfaced verbatim in the field-scoped
// admission error.
func validateContainerResourceName(name corev1.ResourceName) (string, bool) {
	s := string(name)
	switch name {
	case corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
		return "", true
	}
	const hugePagesPrefix = "hugepages-"
	if strings.HasPrefix(s, hugePagesPrefix) {
		suffix := strings.TrimPrefix(s, hugePagesPrefix)
		qty, err := resource.ParseQuantity(suffix)
		if err != nil || qty.Sign() <= 0 {
			return fmt.Sprintf(
				"%q is not a valid container resource name: %q must be followed by a positive page-size quantity (e.g. \"hugepages-2Mi\")",
				s, hugePagesPrefix,
			), false
		}
		return "", true
	}
	if !strings.Contains(s, "/") {
		return fmt.Sprintf(
			"%q is not a valid container resource name: must be one of %q/%q/%q, a hugepages-<size> variant (e.g. \"hugepages-2Mi\"), or a vendor-prefixed extended resource (e.g. \"nvidia.com/gpu\")",
			s, corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage,
		), false
	}
	// "kubernetes.io/" and "requests.kubernetes.io/" are K8s-reserved
	// native-resource prefixes; extended resources MUST use a third-party
	// vendor domain instead. Admitting them here would let a CR through
	// that the apiserver rejects on the rendered Pod.
	if strings.HasPrefix(s, "kubernetes.io/") || strings.HasPrefix(s, "requests.kubernetes.io/") {
		return fmt.Sprintf(
			"%q is not a valid container resource name: %q and %q are K8s-reserved prefixes — extended resources must use a third-party vendor domain (e.g. \"nvidia.com/gpu\")",
			s, "kubernetes.io/", "requests.kubernetes.io/",
		), false
	}
	if msgs := validation.IsQualifiedName(s); len(msgs) > 0 {
		return fmt.Sprintf("%q is not a valid container resource name: %s", s, msgs[0]), false
	}
	return "", true
}

// rejectNegativeResourceQuantities rejects any strictly-negative
// quantity in spec.resources.requests or spec.resources.limits. The
// CRD schema serialises each entry as a resource.Quantity string, which
// admits a leading "-" without complaint at structural validation —
// the apiserver's Pod resource validator later rejects the pod with a
// "must be greater than or equal to 0" error that the operator has to
// chase through child Deployment events. Rejecting at admission turns
// that latent failure into a field-scoped error at `kubectl apply`.
//
// Zero is allowed: a `requests.memory: "0"` shape is unusual but
// explicitly valid under the kubelet's `>= 0` contract — an operator
// who writes it is opting into "no guaranteed minimum", which is the
// kubelet's default treatment of a missing request and a reasonable
// shape to admit verbatim.
func rejectNegativeResourceQuantities(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Resources == nil {
		return nil
	}
	var errs field.ErrorList
	check := func(list corev1.ResourceList, kind string) {
		for name, qty := range list {
			if qty.Sign() >= 0 {
				continue
			}
			errs = append(errs, field.Invalid(
				field.NewPath("spec", "resources", kind).Key(string(name)),
				qty.String(),
				"must be a non-negative quantity",
			))
		}
	}
	check(cb.Spec.Resources.Requests, "requests")
	check(cb.Spec.Resources.Limits, "limits")
	return errs
}

// rejectRequestsOnlyForNonOvercommittableResources rejects a non-
// overcommittable resource (hugepages-*, vendor-prefixed extended
// resource) declared in `spec.resources.requests` without a matching
// entry in `spec.resources.limits`. K8s requires both halves for
// non-overcommittable resources — the kubelet allocates whole pages
// or devices, so the request and limit must be declared together and
// be equal. Limits-only IS admitted by K8s (the apiserver auto-
// populates requests from limits when only limits is set), so the
// rule fires only on the requests-only direction. Overcommittable
// resources (cpu, memory, ephemeral-storage) are unaffected — a
// requests-only cpu / memory shape is the canonical kubelet "no upper
// bound" pattern.
func rejectRequestsOnlyForNonOvercommittableResources(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Resources == nil {
		return nil
	}
	var errs field.ErrorList
	for name := range cb.Spec.Resources.Requests {
		if isOvercommittableResource(name) {
			continue
		}
		if _, ok := cb.Spec.Resources.Limits[name]; ok {
			continue
		}
		qty := cb.Spec.Resources.Requests[name]
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "resources", "requests").Key(string(name)),
			qty.String(),
			fmt.Sprintf("%q is a non-overcommittable resource — it must also be set in spec.resources.limits with the same value (hugepages and extended resources require requests and limits to be declared together)", name),
		))
	}
	return errs
}

// rejectResourceClaims rejects a non-empty spec.resources.claims slice.
// corev1.ResourceRequirements exposes Claims for the Dynamic Resource
// Allocation (DRA) feature, but the runtime adapter only copies
// Container.Resources onto the rendered pod template — it does NOT
// populate the matching pod.spec.resourceClaims that claim names
// reference. Admitting a CR with non-empty Claims would render a
// Deployment the apiserver rejects because the claim names don't
// resolve at the pod level (silent breakage that's hard to triage from
// the CacheBackend side). Reject loudly at admission until the renderer
// learns to plumb the full pod-level DRA surface.
//
// A nil/empty Claims slice is the absence of the field and admits
// unchanged — the rule fires only on operator-supplied entries.
func rejectResourceClaims(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Resources == nil || len(cb.Spec.Resources.Claims) == 0 {
		return nil
	}
	return field.ErrorList{
		field.Forbidden(
			field.NewPath("spec", "resources", "claims"),
			"spec.resources.claims is not supported in v1alpha1: the runtime adapter does not plumb pod.spec.resourceClaims, so a claim-bound container.resources.claims would render a pod the apiserver rejects",
		),
	}
}

// k8sClusterDomain is the standard Kubernetes cluster DNS suffix. Most
// clusters use the default; the rare cluster with a custom cluster
// domain can opt past the cross-namespace rule with
// spec.allowCrossNamespace=true rather than have the parser
// conservatively widen to anything that contains a "svc" label.
const k8sClusterDomain = "cluster.local"

// serviceDNSNamespace returns the namespace segment of an in-cluster
// Service-scoped or Pod-scoped Kubernetes DNS endpoint, or false if the
// endpoint is not recognisable as in-cluster DNS. To avoid misparsing
// external hostnames that happen to contain a "svc" label (e.g.
// "cache.team-b.svc.example.com"), the parser only matches hostnames
// that end with ".svc" or ".svc.cluster.local" — the two canonical
// Kubernetes forms. Recognised shapes (after stripping scheme + path +
// port + optional cluster-domain suffix):
//
//	Service-scoped:
//	  <svc>.<ns>.svc
//	  <svc>.<ns>.svc.cluster.local
//	Pod-scoped (StatefulSet pod-FQDN / headless-service pod-DNS):
//	  <pod>.<svc>.<ns>.svc
//	  <pod>.<svc>.<ns>.svc.cluster.local
//
// Both forms cross the same tenancy boundary — pod-FQDNs are how
// StatefulSet pods are addressed individually and must be treated as
// equivalent to the Service DNS that backs them.
//
// External hostnames (e.g. "cache.example.com"), IP addresses, and
// unqualified names pass through as ok=false — we have no namespace to
// compare against and rejecting them would block legitimate
// external-backend addresses.
func serviceDNSNamespace(endpoint string) (string, bool) {
	host := strings.TrimSpace(endpoint)
	if host == "" {
		return "", false
	}
	// DNS is case-insensitive and a fully-qualified name may carry a
	// trailing dot ("svc.cluster.local."); normalise both so the suffix
	// match below is not bypassed by either variant.
	host = strings.ToLower(host)
	// Strip a leading URL scheme (http://, https://, grpc://, ...).
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	// Strip a path/query suffix.
	if i := strings.IndexAny(host, "/?"); i >= 0 {
		host = host[:i]
	}
	// Strip a trailing :port (works for IPv4/hostnames; an IPv6 literal in
	// brackets would not match the .svc pattern below anyway).
	if i := strings.LastIndex(host, ":"); i >= 0 && !strings.Contains(host[i:], ".") {
		host = host[:i]
	}
	// Drop the FQDN trailing dot (e.g. "...svc.cluster.local.") so the
	// suffix match below isn't bypassed by the absolute-DNS form.
	host = strings.TrimSuffix(host, ".")
	// Strip the optional Kubernetes cluster-domain suffix so the two
	// canonical forms collapse to a single ".svc"-terminated string.
	host = strings.TrimSuffix(host, "."+k8sClusterDomain)
	// Anchored match: in-cluster DNS terminates at ".svc". Anything else
	// (external hostnames, IPs, unqualified names) is not in-cluster.
	if !strings.HasSuffix(host, ".svc") {
		return "", false
	}
	host = strings.TrimSuffix(host, ".svc")
	parts := strings.Split(host, ".")
	// After trimming ".svc", we need at least <svc>.<ns> (Service form)
	// or <pod>.<svc>.<ns> (Pod-FQDN form). The namespace is the
	// rightmost label in both cases.
	if len(parts) < 2 {
		return "", false
	}
	ns := parts[len(parts)-1]
	if ns == "" {
		return "", false
	}
	return ns, true
}

// Compile-time assertions: the handlers implement the controller-runtime
// webhook interfaces. A breaking change in those interfaces fails the
// build here instead of at manager start-up.
var (
	_ admission.Defaulter[*cachev1alpha1.CacheBackend] = (*CacheBackendDefaulter)(nil)
	_ admission.Validator[*cachev1alpha1.CacheBackend] = (*CacheBackendValidator)(nil)
)
