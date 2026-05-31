package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// Phase-1 defaults applied by the mutating webhook. Centralised here so the
// tests pin the same constants the handler uses. `spec.integration.failOpen`
// is defaulted to true at the CRD layer via a +kubebuilder:default marker
// (apiserver defaulting runs before mutating admission), so this webhook does
// not need to stamp it.
const (
	defaultReplicas = int32(1)
)

// memoryOnlyBackends classifies the CacheBackendType values that are
// architecturally in-memory in Phase 1 and therefore cannot accept a PVC.
// Kept as a small map (rather than a property on the enum) so we have one
// place to revise as the substrate adapter set grows — and so the underlying
// list is unit-test addressable. AIBrix and NIXL are KV-pool / transfer
// layers with no on-disk tier; hierarchical backends (LMCache,
// SGLangHiCache, Mooncake) accept PVC-backed tiers and are absent here.
var memoryOnlyBackends = map[cachev1alpha1.CacheBackendType]bool{
	cachev1alpha1.CacheBackendTypeAIBrix: true,
	cachev1alpha1.CacheBackendTypeNIXL:   true,
}

// CacheBackendDefaulter applies Phase-1 defaults to a CacheBackend at
// admission time. Today it stamps only spec.replicas; spec.integration is
// left as-is when unset (downstream code nil-checks it), and
// spec.integration.failOpen is defaulted at the CRD layer via a
// +kubebuilder:default marker. It implements [admission.Defaulter] over
// CacheBackend.
type CacheBackendDefaulter struct{}

// CacheBackendValidator rejects CacheBackend specs that are structurally
// broken — External without an endpoint, persistent storage on a
// memory-only backend, cross-namespace endpoints without explicit opt-in,
// runtime/backend pairs no installed adapter supports — before the
// reconciler ever sees them. It implements [admission.Validator] over
// CacheBackend.
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
	// [adapterruntime.DefaultRegistry] so unit tests and the bare zero
	// value still validate against the controller's shipping adapter set;
	// production wiring in cmd/controller passes the same registry
	// instance the reconciler + pod webhook consume so all three agree on
	// what's supported.
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
	requireEndpointForExternal,
	rejectPersistentStorageOnMemoryOnly,
	rejectCrossNamespaceEndpointWithoutOptIn,
}

// SetupCacheBackendWebhookWithManager registers the defaulting and
// validating webhooks for CacheBackend with mgr. The kubebuilder markers
// below are the single source of truth for the generated webhook
// configurations; do not hand-edit config/webhook/manifests.yaml.
//
// registry is the runtime-adapter [adapterruntime.Registry] the validator
// consults for the (engine, backend) compatibility check; passing nil falls
// back to [adapterruntime.DefaultRegistry]. cmd/controller threads the same
// instance the reconciler + pod webhook receive so all three layers agree on
// what's supported.
func SetupCacheBackendWebhookWithManager(mgr ctrl.Manager, registry *adapterruntime.Registry) error {
	return ctrl.NewWebhookManagedBy(mgr, &cachev1alpha1.CacheBackend{}).
		WithDefaulter(&CacheBackendDefaulter{}).
		WithValidator(&CacheBackendValidator{Registry: registry}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-inferencecache-io-v1alpha1-cachebackend,mutating=true,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachebackends,verbs=create;update,versions=v1alpha1,name=mcachebackend.inferencecache.io,admissionReviewVersions=v1

// Default implements [admission.Defaulter]. It stamps the Phase-1
// defaults onto cb only where the operator did not specify a value: a
// non-nil pointer or a non-zero scalar is treated as an explicit choice
// and left alone.
func (d *CacheBackendDefaulter) Default(ctx context.Context, cb *cachev1alpha1.CacheBackend) error {
	logf.FromContext(ctx).V(1).Info("defaulting CacheBackend",
		"namespace", cb.Namespace, "name", cb.Name)

	if cb.Spec.Replicas == nil {
		v := defaultReplicas
		cb.Spec.Replicas = &v
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

// ValidateUpdate implements [admission.Validator]. Updates are validated
// against the *new* object only: admission rules are functions of the
// desired spec, and re-checking each admit catches a kubectl edit that
// flips a previously-valid field just as it would on create.
func (v *CacheBackendValidator) ValidateUpdate(ctx context.Context, _, newCB *cachev1alpha1.CacheBackend) (admission.Warnings, error) {
	logf.FromContext(ctx).V(1).Info("validating CacheBackend update",
		"namespace", newCB.Namespace, "name", newCB.Name, "type", newCB.Spec.Type)
	return nil, v.validate(newCB)
}

// ValidateDelete implements [admission.Validator]. Deletion is always
// allowed: removing a CacheBackend that was previously admitted under a
// stricter rule must still succeed so operators can clear bad state.
func (v *CacheBackendValidator) ValidateDelete(_ context.Context, _ *cachev1alpha1.CacheBackend) (admission.Warnings, error) {
	return nil, nil
}

// validate runs the configured rule set against cb and returns a single
// aggregated Invalid status, or nil when every rule accepts. Centralised
// here so create + update share one code path. The runtime-adapter
// compatibility check runs after the structural rules so a missing
// required field surfaces as a single field-level error instead of
// stacking an unsupported-pair complaint on top of it.
func (v *CacheBackendValidator) validate(cb *cachev1alpha1.CacheBackend) error {
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
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CacheBackend"},
		cb.Name,
		errs,
	)
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
// The check is bypassed for two CR shapes that never reach the adapter
// registry at reconcile:
//
//   - Spec.Type is empty: there is no defaulting for type and the
//     missing-type rejection is owned by CRD-level / future field-level
//     validation; piling an "adapter for backend=\"\"" cause on top would
//     not help the user.
//   - Spec.Type is [cachev1alpha1.CacheBackendTypeExternal]: External
//     backends are pre-existing services the controller only mirrors to
//     status, not managed workloads — the reconciler routes them through
//     reconcileExternal before any adapter lookup, so admission must not
//     reject them for "no adapter".
func (v *CacheBackendValidator) checkRuntimeAdapter(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Type == "" || cb.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		return nil
	}
	registry := v.Registry
	if registry == nil {
		registry = adapterruntime.DefaultRegistry()
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

	if cb.Spec.Type == "" || cb.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		return errs
	}
	registry := v.Registry
	if registry == nil {
		registry = adapterruntime.DefaultRegistry()
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

// rejectPersistentStorageOnMemoryOnly rejects a PVC-backed storage spec on
// a backend type that is in-memory by design. Letting it through would
// generate a PVC the workload can never mount.
func rejectPersistentStorageOnMemoryOnly(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Storage == nil || cb.Spec.Storage.PVC == nil {
		return nil
	}
	if !memoryOnlyBackends[cb.Spec.Type] {
		return nil
	}
	return field.ErrorList{
		field.Forbidden(
			field.NewPath("spec", "storage", "pvc"),
			fmt.Sprintf("CacheBackend type %q is memory-only and cannot declare spec.storage.pvc", cb.Spec.Type),
		),
	}
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
