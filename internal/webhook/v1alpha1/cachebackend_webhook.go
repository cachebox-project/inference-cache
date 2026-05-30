package v1alpha1

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// Phase-1 defaults applied by the mutating webhook. Centralised here so the
// tests pin the same constants the handler uses; the values come from the
// tech-spec §4.1 example for `CacheBackend.spec.integration`
// (lookupTimeoutMs=50, minimumPrefixTokens=256). `spec.integration.failOpen`
// is also defaulted to true, but at the CRD layer via a +kubebuilder:default
// marker (apiserver defaulting runs before mutating admission), so this
// webhook does not need to stamp it.
const (
	defaultLookupTimeoutMs     = int32(50)
	defaultMinimumPrefixTokens = int32(256)
	defaultReplicas            = int32(1)
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
// admission time so downstream code never has to nil-check the defaulted
// fields. It implements [admission.Defaulter] over CacheBackend.
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
	rejectEndpointOnNonExternal,
	rejectInvalidExternalEndpointScheme,
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

	if cb.Spec.Integration == nil {
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	}
	if cb.Spec.Integration.LookupTimeoutMs == nil {
		v := defaultLookupTimeoutMs
		cb.Spec.Integration.LookupTimeoutMs = &v
	}
	if cb.Spec.Integration.MinimumPrefixTokens == nil {
		v := defaultMinimumPrefixTokens
		cb.Spec.Integration.MinimumPrefixTokens = &v
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
// The check is bypassed only when Spec.Type is empty: there is no
// defaulting for type and the missing-type rejection is owned by
// CRD-level / future field-level validation; piling an "adapter for
// backend=\"\"" cause on top would not help the user.
func (v *CacheBackendValidator) checkRuntimeAdapter(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Type == "" {
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

// rejectInvalidExternalEndpointScheme rejects an External CacheBackend
// whose spec.endpoint carries a scheme other than `lm://`. The vLLM
// External adapter renders the LMCache engine wire (LMCACHE_REMOTE_URL),
// and the helper that builds the URL prepends `lm://` to any value that
// doesn't already carry it — so a `https://...` endpoint would become
// `LMCACHE_REMOTE_URL=lm://https://...` at injection time, which the
// engine connector rejects at runtime. Catch the misconfiguration loudly
// at write time instead of leaving the operator to discover it from
// engine-pod crash logs.
//
// Allowed forms:
//   - bare `host[:port]` (the canonical shape — the helper adds the
//     `lm://` scheme on injection)
//   - `lm://host[:port]` (operators who prefer to be explicit)
//
// Path components are also rejected — LMCache is a TCP-level protocol
// and a path would be silently dropped by the connector. Empty
// endpoint is left to [requireEndpointForExternal]; non-External types
// are left to [rejectEndpointOnNonExternal].
//
// A future SGLang-shaped External adapter (different engine wire) will
// have its own scheme rules; this rule narrows on
// `Type == External` only because the vLLM wire is the only one we
// ship today.
func rejectInvalidExternalEndpointScheme(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Type != cachev1alpha1.CacheBackendTypeExternal {
		return nil
	}
	raw := strings.TrimSpace(cb.Spec.Endpoint)
	if raw == "" {
		return nil // requireEndpointForExternal handles this
	}
	// Parse the optional scheme. We deliberately do NOT use net/url
	// here: net/url.Parse treats a bare `host:port` as having scheme=
	// "host" because it parses everything before the first `:` as a
	// scheme. Hand-roll the scheme check on the leading `://` separator
	// instead.
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme := strings.ToLower(raw[:i])
		rest := raw[i+3:]
		if scheme != "lm" {
			return field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "endpoint"),
					cb.Spec.Endpoint,
					fmt.Sprintf("spec.endpoint scheme %q is not supported for spec.type=External; use a bare host[:port] (the LMCache adapter adds the lm:// scheme) or an explicit lm:// URL — the vLLM engine wire injects LMCACHE_REMOTE_URL and would otherwise concatenate to an invalid value", scheme),
				),
			}
		}
		// scheme=lm; rest must not carry a path/query.
		if strings.ContainsAny(rest, "/?#") {
			return field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "endpoint"),
					cb.Spec.Endpoint,
					"spec.endpoint must not include a path, query, or fragment; LMCache is a TCP-level protocol and would silently drop them — use host[:port] only",
				),
			}
		}
		return nil
	}
	// No scheme — must be a bare host[:port], so reject path-like
	// payloads. Port presence is not enforced (lmcache_server defaults
	// to 65432, but an operator pre-flighting a service mesh may bind
	// elsewhere).
	if strings.ContainsAny(raw, "/?#") {
		return field.ErrorList{
			field.Invalid(
				field.NewPath("spec", "endpoint"),
				cb.Spec.Endpoint,
				"spec.endpoint must be a bare host[:port] (optionally prefixed lm://); paths/queries/fragments are not part of the LMCache wire and would be silently dropped",
			),
		}
	}
	return nil
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
