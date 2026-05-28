package v1alpha1

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
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
// memory-only backend, cross-namespace endpoints without explicit opt-in —
// before the reconciler ever sees them. It implements
// [admission.Validator] over CacheBackend.
//
// The rule set is ordered and pluggable: new admission-time guards (the
// next one will be the M6 runtime-adapter compatibility check) plug in by
// appending to [CacheBackendValidator.Rules], not by editing this type.
// A nil/empty Rules slice falls back to [DefaultValidationRules].
type CacheBackendValidator struct {
	// Rules is the ordered list of admission-time checks the validator
	// applies to every admitted CacheBackend. When nil/empty,
	// [DefaultValidationRules] is used.
	Rules []ValidationRule
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
func SetupCacheBackendWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &cachev1alpha1.CacheBackend{}).
		WithDefaulter(&CacheBackendDefaulter{}).
		WithValidator(&CacheBackendValidator{}).
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
// here so create + update share one code path.
func (v *CacheBackendValidator) validate(cb *cachev1alpha1.CacheBackend) error {
	rules := v.Rules
	if len(rules) == 0 {
		rules = DefaultValidationRules
	}
	var errs field.ErrorList
	for _, rule := range rules {
		errs = append(errs, rule(cb)...)
	}
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(
		schema.GroupKind{Group: cachev1alpha1.GroupVersion.Group, Kind: "CacheBackend"},
		cb.Name,
		errs,
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
