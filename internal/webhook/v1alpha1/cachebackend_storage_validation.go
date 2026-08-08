// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"strings"
)

func rejectNonPositiveHostMemoryCapacity(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.LMCache == nil || cb.Spec.LMCache.HostMemory == nil ||
		cb.Spec.LMCache.HostMemory.Capacity == nil ||
		cb.Spec.LMCache.HostMemory.Capacity.Sign() > 0 {
		return nil
	}

	return field.ErrorList{
		field.Invalid(
			field.NewPath("spec", "lmCache", "hostMemory", "capacity"),
			cb.Spec.LMCache.HostMemory.Capacity.String(),
			"must be greater than zero",
		),
	}
}

func selectedProviderResources(cb *cachev1alpha1.CacheBackend) (*corev1.ResourceRequirements, *field.Path) {
	if cb == nil || cb.Spec.RemoteStorage == nil {
		return nil, nil
	}

	storagePath := field.NewPath("spec", "remoteStorage")
	switch storage := cb.Spec.RemoteStorage; storage.Provider {
	case cachev1alpha1.CacheBackendRemoteStorageProviderRedis:
		if storage.Redis != nil {
			return storage.Redis.Resources, storagePath.Child("redis", "resources")
		}
	case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
		if storage.LMCacheServer != nil {
			return storage.LMCacheServer.Resources, storagePath.Child("lmCacheServer", "resources")
		}
	case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
		if storage.Mooncake != nil {
			return storage.Mooncake.Resources, storagePath.Child("mooncake", "resources")
		}
	}
	return nil, nil
}

func validateCacheHierarchy(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if cb.Spec.RemoteStorage == nil && cb.Spec.Autoscaling != nil {
		errs = append(errs, field.Forbidden(
			specPath.Child("autoscaling"),
			"host-only backends omit spec.remoteStorage and provision no provider workload, so there is nothing to autoscale",
		))
	}

	if cb.Spec.LMCache != nil && cb.Spec.EffectiveCacheType() != cachev1alpha1.CacheBackendTypeLMCache {
		errs = append(errs, field.Forbidden(
			specPath.Child("lmCache"),
			"spec.lmCache is valid only when spec.type=LMCache",
		))
	}

	storage := cb.Spec.RemoteStorage
	if storage == nil {
		return errs
	}
	storagePath := specPath.Child("remoteStorage")
	if storage.Provider == "" {
		errs = append(errs, field.Required(storagePath.Child("provider"), "select a remote-storage provider"))
	}
	if storage.Ownership == "" {
		errs = append(errs, field.Required(storagePath.Child("ownership"), "select Managed or External ownership"))
	}
	switch storage.Ownership {
	case cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged:
		if strings.TrimSpace(storage.Endpoint) != "" {
			errs = append(errs, field.Forbidden(storagePath.Child("endpoint"),
				"managed providers publish their observed endpoint in status.endpoint"))
		}
	case cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal:
		if strings.TrimSpace(storage.Endpoint) == "" {
			errs = append(errs, field.Required(storagePath.Child("endpoint"),
				"required when remoteStorage.ownership=External"))
		} else if err := backendadapter.ValidateExternalEndpoint(storage.Provider, storage.Endpoint); err != nil {
			errs = append(errs, field.Invalid(storagePath.Child("endpoint"), storage.Endpoint, err.Error()))
		}
	}

	type providerConfig struct {
		provider cachev1alpha1.CacheBackendRemoteStorageProvider
		set      bool
		path     *field.Path
	}
	configs := []providerConfig{
		{cachev1alpha1.CacheBackendRemoteStorageProviderRedis, storage.Redis != nil, storagePath.Child("redis")},
		{cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, storage.LMCacheServer != nil, storagePath.Child("lmCacheServer")},
		{cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, storage.Mooncake != nil, storagePath.Child("mooncake")},
	}
	for _, config := range configs {
		if config.set && storage.Provider != config.provider {
			errs = append(errs, field.Forbidden(config.path,
				fmt.Sprintf("configuration belongs to provider %s, but remoteStorage.provider=%s", config.provider, storage.Provider)))
		}
		if config.set && storage.Ownership == cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal {
			errs = append(errs, field.Forbidden(config.path,
				"provider workload configuration is valid only with Managed ownership"))
		}
	}
	if storage.Ownership == cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged {
		switch storage.Provider {
		case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
			if storage.LMCacheServer != nil {
				errs = append(errs, validateManagedProviderCommand(
					storagePath.Child("lmCacheServer", "command"),
					storage.LMCacheServer.Command,
				)...)
			}
		case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
			if storage.Mooncake != nil {
				errs = append(errs, validateManagedProviderCommand(
					storagePath.Child("mooncake", "command"),
					storage.Mooncake.Command,
				)...)
			}
		}
	}

	return errs
}

func validateManagedProviderCommand(path *field.Path, command []string) field.ErrorList {
	if command == nil {
		return nil
	}
	if len(command) == 0 {
		return field.ErrorList{field.Invalid(path, command, "must contain an executable")}
	}

	var errs field.ErrorList
	for i, part := range command {
		if strings.TrimSpace(part) == "" {
			errs = append(errs, field.Invalid(path.Index(i), part, "must not be empty"))
		}
	}
	return errs
}

// rejectCrossNamespaceEndpointWithoutOptIn rejects an external endpoint that
// resolves into a Service in a namespace other than the CacheBackend's
// own, unless spec.allowCrossNamespace is true. Crossing a namespace is
// a tenancy boundary the operator should explicitly acknowledge; the
// rule covers spec.remoteStorage.endpoint and fires only when the endpoint is
// a recognisable in-cluster Service DNS. External hostnames and IPs pass through because they expose no
// namespace to compare against.
func rejectCrossNamespaceEndpointWithoutOptIn(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.RemoteStorage == nil {
		return nil
	}
	endpoint := cb.Spec.RemoteStorage.Endpoint
	endpointPath := field.NewPath("spec", "remoteStorage", "endpoint")

	ns, ok := serviceDNSNamespace(endpoint)
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
			endpointPath,
			fmt.Sprintf("%s references namespace %q but CacheBackend is in namespace %q; "+
				"set spec.allowCrossNamespace=true to opt in to the cross-namespace reference",
				endpointPath.String(), ns, cb.Namespace),
		),
	}
}

// rejectResourceLimitsBelowRequests rejects the selected provider resource
// block when the request/limit relationship is invalid for the named resource. K8s
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
	resources, resourcesPath := selectedProviderResources(cb)
	if resources == nil {
		return nil
	}
	var errs field.ErrorList
	for name, req := range resources.Requests {
		lim, ok := resources.Limits[name]
		if !ok {
			continue
		}
		path := resourcesPath.Child("limits").Key(string(name))
		if isOvercommittableResource(name) {
			if lim.Cmp(req) >= 0 {
				continue
			}
			errs = append(errs, field.Invalid(
				path,
				lim.String(),
				fmt.Sprintf("must be greater than or equal to %s[%s] (%s)", resourcesPath.Child("requests"), name, req.String()),
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
			fmt.Sprintf("must equal %s[%s] (%s) — %q is a non-overcommittable resource (hugepages and extended resources require request == limit)", resourcesPath.Child("requests"), name, req.String(), name),
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
	resources, resourcesPath := selectedProviderResources(cb)
	if resources == nil {
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
					resourcesPath.Child(kind).Key(s),
					qty.String(),
					fmt.Sprintf("must be a multiple of the page size %s — the Linux kernel allocates hugepages in whole-page chunks", suffix),
				))
			}
		}
	}
	check(resources.Requests, "requests")
	check(resources.Limits, "limits")
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
	resources, resourcesPath := selectedProviderResources(cb)
	if resources == nil {
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
					resourcesPath.Child(kind).Key(string(name)),
					qty.String(),
					fmt.Sprintf("%q is an extended resource and must be an integer quantity — K8s allocates extended resources by whole units", name),
				))
			}
		}
	}
	check(resources.Requests, "requests")
	check(resources.Limits, "limits")
	return errs
}

// rejectInvalidResourceNames rejects any selected provider resources.requests or
// resources.limits key that fails the K8s container-resource-name
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
	resources, resourcesPath := selectedProviderResources(cb)
	if resources == nil {
		return nil
	}
	var errs field.ErrorList
	check := func(list corev1.ResourceList, kind string) {
		for name := range list {
			if msg, ok := validateContainerResourceName(name); !ok {
				errs = append(errs, field.Invalid(
					resourcesPath.Child(kind).Key(string(name)),
					string(name),
					msg,
				))
			}
		}
	}
	check(resources.Requests, "requests")
	check(resources.Limits, "limits")
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
// quantity in the selected provider resources.requests or resources.limits. The
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
	resources, resourcesPath := selectedProviderResources(cb)
	if resources == nil {
		return nil
	}
	var errs field.ErrorList
	check := func(list corev1.ResourceList, kind string) {
		for name, qty := range list {
			if qty.Sign() >= 0 {
				continue
			}
			errs = append(errs, field.Invalid(
				resourcesPath.Child(kind).Key(string(name)),
				qty.String(),
				"must be a non-negative quantity",
			))
		}
	}
	check(resources.Requests, "requests")
	check(resources.Limits, "limits")
	return errs
}

// rejectRequestsOnlyForNonOvercommittableResources rejects a non-
// overcommittable resource (hugepages-*, vendor-prefixed extended
// resource) declared in the selected provider `resources.requests` without a
// matching entry in `resources.limits`. K8s requires both halves for
// non-overcommittable resources — the kubelet allocates whole pages
// or devices, so the request and limit must be declared together and
// be equal. Limits-only IS admitted by K8s (the apiserver auto-
// populates requests from limits when only limits is set), so the
// rule fires only on the requests-only direction. Overcommittable
// resources (cpu, memory, ephemeral-storage) are unaffected — a
// requests-only cpu / memory shape is the canonical kubelet "no upper
// bound" pattern.
func rejectRequestsOnlyForNonOvercommittableResources(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	resources, resourcesPath := selectedProviderResources(cb)
	if resources == nil {
		return nil
	}
	var errs field.ErrorList
	for name := range resources.Requests {
		if isOvercommittableResource(name) {
			continue
		}
		if _, ok := resources.Limits[name]; ok {
			continue
		}
		qty := resources.Requests[name]
		errs = append(errs, field.Invalid(
			resourcesPath.Child("requests").Key(string(name)),
			qty.String(),
			fmt.Sprintf("%q is a non-overcommittable resource — it must also be set in %s with the same value (hugepages and extended resources require requests and limits to be declared together)", name, resourcesPath.Child("limits")),
		))
	}
	return errs
}

// rejectResourceClaims rejects a non-empty selected provider resources.claims slice.
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
	resources, resourcesPath := selectedProviderResources(cb)
	if resources == nil || len(resources.Claims) == 0 {
		return nil
	}
	return field.ErrorList{
		field.Forbidden(
			resourcesPath.Child("claims"),
			resourcesPath.Child("claims").String()+" is not supported in v1alpha1: the runtime adapter does not plumb pod.spec.resourceClaims, so a claim-bound container.resources.claims would render a pod the apiserver rejects",
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
