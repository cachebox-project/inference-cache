// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"errors"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"math"
	"sort"
	"strconv"
	"strings"
)

func validateSGLangHiCache(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	hiCachePath := field.NewPath("spec", "hiCache")
	if cb.Spec.Type != cachev1alpha1.CacheBackendTypeSGLangHiCache {
		if cb.Spec.HiCache != nil {
			return field.ErrorList{field.Forbidden(
				hiCachePath,
				fmt.Sprintf("spec.hiCache is only valid when spec.type=%q", cachev1alpha1.CacheBackendTypeSGLangHiCache),
			)}
		}
		return nil
	}

	var errs field.ErrorList
	if cb.Spec.HiCache == nil {
		errs = append(errs, field.Required(hiCachePath,
			fmt.Sprintf("required when spec.type=%q", cachev1alpha1.CacheBackendTypeSGLangHiCache)))
	} else {
		spec := cb.Spec.HiCache
		sizePath := hiCachePath.Child("sizeGB")
		ratioPath := hiCachePath.Child("ratio")
		switch {
		case spec.SizeGB == nil && spec.Ratio == "":
			errs = append(errs, field.Required(hiCachePath,
				"exactly one of sizeGB and ratio must be set"))
		case spec.SizeGB != nil && spec.Ratio != "":
			errs = append(errs, field.Invalid(hiCachePath, spec,
				"sizeGB and ratio are mutually exclusive"))
		}
		if spec.SizeGB != nil && *spec.SizeGB < 1 {
			errs = append(errs, field.Invalid(sizePath, *spec.SizeGB, "must be at least 1"))
		}
		if spec.Ratio != "" {
			ratio, err := strconv.ParseFloat(spec.Ratio, 64)
			if err != nil || ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
				errs = append(errs, field.Invalid(ratioPath, spec.Ratio,
					"must be a finite number greater than zero"))
			}
		}
		if !validHiCacheWritePolicy(spec.WritePolicy) {
			errs = append(errs, field.NotSupported(
				hiCachePath.Child("writePolicy"), spec.WritePolicy,
				[]string{
					string(cachev1alpha1.SGLangHiCacheWriteBack),
					string(cachev1alpha1.SGLangHiCacheWriteThrough),
					string(cachev1alpha1.SGLangHiCacheWriteThroughSelective),
				},
			))
		}
		if !validHiCacheIOBackend(spec.IOBackend) {
			errs = append(errs, field.NotSupported(
				hiCachePath.Child("ioBackend"), spec.IOBackend,
				[]string{
					string(cachev1alpha1.SGLangHiCacheIODirect),
					string(cachev1alpha1.SGLangHiCacheIOKernel),
					string(cachev1alpha1.SGLangHiCacheIOKernelAscend),
				},
			))
		}
		if !validHiCacheMemoryLayout(spec.MemoryLayout) {
			errs = append(errs, field.NotSupported(
				hiCachePath.Child("memoryLayout"), spec.MemoryLayout,
				[]string{
					string(cachev1alpha1.SGLangHiCacheMemoryLayerFirst),
					string(cachev1alpha1.SGLangHiCacheMemoryPageFirst),
					string(cachev1alpha1.SGLangHiCacheMemoryPageFirstDirect),
					string(cachev1alpha1.SGLangHiCacheMemoryPageFirstKVSplit),
					string(cachev1alpha1.SGLangHiCacheMemoryPageHead),
				},
			))
		}
	}

	if adapterruntime.ResolveRuntimeID(cb) != adapterruntime.RuntimeSGLang {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "runtime"), cb.Spec.Runtime,
			"SGLangHiCache requires runtime=SGLang",
		))
	}
	if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
		errs = append(errs, field.Required(
			field.NewPath("spec", "engineSelector", "matchLabels"),
			"SGLangHiCache must select the engine Pods to inject",
		))
	}
	if cachev1alpha1.IntegrationMode(cb.Spec.Integration) != cachev1alpha1.CacheBackendIntegrationModeOffload {
		errs = append(errs, field.NotSupported(
			field.NewPath("spec", "integration", "mode"),
			cb.Spec.Integration.Mode,
			[]string{string(cachev1alpha1.CacheBackendIntegrationModeOffload)},
		))
	}
	if cb.Spec.Integration != nil {
		role := cb.Spec.Integration.Role
		if role != "" && role != cachev1alpha1.CacheBackendIntegrationRoleReadWrite {
			errs = append(errs, field.NotSupported(
				field.NewPath("spec", "integration", "role"),
				role,
				[]string{string(cachev1alpha1.CacheBackendIntegrationRoleReadWrite)},
			))
		}
		if !cachev1alpha1.IntegrationFailOpen(cb.Spec.Integration) {
			errs = append(errs, field.NotSupported(
				field.NewPath("spec", "integration", "failOpen"),
				false,
				[]string{"true"},
			))
		}
	}
	return errs
}

func validHiCacheWritePolicy(value cachev1alpha1.SGLangHiCacheWritePolicy) bool {
	switch value {
	case "",
		cachev1alpha1.SGLangHiCacheWriteBack,
		cachev1alpha1.SGLangHiCacheWriteThrough,
		cachev1alpha1.SGLangHiCacheWriteThroughSelective:
		return true
	default:
		return false
	}
}

func validHiCacheIOBackend(value cachev1alpha1.SGLangHiCacheIOBackend) bool {
	switch value {
	case "",
		cachev1alpha1.SGLangHiCacheIODirect,
		cachev1alpha1.SGLangHiCacheIOKernel,
		cachev1alpha1.SGLangHiCacheIOKernelAscend:
		return true
	default:
		return false
	}
}

func validHiCacheMemoryLayout(value cachev1alpha1.SGLangHiCacheMemoryLayout) bool {
	switch value {
	case "",
		cachev1alpha1.SGLangHiCacheMemoryLayerFirst,
		cachev1alpha1.SGLangHiCacheMemoryPageFirst,
		cachev1alpha1.SGLangHiCacheMemoryPageFirstDirect,
		cachev1alpha1.SGLangHiCacheMemoryPageFirstKVSplit,
		cachev1alpha1.SGLangHiCacheMemoryPageHead:
		return true
	default:
		return false
	}
}

// rejectUnsupportedLMCacheRole rejects a non-ReadWrite spec.integration.role
// on every LMCache backend. SGLang's integration has no producer/consumer
// split. vLLM accepts kv_consumer/kv_producer, but the validated LMCache 0.5.3
// MP connector still stores as a consumer and retrieves as a producer. Until a
// pinned connector enforces directionality and passes GPU validation, accepting
// ReadOnly or WriteOnly would expose an API promise the data plane does not
// honor. Keep the rule scoped to LMCache so other backend implementations can
// define and validate their own role semantics independently.
func rejectUnsupportedLMCacheRole(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Integration == nil {
		return nil
	}
	role := cb.Spec.Integration.Role
	if role == "" || role == cachev1alpha1.CacheBackendIntegrationRoleReadWrite {
		return nil // unset defaults to ReadWrite; ReadWrite is honored
	}
	if cb.Spec.Type != cachev1alpha1.CacheBackendTypeLMCache {
		return nil
	}
	return field.ErrorList{
		field.Invalid(
			field.NewPath("spec", "integration", "role"),
			role,
			fmt.Sprintf("LMCache integrations currently support only %q (the default); %q / %q are rejected until a validated connector enforces directional cache access",
				cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
				cachev1alpha1.CacheBackendIntegrationRoleReadOnly,
				cachev1alpha1.CacheBackendIntegrationRoleWriteOnly),
		),
	}
}

// rejectInvalidKernelCheckAnnotation rejects an unrecognized value for the
// inferencecache.io/lmcache-kernel-check annotation. The annotation is the
// operator's opt-in surface for the engine-side kernel check (auto /
// report-only / strict / off); a typo like "strcit" would otherwise fall back
// to "auto" and silently relax a fail-closed strict gate to report-only
// observability, with no signal to the operator. Validate it at admission
// instead. An unset annotation (or an explicit empty value) is accepted.
func rejectInvalidKernelCheckAnnotation(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	v, ok := cb.Annotations[enginebinding.AnnotationLMCacheKernelCheck]
	if !ok || enginebinding.IsValidKernelCheckMode(v) {
		return nil
	}
	return field.ErrorList{
		field.Invalid(
			field.NewPath("metadata", "annotations").Key(enginebinding.AnnotationLMCacheKernelCheck),
			v,
			fmt.Sprintf("must be one of %q, %q, %q, %q (or unset)",
				enginebinding.KernelCheckModeAuto, enginebinding.KernelCheckModeReportOnly,
				enginebinding.KernelCheckModeStrict, enginebinding.KernelCheckModeOff),
		),
	}
}

// checkRuntimeAdapter rejects a CacheBackend whose (runtime, type) pair no
// installed runtime adapter supports. The runtime is
// resolved through [adapterruntime.ResolveRuntimeID] — the same helper the
// reconciler and pod-mutating webhook consult — so admission, reconcile,
// and pod injection agree on which adapter the registry should pick. In
// particular, an unset runtime remains empty and cannot match an adapter;
// persisted resources cannot reach that state because the CRD schema requires
// spec.runtime. Remote-storage provider and ownership do not affect runtime
// adapter selection; they are validated independently as bindings.
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
		return field.ErrorList{field.InternalError(
			field.NewPath("spec", "runtime"),
			fmt.Errorf("runtime adapter registry is not configured"),
		)}
	}
	runtimeID := adapterruntime.ResolveRuntimeID(cb)
	adapter, err := registry.Select(runtimeID, cb)
	if err != nil {
		if !errors.Is(err, adapterruntime.ErrNoAdapter) {
			// Registry currently only returns ErrNoAdapter from Select; a
			// future error class should surface as-is rather than be
			// rewritten as an unsupported-pair message.
			return field.ErrorList{
				field.InternalError(field.NewPath("spec", "runtime"), err),
			}
		}
		return field.ErrorList{
			field.Invalid(
				field.NewPath("spec", "runtime"),
				cb.Spec.Runtime,
				unsupportedPairMessage(runtimeID, cb.Spec.Type, registry),
			),
		}
	}
	storage := cb.Spec.EffectiveRemoteStorage()
	protocol, err := backendadapter.ProtocolFor(storage)
	if err != nil {
		return field.ErrorList{field.Invalid(
			field.NewPath("spec", "remoteStorage", "provider"),
			storage.Provider,
			err.Error(),
		)}
	}
	binding := backendadapter.BindingFor(storage, protocol, "")
	if !adapter.SupportsBinding(binding) {
		storagePath := field.NewPath("spec", "remoteStorage")
		var rejectedValue any
		if storage != nil {
			storagePath = storagePath.Child("provider")
			rejectedValue = storage.Provider
		}
		return field.ErrorList{field.Invalid(
			storagePath,
			rejectedValue,
			fmt.Sprintf("runtime %s with cache type %s does not accept remote-storage protocol %q",
				runtimeID, cb.Spec.EffectiveCacheType(), protocol),
		)}
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

// rejectEventsOnlyMisconfiguration enforces the constraints of the events-only
// (tier-1 routing) integration mode. EventsOnly provisions no backend server
// and wires no KV connector, so server-shaped configuration is structurally
// meaningless:
//
//   - spec.type must be LMCache (the default), whose adapter supplies the
//     kvevent-subscriber the routing tier needs.
//   - spec.remoteStorage requests an offload provider that the controller
//     deliberately removes in events-only mode.
//
// spec.remoteStorage.endpoint is already forbidden for managed ownership by
// validateCacheHierarchy, so it needs no events-only-specific check.
func rejectEventsOnlyMisconfiguration(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if !cb.Spec.IsEventsOnly() {
		return nil
	}
	var errs field.ErrorList
	switch cb.Spec.Type {
	case "", cachev1alpha1.CacheBackendTypeLMCache:
		// LMCache is the supported events-only managed type; an empty type
		// defaults to LMCache via the CRD marker, so both are allowed.
	default:
		// Any other engine-cache type cannot supply the vLLM event stream
		// contract this mode currently implements.
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "integration", "mode"),
			fmt.Sprintf("mode %q is only supported with spec.type %q; got spec.type %q. Events-only wires no KV connector",
				cachev1alpha1.CacheBackendIntegrationModeEventsOnly, cachev1alpha1.CacheBackendTypeLMCache, cb.Spec.Type),
		))
	}
	if cb.Spec.RemoteStorage != nil {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "remoteStorage"),
			fmt.Sprintf("events-only backends (spec.integration.mode=%q) provision no remote-storage provider; remove spec.remoteStorage",
				cachev1alpha1.CacheBackendIntegrationModeEventsOnly),
		))
	}
	return errs
}
