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
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
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
	if cb.Spec.Autoscaling != nil {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "autoscaling"),
			"SGLangHiCache is engine-local and has no backend workload to autoscale",
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

// rejectUnsupportedSGLangRole rejects a non-ReadWrite spec.integration.role on a
// (sglang, LMCache) backend. SGLang's --enable-lmcache integration has no
// kv_role split equivalent to vLLM's LMCache connector — it always both stores
// and retrieves — so a ReadOnly / WriteOnly role cannot be honored by the
// engine. Rejecting at admission makes that loud rather than silently treating
// the role as ReadWrite. Scoped to (sglang, LMCache): other engines map all
// roles onto their connector (vLLM), and the rule must not fire on an
// already-unsupported pair (e.g. sglang+External, which checkRuntimeAdapter
// rejects on its own). If SGLang's LMCache integration gains a
// producer/consumer split, lift this rule.
func rejectUnsupportedSGLangRole(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Integration == nil {
		return nil
	}
	role := cb.Spec.Integration.Role
	if role == "" || role == cachev1alpha1.CacheBackendIntegrationRoleReadWrite {
		return nil // unset defaults to ReadWrite; ReadWrite is honored
	}
	if adapterruntime.ResolveRuntimeID(cb) != adapterruntime.RuntimeSGLang ||
		cb.Spec.Type != cachev1alpha1.CacheBackendTypeLMCache {
		return nil
	}
	return field.ErrorList{
		field.Invalid(
			field.NewPath("spec", "integration", "role"),
			role,
			fmt.Sprintf("the sglang engine's LMCache integration has no producer/consumer split, so only %q (the default) is honored today; %q / %q are not yet wired for SGLang",
				cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
				cachev1alpha1.CacheBackendIntegrationRoleReadOnly,
				cachev1alpha1.CacheBackendIntegrationRoleWriteOnly),
		),
	}
}

// rejectSGLangRedisL2ScaleOut hard-rejects a multi-replica or autoscaled
// (sglang, LMCache) backend. That pair's managed cache-server is a single plain
// Redis L2 store (the SGLang MP worker's --l2-adapter target), and a plain Redis is
// not clustered: a second pod behind the one ClusterIP Service shards the keyspace
// across independent instances, so a key stored via one is a miss via the other and
// the L2 silently partitions. The failure looks like a healthy backend with a poor
// hit rate, so reject at the door rather than warn — the same posture as
// rejectMooncakeMasterScaleOut (a different singleton for a different reason).
//
// Scoped to (sglang, LMCache): vLLM's lm:// server is an ordinary pod-network
// workload that scales, and other pairs are rejected on their own
// (checkRuntimeAdapter). spec.replicas 0 (disabled) and 1 (the singleton) remain
// valid, as is EventsOnly (which provisions no server at all — see the guard).
// The reconciler's clampSingletonReplicas is the backstop for grandfathered
// objects. If SGLang's shared tier gains a clustered store, lift this rule.
func rejectSGLangRedisL2ScaleOut(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	storage := cb.Spec.EffectiveRemoteStorage()
	if adapterruntime.ResolveRuntimeID(cb) != adapterruntime.RuntimeSGLang ||
		cb.Spec.EffectiveCacheType() != cachev1alpha1.CacheBackendTypeLMCache ||
		storage == nil ||
		storage.Provider != cachev1alpha1.CacheBackendRemoteStorageProviderRedis ||
		storage.Ownership != cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged {
		return nil
	}
	// EventsOnly provisions NO cache server at all (the reconciler sheds any owned
	// workload and wires only the kvevent-subscriber sidecar), so there is no Redis
	// L2 to partition and nothing this rule protects. Rejecting scale-out here would
	// be both factually wrong — the message explains a Redis split that cannot happen
	// — and gratuitously stricter than the otherwise-identical (vllm, LMCache)
	// events-only backend. The rule applies to the Offload path, which is what
	// renders the singleton Redis.
	if cb.Spec.IsEventsOnly() {
		return nil
	}
	var errs field.ErrorList
	if cb.Spec.Replicas != nil && *cb.Spec.Replicas > 1 {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "replicas"), *cb.Spec.Replicas,
			"the (sglang, LMCache) backend's Redis L2 store is a single non-clustered instance: a second replica behind the one Service shards the keyspace and silently partitions the cache. Set spec.replicas to 0 or 1.",
		))
	}
	if cb.Spec.Autoscaling != nil {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "autoscaling"), cb.Spec.Autoscaling,
			"spec.autoscaling is not supported for the (sglang, LMCache) backend: its Redis L2 store is a single non-clustered instance, so scaling it out partitions the cache across independent keyspaces. Remove spec.autoscaling.",
		))
	}
	return errs
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

// mooncakeEngineHostNetworkWarning names the one thing that still stands between a
// Mooncake CacheBackend and working KV transfer. The adapter provisions the master
// correctly (hostNetwork behind a headless Service), but Mooncake's transfer engine
// is a peer-to-peer mesh: the ENGINE pods must run with host networking too. That
// move rewrites a pod the operator owns and hostNetwork is a privilege, so it is
// opt-in via spec.integration.engineHostNetwork rather than injected by default.
// Until the operator opts in, the backend reconciles Ready and moves zero KV — a
// silent failure. Say so out loud at apply time, pointing at the exact field,
// rather than letting them discover it from a flat cache-hit graph.
//
// The text stays within [maxWarningLen]: the API conventions ask for concise
// warnings so clients render them reliably, and a truncated warning is exactly the
// silent failure this exists to prevent. The field and its consequence live here;
// the full rationale (the mesh, node ports, Pod Security ordering) lives in
// docs/design/cachebackend-api.md.
const mooncakeEngineHostNetworkWarning = "Mooncake: set spec.integration.engineHostNetwork=true or engine pods can't join the transfer mesh (Ready, no KV)"

// maxWarningLen is the concise-warning budget from the Kubernetes API conventions.
// Longer text risks truncation or being dropped by clients.
const maxWarningLen = 120

func warnMooncakeEngineHostNetwork(cb *cachev1alpha1.CacheBackend) admission.Warnings {
	if !usesMooncakeStorage(cb) {
		return nil
	}
	if enginebinding.EngineHostNetworkRequested(cb) {
		// Opted in: the pod webhook moves engine pods onto the host network, so the
		// data plane is complete and there is nothing left to warn about.
		return nil
	}
	return admission.Warnings{mooncakeEngineHostNetworkWarning}
}

// rejectEngineHostNetworkOnBackendThatDoesNotNeedIt keeps
// spec.integration.engineHostNetwork from sitting inert. Only Mooncake's
// peer-to-peer transfer engine needs engine pods on the host network; on any other
// backend the flag silently does nothing while leaving the operator convinced they
// changed the pod's networking. hostNetwork is a privilege — a no-op that *looks*
// like it granted one is worse than a rejection, so reject at the door.
func rejectEngineHostNetworkOnBackendThatDoesNotNeedIt(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if !enginebinding.EngineHostNetworkRequested(cb) ||
		usesMooncakeStorage(cb) {
		return nil
	}
	provider := "none (host-only)"
	if storage := cb.Spec.EffectiveRemoteStorage(); storage != nil {
		provider = string(storage.Provider)
	}
	return field.ErrorList{field.Invalid(
		field.NewPath("spec", "integration", "engineHostNetwork"), true,
		fmt.Sprintf("spec.integration.engineHostNetwork is only meaningful when the effective remote storage provider is Mooncake, whose transfer engine dials engine pods "+
			"on real node IPs; provider=%s does not need it and the flag would do nothing. Remove it.", provider),
	)}
}

func usesMooncakeStorage(cb *cachev1alpha1.CacheBackend) bool {
	if cb == nil {
		return false
	}
	storage := cb.Spec.EffectiveRemoteStorage()
	return storage != nil && storage.Provider == cachev1alpha1.CacheBackendRemoteStorageProviderMooncake
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
	// A declared LMCache topology uses the Phase-1 MP support matrix validated
	// by validateLMCacheTopology. The currently shipping runtime adapters still
	// describe the legacy data plane (vLLM IP and the SGLang-specific MP spike),
	// so consulting SupportsBinding here would incorrectly reject the final
	// vLLM+Redis contract before the shared PodLocal renderer lands in Phases
	// 2-4. Pod admission remains fail-open until the matching MP adapter is
	// implemented; this exception is removed when both adapters expose the final
	// binding capabilities.
	if cb.Spec.LMCache != nil && cb.Spec.LMCache.Topology != "" {
		return nil
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
//   - spec.autoscaling has no workload to scale — the controller deploys
//     nothing for an events-only backend.
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
	if cb.Spec.Autoscaling != nil {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "autoscaling"),
			fmt.Sprintf("events-only backends (spec.integration.mode=%q) provision no server workload, so there is nothing to autoscale",
				cachev1alpha1.CacheBackendIntegrationModeEventsOnly),
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

// rejectMooncakeMasterScaleOut hard-rejects a multi-replica or autoscaled Mooncake
// backend. The Mooncake master is a SINGLETON coordinator that the adapter runs on
// the host network, so a second replica has no good outcome. Co-scheduled, it
// cannot serve: it fails to bind ports the first master already holds on that node
// (in practice the scheduler rejects it earlier still, because the API server
// defaults hostPort=containerPort for hostNetwork pods and the NodePorts predicate
// then trips). Scheduled elsewhere, it comes up as an INDEPENDENT master and
// silently splits the store in two. Both failures land long after admission and
// look like a healthy backend, so reject at the door rather than warn — the same
// posture as the other cross-field invariants here.
//
// spec.replicas 0 (disabled) and 1 (the singleton) remain valid. type=LMCache is
// unaffected: its lm:// server is an ordinary pod-network workload that scales.
func rejectMooncakeMasterScaleOut(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	storage := cb.Spec.EffectiveRemoteStorage()
	if storage == nil ||
		storage.Provider != cachev1alpha1.CacheBackendRemoteStorageProviderMooncake ||
		storage.Ownership != cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged {
		return nil
	}
	var errs field.ErrorList
	if cb.Spec.Replicas != nil && *cb.Spec.Replicas > 1 {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "replicas"), *cb.Spec.Replicas,
			"the Mooncake master is a singleton on the host network: a second replica cannot bind the node ports the first already holds, "+
				"and on a different node it becomes an independent master that silently splits the store. Set spec.replicas to 0 or 1.",
		))
	}
	if cb.Spec.Autoscaling != nil {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "autoscaling"), cb.Spec.Autoscaling,
			"spec.autoscaling is not supported for remoteStorage.provider=Mooncake: the master is a singleton on the host network, so scaling it out either cannot bind "+
				"the node's ports or splits the store across independent masters. Remove spec.autoscaling.",
		))
	}
	return errs
}
