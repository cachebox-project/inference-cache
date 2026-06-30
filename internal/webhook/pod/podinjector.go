package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	externaladapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime/external"
	sglangadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime/sglang"
)

// WebhookPath is the URL the kubebuilder marker below registers with the
// webhook server. Exported so cmd/controller can mount the handler at the
// same path the generated MutatingWebhookConfiguration points at.
const WebhookPath = "/mutate--v1-pod"

// AnnotationSkip lets a pod opt out of injection regardless of label match.
// Set the annotation to any non-empty value on a pod that the user has
// already pre-wired (e.g. a hand-crafted reference-stack pod) so the webhook
// does not double-inject or fight a hand-tuned spec.
const AnnotationSkip = "inferencecache.io/skip-inject"

// AnnotationInjectedBy is stamped on a pod whenever the handler patched it,
// recording which CacheBackend the engine was wired to. The webhook itself
// only reads AnnotationSkip; AnnotationInjectedBy is informational, intended
// for operators inspecting `kubectl describe pod`. The handler's own
// short-circuit relies on the env vars the adapter writes, not this
// annotation, so a stripped-by-mistake annotation still does not cause a
// duplicate injection.
//
// The annotation also serves as the trigger for the downstream
// engine-pod-events controller: a watcher on Pod CREATE reads this
// annotation, looks up the named CacheBackend, and emits a Normal
// `InjectedByCacheBackend` Event on the now-persisted pod (which has a
// real UID). Recording the Event from the webhook itself isn't viable:
// the apiserver assigns metadata.uid AFTER mutating admission, so any
// event recorded here would land with involvedObject.uid="" and be
// invisible to `kubectl describe pod`.
const AnnotationInjectedBy = "inferencecache.io/injected-by"

// AnnotationInjectedByUID is stamped alongside [AnnotationInjectedBy] on every
// successful injection and carries the matched CacheBackend's metadata.uid as
// of admission time. It is the webhook-only proof-of-injection: an operator
// or attacker can copy AnnotationInjectedBy into a fresh pod template, but
// they cannot forge a value that matches the live CacheBackend's UID without
// API access to read it first (UIDs are server-assigned and not part of any
// user-authored spec). The engine-pod-events controller validates the
// injected-by/UID pair against the live CR's UID before emitting
// `InjectedByCacheBackend`, which closes the hole the
// `MutatingWebhookConfiguration.failurePolicy=Ignore` posture would otherwise
// leave open: if the webhook is unreachable at admission time, a pod can
// persist with a user-supplied AnnotationInjectedBy, but the UID annotation
// (which only the webhook writes) is absent or stale, so the controller skips
// the event.
const AnnotationInjectedByUID = "inferencecache.io/injected-by-uid"

// AnnotationInjectSkipped is stamped when the webhook intentionally skips
// injection because the operator set [AnnotationSkip]. It lets a persisted pod
// distinguish an explicit opt-out from selector drift or fail-open admission.
const AnnotationInjectSkipped = "inferencecache.io/inject-skipped"

// InjectSkippedReasonSkipAnnotation is the stable value written to
// [AnnotationInjectSkipped] when [AnnotationSkip] opts the pod out.
const InjectSkippedReasonSkipAnnotation = "skip-inject-annotation"

// +kubebuilder:webhook:path=/mutate--v1-pod,mutating=true,failurePolicy=ignore,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpod.inferencecache.io,admissionReviewVersions=v1

// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch

// EngineInjector is the admission.Handler that injects LMCache engine
// configuration into user-provided engine pods. failurePolicy=ignore on the
// MutatingWebhookConfiguration AND a fail-open posture in the handler give a
// belt-and-suspenders guarantee: even if the controller is unreachable or
// the handler returns an error response, pod admission is never blocked.
// The cache is always an optimization, never a serving dependency.
type EngineInjector struct {
	// Reader lists CacheBackends in the pod's namespace. Production wiring
	// passes the manager's APIReader (an uncached live client) — pod
	// CREATE is a one-shot injection opportunity, so a stale informer view
	// of the owning CacheBackend (in particular a status.endpoint that
	// lags reality) would leave the pod permanently unwired. Live reads
	// also avoid a cold-cache window at controller startup. Tests inject
	// a fake.NewClientBuilder()-derived reader, which also satisfies the
	// interface.
	Reader client.Reader

	// Registry resolves the runtime adapter for a (runtime, backend) pair.
	// nil falls back to [adapterruntime.DefaultRegistry] plus the External
	// and SGLang+LMCache adapters (registered explicitly because those
	// subpackages can't be imported by DefaultRegistry without a cycle).
	// Mirrors the production cmd/controller wiring so a bare `EngineInjector{}`
	// doesn't silently fail-open on External CRs that the running webhook
	// would have wired.
	Registry *adapterruntime.Registry

	// Log is the handler's logger. nil falls back to logf.FromContext at
	// call time; tests typically inject logr.Discard().
	Log logr.Logger
}

// Handle implements [admission.Handler]. Any rejection at this layer
// translates to admission.Allowed: a webhook error MUST NOT block pod
// admission (the cache is an optimization). The reason string carries
// enough context that an operator running `kubectl get events` can tell
// why a pod was admitted without injection.
func (h *EngineInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := h.logger(ctx).WithValues(
		"namespace", req.Namespace, "name", req.Name, "uid", string(req.UID),
	)

	if req.Operation == "" {
		// Defensive: not expected from a real apiserver, but a unit test
		// passing a zero Request shouldn't NPE.
		return admission.Allowed("no operation")
	}

	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		log.V(1).Info("fail-open: pod decode failed", "error", err.Error())
		return admission.Allowed(fmt.Sprintf("decode failed (fail-open): %v", err))
	}
	// admission.Request.Namespace is authoritative — the apiserver sets it
	// from the URL, before metadata.namespace is even validated. Mirror it
	// onto the decoded pod so the lookup below uses the right namespace
	// even if the inbound object's metadata.namespace is empty (the
	// apiserver defaults namespace from the URL only AFTER admission, so a
	// CREATE pod commonly arrives with metadata.namespace="").
	if pod.Namespace == "" {
		pod.Namespace = req.Namespace
	}

	if SkipAnnotationOptsOut(pod.Annotations[AnnotationSkip]) {
		return skipInjection(req, &pod)
	}

	cache, err := h.selectCacheBackend(ctx, &pod)
	if err != nil {
		log.V(1).Info("fail-open: backend lookup failed", "error", err.Error())
		return failOpen(req, &pod, fmt.Sprintf("backend lookup failed (fail-open): %v", err))
	}
	if cache == nil {
		// No CacheBackend in the namespace matches this pod. The webhook
		// matches all pods (cluster-wide) — most of them aren't engines —
		// so this is the steady-state path; log only at V(2) so it
		// doesn't drown reconciler logs.
		log.V(2).Info("no matching CacheBackend; pass-through")
		return failOpen(req, &pod, "no matching CacheBackend")
	}
	log = log.WithValues("cachebackend", cache.Namespace+"/"+cache.Name)

	endpoint := effectiveEndpoint(cache)
	if endpoint == "" {
		// The endpoint source is type-scoped (see effectiveEndpoint).
		// Three reasons we can land here:
		//   - managed CR: reconciler hasn't published status.endpoint
		//     yet (steady-state during initial rollout).
		//   - External CR: spec.endpoint is empty (admission rejects
		//     this on fresh CRs; only reachable from a pre-existing
		//     stored value).
		//   - External CR: spec.endpoint is set but fails the shared
		//     shape check (also pre-existing-only; current admission
		//     rejects malformed values). effectiveEndpoint deliberately
		//     returns "" for this case so the engine pod admits
		//     un-wired rather than receiving an LMCACHE_REMOTE_URL the
		//     connector refuses at startup.
		// Surface the field name (and the shape error when applicable)
		// so the operator looks at the right place. Route through
		// failOpen so any pre-supplied injected-by annotations on the
		// inbound pod get stripped — otherwise the events controller
		// would falsely emit InjectedByCacheBackend even though the
		// webhook bailed out without injecting.
		missingField := "status.endpoint"
		extra := ""
		if cache.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
			missingField = "spec.endpoint"
			if err := adapterruntime.ValidateLMCacheEndpoint(cache.Spec.Endpoint); err != nil {
				extra = ": " + err.Error()
			}
		}
		log.V(1).Info("fail-open: CacheBackend endpoint not resolvable",
			"missingField", missingField, "type", string(cache.Spec.Type), "shapeError", extra)
		return failOpen(req, &pod, fmt.Sprintf("CacheBackend %s not usable%s (fail-open)", missingField, extra))
	}

	// No env-presence short-circuit here: the adapter is the source of truth
	// for the full injected contract (env + the adapter-required args/flags),
	// and lenient short-circuits risk admitting a pod that carries only a
	// subset of the wiring (e.g. a pre-set LMCACHE_REMOTE_URL but missing the
	// engine's connector flag — vLLM's --kv-transfer-config or SGLang's
	// --enable-lmcache) permanently un-converged. Call the adapter
	// unconditionally; it merges idempotently (upsertEnv / upsertArgPair /
	// upsertFlag) and a no-op merge produces an
	// empty patch set, so re-admissions on an already-injected pod are
	// free at the apiserver.
	runtimeID := adapterruntime.ResolveRuntimeID(cache)
	registry := h.Registry
	if registry == nil {
		// Mirror production cmd/controller wiring: DefaultRegistry +
		// the External and SGLang+LMCache adapters (registered explicitly
		// because those subpackages can't be imported by DefaultRegistry
		// without a cycle). Keeps the nil-fallback consistent with the
		// running controller so a bare `EngineInjector{}` doesn't silently
		// fail-open on External / SGLang CRs that the production webhook
		// would have wired. The no-arg SGLang adapter renders no subscriber
		// sidecar (no image configured) — engine config injection still
		// happens; only auto-attach is gated on the controller flag.
		registry = adapterruntime.DefaultRegistry()
		registry.Register(externaladapter.NewAdapter())
		registry.Register(sglangadapter.NewAdapter())
	}
	adapter, err := registry.Select(runtimeID, cache)
	if err != nil {
		log.V(1).Info("fail-open: no runtime adapter",
			"runtime", string(runtimeID), "backend", string(cache.Spec.Type), "error", err.Error())
		return failOpen(req, &pod, fmt.Sprintf("no adapter for runtime=%q backend=%q (fail-open): %v",
			runtimeID, cache.Spec.Type, err))
	}

	mutated := pod.DeepCopy()

	// Snapshot the engine container's pre-injection args/env so the
	// override merge below can scope itself to the adapter-owned set. The
	// override surface mutates only what InjectEngineConfig contributes;
	// user pod-template args/env that the adapter does not touch stay
	// protected. We snapshot before the adapter call (rather than re-deriving
	// the canonical set afterwards) so this works for any adapter without
	// changing the [adapterruntime.KVCacheRuntimeAdapter] contract.
	overrides := engineOverridesFor(cache)
	overrideIdx := -1
	var preArgs []string
	var preEnv []corev1.EnvVar
	if overrides != nil {
		if idx, ok := overrideTargetIndex(mutated.Spec.Containers, adapter.EngineContainerName()); ok {
			overrideIdx = idx
			preArgs = append([]string(nil), mutated.Spec.Containers[idx].Args...)
			preEnv = append([]corev1.EnvVar(nil), mutated.Spec.Containers[idx].Env...)
		}
	}

	if err := adapter.InjectEngineConfig(&mutated.Spec, endpoint, cache); err != nil {
		log.V(1).Info("fail-open: adapter rejected pod",
			"runtime", string(runtimeID), "error", err.Error())
		return failOpen(req, &pod, fmt.Sprintf("adapter rejected pod (fail-open): %v", err))
	}

	// Apply spec.integration.engineOverrides scoped to the adapter-owned
	// args/env derived from the pre/post diff. Admission has already
	// hard-rejected overrides that overlap the adapter's reserved
	// declarations, so the entries surviving to this point are safe to
	// merge. Adapters with no canonical engine container (the reference
	// adapter) return EngineContainerName() == "" and overrideIdx stays
	// -1, so the merge is skipped — the override surface is for production
	// adapters that target a specific engine container.
	if overrides != nil && overrideIdx >= 0 {
		mutated.Spec.Containers[overrideIdx].Args, mutated.Spec.Containers[overrideIdx].Env = applyEngineInjectionOverrides(
			preArgs, mutated.Spec.Containers[overrideIdx].Args,
			preEnv, mutated.Spec.Containers[overrideIdx].Env,
			overrides,
		)
	}

	// Inject the kernel-check init container (adapters that opt in via the
	// optional InitContainerProvider interface — vLLM/LMCache today). Resolve
	// it BEFORE the observation-sidecar append below, while Spec.Containers
	// still holds only the engine container(s): the renderer reuses the engine
	// container's image, and a single-container fallback would break once the
	// sidecar is appended. A failure here MUST NOT block admission (the cache
	// is an optimisation). The webhook is AUTHORITATIVE for this container:
	// REPLACE any existing one of the same name in place rather than skipping —
	// otherwise a pre-planted / hand-authored same-name init container could
	// suppress the real check and bypass strict enforcement (the controller
	// trusts the container by name + termination message). On a normal
	// re-admission the rendered spec is identical, so the replace is a no-op.
	if icp, ok := adapter.(adapterruntime.InitContainerProvider); ok {
		if initC, iErr := icp.KernelCheckInitContainer(cache, mutated); iErr != nil {
			log.V(1).Info("fail-open: kernel-check init container rejected",
				"runtime", string(runtimeID), "error", iErr.Error())
		} else if initC != nil {
			if idx := containerIndexByName(mutated.Spec.InitContainers, initC.Name); idx >= 0 {
				mutated.Spec.InitContainers[idx] = *initC
			} else {
				mutated.Spec.InitContainers = append(mutated.Spec.InitContainers, *initC)
			}
			log.V(1).Info("kernel-check init container injected",
				"runtime", string(runtimeID), "container", initC.Name)
		} else if removed := removeContainerByName(&mutated.Spec.InitContainers, adapterruntime.LMCacheKernelCheckContainerName); removed {
			// The adapter DECLINED to inject (mode=off, or auto on a non-GPU /
			// unresolvable pod). The webhook is authoritative for this
			// container, so strip any pre-existing same-name init container: a
			// hand-authored one would otherwise masquerade as a real check and
			// the controller (which trusts the container by name) could publish
			// or even strict-downgrade from it — violating `off` = absent. Only
			// the explicit decline strips; a transient adapter error above is
			// fail-open and leaves the pod untouched.
			log.V(1).Info("kernel-check init container removed (adapter declined to inject)",
				"runtime", string(runtimeID), "container", adapterruntime.LMCacheKernelCheckContainerName)
		}
	}

	// Auto-attach the observation sidecar. The adapter owns the
	// shape decision: vLLM/LMCache returns a kvevent-subscriber container,
	// the reference adapter (and any future External-type adapter) returns
	// (nil, nil). A side-channel failure here MUST NOT block admission —
	// the engine config above already converged, the cache is still an
	// optimisation, and the next admission will retry. Idempotent: skip
	// the append if a container by the same name is already on the pod
	// (re-admission, manual sidecar in the pod template, etc.).
	if sidecar, sErr := adapter.ObservationSidecar(cache, mutated); sErr != nil {
		log.V(1).Info("fail-open: adapter rejected observation sidecar",
			"runtime", string(runtimeID), "error", sErr.Error())
	} else if sidecar != nil && !hasContainer(mutated.Spec.Containers, sidecar.Name) {
		mutated.Spec.Containers = append(mutated.Spec.Containers, *sidecar)
		log.V(1).Info("observation sidecar appended",
			"runtime", string(runtimeID), "container", sidecar.Name)
	}

	if mutated.Annotations == nil {
		mutated.Annotations = map[string]string{}
	}
	delete(mutated.Annotations, AnnotationInjectSkipped)
	mutated.Annotations[AnnotationInjectedBy] = cache.Namespace + "/" + cache.Name
	mutated.Annotations[AnnotationInjectedByUID] = string(cache.UID)

	mutatedRaw, err := json.Marshal(mutated)
	if err != nil {
		log.V(1).Info("fail-open: re-encode failed", "error", err.Error())
		return admission.Allowed(fmt.Sprintf("re-encode failed (fail-open): %v", err))
	}
	resp := admission.PatchResponseFromRaw(req.Object.Raw, mutatedRaw)
	log.V(1).Info("injected", "runtime", string(runtimeID), "endpoint", endpoint, "patches", len(resp.Patches))
	return resp
}

// selectCacheBackend returns the first CacheBackend in pod.Namespace whose
// Spec.EngineSelector.MatchLabels match pod.Labels, or nil when no backend
// claims the pod. Selecting "the first match" is deliberately simple for
// Phase 1: a future revision can grow a tie-break policy (e.g. an explicit
// `inferencecache.io/cachebackend: <name>` annotation), but the current rule
// matches the reconciler's "each CacheBackend owns its EngineSelector"
// contract — an operator running two backends in the same namespace whose
// selectors overlap is misconfigured, and the handler logs the chosen one
// so the ambiguity is observable.
//
// Iteration order is metadata.name ascending so the "first match" is
// deterministic across apiserver List cache states and is shared with the
// CacheIndex poller's annotation-fallback path (see
// internal/controller/cacheindex_controller.go's attributePod). The two
// surfaces MUST agree on which backend owns a given engine pod — the
// webhook stamps `inferencecache.io/injected-by` and the poller relies
// on it as the authoritative signal, but on overlapping-selector
// fallback both sides need to pick the same backend or status will
// disagree with what the engine was wired to.
//
// A CacheBackend with no EngineSelector or with an empty MatchLabels map is
// skipped: a "match everything" selector at admission time would silently
// claim every pod (including the controller's own and the lmcache-server's),
// which is the kind of broad mutation the fail-open posture is meant to
// prevent.
func (h *EngineInjector) selectCacheBackend(ctx context.Context, pod *corev1.Pod) (*cachev1alpha1.CacheBackend, error) {
	var list cachev1alpha1.CacheBackendList
	if err := h.Reader.List(ctx, &list, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("list CacheBackends in %s: %w", pod.Namespace, err)
	}
	idxs := make([]int, 0, len(list.Items))
	for i := range list.Items {
		idxs = append(idxs, i)
	}
	sort.Slice(idxs, func(a, b int) bool {
		return list.Items[idxs[a]].Name < list.Items[idxs[b]].Name
	})
	podLabels := labels.Set(pod.Labels)
	for _, i := range idxs {
		cb := &list.Items[i]
		if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
			continue
		}
		sel := labels.SelectorFromSet(cb.Spec.EngineSelector.MatchLabels)
		if sel.Matches(podLabels) {
			return cb, nil
		}
	}
	return nil, nil
}

// SkipAnnotationOptsOut returns true when the value of [AnnotationSkip]
// should be treated as an opt-out. Truthy values (anything strconv.ParseBool
// accepts as true) opt out; non-empty values that ParseBool can't interpret
// (e.g. "yes", "skip", an operator's free-form note) also opt out — making
// the annotation "set with any meaningful value disables injection."
// Explicitly falsey values ("false", "0", "f", "no") leave injection
// enabled, so `inferencecache.io/skip-inject: "false"` does NOT disable.
func SkipAnnotationOptsOut(value string) bool {
	if value == "" {
		return false
	}
	if b, err := strconv.ParseBool(value); err == nil {
		return b
	}
	// strconv.ParseBool accepts a small set ("1","t","T","true","TRUE",
	// "True","0","f","F","false","FALSE","False"). Treat other free-form
	// values as opt-out unless the user explicitly typed a false synonym.
	switch strings.ToLower(value) {
	case "no", "off", "disable", "disabled":
		return false
	}
	return true
}

// logger returns the handler's configured logger if set, otherwise the
// per-context logger controller-runtime installs (which carries the webhook
// path + request UID added by the runtime).
func (h *EngineInjector) logger(ctx context.Context) logr.Logger {
	if h.Log.GetSink() != nil {
		return h.Log
	}
	return logf.FromContext(ctx)
}

// failOpen builds the admission response for any fail-open return path
// AFTER the pod has been decoded. The webhook's contract is that
// AnnotationInjectedBy and AnnotationInjectSkipped on the persisted pod mean
// "the webhook successfully made this decision" — those are what the
// engine-pod-events controller keys `InjectedByCacheBackend` and
// `SkippedByOperator` off of. The annotations are user-controllable (anyone
// with pod-create RBAC can set them) and the webhook does not overwrite them
// on fail-open paths, so a copy/paste from a mutated pod's metadata, or an
// attacker forging the annotations, would otherwise trip the controller into
// emitting an event for a pod the webhook never touched.
//
// Fix: on every fail-open return, strip the annotation if it was
// preset. Steady-state cost stays at zero patches per pod for the common
// no-match case (the vast majority of pods cluster-wide), because the
// helper short-circuits to admission.Allowed when the annotation is
// absent.
func failOpen(req admission.Request, pod *corev1.Pod, reason string) admission.Response {
	hasInjectedBy := pod.Annotations[AnnotationInjectedBy] != ""
	hasInjectedByUID := pod.Annotations[AnnotationInjectedByUID] != ""
	hasInjectSkipped := pod.Annotations[AnnotationInjectSkipped] != ""
	if !hasInjectedBy && !hasInjectedByUID && !hasInjectSkipped {
		return admission.Allowed(reason)
	}
	cleared := pod.DeepCopy()
	delete(cleared.Annotations, AnnotationInjectedBy)
	delete(cleared.Annotations, AnnotationInjectedByUID)
	delete(cleared.Annotations, AnnotationInjectSkipped)
	if len(cleared.Annotations) == 0 {
		// Avoid emitting an empty-map annotations field; absent is the
		// canonical "no annotations" shape.
		cleared.Annotations = nil
	}
	raw, err := json.Marshal(cleared)
	if err != nil {
		// Marshal failure on a pod we just decoded is extremely unlikely;
		// fall back to plain Allowed so the pod still admits. The
		// controller then sees a forged annotation, but that's strictly
		// no worse than the pre-fix behavior — so this isn't a fail-
		// closed condition.
		return admission.Allowed(reason)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}

func skipInjection(req admission.Request, pod *corev1.Pod) admission.Response {
	mutated := pod.DeepCopy()
	if mutated.Annotations == nil {
		mutated.Annotations = map[string]string{}
	}
	delete(mutated.Annotations, AnnotationInjectedBy)
	delete(mutated.Annotations, AnnotationInjectedByUID)
	mutated.Annotations[AnnotationInjectSkipped] = InjectSkippedReasonSkipAnnotation

	if pod.Annotations[AnnotationInjectSkipped] == InjectSkippedReasonSkipAnnotation &&
		pod.Annotations[AnnotationInjectedBy] == "" &&
		pod.Annotations[AnnotationInjectedByUID] == "" {
		return admission.Allowed("skipped via " + AnnotationSkip)
	}
	raw, err := json.Marshal(mutated)
	if err != nil {
		return admission.Allowed("skipped via " + AnnotationSkip)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}

// effectiveEndpoint returns the address the engine pod should be wired
// to for the given CacheBackend. The source is type-scoped:
//
//   - External: spec.endpoint is authoritative — the operator owns it,
//     admission validates it, status.endpoint is just a reconciler
//     mirror that may briefly lag during an update. If a new pod
//     admits between an operator's spec.endpoint update and the
//     status patch, status would still hold the OLD value and the
//     pod would boot wired to the stale address; pod admission is
//     CREATE-only so that bad wiring is permanent. Preferring
//     trimmed spec.endpoint over status here avoids that race and is
//     consistent with admission's view of the truth.
//   - Managed types (LMCache today): status.endpoint is the only
//     source — the reconciler builds it from the live Service it
//     provisions, and spec.endpoint is admission-rejected for these
//     types (see rejectEndpointOnNonExternal), so there's nothing
//     else to fall back on. The webhook must wait for status.
//
// Returns "" when no endpoint is currently usable; callers fail-open.
//
// Every return path is `strings.TrimSpace`-d so a whitespace-only value
// (a pre-admission CR that mirrored whitespace into status, an
// externally-edited Service endpoint that picked up stray padding) is
// treated as missing and fails open instead of injecting
// `LMCACHE_REMOTE_URL=lm://   ` which the engine connector would reject
// at runtime. The reconciler already trims before publishing
// status.endpoint, but the webhook trims defensively here too so a
// race against an old controller build can't leak whitespace to the
// engine wire.
//
// For External CRs with an empty/whitespace spec.endpoint there is NO
// fallback to status. The reconciler treats that state as
// Ready=False/ExternalEndpointMissing — falling back here would wire
// new pods to a stale status the reconciler considers unusable, which
// is the kind of two-layer disagreement that hides bad CRs from
// operators. Fail-open instead so the operator sees the same gap
// reflected at admission as in status.
func effectiveEndpoint(cache *cachev1alpha1.CacheBackend) string {
	if cache == nil {
		return ""
	}
	if cache.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		// For External, re-apply the admission-time shape check on
		// the stored spec.endpoint. The validating webhook already
		// rejects malformed values at write time, but a pre-existing
		// CR in etcd from before the shape rule shipped (or stored
		// when an earlier, laxer rule set was in effect) can still
		// carry e.g. `https://...`, a portless host, or embedded
		// whitespace. Returning the malformed value would let the
		// adapter prepend `lm://` and inject an URL the engine
		// connector refuses at startup — turning a cache
		// misconfiguration into a serving outage. Treat invalid the
		// same way we treat empty: return "" and let the caller's
		// existing fail-open branch admit the pod un-wired.
		ep := strings.TrimSpace(cache.Spec.Endpoint)
		if ep == "" {
			return ""
		}
		if err := adapterruntime.ValidateLMCacheEndpoint(cache.Spec.Endpoint); err != nil {
			return ""
		}
		return ep
	}
	return strings.TrimSpace(cache.Status.Endpoint)
}

// hasContainer reports whether containers already includes one named name.
// Used to keep the observation-sidecar append idempotent across re-admissions
// and against pod templates that pre-baked the sidecar.
func hasContainer(containers []corev1.Container, name string) bool {
	return containerIndexByName(containers, name) >= 0
}

// containerIndexByName returns the index of the first container named name, or
// -1 if absent. The kernel-check injection uses it to REPLACE (not skip) an
// existing same-name container in place, so the webhook stays authoritative
// for that container.
func containerIndexByName(containers []corev1.Container, name string) int {
	for i := range containers {
		if containers[i].Name == name {
			return i
		}
	}
	return -1
}

// removeContainerByName removes the first container named name from *containers
// (preserving order) and reports whether one was removed. The kernel-check path
// uses it to strip a hand-authored same-name init container when the adapter
// declines to inject, keeping the webhook authoritative even in the
// no-injection case.
func removeContainerByName(containers *[]corev1.Container, name string) bool {
	idx := containerIndexByName(*containers, name)
	if idx < 0 {
		return false
	}
	*containers = append((*containers)[:idx], (*containers)[idx+1:]...)
	return true
}
