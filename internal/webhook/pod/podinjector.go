package pod

import (
	"context"
	"encoding/json"
	"fmt"
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
const AnnotationInjectedBy = "inferencecache.io/injected-by"

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
	// nil falls back to [adapterruntime.DefaultRegistry] so cmd/controller
	// can register the handler with the same single-line wiring the
	// reconciler uses (both consult the same registry).
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

	if skipAnnotationOptsOut(pod.Annotations[AnnotationSkip]) {
		return admission.Allowed("skipped via " + AnnotationSkip)
	}

	cache, err := h.selectCacheBackend(ctx, &pod)
	if err != nil {
		log.V(1).Info("fail-open: backend lookup failed", "error", err.Error())
		return admission.Allowed(fmt.Sprintf("backend lookup failed (fail-open): %v", err))
	}
	if cache == nil {
		// No CacheBackend in the namespace matches this pod. The webhook
		// matches all pods (cluster-wide) — most of them aren't engines —
		// so this is the steady-state path; log only at V(2) so it
		// doesn't drown reconciler logs.
		log.V(2).Info("no matching CacheBackend; pass-through")
		return admission.Allowed("no matching CacheBackend")
	}
	log = log.WithValues("cachebackend", cache.Namespace+"/"+cache.Name)

	endpoint := effectiveEndpoint(cache)
	if endpoint == "" {
		// The reconciler hasn't published the cache-server's endpoint
		// yet. Without it the adapter would write LMCACHE_REMOTE_URL=lm://
		// which a vLLM engine refuses on startup. Fail open so the pod
		// admits unwired; the next pod admission (after the backend
		// becomes Ready) will pick it up.
		log.V(1).Info("fail-open: CacheBackend has no status.endpoint yet")
		return admission.Allowed("CacheBackend status.endpoint not yet published (fail-open)")
	}

	// No env-presence short-circuit here: the adapter is the source of truth
	// for the full injected contract (env + arg), and lenient short-circuits
	// risk admitting a pod that carries only a subset of the wiring (e.g. a
	// pre-set LMCACHE_REMOTE_URL but no --kv-transfer-config / VLLM_USE_V1)
	// permanently un-converged. Call the adapter unconditionally; it merges
	// idempotently (upsertEnv / upsertArgPair) and a no-op merge produces an
	// empty patch set, so re-admissions on an already-injected pod are
	// free at the apiserver.
	runtimeID := adapterruntime.ResolveRuntimeID(cache)
	registry := h.Registry
	if registry == nil {
		registry = adapterruntime.DefaultRegistry()
	}
	adapter, err := registry.Select(runtimeID, cache)
	if err != nil {
		log.V(1).Info("fail-open: no runtime adapter",
			"runtime", string(runtimeID), "backend", string(cache.Spec.Type), "error", err.Error())
		return admission.Allowed(fmt.Sprintf("no adapter for runtime=%q backend=%q (fail-open): %v",
			runtimeID, cache.Spec.Type, err))
	}

	mutated := pod.DeepCopy()
	if err := adapter.InjectEngineConfig(&mutated.Spec, endpoint, cache); err != nil {
		log.V(1).Info("fail-open: adapter rejected pod",
			"runtime", string(runtimeID), "error", err.Error())
		return admission.Allowed(fmt.Sprintf("adapter rejected pod (fail-open): %v", err))
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
	mutated.Annotations[AnnotationInjectedBy] = cache.Namespace + "/" + cache.Name

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
	podLabels := labels.Set(pod.Labels)
	for i := range list.Items {
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

// skipAnnotationOptsOut returns true when the value of [AnnotationSkip]
// should be treated as an opt-out. Truthy values (anything strconv.ParseBool
// accepts as true) opt out; non-empty values that ParseBool can't interpret
// (e.g. "yes", "skip", an operator's free-form note) also opt out — making
// the annotation "set with any meaningful value disables injection."
// Explicitly falsey values ("false", "0", "f", "no") leave injection
// enabled, so `inferencecache.io/skip-inject: "false"` does NOT disable.
func skipAnnotationOptsOut(value string) bool {
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
func effectiveEndpoint(cache *cachev1alpha1.CacheBackend) string {
	if cache == nil {
		return ""
	}
	status := strings.TrimSpace(cache.Status.Endpoint)
	if cache.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		if v := strings.TrimSpace(cache.Spec.Endpoint); v != "" {
			return v
		}
		// Defensive: if spec.endpoint somehow became empty/whitespace
		// (a CR predating admission) but status still carries a value,
		// fall back to it rather than fail-open. Same direction as the
		// reconciler's own defensive Ready=False/Missing branch.
		return status
	}
	return status
}

// hasContainer reports whether containers already includes one named name.
// Used to keep the observation-sidecar append idempotent across re-admissions
// and against pod templates that pre-baked the sidecar.
func hasContainer(containers []corev1.Container, name string) bool {
	for i := range containers {
		if containers[i].Name == name {
			return true
		}
	}
	return false
}
