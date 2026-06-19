package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// Engine-pod Event reasons.
//
// EngineInjected fires once per engine pod CREATE that the mutating Pod
// webhook successfully wired to a CacheBackend (the webhook stamps the
// inferencecache.io/injected-by annotation; this controller mirrors that
// stamp into a K8s Event keyed by the now-persisted pod UID so
// `kubectl describe pod <engine-pod>` surfaces the binding decision).
//
// Recording the Event from the webhook itself is not viable: the
// apiserver assigns metadata.uid AFTER mutating admission, so a webhook-
// recorded event would have involvedObject.uid="" and be invisible to
// `kubectl describe` (which filters events by UID).
const (
	eventReasonEngineInjected    = "InjectedByCacheBackend"
	eventReasonSkippedByOperator = "SkippedByOperator"
)

// EnginePodEventsReconciler watches engine pods that the mutating Pod
// webhook stamped with [podwebhook.AnnotationInjectedBy] or
// [podwebhook.AnnotationInjectSkipped] and emits a describe-visible Kubernetes
// Event keyed by the live pod's UID.
//
// Out of scope on purpose:
//   - No-match events. A pod with no podwebhook.AnnotationInjectedBy could be (a)
//     unrelated to this cache plane or (b) an engine whose labels missed
//     every selector. The controller can't reliably distinguish without
//     re-running the webhook's selector logic, and the cluster-wide noise
//     a no-match event would generate outweighs its diagnostic value when
//     the per-CR `status.matchedEnginePods` already surfaces the same
//     signal (zero matches → operator-actionable drift).
//   - Pod UPDATE / DELETE handling. The webhook stamp is immutable for a
//     given pod (CREATE-only mutation), so re-emitting on update would
//     duplicate without new information; the predicate below filters out
//     non-CREATE events.
type EnginePodEventsReconciler struct {
	client.Client
	Log      logr.Logger
	Recorder events.EventRecorder
	// APIReader is an uncached live client used for the CacheBackend
	// lookup that backs UID validation. The cached client's informer
	// can be momentarily stale (especially right at controller startup
	// or just after a CR's first apply) — a "NotFound" from the cache
	// could be a real deletion OR a cache miss, and we treat NotFound
	// as a permanent skip per the conservative contract. Using the
	// APIReader removes that ambiguity at the cost of one extra
	// apiserver round-trip per CREATE-time reconcile (negligible at
	// this controller's throughput). Production wiring passes
	// mgr.GetAPIReader(); tests that don't exercise the live lookup
	// can leave it nil — lookupCacheBackend falls back to the embedded
	// client.Client so existing fake-client tests still work.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch

// Reconcile emits a single `InjectedByCacheBackend` Event for each Pod
// the webhook stamped. The controller-runtime EventBroadcaster aggregates
// events on the apiserver side (same Reason + InvolvedObject within a
// 10-minute window upserts the existing event), so a re-enqueue on
// restart does not flood the event stream — there is no need for an
// extra "already emitted" annotation round-trip.
//
// Skip table (each case returns `(ctrl.Result{}, nil)` — admission is
// CREATE-only, so a skipped reconcile is a permanent drop, but
// deliberately so for these cases):
//   - Pod has neither `injected-by` nor `inject-skipped` annotation. The
//     predicate already filters this out; defense in depth.
//   - `injected-by` value does not parse as `<ns>/<name>` (malformed or
//     stale under an older annotation contract).
//   - `injected-by-uid` annotation is missing. The webhook always writes
//     it; absence is the failurePolicy=Ignore forgery shape (user pre-set
//     `injected-by` while the webhook was unreachable).
//   - The named CacheBackend is NotFound. Without a live CR we cannot
//     verify the UID, so we cannot tell "CR deleted between inject and
//     reconcile" from a forged annotation — conservative skip.
//   - `injected-by-uid` does not match the live CR's UID (forgery, or
//     CR was deleted and recreated under the same name).
//
// Lookup errors OTHER than NotFound (transient API/RBAC/cache failures)
// surface as reconcile errors so controller-runtime retries with backoff
// rather than permanently dropping the event for the affected pod.
func (r *EnginePodEventsReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log
	if logger.GetSink() == nil {
		logger = log.FromContext(ctx)
	}

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	cbRef := pod.Annotations[podwebhook.AnnotationInjectedBy]
	if cbRef == "" {
		if pod.Annotations[podwebhook.AnnotationInjectSkipped] == podwebhook.InjectSkippedReasonSkipAnnotation {
			if r.Recorder == nil {
				return ctrl.Result{}, nil
			}
			r.Recorder.Eventf(&pod, nil, corev1.EventTypeNormal,
				eventReasonSkippedByOperator, eventReasonSkippedByOperator,
				"Skipped cache injection: %s", podwebhook.InjectSkippedReasonSkipAnnotation)
			logger.V(1).Info("emitted SkippedByOperator event",
				"namespace", pod.Namespace, "name", pod.Name)
			return ctrl.Result{}, nil
		}
		// Predicate already filters this out; defense in depth.
		return ctrl.Result{}, nil
	}
	if r.Recorder == nil {
		return ctrl.Result{}, nil
	}

	// Refuse to emit on a malformed annotation value. The webhook always
	// stamps `<namespace>/<name>`; a value that doesn't parse can only
	// have come from manual tampering or a stale annotation under an
	// older annotation contract — emitting "Injected engine config" for
	// that would falsely claim the webhook did work it never did.
	if !validCacheBackendRef(cbRef) {
		logger.V(1).Info("skipping InjectedByCacheBackend event: malformed injected-by annotation",
			"namespace", pod.Namespace, "name", pod.Name, "cachebackend", cbRef)
		return ctrl.Result{}, nil
	}
	// The injected-by annotation is user-controllable. The webhook is
	// failurePolicy=Ignore on the MutatingWebhookConfiguration, so a pod
	// can persist with a user-supplied injected-by annotation when the
	// webhook is unreachable at admission time. The `injected-by-uid`
	// annotation REDUCES (does NOT eliminate) that attack surface: the
	// value is the matched CacheBackend's metadata.uid, an apiserver-
	// assigned identifier the casual copy/paste from another pod's
	// metadata won't match — but it is NOT a secret. A pod creator with
	// `get` RBAC on CacheBackends can read the live UID and stamp the
	// pair correctly. The check below catches the common "copy a pod
	// template across CR boundaries" mistake and the failurePolicy=Ignore
	// "user template carries stale annotations" case; it does NOT
	// authenticate the webhook against a determined operator with API
	// read access. A truly unforgeable proof would require a webhook-
	// authored signature the apiserver vouches for, which is out of
	// scope here.
	//
	// Require BOTH: a successful CR lookup AND a UID match against the
	// live CR.
	uidRef := pod.Annotations[podwebhook.AnnotationInjectedByUID]
	if uidRef == "" {
		logger.V(1).Info("skipping InjectedByCacheBackend event: injected-by-uid annotation missing",
			"namespace", pod.Namespace, "name", pod.Name, "cachebackend", cbRef)
		return ctrl.Result{}, nil
	}
	cb, lookupErr := r.lookupCacheBackend(ctx, cbRef)
	switch {
	case lookupErr == nil:
		// Happy path: CR exists, fall through to UID validation below.
	case apierrors.IsNotFound(lookupErr):
		// CR truly absent. Without a live CR we cannot verify the UID,
		// and without verification we cannot tell a real "CR deleted
		// between inject and reconcile" from a forged annotation
		// surviving a failurePolicy=Ignore admission. Skip emission —
		// the missing event is the conservative tradeoff for keeping
		// this signal authoritative.
		logger.V(1).Info("skipping InjectedByCacheBackend event: CacheBackend not found",
			"namespace", pod.Namespace, "name", pod.Name, "cachebackend", cbRef)
		return ctrl.Result{}, nil
	default:
		// Transient API/RBAC/cache error. We MUST NOT swallow this:
		// admission is CREATE-only, so a one-and-done skip on a
		// transient hiccup permanently drops the event for this pod.
		// Surface the error so controller-runtime backs off and
		// retries — once the transient condition clears, the next
		// reconcile validates the UID and emits.
		return ctrl.Result{}, fmt.Errorf("lookup CacheBackend %q: %w", cbRef, lookupErr)
	}
	if uidRef != string(cb.UID) {
		// Annotation UID doesn't match the live CR's UID — either the
		// user forged both annotations (and got the UID wrong, e.g.
		// from a previous incarnation of the CR), or the CR was
		// deleted and recreated after the webhook stamped the pod.
		// Skip emission.
		logger.V(1).Info("skipping InjectedByCacheBackend event: injected-by-uid mismatch",
			"namespace", pod.Namespace, "name", pod.Name, "cachebackend", cbRef,
			"annotationUID", uidRef, "liveUID", string(cb.UID))
		return ctrl.Result{}, nil
	}
	r.Recorder.Eventf(&pod, cb, corev1.EventTypeNormal,
		eventReasonEngineInjected, eventReasonEngineInjected,
		"Injected engine config from CacheBackend %q", cbRef)
	logger.V(1).Info("emitted InjectedByCacheBackend event",
		"namespace", pod.Namespace, "name", pod.Name, "cachebackend", cbRef)
	return ctrl.Result{}, nil
}

// validCacheBackendRef reports whether ref has the `<namespace>/<name>` shape
// the webhook always writes for the injected-by annotation. The webhook never
// produces any other shape, so a ref that does not parse cleanly is by
// definition stale or manually-tampered — callers use this to short-circuit
// the InjectedByCacheBackend event emission for those pods.
//
// Reject EXACTLY the cases the webhook never produces:
//   - Missing slash separator, empty namespace half, or empty name half.
//   - Multiple slashes (`ns/name/extra` is not a shape the webhook emits).
//   - Either half fails Kubernetes name validation. K8s namespace names are
//     DNS-1123 labels, resource names are DNS-1123 subdomains; both are
//     lowercase alphanumeric + hyphens (+ dots for subdomain). A ref like
//     `ns/UPPER` or `bad_ns/cb` is structurally slash-shaped but cannot
//     identify any real K8s object — passing it through to the apiserver
//     Get would surface as a BadRequest, which the caller treats as a
//     transient error and retries, hot-looping the reconciler on a forged
//     pod.
func validCacheBackendRef(ref string) bool {
	parts := strings.SplitN(ref, "/", 3)
	if len(parts) != 2 {
		return false
	}
	ns, name := parts[0], parts[1]
	if ns == "" || name == "" {
		return false
	}
	if errs := validation.IsDNS1123Label(ns); len(errs) > 0 {
		return false
	}
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return false
	}
	return true
}

// lookupCacheBackend parses the "<namespace>/<name>" annotation value and
// fetches the named CacheBackend. Returns (nil, err) on a malformed
// reference, NotFound, or any transient Get error; callers MUST
// distinguish between these to decide whether to skip emission
// (malformed / NotFound) or surface the error to controller-runtime for
// retry (transient). The malformed-ref error is preserved for symmetry
// with the live-API errors but callers typically short-circuit the
// malformed case earlier via [validCacheBackendRef].
func (r *EnginePodEventsReconciler) lookupCacheBackend(ctx context.Context, ref string) (*cachev1alpha1.CacheBackend, error) {
	ns, name, ok := strings.Cut(ref, "/")
	if !ok || ns == "" || name == "" {
		return nil, fmt.Errorf("malformed CacheBackend reference %q", ref)
	}
	// Prefer the uncached APIReader so a stale informer cache cannot
	// surface as a fake NotFound and silently drop the one-shot event.
	// Fall back to the cached client only when APIReader is unset (test
	// wiring without a real APIReader); the reconciler still functions,
	// it just inherits the cache-miss race.
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var cb cachev1alpha1.CacheBackend
	if err := reader.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &cb); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, fmt.Errorf("get CacheBackend %s: %w", ref, err)
	}
	return &cb, nil
}

// SetupWithManager wires the reconciler to Pod CREATE events filtered by
// the webhook's injection-decision annotations. The CREATE-only predicate
// means label edits, status updates, and deletions don't enqueue; the per-pod
// event is emitted exactly once over the pod's life.
func (r *EnginePodEventsReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("engine-pod-events")
	}
	if r.APIReader == nil {
		// Default to the manager's uncached APIReader so production
		// wiring doesn't have to thread it explicitly. Envtest
		// integration tests that boot a real manager pick this up
		// automatically too.
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("engine-pod-events").
		For(&corev1.Pod{},
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return enginePodEventCandidate(obj)
			}),
				createOnlyPredicate{}),
		).
		Complete(r)
}

func enginePodEventCandidate(obj client.Object) bool {
	annotations := obj.GetAnnotations()
	return annotations[podwebhook.AnnotationInjectedBy] != "" ||
		annotations[podwebhook.AnnotationInjectSkipped] == podwebhook.InjectSkippedReasonSkipAnnotation
}

// createOnlyPredicate enqueues a Pod only on CREATE. UPDATE/DELETE/GENERIC
// are dropped: the podwebhook.AnnotationInjectedBy stamp is set at admission time and
// never changes over a pod's life, so updates carry no new injection
// signal worth a fresh event.
type createOnlyPredicate struct{ predicate.Funcs }

func (createOnlyPredicate) Create(_ event.CreateEvent) bool   { return true }
func (createOnlyPredicate) Update(_ event.UpdateEvent) bool   { return false }
func (createOnlyPredicate) Delete(_ event.DeleteEvent) bool   { return false }
func (createOnlyPredicate) Generic(_ event.GenericEvent) bool { return false }
