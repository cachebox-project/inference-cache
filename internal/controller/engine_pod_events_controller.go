package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
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
	eventReasonEngineInjected = "InjectedByCacheBackend"
)

// EnginePodEventsReconciler watches engine pods that the mutating Pod
// webhook stamped with [podwebhook.AnnotationInjectedBy] and emits a Normal
// `InjectedByCacheBackend` Kubernetes Event on each pod, referencing the
// matched CacheBackend. The controller is intentionally narrow — its only
// job is to convert the webhook's injection annotation into a
// describe-visible Event keyed by the live pod's UID.
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
// Fail-soft: a Get failure other than NotFound surfaces as a normal
// reconcile error (controller-runtime requeues with backoff). A
// CacheBackend lookup failure is not surfaced — the event still fires
// with the annotation value as the cache identity, since the only thing
// that lookup adds is the CR object as the event's Related reference
// (informational; the Event's primary signal is the involvedObject and
// the message).
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
		// Predicate already filters this out; defense in depth.
		return ctrl.Result{}, nil
	}
	if r.Recorder == nil {
		return ctrl.Result{}, nil
	}

	// Try to load the named CacheBackend so the event carries it as the
	// Related object. Failure is non-fatal: emit the event anyway with
	// related=nil. The annotation value alone carries the binding
	// identity in the human-readable message. The lookup error is logged
	// so RBAC failures, malformed refs, or transient API hiccups are not
	// silently swallowed — the event still fires, but an operator
	// inspecting controller logs sees why the Related reference is
	// missing.
	cb, lookupErr := r.lookupCacheBackend(ctx, cbRef)
	if lookupErr != nil {
		logger.V(1).Info("CacheBackend related-ref lookup failed; emitting event without Related",
			"namespace", pod.Namespace, "name", pod.Name, "cachebackend", cbRef, "error", lookupErr.Error())
	}
	r.Recorder.Eventf(&pod, cb, corev1.EventTypeNormal,
		eventReasonEngineInjected, eventReasonEngineInjected,
		"Injected engine config from CacheBackend %q", cbRef)
	logger.V(1).Info("emitted InjectedByCacheBackend event",
		"namespace", pod.Namespace, "name", pod.Name, "cachebackend", cbRef)
	return ctrl.Result{}, nil
}

// lookupCacheBackend parses the "<namespace>/<name>" annotation value and
// fetches the named CacheBackend. The lookup is best-effort: a missing or
// malformed reference returns (nil, err) and the caller emits the event
// without the Related field rather than dropping it.
func (r *EnginePodEventsReconciler) lookupCacheBackend(ctx context.Context, ref string) (*cachev1alpha1.CacheBackend, error) {
	ns, name, ok := strings.Cut(ref, "/")
	if !ok || ns == "" || name == "" {
		return nil, fmt.Errorf("malformed CacheBackend reference %q", ref)
	}
	var cb cachev1alpha1.CacheBackend
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &cb); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, fmt.Errorf("get CacheBackend %s: %w", ref, err)
	}
	return &cb, nil
}

// SetupWithManager wires the reconciler to Pod CREATE events filtered by
// the podwebhook.AnnotationInjectedBy presence. The CREATE-only predicate means
// label edits, status updates, and deletions don't enqueue — the
// per-pod event is emitted exactly once over the pod's life.
func (r *EnginePodEventsReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("engine-pod-events")
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("engine-pod-events").
		For(&corev1.Pod{},
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetAnnotations()[podwebhook.AnnotationInjectedBy] != ""
			}),
				createOnlyPredicate{}),
		).
		Complete(r)
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
