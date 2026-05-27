package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

// conditionTypeReady reports whether the managed backend workload is serving.
const conditionTypeReady = "Ready"

// CacheBackendReconciler reconciles a CacheBackend object.
type CacheBackendReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a CacheBackend toward its desired state. External backends only
// mirror their configured endpoint to status; managed backends (LMCache in Phase 1)
// template a Deployment + Service from the reference stack and own them via owner
// references so they are garbage-collected with the CR.
func (r *CacheBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log
	if logger.GetSink() == nil {
		logger = log.FromContext(ctx)
	}

	var backend cachev1alpha1.CacheBackend
	if err := r.Get(ctx, req.NamespacedName, &backend); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if backend.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		return ctrl.Result{}, r.reconcileExternal(ctx, &backend)
	}

	builder, ok := backendadapter.For(backend.Spec.Type)
	if !ok {
		// Only LMCache is managed in Phase 1 (C2). Other managed types are out of
		// scope here; leave them untouched until their builders land.
		logger.V(1).Info("no managed builder for backend type",
			"type", backend.Spec.Type, "namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, nil
	}

	// StatefulSet-backed (PVC) provisioning + autoscaling is the C3 module. Phase 1
	// manages a Deployment only.
	if backend.Spec.DeploymentKind == cachev1alpha1.CacheBackendDeploymentKindStatefulSet {
		logger.V(1).Info("StatefulSet deploymentKind deferred to C3; skipping",
			"namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, nil
	}

	return r.reconcileManaged(ctx, logger, &backend, builder)
}

// reconcileExternal mirrors a pre-existing backend's configured endpoint to status.
func (r *CacheBackendReconciler) reconcileExternal(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	if backend.Status.Endpoint == backend.Spec.Endpoint {
		return nil
	}
	patch := client.MergeFrom(backend.DeepCopy())
	backend.Status.Endpoint = backend.Spec.Endpoint
	if err := r.Status().Patch(ctx, backend, patch); err != nil {
		return fmt.Errorf("patch CacheBackend status %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	return nil
}

// reconcileManaged templates and applies the child workload, then publishes status.
func (r *CacheBackendReconciler) reconcileManaged(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend, builder backendadapter.Builder) (ctrl.Result, error) {
	workload, err := builder.Build(backend)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("build workload for %s/%s: %w", backend.Namespace, backend.Name, err)
	}

	if err := r.applyDeployment(ctx, backend, workload.Deployment); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyService(ctx, backend, workload.Service); err != nil {
		return ctrl.Result{}, err
	}

	var dep appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKeyFromObject(workload.Deployment), &dep); err != nil {
		return ctrl.Result{}, fmt.Errorf("get deployment %s/%s: %w", workload.Deployment.Namespace, workload.Deployment.Name, err)
	}

	if err := r.updateManagedStatus(ctx, backend, workload.Endpoint, &dep); err != nil {
		return ctrl.Result{}, err
	}

	logger.V(1).Info("reconciled managed CacheBackend",
		"namespace", backend.Namespace, "name", backend.Name, "endpoint", workload.Endpoint)
	return ctrl.Result{}, nil
}

// applyDeployment creates or updates the backend Deployment idempotently, owned by the CR.
//
// On create we establish the full templated spec. On update we touch only the
// fields this module owns (replicas + the managed container's image/command/
// args/env) and leave everything else intact — overwriting the whole PodTemplate
// would strip API-server-defaulted fields (port Protocol, RestartPolicy, probe
// thresholds, ...), and since those are re-defaulted on every write it would spin
// a perpetual update loop via the Owns(Deployment) watch.
func (r *CacheBackendReconciler) applyDeployment(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *appsv1.Deployment) error {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = desired.Labels
		if dep.CreationTimestamp.IsZero() {
			dep.Spec = *desired.Spec.DeepCopy()
		} else {
			dep.Spec.Replicas = desired.Spec.Replicas
			reconcileManagedContainer(&dep.Spec.Template.Spec, &desired.Spec.Template.Spec)
		}
		return controllerutil.SetControllerReference(backend, dep, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("apply deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// applyService creates or updates the backend Service idempotently, owned by the CR.
// Ports/type are static in Phase 1, so on update we reconcile only the selector and
// labels and preserve the API-server-defaulted fields (Protocol, ClusterIP) to avoid
// write churn through the Owns(Service) watch.
func (r *CacheBackendReconciler) applyService(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *corev1.Service) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Selector = desired.Spec.Selector
		if svc.CreationTimestamp.IsZero() {
			svc.Spec.Type = desired.Spec.Type
			svc.Spec.Ports = desired.Spec.Ports
		}
		return controllerutil.SetControllerReference(backend, svc, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("apply service %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// reconcileManagedContainer updates the spec-driven fields of the managed backend
// container in place, leaving API-server-defaulted container fields untouched.
func reconcileManagedContainer(live *corev1.PodSpec, desired *corev1.PodSpec) {
	if len(desired.Containers) == 0 {
		return
	}
	want := desired.Containers[0]
	for i := range live.Containers {
		if live.Containers[i].Name == want.Name {
			live.Containers[i].Image = want.Image
			live.Containers[i].Command = want.Command
			live.Containers[i].Args = want.Args
			live.Containers[i].Env = want.Env
			return
		}
	}
	// The managed container is missing (e.g. an out-of-band edit) — restore it.
	live.Containers = append(live.Containers, *want.DeepCopy())
}

// updateManagedStatus derives health from the Deployment and patches status only when it changes.
func (r *CacheBackendReconciler) updateManagedStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, endpoint string, dep *appsv1.Deployment) error {
	wantReplicas := int32(1)
	if backend.Spec.Replicas != nil {
		wantReplicas = *backend.Spec.Replicas
	}

	var health cachev1alpha1.CacheBackendHealth
	var condStatus metav1.ConditionStatus
	var reason, message string
	switch {
	case dep.Status.ReadyReplicas >= wantReplicas:
		health, condStatus, reason = cachev1alpha1.CacheBackendHealthReady, metav1.ConditionTrue, "BackendReady"
		message = fmt.Sprintf("%d/%d replicas ready", dep.Status.ReadyReplicas, wantReplicas)
	case dep.Status.ReadyReplicas == 0:
		health, condStatus, reason = cachev1alpha1.CacheBackendHealthPending, metav1.ConditionFalse, "WorkloadStarting"
		message = "waiting for backend pods to become ready"
	default:
		health, condStatus, reason = cachev1alpha1.CacheBackendHealthDegraded, metav1.ConditionFalse, "PartiallyReady"
		message = fmt.Sprintf("%d/%d replicas ready", dep.Status.ReadyReplicas, wantReplicas)
	}

	before := backend.DeepCopy()
	backend.Status.Endpoint = endpoint
	backend.Status.Health = health
	backend.Status.ObservedGeneration = backend.Generation
	meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             condStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: backend.Generation,
	})

	if equality.Semantic.DeepEqual(before.Status, backend.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch CacheBackend status %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CacheBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.CacheBackend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
