// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Default HPA tuning when the autoscaling spec leaves them unset.
const (
	defaultHPAMinReplicas                 = int32(1)
	defaultHPATargetCPUUtilizationPercent = int32(80)
)

// buildDeployment wraps the adapter-rendered PodSpec into a Deployment the
// controller owns: ObjectMeta + labels + replicas + selector come from the
// CacheBackend identity, not the adapter.
func (r *CacheBackendReconciler) buildDeployment(backend *cachev1alpha1.CacheBackend, podSpec *corev1.PodSpec) *appsv1.Deployment {
	replicas := initialReplicas(backend)
	selector := selectorLabels(backend.Name)
	podLabels := podTemplateLabels(backend)

	pod := podSpec.DeepCopy()
	applyPodOverrides(pod, backend.Spec.Template)

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backend.Name,
			Namespace: backend.Namespace,
			Labels:    podLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec:       *pod,
			},
		},
	}

	// A hostNetwork cache-server binds its ports directly on the node, so the
	// default RollingUpdate would surge a second pod onto the same host ports.
	// That surge pod cannot serve: it CrashLoops failing to bind while the old pod
	// still holds the port. In practice the scheduler rejects it even earlier,
	// because the API server defaults hostPort=containerPort for hostNetwork pods
	// (core/v1 defaultHostNetworkPorts — confirmed against a live apiserver: a
	// hostNetwork pod declaring only containerPort=50051 comes back with
	// hostPort=50051), which trips the NodePorts predicate. Recreate tears the old
	// pod down first, so neither failure mode is reachable.
	// Only a backend whose data plane requires the host network renders one
	// (Mooncake today); every other adapter keeps the default RollingUpdate.
	if pod.HostNetwork {
		dep.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	}
	clampSingletonReplicas(&dep.Spec, backend)
	return dep
}

// cacheServerIsSingleton reports whether the backend's managed cache-server must run
// as exactly one replica — no scale-out, no HPA. Two backends require it, for
// different reasons:
//
//   - a host-network server (the Mooncake master): a second replica cannot bind the
//     node ports the first already holds, and on a different node comes up as an
//     independent master that silently splits the store;
//   - the (sglang, LMCache) Redis L2 store: a plain Redis is not clustered, so a
//     second pod behind the one Service shards the keyspace across independent
//     instances and silently partitions the cache.
//
// Admission rejects spec.replicas>1 / spec.autoscaling for both
// (rejectMooncakeMasterScaleOut, rejectSGLangRedisL2ScaleOut); this is the shared
// predicate the reconciler backstop keys on.
func cacheServerIsSingleton(backend *cachev1alpha1.CacheBackend, pod *corev1.PodSpec) bool {
	// EventsOnly renders no cache-server at all (the reconciler sheds any owned
	// workload), so there is no singleton to protect — keep the predicate honest, and
	// aligned with the admission rules, which exempt EventsOnly for the same reason.
	if backend.Spec.IsEventsOnly() {
		return false
	}
	if pod != nil && pod.HostNetwork {
		return true
	}
	return adapterruntime.ResolveRuntimeID(backend) == adapterruntime.RuntimeSGLang &&
		backend.Spec.Type == cachev1alpha1.CacheBackendTypeLMCache
}

// clampSingletonReplicas caps a singleton cache-server (see cacheServerIsSingleton)
// at one replica.
//
// Admission rejects spec.replicas>1 and spec.autoscaling for such a backend, but
// that is not sufficient: ValidateUpdate only rejects violations an edit
// *introduces*, so an object written before the rule existed — or before its
// backend moved onto a singleton data plane — stays in etcd with replicas=3 and an
// HPA, and is never re-validated. Rendering that faithfully would schedule several
// servers: host-network masters contend for the same node ports or split the store;
// several Redis pods behind one Service partition the keyspace.
//
// The reconciler is therefore the last line of defense, and it clamps rather than
// obeys. 0 is preserved: that is "disabled", not "scaled out".
func clampSingletonReplicas(spec *appsv1.DeploymentSpec, backend *cachev1alpha1.CacheBackend) {
	if !cacheServerIsSingleton(backend, &spec.Template.Spec) || spec.Replicas == nil || *spec.Replicas <= 1 {
		return
	}
	one := int32(1)
	spec.Replicas = &one
}

// buildService wraps the adapter-rendered Service spec into a Service the
// controller owns: ObjectMeta + Selector come from the CacheBackend identity.
// Adapter-provided fields (Spec.Type, Spec.Ports) are preserved as-is.
func (r *CacheBackendReconciler) buildService(backend *cachev1alpha1.CacheBackend, src *corev1.Service) *corev1.Service {
	selector := selectorLabels(backend.Name)
	labels := podTemplateLabels(backend)
	out := src.DeepCopy()
	out.ObjectMeta = metav1.ObjectMeta{
		Name:      backend.Name,
		Namespace: backend.Namespace,
		Labels:    labels,
	}
	out.Spec.Selector = selector
	if out.Spec.Type == "" {
		out.Spec.Type = corev1.ServiceTypeClusterIP
	}
	return out
}

// selectorLabels are the immutable identity labels for a backend's child objects.
func selectorLabels(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "cachebackend",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/managed-by": "inference-cache-controller",
	}
}

// podTemplateLabels add backend-type identity on top of the selector labels.
// The backend-type label is informational (kubectl filtering); the controller
// only relies on the selector labels.
func podTemplateLabels(backend *cachev1alpha1.CacheBackend) map[string]string {
	labels := selectorLabels(backend.Name)
	if t := string(backend.Spec.Type); t != "" {
		labels["inferencecache.io/backend-type"] = t
	}
	return labels
}

// applyPodOverrides copies optional pod-level scheduling/security overrides
// from the spec onto the rendered pod spec. Server-defaulted fields
// (schedulerName, terminationGracePeriodSeconds) are always set to their
// defaults when unset so the rendered template matches the API-server-
// defaulted object and updates don't churn.
func applyPodOverrides(spec *corev1.PodSpec, override *cachev1alpha1.CacheBackendPodSpecOverride) {
	if spec.SchedulerName == "" {
		spec.SchedulerName = "default-scheduler"
	}
	if spec.TerminationGracePeriodSeconds == nil {
		defaultGrace := int64(30)
		spec.TerminationGracePeriodSeconds = &defaultGrace
	}
	if override == nil {
		return
	}
	spec.NodeSelector = override.NodeSelector
	spec.Affinity = override.Affinity
	spec.Tolerations = override.Tolerations
	spec.TopologySpreadConstraints = override.TopologySpreadConstraints
	spec.ImagePullSecrets = override.ImagePullSecrets
	spec.ServiceAccountName = override.ServiceAccountName
	spec.SecurityContext = override.SecurityContext
	spec.PriorityClassName = override.PriorityClassName
	spec.RuntimeClassName = override.RuntimeClassName
	if override.SchedulerName != "" {
		spec.SchedulerName = override.SchedulerName
	}
	if override.TerminationGracePeriodSeconds != nil {
		spec.TerminationGracePeriodSeconds = override.TerminationGracePeriodSeconds
	}
}

// serviceEndpoint formats the published cache endpoint as host:port using the
// service's first port. Engine-protocol prefixes (e.g. lm:// for LMCache) are
// the adapter's responsibility — status.endpoint stays engine-agnostic.
func serviceEndpoint(svc *corev1.Service) string {
	if len(svc.Spec.Ports) == 0 {
		return ""
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, svc.Spec.Ports[0].Port)
}

// applyDeployment creates or updates the backend Deployment idempotently, owned by the CR.
//
// On create we establish the full templated spec. On update we touch only the
// fields this module owns — replicas, the rollout strategy, and everything
// reconcileManagedPodSpec covers (the managed container's image/command/args/env,
// pod-level overrides and volumes, HostNetwork/DNSPolicy) — and leave everything
// else intact. Overwriting the whole PodTemplate would strip API-server-defaulted
// fields (port Protocol, RestartPolicy, probe thresholds, ...), and since those are
// re-defaulted on every write it would spin a perpetual update loop via the
// Owns(Deployment) watch.
//
// When an HPA owns scaling (spec.autoscaling set), the reconciler defers to the
// HPA's replica count rather than overwriting it — re-asserting replicas on
// every reconcile would fight the HPA and churn the rollout. The one exception is
// a singleton cache-server (a host-network master, or the SGLang Redis L2 — see
// cacheServerIsSingleton): clampSingletonReplicas runs last and overrides both the
// spec and the HPA (see its godoc for why admission alone cannot protect a
// grandfathered object).
//
// Wrapped in RetryOnConflict because the kube Deployment controller writes
// Deployment.Status often during rollout; without retry, the Get/Update inside
// CreateOrUpdate races those writes and surfaces a 409 that aborts the
// reconcile pass and freezes CR status.
func (r *CacheBackendReconciler) applyDeployment(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *appsv1.Deployment) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
			// Snapshot the HPA-owned field BEFORE we mutate the live spec. When an
			// HPA is configured the controller must never re-assert replicas; doing
			// so would fight the HPA on every reconcile and churn the rollout.
			liveReplicas := dep.Spec.Replicas

			dep.Labels = desired.Labels
			if dep.CreationTimestamp.IsZero() {
				dep.Spec = *desired.Spec.DeepCopy()
			} else {
				dep.Spec.Replicas = desired.Spec.Replicas
				reconcileManagedPodSpec(&dep.Spec.Template.Spec, &desired.Spec.Template.Spec)
				// The rollout strategy is part of the desired shape: a hostNetwork
				// server must use Recreate or a rolling surge collides on the node's
				// ports. Reconcile it in BOTH directions — switching a backend back
				// to a pod-network type must restore RollingUpdate rather than strand
				// the Deployment on Recreate. Compare against the *effective* desired
				// type (an adapter that leaves it unset means "the API-server
				// default"), and assign the whole struct so a stale rollingUpdate
				// block is cleared: the API server rejects it alongside Recreate.
				// An already-correct type is left untouched, so a pod-network backend
				// never churns the API-server-defaulted rollingUpdate block.
				wantStrategy := desired.Spec.Strategy.Type
				if wantStrategy == "" {
					wantStrategy = appsv1.RollingUpdateDeploymentStrategyType
				}
				if dep.Spec.Strategy.Type != wantStrategy {
					dep.Spec.Strategy = appsv1.DeploymentStrategy{Type: wantStrategy}
				}
			}
			if backend.Spec.Autoscaling != nil && liveReplicas != nil {
				// Preserve the HPA's writes — but clamp to the configured floor so
				// raising autoscaling.minReplicas doesn't briefly publish Ready
				// against the old smaller live count before the HPA catches up.
				preserved := *liveReplicas
				if floor := autoscalingFloor(backend.Spec.Autoscaling); preserved < floor {
					preserved = floor
				}
				dep.Spec.Replicas = &preserved
			}
			// LAST, after every other writer (desired spec, HPA preservation): a
			// singleton cache-server must never be scaled out, no matter what the
			// spec or a stale HPA asks for.
			clampSingletonReplicas(&dep.Spec, backend)
			return controllerutil.SetControllerReference(backend, dep, r.Scheme)
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("apply deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// autoscalingFloor is the effective minReplicas value for the HPA — the
// user's setting, or the default floor when unset. Mirrors the resolution
// buildHPA does so the reconciler and the HPA agree on the lower bound.
func autoscalingFloor(spec *cachev1alpha1.CacheBackendAutoscalingSpec) int32 {
	if spec == nil {
		return defaultHPAMinReplicas
	}
	if spec.MinReplicas != nil {
		return *spec.MinReplicas
	}
	return defaultHPAMinReplicas
}

// applyService creates or updates the backend Service idempotently, owned by the CR.
// Type, selector, and ports are reconciled (so out-of-band drift is corrected); the
// rendered ports carry Protocol=TCP so they match the API-server-defaulted object,
// and nodePort stays an allocated field we never touch — so reconciling ports does
// not churn through the Owns(Service) watch.
//
// clusterIP is the exception. It is IMMUTABLE after creation and it is the field
// that decides whether the Service is headless. A backend whose data plane is a
// peer-to-peer mesh (Mooncake) must be headless, so the Service DNS name resolves
// to the pod's (hostNetwork: node) IP with every dynamically negotiated port
// reachable; a virtual ClusterIP forwards only the declared ports and strands the
// mesh. Therefore:
//   - on create we propagate the adapter's clusterIP (e.g. "None"),
//   - on update we never touch it (immutable),
//   - and when a live Service's headless-ness diverges from what the adapter now
//     renders, we delete it so the next reconcile recreates it correctly. Without
//     that, the in-place update would fail forever and the backend would stay
//     silently broken — Ready, but transferring nothing.
func (r *CacheBackendReconciler) applyService(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *corev1.Service) error {
	var live corev1.Service
	switch err := r.Get(ctx, client.ObjectKeyFromObject(desired), &live); {
	case err == nil:
		// Only ever delete a Service this CacheBackend controls.
		if metav1.IsControlledBy(&live, backend) &&
			headlessnessDiverges(live.Spec.ClusterIP, desired.Spec.ClusterIP) {
			if delErr := r.Delete(ctx, &live); delErr != nil && !apierrors.IsNotFound(delErr) {
				return fmt.Errorf("recreate service %s/%s for immutable clusterIP change: %w",
					desired.Namespace, desired.Name, delErr)
			}
			log.FromContext(ctx).Info("deleted service to change immutable clusterIP; recreating",
				"namespace", desired.Namespace, "name", desired.Name,
				"liveClusterIP", live.Spec.ClusterIP, "desiredClusterIP", desired.Spec.ClusterIP)
			// The Owns(Service) watch requeues on this delete; recreate on the next
			// pass rather than racing a stale cache read inside CreateOrUpdate below.
			return nil
		}
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("get service %s/%s: %w", desired.Namespace, desired.Name, err)
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = desired.Labels
			svc.Spec.Type = desired.Spec.Type
			svc.Spec.Selector = desired.Spec.Selector
			svc.Spec.Ports = desired.Spec.Ports
			// Settable only at creation. On an existing Service this is already
			// "None" or an allocated VIP, and a divergent one was deleted above.
			if svc.Spec.ClusterIP == "" {
				svc.Spec.ClusterIP = desired.Spec.ClusterIP
			}
			return controllerutil.SetControllerReference(backend, svc, r.Scheme)
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("apply service %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// headlessnessDiverges reports whether a live Service's clusterIP allocation is
// incompatible with what the adapter now renders. spec.clusterIP is immutable, so
// a Service cannot be migrated in place between headless ("None") and a virtual
// ClusterIP in either direction — it must be recreated.
//
// An empty live value means the API server has not assigned one yet; treat that as
// "no divergence" so a transient read never triggers a delete.
func headlessnessDiverges(live, desired string) bool {
	if live == "" {
		return false
	}
	return (live == corev1.ClusterIPNone) != (desired == corev1.ClusterIPNone)
}

// reconcileManagedPodSpec updates the spec-driven fields of the live pod spec in
// place: the managed container's image/command/args/env, the pod-level override
// fields, and the networking the adapter owns (HostNetwork, and DNSPolicy
// normalized to its API-server default when the adapter leaves it unset).
// API-server-defaulted fields we don't own (RestartPolicy, probe thresholds, port
// Protocol, ...) are left untouched so the update does not churn. The desired pod
// spec already carries the canonical defaults for the server-defaulted override
// fields (schedulerName, terminationGracePeriodSeconds), so copying them is
// idempotent.
//
// Volumes are adapter-owned (per [adapterruntime.KVCacheRuntimeAdapter] — the
// adapter fills PodSpec.Containers + PodSpec.Volumes) and are not
// API-server-defaulted in a Deployment template, so the reconciler always
// propagates them from desired. That corrects two cases the previous
// gated-on-container-set-change behaviour missed:
//   - Out-of-band volume drift on a steady-state Deployment.
//   - An adapter update that adds/changes pod-level volumes without changing
//     the container set.
//
// The current LMCache adapter renders no pod-level volumes, so this is a
// no-op for the steady-state path; on an in-place upgrade from the previous
// colocated all-in-one rendering it still prunes the stale cache-home + shm
// volumes that container left behind.
func reconcileManagedPodSpec(live *corev1.PodSpec, desired *corev1.PodSpec) {
	reconcileManagedContainer(live, desired)
	live.Volumes = desired.Volumes

	live.NodeSelector = desired.NodeSelector
	live.Affinity = desired.Affinity
	live.Tolerations = desired.Tolerations
	live.TopologySpreadConstraints = desired.TopologySpreadConstraints
	live.ImagePullSecrets = desired.ImagePullSecrets
	live.ServiceAccountName = desired.ServiceAccountName
	live.SecurityContext = desired.SecurityContext
	live.PriorityClassName = desired.PriorityClassName
	live.SchedulerName = desired.SchedulerName
	live.RuntimeClassName = desired.RuntimeClassName
	live.TerminationGracePeriodSeconds = desired.TerminationGracePeriodSeconds

	// Host networking is part of the adapter's desired shape, not an
	// API-server-defaulted field: a backend whose data plane is a peer-to-peer
	// mesh (Mooncake) is unreachable on overlay pod IPs. Reconcile it on UPDATE
	// too, or an already-provisioned backend would never migrate onto the host
	// network and would keep transferring nothing.
	live.HostNetwork = desired.HostNetwork
	// dnsPolicy IS API-server-defaulted (ClusterFirst), but it must still be
	// reconciled in BOTH directions: a hostNetwork backend needs
	// ClusterFirstWithHostNet, and switching back to a pod-network backend must
	// restore the default rather than leave the stale host-net policy behind.
	// Normalize an unset desired value to the API-server default so the write is
	// idempotent and the template never churns.
	wantDNS := desired.DNSPolicy
	if wantDNS == "" {
		wantDNS = corev1.DNSClusterFirst
	}
	live.DNSPolicy = wantDNS
}

// reconcileManagedContainer updates the spec-driven fields of the managed backend
// container in place, leaving API-server-defaulted container fields untouched.
//
// Containers in live whose names are not in desired are dropped — this is the
// upgrade path from a previous colocated all-in-one rendering (container
// name "vllm") to the standalone topology (container name "lmcache-server"):
// an in-place upgrade must replace the old managed container, not stack the
// new one alongside it. We never drop containers that match a desired name
// (we only update their managed fields), so a Deployment carrying sidecars
// in addition to the managed container loses the sidecars — sidecars were
// not supported in the previous rendering and remain unsupported here.
func reconcileManagedContainer(live *corev1.PodSpec, desired *corev1.PodSpec) {
	if len(desired.Containers) == 0 {
		return
	}
	desiredNames := make(map[string]int, len(desired.Containers))
	for i := range desired.Containers {
		desiredNames[desired.Containers[i].Name] = i
	}

	// First pass: drop any live container whose name isn't desired (the
	// upgrade-from-previous-managed-shape case).
	kept := live.Containers[:0]
	for i := range live.Containers {
		if _, ok := desiredNames[live.Containers[i].Name]; ok {
			kept = append(kept, live.Containers[i])
		}
	}
	live.Containers = kept

	// Second pass: for each desired container, update the matching live one
	// in place (preserving API-server-defaulted fields) or append it.
	for i := range desired.Containers {
		want := desired.Containers[i]
		matched := false
		for j := range live.Containers {
			if live.Containers[j].Name == want.Name {
				// Adapter-owned fields the reconciler propagates from desired:
				// the cache-server's serving port, probes, resource shape, and
				// the connector args/env. Adapters render these explicitly
				// (with API-server-defaulted fields like Port Protocol set in
				// the rendering), so copying them is idempotent and doesn't
				// churn the Owns watch. Leaving Ports/Probes/VolumeMounts
				// untouched would let port drift break the Service's
				// TargetPort lookup or hide a probe regression. Resources
				// likewise differ by profile (e.g. GPU vs CPU canary) and
				// aren't API-server-defaulted, so reconciling them is
				// churn-free.
				live.Containers[j].Image = want.Image
				live.Containers[j].ImagePullPolicy = want.ImagePullPolicy
				live.Containers[j].Command = want.Command
				live.Containers[j].Args = want.Args
				live.Containers[j].Env = want.Env
				live.Containers[j].Ports = want.Ports
				live.Containers[j].Resources = want.Resources
				live.Containers[j].VolumeMounts = want.VolumeMounts
				live.Containers[j].ReadinessProbe = want.ReadinessProbe
				live.Containers[j].LivenessProbe = want.LivenessProbe
				live.Containers[j].StartupProbe = want.StartupProbe
				matched = true
				break
			}
		}
		if !matched {
			live.Containers = append(live.Containers, *want.DeepCopy())
		}
	}
}

// cleanupOwnedWorkload best-effort deletes the Deployment + Service + HPA this
// CR owns, used when a backend is no longer a managed Deployment (type/kind
// changed). Normal CR deletion is handled by owner-reference garbage
// collection; this covers the in-place mutation case where the CR itself
// still exists.
func (r *CacheBackendReconciler) cleanupOwnedWorkload(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	key := types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace}

	var dep appsv1.Deployment
	if err := r.deleteIfOwned(ctx, key, &dep, backend); err != nil {
		return err
	}
	var svc corev1.Service
	if err := r.deleteIfOwned(ctx, key, &svc, backend); err != nil {
		return err
	}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	return r.deleteIfOwned(ctx, key, &hpa, backend)
}

// deleteIfOwned deletes obj only if it exists and is controller-owned by backend.
func (r *CacheBackendReconciler) deleteIfOwned(ctx context.Context, key types.NamespacedName, obj client.Object, backend *cachev1alpha1.CacheBackend) error {
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, backend) {
		return nil
	}
	if err := r.Delete(ctx, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// initialReplicas picks the Deployment's initial replica count. With
// autoscaling configured, spec.autoscaling.minReplicas is the source of truth
// (defaulting to 1 when unset), so the workload comes up at or above the HPA
// floor on first apply instead of starting at 1 and waiting for the HPA to
// patch it. Without autoscaling, spec.replicas wins (default 1).
func initialReplicas(backend *cachev1alpha1.CacheBackend) int32 {
	if backend.Spec.Autoscaling != nil {
		if backend.Spec.Autoscaling.MinReplicas != nil {
			return *backend.Spec.Autoscaling.MinReplicas
		}
		return 1
	}
	if backend.Spec.Replicas != nil {
		return *backend.Spec.Replicas
	}
	return 1
}

// reconcileHPA creates, updates, or deletes the HorizontalPodAutoscaler that
// drives the backend Deployment's replica count. The HPA exists iff
// spec.autoscaling is set; otherwise any controller-owned HPA is removed.
func (r *CacheBackendReconciler) reconcileHPA(ctx context.Context, backend *cachev1alpha1.CacheBackend, deployment *appsv1.Deployment) error {
	// A singleton cache-server (see cacheServerIsSingleton): an HPA would fight the
	// clampSingletonReplicas clamp on every reconcile and, whenever it won, put a
	// second server on the cluster — a split store (host-network master) or a
	// partitioned keyspace (Redis L2). Admission rejects spec.autoscaling for these
	// backends, but a grandfathered object still carries one — so tear the HPA down
	// from the observed shape rather than trusting the spec.
	if backend.Spec.Autoscaling == nil || cacheServerIsSingleton(backend, &deployment.Spec.Template.Spec) {
		// Autoscaling disabled (or impossible) — clean up any HPA we previously owned.
		return r.deleteOwnedHPA(ctx, backend, deployment.Name)
	}

	desired := buildHPA(backend, deployment)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		hpa.Labels = desired.Labels
		hpa.Spec = desired.Spec
		return controllerutil.SetControllerReference(backend, hpa, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("apply HPA %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// buildHPA renders the desired HorizontalPodAutoscaler for a CacheBackend whose
// spec.autoscaling is set. Targets the managed Deployment by name. Phase 1 ships
// a CPU-utilization target; cache-aware (custom-metric) HPAs come later.
func buildHPA(backend *cachev1alpha1.CacheBackend, deployment *appsv1.Deployment) *autoscalingv2.HorizontalPodAutoscaler {
	spec := backend.Spec.Autoscaling
	minReplicas := defaultHPAMinReplicas
	if spec.MinReplicas != nil {
		minReplicas = *spec.MinReplicas
	}
	target := defaultHPATargetCPUUtilizationPercent
	if spec.TargetCPUUtilizationPercent != nil {
		target = *spec.TargetCPUUtilizationPercent
	}
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployment.Name,
			Namespace: deployment.Namespace,
			Labels:    deployment.Labels,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment.Name,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: spec.MaxReplicas,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: &target,
						},
					},
				},
			},
		},
	}
}

// deleteOwnedHPA removes a previously-owned HPA (e.g. spec.autoscaling cleared).
// Missing HPA is a no-op.
func (r *CacheBackendReconciler) deleteOwnedHPA(ctx context.Context, backend *cachev1alpha1.CacheBackend, name string) error {
	key := types.NamespacedName{Name: name, Namespace: backend.Namespace}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := r.Get(ctx, key, &hpa); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get HPA %s/%s: %w", backend.Namespace, name, err)
	}
	if !metav1.IsControlledBy(&hpa, backend) {
		return nil
	}
	if err := r.Delete(ctx, &hpa); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}
