// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// buildDeployment wraps the adapter-rendered PodSpec into a Deployment the
// controller owns: ObjectMeta + labels + replicas + selector come from the
// CacheBackend identity, not the adapter.
func (r *CacheBackendReconciler) buildDeployment(backend *cachev1alpha1.CacheBackend, podSpec *corev1.PodSpec) *appsv1.Deployment {
	replicas := int32(1)
	selector := selectorLabels(backend.Name)
	podLabels := podTemplateLabels(backend)

	pod := podSpec.DeepCopy()
	applyManagedWorkload(pod, backend.Spec.RemoteStorage)

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

	return dep
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

// applyManagedWorkload materializes server-defaulted fields and applies the
// scheduling/security contract for a controller-managed remote provider.
// schedulerName and terminationGracePeriodSeconds are always materialized so
// the desired template matches the API-server-defaulted object and updates do
// not churn.
func applyManagedWorkload(spec *corev1.PodSpec, storage *cachev1alpha1.CacheBackendRemoteStorageSpec) {
	if spec.SchedulerName == "" {
		spec.SchedulerName = "default-scheduler"
	}
	if spec.TerminationGracePeriodSeconds == nil {
		defaultGrace := int64(30)
		spec.TerminationGracePeriodSeconds = &defaultGrace
	}
	if storage == nil || storage.Ownership != cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged || storage.Workload == nil {
		return
	}
	override := storage.Workload.DeepCopy()
	if override.NodeSelector != nil {
		spec.NodeSelector = override.NodeSelector
	}
	if override.Affinity != nil {
		spec.Affinity = override.Affinity
	}
	if override.Tolerations != nil {
		spec.Tolerations = override.Tolerations
	}
	if override.TopologySpreadConstraints != nil {
		spec.TopologySpreadConstraints = override.TopologySpreadConstraints
	}
	if override.ImagePullSecrets != nil {
		spec.ImagePullSecrets = override.ImagePullSecrets
	}
	if override.ServiceAccountName != "" {
		spec.ServiceAccountName = override.ServiceAccountName
	}
	if override.SecurityContext != nil {
		spec.SecurityContext = override.SecurityContext
	}
	if override.PriorityClassName != "" {
		spec.PriorityClassName = override.PriorityClassName
	}
	if override.RuntimeClassName != nil {
		spec.RuntimeClassName = override.RuntimeClassName
	}
	if override.SchedulerName != "" {
		spec.SchedulerName = override.SchedulerName
	}
	if override.TerminationGracePeriodSeconds != nil {
		spec.TerminationGracePeriodSeconds = override.TerminationGracePeriodSeconds
	}
}

// serviceEndpoint formats the managed remote-storage endpoint as host:port
// using the Service's first port.
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
// pod-level defaults and volumes) — and leave everything
// else intact. Overwriting the whole PodTemplate would strip API-server-defaulted
// fields (port Protocol, RestartPolicy, probe thresholds, ...), and since those are
// re-defaulted on every write it would spin a perpetual update loop via the
// Owns(Deployment) watch.
//
// Wrapped in RetryOnConflict because the kube Deployment controller writes
// Deployment.Status often during rollout; without retry, the Get/Update inside
// CreateOrUpdate races those writes and surfaces a 409 that aborts the
// reconcile pass and freezes CR status.
func (r *CacheBackendReconciler) applyDeployment(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *appsv1.Deployment) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
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
			return controllerutil.SetControllerReference(backend, dep, r.Scheme)
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("apply deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// applyService creates or updates the backend Service idempotently, owned by the CR.
// Type, selector, and ports are reconciled (so out-of-band drift is corrected); the
// rendered ports carry Protocol=TCP so they match the API-server-defaulted object,
// and nodePort stays an allocated field we never touch — so reconciling ports does
// not churn through the Owns(Service) watch.
func (r *CacheBackendReconciler) applyService(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *corev1.Service) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = desired.Labels
			svc.Spec.Type = desired.Spec.Type
			svc.Spec.Selector = desired.Spec.Selector
			svc.Spec.Ports = desired.Spec.Ports
			// ClusterIP is settable only at creation. Managed Redis uses the normal
			// allocated ClusterIP when the renderer leaves this empty.
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

	// Keep generic PodSpec network fields aligned with the provider renderer.
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
// Containers in live whose names are not in desired are dropped. The managed
// provider Deployment is fully controller-owned, including its container set;
// operator-added sidecars are not supported and must not survive reconciliation.
func reconcileManagedContainer(live *corev1.PodSpec, desired *corev1.PodSpec) {
	if len(desired.Containers) == 0 {
		return
	}
	desiredNames := make(map[string]int, len(desired.Containers))
	for i := range desired.Containers {
		desiredNames[desired.Containers[i].Name] = i
	}

	// First pass: drop any live container whose name isn't desired.
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

// cleanupOwnedWorkload best-effort deletes the Deployment and Service this CR
// owns when the backend no longer requests managed remote storage.
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
	return nil
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
