// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

const nodeLocalShmCleanupFinalizer = "inferencecache.io/nodelocal-shm-cleanup"

func isTypedLMCacheNodeLocal(backend *cachev1alpha1.CacheBackend) bool {
	return backend != nil &&
		backend.Spec.EffectiveCacheType() == cachev1alpha1.CacheBackendTypeLMCache &&
		backend.Spec.LMCache != nil &&
		backend.Spec.LMCache.Topology == cachev1alpha1.LMCacheTopologyNodeLocal
}

func isTypedLMCacheMP(backend *cachev1alpha1.CacheBackend) bool {
	return isTypedLMCachePodLocal(backend) || isTypedLMCacheNodeLocal(backend)
}

// reconcileLMCacheNodeLocalServerPods follows the inference system's actual
// placement. One server Pod is created for every distinct node that currently
// hosts an active engine injected for this CacheBackend. Engines remain the
// scheduling authority; an unscheduled engine never causes speculative server
// placement.
func (r *CacheBackendReconciler) reconcileLMCacheNodeLocalServerPods(ctx context.Context, backend *cachev1alpha1.CacheBackend, binding *backendadapter.Binding) error {
	if !isTypedLMCacheNodeLocal(backend) {
		if controllerutil.ContainsFinalizer(backend, nodeLocalShmCleanupFinalizer) {
			if err := r.ensureLMCacheNodeLocalCleanupForConsumers(ctx, backend); err != nil {
				return err
			}
		}
		return r.cleanupLMCacheNodeLocalServerPods(ctx, backend)
	}
	demand, err := r.nodeLocalEngineDemand(ctx, backend)
	if err != nil {
		return err
	}
	wantShmHostPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		return err
	}

	var servers corev1.PodList
	if err := r.Client.List(ctx, &servers,
		client.InNamespace(backend.Namespace),
		client.MatchingLabels{
			enginebinding.LabelLMCacheNodeLocalServer: "true",
			enginebinding.LabelCacheBackendUID:        string(backend.UID),
		},
	); err != nil {
		return fmt.Errorf("list LMCache NodeLocal server Pods for %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	cleanupNodes, err := r.reconcileLMCacheNodeLocalCleanupPods(ctx, backend, demand, true)
	if err != nil {
		return err
	}

	wantGeneration := strconv.FormatInt(backend.Generation, 10)
	liveByNode := make(map[string]*corev1.Pod, len(servers.Items))
	for i := range servers.Items {
		pod := &servers.Items[i]
		if !metav1.IsControlledBy(pod, backend) {
			continue
		}
		targetNode := nodeLocalServerTargetNode(pod)
		_, wanted := demand[targetNode]
		runtimeCurrent := nodeLocalServerHasRuntimeIdentity(pod, wantShmHostPath)
		current := pod.Annotations[enginebinding.AnnotationNodeLocalOwnerUID] == string(backend.UID) &&
			pod.Annotations[enginebinding.AnnotationNodeLocalGeneration] == wantGeneration &&
			pod.Name == builtinruntime.NodeLocalServerPodName(backend.Name, targetNode) &&
			runtimeCurrent
		if !current {
			if pod.DeletionTimestamp == nil {
				if nodeLocalServerUsesShmHostPath(pod, wantShmHostPath) && (!runtimeCurrent || !wanted) {
					if err := r.ensureLMCacheNodeLocalCleanupPod(ctx, backend, pod); err != nil {
						return err
					}
					cleanupNodes[targetNode] = true
				}
				if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("delete stale LMCache NodeLocal server Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
			}
			continue
		}
		if !wanted {
			retention := time.Duration(backend.Spec.LMCache.NodeLocal.IdleRetentionSeconds) * time.Second
			if retention <= 0 {
				if pod.DeletionTimestamp == nil {
					if err := r.ensureLMCacheNodeLocalCleanupPod(ctx, backend, pod); err != nil {
						return err
					}
					if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
						return fmt.Errorf("delete idle LMCache NodeLocal server Pod %s/%s: %w", pod.Namespace, pod.Name, err)
					}
				}
				continue
			}
			idleSince, parseErr := time.Parse(time.RFC3339Nano, pod.Annotations[enginebinding.AnnotationNodeLocalIdleSince])
			if parseErr != nil {
				before := pod.DeepCopy()
				if pod.Annotations == nil {
					pod.Annotations = map[string]string{}
				}
				pod.Annotations[enginebinding.AnnotationNodeLocalIdleSince] = time.Now().UTC().Format(time.RFC3339Nano)
				if err := r.Client.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
					return fmt.Errorf("mark LMCache NodeLocal server Pod %s/%s idle: %w", pod.Namespace, pod.Name, err)
				}
				continue
			}
			if time.Since(idleSince) >= retention && pod.DeletionTimestamp == nil {
				if err := r.ensureLMCacheNodeLocalCleanupPod(ctx, backend, pod); err != nil {
					return err
				}
				if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("delete expired idle LMCache NodeLocal server Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
			}
			continue
		}
		if _, wasIdle := pod.Annotations[enginebinding.AnnotationNodeLocalIdleSince]; wasIdle {
			before := pod.DeepCopy()
			delete(pod.Annotations, enginebinding.AnnotationNodeLocalIdleSince)
			if err := r.Client.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
				return fmt.Errorf("reactivate LMCache NodeLocal server Pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		if prior := liveByNode[targetNode]; prior != nil {
			if pod.DeletionTimestamp == nil {
				if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("delete duplicate LMCache NodeLocal server Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
			}
			continue
		}
		liveByNode[targetNode] = pod
	}

	nodes := make([]string, 0, len(demand))
	for nodeName := range demand {
		nodes = append(nodes, nodeName)
	}
	sort.Strings(nodes)
	for _, nodeName := range nodes {
		if liveByNode[nodeName] != nil || cleanupNodes[nodeName] {
			continue
		}
		desired, err := builtinruntime.RenderLMCacheNodeLocalServerPod(backend, binding, nodeName, demand[nodeName])
		if err != nil {
			return err
		}
		if err := controllerutil.SetControllerReference(backend, desired, r.Scheme); err != nil {
			return fmt.Errorf("own LMCache NodeLocal server Pod %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		if err := r.Client.Create(ctx, desired); err != nil {
			if apierrors.IsAlreadyExists(err) {
				var existing corev1.Pod
				if getErr := r.Client.Get(ctx, client.ObjectKeyFromObject(desired), &existing); getErr != nil {
					return fmt.Errorf("inspect colliding LMCache NodeLocal server Pod %s/%s: %w", desired.Namespace, desired.Name, getErr)
				}
				if metav1.IsControlledBy(&existing, backend) && existing.DeletionTimestamp != nil {
					continue
				}
				return fmt.Errorf("LMCache NodeLocal server Pod name %s/%s is already occupied by another object", desired.Namespace, desired.Name)
			}
			return fmt.Errorf("create LMCache NodeLocal server Pod %s/%s: %w", desired.Namespace, desired.Name, err)
		}
	}
	return nil
}

func nodeLocalServerHasRuntimeIdentity(pod *corev1.Pod, wantHostPath string) bool {
	if pod == nil || wantHostPath == "" {
		return false
	}
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name != lmCacheMPServerStatusContainerName {
			continue
		}
		container := &pod.Spec.Containers[i]
		var shmVolumeName string
		for j := range container.VolumeMounts {
			mount := &container.VolumeMounts[j]
			if mount.MountPath == "/dev/shm" && !mount.ReadOnly && mount.SubPath == "" && mount.SubPathExpr == "" {
				shmVolumeName = mount.Name
				break
			}
		}
		if shmVolumeName == "" {
			return false
		}
		validHostPath := false
		for j := range pod.Spec.Volumes {
			volume := &pod.Spec.Volumes[j]
			if volume.Name == shmVolumeName && volume.HostPath != nil && volume.HostPath.Path == wantHostPath &&
				volume.HostPath.Type != nil && *volume.HostPath.Type == corev1.HostPathDirectoryOrCreate {
				validHostPath = true
				break
			}
		}
		if !validHostPath {
			return false
		}
		return builtinruntime.IsLMCacheMPCUDAServerProfile(container.Args)
	}
	return false
}

func nodeLocalServerTargetNode(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	if nodeName := pod.Annotations[enginebinding.AnnotationNodeLocalTargetNode]; nodeName != "" {
		return nodeName
	}
	return pod.Spec.NodeName
}

func nodeLocalServerUsesShmHostPath(pod *corev1.Pod, wantHostPath string) bool {
	if pod == nil || wantHostPath == "" {
		return false
	}
	for i := range pod.Spec.Volumes {
		if hostPath := pod.Spec.Volumes[i].HostPath; hostPath != nil && hostPath.Path == wantHostPath {
			return true
		}
	}
	return false
}

// nodeLocalEngineDemand returns one deterministic source engine per active
// node. The webhook-authored backend name+UID pair is required so overlapping
// selectors cannot make two CacheBackends provision servers for one engine.
// Generation is intentionally not required for lifecycle demand: after a
// CacheBackend update, existing engines remain a reason to keep their node's
// server alive while status reports that those immutable Pods need recreation.
func (r *CacheBackendReconciler) nodeLocalEngineDemand(ctx context.Context, backend *cachev1alpha1.CacheBackend) (map[string]*corev1.Pod, error) {
	out := map[string]*corev1.Pod{}
	selector := backend.Spec.EngineSelector
	if selector == nil || len(selector.MatchLabels) == 0 {
		return out, nil
	}
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(selector.MatchLabels)},
	); err != nil {
		return nil, fmt.Errorf("list selected engines for NodeLocal CacheBackend %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	wantOwner := backend.Namespace + "/" + backend.Name
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed || pod.Spec.NodeName == "" {
			continue
		}
		if pod.Annotations[enginebinding.AnnotationInjectedBy] != wantOwner ||
			pod.Annotations[enginebinding.AnnotationInjectedByUID] != string(backend.UID) {
			continue
		}
		if out[pod.Spec.NodeName] == nil {
			out[pod.Spec.NodeName] = pod
		}
	}
	return out, nil
}

func (r *CacheBackendReconciler) cleanupLMCacheNodeLocalServerPods(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	if backend == nil || backend.UID == "" {
		return nil
	}
	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabels{
			enginebinding.LabelLMCacheNodeLocalServer: "true",
			enginebinding.LabelCacheBackendUID:        string(backend.UID),
		},
	); err != nil {
		return fmt.Errorf("list obsolete LMCache NodeLocal server Pods for %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	wantShmHostPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, backend) || pod.DeletionTimestamp != nil {
			continue
		}
		if nodeLocalServerUsesShmHostPath(pod, wantShmHostPath) {
			if err := r.ensureLMCacheNodeLocalCleanupPod(ctx, backend, pod); err != nil {
				return err
			}
		}
		if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete LMCache NodeLocal server Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	_, err = r.reconcileLMCacheNodeLocalCleanupPods(ctx, backend, nil, false)
	return err
}

func (r *CacheBackendReconciler) ensureLMCacheNodeLocalCleanupPod(ctx context.Context, backend *cachev1alpha1.CacheBackend, server *corev1.Pod) error {
	if backend == nil || server == nil || !metav1.IsControlledBy(server, backend) {
		return fmt.Errorf("create LMCache NodeLocal SHM cleanup intent: controlled source server is required")
	}
	nodeName := nodeLocalServerTargetNode(server)
	return r.ensureLMCacheNodeLocalCleanupForNode(ctx, backend, nodeName, server)
}

func (r *CacheBackendReconciler) ensureLMCacheNodeLocalCleanupForNode(ctx context.Context, backend *cachev1alpha1.CacheBackend, nodeName string, source *corev1.Pod) error {
	var cleanups corev1.PodList
	if err := r.Client.List(ctx, &cleanups, client.InNamespace(backend.Namespace), client.MatchingLabels{
		enginebinding.LabelLMCacheNodeLocalCleanup: "true",
		enginebinding.LabelCacheBackendUID:         string(backend.UID),
	}); err != nil {
		return fmt.Errorf("list existing LMCache NodeLocal SHM cleanup Pods for %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	for i := range cleanups.Items {
		cleanup := &cleanups.Items[i]
		if metav1.IsControlledBy(cleanup, backend) && cleanup.Annotations[enginebinding.AnnotationNodeLocalTargetNode] == nodeName {
			if builtinruntime.IsLMCacheNodeLocalCleanupPod(cleanup, backend, nodeName, r.NodeLocalShmCleanupImage) &&
				builtinruntime.LMCacheNodeLocalCleanupIsGated(cleanup) {
				return nil
			}
			if err := r.Client.Delete(ctx, cleanup); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete invalid LMCache NodeLocal SHM cleanup Pod %s/%s: %w", cleanup.Namespace, cleanup.Name, err)
			}
			return fmt.Errorf("deleted invalid LMCache NodeLocal SHM cleanup Pod %s/%s", cleanup.Namespace, cleanup.Name)
		}
	}

	desired, err := builtinruntime.RenderLMCacheNodeLocalCleanupPod(backend, nodeName, r.NodeLocalShmCleanupImage, source)
	if err != nil {
		return err
	}
	if err := controllerutil.SetControllerReference(backend, desired, r.Scheme); err != nil {
		return fmt.Errorf("own LMCache NodeLocal SHM cleanup Pod %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if err := r.Client.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create LMCache NodeLocal SHM cleanup Pod %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		var existing corev1.Pod
		if getErr := r.Client.Get(ctx, client.ObjectKeyFromObject(desired), &existing); getErr != nil {
			return fmt.Errorf("inspect existing LMCache NodeLocal SHM cleanup Pod %s/%s: %w", desired.Namespace, desired.Name, getErr)
		}
		if !builtinruntime.IsLMCacheNodeLocalCleanupPod(&existing, backend, nodeName, r.NodeLocalShmCleanupImage) ||
			!builtinruntime.LMCacheNodeLocalCleanupIsGated(&existing) {
			return fmt.Errorf("LMCache NodeLocal SHM cleanup Pod name %s/%s is occupied by another object", desired.Namespace, desired.Name)
		}
	}
	return nil
}

func (r *CacheBackendReconciler) ensureLMCacheNodeLocalCleanupForConsumers(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	if backend == nil || backend.UID == "" {
		return nil
	}
	wantPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		return err
	}
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(backend.Namespace)); err != nil {
		return fmt.Errorf("list NodeLocal SHM consumers for finalization of %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	wantOwner := backend.Namespace + "/" + backend.Name
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == "" || pod.Labels[enginebinding.LabelLMCacheNodeLocalCleanup] == "true" ||
			pod.Annotations[enginebinding.AnnotationInjectedBy] != wantOwner ||
			pod.Annotations[enginebinding.AnnotationInjectedByUID] != string(backend.UID) {
			continue
		}
		usesPath := false
		for j := range pod.Spec.Volumes {
			usesPath = usesPath || (pod.Spec.Volumes[j].HostPath != nil && pod.Spec.Volumes[j].HostPath.Path == wantPath)
		}
		if usesPath {
			if err := r.ensureLMCacheNodeLocalCleanupForNode(ctx, backend, pod.Spec.NodeName, pod); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *CacheBackendReconciler) finalizeLMCacheNodeLocal(ctx context.Context, backend *cachev1alpha1.CacheBackend) (ctrl.Result, error) {
	if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureLMCacheNodeLocalCleanupForConsumers(ctx, backend); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.cleanupLMCacheNodeLocalServerPods(ctx, backend); err != nil {
		return ctrl.Result{}, err
	}
	if _, err := r.reconcileLMCacheNodeLocalCleanupPods(ctx, backend, nil, false); err != nil {
		return ctrl.Result{}, err
	}

	var servers, cleanups corev1.PodList
	if err := r.Client.List(ctx, &servers, client.InNamespace(backend.Namespace), client.MatchingLabels{
		enginebinding.LabelLMCacheNodeLocalServer: "true",
		enginebinding.LabelCacheBackendUID:        string(backend.UID),
	}); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Client.List(ctx, &cleanups, client.InNamespace(backend.Namespace), client.MatchingLabels{
		enginebinding.LabelLMCacheNodeLocalCleanup: "true",
		enginebinding.LabelCacheBackendUID:         string(backend.UID),
	}); err != nil {
		return ctrl.Result{}, err
	}
	if len(servers.Items) != 0 || len(cleanups.Items) != 0 {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	consumers, err := r.nodeLocalShmConsumers(ctx, backend, "")
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(consumers) != 0 {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	before := backend.DeepCopy()
	controllerutil.RemoveFinalizer(backend, nodeLocalShmCleanupFinalizer)
	if err := r.Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove NodeLocal SHM cleanup finalizer from %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	return ctrl.Result{}, nil
}

// reconcileLMCacheNodeLocalCleanupPods advances existing cleanup intents. A
// gated cleanup is cancelled when normal engine demand returns before it
// starts; once scheduled, it blocks server recreation until it succeeds.
func (r *CacheBackendReconciler) reconcileLMCacheNodeLocalCleanupPods(ctx context.Context, backend *cachev1alpha1.CacheBackend, demand map[string]*corev1.Pod, cancelOnDemand bool) (map[string]bool, error) {
	blocked := map[string]bool{}
	if backend == nil || backend.UID == "" {
		return blocked, nil
	}
	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods, client.InNamespace(backend.Namespace), client.MatchingLabels{
		enginebinding.LabelLMCacheNodeLocalCleanup: "true",
		enginebinding.LabelCacheBackendUID:         string(backend.UID),
	}); err != nil {
		return nil, fmt.Errorf("list LMCache NodeLocal SHM cleanup Pods for %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	activeByNode := map[string]bool{}
	succeededByNode := map[string]bool{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, backend) {
			continue
		}
		nodeName := pod.Annotations[enginebinding.AnnotationNodeLocalTargetNode]
		if !builtinruntime.IsLMCacheNodeLocalCleanupPod(pod, backend, nodeName, r.NodeLocalShmCleanupImage) {
			if pod.DeletionTimestamp == nil {
				if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("delete invalid LMCache NodeLocal SHM cleanup Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
			}
			return nil, fmt.Errorf("delete invalid LMCache NodeLocal SHM cleanup Pod %s/%s", pod.Namespace, pod.Name)
		}
		switch {
		case builtinruntime.LMCacheNodeLocalCleanupSucceeded(pod, nodeName):
			succeededByNode[nodeName] = true
		case pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded:
		default:
			if pod.DeletionTimestamp == nil {
				activeByNode[nodeName] = true
			}
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, backend) {
			continue
		}
		nodeName := pod.Annotations[enginebinding.AnnotationNodeLocalTargetNode]
		if builtinruntime.LMCacheNodeLocalCleanupSucceeded(pod, nodeName) {
			if pod.DeletionTimestamp == nil {
				if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("delete completed LMCache NodeLocal SHM cleanup Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
			}
			continue
		}
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			blocked[nodeName] = true
			if !succeededByNode[nodeName] && !activeByNode[nodeName] {
				retry, err := builtinruntime.RenderLMCacheNodeLocalCleanupRetryPod(backend, pod)
				if err != nil {
					return nil, err
				}
				if err := controllerutil.SetControllerReference(backend, retry, r.Scheme); err != nil {
					return nil, fmt.Errorf("own LMCache NodeLocal SHM cleanup retry Pod %s/%s: %w", retry.Namespace, retry.Name, err)
				}
				if err := r.Client.Create(ctx, retry); err != nil {
					if !apierrors.IsAlreadyExists(err) {
						return nil, fmt.Errorf("create LMCache NodeLocal SHM cleanup retry Pod %s/%s: %w", retry.Namespace, retry.Name, err)
					}
					var existing corev1.Pod
					if getErr := r.Client.Get(ctx, client.ObjectKeyFromObject(retry), &existing); getErr != nil {
						return nil, fmt.Errorf("inspect existing LMCache NodeLocal SHM cleanup retry Pod %s/%s: %w", retry.Namespace, retry.Name, getErr)
					}
					if !metav1.IsControlledBy(&existing, backend) {
						return nil, fmt.Errorf("LMCache NodeLocal SHM cleanup retry Pod name %s/%s is occupied by another object", retry.Namespace, retry.Name)
					}
				}
				activeByNode[nodeName] = true
			}
			if pod.DeletionTimestamp == nil {
				if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("delete failed LMCache NodeLocal SHM cleanup Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
			}
			continue
		}
		gated := len(pod.Spec.SchedulingGates) > 0
		if gated && cancelOnDemand && demand[nodeName] != nil {
			if pod.DeletionTimestamp == nil {
				if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("cancel LMCache NodeLocal SHM cleanup Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
			}
			continue
		}
		blocked[nodeName] = true
		if !gated || pod.DeletionTimestamp != nil {
			continue
		}
		consumers, err := r.nodeLocalShmConsumers(ctx, backend, nodeName)
		if err != nil {
			return nil, err
		}
		if len(consumers) != 0 {
			continue
		}
		// A managed Engine admitted after this snapshot cannot use the UID
		// directory while cleanup runs: only its main container receives the
		// inference-cache-authored SHM mount, and that container remains behind
		// the same-node startup gate. This cleanup Pod keeps Server recreation
		// blocked until it succeeds, so the gate cannot complete early. Races
		// from unmanaged processes mounting the hostPath are outside the trusted
		// node boundary, but the cluster-wide snapshot above still waits for any
		// such Pod that already exists.
		before := pod.DeepCopy()
		pod.Spec.SchedulingGates = nil
		if err := r.Client.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return nil, fmt.Errorf("release LMCache NodeLocal SHM cleanup Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return blocked, nil
}

func (r *CacheBackendReconciler) nodeLocalShmConsumers(ctx context.Context, backend *cachev1alpha1.CacheBackend, nodeName string) ([]string, error) {
	wantPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		return nil, err
	}
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods); err != nil {
		return nil, fmt.Errorf("list cluster-wide SHM consumers for NodeLocal CacheBackend %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	consumers := []string{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if nodeName != "" && pod.Spec.NodeName != nodeName {
			continue
		}
		if pod.Namespace == backend.Namespace && pod.Labels[enginebinding.LabelLMCacheNodeLocalCleanup] == "true" && metav1.IsControlledBy(pod, backend) {
			continue
		}
		for j := range pod.Spec.Volumes {
			hostPath := pod.Spec.Volumes[j].HostPath
			if hostPath != nil && hostPath.Path == wantPath {
				consumers = append(consumers, pod.Namespace+"/"+pod.Name)
				break
			}
		}
	}
	return consumers, nil
}
