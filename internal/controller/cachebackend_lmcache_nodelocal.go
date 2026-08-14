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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

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
		return r.cleanupLMCacheNodeLocalServerPods(ctx, backend)
	}
	demand, err := r.nodeLocalEngineDemand(ctx, backend)
	if err != nil {
		return err
	}
	wantShmName, err := builtinruntime.NodeLocalServerShmName(backend)
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

	wantGeneration := strconv.FormatInt(backend.Generation, 10)
	liveByNode := make(map[string]*corev1.Pod, len(servers.Items))
	for i := range servers.Items {
		pod := &servers.Items[i]
		if !metav1.IsControlledBy(pod, backend) {
			continue
		}
		targetNode := pod.Annotations[enginebinding.AnnotationNodeLocalTargetNode]
		_, wanted := demand[targetNode]
		current := pod.Annotations[enginebinding.AnnotationNodeLocalOwnerUID] == string(backend.UID) &&
			pod.Annotations[enginebinding.AnnotationNodeLocalGeneration] == wantGeneration &&
			pod.Name == builtinruntime.NodeLocalServerPodName(backend.Name, targetNode) &&
			nodeLocalServerHasShmIdentity(pod, wantShmName, wantShmHostPath)
		if !current {
			if pod.DeletionTimestamp == nil {
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
		if liveByNode[nodeName] != nil {
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

func nodeLocalServerHasShmIdentity(pod *corev1.Pod, wantName, wantHostPath string) bool {
	if pod == nil || wantName == "" || wantHostPath == "" || pod.Annotations[enginebinding.AnnotationNodeLocalShmName] != wantName {
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
		args := container.Args
		for j := 0; j+1 < len(args); j++ {
			if args[j] == "--shm-name" && args[j+1] == wantName {
				return true
			}
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
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, backend) || pod.DeletionTimestamp != nil {
			continue
		}
		if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete LMCache NodeLocal server Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}
