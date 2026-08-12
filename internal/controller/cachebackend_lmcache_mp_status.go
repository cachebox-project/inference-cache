// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

const (
	conditionTypeConnectorReady     = "ConnectorReady"
	conditionTypeRemoteStorageReady = "RemoteStorageReady"

	reasonConnectorReady           = "ConnectorReady"
	reasonConnectorUnverified      = "ConnectorInjectionUnverified"
	reasonNoEnginePods             = "NoEnginePods"
	reasonMPServersNotReady        = "MPServersNotReady"
	reasonRemoteStorageReady       = "RemoteStorageReady"
	reasonRemoteStorageAbsent      = "RemoteStorageNotConfigured"
	reasonRemoteStoragePending     = "RemoteStoragePending"
	reasonRemoteStorageUnavailable = "RemoteStorageUnavailable"

	lmCacheMPServerStatusContainerName = "lmcache-mp-server"
)

func isTypedLMCachePodLocal(backend *cachev1alpha1.CacheBackend) bool {
	return backend != nil &&
		backend.Spec.EffectiveCacheType() == cachev1alpha1.CacheBackendTypeLMCache &&
		backend.Spec.LMCache != nil &&
		backend.Spec.LMCache.Topology == cachev1alpha1.LMCacheTopologyPodLocal
}

// refreshLMCacheMPConnectorStatus projects PodLocal server health independently
// from the optional remote L3. Native sidecars report their state under
// status.initContainerStatuses, not containerStatuses.
//
// Like matchedEnginePods, this is a bounded-cadence observation rather than a
// cluster-wide Pod watch. List/patch errors preserve the prior verdict and are
// fail-soft so connector observability cannot block normal reconciliation.
func (r *CacheBackendReconciler) refreshLMCacheMPConnectorStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend) {
	if !isTypedLMCachePodLocal(backend) {
		if backend.Status.Connector == nil && meta.FindStatusCondition(backend.Status.Conditions, conditionTypeConnectorReady) == nil {
			return
		}
		before := backend.DeepCopy()
		backend.Status.Connector = nil
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeConnectorReady)
		if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
			backend.Status = before.Status
			log.FromContext(ctx).V(1).Info("LMCache MP connector status clear skipped: patch failed",
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		}
		return
	}

	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	sel := backend.Spec.EngineSelector
	if sel != nil && len(sel.MatchLabels) > 0 {
		if err := reader.List(ctx, &pods,
			client.InNamespace(backend.Namespace),
			client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(sel.MatchLabels)},
		); err != nil {
			log.FromContext(ctx).V(1).Info("LMCache MP connector status refresh skipped: pod list failed",
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
			return
		}
	}

	var matched, verified, readyEngines, desiredServers, readyServers, covered int32
	wantInjectedBy := backend.Namespace + "/" + backend.Name
	wantUID := string(backend.UID)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		matched++
		desiredServers++
		annotations := pod.GetAnnotations()
		injected := annotations[enginebinding.AnnotationInjectedBy] == wantInjectedBy &&
			wantUID != "" && annotations[enginebinding.AnnotationInjectedByUID] == wantUID &&
			annotations[enginebinding.AnnotationInjectedGeneration] == strconv.FormatInt(backend.Generation, 10)
		if injected {
			verified++
		}
		serverReady := injected && nativeSidecarReady(pod.Status.InitContainerStatuses, lmCacheMPServerStatusContainerName)
		if serverReady {
			readyServers++
			covered++
			if podReady(pod) {
				readyEngines++
			}
		}
	}

	connector := &cachev1alpha1.CacheBackendConnectorStatus{
		Mode:                cachev1alpha1.LMCacheConnectorModeMultiprocess,
		Topology:            cachev1alpha1.LMCacheTopologyPodLocal,
		MatchedEnginePods:   matched,
		ReadyEnginePods:     readyEngines,
		DesiredServers:      desiredServers,
		ReadyServers:        readyServers,
		CoveredEnginePods:   covered,
		UncoveredEnginePods: matched - covered,
	}
	status, reason, message := metav1.ConditionFalse, reasonMPServersNotReady,
		fmt.Sprintf("%d/%d selected engine Pods have a Ready LMCache MP native sidecar; %d engine Pods are Ready with the connector", readyServers, desiredServers, readyEngines)
	if matched == 0 {
		reason = reasonNoEnginePods
		message = "no active engine Pods match spec.engineSelector"
	} else if verified != matched {
		status = metav1.ConditionUnknown
		reason = reasonConnectorUnverified
		message = fmt.Sprintf("%d/%d selected engine Pods carry the webhook-authenticated injection record for this CacheBackend generation; unverified Pods are left un-injected", verified, matched)
	} else if readyServers == desiredServers && readyEngines == matched {
		status = metav1.ConditionTrue
		reason = reasonConnectorReady
		message = fmt.Sprintf("all %d selected engine Pods have a Ready LMCache MP server and connector", matched)
	}

	before := backend.DeepCopy()
	backend.Status.Connector = connector
	meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
		Type:               conditionTypeConnectorReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: backend.Generation,
	})
	if equality.Semantic.DeepEqual(before.Status, backend.Status) {
		return
	}
	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		backend.Status = before.Status
		log.FromContext(ctx).V(1).Info("LMCache MP connector status refresh skipped: patch failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
	}
}

func nativeSidecarReady(statuses []corev1.ContainerStatus, name string) bool {
	for i := range statuses {
		if statuses[i].Name == name {
			return statuses[i].Ready && statuses[i].State.Running != nil
		}
	}
	return false
}

func podReady(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return pod.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

// lmCacheMPReadyBase aggregates the required PodLocal connector and optional
// remote L3 without hiding either component's dedicated condition. The
// connector always gates Ready because SGLang cannot run with
// --enable-lmcache when its co-scheduled MP server is unavailable. Remote
// storage gates Ready only when the operator explicitly selects fail-closed;
// in the default fail-open mode the MP server can continue serving from L1.
//
// The connector condition is refreshed after the main reconcile dispatch, so
// a newly created or newly updated object can spend one reconcile at Unknown
// before the Pod observation is folded into Ready. That is preferable to
// briefly publishing Ready=True from the Redis workload alone.
func lmCacheMPReadyBase(
	backend *cachev1alpha1.CacheBackend,
	remoteStatus metav1.ConditionStatus,
	remoteReason, remoteMessage string,
) (metav1.ConditionStatus, string, string) {
	if !isTypedLMCachePodLocal(backend) {
		return remoteStatus, remoteReason, remoteMessage
	}

	connector := meta.FindStatusCondition(backend.Status.Conditions, conditionTypeConnectorReady)
	if connector == nil || connector.ObservedGeneration != backend.Generation {
		return metav1.ConditionUnknown, reasonConnectorUnverified,
			"connector health for the current CacheBackend generation has not been observed yet"
	}
	if connector.Status != metav1.ConditionTrue {
		return connector.Status, connector.Reason, connector.Message
	}

	storage := backend.Spec.EffectiveRemoteStorage()
	if storage != nil && !cachev1alpha1.IntegrationFailOpen(backend.Spec.Integration) && remoteStatus != metav1.ConditionTrue {
		return remoteStatus, remoteReason, remoteMessage
	}
	if storage != nil && remoteStatus != metav1.ConditionTrue {
		return metav1.ConditionTrue, reasonConnectorReady,
			"the Pod-local MP connector is ready; remote storage is degraded but does not gate Ready while failOpen is true"
	}
	return metav1.ConditionTrue, reasonConnectorReady, connector.Message
}

func setRemoteStorageStatus(backend *cachev1alpha1.CacheBackend, endpoint string, ready metav1.ConditionStatus, reason, message string, observedGeneration int64) {
	if !isTypedLMCachePodLocal(backend) {
		backend.Status.RemoteStorage = nil
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeRemoteStorageReady)
		return
	}
	storage := backend.Spec.EffectiveRemoteStorage()
	if storage == nil {
		backend.Status.RemoteStorage = nil
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeRemoteStorageReady)
		return
	}
	backend.Status.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageStatus{
		Provider: storage.Provider,
		Endpoint: endpoint,
		Ready:    ready,
	}
	meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
		Type:               conditionTypeRemoteStorageReady,
		Status:             ready,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
	})
}
