// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

const (
	conditionTypeConnectorReady     = "ConnectorReady"
	conditionTypeRemoteStorageReady = "RemoteStorageReady"

	reasonConnectorReady            = "ConnectorReady"
	reasonConnectorUnverified       = "ConnectorInjectionUnverified"
	reasonNoEnginePods              = "NoEnginePods"
	reasonMPServersNotReady         = "MPServersNotReady"
	reasonNodeLocalPoolPending      = "NodeLocalServerPoolPending"
	reasonNodeLocalHostPortConflict = "NodeLocalHostPortConflict"
	reasonNodeLocalWorkerCapacity   = "NodeLocalWorkerCapacityExceeded"
	reasonNodeLocalAmbiguousServers = "AmbiguousNodeLocalServers"
	reasonRemoteStorageReady        = "RemoteStorageReady"
	reasonRemoteStorageAbsent       = "RemoteStorageNotConfigured"
	reasonRemoteStoragePending      = "RemoteStoragePending"
	reasonRemoteStorageUnavailable  = "RemoteStorageUnavailable"

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
	if !isTypedLMCacheMP(backend) {
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
	if isTypedLMCacheNodeLocal(backend) {
		r.refreshLMCacheNodeLocalConnectorStatus(ctx, backend)
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
	engineCoverage := make([]cachev1alpha1.CacheBackendEnginePodCoverageStatus, 0, len(pods.Items))
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
		coverageReason := reasonMPServersNotReady
		if !injected {
			coverageReason = reasonConnectorUnverified
		} else if serverReady {
			coverageReason = reasonConnectorReady
		}
		engineCoverage = append(engineCoverage, cachev1alpha1.CacheBackendEnginePodCoverageStatus{
			Name: pod.Name, NodeName: pod.Spec.NodeName, Ready: podReady(pod), Covered: serverReady, Reason: coverageReason,
		})
		if serverReady {
			readyServers++
			covered++
			if podReady(pod) {
				readyEngines++
			}
		}
	}
	sort.Slice(engineCoverage, func(i, j int) bool { return engineCoverage[i].Name < engineCoverage[j].Name })

	connector := &cachev1alpha1.CacheBackendConnectorStatus{
		Mode:                cachev1alpha1.LMCacheConnectorModeMultiprocess,
		Topology:            cachev1alpha1.LMCacheTopologyPodLocal,
		MatchedEnginePods:   matched,
		ReadyEnginePods:     readyEngines,
		DesiredServers:      desiredServers,
		ReadyServers:        readyServers,
		CoveredEnginePods:   covered,
		UncoveredEnginePods: matched - covered,
		EnginePodCoverage:   engineCoverage,
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

func (r *CacheBackendReconciler) refreshLMCacheNodeLocalConnectorStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend) {
	// Admission rejects incomplete NodeLocal objects, but a controller can still
	// encounter an older or admission-bypassed object. Dispatch reports the
	// renderer error; keep status refresh fail-soft instead of dereferencing a
	// missing server declaration on that error path.
	if backend == nil || backend.Spec.LMCache == nil || backend.Spec.LMCache.NodeLocal == nil ||
		backend.Spec.LMCache.NodeLocal.Server == nil {
		log.FromContext(ctx).V(1).Info("LMCache NodeLocal status refresh skipped: server configuration is incomplete")
		return
	}
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var engines corev1.PodList
	selector := backend.Spec.EngineSelector
	if selector != nil && len(selector.MatchLabels) > 0 {
		if err := reader.List(ctx, &engines,
			client.InNamespace(backend.Namespace),
			client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(selector.MatchLabels)},
		); err != nil {
			log.FromContext(ctx).V(1).Info("LMCache NodeLocal status refresh skipped: engine pod list failed", "error", err.Error())
			return
		}
	}

	var servers corev1.PodList
	if err := reader.List(ctx, &servers,
		client.InNamespace(backend.Namespace),
		client.MatchingLabels{
			enginebinding.LabelLMCacheNodeLocalServer: "true",
			enginebinding.LabelCacheBackendUID:        string(backend.UID),
		},
	); err != nil {
		log.FromContext(ctx).V(1).Info("LMCache NodeLocal status refresh skipped: server pod list failed", "error", err.Error())
		return
	}

	wantOwner := backend.Namespace + "/" + backend.Name
	wantUID := string(backend.UID)
	wantGeneration := strconv.FormatInt(backend.Generation, 10)
	wantShmName, shmNameErr := builtinruntime.NodeLocalServerShmName(backend)
	if shmNameErr != nil {
		log.FromContext(ctx).V(1).Info("LMCache NodeLocal status refresh skipped: shared-memory identity is invalid", "error", shmNameErr.Error())
		return
	}
	readyByNode := map[string]int32{}
	conflictByNode := map[string]bool{}
	for i := range servers.Items {
		pod := &servers.Items[i]
		if !metav1.IsControlledBy(pod, backend) || pod.DeletionTimestamp != nil {
			continue
		}
		annotations := pod.GetAnnotations()
		if annotations[enginebinding.AnnotationNodeLocalOwner] != wantOwner ||
			annotations[enginebinding.AnnotationNodeLocalOwnerUID] != wantUID ||
			annotations[enginebinding.AnnotationNodeLocalGeneration] != wantGeneration ||
			!nodeLocalServerHasShmIdentity(pod, wantShmName) {
			continue
		}
		targetNode := annotations[enginebinding.AnnotationNodeLocalTargetNode]
		if nodeLocalHostPortConflict(pod) {
			conflictByNode[targetNode] = true
		}
		if targetNode != "" && pod.Spec.NodeName == targetNode && podReady(pod) && normalContainerReady(pod.Status.ContainerStatuses, lmCacheMPServerStatusContainerName) {
			readyByNode[targetNode]++
		}
	}
	activeEngines := make([]*corev1.Pod, 0, len(engines.Items))
	engineCountByNode := map[string]int32{}
	desiredNodes := map[string]struct{}{}
	for i := range engines.Items {
		pod := &engines.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		activeEngines = append(activeEngines, pod)
		annotations := pod.GetAnnotations()
		owned := annotations[enginebinding.AnnotationInjectedBy] == wantOwner &&
			wantUID != "" && annotations[enginebinding.AnnotationInjectedByUID] == wantUID
		if owned && pod.Spec.NodeName != "" {
			engineCountByNode[pod.Spec.NodeName]++
			desiredNodes[pod.Spec.NodeName] = struct{}{}
		}
	}
	desiredServers := int32(len(desiredNodes))
	readyServers := int32(0)
	hostPortConflict := false
	for node := range desiredNodes {
		if readyByNode[node] == 1 {
			readyServers++
		}
		if conflictByNode[node] {
			hostPortConflict = true
		}
	}
	matched := int32(len(activeEngines))
	verified, covered, readyEngines := int32(0), int32(0), int32(0)
	engineCoverage := make([]cachev1alpha1.CacheBackendEnginePodCoverageStatus, 0, len(activeEngines))
	workerCapacityExceeded := false
	ambiguousServers := false
	maxGPUWorkers := backend.Spec.LMCache.NodeLocal.Server.MaxGPUWorkers
	for _, pod := range activeEngines {
		annotations := pod.GetAnnotations()
		injected := annotations[enginebinding.AnnotationInjectedBy] == wantOwner &&
			wantUID != "" && annotations[enginebinding.AnnotationInjectedByUID] == wantUID &&
			annotations[enginebinding.AnnotationInjectedGeneration] == wantGeneration
		if injected {
			verified++
		}
		node := pod.Spec.NodeName
		withinWorkers := node != "" && engineCountByNode[node] > 0 && engineCountByNode[node] <= maxGPUWorkers
		if injected && node != "" && !withinWorkers {
			workerCapacityExceeded = true
		}
		isCovered := injected && withinWorkers && readyByNode[node] == 1
		coverageReason := reasonMPServersNotReady
		switch {
		case !injected:
			coverageReason = reasonConnectorUnverified
		case node == "":
			coverageReason = "EngineSchedulingPending"
		case !withinWorkers:
			coverageReason = reasonNodeLocalWorkerCapacity
		case readyByNode[node] > 1:
			ambiguousServers = true
			coverageReason = reasonNodeLocalAmbiguousServers
		case isCovered:
			coverageReason = reasonConnectorReady
		}
		engineCoverage = append(engineCoverage, cachev1alpha1.CacheBackendEnginePodCoverageStatus{
			Name: pod.Name, NodeName: node, Ready: podReady(pod), Covered: isCovered, Reason: coverageReason,
		})
		if isCovered {
			covered++
			if podReady(pod) {
				readyEngines++
			}
		}
	}
	sort.Slice(engineCoverage, func(i, j int) bool { return engineCoverage[i].Name < engineCoverage[j].Name })

	connector := &cachev1alpha1.CacheBackendConnectorStatus{
		Mode:                cachev1alpha1.LMCacheConnectorModeMultiprocess,
		Topology:            cachev1alpha1.LMCacheTopologyNodeLocal,
		MatchedEnginePods:   matched,
		ReadyEnginePods:     readyEngines,
		DesiredServers:      desiredServers,
		ReadyServers:        readyServers,
		CoveredEnginePods:   covered,
		UncoveredEnginePods: matched - covered,
		EnginePodCoverage:   engineCoverage,
	}
	status, reason, message := metav1.ConditionFalse, reasonNodeLocalPoolPending,
		fmt.Sprintf("%d/%d engine-demanded LMCache MP servers are Ready; %d/%d selected engine Pods have exactly one healthy same-node server", readyServers, desiredServers, covered, matched)
	switch {
	case matched == 0:
		reason = reasonNoEnginePods
		message = "no active engine Pods match spec.engineSelector"
	case verified != matched:
		status = metav1.ConditionUnknown
		reason = reasonConnectorUnverified
		message = fmt.Sprintf("%d/%d selected engine Pods carry the current CacheBackend name, UID, and generation injection record", verified, matched)
	case desiredServers == 0:
		reason = reasonNodeLocalPoolPending
		message = "selected engine Pods have not been scheduled onto a node yet"
	case hostPortConflict:
		reason = reasonNodeLocalHostPortConflict
		message = "a NodeLocal server Pod is unschedulable because a declared host port is already allocated; choose disjoint MP and HTTP ports"
	case workerCapacityExceeded:
		reason = reasonNodeLocalWorkerCapacity
		message = fmt.Sprintf("at least one node has more selected engine instances than maxGPUWorkers=%d", maxGPUWorkers)
	case ambiguousServers:
		reason = reasonNodeLocalAmbiguousServers
		message = "more than one healthy current-generation NodeLocal server claims the same engine node"
	case readyServers == desiredServers && covered == matched && readyEngines == matched:
		status = metav1.ConditionTrue
		reason = reasonConnectorReady
		message = fmt.Sprintf("all %d selected engine Pods are Ready and covered by exactly one healthy same-node server; all %d engine-demanded servers are Ready", matched, desiredServers)
	}

	before := backend.DeepCopy()
	backend.Status.Connector = connector
	meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
		Type: conditionTypeConnectorReady, Status: status, Reason: reason, Message: message,
		ObservedGeneration: backend.Generation,
	})
	if equality.Semantic.DeepEqual(before.Status, backend.Status) {
		return
	}
	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		backend.Status = before.Status
		log.FromContext(ctx).V(1).Info("LMCache NodeLocal connector status refresh skipped: patch failed", "error", err.Error())
	}
}

func normalContainerReady(statuses []corev1.ContainerStatus, name string) bool {
	for i := range statuses {
		if statuses[i].Name == name {
			return statuses[i].Ready && statuses[i].State.Running != nil
		}
	}
	return false
}

func nodeLocalHostPortConflict(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		condition := pod.Status.Conditions[i]
		if condition.Type != corev1.PodScheduled || condition.Status != corev1.ConditionFalse || condition.Reason != corev1.PodReasonUnschedulable {
			continue
		}
		message := strings.ToLower(condition.Message)
		if strings.Contains(message, "free ports") || strings.Contains(message, "hostport") || strings.Contains(message, "host port") {
			return true
		}
	}
	return false
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

// lmCacheMPReadyBase aggregates the required typed MP connector and optional
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
	if !isTypedLMCacheMP(backend) {
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
			"the LMCache MP connector is ready; remote storage is degraded but does not gate Ready while failOpen is true"
	}
	return metav1.ConditionTrue, reasonConnectorReady, connector.Message
}

func setRemoteStorageStatus(backend *cachev1alpha1.CacheBackend, endpoint string, ready metav1.ConditionStatus, reason, message string, observedGeneration int64) {
	if !isTypedLMCacheMP(backend) {
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
