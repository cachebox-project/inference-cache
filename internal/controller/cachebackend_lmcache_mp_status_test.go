// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

func typedMPStatusBackend() *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1", UID: types.UID("cache-uid"), Generation: 3},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{
				"app": "sglang",
			}},
			LMCache: &cachev1alpha1.LMCacheEngineSpec{
				Topology: cachev1alpha1.LMCacheTopologyPodLocal,
				PodLocal: &cachev1alpha1.LMCachePodLocalSpec{Server: &cachev1alpha1.LMCachePodLocalServerSpec{
					Image:      "lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Port:       6500,
					L1Capacity: resource.MustParse("1Gi"),
					MaxWorkers: 1,
				}},
			},
		},
	}
}

func typedMPStatusPod(name string, injected, serverReady, engineReady bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns1", Labels: map[string]string{"app": "sglang"}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if injected {
		pod.Annotations = map[string]string{
			enginebinding.AnnotationInjectedBy:         "ns1/cache",
			enginebinding.AnnotationInjectedByUID:      "cache-uid",
			enginebinding.AnnotationInjectedGeneration: "3",
		}
	}
	if serverReady {
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
			Name:  lmCacheMPServerStatusContainerName,
			Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
	}
	if engineReady {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return pod
}

func TestRefreshLMCacheMPConnectorStatusTransitions(t *testing.T) {
	ctx := context.Background()
	scheme := newScheme(t)
	backend := typedMPStatusBackend()
	ready := typedMPStatusPod("ready", true, true, true)
	uncovered := typedMPStatusPod("uncovered", false, false, true)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &corev1.Pod{}).
		WithObjects(backend, ready, uncovered).Build()
	r := &CacheBackendReconciler{Client: c, APIReader: c}

	r.refreshLMCacheMPConnectorStatus(ctx, backend)
	var got cachev1alpha1.CacheBackend
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "cache"}, &got); err != nil {
		t.Fatalf("get backend: %v", err)
	}
	status := got.Status.Connector
	if status == nil || status.MatchedEnginePods != 2 || status.DesiredServers != 2 || status.ReadyServers != 1 ||
		status.ReadyEnginePods != 1 || status.CoveredEnginePods != 1 || status.UncoveredEnginePods != 1 {
		t.Fatalf("connector status = %+v", status)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, conditionTypeConnectorReady)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != reasonConnectorUnverified {
		t.Fatalf("ConnectorReady = %+v", cond)
	}

	// Simulate the previously uncovered Pod being recreated through the webhook
	// and the native sidecar becoming Ready. The next bounded-cadence refresh
	// transitions the connector independently of remote storage.
	var live corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "uncovered"}, &live); err != nil {
		t.Fatalf("get uncovered pod: %v", err)
	}
	live.Annotations = map[string]string{
		enginebinding.AnnotationInjectedBy:         "ns1/cache",
		enginebinding.AnnotationInjectedByUID:      "cache-uid",
		enginebinding.AnnotationInjectedGeneration: "3",
	}
	if err := c.Update(ctx, &live); err != nil {
		t.Fatalf("update pod annotations: %v", err)
	}
	live.Status.InitContainerStatuses = []corev1.ContainerStatus{{
		Name:  lmCacheMPServerStatusContainerName,
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}
	live.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(ctx, &live); err != nil {
		t.Fatalf("update pod status: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "cache"}, backend); err != nil {
		t.Fatalf("refresh backend object: %v", err)
	}
	r.refreshLMCacheMPConnectorStatus(ctx, backend)
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "cache"}, &got); err != nil {
		t.Fatalf("get transitioned backend: %v", err)
	}
	cond = meta.FindStatusCondition(got.Status.Conditions, conditionTypeConnectorReady)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != reasonConnectorReady {
		t.Fatalf("ConnectorReady after recovery = %+v", cond)
	}
	if got.Status.Connector.ReadyServers != 2 || got.Status.Connector.UncoveredEnginePods != 0 {
		t.Fatalf("connector status after recovery = %+v", got.Status.Connector)
	}

	// A CacheBackend spec generation change does not mutate existing Pods.
	// Their generation stamp makes the old render explicitly unverified until
	// the inference owner recreates/rolls them through admission.
	got.Generation = 4
	if err := c.Update(ctx, &got); err != nil {
		t.Fatalf("advance backend generation: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "cache"}, backend); err != nil {
		t.Fatalf("get generation-4 backend: %v", err)
	}
	r.refreshLMCacheMPConnectorStatus(ctx, backend)
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns1", Name: "cache"}, &got); err != nil {
		t.Fatalf("get stale-wiring backend: %v", err)
	}
	cond = meta.FindStatusCondition(got.Status.Conditions, conditionTypeConnectorReady)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != reasonConnectorUnverified {
		t.Fatalf("ConnectorReady after spec generation change = %+v", cond)
	}
}

func TestSetRemoteStorageStatusIndependentFromConnector(t *testing.T) {
	backend := typedMPStatusBackend()
	backend.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal,
		Endpoint:  "redis.example:6379",
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	backend.Status.Connector = &cachev1alpha1.CacheBackendConnectorStatus{ReadyServers: 0, DesiredServers: 1}
	setRemoteStorageStatus(backend, "redis.example:6379", metav1.ConditionTrue, reasonRemoteStorageReady, "ready", backend.Generation)
	if backend.Status.RemoteStorage == nil || backend.Status.RemoteStorage.Ready != metav1.ConditionTrue || backend.Status.RemoteStorage.Endpoint != "redis.example:6379" {
		t.Fatalf("remote storage status = %+v", backend.Status.RemoteStorage)
	}
	if backend.Status.Connector.ReadyServers != 0 {
		t.Fatalf("remote status update changed connector: %+v", backend.Status.Connector)
	}
	cond := meta.FindStatusCondition(backend.Status.Conditions, conditionTypeRemoteStorageReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("RemoteStorageReady = %+v", cond)
	}
}

func TestLMCacheMPReadyBase(t *testing.T) {
	falseV := false
	tests := []struct {
		name         string
		connector    *metav1.Condition
		remoteStatus metav1.ConditionStatus
		failClosed   bool
		wantStatus   metav1.ConditionStatus
		wantReason   string
	}{
		{
			name:         "current connector observation is required",
			remoteStatus: metav1.ConditionTrue,
			wantStatus:   metav1.ConditionUnknown,
			wantReason:   reasonConnectorUnverified,
		},
		{
			name: "connector failure always gates ready",
			connector: &metav1.Condition{
				Type: conditionTypeConnectorReady, Status: metav1.ConditionFalse,
				Reason: reasonMPServersNotReady, Message: "server down", ObservedGeneration: 3,
			},
			remoteStatus: metav1.ConditionTrue,
			wantStatus:   metav1.ConditionFalse,
			wantReason:   reasonMPServersNotReady,
		},
		{
			name: "remote failure is independent under default fail open",
			connector: &metav1.Condition{
				Type: conditionTypeConnectorReady, Status: metav1.ConditionTrue,
				Reason: reasonConnectorReady, Message: "connector ready", ObservedGeneration: 3,
			},
			remoteStatus: metav1.ConditionFalse,
			wantStatus:   metav1.ConditionTrue,
			wantReason:   reasonConnectorReady,
		},
		{
			name: "remote failure gates explicit fail closed",
			connector: &metav1.Condition{
				Type: conditionTypeConnectorReady, Status: metav1.ConditionTrue,
				Reason: reasonConnectorReady, Message: "connector ready", ObservedGeneration: 3,
			},
			remoteStatus: metav1.ConditionFalse,
			failClosed:   true,
			wantStatus:   metav1.ConditionFalse,
			wantReason:   reasonRemoteStorageUnavailable,
		},
		{
			name: "stale connector generation stays unknown",
			connector: &metav1.Condition{
				Type: conditionTypeConnectorReady, Status: metav1.ConditionTrue,
				Reason: reasonConnectorReady, Message: "old render", ObservedGeneration: 2,
			},
			remoteStatus: metav1.ConditionTrue,
			wantStatus:   metav1.ConditionUnknown,
			wantReason:   reasonConnectorUnverified,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := typedMPStatusBackend()
			backend.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
				Provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
				Redis:    &cachev1alpha1.RedisRemoteStorageSpec{},
			}
			if tc.failClosed {
				backend.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{FailOpen: &falseV}
			}
			if tc.connector != nil {
				meta.SetStatusCondition(&backend.Status.Conditions, *tc.connector)
			}

			gotStatus, gotReason, _ := lmCacheMPReadyBase(
				backend, tc.remoteStatus, reasonRemoteStorageUnavailable, "Redis unavailable",
			)
			if gotStatus != tc.wantStatus || gotReason != tc.wantReason {
				t.Fatalf("ready base = (%s, %q), want (%s, %q)", gotStatus, gotReason, tc.wantStatus, tc.wantReason)
			}
		})
	}
}

func TestMPConditionEventsFireOnTransitionOrObservedGeneration(t *testing.T) {
	backend := typedMPStatusBackend()
	meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
		Type: conditionTypeConnectorReady, Status: metav1.ConditionFalse,
		Reason: reasonMPServersNotReady, Message: "server restarting", ObservedGeneration: 3,
	})
	meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
		Type: conditionTypeRemoteStorageReady, Status: metav1.ConditionTrue,
		Reason: reasonRemoteStorageReady, Message: "Redis ready", ObservedGeneration: 3,
	})
	recorder := events.NewFakeRecorder(8)
	r := &CacheBackendReconciler{Recorder: recorder}

	r.emitTransitionEvents(backend, stateSnapshot{})
	got := strings.Join(drainEvents(recorder), "\n")
	if !strings.Contains(got, reasonMPServersNotReady) || !strings.Contains(got, reasonRemoteStorageReady) {
		t.Fatalf("transition events = %q", got)
	}

	before := snapshotState(backend)
	r.emitTransitionEvents(backend, before)
	if events := drainEvents(recorder); len(events) != 0 {
		t.Fatalf("steady-state events = %v", events)
	}
	condition := meta.FindStatusCondition(backend.Status.Conditions, conditionTypeConnectorReady)
	condition.ObservedGeneration = 4
	r.emitTransitionEvents(backend, before)
	got = strings.Join(drainEvents(recorder), "\n")
	if !strings.Contains(got, reasonMPServersNotReady) {
		t.Fatalf("generation-change events = %q", got)
	}
}
