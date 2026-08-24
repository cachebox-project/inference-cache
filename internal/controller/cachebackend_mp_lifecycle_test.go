// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

func nodeLocalBackend(name, namespace string) *cachev1alpha1.CacheBackend {
	backend := lmcacheBackend(name, namespace)
	backend.UID = types.UID("11111111-2222-3333-4444-555555555555")
	backend.Spec.RemoteStorage = nil
	backend.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "engine"}}
	backend.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
	backend.Spec.LMCache.PodLocal = nil
	backend.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{
		IdleRetentionSeconds: 300,
		Server: &cachev1alpha1.LMCacheNodeLocalServerSpec{
			Image: "registry.example/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Port:  6555, HTTPPort: 18080,
			L1Capacity: resource.MustParse("4Gi"), MaxGPUWorkers: 4, MaxCPUWorkers: 4,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("5Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("5Gi")},
			},
		},
		Scheduling: &cachev1alpha1.LMCacheNodeLocalSchedulingSpec{},
	}
	return backend
}

func nodeLocalEngine(backend *cachev1alpha1.CacheBackend, name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: backend.Namespace, Labels: map[string]string{"app": "engine"}, Annotations: map[string]string{
			enginebinding.AnnotationInjectedBy:         backend.Namespace + "/" + backend.Name,
			enginebinding.AnnotationInjectedByUID:      string(backend.UID),
			enginebinding.AnnotationInjectedGeneration: "1",
		}},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "vllm"}}},
	}
}

func TestNodeLocalCleanupHelperGuards(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	reconciler := newReconciler(newScheme(t), backend)
	if nodeLocalServerTargetNode(nil) != "" {
		t.Fatal("nil server Pod produced a target node")
	}
	server := &corev1.Pod{Spec: corev1.PodSpec{NodeName: "node-a"}}
	if nodeLocalServerTargetNode(server) != "node-a" {
		t.Fatal("server target did not fall back to spec.nodeName")
	}
	if nodeLocalServerUsesShmHostPath(nil, "/dev/shm/inference-cache/x") || nodeLocalServerUsesShmHostPath(server, "") {
		t.Fatal("incomplete server identity reported an SHM hostPath")
	}
	if err := reconciler.cleanupLMCacheNodeLocalServerPods(context.Background(), nil); err != nil {
		t.Fatalf("nil backend cleanup: %v", err)
	}
	if err := reconciler.ensureLMCacheNodeLocalCleanupPod(context.Background(), backend, nil); err == nil {
		t.Fatal("nil source server was accepted for cleanup intent")
	}
	if _, err := reconciler.reconcileLMCacheNodeLocalCleanupPods(context.Background(), nil, nil, false); err != nil {
		t.Fatalf("nil backend cleanup reconcile: %v", err)
	}
	if _, err := reconciler.nodeLocalShmConsumers(context.Background(), nil, ""); err == nil {
		t.Fatal("nil backend produced an SHM consumer snapshot")
	}
	if err := reconciler.ensureLMCacheNodeLocalCleanupForNode(context.Background(), backend, "", server); err == nil {
		t.Fatal("empty cleanup target node was accepted")
	}
}

func TestNodeLocalCleanupPropagatesListErrors(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	listErr := errors.New("list denied")
	reconciler := newReconcilerWithInterceptor(newScheme(t), interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return listErr
		},
	}, backend)
	server := nodeLocalEngine(backend, "engine-a", "node-a")
	checks := []func() error{
		func() error { return reconciler.cleanupLMCacheNodeLocalServerPods(context.Background(), backend) },
		func() error {
			return reconciler.ensureLMCacheNodeLocalCleanupForNode(context.Background(), backend, "node-a", server)
		},
		func() error {
			return reconciler.ensureLMCacheNodeLocalCleanupForConsumers(context.Background(), backend)
		},
		func() error {
			_, err := reconciler.reconcileLMCacheNodeLocalCleanupPods(context.Background(), backend, nil, false)
			return err
		},
		func() error {
			_, err := reconciler.nodeLocalShmConsumers(context.Background(), backend, "")
			return err
		},
	}
	for i, check := range checks {
		if err := check(); !errors.Is(err, listErr) {
			t.Fatalf("list error check %d = %v, want %v", i, err, listErr)
		}
	}
}

func markNodeLocalCleanupSucceeded(t *testing.T, reconciler *CacheBackendReconciler, pod *corev1.Pod, nodeName string) {
	t.Helper()
	pod.Spec.NodeName = nodeName
	if err := reconciler.Update(context.Background(), pod); err != nil {
		t.Fatalf("bind cleanup Pod to %s: %v", nodeName, err)
	}
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "lmcache-node-shm-cleanup",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 0,
		}},
	}}
	if err := reconciler.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("mark cleanup succeeded: %v", err)
	}
}

func TestReconcileNodeLocalFinalizerCompletesEmptyBackendDeletion(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	reconciler := newReconciler(newScheme(t), backend)
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	live := getBackend(t, reconciler, backend.Name, backend.Namespace)
	if !controllerutil.ContainsFinalizer(live, nodeLocalShmCleanupFinalizer) {
		t.Fatalf("NodeLocal cleanup finalizer was not installed: %v", live.Finalizers)
	}
	if err := reconciler.Delete(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(backend), &cachev1alpha1.CacheBackend{}); !apierrors.IsNotFound(err) {
		t.Fatalf("empty NodeLocal backend after finalization = %v, want NotFound", err)
	}
}

func TestReconcileNodeLocalCreatesServersOnlyForEngineNodes(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	reconciler := newReconciler(newScheme(t), backend)

	reconcile(t, reconciler, backend.Name, backend.Namespace)
	var empty corev1.PodList
	if err := reconciler.List(context.Background(), &empty, client.InNamespace(backend.Namespace), client.MatchingLabels{enginebinding.LabelLMCacheNodeLocalServer: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(empty.Items) != 0 {
		t.Fatalf("servers without scheduled engines = %d, want 0", len(empty.Items))
	}

	engineA := nodeLocalEngine(backend, "engine-a", "node-a")
	engineB := nodeLocalEngine(backend, "engine-b", "node-a")
	engineC := nodeLocalEngine(backend, "engine-c", "node-b")
	for _, engine := range []*corev1.Pod{engineA, engineB, engineC} {
		if err := reconciler.Create(context.Background(), engine); err != nil {
			t.Fatalf("create engine %s: %v", engine.Name, err)
		}
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	var servers corev1.PodList
	if err := reconciler.List(context.Background(), &servers, client.InNamespace(backend.Namespace), client.MatchingLabels{enginebinding.LabelLMCacheNodeLocalServer: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(servers.Items) != 2 {
		t.Fatalf("server Pods = %d, want one per distinct engine node", len(servers.Items))
	}
	for i := range servers.Items {
		server := &servers.Items[i]
		if !metav1.IsControlledBy(server, backend) || !server.Spec.HostNetwork || server.Spec.NodeName != "" {
			t.Fatalf("server ownership/placement = owner:%v hostNetwork:%v nodeName:%q", server.OwnerReferences, server.Spec.HostNetwork, server.Spec.NodeName)
		}
		target := server.Annotations[enginebinding.AnnotationNodeLocalTargetNode]
		if target != "node-a" && target != "node-b" {
			t.Fatalf("target node = %q", target)
		}
		if server.Name != builtinruntime.NodeLocalServerPodName(backend.Name, target) {
			t.Fatalf("server name = %q, target %q", server.Name, target)
		}
	}
	key := types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace}
	if err := reconciler.Get(context.Background(), key, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("NodeLocal MP Service lookup = %v, want NotFound", err)
	}
	if err := reconciler.Get(context.Background(), key, &appsv1.Deployment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("host-only NodeLocal provider Deployment lookup = %v, want NotFound", err)
	}
}

func TestReconcileNodeLocalRetainsIdleServerAndReusesItUntilExpiry(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engineA := nodeLocalEngine(backend, "engine-a", "node-a")
	engineB := nodeLocalEngine(backend, "engine-b", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engineA, engineB)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	serverKey := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	if err := reconciler.Delete(context.Background(), engineA); err != nil {
		t.Fatalf("delete first engine: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), serverKey, &corev1.Pod{}); err != nil {
		t.Fatalf("shared server removed while one engine remained: %v", err)
	}

	if err := reconciler.Delete(context.Background(), engineB); err != nil {
		t.Fatalf("delete last engine: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	var idle corev1.Pod
	if err := reconciler.Get(context.Background(), serverKey, &idle); err != nil {
		t.Fatalf("server was not retained after last engine left: %v", err)
	}
	idleSince := idle.Annotations[enginebinding.AnnotationNodeLocalIdleSince]
	if _, err := time.Parse(time.RFC3339Nano, idleSince); err != nil {
		t.Fatalf("idle-since annotation = %q: %v", idleSince, err)
	}
	originalUID := idle.UID

	replacement := nodeLocalEngine(backend, "engine-c", "node-a")
	if err := reconciler.Create(context.Background(), replacement); err != nil {
		t.Fatalf("create replacement engine: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	var reused corev1.Pod
	if err := reconciler.Get(context.Background(), serverKey, &reused); err != nil {
		t.Fatalf("get reused server: %v", err)
	}
	if reused.UID != originalUID {
		t.Fatalf("server UID changed during idle reuse: got %q, want %q", reused.UID, originalUID)
	}
	if _, found := reused.Annotations[enginebinding.AnnotationNodeLocalIdleSince]; found {
		t.Fatalf("idle marker was not removed after demand returned: %+v", reused.Annotations)
	}

	if err := reconciler.Delete(context.Background(), replacement); err != nil {
		t.Fatalf("delete replacement engine: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), serverKey, &idle); err != nil {
		t.Fatalf("get second idle server: %v", err)
	}
	before := idle.DeepCopy()
	idle.Annotations[enginebinding.AnnotationNodeLocalIdleSince] = time.Now().Add(-301 * time.Second).UTC().Format(time.RFC3339Nano)
	if err := reconciler.Patch(context.Background(), &idle, client.MergeFrom(before)); err != nil {
		t.Fatalf("age idle marker: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), serverKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("server after idle retention expired = %v, want NotFound", err)
	}
}

func TestReconcileNodeLocalZeroIdleRetentionDeletesImmediately(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	backend.Spec.LMCache.NodeLocal.IdleRetentionSeconds = 0
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	if err := reconciler.Delete(context.Background(), engine); err != nil {
		t.Fatalf("delete engine: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	serverKey := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	if err := reconciler.Get(context.Background(), serverKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("server with zero idle retention = %v, want NotFound", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	cleanupKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupPodName(backend.UID, "node-a"), Namespace: backend.Namespace}
	var cleanup corev1.Pod
	if err := reconciler.Get(context.Background(), cleanupKey, &cleanup); err != nil {
		t.Fatalf("get SHM cleanup Pod: %v", err)
	}
	if len(cleanup.Spec.SchedulingGates) != 0 || cleanup.Spec.HostNetwork || cleanup.Spec.HostPID || cleanup.Spec.HostIPC {
		t.Fatalf("released cleanup Pod = gates:%+v hostNetwork:%v hostPID:%v hostIPC:%v", cleanup.Spec.SchedulingGates, cleanup.Spec.HostNetwork, cleanup.Spec.HostPID, cleanup.Spec.HostIPC)
	}
	if cleanup.Spec.Containers[0].Image != reconciler.NodeLocalShmCleanupImage {
		t.Fatalf("cleanup image = %q, want platform helper %q", cleanup.Spec.Containers[0].Image, reconciler.NodeLocalShmCleanupImage)
	}
	markNodeLocalCleanupSucceeded(t, reconciler, &cleanup, "node-a")
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), cleanupKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("completed cleanup Pod = %v, want NotFound", err)
	}
}

func TestReconcileNodeLocalCancelsGatedCleanupWhenDemandReturns(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	backend.Spec.LMCache.NodeLocal.IdleRetentionSeconds = 0
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Delete(context.Background(), engine); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	replacement := nodeLocalEngine(backend, "engine-b", "node-a")
	if err := reconciler.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	cleanupKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupPodName(backend.UID, "node-a"), Namespace: backend.Namespace}
	if err := reconciler.Get(context.Background(), cleanupKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("gated cleanup after demand returned = %v, want NotFound", err)
	}
	serverKey := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	if err := reconciler.Get(context.Background(), serverKey, &corev1.Pod{}); err != nil {
		t.Fatalf("server was not recreated after cleanup cancellation: %v", err)
	}
}

func TestReconcileNodeLocalActiveCleanupBlocksServerForNewDemand(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	backend.Spec.LMCache.NodeLocal.IdleRetentionSeconds = 0
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Delete(context.Background(), engine); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	cleanupKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupPodName(backend.UID, "node-a"), Namespace: backend.Namespace}
	var cleanup corev1.Pod
	if err := reconciler.Get(context.Background(), cleanupKey, &cleanup); err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Spec.SchedulingGates) != 0 {
		t.Fatalf("cleanup gate was not released before new demand: %+v", cleanup.Spec.SchedulingGates)
	}

	replacement := nodeLocalEngine(backend, "engine-b", "node-a")
	if err := builtinruntime.NewVLLMLMCacheMPAdapter(builtinruntime.SubscriberConfig{}).InjectEngineConfig(&replacement.Spec, nil, backend); err != nil {
		t.Fatalf("inject replacement Engine startup gate: %v", err)
	}
	if len(replacement.Spec.InitContainers) != 1 || replacement.Spec.InitContainers[0].Name != "lmcache-node-local-gate" || len(replacement.Spec.InitContainers[0].VolumeMounts) != 0 {
		t.Fatalf("replacement Engine does not have an SHM-free startup gate: %+v", replacement.Spec.InitContainers)
	}
	if err := reconciler.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	serverKey := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	if err := reconciler.Get(context.Background(), serverKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("server while cleanup is active = %v, want NotFound", err)
	}
	markNodeLocalCleanupSucceeded(t, reconciler, &cleanup, "node-a")
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), serverKey, &corev1.Pod{}); err != nil {
		t.Fatalf("server after cleanup succeeded: %v", err)
	}
}

func TestReconcileNodeLocalCleanupWaitsForExactHostPathConsumer(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	backend.Spec.LMCache.NodeLocal.IdleRetentionSeconds = 0
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Delete(context.Background(), engine); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	wantPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		t.Fatal(err)
	}
	consumer := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "terminating-consumer", Namespace: "other-ns"}, Spec: corev1.PodSpec{
		NodeName: "node-a", Containers: []corev1.Container{{Name: "engine"}}, Volumes: []corev1.Volume{{
			Name: "shm", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: wantPath}},
		}},
	}}
	if err := reconciler.Create(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	cleanupKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupPodName(backend.UID, "node-a"), Namespace: backend.Namespace}
	var cleanup corev1.Pod
	if err := reconciler.Get(context.Background(), cleanupKey, &cleanup); err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Spec.SchedulingGates) != 1 {
		t.Fatalf("cleanup gate released while cross-namespace hostPath consumer remained: %+v", cleanup.Spec.SchedulingGates)
	}
	consumer.Status.Phase = corev1.PodSucceeded
	if err := reconciler.Status().Update(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), cleanupKey, &cleanup); err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Spec.SchedulingGates) != 0 {
		t.Fatalf("cleanup gate remained after consumer became terminal: %+v", cleanup.Spec.SchedulingGates)
	}
}

func TestNodeLocalShmConsumersIgnoresUnscheduledPod(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	wantPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		t.Fatal(err)
	}
	consumer := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unscheduled", Namespace: "other-ns"}, Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "engine"}},
		Volumes: []corev1.Volume{{Name: "shm", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: wantPath,
		}}}},
	}}
	reconciler := newReconciler(newScheme(t), backend, consumer)
	consumers, err := reconciler.nodeLocalShmConsumers(context.Background(), backend, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(consumers) != 0 {
		t.Fatalf("unscheduled Pod reported as SHM consumer: %v", consumers)
	}
}

func TestReconcileNodeLocalRetriesFailedCleanupPod(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	backend.Spec.LMCache.NodeLocal.IdleRetentionSeconds = 0
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Delete(context.Background(), engine); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	cleanupKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupPodName(backend.UID, "node-a"), Namespace: backend.Namespace}
	var failed corev1.Pod
	if err := reconciler.Get(context.Background(), cleanupKey, &failed); err != nil {
		t.Fatal(err)
	}
	failed.Status.Phase = corev1.PodFailed
	if err := reconciler.Status().Update(context.Background(), &failed); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	retryKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupRetryPodName(backend.UID, "node-a", failed.Name), Namespace: backend.Namespace}
	var retry corev1.Pod
	if err := reconciler.Get(context.Background(), retryKey, &retry); err != nil {
		t.Fatalf("get cleanup retry: %v", err)
	}
	if len(retry.Spec.SchedulingGates) != 1 {
		t.Fatalf("cleanup retry did not re-check quiescence: %+v", retry.Spec.SchedulingGates)
	}
	if retry.Annotations[nodeLocalShmCleanupRetryAnnotation] != "1" {
		t.Fatalf("cleanup retry count = %q, want 1", retry.Annotations[nodeLocalShmCleanupRetryAnnotation])
	}
	if err := reconciler.Get(context.Background(), cleanupKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed cleanup Pod after replacement = %v, want NotFound", err)
	}

	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), retryKey, &retry); err != nil {
		t.Fatal(err)
	}
	if len(retry.Spec.SchedulingGates) != 0 {
		t.Fatalf("cleanup retry gate remained without consumers: %+v", retry.Spec.SchedulingGates)
	}
	markNodeLocalCleanupSucceeded(t, reconciler, &retry, "node-a")
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), retryKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("completed cleanup retry = %v, want NotFound", err)
	}
}

func TestReconcileNodeLocalRetriesSucceededPodWithoutHelperStatus(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	cleanup, err := builtinruntime.RenderLMCacheNodeLocalCleanupPod(
		backend,
		"node-a",
		"registry.example/inference-cache-shm-cleanup@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		nodeLocalEngine(backend, "engine-a", "node-a"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(backend, cleanup, newScheme(t)); err != nil {
		t.Fatal(err)
	}
	cleanup.Spec.SchedulingGates = nil
	cleanup.Spec.NodeName = "node-a"
	cleanup.Status.Phase = corev1.PodSucceeded
	cleanup.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "foreign",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 0,
		}},
	}}
	reconciler := newReconciler(newScheme(t), backend, cleanup)
	if _, err := reconciler.reconcileLMCacheNodeLocalCleanupPods(context.Background(), backend, nil, false); err != nil {
		t.Fatal(err)
	}

	retryKey := types.NamespacedName{
		Name:      builtinruntime.NodeLocalCleanupRetryPodName(backend.UID, "node-a", cleanup.Name),
		Namespace: backend.Namespace,
	}
	var retry corev1.Pod
	if err := reconciler.Get(context.Background(), retryKey, &retry); err != nil {
		t.Fatalf("get retry after untrusted success status: %v", err)
	}
	if !builtinruntime.LMCacheNodeLocalCleanupIsGated(&retry) {
		t.Fatalf("retry after untrusted success status is not gated: %+v", retry.Spec.SchedulingGates)
	}
	if retry.Annotations[nodeLocalShmCleanupRetryAnnotation] != "1" {
		t.Fatalf("cleanup retry count = %q, want 1", retry.Annotations[nodeLocalShmCleanupRetryAnnotation])
	}
}

func TestReconcileNodeLocalStopsAfterCleanupRetryLimit(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	cleanup, err := builtinruntime.RenderLMCacheNodeLocalCleanupPod(
		backend,
		"node-a",
		"registry.example/inference-cache-shm-cleanup@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		nodeLocalEngine(backend, "engine-a", "node-a"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(backend, cleanup, newScheme(t)); err != nil {
		t.Fatal(err)
	}
	cleanup.Spec.SchedulingGates = nil
	cleanup.Spec.NodeName = "node-a"
	cleanup.Annotations[nodeLocalShmCleanupRetryAnnotation] = "3"
	cleanup.Status.Phase = corev1.PodFailed
	reconciler := newReconciler(newScheme(t), backend, cleanup)
	if _, err := reconciler.reconcileLMCacheNodeLocalCleanupPods(context.Background(), backend, nil, false); err != nil {
		t.Fatal(err)
	}

	retryKey := types.NamespacedName{
		Name:      builtinruntime.NodeLocalCleanupRetryPodName(backend.UID, "node-a", cleanup.Name),
		Namespace: backend.Namespace,
	}
	if err := reconciler.Get(context.Background(), retryKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cleanup retry beyond limit = %v, want NotFound", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(cleanup), &corev1.Pod{}); err != nil {
		t.Fatalf("terminal cleanup Pod was not retained for diagnosis: %v", err)
	}
}

func TestReconcileNodeLocalFinalizerSurvivesTopologyChange(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	wantPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		t.Fatal(err)
	}
	pathType := corev1.HostPathDirectoryOrCreate
	engine.Spec.Volumes = []corev1.Volume{{Name: "shm", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: wantPath, Type: &pathType}}}}
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	live := getBackend(t, reconciler, backend.Name, backend.Namespace)
	live.Spec.LMCache = lmcacheBackend("fixture", "ns1").Spec.LMCache.DeepCopy()
	if err := reconciler.Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	cleanupKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupPodName(backend.UID, "node-a"), Namespace: backend.Namespace}
	var cleanup corev1.Pod
	if err := reconciler.Get(context.Background(), cleanupKey, &cleanup); err != nil {
		t.Fatalf("cleanup intent after topology change: %v", err)
	}
	if cleanup.Spec.Containers[0].Image != reconciler.NodeLocalShmCleanupImage || len(cleanup.Spec.SchedulingGates) != 1 {
		t.Fatalf("topology-change cleanup = image:%q gates:%+v", cleanup.Spec.Containers[0].Image, cleanup.Spec.SchedulingGates)
	}

	live = getBackend(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Delete(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Delete(context.Background(), engine); err != nil {
		t.Fatal(err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), cleanupKey, &cleanup); err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Spec.SchedulingGates) != 0 {
		t.Fatalf("topology-change cleanup gate remained after consumer deletion: %+v", cleanup.Spec.SchedulingGates)
	}
	markNodeLocalCleanupSucceeded(t, reconciler, &cleanup, "node-a")
	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(backend), &cachev1alpha1.CacheBackend{}); !apierrors.IsNotFound(err) {
		t.Fatalf("topology-changed backend after cleanup finalization = %v, want NotFound", err)
	}
}

func TestReconcileNodeLocalReturnsInvalidCleanupDeleteError(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	cleanup, err := builtinruntime.RenderLMCacheNodeLocalCleanupPod(backend, "node-a", "registry.example/inference-cache-shm-cleanup@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", nodeLocalEngine(backend, "engine-a", "node-a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := controllerutil.SetControllerReference(backend, cleanup, newScheme(t)); err != nil {
		t.Fatal(err)
	}
	cleanup.Spec.Volumes[0].HostPath.Path = "/dev/shm/inference-cache/foreign"
	deleteErr := errors.New("cleanup delete denied")
	reconciler := newReconcilerWithInterceptor(newScheme(t), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == cleanup.Name {
				return deleteErr
			}
			return c.Delete(ctx, obj, opts...)
		},
	}, backend, cleanup)
	if _, err := reconciler.reconcileLMCacheNodeLocalCleanupPods(context.Background(), backend, nil, false); !errors.Is(err, deleteErr) {
		t.Fatalf("invalid cleanup delete error = %v, want %v", err, deleteErr)
	}
}

func TestReconcileNodeLocalIgnoresSelectorMatchOwnedByAnotherBackend(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	engine.Annotations[enginebinding.AnnotationInjectedBy] = "ns1/other-cache"
	engine.Annotations[enginebinding.AnnotationInjectedByUID] = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	var servers corev1.PodList
	if err := reconciler.List(context.Background(), &servers, client.InNamespace(backend.Namespace), client.MatchingLabels{
		enginebinding.LabelLMCacheNodeLocalServer: "true",
	}); err != nil {
		t.Fatal(err)
	}
	if len(servers.Items) != 0 {
		t.Fatalf("cross-CacheBackend selector overlap provisioned %d servers", len(servers.Items))
	}
}

func TestReconcileNodeLocalRejectsOccupiedServerPodName(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace,
	}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "foreign"}}}}
	reconciler := newReconciler(newScheme(t), backend, engine, foreign)
	if err := reconciler.reconcileLMCacheNodeLocalServerPods(context.Background(), backend, nil); err == nil {
		t.Fatal("occupied deterministic server Pod name was accepted")
	}
}

func TestReconcileNodeLocalManagedRedisKeepsLifecyclesIndependent(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	backend.Spec.RemoteStorage = lmcacheBackend("fixture", "ns1").Spec.RemoteStorage.DeepCopy()
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)

	reconcile(t, reconciler, backend.Name, backend.Namespace)

	key := types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace}
	for kind, object := range map[string]client.Object{
		"NodeLocal server": &corev1.Pod{},
		"Redis Deployment": &appsv1.Deployment{},
		"Redis Service":    &corev1.Service{},
	} {
		objectKey := key
		if kind == "NodeLocal server" {
			objectKey.Name = builtinruntime.NodeLocalServerPodName(backend.Name, "node-a")
		}
		if err := reconciler.Get(context.Background(), objectKey, object); err != nil {
			t.Fatalf("get %s: %v", kind, err)
		}
	}
}

func TestReconcileNodeLocalToPodLocalDeletesServerPods(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	live := getBackend(t, reconciler, backend.Name, backend.Namespace)
	live.Spec.LMCache = lmcacheBackend("fixture", "ns1").Spec.LMCache.DeepCopy()
	if err := reconciler.Update(context.Background(), live); err != nil {
		t.Fatalf("update backend to PodLocal: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	key := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	if err := reconciler.Get(context.Background(), key, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("server Pod after PodLocal transition = %v, want NotFound", err)
	}
}

func TestReconcileNodeLocalReplacesServerOnBackendGenerationChange(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	live := getBackend(t, reconciler, backend.Name, backend.Namespace)
	live.Spec.LMCache.NodeLocal.Server.HTTPPort = 18081
	live.Generation++ // fake client does not emulate apiserver generation bumps
	if err := reconciler.Update(context.Background(), live); err != nil {
		t.Fatalf("update NodeLocal backend: %v", err)
	}
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	var server corev1.Pod
	key := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	if err := reconciler.Get(context.Background(), key, &server); err != nil {
		t.Fatalf("get updated server Pod: %v", err)
	}
	ports := server.Spec.Containers[0].Ports
	if len(ports) != 2 || ports[1].HostPort != 18081 {
		t.Fatalf("updated server ports = %+v", ports)
	}
}

func TestReconcileNodeLocalCreatesCleanupBeforeReplacingLegacyServer(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	key := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	var old corev1.Pod
	if err := reconciler.Get(context.Background(), key, &old); err != nil {
		t.Fatalf("get original server Pod: %v", err)
	}
	if err := reconciler.Delete(context.Background(), &old); err != nil {
		t.Fatalf("delete original server Pod: %v", err)
	}
	old.ResourceVersion = ""
	old.UID = ""
	old.CreationTimestamp = metav1.Time{}
	args := old.Spec.Containers[0].Args
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--supported-transfer-mode" {
			old.Spec.Containers[0].Args = append(args[:i], args[i+2:]...)
			break
		}
	}
	if err := reconciler.Create(context.Background(), &old); err != nil {
		t.Fatalf("create legacy server Pod: %v", err)
	}

	reconcile(t, reconciler, backend.Name, backend.Namespace)
	if err := reconciler.Get(context.Background(), key, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy server after cleanup intent = %v, want NotFound", err)
	}
	cleanupKey := types.NamespacedName{Name: builtinruntime.NodeLocalCleanupPodName(backend.UID, "node-a"), Namespace: backend.Namespace}
	var cleanup corev1.Pod
	if err := reconciler.Get(context.Background(), cleanupKey, &cleanup); err != nil {
		t.Fatalf("cleanup intent for legacy server: %v", err)
	}
	if len(cleanup.Spec.SchedulingGates) != 1 {
		t.Fatalf("legacy server cleanup was not gated: %+v", cleanup.Spec.SchedulingGates)
	}

	reconcile(t, reconciler, backend.Name, backend.Namespace)
	var replaced corev1.Pod
	if err := reconciler.Get(context.Background(), key, &replaced); err != nil {
		t.Fatalf("get replacement server Pod: %v", err)
	}
	wantPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		t.Fatal(err)
	}
	if !nodeLocalServerHasRuntimeIdentity(&replaced, wantPath) {
		t.Fatalf("replacement server lacks managed CUDA runtime identity: volumes=%v args=%v", replaced.Spec.Volumes, replaced.Spec.Containers[0].Args)
	}
}

func TestReconcileNodeLocalReplacesServerWithWrongUIDScopedShmDirectory(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	engine := nodeLocalEngine(backend, "engine-a", "node-a")
	reconciler := newReconciler(newScheme(t), backend, engine)
	reconcile(t, reconciler, backend.Name, backend.Namespace)

	key := types.NamespacedName{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: backend.Namespace}
	var old corev1.Pod
	if err := reconciler.Get(context.Background(), key, &old); err != nil {
		t.Fatalf("get original server Pod: %v", err)
	}
	if err := reconciler.Delete(context.Background(), &old); err != nil {
		t.Fatalf("delete original server Pod: %v", err)
	}
	old.ResourceVersion = ""
	old.UID = ""
	old.CreationTimestamp = metav1.Time{}
	old.Spec.Volumes[0].HostPath.Path = "/dev/shm/inference-cache/foreign-cachebackend-uid"
	if err := reconciler.Create(context.Background(), &old); err != nil {
		t.Fatalf("create server Pod with foreign SHM directory: %v", err)
	}

	reconcile(t, reconciler, backend.Name, backend.Namespace)
	var replaced corev1.Pod
	if err := reconciler.Get(context.Background(), key, &replaced); err != nil {
		t.Fatalf("get replacement server Pod: %v", err)
	}
	wantPath, err := builtinruntime.NodeLocalServerShmHostPath(backend)
	if err != nil {
		t.Fatal(err)
	}
	if !nodeLocalServerHasRuntimeIdentity(&replaced, wantPath) {
		t.Fatalf("replacement server lacks UID-scoped SHM directory: volumes=%v", replaced.Spec.Volumes)
	}
}

func TestCleanupNodeLocalPreservesUnownedServerPod(t *testing.T) {
	backend := nodeLocalBackend("node-cache", "ns1")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: backend.Namespace, Labels: map[string]string{
		enginebinding.LabelLMCacheNodeLocalServer: "true", enginebinding.LabelCacheBackendUID: string(backend.UID),
	}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "foreign"}}}}
	reconciler := newReconciler(newScheme(t), backend, pod)
	if err := reconciler.cleanupLMCacheNodeLocalServerPods(context.Background(), backend); err != nil {
		t.Fatalf("cleanup unowned server Pod: %v", err)
	}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Fatalf("unowned server Pod was removed: %v", err)
	}
	if err := reconciler.cleanupLMCacheNodeLocalServerPods(context.Background(), nil); err != nil {
		t.Fatalf("nil backend cleanup: %v", err)
	}
}

func TestReconcileManagedRedisCreatesSingletonWorkload(t *testing.T) {
	backend := lmcacheBackend("cache", "ns1")
	reconciler := newReconciler(newScheme(t), backend)

	reconcile(t, reconciler, backend.Name, backend.Namespace)

	deployment := getDeployment(t, reconciler, backend.Name, backend.Namespace)
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
		t.Fatalf("managed Redis replicas = %v, want 1", deployment.Spec.Replicas)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 || deployment.Spec.Template.Spec.Containers[0].Name != "redis-l2" {
		t.Fatalf("managed Redis containers = %+v", deployment.Spec.Template.Spec.Containers)
	}
	var service corev1.Service
	if err := reconciler.Get(context.Background(), types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace}, &service); err != nil {
		t.Fatalf("get managed Redis Service: %v", err)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 6379 {
		t.Fatalf("managed Redis Service ports = %+v", service.Spec.Ports)
	}

	got := getBackend(t, reconciler, backend.Name, backend.Namespace)
	wantEndpoint := "cache.ns1.svc.cluster.local:6379"
	if got.Status.RemoteStorage == nil || got.Status.RemoteStorage.Provider != cachev1alpha1.CacheBackendRemoteStorageProviderRedis || got.Status.RemoteStorage.Endpoint != wantEndpoint {
		t.Fatalf("remote-storage status = %+v, want Redis endpoint %q", got.Status.RemoteStorage, wantEndpoint)
	}
}

func TestReconcileExternalRedisCreatesNoWorkload(t *testing.T) {
	backend := lmcacheBackend("external", "ns1")
	backend.Spec.RemoteStorage = externalRedisStorage("redis.example:6379")
	reconciler := newReconciler(newScheme(t), backend)

	reconcile(t, reconciler, backend.Name, backend.Namespace)

	assertNoManagedWorkload(t, reconciler, backend.Name, backend.Namespace)
	got := getBackend(t, reconciler, backend.Name, backend.Namespace)
	if got.Status.RemoteStorage == nil || got.Status.RemoteStorage.Endpoint != "redis.example:6379" || got.Status.RemoteStorage.Ready != metav1.ConditionTrue {
		t.Fatalf("external Redis status = %+v", got.Status.RemoteStorage)
	}
	ready := findCondition(got.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionUnknown || ready.Reason != reasonConnectorUnverified {
		t.Fatalf("Ready = %+v, want Unknown/%s until an injected engine Pod is observed", ready, reasonConnectorUnverified)
	}
}

func TestReconcileHostOnlyMPHasNoProviderWorkload(t *testing.T) {
	backend := lmcacheBackend("host-only", "ns1")
	backend.Spec.RemoteStorage = nil
	reconciler := newReconciler(newScheme(t), backend)

	reconcile(t, reconciler, backend.Name, backend.Namespace)

	assertNoManagedWorkload(t, reconciler, backend.Name, backend.Namespace)
	got := getBackend(t, reconciler, backend.Name, backend.Namespace)
	if got.Status.RemoteStorage != nil {
		t.Fatalf("host-only backend published remote-storage status: %+v", got.Status.RemoteStorage)
	}
}

func assertNoManagedWorkload(t *testing.T, reconciler *CacheBackendReconciler, name, namespace string) {
	t.Helper()
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := reconciler.Get(context.Background(), key, &appsv1.Deployment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Deployment lookup error = %v, want NotFound", err)
	}
	if err := reconciler.Get(context.Background(), key, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Service lookup error = %v, want NotFound", err)
	}
}
