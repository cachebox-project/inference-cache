package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// TestIntegrationCacheBackendServerRestartCascade exercises the
// cache-server restart cascade against a real apiserver (envtest), so
// it covers behavior the fake client can't: real Patch + Status().Patch
// semantics, real Pod readiness-condition handling, and the
// pod→ReplicaSet→Deployment owner-resolution chain Get'd through the
// uncached client.
//
// What's not covered here: a live kubelet rolling a Pod. envtest has no
// kubelet, so we simulate "the cache-server pod restarted" by deleting
// the old Pod and creating a new one with the same selector labels and a
// fresh UID. That matches the only signal the controller is supposed to
// react to — a UID transition on the Ready cache-server pod.
func TestIntegrationCacheBackendServerRestartCascade(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{
		Client:                          k8s,
		Scheme:                          scheme,
		Log:                             logr.Discard(),
		MinServerRestartCascadeInterval: 100 * time.Millisecond,
		serverInstanceCascade:           newServerInstanceCascade(),
	}
	ctx := context.Background()

	t.Run("UIDTransitionAnnotatesEngineDeployment", func(t *testing.T) {
		ns := freshNS(t, k8s)
		resetBackendServerRestartsTotalForTest()

		cb := lmcacheBackend("cache", ns)
		cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{
			MatchLabels: map[string]string{"app": "vllm-engine"},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}

		// Engine wiring: a Deployment owns a ReplicaSet which owns a
		// Pod stamped by the webhook. We build the chain by hand
		// (envtest doesn't run the Deployment controller).
		engineDep := newEngineDeployment(ns, "vllm-engine")
		if err := k8s.Create(ctx, engineDep); err != nil {
			t.Fatalf("create engine deployment: %v", err)
		}
		engineRS := newEngineReplicaSet(ns, "vllm-engine-rs", engineDep)
		if err := k8s.Create(ctx, engineRS); err != nil {
			t.Fatalf("create engine RS: %v", err)
		}
		fetchAfterCreate(t, k8s, engineDep)
		fetchAfterCreate(t, k8s, engineRS)
		enginePod := newEngineInjectedPod(ns, "vllm-engine-aaa", engineRS, ns, "cache", string(cb.UID))
		if err := k8s.Create(ctx, enginePod); err != nil {
			t.Fatalf("create engine pod: %v", err)
		}

		// First Ready cache-server pod. The reconciler should see
		// this and persist it as the baseline (no cascade). The
		// cache-server pod must be owner-referenced up to the
		// reconciler-created Deployment so currentServerInstanceUID's
		// transitive-ownership check admits it. The reconciler creates
		// the Deployment on the first reconcile; the RS that would
		// normally own pods is fabricated here (envtest runs no apps
		// controller).
		reconcile(t, r, "cache", ns)
		serverRS1 := newServerReplicaSet(t, k8s, ns, "cache", "cache-rs-1")
		serverPod1 := newReadyServerPod(ns, "cache-pod-1", "cache")
		setServerPodOwner(serverPod1, serverRS1)
		createReady(t, k8s, serverPod1)
		reconcile(t, r, "cache", ns)

		reloaded := getBackend(t, r, "cache", ns)
		if got := reloaded.Status.ObservedServerInstance; got != string(serverPod1.UID) {
			t.Fatalf("baseline ObservedServerInstance = %q, want %q", got, serverPod1.UID)
		}
		gotDep := &appsv1.Deployment{}
		if err := k8s.Get(ctx, types.NamespacedName{Name: "vllm-engine", Namespace: ns}, gotDep); err != nil {
			t.Fatalf("get engine dep: %v", err)
		}
		if _, ok := gotDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
			t.Fatalf("engine deployment annotated on first observation; want no cascade yet")
		}
		if got := cascadeRestartsCount(t, ns, "cache", cascadeRestartReasonServerInstanceChanged); got != 0 {
			t.Fatalf("counter = %v, want 0 (no cascade on first observation)", got)
		}

		// Simulate the cache-server pod restarting: delete + recreate
		// with a fresh UID. The Pod controller in envtest does not
		// run; the test is the authority on what pods exist. The
		// replacement pod is owner-referenced to the same RS as the
		// first — that's what a rolling restart of the Deployment
		// would produce.
		if err := k8s.Delete(ctx, serverPod1); err != nil {
			t.Fatalf("delete first server pod: %v", err)
		}
		serverPod2 := newReadyServerPod(ns, "cache-pod-2", "cache")
		setServerPodOwner(serverPod2, serverRS1)
		createReady(t, k8s, serverPod2)

		reconcile(t, r, "cache", ns)

		reloaded = getBackend(t, r, "cache", ns)
		if got := reloaded.Status.ObservedServerInstance; got != string(serverPod2.UID) {
			t.Fatalf("ObservedServerInstance after UID flip = %q, want %q", got, serverPod2.UID)
		}
		if err := k8s.Get(ctx, types.NamespacedName{Name: "vllm-engine", Namespace: ns}, gotDep); err != nil {
			t.Fatalf("get engine dep: %v", err)
		}
		annot := gotDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]
		if annot != string(serverPod2.UID) {
			t.Fatalf("cascade annotation = %q, want %q", annot, serverPod2.UID)
		}
		if got := cascadeRestartsCount(t, ns, "cache", cascadeRestartReasonServerInstanceChanged); got != 1 {
			t.Fatalf("counter = %v, want 1", got)
		}
	})

	t.Run("RateLimitedSecondCascadeIsDeferred", func(t *testing.T) {
		ns := freshNS(t, k8s)
		resetBackendServerRestartsTotalForTest()

		// Use a long window so the second cascade is definitely
		// inside it. Restored after the test so other subtests use
		// the snug 100ms.
		prev := r.MinServerRestartCascadeInterval
		r.MinServerRestartCascadeInterval = 1 * time.Hour
		t.Cleanup(func() { r.MinServerRestartCascadeInterval = prev })

		cb := lmcacheBackend("cache", ns)
		cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{
			MatchLabels: map[string]string{"app": "vllm-engine"},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		engineDep := newEngineDeployment(ns, "vllm-engine")
		if err := k8s.Create(ctx, engineDep); err != nil {
			t.Fatalf("create engine deployment: %v", err)
		}
		engineRS := newEngineReplicaSet(ns, "vllm-engine-rs", engineDep)
		if err := k8s.Create(ctx, engineRS); err != nil {
			t.Fatalf("create engine RS: %v", err)
		}
		fetchAfterCreate(t, k8s, engineDep)
		fetchAfterCreate(t, k8s, engineRS)
		enginePod := newEngineInjectedPod(ns, "vllm-engine-aaa", engineRS, ns, "cache", string(cb.UID))
		if err := k8s.Create(ctx, enginePod); err != nil {
			t.Fatalf("create engine pod: %v", err)
		}

		// Baseline observation + first cascade. The cache-server pod
		// must be owner-referenced up to the reconciler-created
		// Deployment so currentServerInstanceUID's transitive-
		// ownership check admits it.
		reconcile(t, r, "cache", ns)
		serverRS := newServerReplicaSet(t, k8s, ns, "cache", "cache-rs-1")
		serverPod1 := newReadyServerPod(ns, "cache-pod-1", "cache")
		setServerPodOwner(serverPod1, serverRS)
		createReady(t, k8s, serverPod1)
		reconcile(t, r, "cache", ns)

		serverPod2 := newReadyServerPod(ns, "cache-pod-2", "cache")
		setServerPodOwner(serverPod2, serverRS)
		if err := k8s.Delete(ctx, serverPod1); err != nil {
			t.Fatalf("delete first server pod: %v", err)
		}
		createReady(t, k8s, serverPod2)
		reconcile(t, r, "cache", ns)

		gotDep := &appsv1.Deployment{}
		if err := k8s.Get(ctx, types.NamespacedName{Name: "vllm-engine", Namespace: ns}, gotDep); err != nil {
			t.Fatalf("get engine dep: %v", err)
		}
		firstCascadeUID := gotDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]
		if firstCascadeUID != string(serverPod2.UID) {
			t.Fatalf("first cascade annotation = %q, want %q", firstCascadeUID, serverPod2.UID)
		}
		if got := cascadeRestartsCount(t, ns, "cache", cascadeRestartReasonServerInstanceChanged); got != 1 {
			t.Fatalf("counter after first cascade = %v, want 1", got)
		}

		// Second back-to-back UID flip while still inside the 1h
		// rate-limit window. The Deployment annotation must stay
		// pinned to the first cascade's UID; status must stay pinned
		// likewise.
		serverPod3 := newReadyServerPod(ns, "cache-pod-3", "cache")
		setServerPodOwner(serverPod3, serverRS)
		if err := k8s.Delete(ctx, serverPod2); err != nil {
			t.Fatalf("delete second server pod: %v", err)
		}
		createReady(t, k8s, serverPod3)
		reconcile(t, r, "cache", ns)

		if err := k8s.Get(ctx, types.NamespacedName{Name: "vllm-engine", Namespace: ns}, gotDep); err != nil {
			t.Fatalf("get engine dep: %v", err)
		}
		if got := gotDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != firstCascadeUID {
			t.Fatalf("cascade annotation drifted while rate-limited: got %q, want pinned %q", got, firstCascadeUID)
		}
		reloaded := getBackend(t, r, "cache", ns)
		if got := reloaded.Status.ObservedServerInstance; got != firstCascadeUID {
			t.Fatalf("ObservedServerInstance drifted while rate-limited: got %q, want pinned %q", got, firstCascadeUID)
		}
		if got := cascadeRestartsCount(t, ns, "cache", cascadeRestartReasonServerInstanceChanged); got != 1 {
			t.Fatalf("counter after rate-limited cascade = %v, want 1", got)
		}
	})
}

// newEngineDeployment fabricates a minimal apps/v1 Deployment shaped like a
// vLLM engine workload — enough for the cascade tests' selector match +
// owner-resolution + Status().Patch.
func newEngineDeployment(namespace, name string) *appsv1.Deployment {
	one := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app": "vllm-engine"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "vllm-engine"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "vllm-engine"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "vllm", Image: "vllm:test"}},
				},
			},
		},
	}
}

// newEngineReplicaSet fabricates the ReplicaSet the apps/v1 Deployment
// controller would normally create (envtest does not run that
// controller). The Deployment is named via a controller-owner reference so
// podOwningDeploymentName can walk pod → RS → Deployment.
func newEngineReplicaSet(namespace, name string, dep *appsv1.Deployment) *appsv1.ReplicaSet {
	tru := true
	one := int32(1)
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": "vllm-engine"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       dep.Name,
				UID:        dep.UID,
				Controller: &tru,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "vllm-engine"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "vllm-engine"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "vllm", Image: "vllm:test"}},
				},
			},
		},
	}
}

// newEngineInjectedPod fabricates an engine pod that already carries the
// pod-webhook's injected-by annotations, so the cascade filter (which
// gates on the annotation, NOT just the selector) admits it.
func newEngineInjectedPod(namespace, name string, rs *appsv1.ReplicaSet, backendNS, backendName, backendUID string) *corev1.Pod {
	tru := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": "vllm-engine"},
			Annotations: map[string]string{
				podwebhook.AnnotationInjectedBy:    backendNS + "/" + backendName,
				podwebhook.AnnotationInjectedByUID: backendUID,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       rs.Name,
				UID:        rs.UID,
				Controller: &tru,
			}},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "vllm", Image: "vllm:test"}},
		},
	}
}

// newReadyServerPod builds a cache-server pod labeled the way the
// reconciler labels its own children. UID is assigned by envtest on
// create; the caller reads it back to compare against
// status.observedServerInstance.
func newReadyServerPod(namespace, name, backendName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "cachebackend",
				"app.kubernetes.io/instance":   backendName,
				"app.kubernetes.io/managed-by": "inference-cache-controller",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "lmcache-server", Image: "lmcache:test"}},
		},
	}
}

// createReady creates the pod and then patches its status to Running +
// Ready=True (envtest does not run kubelet, so spec.status is otherwise
// empty). The caller can read pod.UID after this returns.
func createReady(t *testing.T, k8s interface {
	Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error
	Status() client.StatusWriter
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
}, pod *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	if err := k8s.Create(ctx, pod); err != nil {
		t.Fatalf("create server pod %s: %v", pod.Name, err)
	}
	// Status().Patch with a snapshot: refetch the freshly-created pod so
	// the patch base carries the apiserver-assigned ResourceVersion.
	live := &corev1.Pod{}
	if err := k8s.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, live); err != nil {
		t.Fatalf("refetch server pod %s: %v", pod.Name, err)
	}
	live.Status.Phase = corev1.PodRunning
	live.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := k8s.Status().Update(ctx, live); err != nil {
		t.Fatalf("set server pod %s ready: %v", pod.Name, err)
	}
	// Copy UID back so the caller can compare.
	pod.UID = live.UID
}

// fetchAfterCreate refreshes obj's ResourceVersion + UID after a Create,
// so chained creates that reference obj.UID see the value the apiserver
// assigned.
func fetchAfterCreate(t *testing.T, k8s interface {
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
}, obj client.Object) {
	t.Helper()
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("refetch after create: %v", err)
	}
}

// newServerReplicaSet fabricates the ReplicaSet the apps/v1 Deployment
// controller would normally create for the reconciler-managed cache-
// server Deployment. envtest runs no apps controller, so the test is
// the authority on what ReplicaSets/Pods exist. The RS is owner-
// referenced to the reconciler-created Deployment (looked up after the
// first reconcile creates it) so currentServerInstanceUID's transitive
// ownership check (pod → RS → Deployment) admits owned pods.
func newServerReplicaSet(t *testing.T, k8s client.Client, namespace, backendName, rsName string) *appsv1.ReplicaSet {
	t.Helper()
	dep := &appsv1.Deployment{}
	if err := k8s.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: backendName}, dep); err != nil {
		t.Fatalf("server Deployment %s/%s not present (run reconcile first): %v", namespace, backendName, err)
	}
	tru := true
	one := int32(1)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       dep.Name,
				UID:        dep.UID,
				Controller: &tru,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels(backendName)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels(backendName)},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "lmcache-server", Image: "lmcache:test"}},
				},
			},
		},
	}
	if err := k8s.Create(context.Background(), rs); err != nil {
		t.Fatalf("create server RS: %v", err)
	}
	fetchAfterCreate(t, k8s, rs)
	return rs
}

// setServerPodOwner stamps the controller-owner reference from a
// cache-server pod up to its RS — the missing link the test needs to
// build for envtest (the apps controller would normally do this).
func setServerPodOwner(pod *corev1.Pod, rs *appsv1.ReplicaSet) {
	tru := true
	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       rs.Name,
		UID:        rs.UID,
		Controller: &tru,
	})
}
