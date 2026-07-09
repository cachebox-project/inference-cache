package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

// These tests run the reconciler against a real kube-apiserver (envtest), so they
// cover behavior the fake client cannot: real API-server defaulting and the
// idempotency/no-churn against it, real Status subresource semantics on the
// child Deployment, real CRD validation (e.g. the autoscaling XValidation rule),
// HPA reconciliation, and — in TestIntegrationCacheBackendWatch — the Owns()
// watch re-trigger via a real manager.
//
// Skipped unless KUBEBUILDER_ASSETS is set. CI installs envtest in
// .github/workflows/ci.yml before `make test-race`, so the suite runs there;
// locally run `KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./...`.

func skipWithoutEnvtest(t *testing.T) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with `KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test` for envtest")
	}
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(os.Stderr)))
}

// startEnv boots an envtest apiserver with the project CRDs and returns a client.
func startEnv(t *testing.T) (client.Client, *runtime.Scheme, *rest.Config) {
	t.Helper()
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	return k8s, scheme, cfg
}

var itNSCounter int64

func freshNS(t *testing.T, k8s client.Client) string {
	t.Helper()
	name := fmt.Sprintf("it-%d", atomic.AddInt64(&itNSCounter, 1))
	if err := k8s.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	return name
}

func getService(t *testing.T, r *CacheBackendReconciler, name, namespace string) *corev1.Service {
	t.Helper()
	var svc corev1.Service
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &svc); err != nil {
		t.Fatalf("get service %s/%s: %v", namespace, name, err)
	}
	return &svc
}

func getRV(t *testing.T, r *CacheBackendReconciler, name, namespace string, obj client.Object) string {
	t.Helper()
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, obj); err != nil {
		t.Fatalf("get %T for resourceVersion: %v", obj, err)
	}
	return obj.GetResourceVersion()
}

func ptrBool(v bool) *bool { return &v }

type getObservingClient struct {
	client.Client
	onGet func(types.NamespacedName, client.Object)
}

func (c *getObservingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	if c.onGet != nil {
		// Record attempts, including NotFound, so the watch drain sees stale
		// queued reconciles that would recreate a deleted child.
		c.onGet(types.NamespacedName(key), obj)
	}
	return err
}

// pollDeployment waits for a Deployment to exist at key, returning its UID.
func pollDeployment(t *testing.T, k8s client.Client, key types.NamespacedName, what string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var dep appsv1.Deployment
		if err := k8s.Get(context.Background(), key, &dep); err == nil {
			return string(dep.UID)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for deployment to %s", what)
	return ""
}

func setDeploymentStatus(t *testing.T, r *CacheBackendReconciler, name, ns string, mutate func(*appsv1.Deployment)) {
	t.Helper()
	dep := getDeployment(t, r, name, ns)
	mutate(dep)
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status: %v", err)
	}
}

func TestIntegrationCacheBackendReconcile(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	t.Run("LMCacheServerWorkloadShape", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(2)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		dep := getDeployment(t, r, "cache", ns)
		if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
			t.Fatalf("replicas = %v, want 2", dep.Spec.Replicas)
		}
		// Selector is a subset of the pod template labels.
		sel := dep.Spec.Selector.MatchLabels
		podLabels := dep.Spec.Template.Labels
		for k, v := range sel {
			if podLabels[k] != v {
				t.Fatalf("selector label %s=%s missing from pod labels %v", k, v, podLabels)
			}
		}

		if len(dep.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf("containers = %d, want 1", len(dep.Spec.Template.Spec.Containers))
		}
		c := dep.Spec.Template.Spec.Containers[0]
		if c.Name != "lmcache-server" {
			t.Fatalf("container name = %q, want lmcache-server", c.Name)
		}
		if !strings.Contains(c.Image, "lmcache/standalone") {
			t.Fatalf("default image = %q, want the lmcache/standalone reference image", c.Image)
		}
		if !containsStr(c.Command, "lmcache_server") {
			t.Fatalf("command = %v, want lmcache_server", c.Command)
		}
		if !containsStr(c.Args, "65432") || !containsStr(c.Args, "cpu") || !containsStr(c.Args, "0.0.0.0") {
			t.Fatalf("args = %v, want [host port storage]", c.Args)
		}
		if len(c.Ports) != 1 || c.Ports[0].ContainerPort != 65432 || c.Ports[0].Protocol != corev1.ProtocolTCP {
			t.Fatalf("ports = %v, want a single TCP port on 65432", c.Ports)
		}
		if c.ReadinessProbe == nil || c.ReadinessProbe.TCPSocket == nil {
			t.Fatalf("readiness probe = %+v, want a TCP-socket probe on the lmcache port", c.ReadinessProbe)
		}

		svc := getService(t, r, "cache", ns)
		if svc.Spec.Type != corev1.ServiceTypeClusterIP || svc.Spec.ClusterIP == "" {
			t.Fatalf("service type/clusterIP = %q/%q, want ClusterIP with an allocated IP", svc.Spec.Type, svc.Spec.ClusterIP)
		}
		if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 65432 {
			t.Fatalf("service ports = %v, want a single port on 65432", svc.Spec.Ports)
		}

		// Real API-server pod defaulting is applied to the stored template.
		spec := dep.Spec.Template.Spec
		if spec.RestartPolicy == "" || spec.DNSPolicy == "" {
			t.Fatalf("expected pod defaulting, got restartPolicy=%q dnsPolicy=%q", spec.RestartPolicy, spec.DNSPolicy)
		}
	})

	t.Run("MooncakeMasterWorkloadShape", func(t *testing.T) {
		// Mooncake reconcile contract against a real apiserver: type=Mooncake
		// reconciles into a mooncake_master Deployment + Service via the
		// Mooncake adapter, and status.endpoint resolves the master's RPC port.
		ns := freshNS(t, k8s)
		cb := mooncakeBackend("cache", ns)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		dep := getDeployment(t, r, "cache", ns)
		if len(dep.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf("containers = %d, want 1", len(dep.Spec.Template.Spec.Containers))
		}
		c := dep.Spec.Template.Spec.Containers[0]
		if c.Name != "mooncake-master" {
			t.Fatalf("container name = %q, want mooncake-master", c.Name)
		}
		if !strings.Contains(c.Image, "mooncake") {
			t.Fatalf("default image = %q, want the Mooncake reference image", c.Image)
		}
		if !containsStr(c.Command, "mooncake_master") {
			t.Fatalf("command = %v, want mooncake_master", c.Command)
		}
		if !containsStr(c.Args, "--rpc_port=50051") {
			t.Fatalf("args = %v, want --rpc_port=50051", c.Args)
		}
		if len(c.Ports) == 0 || c.Ports[0].ContainerPort != 50051 || c.Ports[0].Protocol != corev1.ProtocolTCP {
			t.Fatalf("first port = %v, want the RPC port 50051 first", c.Ports)
		}
		if c.ReadinessProbe == nil || c.ReadinessProbe.TCPSocket == nil {
			t.Fatalf("readiness probe = %+v, want a TCP-socket probe on the RPC port", c.ReadinessProbe)
		}

		svc := getService(t, r, "cache", ns)
		if len(svc.Spec.Ports) == 0 || svc.Spec.Ports[0].Port != 50051 {
			t.Fatalf("service first port = %v, want RPC port 50051 first", svc.Spec.Ports)
		}

		got := getBackend(t, r, "cache", ns)
		wantEndpoint := "cache." + ns + ".svc.cluster.local:50051"
		if got.Status.Endpoint != wantEndpoint {
			t.Fatalf("status.endpoint = %q, want %q", got.Status.Endpoint, wantEndpoint)
		}
	})

	t.Run("StatusEndpointAndObservedGeneration", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		wantEndpoint := fmt.Sprintf("cache.%s.svc.cluster.local:65432", ns)
		if cb.Status.Endpoint != wantEndpoint {
			t.Fatalf("status.endpoint = %q, want %q", cb.Status.Endpoint, wantEndpoint)
		}
		if cb.Status.ObservedGeneration != cb.Generation {
			t.Fatalf("observedGeneration = %d, want %d", cb.Status.ObservedGeneration, cb.Generation)
		}
		if cond := findCondition(cb.Status.Conditions, conditionTypeReady); cond == nil {
			t.Fatalf("Ready condition missing")
		}
	})

	t.Run("OwnerReferencesDriveGC", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		for _, obj := range []client.Object{getDeployment(t, r, "cache", ns), getService(t, r, "cache", ns)} {
			owner := metav1.GetControllerOf(obj)
			if owner == nil || owner.Kind != "CacheBackend" || owner.Name != "cache" {
				t.Fatalf("%T controller owner = %+v", obj, owner)
			}
			if owner.Controller == nil || !*owner.Controller {
				t.Fatalf("%T owner Controller flag not set", obj)
			}
			if owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
				t.Fatalf("%T owner BlockOwnerDeletion not set (needed for GC)", obj)
			}
		}
	})

	t.Run("NoChurnAgainstRealDefaulting", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		// Two reconciles to converge any first-write differences before the RV snapshot.
		reconcile(t, r, "cache", ns)
		depRV := getRV(t, r, "cache", ns, &appsv1.Deployment{})
		svcRV := getRV(t, r, "cache", ns, &corev1.Service{})
		reconcile(t, r, "cache", ns)
		if got := getRV(t, r, "cache", ns, &appsv1.Deployment{}); got != depRV {
			t.Fatalf("deployment churned: RV %s -> %s", depRV, got)
		}
		if got := getRV(t, r, "cache", ns, &corev1.Service{}); got != svcRV {
			t.Fatalf("service churned: RV %s -> %s", svcRV, got)
		}
	})

	t.Run("ReadyConditionTransitions", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(2)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if cond := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonRolloutInProgress {
			t.Fatalf("fresh Ready condition = %+v, want False/RolloutInProgress", cond)
		}

		// Mid-rollout: generation observed but updated replicas lag -> Ready=False/RolloutInProgress.
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
			d.Status.Replicas = 2
			d.Status.UpdatedReplicas = 1
			d.Status.AvailableReplicas = 2
			d.Status.ReadyReplicas = 2
		})
		reconcile(t, r, "cache", ns)
		if cond := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonRolloutInProgress {
			t.Fatalf("mid-rollout Ready condition = %+v, want False/RolloutInProgress", cond)
		}

		// Fully rolled out -> Ready=True.
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
			d.Status.Replicas = 2
			d.Status.UpdatedReplicas = 2
			d.Status.AvailableReplicas = 2
			d.Status.ReadyReplicas = 2
		})
		reconcile(t, r, "cache", ns)
		cb = getBackend(t, r, "cache", ns)
		if cond := findCondition(cb.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
			t.Fatalf("Ready condition = %+v, want True", cond)
		}

		// Rolled out but replicas unavailable -> Ready=False/ReplicasUnavailable.
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
			d.Status.Replicas = 2
			d.Status.UpdatedReplicas = 2
			d.Status.AvailableReplicas = 1
			d.Status.ReadyReplicas = 1
		})
		reconcile(t, r, "cache", ns)
		if cond := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditionReasonReplicasUnavailable {
			t.Fatalf("unavailable Ready condition = %+v, want False/ReplicasUnavailable", cond)
		}
	})

	t.Run("ZeroReplicasNotReady", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(0)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
		})
		reconcile(t, r, "cache", ns)
		cb = getBackend(t, r, "cache", ns)
		if cond := findCondition(cb.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionFalse {
			t.Fatalf("Ready condition = %+v, want False for zero replicas", cond)
		}
	})

	t.Run("ServerImageOverrideAndUpdate", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.BackendConfig = map[string]string{"serverImage": "example.com/lmcache-server:v1"}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if got := getDeployment(t, r, "cache", ns).Spec.Template.Spec.Containers[0].Image; got != "example.com/lmcache-server:v1" {
			t.Fatalf("image = %q, want override", got)
		}

		live := getBackend(t, r, "cache", ns)
		live.Spec.BackendConfig["serverImage"] = "example.com/lmcache-server:v2"
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("update image: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if got := getDeployment(t, r, "cache", ns).Spec.Template.Spec.Containers[0].Image; got != "example.com/lmcache-server:v2" {
			t.Fatalf("updated image = %q, want v2", got)
		}
	})

	t.Run("ReplicaScale", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(1)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		live := getBackend(t, r, "cache", ns)
		live.Spec.Replicas = ptrInt32(4)
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("update replicas: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if got := getDeployment(t, r, "cache", ns).Spec.Replicas; got == nil || *got != 4 {
			t.Fatalf("replicas = %v, want 4", got)
		}
	})

	t.Run("PodOverrideUpdate", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		live := getBackend(t, r, "cache", ns)
		live.Spec.Template = &cachev1alpha1.CacheBackendPodSpecOverride{
			NodeSelector:       map[string]string{"accelerator": "h100"},
			ServiceAccountName: "backend-sa",
		}
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("update overrides: %v", err)
		}
		reconcile(t, r, "cache", ns)
		spec := getDeployment(t, r, "cache", ns).Spec.Template.Spec
		if spec.NodeSelector["accelerator"] != "h100" || spec.ServiceAccountName != "backend-sa" {
			t.Fatalf("overrides not reconciled: nodeSelector=%v sa=%q", spec.NodeSelector, spec.ServiceAccountName)
		}
	})

	t.Run("ServicePortDriftCorrected", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		svc := getService(t, r, "cache", ns)
		// Drift the owned Service out-of-band: change the port number.
		svc.Spec.Ports[0].Port = 9999
		if err := k8s.Update(ctx, svc); err != nil {
			t.Fatalf("drift service: %v", err)
		}
		reconcile(t, r, "cache", ns)
		svc = getService(t, r, "cache", ns)
		if svc.Spec.Ports[0].Port != 65432 {
			t.Fatalf("service port = %d, want 65432 restored after drift", svc.Spec.Ports[0].Port)
		}
	})

	t.Run("HPACreatedAndUpdatedAndDeleted", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
			MinReplicas:                 ptrInt32(2),
			MaxReplicas:                 5,
			TargetCPUUtilizationPercent: ptrInt32(60),
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		// HPA created and points at the managed Deployment.
		hpa := getHPA(t, r, "cache", ns)
		if hpa.Spec.ScaleTargetRef.Kind != "Deployment" || hpa.Spec.ScaleTargetRef.Name != "cache" {
			t.Fatalf("HPA target = %+v, want Deployment/cache", hpa.Spec.ScaleTargetRef)
		}
		if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 5 {
			t.Fatalf("HPA min/max = %v/%d, want 2/5", hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
		}
		// When autoscaling is set the lmcache-server container carries CPU requests
		// (the utilization denominator the HPA needs).
		if cpu := getDeployment(t, r, "cache", ns).Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]; cpu.IsZero() {
			t.Fatalf("autoscaling backend should request CPU on the container (HPA denominator)")
		}

		// Update bounds and target — reflected on the HPA.
		live := getBackend(t, r, "cache", ns)
		live.Spec.Autoscaling.MinReplicas = ptrInt32(3)
		live.Spec.Autoscaling.MaxReplicas = 8
		live.Spec.Autoscaling.TargetCPUUtilizationPercent = ptrInt32(75)
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("update autoscaling: %v", err)
		}
		reconcile(t, r, "cache", ns)
		hpa = getHPA(t, r, "cache", ns)
		if *hpa.Spec.MinReplicas != 3 || hpa.Spec.MaxReplicas != 8 {
			t.Fatalf("HPA min/max = %v/%d, want 3/8 after update", hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
		}

		// Clear autoscaling — the HPA must be garbage-collected explicitly.
		live = getBackend(t, r, "cache", ns)
		live.Spec.Autoscaling = nil
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("clear autoscaling: %v", err)
		}
		reconcile(t, r, "cache", ns)
		var hpaList autoscalingv2.HorizontalPodAutoscalerList
		if err := k8s.List(ctx, &hpaList, client.InNamespace(ns)); err != nil {
			t.Fatalf("list HPAs: %v", err)
		}
		if len(hpaList.Items) != 0 {
			t.Fatalf("HPAs after clearing autoscaling = %d, want 0", len(hpaList.Items))
		}
	})

	t.Run("HPANoChurnAgainstRealDefaulting", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, autoscalingBackend("cache", ns, 2, 5, ptrInt32(60))); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		// Two reconciles to converge any first-write differences before the RV snapshot.
		reconcile(t, r, "cache", ns)
		hpaRV := getRV(t, r, "cache", ns, &autoscalingv2.HorizontalPodAutoscaler{})

		reconcile(t, r, "cache", ns)
		if got := getRV(t, r, "cache", ns, &autoscalingv2.HorizontalPodAutoscaler{}); got != hpaRV {
			t.Fatalf("HPA churned: RV %s -> %s", hpaRV, got)
		}
	})

	t.Run("CRDValidationRejectsBadAutoscaling", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("bad", ns)
		// XValidation: minReplicas must not exceed maxReplicas.
		cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
			MinReplicas: ptrInt32(7),
			MaxReplicas: 3,
		}
		if err := k8s.Create(ctx, cb); err == nil {
			t.Fatalf("expected CRD validation to reject minReplicas>maxReplicas")
		}
	})

	t.Run("SwitchToExternalCleansUpAndMirrors", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if _, err := getOptionalDeployment(t, r, "cache", ns); err != nil {
			t.Fatalf("expected managed deployment first: %v", err)
		}

		live := getBackend(t, r, "cache", ns)
		live.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
		live.Spec.Endpoint = "external.example.svc:8080"
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("switch to external: %v", err)
		}
		reconcile(t, r, "cache", ns)

		if _, err := getOptionalDeployment(t, r, "cache", ns); err == nil {
			t.Fatalf("deployment should be deleted after switch to External")
		}
		cb := getBackend(t, r, "cache", ns)
		if cb.Status.Endpoint != "external.example.svc:8080" {
			t.Fatalf("status.endpoint = %q, want mirrored external endpoint", cb.Status.Endpoint)
		}
		// After the switch to External the controller publishes
		// Ready=True with reason ExternalEndpointAccepted — admission
		// acceptance of spec.endpoint is the only readiness signal we
		// have without provisioning a Service to probe.
		ready := findCondition(cb.Status.Conditions, conditionTypeReady)
		if ready == nil {
			t.Fatalf("Ready condition missing after switch to External; conditions = %v", cb.Status.Conditions)
		}
		if ready.Status != metav1.ConditionTrue || ready.Reason != "ExternalEndpointAccepted" {
			t.Fatalf("Ready condition = %+v, want Status=True Reason=ExternalEndpointAccepted", ready)
		}
	})

	t.Run("ExternalCreateProducesNoWorkloadAndReady", func(t *testing.T) {
		// A CacheBackend{type: External} reconciled against a real
		// apiserver must (a) leave the CR's namespace free of any
		// controller-rendered Deployment or Service, (b) mirror
		// spec.endpoint into status.endpoint verbatim, and (c) publish
		// Ready=True with reason ExternalEndpointAccepted so downstream
		// consumers (the future readiness gate, the indexParticipation
		// poller) treat the CR as usable.
		ns := freshNS(t, k8s)
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "ext-fresh", Namespace: ns},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type:     cachev1alpha1.CacheBackendTypeExternal,
				Endpoint: "lm://my-cache.example:8200",
			},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "ext-fresh", ns)

		var deps appsv1.DeploymentList
		if err := k8s.List(ctx, &deps, client.InNamespace(ns)); err != nil {
			t.Fatalf("list deployments: %v", err)
		}
		if len(deps.Items) != 0 {
			t.Fatalf("deployments = %d, want 0 for External backend", len(deps.Items))
		}
		var svcs corev1.ServiceList
		if err := k8s.List(ctx, &svcs, client.InNamespace(ns)); err != nil {
			t.Fatalf("list services: %v", err)
		}
		if len(svcs.Items) != 0 {
			t.Fatalf("services = %d, want 0 for External backend", len(svcs.Items))
		}

		got := getBackend(t, r, "ext-fresh", ns)
		if got.Status.Endpoint != "lm://my-cache.example:8200" {
			t.Fatalf("status.endpoint = %q, want lm://my-cache.example:8200", got.Status.Endpoint)
		}
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "ExternalEndpointAccepted" {
			t.Fatalf("Ready condition = %+v, want Status=True Reason=ExternalEndpointAccepted", ready)
		}
	})

	t.Run("ExternalAdvancesObservedGeneration", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: ns},
			Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeExternal, Endpoint: "ext.example.svc:8080"},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "ext", ns)
		got := getBackend(t, r, "ext", ns)
		if got.Status.Endpoint != "ext.example.svc:8080" {
			t.Fatalf("status.endpoint = %q", got.Status.Endpoint)
		}
		if got.Status.ObservedGeneration != got.Generation {
			t.Fatalf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
		}
	})

	t.Run("UnmanagedTypeNoWorkload", func(t *testing.T) {
		ns := freshNS(t, k8s)
		// AIBrix has no registered adapter → unmanaged path. (Mooncake now
		// has an adapter and reconciles managed — see the Managed Mooncake
		// integration subtest.)
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "mc", Namespace: ns},
			Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeAIBrix},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "mc", ns)
		if _, err := getOptionalDeployment(t, r, "mc", ns); err == nil {
			t.Fatalf("unmanaged type should not create a deployment")
		}
		got := getBackend(t, r, "mc", ns)
		if got.Status.ObservedGeneration != got.Generation {
			t.Fatalf("observedGeneration not advanced for unmanaged type")
		}
	})

	t.Run("SwitchToStatefulSetKindCleansUpAndClearsStatus", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if getBackend(t, r, "cache", ns).Status.Endpoint == "" {
			t.Fatalf("expected published endpoint first")
		}

		live := getBackend(t, r, "cache", ns)
		live.Spec.DeploymentKind = cachev1alpha1.CacheBackendDeploymentKindStatefulSet
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("switch kind: %v", err)
		}
		reconcile(t, r, "cache", ns)

		if _, err := getOptionalDeployment(t, r, "cache", ns); err == nil {
			t.Fatalf("deployment should be deleted after switch to StatefulSet kind")
		}
		cb := getBackend(t, r, "cache", ns)
		if cb.Status.Endpoint != "" {
			t.Fatalf("status.endpoint = %q, want cleared", cb.Status.Endpoint)
		}
		if cond := findCondition(cb.Status.Conditions, conditionTypeReady); cond != nil {
			t.Fatalf("Ready condition = %+v, want removed", cond)
		}
	})

	t.Run("FailOpenStatusMirrorsSpec", func(t *testing.T) {
		ns := freshNS(t, k8s)
		// Default (no integration spec): status.failOpen mirrors the API default (true).
		if err := k8s.Create(ctx, lmcacheBackend("def", ns)); err != nil {
			t.Fatalf("create default: %v", err)
		}
		reconcile(t, r, "def", ns)
		if got := getBackend(t, r, "def", ns).Status.FailOpen; got == nil || !*got {
			t.Fatalf("default status.failOpen = %v, want true", got)
		}
		// Explicit fail-closed: status.failOpen reflects it.
		strict := lmcacheBackend("strict", ns)
		strict.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{FailOpen: ptrBool(false)}
		if err := k8s.Create(ctx, strict); err != nil {
			t.Fatalf("create strict: %v", err)
		}
		reconcile(t, r, "strict", ns)
		if got := getBackend(t, r, "strict", ns).Status.FailOpen; got == nil || *got {
			t.Fatalf("strict status.failOpen = %v, want false", got)
		}
	})

	t.Run("EngineNameCaseInsensitiveRouting", func(t *testing.T) {
		ns := freshNS(t, k8s)
		// Upper-case engine name still routes to the vllm adapter.
		up := lmcacheBackend("up", ns)
		up.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "VLLM"}
		if err := k8s.Create(ctx, up); err != nil {
			t.Fatalf("create VLLM: %v", err)
		}
		reconcile(t, r, "up", ns)
		if _, err := getOptionalDeployment(t, r, "up", ns); err != nil {
			t.Fatalf("VLLM (uppercase) should match the vllm adapter and produce a Deployment: %v", err)
		}

		// sglang now has a shipping adapter (the SGLang+LMCache adapter is in
		// the reconciler's nil-registry fallback), so a (sglang, LMCache)
		// backend is managed the same way vLLM is — it renders the standalone
		// lmcache-server Deployment.
		sg := lmcacheBackend("sg", ns)
		sg.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "sglang"}
		if err := k8s.Create(ctx, sg); err != nil {
			t.Fatalf("create sglang: %v", err)
		}
		reconcile(t, r, "sg", ns)
		if _, err := getOptionalDeployment(t, r, "sg", ns); err != nil {
			t.Fatalf("sglang now has a shipping adapter; expected a managed Deployment: %v", err)
		}

		// An engine with no registered adapter still falls into the unmanaged path.
		unk := lmcacheBackend("unk", ns)
		unk.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "no-such-engine"}
		if err := k8s.Create(ctx, unk); err != nil {
			t.Fatalf("create unknown-engine: %v", err)
		}
		reconcile(t, r, "unk", ns)
		if _, err := getOptionalDeployment(t, r, "unk", ns); err == nil {
			t.Fatalf("unknown engine has no adapter; expected no Deployment (unmanaged path)")
		}
	})

	t.Run("MissingObjectIsNoError", func(t *testing.T) {
		ns := freshNS(t, k8s)
		reconcile(t, r, "does-not-exist", ns)
	})
}

// TestIntegrationEnginePodEvents exercises the engine-pod-events controller
// against a real apiserver. The controller's contract is "emit a Normal
// InjectedByCacheBackend Event on every pod the mutating webhook stamped
// with inferencecache.io/injected-by". The user-visible promise is that
// `kubectl describe pod` surfaces the event, and describe filters events
// by involvedObject.uid — so this test asserts the recorded events carry
// the persisted Pod UID, not just the name.
//
// (This is the regression the webhook-recorded approach would have
// broken: at admission time the apiserver hasn't assigned the UID yet,
// so an event recorded from the webhook lands with involvedObject.uid=""
// and is invisible under describe.)
func TestIntegrationEnginePodEvents(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, cfg := startEnv(t)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptrBool(true)},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := (&EnginePodEventsReconciler{
		Client: mgr.GetClient(),
		Log:    logr.Discard(),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatalf("cache did not sync")
	}

	ns := freshNS(t, k8s)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
		},
	}
	if err := k8s.Create(context.Background(), cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}
	// The apiserver assigns cb.UID on Create. The injected-by-uid
	// annotation below pins that UID so the events controller's UID
	// match passes — without it, the controller would skip emission
	// per the failurePolicy=Ignore forgery guard.
	if cb.UID == "" {
		t.Fatalf("apiserver returned empty UID for persisted CacheBackend — envtest invariant broken")
	}

	// Create a pod with the injected-by annotations the webhook would
	// have stamped. The webhook is NOT installed in this test — we are
	// exercising the controller's behavior on a pod that LOOKS like one
	// the webhook produced.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engine-a",
			Namespace: ns,
			Annotations: map[string]string{
				"inferencecache.io/injected-by":     ns + "/" + cb.Name,
				"inferencecache.io/injected-by-uid": string(cb.UID),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "engine",
				Image: "registry.example.com/vllm:test",
			}},
		},
	}
	if err := k8s.Create(context.Background(), pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	if pod.UID == "" {
		t.Fatalf("apiserver returned empty UID for persisted pod — envtest invariant broken")
	}

	skipped := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engine-skipped",
			Namespace: ns,
			Annotations: map[string]string{
				podwebhook.AnnotationSkip:          "true",
				podwebhook.AnnotationInjectSkipped: podwebhook.InjectSkippedReasonSkipAnnotation,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
		},
	}
	if err := k8s.Create(context.Background(), skipped); err != nil {
		t.Fatalf("create skipped pod: %v", err)
	}
	if skipped.UID == "" {
		t.Fatalf("apiserver returned empty UID for skipped pod; envtest invariant broken")
	}

	// An unannotated pod that should NOT generate an event. Pins the
	// predicate filtering.
	unrelated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "x", Image: "x"}},
		},
	}
	if err := k8s.Create(context.Background(), unrelated); err != nil {
		t.Fatalf("create unrelated pod: %v", err)
	}

	// Poll for the InjectedByCacheBackend event on the persisted pod's
	// UID. The events.EventRecorder broadcasts asynchronously, so the
	// event lags pod creation by a few hundred ms.
	deadline := time.Now().Add(20 * time.Second)
	var sawInjected bool
	var sawSkipped bool
	var sawSpurious bool
	for time.Now().Before(deadline) && (!sawInjected || !sawSkipped) {
		var list eventsv1.EventList
		if err := k8s.List(context.Background(), &list, client.InNamespace(ns)); err == nil {
			for _, ev := range list.Items {
				if ev.Regarding.Kind != "Pod" {
					continue
				}
				if ev.Regarding.UID == pod.UID && ev.Reason == "InjectedByCacheBackend" {
					sawInjected = true
				}
				if ev.Regarding.UID == skipped.UID && ev.Reason == "SkippedByOperator" {
					sawSkipped = true
				}
				if ev.Regarding.UID == unrelated.UID && ev.Reason == "InjectedByCacheBackend" {
					sawSpurious = true
				}
				if ev.Regarding.UID == unrelated.UID && ev.Reason == "SkippedByOperator" {
					sawSpurious = true
				}
			}
		}
		if sawInjected && sawSkipped {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !sawInjected {
		t.Errorf("did not observe InjectedByCacheBackend event with involvedObject.uid=%q within timeout", pod.UID)
	}
	if !sawSkipped {
		t.Errorf("did not observe SkippedByOperator event with involvedObject.uid=%q within timeout", skipped.UID)
	}
	if sawSpurious {
		t.Errorf("controller emitted an engine-pod event on an unannotated pod (uid=%q); predicate failed", unrelated.UID)
	}
}

// TestIntegrationCacheBackendMatchedEnginePodsRequeueCadence verifies the
// new self-requeue cadence: a manager-driven reconciler (no Pod watch, no
// explicit reconcile() calls) must still converge `status.matchedEnginePods`
// after a pod CREATE because the previous reconcile scheduled a
// RequeueAfter when the CR's EngineSelector was non-empty. Without that
// requeue, pod birth/death would only refresh the count when an unrelated
// CR/owned-workload event fired, leaving the operator-facing column
// indefinitely stale.
func TestIntegrationCacheBackendMatchedEnginePodsRequeueCadence(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, cfg := startEnv(t)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptrBool(true)},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	// Keep the steady cadence long and inject a short churn cadence so this
	// test proves the conditional fast path: while desired Deployment replicas
	// and observed matching pods disagree, the controller self-requeues quickly
	// without a Pod watch.
	const steadyRequeueInterval = 30 * time.Second
	const churnRequeueInterval = 250 * time.Millisecond
	if err := (&CacheBackendReconciler{
		Client:                                mgr.GetClient(),
		Scheme:                                mgr.GetScheme(),
		Log:                                   logr.Discard(),
		MatchedEnginePodsRequeueInterval:      steadyRequeueInterval,
		MatchedEnginePodsChurnRequeueInterval: churnRequeueInterval,
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatalf("cache did not sync")
	}

	ns := freshNS(t, k8s)
	sel := map[string]string{"app": "test-engine"}
	engineDep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "engine", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrInt32(2),
			Selector: &metav1.LabelSelector{MatchLabels: sel},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: sel},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
				},
			},
		},
	}
	if err := k8s.Create(context.Background(), engineDep); err != nil {
		t.Fatalf("create engine Deployment: %v", err)
	}
	cb := lmcacheBackend("cache", ns)
	cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: sel}
	if err := k8s.Create(context.Background(), cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}

	// Helper to read the live count.
	read := func() *int32 {
		var live cachev1alpha1.CacheBackend
		if err := k8s.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: ns}, &live); err != nil {
			t.Fatalf("get CacheBackend: %v", err)
		}
		return live.Status.MatchedEnginePods
	}
	waitForCount := func(want int32, what string) {
		t.Helper()
		// With the injected churn cadence (250ms), the next requeue
		// after a pod change lands well under a second. Add 5s of
		// envtest-jitter slack on top.
		deadline := time.Now().Add(churnRequeueInterval + 5*time.Second)
		for time.Now().Before(deadline) {
			got := read()
			if got != nil && *got == want {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Fatalf("status.matchedEnginePods did not converge to %d (%s) within timeout; last value = %v", want, what, read())
	}

	// First reconcile (manager-driven; no explicit reconcile() call):
	// no matching pods → expect 0.
	waitForCount(0, "no matching pods")

	// Create matching pods AFTER the initial reconcile and verify the
	// count catches up WITHOUT us forcing a reconcile. The self-
	// requeue cadence is what makes this work.
	for i := 0; i < 2; i++ {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("engine-%d", i),
				Namespace: ns,
				Labels:    sel,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
			},
		}
		if err := k8s.Create(context.Background(), p); err != nil {
			t.Fatalf("create pod %s: %v", p.Name, err)
		}
	}
	waitForCount(2, "after creating 2 matching pods")
}

func TestIntegrationCacheBackendEngineSelectorUnmatchedDiagnostics(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, cfg := startEnv(t)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptrBool(true)},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := (&CacheBackendReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    logr.Discard(),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatalf("cache did not sync")
	}

	ns := freshNS(t, k8s)
	cb := lmcacheBackend("cache", ns)
	cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{
		MatchLabels: map[string]string{"app": "definitely-not-present"},
	}
	if err := k8s.Create(context.Background(), cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}

	var lastMsg string
	var sawEvent bool
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var live cachev1alpha1.CacheBackend
		if err := k8s.Get(context.Background(), types.NamespacedName{Name: "cache", Namespace: ns}, &live); err != nil {
			t.Fatalf("get CacheBackend: %v", err)
		}
		lastMsg = live.Status.EngineSelectorMessage

		var list eventsv1.EventList
		if err := k8s.List(context.Background(), &list, client.InNamespace(ns)); err == nil {
			for _, ev := range list.Items {
				if ev.Regarding.Name == "cache" &&
					ev.Regarding.Kind == "CacheBackend" &&
					ev.Reason == eventReasonEngineSelectorUnmatched {
					sawEvent = true
				}
			}
		}

		if live.Status.MatchedEnginePods != nil &&
			*live.Status.MatchedEnginePods == 0 &&
			strings.Contains(lastMsg, "spec.engineSelector.matchLabels={app:definitely-not-present}") &&
			strings.Contains(lastMsg, "no Pods in namespace match") &&
			sawEvent {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("unmatched selector diagnostics did not converge: message=%q sawEvent=%v", lastMsg, sawEvent)
}

// TestIntegrationCacheBackendMatchedEnginePods exercises the
// status.matchedEnginePods writer against a real apiserver: the count
// reflects the live pod inventory in the CR's namespace, ignores pods in
// other namespaces, and survives pod birth/death between reconciles.
//
// The writer counts at reconcile cadence (no Pod watch); the test therefore
// drives reconcile() explicitly after each pod mutation.
func TestIntegrationCacheBackendMatchedEnginePods(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	createPod := func(t *testing.T, namespace, name string, podLabels map[string]string) {
		t.Helper()
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: podLabels},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "engine",
					Image: "registry.example.com/vllm:test",
				}},
			},
		}
		if err := k8s.Create(ctx, p); err != nil {
			t.Fatalf("create pod %s/%s: %v", namespace, name, err)
		}
	}

	ns := freshNS(t, k8s)
	other := freshNS(t, k8s)
	sel := map[string]string{"app": "test-engine"}

	cb := lmcacheBackend("cache", ns)
	cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: sel}
	if err := k8s.Create(ctx, cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}

	// No matching pods yet → after first reconcile the field is an
	// observed 0 (not nil, which would mean "not yet computed").
	reconcile(t, r, "cache", ns)
	if got := getBackend(t, r, "cache", ns).Status.MatchedEnginePods; got == nil || *got != 0 {
		t.Fatalf("with zero matching pods: matchedEnginePods = %v, want 0", got)
	}

	// Three matching pods land. A pod with the same labels in a
	// different namespace and a non-matching pod in this one must not
	// inflate the count.
	createPod(t, ns, "engine-1", sel)
	createPod(t, ns, "engine-2", sel)
	createPod(t, ns, "engine-3", sel)
	createPod(t, ns, "router-1", map[string]string{"app": "router"})
	createPod(t, other, "engine-foreign", sel)

	reconcile(t, r, "cache", ns)
	if got := getBackend(t, r, "cache", ns).Status.MatchedEnginePods; got == nil || *got != 3 {
		t.Fatalf("after creating 3 matching pods: matchedEnginePods = %v, want 3", got)
	}

	// Delete one of the matching pods (force, since envtest has no
	// kubelet and pods never go past Pending → no graceful delete).
	zero := int64(0)
	if err := k8s.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-1", Namespace: ns}},
		&client.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	// Tight poll: envtest's delete is async; reconcile until the count
	// catches up (or the deadline fires).
	deadline := time.Now().Add(10 * time.Second)
	var last *int32
	for time.Now().Before(deadline) {
		reconcile(t, r, "cache", ns)
		last = getBackend(t, r, "cache", ns).Status.MatchedEnginePods
		if last != nil && *last == 2 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("after deleting 1 of 3 matching pods: matchedEnginePods = %v, want 2", last)
}

// TestIntegrationCacheBackendWatch runs a real manager so the Owns(...) watches
// are exercised end to end: deleting managed Deployment/HPA children re-triggers
// reconcile and the controller recreates them.
func TestIntegrationCacheBackendWatch(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, cfg := startEnv(t)

	// SkipNameValidation: multiple manager-based subtests in the same test binary
	// would otherwise collide on the global controller-name registry.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptrBool(true)},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	hpaGets := make(chan types.NamespacedName, 100)
	observedClient := &getObservingClient{
		Client: mgr.GetClient(),
		onGet: func(key types.NamespacedName, obj client.Object) {
			if _, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler); !ok {
				return
			}
			select {
			case hpaGets <- key:
			default:
			}
		},
	}
	if err := (&CacheBackendReconciler{
		Client: observedClient,
		Scheme: mgr.GetScheme(),
		Log:    logr.Discard(),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatalf("cache did not sync")
	}

	waitForHPA := func(t *testing.T, key types.NamespacedName, what string) string {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var hpa autoscalingv2.HorizontalPodAutoscaler
			if err := k8s.Get(context.Background(), key, &hpa); err == nil {
				return string(hpa.UID)
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for HPA to %s", what)
		return ""
	}
	waitForObservedGeneration := func(t *testing.T, key types.NamespacedName, want int64) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var live cachev1alpha1.CacheBackend
			if err := k8s.Get(context.Background(), key, &live); err == nil && live.Status.ObservedGeneration >= want {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for CacheBackend %s/%s observedGeneration to reach %d", key.Namespace, key.Name, want)
	}
	drainHPAGetEvents := func() {
		for {
			select {
			case <-hpaGets:
			default:
				return
			}
		}
	}
	waitForHPAGet := func(t *testing.T, key types.NamespacedName, what string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case got := <-hpaGets:
				if got == key {
					return
				}
			case <-time.After(200 * time.Millisecond):
			}
		}
		t.Fatalf("timed out waiting for reconciler to get HPA %s/%s (%s)", key.Namespace, key.Name, what)
	}
	waitForQuietHPAGets := func(t *testing.T, key types.NamespacedName, quietFor time.Duration) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		quietUntil := time.Now().Add(quietFor)
		for {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for reconciler HPA gets on %s/%s to go quiet", key.Namespace, key.Name)
			}
			remainingQuiet := time.Until(quietUntil)
			if remainingQuiet <= 0 {
				return
			}
			select {
			case got := <-hpaGets:
				if got == key {
					quietUntil = time.Now().Add(quietFor)
				}
			case <-time.After(remainingQuiet):
				return
			}
		}
	}

	t.Run("OwnsDeploymentWatchRecreatesDeletedChild", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(context.Background(), lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}

		key := types.NamespacedName{Name: "cache", Namespace: ns}
		originalUID := pollDeployment(t, k8s, key, "be created by the manager")

		// Delete the child; the Owns() watch must re-trigger reconcile and recreate it.
		if err := k8s.Delete(context.Background(), &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: ns},
		}); err != nil {
			t.Fatalf("delete deployment: %v", err)
		}
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var dep appsv1.Deployment
			if err := k8s.Get(context.Background(), key, &dep); err == nil && string(dep.UID) != originalUID {
				return // recreated with a new UID — Owns(Deployment) watch re-trigger confirmed
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("deployment was not recreated after deletion (Owns watch did not re-trigger)")
	})

	t.Run("OwnsHPAWatchRecreatesDeletedChild", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(context.Background(), autoscalingBackend("cache", ns, 2, 5, ptrInt32(60))); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}

		key := types.NamespacedName{Name: "cache", Namespace: ns}
		waitForHPA(t, key, "be created by the manager")

		// Drain the initial create/status/owned-child event burst before deleting
		// the HPA. Otherwise an already-queued parent/Deployment reconcile could
		// recreate the HPA and make this test pass even if Owns(HPA) were absent.
		//
		// The drain waits for a known spec-driven reconcile to reach the HPA, then
		// waits for HPA reads to go quiet. It deliberately does not require a
		// status-only parent reconcile; that keeps the test valid if the parent
		// watch is ever narrowed to generation-changing updates.
		drainHPAGetEvents()
		var live cachev1alpha1.CacheBackend
		if err := k8s.Get(context.Background(), key, &live); err != nil {
			t.Fatalf("get CacheBackend before drain update: %v", err)
		}
		beforeGeneration := live.Generation
		if live.Spec.BackendConfig == nil {
			live.Spec.BackendConfig = map[string]string{}
		}
		live.Spec.BackendConfig["testDrain"] = time.Now().Format(time.RFC3339Nano)
		if err := k8s.Update(context.Background(), &live); err != nil {
			t.Fatalf("update CacheBackend to drain initial queue: %v", err)
		}
		if live.Generation <= beforeGeneration {
			t.Fatalf("drain update did not advance generation: %d -> %d", beforeGeneration, live.Generation)
		}
		waitForObservedGeneration(t, key, live.Generation)
		waitForHPAGet(t, key, "drain spec update reconcile")
		waitForQuietHPAGets(t, key, 500*time.Millisecond)

		originalUID := waitForHPA(t, key, "remain after the drain reconcile")

		// Delete the child; the Owns() watch must re-trigger reconcile and recreate it.
		if err := k8s.Delete(context.Background(), &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: ns},
		}); err != nil {
			t.Fatalf("delete HPA: %v", err)
		}
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var hpa autoscalingv2.HorizontalPodAutoscaler
			if err := k8s.Get(context.Background(), key, &hpa); err == nil && string(hpa.UID) != originalUID {
				return // recreated with a new UID — Owns(HPA) watch re-trigger confirmed
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("HPA was not recreated after deletion (Owns watch did not re-trigger)")
	})
}

// TestIntegrationCacheIndexPollerProjectsParticipation exercises the poller
// against a real apiserver to confirm that Status().Patch on CacheBackend
// applies the indexParticipation projection (pod-label-based attribution),
// and that a steady snapshot does not churn the backend's resourceVersion
// (the no-churn invariant under real apiserver defaulting). Catches the
// class of bug a fake client misses — the fake client skips apiserver
// defaulting that can flip semantic equality on round-trip and cause
// spurious writes.
func TestIntegrationCacheIndexPollerProjectsParticipation(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()
	ns := freshNS(t, k8s)

	// Seed two CacheBackends with EngineSelectors plus an engine pod each.
	// External type keeps the CacheBackend reconciler out of the picture —
	// we are testing the poller's Status().Patch in isolation.
	mkBackend := func(name string, selector map[string]string) *cachev1alpha1.CacheBackend {
		return &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type:           cachev1alpha1.CacheBackendTypeExternal,
				Endpoint:       "external.example:6379",
				EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: selector},
			},
		}
	}
	mkPod := func(name string, labels map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "vllm", Image: "vllm/vllm-openai:latest"}}},
		}
	}
	for _, obj := range []client.Object{
		mkBackend("backend-a", map[string]string{"app": "vllm-a"}),
		mkBackend("backend-b", map[string]string{"app": "vllm-b"}),
		mkPod("vllm-a-0", map[string]string{"app": "vllm-a"}),
		mkPod("vllm-b-0", map[string]string{"app": "vllm-b"}),
	} {
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
		}
	}

	tEvent := time.Now().Add(-30 * time.Second).UTC().Truncate(time.Second)
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: ns, PrefixCount: 4, LastEventAt: tEvent},
			{ReplicaID: "vllm-b-0", Tenant: ns, PrefixCount: 1, LastEventAt: tEvent},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(served)
	}))
	defer srv.Close()

	p := &CacheIndexPoller{Client: k8s, SnapshotURL: srv.URL, HTTPClient: srv.Client(), Name: "cluster-default"}
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	a := getBackendDirect(t, k8s, "backend-a", ns)
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 4 {
		t.Fatalf("backend-a participation = %+v, want prefixCount 4", a.Status.IndexParticipation)
	}
	rvA := a.ResourceVersion

	// Second refresh on identical snapshot → no churn (apiserver-side).
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	a2 := getBackendDirect(t, k8s, "backend-a", ns)
	if a2.ResourceVersion != rvA {
		t.Fatalf("steady snapshot churned resourceVersion (%s → %s)", rvA, a2.ResourceVersion)
	}
}

// TestIntegrationCacheIndexAcceptsUntenantedTenantRow proves the empty-string
// tenant sentinel survives a real apiserver write. Untenanted prefixes bucket
// under tenantID "" so the Σ tenants[].indexEntries == totalPrefixes invariant
// holds; that "" row flows into CacheIndex.status.tenants[], whose listMapKey is
// `id`. An empty listMapKey value is unusual, so verify the apiserver accepts it
// (a fake client would not catch a structural-schema rejection).
func TestIntegrationCacheIndexAcceptsUntenantedTenantRow(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()

	served := index.Snapshot{
		TotalPrefixes: 5,
		Tenants: []index.TenantSnapshot{
			{TenantID: "", IndexEntries: 2}, // untenanted bucket
			{TenantID: "team", IndexEntries: 3},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(served)
	}))
	defer srv.Close()

	p := &CacheIndexPoller{Client: k8s, SnapshotURL: srv.URL, HTTPClient: srv.Client(), Name: "cluster-default"}
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("refresh writing a CacheIndex with an empty-id tenant row: %v", err)
	}

	var ci cachev1alpha1.CacheIndex
	if err := k8s.Get(ctx, types.NamespacedName{Name: "cluster-default"}, &ci); err != nil {
		t.Fatalf("get CacheIndex: %v", err)
	}
	var sawUntenanted, sawTeam bool
	var sum int64
	for _, tn := range ci.Status.Tenants {
		if tn.IndexEntries != nil {
			sum += *tn.IndexEntries
		}
		switch tn.ID {
		case "":
			sawUntenanted = true
		case "team":
			sawTeam = true
		}
	}
	if !sawUntenanted || !sawTeam {
		t.Fatalf("CacheIndex tenants = %+v, want both \"\" (untenanted) and \"team\"", ci.Status.Tenants)
	}
	// The invariant is verifiable from the CacheIndex CR: Σ tenants[].indexEntries
	// == prefixes.summary.total (2 + 3 == 5).
	if sum != int64(ci.Status.Prefixes.Summary.Total) {
		t.Fatalf("Σ tenants[].indexEntries = %d, want == prefixes.summary.total %d", sum, ci.Status.Prefixes.Summary.Total)
	}
}

// TestIntegrationCacheBackendPrinterColumnsRenderParticipation verifies the
// operator-facing promise: `kubectl get cachebackend` shows Prefixes and
// LastEvent columns sourced from status.indexParticipation. We hit the
// apiserver's Table content type — exactly the negotiation kubectl does
// under the hood — and assert column headers, types, and per-row cell
// values match. Catches accidental removal of the +kubebuilder:printcolumn
// markers, JSONPath drift, and renames of the underlying status fields.
func TestIntegrationCacheBackendPrinterColumnsRenderParticipation(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, cfg := startEnv(t)
	ctx := context.Background()
	ns := freshNS(t, k8s)

	// Two backends: one with positive participation, one drained-but-quiet.
	// External type keeps the CacheBackend reconciler out of the picture so
	// we are testing the printer-column projection from status, not the
	// reconciler.
	active := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "backend-a", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: "lm://cache-svc:6379",
		},
	}
	if err := k8s.Create(ctx, active); err != nil {
		t.Fatalf("create active: %v", err)
	}
	quiet := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "backend-b", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Endpoint: "lm://cache-svc:6379",
		},
	}
	if err := k8s.Create(ctx, quiet); err != nil {
		t.Fatalf("create quiet: %v", err)
	}

	// Set the participation status directly via the status subresource —
	// same path the poller uses, no poller in this test.
	lastEvent := metav1.NewTime(time.Now().Add(-5 * time.Minute).UTC().Truncate(time.Second))
	active.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{
		PrefixCount: 42,
		LastEventAt: &lastEvent,
	}
	if err := k8s.Status().Update(ctx, active); err != nil {
		t.Fatalf("update active status: %v", err)
	}
	quiet.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{
		PrefixCount: 0,
		// LastEventAt deliberately nil — kubectl renders this as <none>.
	}
	if err := k8s.Status().Update(ctx, quiet); err != nil {
		t.Fatalf("update quiet status: %v", err)
	}

	// Hit the apiserver with the Table accept header. This is exactly what
	// `kubectl get` does: the server-side rendering is what defines the
	// columns the operator sees, so this is the most honest test of the
	// before/after promise.
	// Use the typed REST client so namespace/resource path construction and
	// auth wiring exactly match what kubectl does internally.
	restCfg := rest.CopyConfig(cfg)
	gv := cachev1alpha1.GroupVersion
	restCfg.GroupVersion = &gv
	restCfg.APIPath = "/apis"
	restCfg.NegotiatedSerializer = serializer.NewCodecFactory(runtime.NewScheme()).WithoutConversion()
	restClient, err := rest.RESTClientFor(restCfg)
	if err != nil {
		t.Fatalf("build REST client: %v", err)
	}
	raw, err := restClient.Get().
		Namespace(ns).
		Resource("cachebackends").
		SetHeader("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io").
		DoRaw(ctx)
	if err != nil {
		t.Fatalf("apiserver Table request: %v", err)
	}

	var table metav1.Table
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatalf("decode Table: %v\nraw=%s", err, raw)
	}
	if len(table.ColumnDefinitions) == 0 || len(table.Rows) == 0 {
		t.Fatalf("apiserver returned an empty Table: %+v", table)
	}

	// Find the Prefixes and LastEvent columns and assert their types match
	// the +kubebuilder:printcolumn markers in api/v1alpha1/cachebackend_types.go.
	wantCols := map[string]string{"Prefixes": "integer", "LastEvent": "date"}
	colIdx := map[string]int{}
	for i, col := range table.ColumnDefinitions {
		if wantType, ok := wantCols[col.Name]; ok {
			if col.Type != wantType {
				t.Errorf("column %q type = %q, want %q", col.Name, col.Type, wantType)
			}
			colIdx[col.Name] = i
		}
	}
	for name := range wantCols {
		if _, ok := colIdx[name]; !ok {
			t.Fatalf("column %q missing from `kubectl get cachebackend` output", name)
		}
	}

	// Per-row cell assertions: active shows 42, quiet shows 0; LastEvent
	// on quiet is the empty/<none> cell.
	wantPrefixes := map[string]float64{"backend-a": 42, "backend-b": 0}
	for _, row := range table.Rows {
		var obj metav1.PartialObjectMetadata
		if err := json.Unmarshal(row.Object.Raw, &obj); err != nil {
			t.Fatalf("decode row object: %v", err)
		}
		expected, ok := wantPrefixes[obj.Name]
		if !ok {
			continue
		}
		gotPrefixes, ok := row.Cells[colIdx["Prefixes"]].(float64)
		if !ok {
			t.Fatalf("%s Prefixes cell type = %T (%v), want number", obj.Name, row.Cells[colIdx["Prefixes"]], row.Cells[colIdx["Prefixes"]])
		}
		if gotPrefixes != expected {
			t.Errorf("%s Prefixes cell = %v, want %v", obj.Name, gotPrefixes, expected)
		}
		switch obj.Name {
		case "backend-a":
			// Set lastEventAt → cell should be a non-empty string (the apiserver
			// renders date columns as relative ages like "5m").
			cell, _ := row.Cells[colIdx["LastEvent"]].(string)
			if cell == "" || cell == "<none>" {
				t.Errorf("backend-a LastEvent cell = %q, want a rendered duration", cell)
			}
		case "backend-b":
			// Nil lastEventAt → cell is empty / <none>; the apiserver returns
			// the empty string for a missing date field.
			cell := row.Cells[colIdx["LastEvent"]]
			if s, ok := cell.(string); ok && s != "" && s != "<none>" {
				t.Errorf("backend-b LastEvent cell = %q, want empty/<none>", s)
			}
		}
	}
}

// TestIntegrationCacheBackendEvents runs a real manager (so the Recorder is
// auto-wired) and asserts the two transition Events the controller emits on
// status changes — FailClosedEnabled (spec.integration.failOpen flipped to
// false) and BackendDegraded (rolled out, but no available replicas) — actually
// reach the apiserver, end to end.
func TestIntegrationCacheBackendEvents(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, cfg := startEnv(t)

	// SkipNameValidation: multiple manager-based subtests in the same test binary
	// would otherwise collide on the global controller-name registry.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptrBool(true)},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := (&CacheBackendReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    logr.Discard(),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatalf("cache did not sync")
	}

	// A fresh CR with spec.integration.failOpen=false: the first reconcile
	// observes a true→false transition (status.failOpen defaults to true when
	// unset) and emits FailClosedEnabled.
	ns := freshNS(t, k8s)
	cb := lmcacheBackend("cache", ns)
	cb.Spec.Replicas = ptrInt32(1)
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{FailOpen: ptrBool(false)}
	if err := k8s.Create(context.Background(), cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}

	key := types.NamespacedName{Name: "cache", Namespace: ns}
	pollDeployment(t, k8s, key, "be created by the manager")

	// Drive a Pending→Degraded transition by patching the Deployment status to
	// rolled-out but with no available replicas.
	var dep appsv1.Deployment
	if err := k8s.Get(context.Background(), key, &dep); err != nil {
		t.Fatalf("get dep: %v", err)
	}
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = 1
	dep.Status.UpdatedReplicas = 1
	dep.Status.AvailableReplicas = 0
	dep.Status.ReadyReplicas = 0
	if err := k8s.Status().Update(context.Background(), &dep); err != nil {
		t.Fatalf("patch dep status: %v", err)
	}

	wantReasons := map[string]bool{
		eventReasonFailClosedEnabled: false,
		eventReasonBackendDegraded:   false,
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var list eventsv1.EventList
		if err := k8s.List(context.Background(), &list, client.InNamespace(ns)); err == nil {
			for _, ev := range list.Items {
				if ev.Regarding.Name != "cache" || ev.Regarding.Kind != "CacheBackend" {
					continue
				}
				if _, ok := wantReasons[ev.Reason]; ok {
					wantReasons[ev.Reason] = true
				}
			}
		}
		allSeen := true
		for _, seen := range wantReasons {
			if !seen {
				allSeen = false
				break
			}
		}
		if allSeen {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	for reason, seen := range wantReasons {
		if !seen {
			t.Errorf("did not observe Event reason=%s on CacheBackend cache/%s within timeout", reason, ns)
		}
	}
}
