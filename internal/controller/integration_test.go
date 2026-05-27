package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// These tests run the reconciler against a real kube-apiserver (envtest), so they
// cover behavior the fake client cannot: real API-server defaulting, idempotency
// against that defaulting, Deployment rollout-derived health, explicit child
// cleanup, and (in TestIntegrationCacheBackendWatch) the Owns() watch re-trigger.
//
// They are skipped unless KUBEBUILDER_ASSETS is set (e.g. `make test-env`), so the
// default `go test ./...` in CI does not require envtest binaries.

func skipWithoutEnvtest(t *testing.T) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with `KUBEBUILDER_ASSETS=$(make test-env) go test` for envtest")
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

func anyArgContains(args []string, sub string) bool {
	for _, a := range args {
		if strings.Contains(a, sub) {
			return true
		}
	}
	return false
}

func findEnvVar(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
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

	t.Run("LMCacheTemplatedWorkloadShape", func(t *testing.T) {
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
		// Selector is a subset of the pod template labels, with backend/engine identity.
		sel := dep.Spec.Selector.MatchLabels
		podLabels := dep.Spec.Template.Labels
		for k, v := range sel {
			if podLabels[k] != v {
				t.Fatalf("selector label %s=%s missing from pod labels %v", k, v, podLabels)
			}
		}
		if podLabels["inferencecache.io/backend-type"] != "lmcache" || podLabels["inferencecache.io/engine"] != "vllm" {
			t.Fatalf("pod labels missing backend/engine identity: %v", podLabels)
		}

		c := dep.Spec.Template.Spec.Containers[0]
		if c.Name != "vllm" {
			t.Fatalf("container name = %q, want vllm", c.Name)
		}
		if !strings.Contains(c.Image, "lmcache/vllm-openai") {
			t.Fatalf("default image = %q, want the lmcache reference image", c.Image)
		}
		if len(c.Command) < 2 || c.Command[0] != "vllm" || c.Command[1] != "serve" {
			t.Fatalf("command = %v, want [vllm serve <model>]", c.Command)
		}
		if !strings.Contains(c.Command[len(c.Command)-1], "Llama-3.1-8B-Instruct") {
			t.Fatalf("default model = %v", c.Command)
		}
		if !containsStr(c.Args, "--enable-prefix-caching") ||
			!containsStr(c.Args, "--kv-transfer-config") ||
			!containsStr(c.Args, "--kv-events-config") {
			t.Fatalf("args missing required flags: %v", c.Args)
		}
		if !anyArgContains(c.Args, `"kv_connector":"LMCacheConnectorV1"`) {
			t.Fatalf("args missing LMCache connector config: %v", c.Args)
		}
		if !anyArgContains(c.Args, `"enable_kv_cache_events":true`) {
			t.Fatalf("args missing KV-events config: %v", c.Args)
		}

		if !hasEnv(c.Env, "VLLM_USE_V1", "1") ||
			!hasEnv(c.Env, "LMCACHE_CHUNK_SIZE", "256") ||
			!hasEnv(c.Env, "LMCACHE_LOCAL_CPU", "True") ||
			!hasEnv(c.Env, "LMCACHE_MAX_LOCAL_CPU_SIZE", "20") {
			t.Fatalf("env missing LMCache settings: %v", c.Env)
		}
		hf := findEnvVar(c.Env, "HF_TOKEN")
		if hf == nil || hf.ValueFrom == nil || hf.ValueFrom.SecretKeyRef == nil ||
			hf.ValueFrom.SecretKeyRef.Name != "hf-token" || hf.ValueFrom.SecretKeyRef.Key != "token" ||
			hf.ValueFrom.SecretKeyRef.Optional == nil || !*hf.ValueFrom.SecretKeyRef.Optional {
			t.Fatalf("HF_TOKEN secret ref = %+v, want optional hf-token/token", hf)
		}

		if len(c.Ports) != 3 {
			t.Fatalf("container ports = %d, want 3", len(c.Ports))
		}
		for _, p := range c.Ports {
			if p.Protocol != corev1.ProtocolTCP {
				t.Fatalf("port %s protocol = %q, want TCP", p.Name, p.Protocol)
			}
		}
		gpu := c.Resources.Limits[corev1.ResourceName("nvidia.com/gpu")]
		if gpu.Value() != 1 {
			t.Fatalf("nvidia.com/gpu limit = %v, want 1", gpu.Value())
		}
		if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet == nil ||
			c.ReadinessProbe.HTTPGet.Path != "/health" || c.ReadinessProbe.HTTPGet.Port.StrVal != "http" {
			t.Fatalf("readiness probe = %+v, want HTTP /health on http port", c.ReadinessProbe)
		}

		spec := dep.Spec.Template.Spec
		if spec.SchedulerName != "default-scheduler" {
			t.Fatalf("schedulerName = %q, want default-scheduler", spec.SchedulerName)
		}
		if spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != 30 {
			t.Fatalf("terminationGracePeriodSeconds = %v, want 30", spec.TerminationGracePeriodSeconds)
		}
		var shm *corev1.Volume
		for i := range spec.Volumes {
			if spec.Volumes[i].Name == "shm" {
				shm = &spec.Volumes[i]
			}
		}
		if shm == nil || shm.EmptyDir == nil || shm.EmptyDir.Medium != corev1.StorageMediumMemory {
			t.Fatalf("shm volume = %+v, want in-memory emptyDir", shm)
		}

		svc := getService(t, r, "cache", ns)
		if svc.Spec.Type != corev1.ServiceTypeClusterIP {
			t.Fatalf("service type = %q, want ClusterIP", svc.Spec.Type)
		}
		if svc.Spec.ClusterIP == "" {
			t.Fatalf("expected API server to allocate a ClusterIP")
		}
		if len(svc.Spec.Ports) != 3 {
			t.Fatalf("service ports = %d, want 3", len(svc.Spec.Ports))
		}

		// Real API-server pod defaulting is applied to the stored template.
		if spec.RestartPolicy == "" || spec.DNSPolicy == "" {
			t.Fatalf("expected pod defaulting, got restartPolicy=%q dnsPolicy=%q", spec.RestartPolicy, spec.DNSPolicy)
		}
	})

	t.Run("StatusEndpointAndObservedGeneration", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		wantEndpoint := fmt.Sprintf("cache.%s.svc.cluster.local:8000", ns)
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

	t.Run("HealthTransitions", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.Replicas = ptrInt32(2)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if got := getBackend(t, r, "cache", ns).Status.Health; got != cachev1alpha1.CacheBackendHealthPending {
			t.Fatalf("fresh health = %q, want Pending", got)
		}

		// Mid-rollout: generation observed but updated replicas lag -> Pending.
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
			d.Status.Replicas = 2
			d.Status.UpdatedReplicas = 1
			d.Status.AvailableReplicas = 2
			d.Status.ReadyReplicas = 2
		})
		reconcile(t, r, "cache", ns)
		if got := getBackend(t, r, "cache", ns).Status.Health; got != cachev1alpha1.CacheBackendHealthPending {
			t.Fatalf("mid-rollout health = %q, want Pending", got)
		}

		// Fully rolled out -> Ready.
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
			d.Status.Replicas = 2
			d.Status.UpdatedReplicas = 2
			d.Status.AvailableReplicas = 2
			d.Status.ReadyReplicas = 2
		})
		reconcile(t, r, "cache", ns)
		cb = getBackend(t, r, "cache", ns)
		if cb.Status.Health != cachev1alpha1.CacheBackendHealthReady {
			t.Fatalf("rolled-out health = %q, want Ready", cb.Status.Health)
		}
		if cond := findCondition(cb.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionTrue {
			t.Fatalf("Ready condition = %+v, want True", cond)
		}

		// Rolled out but replicas unavailable -> Degraded.
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
			d.Status.Replicas = 2
			d.Status.UpdatedReplicas = 2
			d.Status.AvailableReplicas = 1
			d.Status.ReadyReplicas = 1
		})
		reconcile(t, r, "cache", ns)
		if got := getBackend(t, r, "cache", ns).Status.Health; got != cachev1alpha1.CacheBackendHealthDegraded {
			t.Fatalf("unavailable health = %q, want Degraded", got)
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
		// Even once the (empty) rollout is observed, a zero-replica backend isn't Ready.
		setDeploymentStatus(t, r, "cache", ns, func(d *appsv1.Deployment) {
			d.Status.ObservedGeneration = d.Generation
		})
		reconcile(t, r, "cache", ns)
		cb = getBackend(t, r, "cache", ns)
		if cb.Status.Health == cachev1alpha1.CacheBackendHealthReady {
			t.Fatalf("zero-replica health = Ready, want non-Ready")
		}
		if cond := findCondition(cb.Status.Conditions, conditionTypeReady); cond == nil || cond.Status != metav1.ConditionFalse {
			t.Fatalf("Ready condition = %+v, want False for zero replicas", cond)
		}
	})

	t.Run("ImageOverrideAndUpdate", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := lmcacheBackend("cache", ns)
		cb.Spec.BackendConfig = map[string]string{"image": "example.com/vllm-lmcache:v1", "model": "org/custom-model"}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		dep := getDeployment(t, r, "cache", ns)
		if got := dep.Spec.Template.Spec.Containers[0].Image; got != "example.com/vllm-lmcache:v1" {
			t.Fatalf("image = %q, want override", got)
		}
		if got := dep.Spec.Template.Spec.Containers[0].Command; got[len(got)-1] != "org/custom-model" {
			t.Fatalf("model = %v, want override", got)
		}

		live := getBackend(t, r, "cache", ns)
		live.Spec.BackendConfig["image"] = "example.com/vllm-lmcache:v2"
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("update image: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if got := getDeployment(t, r, "cache", ns).Spec.Template.Spec.Containers[0].Image; got != "example.com/vllm-lmcache:v2" {
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
		svc.Spec.Ports = svc.Spec.Ports[:1]
		if err := k8s.Update(ctx, svc); err != nil {
			t.Fatalf("drift service: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if got := len(getService(t, r, "cache", ns).Spec.Ports); got != 3 {
			t.Fatalf("service ports = %d, want 3 restored", got)
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
		if cond := findCondition(cb.Status.Conditions, conditionTypeReady); cond != nil {
			t.Fatalf("Ready condition = %+v, want removed for external", cond)
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
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "mc", Namespace: ns},
			Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeMooncake},
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

	t.Run("MissingObjectIsNoError", func(t *testing.T) {
		ns := freshNS(t, k8s)
		reconcile(t, r, "does-not-exist", ns)
	})
}

// TestIntegrationCacheBackendWatch runs a real manager so the Owns(Deployment)
// watch is exercised end to end: deleting the managed child re-triggers reconcile
// and the controller recreates it.
func TestIntegrationCacheBackendWatch(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, cfg := startEnv(t)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
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
	if err := k8s.Create(context.Background(), lmcacheBackend("cache", ns)); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}

	key := types.NamespacedName{Name: "cache", Namespace: ns}
	waitForDeployment := func(what string) string {
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

	originalUID := waitForDeployment("be created by the manager")

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
			return // recreated with a new UID — watch re-trigger confirmed
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("deployment was not recreated after deletion (Owns watch did not re-trigger)")
}
