package pod

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// TestWebhookOnEnvtest_EndToEnd boots a real apiserver via envtest, installs
// the controller's MutatingWebhookConfiguration, starts the controller-runtime
// manager with the Pod admission handler registered, then creates a
// CacheBackend whose status.endpoint is populated and a matching engine Pod.
// On admission the apiserver routes the CREATE through the webhook over the
// local serving cert and asserts the persisted pod carries the LMCache env +
// the kv-transfer-config arg the adapter writes.
//
// Skips when KUBEBUILDER_ASSETS is unset so default CI stays green.
// Run locally via:
//
//	KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./internal/webhook/pod/...
func TestWebhookOnEnvtest_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS unset; skipping webhook envtest")
	}

	// Install only the Pod MutatingWebhookConfiguration. The shipped
	// config/webhook/manifests.yaml also carries the CacheBackend
	// defaulting/validating webhooks (B3) whose handlers this test does
	// not register; installing them would route every test CacheBackend
	// CREATE to a non-existent path. Splice in a temp file with just the
	// pod webhook configuration.
	podOnlyWebhook := writePodOnlyWebhookManifest(t)

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{podOnlyWebhook},
		},
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest.Start: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("clientgoscheme.AddToScheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("cachev1alpha1.AddToScheme: %v", err)
	}

	wopts := env.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    wopts.LocalServingHost,
			Port:    wopts.LocalServingPort,
			CertDir: wopts.LocalServingCertDir,
		}),
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		t.Fatalf("ctrl.NewManager: %v", err)
	}
	// Opt in to the subscriber sidecar auto-attach path by configuring
	// the registry with a subscriber image — the integration test exists
	// to gate that end-to-end behaviour. Production operators do the same
	// by passing --kvevent-subscriber-image to the controller.
	mgr.GetWebhookServer().Register(WebhookPath, &webhook.Admission{
		Handler: &EngineInjector{
			Reader: mgr.GetAPIReader(),
			Registry: adapterruntime.DefaultRegistry(
				adapterruntime.WithSubscriberImage(adapterruntime.DefaultSubscriberImage),
			),
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()
	// Single combined cleanup: cancel the context FIRST, then wait for the
	// manager goroutine to exit. Two separate t.Cleanups would fire in
	// reverse registration order (LIFO), so registering cancel and then
	// the wait would mean the wait runs first against a still-live
	// context — the 5s deadline would fire on every test instead of the
	// manager exiting promptly on cancellation.
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-mgrErr:
			if err != nil && !isContextCanceledErr(err) {
				t.Logf("manager exited with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Logf("manager did not exit within 5s")
		}
	})

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatalf("manager cache did not sync")
	}
	waitForWebhookReady(t, wopts.LocalServingHost, wopts.LocalServingPort)

	const ns = "default"
	const modelID = "Qwen/Qwen2.5-0.5B-Instruct"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "envtest-cb", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm-test"},
			},
			// backendConfig.model is the source of the
			// subscriber sidecar's --model-id flag. Set it here so
			// the auto-attach assertion below has something to match.
			BackendConfig: map[string]string{"model": modelID},
		},
	}
	if err := mgr.GetClient().Create(ctx, cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}
	cb.Status.Endpoint = "envtest-cb.default.svc.cluster.local:65432"
	if err := mgr.GetClient().Status().Update(ctx, cb); err != nil {
		t.Fatalf("set CacheBackend status: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vllm-engine",
			Namespace: ns,
			Labels:    map[string]string{"app": "vllm-test"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  adapterruntime.EngineContainerName,
				Image: "vllm/vllm-openai-cpu:latest",
				Args:  []string{"--model", "Qwen/Qwen2.5-0.5B-Instruct"},
			}},
		},
	}
	if err := mgr.GetClient().Create(ctx, pod); err != nil {
		t.Fatalf("create Pod: %v", err)
	}

	var got corev1.Pod
	if err := mgr.GetAPIReader().Get(ctx, types.NamespacedName{Namespace: ns, Name: pod.Name}, &got); err != nil {
		t.Fatalf("get pod after create: %v", err)
	}

	mustHaveContainerEnv(t, &got, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
	mustHaveContainerEnv(t, &got, adapterruntime.EnvVLLMUseV1, "1")
	if got.Annotations[AnnotationInjectedBy] != ns+"/"+cb.Name {
		t.Fatalf("annotation %s: got %q want %q",
			AnnotationInjectedBy, got.Annotations[AnnotationInjectedBy], ns+"/"+cb.Name)
	}
	if !containsArgFlag(got.Spec.Containers[0].Args, "--kv-transfer-config") {
		t.Fatalf("--kv-transfer-config flag not injected; args = %v", got.Spec.Containers[0].Args)
	}
	if !containsArgPair(got.Spec.Containers[0].Args, "--model", "Qwen/Qwen2.5-0.5B-Instruct") {
		t.Fatalf("user --model arg was lost; args = %v", got.Spec.Containers[0].Args)
	}

	// The persisted pod must carry the kvevent-subscriber sidecar
	// the adapter rendered, with --model-id derived from the CR — the end-
	// to-end auto-attach gate the ticket DoD calls out.
	if len(got.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers (engine + subscriber); got %d: %v", len(got.Spec.Containers), envtestContainerNames(&got))
	}
	sub := envtestFindContainer(&got, adapterruntime.SubscriberContainerName)
	if sub == nil {
		t.Fatalf("subscriber sidecar missing; containers = %v", envtestContainerNames(&got))
	}
	if !containsArgFlag(sub.Args, "--model-id="+modelID) {
		t.Fatalf("subscriber --model-id not derived from CR; args = %v", sub.Args)
	}
	if !containsArgFlag(sub.Args, "--replica-id=$(POD_NAME)") {
		t.Fatalf("subscriber --replica-id must use downward-API POD_NAME; args = %v", sub.Args)
	}

	pod2 := pod.DeepCopy()
	pod2.Name = "vllm-engine-2"
	pod2.ResourceVersion = ""
	if err := mgr.GetClient().Create(ctx, pod2); err != nil {
		t.Fatalf("create second Pod: %v", err)
	}
	var got2 corev1.Pod
	if err := mgr.GetAPIReader().Get(ctx, types.NamespacedName{Namespace: ns, Name: pod2.Name}, &got2); err != nil {
		t.Fatalf("get second pod: %v", err)
	}
	mustHaveContainerEnv(t, &got2, adapterruntime.EnvLMCacheRemoteURL, "lm://"+cb.Status.Endpoint)
}

// mustHaveContainerEnv fails the test if the first container's env array
// does not include name=value.
func mustHaveContainerEnv(t *testing.T, pod *corev1.Pod, name, value string) {
	t.Helper()
	if len(pod.Spec.Containers) == 0 {
		t.Fatalf("no containers on pod %s", pod.Name)
	}
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == name {
			if e.Value != value {
				t.Fatalf("env %s on %s: got %q want %q", name, pod.Name, e.Value, value)
			}
			return
		}
	}
	t.Fatalf("env %s missing on %s; have %v", name, pod.Name, pod.Spec.Containers[0].Env)
}

// envtestFindContainer returns the container in pod with the given name, or
// nil if absent. The non-envtest unit tests have a similarly named helper —
// the two test files don't share state (envtest_integration_test.go skips
// without KUBEBUILDER_ASSETS), so each file declares its own.
func envtestFindContainer(pod *corev1.Pod, name string) *corev1.Container {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i]
		}
	}
	return nil
}

// envtestContainerNames returns the container names of pod for error messages.
func envtestContainerNames(pod *corev1.Pod) []string {
	out := make([]string, len(pod.Spec.Containers))
	for i, c := range pod.Spec.Containers {
		out[i] = c.Name
	}
	return out
}

func containsArgFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

func containsArgPair(args []string, flag, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

// isContextCanceledErr returns true when err is the manager's clean
// shutdown signal — distinguished from a real run-time failure so the test
// teardown doesn't spam t.Logf on a graceful exit.
func isContextCanceledErr(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return strings.Contains(err.Error(), "context canceled")
}

// waitForWebhookReady polls the manager's serving port over TLS until the
// listener accepts a TCP connection, so subsequent Pod CREATEs don't race a
// not-yet-listening webhook (envtest installs the configuration before the
// manager binds the port).
func waitForWebhookReady(t *testing.T, host string, port int) {
	t.Helper()
	addr := fmt.Sprintf("%s:%d", host, port)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 500 * time.Millisecond}, "tcp", addr,
			&tls.Config{InsecureSkipVerify: true}) // testing only
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("webhook never became ready at %s", addr)
}

// writePodOnlyWebhookManifest emits a temporary MutatingWebhookConfiguration
// containing only the Pod webhook, so envtest doesn't install the
// CacheBackend defaulting/validating webhooks alongside it (those would
// route every test CacheBackend CREATE to a path we don't serve in this
// test). The content mirrors the generated config/webhook/manifests.yaml
// for the Pod webhook — kept narrow on purpose so an unrelated webhook
// rename in the generated manifest doesn't silently de-install this one.
func writePodOnlyWebhookManifest(t *testing.T) string {
	t.Helper()
	manifest := `---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: mutating-webhook-configuration-pod-only
webhooks:
- admissionReviewVersions:
  - v1
  clientConfig:
    service:
      name: webhook-service
      namespace: system
      path: ` + WebhookPath + `
  failurePolicy: Ignore
  name: mpod.inferencecache.io
  rules:
  - apiGroups:
    - ""
    apiVersions:
    - v1
    operations:
    - CREATE
    resources:
    - pods
  sideEffects: None
`
	dir := t.TempDir()
	path := filepath.Join(dir, "pod-webhook.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o644); err != nil {
		t.Fatalf("write temp webhook manifest: %v", err)
	}
	return path
}
