package v1alpha1

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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestCachePolicyCacheTenantWebhooks_OnEnvtest boots a real apiserver via
// envtest, installs ONLY the CachePolicy + CacheTenant webhook configurations,
// starts a manager with both webhooks registered, and exercises the cross-CR
// admission rules end-to-end (the apiserver routes each CREATE/UPDATE through
// the webhook over the local serving cert):
//
//   - a first CachePolicy in a namespace is admitted; a SECOND is rejected
//     with an error naming the existing policy; a policy in another namespace
//     is fine (the rule is per-namespace).
//   - a first CacheTenant is admitted; a second with the SAME tenantID is
//     rejected naming the existing tenant; a different tenantID is fine; the
//     same tenantID in another namespace is fine.
//
// Skips when KUBEBUILDER_ASSETS is unset so default CI stays green. Run via:
//
//	KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./internal/webhook/v1alpha1/...
func TestCachePolicyCacheTenantWebhooks_OnEnvtest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS unset; skipping webhook envtest")
	}

	// Install only the CachePolicy + CacheTenant webhook configurations. The
	// shipped config/webhook/manifests.yaml also carries the pod and
	// CacheBackend webhooks whose handlers this test does not register;
	// installing them would route unrelated CREATEs to non-existent paths.
	webhookManifest := writeCachePolicyTenantWebhookManifest(t)

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{webhookManifest},
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
	if err := SetupCachePolicyWebhookWithManager(mgr); err != nil {
		t.Fatalf("SetupCachePolicyWebhookWithManager: %v", err)
	}
	if err := SetupCacheTenantWebhookWithManager(mgr); err != nil {
		t.Fatalf("SetupCacheTenantWebhookWithManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()
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
	waitForWebhookPort(t, wopts.LocalServingHost, wopts.LocalServingPort)

	k8s := mgr.GetClient()
	mkNamespace(t, ctx, k8s, "team-a")
	mkNamespace(t, ctx, k8s, "team-b")

	// --- CachePolicy: one-per-namespace ---------------------------------
	if err := k8s.Create(ctx, &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "team-a"},
	}); err != nil {
		t.Fatalf("first CachePolicy in team-a should be admitted: %v", err)
	}

	err = k8s.Create(ctx, &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: "team-a"},
	})
	if err == nil {
		t.Fatalf("second CachePolicy in team-a must be rejected")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("expected Invalid, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "primary") {
		t.Errorf("rejection should name the existing policy 'primary': %v", err)
	}

	if err := k8s.Create(ctx, &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "team-b"},
	}); err != nil {
		t.Fatalf("CachePolicy in a different namespace must be admitted: %v", err)
	}

	// --- CacheTenant: tenantID uniqueness within namespace --------------
	if err := k8s.Create(ctx, newTenantCR("vision", "team-a", "team-vision")); err != nil {
		t.Fatalf("first CacheTenant should be admitted: %v", err)
	}

	err = k8s.Create(ctx, newTenantCR("vision-dup", "team-a", "team-vision"))
	if err == nil {
		t.Fatalf("duplicate tenantID in team-a must be rejected")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("expected Invalid, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "vision") || !strings.Contains(err.Error(), "team-vision") {
		t.Errorf("rejection should name the existing tenant and tenantID: %v", err)
	}

	if err := k8s.Create(ctx, newTenantCR("search", "team-a", "team-search")); err != nil {
		t.Fatalf("distinct tenantID in same namespace must be admitted: %v", err)
	}

	if err := k8s.Create(ctx, newTenantCR("vision", "team-b", "team-vision")); err != nil {
		t.Fatalf("same tenantID in a different namespace must be admitted: %v", err)
	}
}

func newTenantCR(name, ns, tenantID string) *cachev1alpha1.CacheTenant {
	return &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       cachev1alpha1.CacheTenantSpec{TenantID: tenantID},
	}
}

func mkNamespace(t *testing.T, ctx context.Context, k8s client.Client, name string) {
	t.Helper()
	if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

// writeCachePolicyTenantWebhookManifest writes a temp webhook manifest
// carrying only the CachePolicy + CacheTenant defaulting/validating
// configurations. envtest's WebhookInstallOptions rewrites each clientConfig
// to point at the manager's local serving port and injects the serving CA.
func writeCachePolicyTenantWebhookManifest(t *testing.T) string {
	t.Helper()
	const manifest = `---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: cachepolicy-cachetenant-mutating
webhooks:
- admissionReviewVersions: [v1]
  clientConfig:
    service:
      name: webhook-service
      namespace: system
      path: /mutate-inferencecache-io-v1alpha1-cachepolicy
  failurePolicy: Fail
  name: mcachepolicy.inferencecache.io
  rules:
  - apiGroups: [inferencecache.io]
    apiVersions: [v1alpha1]
    operations: [CREATE, UPDATE]
    resources: [cachepolicies]
  sideEffects: None
- admissionReviewVersions: [v1]
  clientConfig:
    service:
      name: webhook-service
      namespace: system
      path: /mutate-inferencecache-io-v1alpha1-cachetenant
  failurePolicy: Fail
  name: mcachetenant.inferencecache.io
  rules:
  - apiGroups: [inferencecache.io]
    apiVersions: [v1alpha1]
    operations: [CREATE, UPDATE]
    resources: [cachetenants]
  sideEffects: None
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: cachepolicy-cachetenant-validating
webhooks:
- admissionReviewVersions: [v1]
  clientConfig:
    service:
      name: webhook-service
      namespace: system
      path: /validate-inferencecache-io-v1alpha1-cachepolicy
  failurePolicy: Fail
  name: vcachepolicy.inferencecache.io
  rules:
  - apiGroups: [inferencecache.io]
    apiVersions: [v1alpha1]
    operations: [CREATE, UPDATE]
    resources: [cachepolicies]
  sideEffects: None
- admissionReviewVersions: [v1]
  clientConfig:
    service:
      name: webhook-service
      namespace: system
      path: /validate-inferencecache-io-v1alpha1-cachetenant
  failurePolicy: Fail
  name: vcachetenant.inferencecache.io
  rules:
  - apiGroups: [inferencecache.io]
    apiVersions: [v1alpha1]
    operations: [CREATE, UPDATE]
    resources: [cachetenants]
  sideEffects: None
`
	dir := t.TempDir()
	path := filepath.Join(dir, "cachepolicy-cachetenant-webhook.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o644); err != nil {
		t.Fatalf("write temp webhook manifest: %v", err)
	}
	return path
}

// waitForWebhookPort polls the manager's TLS serving port until it accepts a
// connection, so the first CREATE doesn't race a not-yet-listening webhook
// (envtest installs the configuration before the manager binds the port).
func waitForWebhookPort(t *testing.T, host string, port int) {
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

// isContextCanceledErr returns true when err is the manager's clean shutdown
// signal, so teardown doesn't log it as a failure.
func isContextCanceledErr(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return strings.Contains(err.Error(), "context canceled")
}
