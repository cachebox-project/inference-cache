package v1alpha1

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// TestCacheBackendDefaulter_MinimumViableYAMLGetsFullyDefaulted is the
// end-to-end pin for the defaulter-sweep operator-UX win: applying a
// CacheBackend with ONLY the three fields the substrate genuinely cannot
// guess (engineSelector, backendConfig.model, and a name) must produce a
// fully-defaulted CR with every Phase-1 default stamped — Type=LMCache,
// DeploymentKind=Deployment, Replicas=1, Integration.Engine=vllm,
// Integration.Role=ReadWrite, Integration.FailOpen=true,
// Integration.FirstEventTimeout=5m. The apiserver in the loop applies
// `+kubebuilder:default=` markers; the webhook materialises
// spec.integration solely to persist firstEventTimeout.
//
// This test boots a real apiserver via envtest so the CRD-schema defaults
// (which a raw-struct unit test cannot exercise) are part of the assertion.
// Skips when KUBEBUILDER_ASSETS is unset so default CI stays green. Run via:
//
//	KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./internal/webhook/v1alpha1/...
func TestCacheBackendDefaulter_MinimumViableYAMLGetsFullyDefaulted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS unset; skipping CacheBackend defaulter envtest")
	}

	// Install the SHIPPED config/webhook/manifests.yaml so the test also
	// guards the generated CacheBackend webhook wiring (path, resource,
	// operations) against drift.
	webhookManifest := filepath.Join("..", "..", "..", "config", "webhook", "manifests.yaml")

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
	// Nil registry → defaultShippingRegistry (DefaultRegistry + External),
	// matching the production cmd/controller wiring.
	if err := SetupCacheBackendWebhookWithManager(mgr, nil); err != nil {
		t.Fatalf("SetupCacheBackendWebhookWithManager: %v", err)
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

	// Writes go through the cached client so the manager's informers see
	// every CREATE; reads back use the live API reader so we see the
	// apiserver's persisted view immediately (the informer is allowed to
	// lag behind a CREATE and the assertions below need the post-default
	// shape right after Create returns, not whenever the cache catches up).
	k8s := mgr.GetClient()
	live := mgr.GetAPIReader()
	mkNamespace(t, ctx, k8s, "team-a")

	// --- Minimum-viable CR: engineSelector + backendConfig.model only ---
	//
	// An apply with no Type, no DeploymentKind, no Replicas, no Integration
	// block, no Storage, no Autoscaling. Every other field must be stamped
	// by the apiserver (kubebuilder-marker defaults) + the defaulter webhook
	// (cluster-context defaults).
	mvCR := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "minimum", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app.kubernetes.io/name": "vllm"},
			},
			BackendConfig: map[string]string{
				"model": "meta-llama/Meta-Llama-3-8B-Instruct",
			},
		},
	}
	if err := k8s.Create(ctx, mvCR); err != nil {
		t.Fatalf("minimum-viable CacheBackend should be admitted: %v", err)
	}

	// Read back the persisted CR — what the apiserver actually stored
	// after every default layer (CRD-schema markers + the webhook) ran.
	var got cachev1alpha1.CacheBackend
	if err := live.Get(ctx, client.ObjectKey{Name: "minimum", Namespace: "team-a"}, &got); err != nil {
		t.Fatalf("get back persisted CR: %v", err)
	}

	// --- Phase-1 default surface assertions ---
	//
	// Each assertion below pins one item from the default sweep. If a future
	// change drops a default marker or rewrites the defaulter, the
	// corresponding line fails — surfacing the operator-UX regression at PR
	// time instead of in a confused operator's `kubectl get -o yaml`.

	if want := cachev1alpha1.CacheBackendTypeLMCache; got.Spec.Type != want {
		t.Errorf("spec.type = %q, want %q (kubebuilder default)", got.Spec.Type, want)
	}
	if want := cachev1alpha1.CacheBackendDeploymentKindDeployment; got.Spec.DeploymentKind != want {
		t.Errorf("spec.deploymentKind = %q, want %q (kubebuilder default)", got.Spec.DeploymentKind, want)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Errorf("spec.replicas = %v, want 1 (kubebuilder default)", got.Spec.Replicas)
	}
	if got.Spec.Integration == nil {
		t.Fatalf("spec.integration was not materialised by the defaulter; got nil")
	}
	if want := "vllm"; got.Spec.Integration.Engine != want {
		t.Errorf("spec.integration.engine = %q, want %q (kubebuilder default)", got.Spec.Integration.Engine, want)
	}
	if want := cachev1alpha1.CacheBackendIntegrationRoleReadWrite; got.Spec.Integration.Role != want {
		t.Errorf("spec.integration.role = %q, want %q (kubebuilder default)", got.Spec.Integration.Role, want)
	}
	if got.Spec.Integration.FailOpen == nil || !*got.Spec.Integration.FailOpen {
		t.Errorf("spec.integration.failOpen = %v, want true (kubebuilder default)", got.Spec.Integration.FailOpen)
	}
	if got.Spec.Integration.FirstEventTimeout == nil ||
		got.Spec.Integration.FirstEventTimeout.Duration != defaultFirstEventTimeout {
		t.Errorf("spec.integration.firstEventTimeout = %v, want %s (defaulter-stamped)",
			got.Spec.Integration.FirstEventTimeout, defaultFirstEventTimeout)
	}

	// --- Non-clobber pin: an explicit CR overrides every default ---
	//
	// Same minimum-viable shape but with an operator-set replicas and a
	// pinned Type. Both values must survive every default layer — proving
	// the "defaulter never clobbers" contract holds for the new markers
	// just as it did for the webhook-stamped defaults before the marker
	// sweep landed.
	explicitCR := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "explicit", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeExternal,
			Replicas: i32p(5),
			Endpoint: "team-a-cache.team-a.svc.cluster.local:9000",
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app.kubernetes.io/name": "vllm"},
			},
			BackendConfig: map[string]string{
				"model": "meta-llama/Meta-Llama-3-8B-Instruct",
			},
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadOnly,
			},
		},
	}
	if err := k8s.Create(ctx, explicitCR); err != nil {
		t.Fatalf("explicit CacheBackend should be admitted: %v", err)
	}

	var explicit cachev1alpha1.CacheBackend
	if err := live.Get(ctx, client.ObjectKey{Name: "explicit", Namespace: "team-a"}, &explicit); err != nil {
		t.Fatalf("get back explicit CR: %v", err)
	}
	if explicit.Spec.Type != cachev1alpha1.CacheBackendTypeExternal {
		t.Errorf("operator type clobbered: got %q, want External", explicit.Spec.Type)
	}
	if explicit.Spec.Replicas == nil || *explicit.Spec.Replicas != 5 {
		t.Errorf("operator replicas clobbered: got %v, want 5", explicit.Spec.Replicas)
	}
	if explicit.Spec.Integration.Role != cachev1alpha1.CacheBackendIntegrationRoleReadOnly {
		t.Errorf("operator integration.role clobbered: got %q, want ReadOnly", explicit.Spec.Integration.Role)
	}

	// --- Autoscaling defaulter-computed minReplicas ---
	//
	// Pins the one non-literal default: when an operator opts into
	// autoscaling without pinning the floor, the defaulter computes
	// minReplicas from spec.replicas (post-marker-default, so =1 here)
	// rather than a hard-coded constant. This is the only field on the
	// default that needs cluster context — every other default rides on
	// a marker.
	hpaCR := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "hpa", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app.kubernetes.io/name": "vllm"},
			},
			BackendConfig: map[string]string{
				"model": "meta-llama/Meta-Llama-3-8B-Instruct",
			},
			Autoscaling: &cachev1alpha1.CacheBackendAutoscalingSpec{
				MaxReplicas: 10,
			},
		},
	}
	if err := k8s.Create(ctx, hpaCR); err != nil {
		t.Fatalf("HPA-opted-in CacheBackend should be admitted: %v", err)
	}
	var hpa cachev1alpha1.CacheBackend
	if err := live.Get(ctx, client.ObjectKey{Name: "hpa", Namespace: "team-a"}, &hpa); err != nil {
		t.Fatalf("get back hpa CR: %v", err)
	}
	if hpa.Spec.Autoscaling == nil || hpa.Spec.Autoscaling.MinReplicas == nil ||
		*hpa.Spec.Autoscaling.MinReplicas != 1 {
		t.Errorf("autoscaling.minReplicas = %v, want 1 (= post-default spec.replicas)",
			hpa.Spec.Autoscaling.MinReplicas)
	}
}
