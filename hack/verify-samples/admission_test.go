package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
	cachewebhookv1alpha1 "github.com/cachebox-project/inference-cache/internal/webhook/v1alpha1"
)

// TestVerifySamplesAdmissionEndToEnd exercises the same admission path
// the gate drives — envtest apiserver + the CacheBackend webhook
// installed from config/webhook/manifests.yaml and registered with the
// shipping adapter registry — and asserts the path both ACCEPTS a known-
// good CacheBackend and REJECTS a known-bad one with an actionable error.
//
// This is the regression backstop the helper-only unit tests don't
// provide: if a future refactor silently stopped invoking admission
// (e.g. broken webhook registration, lost manifest install, kubectl
// errors swallowed), this test fails even when every shipping sample
// still happens to be valid.
//
// Skipped unless KUBEBUILDER_ASSETS is set so default `go test` stays
// fast; CI installs envtest binaries before `make test-race`, so the
// test runs there. Locally run via
//
//	KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./hack/verify-samples/...
func TestVerifySamplesAdmissionEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping envtest in short mode")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS unset; skipping admission end-to-end test")
	}

	rootDir, err := repoRootFromTest()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(rootDir, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join(rootDir, "config", "webhook", "manifests.yaml")},
			// LocalServingPort intentionally left at 0 (the zero
			// value). envtest's generateHostPort (controller-runtime
			// pkg/envtest/webhook.go:121-127) calls addr.Suggest on
			// a zero port, which picks a FREE OS port and stores it
			// back in wopts.LocalServingPort. The webhook server
			// below consumes that allocated port — so two test
			// binaries (e.g. this package and internal/webhook/pod)
			// running in parallel under `go test ./...` each get a
			// distinct listener. No fixed-port collision.
		},
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest.Start: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("clientgoscheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("cachev1alpha1 scheme: %v", err)
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
	if err := cachewebhookv1alpha1.SetupCacheBackendWebhookWithManager(mgr, nil); err != nil {
		t.Fatalf("register CacheBackend webhook: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-mgrErr:
		case <-time.After(5 * time.Second):
			t.Log("manager did not exit within 5s of cancel")
		}
	})

	if err := waitForWebhookReady(wopts.LocalServingHost, wopts.LocalServingPort, webhookReadyTimeout); err != nil {
		t.Fatalf("webhook never became ready: %v", err)
	}

	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	// Known-GOOD: matches an adapter the shipping registry knows.
	// DryRunAll → request goes through full admission but does not persist.
	t.Run("good_sample_is_admitted", func(t *testing.T) {
		good := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "good", Namespace: "default"},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type: cachev1alpha1.CacheBackendTypeLMCache,
				Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
					Engine: "vllm",
				},
			},
		}
		if err := cl.Create(ctx, good, client.DryRunAll); err != nil {
			t.Fatalf("expected good sample to be admitted, got error: %v", err)
		}
	})

	// Known-BAD: an engine the adapter registry has never heard of.
	// Admission MUST reject with a message that names the offending
	// (engine, type) pair so a real operator gets an actionable error.
	t.Run("bad_engine_is_rejected", func(t *testing.T) {
		bad := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "default"},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type: cachev1alpha1.CacheBackendTypeLMCache,
				Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
					Engine: "bogus",
				},
			},
		}
		err := cl.Create(ctx, bad, client.DryRunAll)
		if err == nil {
			t.Fatal("expected bad sample to be rejected by admission, got nil error")
		}
		// Loose substring match so we don't pin the test to the exact
		// rule wording, but the failure mode (unsupported pair) must
		// stay surfaced in the error.
		if !strings.Contains(err.Error(), "no runtime adapter") {
			t.Fatalf("expected error to mention 'no runtime adapter'; got: %v", err)
		}
	})
}

// repoRootFromTest walks up from the test's working directory looking
// for go.mod. Tests run with cwd set to the package dir, so the walk is
// equivalent to the production repoRoot but kept separate to avoid
// taking a hard dependency on package internals.
func repoRootFromTest() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
