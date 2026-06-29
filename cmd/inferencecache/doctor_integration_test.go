package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestDoctorEnvtest spins up a real apiserver, applies the CRDs, creates a
// CacheBackend whose engineSelector matches no pods, then runs the actual
// doctor binary against the apiserver in --config-only mode and asserts it
// surfaces the selector-mismatch WARN (CB002) and exits 1.
//
// Skipped unless KUBEBUILDER_ASSETS is set (so default `go test ./...` stays
// green). Run locally with:
//
//	KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./cmd/inferencecache/...
func TestDoctorEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with `KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test`")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgoscheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("cachev1alpha1: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	ctx := context.Background()
	const ns = "doctor-it"
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	// engineSelector matches no pods in the namespace => LikelySelectorMismatch.
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "mismatched", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "nonexistent-engine"},
			},
		},
	}
	if err := c.Create(ctx, cb); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}

	kubeconfig := writeKubeconfig(t, cfg)
	bin := buildDoctorBinary(t)

	cmd := exec.CommandContext(ctx, bin, "doctor",
		"--config-only",
		"--kubeconfig", kubeconfig,
		"--namespace", ns,
		"--output", "json",
		"--no-color",
	)
	out, runErr := cmd.Output()

	exitCode := 0
	if runErr != nil {
		ee, ok := runErr.(*exec.ExitError)
		if !ok {
			t.Fatalf("run doctor: %v (output: %s)", runErr, out)
		}
		exitCode = ee.ExitCode()
	}

	var report struct {
		Summary  struct{ ExitCode int } `json:"summary"`
		Findings []struct {
			Code   string `json:"code"`
			Status string `json:"status"`
		} `json:"findings"`
	}
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatalf("parse doctor JSON output: %v\n%s", err, out)
	}

	var foundCB002 bool
	for _, f := range report.Findings {
		if f.Code == "CB002" {
			foundCB002 = true
			if f.Status != "WARN" {
				t.Errorf("CB002 status = %q, want WARN", f.Status)
			}
		}
	}
	if !foundCB002 {
		t.Errorf("expected CB002 (LikelySelectorMismatch) WARN; got findings %s", out)
	}
	if exitCode != 1 {
		t.Errorf("doctor exit code = %d, want 1 (any WARN); output: %s", exitCode, out)
	}
	if report.Summary.ExitCode != 1 {
		t.Errorf("summary.exitCode = %d, want 1", report.Summary.ExitCode)
	}
}

// writeKubeconfig serializes the envtest rest.Config (client-cert auth) into a
// kubeconfig file the exec'd binary can load via --kubeconfig.
func writeKubeconfig(t *testing.T, cfg *rest.Config) string {
	t.Helper()
	api := clientcmdapi.NewConfig()
	api.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   cfg.Host,
		CertificateAuthorityData: cfg.CAData,
	}
	api.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
	}
	api.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
	api.CurrentContext = "envtest"

	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*api, path); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// buildDoctorBinary compiles the doctor command to a temp path so the test
// exercises the real binary end-to-end (flag parsing, client wiring, exit code).
func buildDoctorBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "inferencecache")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("build doctor binary: %v", err)
	}
	return bin
}
