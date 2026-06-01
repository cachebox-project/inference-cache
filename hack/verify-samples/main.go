// Package main is the verify-samples CI helper. It runs every YAML file under
// config/samples/ through admission against an in-process envtest apiserver
// plus the CacheBackend admission webhook, using `kubectl apply
// --dry-run=server`.
//
// It catches sample-vs-schema drift before release: if a sample would be
// rejected by a real cluster (unknown engine value, removed CRD field,
// unregistered runtime/backend pair, reserved-arg/env conflict, etc.), the
// gate fails the PR.
//
// Opt-out: a sample whose top-of-file comment block contains a line equal
// to `# verify-samples: skip` is reported as SKIP and not applied. Use this
// only for intentionally-illustrative samples that the gate would otherwise
// reject (rare).
//
// Driven by `make verify-samples`, which sets KUBEBUILDER_ASSETS and runs
// `go run ./hack/verify-samples`. The CacheBackend webhook is registered
// in-process with the same shipping adapter registry the controller uses
// in production, so the gate exercises the validator an operator would
// hit on `kubectl apply` against a real cluster.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cachewebhookv1alpha1 "github.com/cachebox-project/inference-cache/internal/webhook/v1alpha1"
)

// skipMarker is the opt-out comment a sample author can place at the top of
// a YAML file to exclude it from the gate. Must appear on its own line in
// the top-of-file comment block, before any non-comment line.
const skipMarker = "# verify-samples: skip"

// webhookReadyTimeout caps how long we'll wait for the in-process webhook
// server to accept TLS connections after the manager starts. 20s matches
// the existing envtest webhook integration test and is generous on CI.
const webhookReadyTimeout = 20 * time.Second

// perSampleTimeout bounds each `kubectl apply --dry-run=server` invocation
// so a hung kubectl, apiserver, or webhook fails the gate with a clear
// per-file diagnostic instead of waiting for the outer CI job timeout.
// 30s is generous against the local envtest apiserver (real applies take
// <100ms) but tight enough that a deadlock surfaces quickly.
const perSampleTimeout = 30 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("verify-samples: %v", err)
	}
}

func run() error {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return fmt.Errorf("KUBEBUILDER_ASSETS unset — run via `make verify-samples` (the target installs envtest binaries first)")
	}

	rootDir, err := repoRoot()
	if err != nil {
		return fmt.Errorf("locate repo root: %w", err)
	}
	samplesDir := filepath.Join(rootDir, "config", "samples")
	crdDir := filepath.Join(rootDir, "config", "crd", "bases")
	webhookManifest := filepath.Join(rootDir, "config", "webhook", "manifests.yaml")

	samples, err := listSamples(samplesDir)
	if err != nil {
		return fmt.Errorf("list samples: %w", err)
	}
	if len(samples) == 0 {
		return fmt.Errorf("no YAML samples found under %s", samplesDir)
	}

	fmt.Printf("verify-samples: %d sample file(s) under %s\n", len(samples), mustRel(rootDir, samplesDir))

	// Bring up envtest with our CRDs + the generated webhook configuration
	// installed. envtest patches the manifest's caBundle to match the
	// auto-issued local serving cert.
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{webhookManifest},
		},
	}
	cfg, err := env.Start()
	if err != nil {
		return fmt.Errorf("envtest.Start: %w", err)
	}
	defer func() {
		if err := env.Stop(); err != nil {
			log.Printf("envtest.Stop: %v", err)
		}
	}()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("clientgoscheme: %w", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("cachev1alpha1 scheme: %w", err)
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
		return fmt.Errorf("ctrl.NewManager: %w", err)
	}

	// Register the CacheBackend defaulting + validating webhooks with the
	// shipping adapter registry (nil → defaultShippingRegistry, the same
	// wiring the controller uses in production). The Pod injector is
	// intentionally NOT registered: its MutatingWebhookConfiguration uses
	// failurePolicy=Ignore, so Pod creates (none in this suite anyway)
	// would just bypass it; CacheBackend is what we need to exercise.
	if err := cachewebhookv1alpha1.SetupCacheBackendWebhookWithManager(mgr, nil); err != nil {
		return fmt.Errorf("register CacheBackend webhook: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()
	defer func() {
		// Cancel first, then wait briefly for the manager goroutine to
		// drain. The select bound keeps shutdown from hanging if the
		// manager misbehaves.
		cancel()
		select {
		case err := <-mgrErr:
			if err != nil && !isContextCanceled(err) {
				log.Printf("manager exited with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			log.Printf("manager did not exit within 5s of cancel")
		}
	}()

	if err := waitForWebhookReady(wopts.LocalServingHost, wopts.LocalServingPort, webhookReadyTimeout); err != nil {
		return fmt.Errorf("webhook never became ready: %w", err)
	}

	kubeconfigPath, err := writeKubeconfig(cfg)
	if err != nil {
		return fmt.Errorf("write kubeconfig: %w", err)
	}
	defer func() { _ = os.Remove(kubeconfigPath) }()

	kubectl, err := findKubectl()
	if err != nil {
		return fmt.Errorf("locate kubectl: %w", err)
	}

	var okCount, skipCount, failCount int
	for _, f := range samples {
		rel := mustRel(rootDir, f)
		data, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", rel, err)
		}
		skip, err := hasSkipMarker(data)
		if err != nil {
			return fmt.Errorf("scan %s: %w", rel, err)
		}
		if skip {
			fmt.Printf("  SKIP %s (opt-out: %q)\n", rel, skipMarker)
			skipCount++
			continue
		}
		// Bound each apply so a hung kubectl / apiserver / webhook fails
		// the offending file with a clear diagnostic instead of stalling
		// the whole gate.
		applyCtx, applyCancel := context.WithTimeout(ctx, perSampleTimeout)
		out, runErr := runKubectl(applyCtx, kubectl, kubeconfigPath, f)
		applyCancel()
		if runErr != nil {
			fmt.Printf("  FAIL %s\n", rel)
			if applyCtx.Err() == context.DeadlineExceeded {
				fmt.Printf("    apply exceeded per-sample timeout (%s)\n", perSampleTimeout)
			}
			fmt.Print(indent(out, "    "))
			failCount++
			continue
		}
		fmt.Printf("  OK   %s\n", rel)
		okCount++
	}

	fmt.Printf("\nverify-samples: %d ok, %d skipped, %d failed\n", okCount, skipCount, failCount)
	if failCount > 0 {
		return fmt.Errorf("%d sample(s) rejected by admission — see FAIL lines above", failCount)
	}
	return nil
}

// listSamples returns every *.yaml / *.yml file under dir, sorted, so the
// gate's output is deterministic across runs and CI environments.
func listSamples(dir string) ([]string, error) {
	var out []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

// hasSkipMarker scans the top-of-file comment block for the opt-out marker.
// Only leading blank and `#`-prefixed lines are inspected — once any non-
// comment line appears (typically `apiVersion:`), scanning stops. The
// marker must match exactly so authors can't accidentally trigger it.
//
// Returns a parse error on bufio.Scanner failures (e.g. a leading line
// larger than the default 64KB scanner buffer). We intentionally surface
// these instead of silently treating the file as "not skipped" — a
// scanner error means we couldn't decide, and the gate should fail
// loudly rather than guess.
func hasSkipMarker(data []byte) (bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "#") {
			return false, nil
		}
		if line == skipMarker {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("scan top-of-file comments: %w", err)
	}
	return false, nil
}

// waitForWebhookReady polls the manager's serving port with a plain TCP
// dial until the listener accepts a connection. envtest installs the
// webhook configuration before the manager binds the port, so a sample
// apply that races the binding would otherwise see a transient
// connection-refused.
//
// A plain TCP dial is intentional: we only need to know the OS has
// finished bind+listen on the port. The full TLS handshake is exercised
// by the apiserver when it routes the subsequent CREATE through the
// webhook — that's where a misconfigured cert would surface, with a
// proper error attributable to a specific sample.
func waitForWebhookReady(host string, port int, deadline time.Duration) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	end := time.Now().Add(deadline)
	dialer := &net.Dialer{Timeout: 500 * time.Millisecond}
	for time.Now().Before(end) {
		conn, err := dialer.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("not ready at %s after %s", addr, deadline)
}

// writeKubeconfig emits a kubeconfig pointing at the envtest apiserver,
// embedding the auto-generated client cert + CA. It returns the path to
// the temp file (caller removes).
func writeKubeconfig(cfg *rest.Config) (string, error) {
	api := clientcmdapi.NewConfig()
	api.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   cfg.Host,
		CertificateAuthorityData: cfg.CAData,
	}
	api.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
	}
	api.Contexts["envtest"] = &clientcmdapi.Context{
		Cluster:   "envtest",
		AuthInfo:  "envtest",
		Namespace: "default",
	}
	api.CurrentContext = "envtest"
	f, err := os.CreateTemp("", "verify-samples-kubeconfig-*.yaml")
	if err != nil {
		return "", err
	}
	_ = f.Close()
	if err := clientcmd.WriteToFile(*api, f.Name()); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// findKubectl returns the kubectl shipped in the envtest assets directory.
// We deliberately do NOT fall back to $PATH: a kubectl shipped with a
// different client version could parse / serialize manifests differently
// from the one envtest provisions alongside the apiserver, which would
// make the gate's verdict dependent on the developer's local toolchain.
// Tying kubectl to KUBEBUILDER_ASSETS keeps the gate hermetic.
func findKubectl() (string, error) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		return "", fmt.Errorf("KUBEBUILDER_ASSETS unset")
	}
	cand := filepath.Join(assets, "kubectl")
	if _, err := os.Stat(cand); err != nil {
		return "", fmt.Errorf("kubectl not found at %s (envtest assets dir is missing the binary): %w", cand, err)
	}
	return cand, nil
}

// runKubectl invokes `kubectl --kubeconfig=… apply --dry-run=server -f file`
// and returns its combined stdout/stderr plus the exit error. Any non-zero
// exit (admission rejection, parse error, schema violation) shows up as
// err != nil.
func runKubectl(ctx context.Context, kubectl, kubeconfig, file string) (string, error) {
	cmd := exec.CommandContext(ctx, kubectl,
		"--kubeconfig", kubeconfig,
		"apply", "--dry-run=server", "-f", file,
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// indent prefixes every line of s with prefix, ensuring the result ends in
// a single newline. Used to nest kubectl's error output under the file's
// FAIL marker so the per-sample block stays scannable.
func indent(s, prefix string) string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return ""
	}
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		lines[i] = prefix + ln
	}
	return strings.Join(lines, "\n") + "\n"
}

// repoRoot walks up from cwd looking for go.mod. The gate is invoked via
// `make verify-samples` which sets cwd to the repo root, so the walk
// usually terminates immediately — but the loop keeps it robust against
// `go run ./hack/verify-samples` from a subdirectory.
func repoRoot() (string, error) {
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
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

func mustRel(base, p string) string {
	if r, err := filepath.Rel(base, p); err == nil {
		return r
	}
	return p
}

// isContextCanceled lets the manager-exit log treat clean shutdown as
// non-error noise. Mirrors the helper used in the existing envtest
// webhook integration test.
func isContextCanceled(err error) bool {
	if err == nil {
		return true
	}
	return strings.Contains(err.Error(), "context canceled") ||
		strings.Contains(err.Error(), "context deadline exceeded")
}
