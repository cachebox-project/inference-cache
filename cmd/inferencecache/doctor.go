package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	grpccreds "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/cli/doctor/checks"
	"github.com/cachebox-project/inference-cache/pkg/cli/doctor/output"
)

// Server-discovery defaults. The Service name and system namespace match the
// shipped install manifests (config/server, config/default); the ports match
// the server's gRPC (:9090) and internal snapshot/policy (:8081) listeners.
const (
	defaultServerService   = "inference-cache-server"
	defaultSystemNamespace = "inference-cache-system"
	defaultGRPCPort        = "9090"
	defaultSnapshotPort    = "8081"

	// inClusterTokenPath is the projected ServiceAccount token doctor presents
	// to /snapshot when run inside the cluster.
	inClusterTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec // well-known path, not a credential
)

type doctorOptions struct {
	kubeconfig     string
	kubeContext    string
	namespace      string
	serverEndpoint string
	tokenFile      string
	outputFormat   string
	noColor        bool
	configOnly     bool
	timeout        time.Duration
}

func newDoctorCommand(code *int) *cobra.Command {
	opts := &doctorOptions{
		outputFormat: string(output.FormatHuman),
		tokenFile:    inClusterTokenPath,
		timeout:      30 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Pre-flight diagnostic for an inference-cache installation",
		Long: `doctor runs a read-only series of checks against an inference-cache
installation and reports OK / WARN / FAIL findings with stable, greppable codes.

It checks the cache-plane server's gRPC health and its /snapshot + /policy
endpoints, then every CacheBackend's readiness, engine-pod matching, index
participation and endpoint reachability, engine-pod injection, orphaned engine
pods, CacheTenant quota status, and CachePolicy coverage.

Exit code: 0 when nothing is worse than INFO, 1 on any WARN, 2 on any FAIL —
suitable for CI gating. Pass --config-only to validate cluster configuration
without probing the live server endpoints.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd.Context(), opts, code)
		},
	}

	f := cmd.Flags()
	f.StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to the kubeconfig file (defaults to KUBECONFIG / ~/.kube/config / in-cluster).")
	f.StringVar(&opts.kubeContext, "context", "", "Name of the kubeconfig context to use (defaults to the current context).")
	f.StringVarP(&opts.namespace, "namespace", "n", "", "Namespace to scope the checks to (default: all namespaces).")
	f.StringVar(&opts.serverEndpoint, "server-endpoint", "", "Override the cache server host[:gRPCport] (default: discover the inference-cache-server Service). HTTP probes use the same host on port "+defaultSnapshotPort+".")
	f.StringVar(&opts.tokenFile, "snapshot-token-file", opts.tokenFile, "Path to a ServiceAccount bearer token presented to /snapshot. Missing file => unauthenticated probe (flagged in output).")
	f.StringVarP(&opts.outputFormat, "output", "o", opts.outputFormat, "Output format: human, json, or table.")
	f.BoolVar(&opts.noColor, "no-color", false, "Disable ANSI color in human output (color is auto-disabled when stdout is not a TTY).")
	f.BoolVar(&opts.configOnly, "config-only", false, "Skip the live server endpoint probes (checks 1-3); run only the cluster-configuration checks.")
	f.DurationVar(&opts.timeout, "timeout", opts.timeout, "Overall timeout for the diagnostic run.")
	return cmd
}

func runDoctor(ctx context.Context, opts *doctorOptions, code *int) error {
	format, err := output.ParseFormat(opts.outputFormat)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()

	restCfg, err := restConfig(opts.kubeconfig, opts.kubeContext)
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}
	k8s, err := client.New(restCfg, client.Options{Scheme: scheme()})
	if err != nil {
		return fmt.Errorf("build Kubernetes client: %w", err)
	}

	deps := checks.Deps{
		K8s:                k8s,
		Namespace:          opts.namespace,
		SkipEndpointChecks: opts.configOnly,
	}

	if !opts.configOnly {
		// CacheBackend endpoint TCP reachability is independent of finding the
		// cache server, so wire the dialer regardless of server discovery.
		deps.DialTCP = dialTCP

		grpcTarget, snapshotURL, policyURL, probeURL, err := resolveEndpoints(ctx, k8s, opts.serverEndpoint)
		if err != nil {
			// Don't abort: the cluster-configuration checks still provide value,
			// and leaving the endpoint deps nil makes checks 1-3 emit structured
			// FAIL findings (exit 2) — consistent with a server that is down,
			// rather than a bare error on a separate channel.
			fmt.Fprintf(os.Stderr, "warning: %v; skipping live server endpoint probes\n", err)
		} else {
			conn, err := grpccreds.NewClient(grpcTarget, grpccreds.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("dial server gRPC %q: %w", grpcTarget, err)
			}
			defer conn.Close()

			deps.Health = healthpb.NewHealthClient(conn)
			deps.ServerTarget = grpcTarget
			deps.HTTP = &http.Client{Timeout: opts.timeout}
			deps.SnapshotURL = snapshotURL
			deps.PolicyURL = policyURL
			deps.ProbeURL = probeURL
			deps.Token = readToken(opts.tokenFile)
		}
	}

	report := checks.Run(ctx, deps)

	color := format == output.FormatHuman && !opts.noColor && term.IsTerminal(int(os.Stdout.Fd()))
	if err := output.Render(os.Stdout, report, format, color); err != nil {
		return fmt.Errorf("render output: %w", err)
	}

	*code = report.ExitCode()
	return nil
}

// scheme builds the client scheme: core types (Pods, Events, Services) plus the
// inference-cache CRDs doctor reads.
func scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = cachev1alpha1.AddToScheme(s)
	return s
}

// restConfig honors --kubeconfig and --context using the same client-go loading
// rules kubectl uses (KUBECONFIG / ~/.kube/config), and falls back to the
// in-cluster ServiceAccount config when no kubeconfig is resolvable — so doctor
// works both from a workstation and when run as a pod. The in-cluster fallback
// is taken only when the operator did NOT explicitly pass --kubeconfig/--context
// (an explicit path that fails to load is a real error worth surfacing).
func restConfig(kubeconfig, kubeContext string) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if kubeContext != "" {
		overrides.CurrentContext = kubeContext
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err == nil {
		return cfg, nil
	}
	if kubeconfig == "" && kubeContext == "" {
		if inCluster, icErr := rest.InClusterConfig(); icErr == nil {
			return inCluster, nil
		}
	}
	return nil, err
}

// resolveEndpoints returns the gRPC target and the /snapshot + /policy URLs,
// either from an explicit --server-endpoint override or by discovering the
// inference-cache-server Service. The discovered host is the in-cluster Service
// DNS name (resolvable when doctor runs in-cluster); from a workstation, pass
// --server-endpoint pointing at a kubectl port-forward.
//
// Discovery is independent of the --namespace flag: --namespace scopes which
// CacheBackends/Tenants/Policies doctor inspects, not where the server lives.
// The server is found in the default system namespace, or by a cluster-wide
// search, so `doctor -n app-ns` still probes the real server rather than
// inference-cache-server.app-ns.svc.
func resolveEndpoints(ctx context.Context, c client.Client, override string) (grpcTarget, snapshotURL, policyURL, probeURL string, err error) {
	var host string
	grpcPort := defaultGRPCPort
	if override != "" {
		if h, p, splitErr := net.SplitHostPort(override); splitErr == nil {
			host, grpcPort = h, p
		} else {
			host = override
		}
	} else {
		ns, findErr := findServerServiceNamespace(ctx, c)
		if findErr != nil {
			return "", "", "", "", findErr
		}
		host = fmt.Sprintf("%s.%s.svc", defaultServerService, ns)
	}
	grpcTarget = net.JoinHostPort(host, grpcPort)
	base := "http://" + net.JoinHostPort(host, defaultSnapshotPort)
	return grpcTarget, base + "/snapshot", base + "/policy", base + "/probe", nil
}

// findServerServiceNamespace locates the namespace hosting the
// inference-cache-server Service: the default system namespace if the Service is
// there, else a cluster-wide search.
func findServerServiceNamespace(ctx context.Context, c client.Client) (string, error) {
	var svc corev1.Service
	if err := c.Get(ctx, client.ObjectKey{Namespace: defaultSystemNamespace, Name: defaultServerService}, &svc); err == nil {
		return defaultSystemNamespace, nil
	}
	var svcs corev1.ServiceList
	if err := c.List(ctx, &svcs); err != nil {
		return "", fmt.Errorf("list Services to discover %q: %w", defaultServerService, err)
	}
	for i := range svcs.Items {
		if svcs.Items[i].Name == defaultServerService {
			return svcs.Items[i].Namespace, nil
		}
	}
	return "", fmt.Errorf("could not find a %q Service in any namespace; pass --server-endpoint", defaultServerService)
}

// readToken returns the trimmed bearer token at path. A missing file is the
// expected workstation case and silently yields "" (doctor then probes the
// unauthenticated path). A file that EXISTS but cannot be read (e.g. a
// permission error) is surfaced as a warning rather than masquerading as an
// unauthenticated probe, and the token is whitespace-trimmed so a trailing
// newline in a hand-created file never corrupts the Authorization header.
func readToken(path string) string {
	if path == "" {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "warning: could not read --snapshot-token-file %q: %v; probing /snapshot unauthenticated\n", path, err)
		}
		return ""
	}
	return strings.TrimSpace(string(b))
}

// dialTCP reports whether a TCP connection to addr succeeds. addr may carry an
// lm:// scheme (LMCache endpoints) which is stripped before dialing.
func dialTCP(ctx context.Context, addr string) error {
	addr = stripScheme(addr)
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return conn.Close()
}

func stripScheme(addr string) string {
	for _, scheme := range []string{"lm://", "http://", "https://"} {
		if strings.HasPrefix(addr, scheme) {
			return strings.TrimPrefix(addr, scheme)
		}
	}
	return addr
}
