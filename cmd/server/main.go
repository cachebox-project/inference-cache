package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/cachebox-project/inference-cache/pkg/server"
	"github.com/cachebox-project/inference-cache/pkg/server/auth"
	"github.com/cachebox-project/inference-cache/pkg/tokenize"
	"github.com/cachebox-project/inference-cache/pkg/version"
)

func main() {
	cfg := server.DefaultConfig()
	logFormat := flag.String("log-format", string(server.LogFormatJSON), "Log output format (json|text). JSON is the production default; text is for local development.")
	logLevel := flag.String("log-level", "info", "Log level (debug|info|warn|error).")
	flag.StringVar(&cfg.GRPCAddr, "grpc-bind-address", cfg.GRPCAddr, "The address the gRPC server binds to.")
	flag.StringVar(&cfg.HTTPAddr, "http-bind-address", cfg.HTTPAddr, "The address the public HTTP server binds to (serves /healthz, /readyz, /metrics).")
	flag.StringVar(&cfg.SnapshotAddr, "snapshot-bind-address", cfg.SnapshotAddr, "The address the internal controller-facing HTTP server binds to (serves /snapshot, /policy, and /probe, all gated by ServiceAccount bearer auth + audience binding).")
	expectedSA := flag.String("allowed-controller-sa", "", "Fully-qualified ServiceAccount username allowed to call /snapshot, /policy, and /probe, e.g. system:serviceaccount:inference-cache-system:inference-cache-controller-manager. REQUIRED in production. Without it the server refuses to start; passing --insecure-disable-auth is the explicit, named escape hatch for local development.")
	insecureNoAuth := flag.Bool("insecure-disable-auth", false, "Local-development only: serve /snapshot, /policy, and /probe without authentication. The flag is named to make any operator who runs it on a real cluster notice. Mutually exclusive with --allowed-controller-sa.")
	// Long-form rationale lives here rather than in the flag help so
	// `--help` stays scannable: an empty value would silently disable
	// audience binding while the rest of the boot log claims auth is
	// enabled, which is the exact failure mode the startup check below
	// rejects. The same audience gates /snapshot, /policy, and /probe
	// since they share one middleware identity. Must match the audience
	// listed in the controller's projected SA token volume (see
	// config/manager/manager.yaml).
	controllerAudience := flag.String("controller-audience", auth.ControllerAudience, "Audience the apiserver enforces on /snapshot, /policy, and /probe bearer tokens (TokenReviewSpec.Audiences). Must match the controller's projected SA token volume. REQUIRED non-empty when --allowed-controller-sa is set.")
	// gRPC transport posture. Both set → the :9090 policy port
	// terminates Service TLS in-process; both empty → plaintext (the default —
	// config/default serves :9090 plaintext; TLS is the opt-in
	// config/overlays/server-tls overlay); exactly one set → refuse to start.
	// mTLS (client-cert verification) is a Phase 2 feature flag, not
	// implemented here. See docs/design/grpc-tls.md.
	tlsCertFile := flag.String("tls-cert-file", "", "Path to the PEM server certificate for the gRPC port (:9090). Set together with --tls-key-file to enable TLS; leave both empty for plaintext (dev/CI).")
	tlsKeyFile := flag.String("tls-key-file", "", "Path to the PEM private key for the gRPC port (:9090). Set together with --tls-cert-file to enable TLS; leave both empty for plaintext (dev/CI).")
	tokenizerModelsDir := flag.String("tokenizer-models-dir", "", "Directory of vetted per-model tokenizer artifacts (<dir>/<model_id>/tokenizer.json, where <model_id> may contain a namespace, e.g. Qwen/Qwen2.5-0.5B-Instruct) used to tokenize LookupRoute prompt_text server-side. Tokenizers are loaded eagerly at startup and confined to this directory; a request model_id is matched only against the pre-loaded set (never joined onto a path). Empty (the default) disables server-side tokenization — the prompt_text lookup path fails open to NO_HINT. Only effective in the tokenizer-enabled build (-tags smgcgo).")
	engineBlockSize := flag.Int("engine-block-size", server.DefaultEngineBlockSize, "KV block size (tokens per block) used to fingerprint token_ids / tokenized prompt_text on LookupRoute. MUST match the engine's KV block size and the kvevent-subscriber's. vLLM's default is 16.")
	flag.Parse()

	format, err := server.ParseLogFormat(*logFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	level, err := server.ParseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	handler, err := server.NewLogHandler(format, level, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(handler))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Fail closed by default. Validation lives in a separate testable helper
	// so the full matrix (auth on/off, audience set/empty/whitespace, SA
	// whitespace, insecure escape hatch) can be pinned by unit tests
	// instead of relying on integration only.
	if msg := validateControllerAuthFlags(*expectedSA, *controllerAudience, *insecureNoAuth); msg != "" {
		fmt.Fprintln(os.Stderr, msg)
		os.Exit(2)
	}

	// Wire the server-owned tokenizer for the (model, prompt_text) LookupRoute
	// path. tokenize.New returns the real cgo tokenizer only under the smgcgo
	// build; otherwise it is Unavailable (the prompt_text path fails open to
	// NO_HINT), so this is safe to wire unconditionally.
	var opts []server.Option
	opts = append(opts, server.WithTokenizer(tokenize.New(tokenize.Config{ModelsDir: *tokenizerModelsDir})))
	opts = append(opts, server.WithEngineBlockSize(*engineBlockSize))
	if *expectedSA != "" {
		restCfg, err := rest.InClusterConfig()
		if err != nil {
			slog.ErrorContext(ctx, "in_cluster_config", "err", err)
			os.Exit(1)
		}
		clientset, err := kubernetes.NewForConfig(restCfg)
		if err != nil {
			slog.ErrorContext(ctx, "kube_client", "err", err)
			os.Exit(1)
		}
		opts = append(opts, server.WithControllerAuth(auth.FromClientset(clientset), *expectedSA, *controllerAudience))
		slog.InfoContext(ctx, "controller_auth_enabled", "expected_sa", *expectedSA, "audience", *controllerAudience)
	} else {
		// *insecureNoAuth == true, verified above.
		slog.WarnContext(ctx, "controller_auth_disabled",
			"reason", "--insecure-disable-auth was set; /snapshot, /policy, and /probe are unauthenticated. This must NEVER be used in production.")
	}

	// Resolve the gRPC transport posture before serving. LoadGRPCTLSCredentials
	// owns the both-or-neither rule: exactly one of the cert/key paths set is a
	// startup error (exit 2, like the auth-flag validation above); a path pair
	// that won't load is a runtime error (exit 1).
	grpcCreds, err := server.LoadGRPCTLSCredentials(*tlsCertFile, *tlsKeyFile)
	if err != nil {
		if errors.Is(err, server.ErrTLSPartialConfig) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		slog.ErrorContext(ctx, "grpc_tls_load", "err", err)
		os.Exit(1)
	}
	if grpcCreds != nil {
		opts = append(opts, server.WithGRPCTLS(grpcCreds))
	}

	slog.InfoContext(ctx, "startup",
		"version", version.GitVersion,
		"commit", version.GitCommit,
		"grpc_addr", cfg.GRPCAddr,
		"grpc_tls", grpcCreds != nil,
		"http_addr", cfg.HTTPAddr,
		"snapshot_addr", cfg.SnapshotAddr,
	)
	if err := server.ListenAndServe(ctx, cfg, opts...); err != nil {
		// Terminal error — log once here. pkg/server.Serve does NOT log on
		// the errCh branch so we don't double-emit when a listener fails.
		slog.ErrorContext(ctx, "serve_error", "err", err)
		os.Exit(1)
	}
}

// validateControllerAuthFlags pins the operator-facing startup contract for
// the controller-facing auth flags (--allowed-controller-sa,
// --insecure-disable-auth, --controller-audience). Returns the diagnostic
// message the server should print before exiting, or "" if the combination
// is valid. Extracted from main() so the matrix can be unit-tested without
// a subprocess harness.
//
// The contract (/snapshot, /policy, and /probe share this gate since they
// share one middleware identity):
//   - --allowed-controller-sa AND --insecure-disable-auth are mutually
//     exclusive (either you authenticate or you explicitly opted out; not both).
//   - At least one must be set: the previous shape (silent unauth on empty
//     flag) made it trivial for a real-cluster deploy to accidentally ship
//     wide-open controller-facing endpoints, which defeats the hardening.
//   - --allowed-controller-sa, when set, must exactly match its own trimmed
//     form: kube-apiserver returns a username that exactly matches
//     "system:serviceaccount:NS:NAME" (no whitespace), so a pasted SA value
//     with leading/trailing whitespace would never match — every controller
//     scrape AND every controller push would 403, and the operator would
//     chase a logically-correct-but-mistyped flag. Fail fast at startup
//     with the value echoed back.
//   - When auth is on (--allowed-controller-sa set), --controller-audience
//     must be a non-empty string that exactly matches its own trimmed form:
//     any leading/trailing whitespace is rejected here at startup with a
//     fail-fast diagnostic instead of being silently passed to
//     TokenReviewSpec.Audiences (where a stray " " would not match the JWT-
//     baked audience and would silently produce a runtime 401 loop). The
//     fully-empty / whitespace-only case is the same rejection path: there
//     is no legitimate reason to disable audience binding via the CLI (the
//     test-only legacy posture is reachable only via server.WithControllerAuth).
func validateControllerAuthFlags(expectedSA, audience string, insecureNoAuth bool) string {
	switch {
	case expectedSA != "" && insecureNoAuth:
		return "--allowed-controller-sa and --insecure-disable-auth are mutually exclusive"
	case expectedSA == "" && !insecureNoAuth:
		return "missing --allowed-controller-sa; pass --insecure-disable-auth to run /snapshot, /policy, and /probe without authentication (local development only)"
	case expectedSA != "" && strings.TrimSpace(expectedSA) != expectedSA:
		return fmt.Sprintf("--allowed-controller-sa has leading/trailing whitespace (%q); the apiserver returns SA usernames without whitespace, so this value would never match and every controller request would 403", expectedSA)
	case expectedSA != "" && strings.TrimSpace(audience) == "":
		return "--controller-audience cannot be empty or whitespace-only when --allowed-controller-sa is set; an empty/blank audience would silently disable the defense-in-depth gate that pairs with TokenReview.Audiences"
	case expectedSA != "" && strings.TrimSpace(audience) != audience:
		return fmt.Sprintf("--controller-audience has leading/trailing whitespace (%q); the JWT-baked audience will not match this value and the server would produce a runtime 401 loop instead of the fail-fast operators expect", audience)
	}
	return ""
}
