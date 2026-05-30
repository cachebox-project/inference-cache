package main

import (
	"crypto/tls"
	"flag"
	"os"
	"time"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/controller"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	cachewebhookv1alpha1 "github.com/cachebox-project/inference-cache/internal/webhook/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/version"
)

const leaderLockName = "inference-cache-controller-leader-lock"

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cachev1alpha1.AddToScheme(scheme))
}

type options struct {
	metricsAddr             string
	secureMetrics           bool
	enableHTTP2             bool
	enableLeaderElection    bool
	probeAddr               string
	serverSnapshotURL       string
	serverPolicyURL         string
	cacheIndexRefreshEvery  time.Duration
	policyPushEvery         time.Duration
	subscriberImage         string
	policyServerGRPCAddress string
	zapOpts                 zap.Options
}

func defaultOptions() options {
	return options{
		metricsAddr:             ":8080",
		probeAddr:               ":8081",
		secureMetrics:           false,
		enableHTTP2:             false,
		serverSnapshotURL:       "http://inference-cache-server:8080/snapshot",
		serverPolicyURL:         "http://inference-cache-server:8080/policy",
		cacheIndexRefreshEvery:  controller.DefaultRefreshInterval,
		policyPushEvery:         controller.DefaultPolicyPushInterval,
		subscriberImage:         "",
		policyServerGRPCAddress: adapterruntime.DefaultPolicyServerGRPCAddress,
		zapOpts: zap.Options{
			TimeEncoder: zapcore.RFC3339TimeEncoder,
		},
	}
}

func parseOptions() options {
	opts := defaultOptions()
	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", opts.metricsAddr, "The address the metric endpoint binds to.")
	flag.BoolVar(&opts.secureMetrics, "metrics-secure", opts.secureMetrics, "Serve metrics over HTTPS.")
	flag.BoolVar(&opts.enableHTTP2, "enable-http2", opts.enableHTTP2, "Enable HTTP/2 for metrics.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", opts.enableLeaderElection, "Enable leader election for controller manager.")
	flag.StringVar(&opts.probeAddr, "health-probe-bind-address", opts.probeAddr, "The address the probe endpoint binds to.")
	flag.StringVar(&opts.serverSnapshotURL, "server-snapshot-url", opts.serverSnapshotURL, "URL of the cache server's /snapshot endpoint, scraped to populate the CacheIndex status.")
	flag.StringVar(&opts.serverPolicyURL, "server-policy-url", opts.serverPolicyURL, "URL of the cache server's /policy endpoint, the controller PUSHES resolved CachePolicy snapshots to.")
	flag.DurationVar(&opts.cacheIndexRefreshEvery, "cacheindex-refresh-interval", opts.cacheIndexRefreshEvery, "How often to refresh the CacheIndex status from the server snapshot.")
	flag.DurationVar(&opts.policyPushEvery, "cachepolicy-push-interval", opts.policyPushEvery, "How often to re-push the full CachePolicy snapshot to the server (self-healing on server restart).")
	flag.StringVar(&opts.subscriberImage, "kvevent-subscriber-image", opts.subscriberImage, "Image reference the pod-mutating webhook uses for the kvevent-subscriber sidecar it auto-attaches to vLLM engine pods. Empty (default) disables auto-attach — the engine pod wiring still happens but no subscriber container is appended. Pin to a digest in production.")
	flag.StringVar(&opts.policyServerGRPCAddress, "policy-server-grpc-address", opts.policyServerGRPCAddress, "host:port the kvevent-subscriber sidecar dials to ReportCacheState. Defaults to the in-cluster Service DNS in the inference-cache-system namespace.")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}

func main() {
	opts := parseOptions()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.zapOpts)))
	setupLog.Info("initializing", "gitVersion", version.GitVersion, "gitCommit", version.GitCommit)

	tlsOpts := []func(*tls.Config){}
	if !opts.enableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			setupLog.Info("disabling http/2")
			c.NextProtos = []string{"http/1.1"}
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   opts.metricsAddr,
			SecureServing: opts.secureMetrics,
			TLSOpts:       tlsOpts,
		},
		HealthProbeBindAddress: opts.probeAddr,
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       leaderLockName,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// One Registry shared by the reconciler, the pod-mutating webhook,
	// and the CacheBackend validating webhook. Building it here keeps the
	// three call sites in agreement: whatever pair the validator admits,
	// the reconciler will be able to render and the pod webhook will be
	// able to inject. Adding a new adapter is a one-line registry
	// change, not three.
	//
	// The kvevent-subscriber sidecar image + policy-server gRPC
	// address are operator-supplied: pinning the image to a digest in
	// production and pointing the sidecar at the right Service DNS are
	// deployment concerns, not CR-level knobs.
	adapterRegistry := adapterruntime.DefaultRegistry(
		adapterruntime.WithSubscriberImage(opts.subscriberImage),
		adapterruntime.WithPolicyServerGRPCAddress(opts.policyServerGRPCAddress),
	)

	if err := (&controller.CacheBackendReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Log:      ctrl.Log.WithName("controllers").WithName("CacheBackend"),
		Recorder: mgr.GetEventRecorder("cachebackend-controller"),
		Registry: adapterRegistry,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CacheBackend")
		os.Exit(1)
	}

	if err := mgr.Add(&controller.CacheIndexPoller{
		Client:      mgr.GetClient(),
		Log:         ctrl.Log.WithName("controllers").WithName("CacheIndex"),
		SnapshotURL: opts.serverSnapshotURL,
		Interval:    opts.cacheIndexRefreshEvery,
	}); err != nil {
		setupLog.Error(err, "unable to add CacheIndex poller")
		os.Exit(1)
	}

	if err := (&controller.CachePolicyReconciler{
		Client:          mgr.GetClient(),
		Log:             ctrl.Log.WithName("controllers").WithName("CachePolicy"),
		ServerPolicyURL: opts.serverPolicyURL,
		PushInterval:    opts.policyPushEvery,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CachePolicy")
		os.Exit(1)
	}

	if err := cachewebhookv1alpha1.SetupCacheBackendWebhookWithManager(mgr, adapterRegistry); err != nil {
		setupLog.Error(err, "unable to register webhook", "webhook", "CacheBackend")
		os.Exit(1)
	}

	// The Pod admission handler uses the manager's APIReader (uncached
	// live client) instead of the cached client: pod CREATE is a
	// one-shot opportunity to inject, so a stale informer view of the
	// owning CacheBackend (in particular a status.endpoint that lags
	// reality) would leave the pod permanently unwired. Live reads also
	// avoid a cold-cache window on controller startup.
	mgr.GetWebhookServer().Register(podwebhook.WebhookPath, &webhook.Admission{
		Handler: &podwebhook.EngineInjector{
			Reader:   mgr.GetAPIReader(),
			Registry: adapterRegistry,
			Log:      ctrl.Log.WithName("webhooks").WithName("pod-injector"),
			Recorder: mgr.GetEventRecorder("cachebackend-pod-webhook"),
		},
	})

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
