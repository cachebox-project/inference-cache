package main

import (
	"crypto/tls"
	"flag"
	"os"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/controller"
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
	metricsAddr          string
	secureMetrics        bool
	enableHTTP2          bool
	enableLeaderElection bool
	probeAddr            string
	zapOpts              zap.Options
}

func defaultOptions() options {
	return options{
		metricsAddr:   ":8080",
		probeAddr:     ":8081",
		secureMetrics: false,
		enableHTTP2:   false,
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

	if err := (&controller.CacheBackendReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Log:      ctrl.Log.WithName("controllers").WithName("CacheBackend"),
		Recorder: mgr.GetEventRecorder("cachebackend-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CacheBackend")
		os.Exit(1)
	}

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
