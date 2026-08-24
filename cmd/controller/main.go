// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
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
	builtinadapters "github.com/cachebox-project/inference-cache/internal/adapters/builtin"
	"github.com/cachebox-project/inference-cache/internal/controller"
	"github.com/cachebox-project/inference-cache/internal/version"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	cachewebhookv1alpha1 "github.com/cachebox-project/inference-cache/internal/webhook/v1alpha1"
)

const leaderLockName = "inference-cache-controller-leader-lock"

var sha256ImagePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[a-f0-9]{64}$`)

const zeroSHA256Digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

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
	serverProbeURL          string
	cacheIndexRefreshEvery  time.Duration
	policyPushEvery         time.Duration
	subscriberImage         string
	nodeLocalCleanupImage   string
	policyServerGRPCAddress string
	zapOpts                 zap.Options
}

func defaultOptions() options {
	return options{
		metricsAddr:             ":8080",
		probeAddr:               ":8081",
		secureMetrics:           false,
		enableHTTP2:             false,
		serverSnapshotURL:       "http://inference-cache-server:8081/snapshot",
		serverPolicyURL:         "http://inference-cache-server:8081/policy",
		serverProbeURL:          "http://inference-cache-server:8081/probe",
		cacheIndexRefreshEvery:  controller.DefaultRefreshInterval,
		policyPushEvery:         controller.DefaultPolicyPushInterval,
		subscriberImage:         "",
		nodeLocalCleanupImage:   "",
		policyServerGRPCAddress: "inference-cache-server.inference-cache-system.svc.cluster.local:9090",
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
	flag.StringVar(&opts.serverProbeURL, "server-probe-url", opts.serverProbeURL, "URL of the cache server's /probe endpoint, the controller POSTs per CacheBackend to drive the functional self-test. Empty disables the gate (FunctionalProbeOK condition is not written and the Ready gate is unchanged).")
	flag.DurationVar(&opts.cacheIndexRefreshEvery, "cacheindex-refresh-interval", opts.cacheIndexRefreshEvery, "How often to refresh the CacheIndex status from the server snapshot.")
	flag.DurationVar(&opts.policyPushEvery, "cachepolicy-push-interval", opts.policyPushEvery, "How often to re-push the full CachePolicy snapshot to the server (self-healing on server restart).")
	flag.StringVar(&opts.subscriberImage, "kvevent-subscriber-image", opts.subscriberImage, "Image reference the pod-mutating webhook uses for the kvevent-subscriber sidecar it auto-attaches to managed-LMCache engine pods (vLLM and SGLang). Empty (default) disables auto-attach — the engine pod wiring still happens but no subscriber container is appended. Pin to a digest in production.")
	flag.StringVar(&opts.nodeLocalCleanupImage, "node-local-shm-cleanup-image", opts.nodeLocalCleanupImage, "Digest-pinned inference-cache helper image used to reclaim NodeLocal LMCache SHM pools. Required.")
	flag.StringVar(&opts.policyServerGRPCAddress, "policy-server-grpc-address", opts.policyServerGRPCAddress, "host:port the kvevent-subscriber sidecar dials to ReportCacheState. Defaults to the in-cluster Service DNS in the inference-cache-system namespace.")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}

func main() {
	opts := parseOptions()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.zapOpts)))
	if err := validateOptions(opts); err != nil {
		setupLog.Error(err, "invalid controller configuration")
		os.Exit(1)
	}
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

	// One complete built-in composition shared by the reconciler and both
	// webhooks. Whatever admission accepts is therefore renderable and
	// injectable without each caller remembering extra registrations.
	//
	// The kvevent-subscriber sidecar image and policy-server gRPC address are
	// operator-supplied. Pinning images to compatible digests in production and
	// selecting the right Service DNS are
	// deployment concerns; a CacheBackend may still override its own provider
	// image when needed.
	adapterRegistries := builtinadapters.New(builtinadapters.Options{
		SubscriberImage:         opts.subscriberImage,
		PolicyServerGRPCAddress: opts.policyServerGRPCAddress,
	})
	adapterRegistry := adapterRegistries.Runtime

	// /probe wrapper for the CacheBackend reconciler's functional-probe gate.
	// An empty ProbeURL disables the gate — useful for local-dev runs that
	// don't have a server reachable, and to keep fake-client unit tests from
	// making real HTTP calls. The bearer-token path matches the snapshot
	// poller, while the /policy pusher uses its own write-side projected token.
	probeClient := &controller.ProbeClient{ProbeURL: opts.serverProbeURL}

	if err := (&controller.CacheBackendReconciler{
		Client:                   mgr.GetClient(),
		Scheme:                   mgr.GetScheme(),
		Log:                      ctrl.Log.WithName("controllers").WithName("CacheBackend"),
		Recorder:                 mgr.GetEventRecorder("cachebackend-controller"),
		APIReader:                mgr.GetAPIReader(),
		Registry:                 adapterRegistry,
		BackendRegistry:          adapterRegistries.Storage,
		ProbeClient:              probeClient,
		NodeLocalShmCleanupImage: opts.nodeLocalCleanupImage,
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

	if err := (&controller.ControlPlaneReconciler{
		Client:          mgr.GetClient(),
		Log:             ctrl.Log.WithName("controllers").WithName("ControlPlane"),
		ServerPolicyURL: opts.serverPolicyURL,
		PushInterval:    opts.policyPushEvery,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ControlPlane")
		os.Exit(1)
	}

	// EnginePodEventsReconciler emits the InjectedByCacheBackend Event on
	// each engine pod after the mutating webhook stamps it. The webhook
	// itself can't emit: at admission time the apiserver hasn't assigned
	// pod.metadata.uid, so a webhook-recorded event would land with
	// involvedObject.uid="" and be invisible to `kubectl describe pod`.
	if err := (&controller.EnginePodEventsReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Log:       ctrl.Log.WithName("controllers").WithName("EnginePodEvents"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EnginePodEvents")
		os.Exit(1)
	}

	if err := cachewebhookv1alpha1.SetupCacheBackendWebhookWithManager(mgr, adapterRegistry); err != nil {
		setupLog.Error(err, "unable to register webhook", "webhook", "CacheBackend")
		os.Exit(1)
	}

	// CachePolicy + CacheTenant validating/defaulting webhooks. Both
	// validators read sibling CRs (one-CachePolicy-per-namespace,
	// tenantID-uniqueness) via the manager's live APIReader, which
	// SetupCache*WebhookWithManager wires internally.
	if err := cachewebhookv1alpha1.SetupCachePolicyWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to register webhook", "webhook", "CachePolicy")
		os.Exit(1)
	}
	if err := cachewebhookv1alpha1.SetupCacheTenantWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to register webhook", "webhook", "CacheTenant")
		os.Exit(1)
	}

	// The Pod admission handler uses the manager's APIReader (uncached
	// live client) instead of the cached client: pod CREATE is a
	// one-shot opportunity to inject, so a stale informer view of the
	// owning CacheBackend (in particular remote-storage status that lags
	// reality) would leave the pod permanently unwired. Live reads also
	// avoid a cold-cache window on controller startup.
	mgr.GetWebhookServer().Register(podwebhook.WebhookPath, &webhook.Admission{
		Handler: &podwebhook.EngineInjector{
			Reader:   mgr.GetAPIReader(),
			Registry: adapterRegistry,
			Log:      ctrl.Log.WithName("webhooks").WithName("pod-injector"),
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

func validateOptions(opts options) error {
	if !sha256ImagePattern.MatchString(opts.nodeLocalCleanupImage) || strings.HasSuffix(opts.nodeLocalCleanupImage, zeroSHA256Digest) {
		return fmt.Errorf("--node-local-shm-cleanup-image must be a digest-pinned image reference")
	}
	return nil
}
