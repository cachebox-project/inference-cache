// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
// typed CacheBackend with type and optional parents omitted must produce a
// fully-defaulted CR with the current defaults stamped — Type=LMCache,
// Integration.Role=ReadWrite, Integration.Mode=Offload,
// Integration.FailOpen=true, and Observation.FirstEventTimeout=5m. The apiserver in the loop applies
// `+kubebuilder:default=` markers; the webhook materialises
// spec.integration and spec.observation so their nested defaults persist.
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
	// Inject the same complete runtime set as the production composition root.
	if err := SetupCacheBackendWebhookWithManager(mgr, defaultShippingRegistry()); err != nil {
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

	// --- Minimum typed MP CR with optional/defaulted fields omitted ---
	//
	// An apply with no Type, Integration, Observation, or remoteStorage. The
	// required typed PodLocal shape remains explicit; optional defaults are stamped
	// by the apiserver (kubebuilder-marker defaults) + the defaulter webhook
	// (cluster-context defaults).
	mvCR := validPodLocalMPBackend()
	mvCR.Name = "minimum"
	mvCR.Namespace = "team-a"
	mvCR.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "minimum"}
	mvCR.Spec.Type = ""
	mvCR.Spec.Integration = nil
	mvCR.Spec.Observation = nil
	if err := k8s.Create(ctx, mvCR); err != nil {
		t.Fatalf("minimum-viable CacheBackend should be admitted: %v", err)
	}

	// Read back the persisted CR — what the apiserver actually stored
	// after every default layer (CRD-schema markers + the webhook) ran.
	var got cachev1alpha1.CacheBackend
	if err := live.Get(ctx, client.ObjectKey{Name: "minimum", Namespace: "team-a"}, &got); err != nil {
		t.Fatalf("get back persisted CR: %v", err)
	}

	// --- Current default surface assertions ---
	//
	// Each assertion below pins one item from the default sweep. If a future
	// change drops a default marker or rewrites the defaulter, the
	// corresponding line fails — surfacing the operator-UX regression at PR
	// time instead of in a confused operator's `kubectl get -o yaml`.

	if want := cachev1alpha1.CacheBackendTypeLMCache; got.Spec.Type != want {
		t.Errorf("spec.type = %q, want %q (kubebuilder default)", got.Spec.Type, want)
	}
	if got.Spec.Integration == nil {
		t.Fatalf("spec.integration was not materialised by the defaulter; got nil")
	}
	if want := cachev1alpha1.CacheBackendIntegrationRoleReadWrite; got.Spec.Integration.Role != want {
		t.Errorf("spec.integration.role = %q, want %q (kubebuilder default)", got.Spec.Integration.Role, want)
	}
	if want := cachev1alpha1.CacheBackendIntegrationModeOffload; got.Spec.Integration.Mode != want {
		t.Errorf("spec.integration.mode = %q, want %q (kubebuilder default)", got.Spec.Integration.Mode, want)
	}
	if got.Spec.Integration.FailOpen == nil || !*got.Spec.Integration.FailOpen {
		t.Errorf("spec.integration.failOpen = %v, want true (kubebuilder default)", got.Spec.Integration.FailOpen)
	}
	if got.Spec.Observation == nil || got.Spec.Observation.FirstEventTimeout == nil ||
		got.Spec.Observation.FirstEventTimeout.Duration != defaultFirstEventTimeout {
		t.Errorf("spec.observation.firstEventTimeout = %v, want %s (defaulter-stamped)",
			got.Spec.Observation, defaultFirstEventTimeout)
	}

	// --- Canonical resources do not inherit legacy provider configuration ---
	//
	// A canonical SGLang + LMCache hierarchy without remoteStorage is
	// engine-local. The apiserver and webhook must preserve the absence of
	// deprecated top-level resources rather than claiming a provider workload
	// this resource did not request.
	canonicalCR := validPodLocalMPBackend()
	canonicalCR.Name = "canonical-host-only"
	canonicalCR.Namespace = "team-a"
	canonicalCR.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "canonical"}
	canonicalCR.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	canonicalCR.Spec.RemoteStorage = nil
	if err := k8s.Create(ctx, canonicalCR); err != nil {
		t.Fatalf("canonical host-only CacheBackend should be admitted: %v", err)
	}

	var canonical cachev1alpha1.CacheBackend
	if err := live.Get(ctx, client.ObjectKey{Name: "canonical-host-only", Namespace: "team-a"}, &canonical); err != nil {
		t.Fatalf("get back canonical host-only CR: %v", err)
	}
	if canonical.Spec.RemoteStorage != nil {
		t.Errorf("canonical spec.remoteStorage = %+v, want nil host-only hierarchy", canonical.Spec.RemoteStorage)
	}
	if canonical.Spec.Integration == nil {
		t.Errorf("canonical integration = nil, want materialised defaults parent")
	}

	// --- Non-clobber pin: an explicit CR overrides every default ---
	//
	// Same shape but with an operator-pinned Type. It must survive every
	// default layer, proving
	// the "defaulter never clobbers" contract holds for the new markers
	// just as it did for the webhook-stamped defaults before the marker
	// sweep landed.
	explicitCR := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "explicit", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeSGLang,
			Type:    cachev1alpha1.CacheBackendTypeSGLangHiCache,
			HiCache: &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{cachev1alpha1.CacheBackendDomainLabel: "explicit"},
			},
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
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
	if explicit.Spec.Type != cachev1alpha1.CacheBackendTypeSGLangHiCache {
		t.Errorf("operator type clobbered: got %q, want SGLangHiCache", explicit.Spec.Type)
	}

	// --- Current MP API CREATE/UPDATE compatibility ---
	//
	// The real apiserver must accept both typed MP topologies and enforce the
	// NodeLocal host/scheduling contract on CREATE and UPDATE.
	mpCR := validPodLocalMPBackend()
	mpCR.Name = "podlocal-mp"
	mpCR.Namespace = "team-a"
	mpCR.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "podlocal"}
	if err := k8s.Create(ctx, mpCR); err != nil {
		t.Fatalf("PodLocal MP CacheBackend should be admitted: %v", err)
	}
	var persistedMP cachev1alpha1.CacheBackend
	if err := live.Get(ctx, client.ObjectKey{Name: mpCR.Name, Namespace: mpCR.Namespace}, &persistedMP); err != nil {
		t.Fatalf("get back PodLocal MP CR: %v", err)
	}
	if persistedMP.Spec.LMCache == nil || persistedMP.Spec.LMCache.Topology != cachev1alpha1.LMCacheTopologyPodLocal ||
		persistedMP.Spec.LMCache.PodLocal == nil || persistedMP.Spec.LMCache.PodLocal.Server == nil {
		t.Fatalf("persisted PodLocal MP shape was lost: %+v", persistedMP.Spec.LMCache)
	}
	if persistedMP.Labels == nil {
		persistedMP.Labels = map[string]string{}
	}
	persistedMP.Labels["phase"] = "one"
	if err := k8s.Update(ctx, &persistedMP); err != nil {
		t.Fatalf("unrelated update on PodLocal MP object should be admitted: %v", err)
	}
	toNodeLocal := func(cb *cachev1alpha1.CacheBackend) {
		cb.Spec.LMCache.Topology = cachev1alpha1.LMCacheTopologyNodeLocal
		cb.Spec.LMCache.PodLocal = nil
		cb.Spec.LMCache.NodeLocal = &cachev1alpha1.LMCacheNodeLocalSpec{
			Server: &cachev1alpha1.LMCacheNodeLocalServerSpec{
				Image: "registry.example/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Port:  6555, HTTPPort: 18080, L1Capacity: resource.MustParse("4Gi"), MaxGPUWorkers: 4, MaxCPUWorkers: 4,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("5Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("5Gi")},
				},
			},
			Scheduling: &cachev1alpha1.LMCacheNodeLocalSchedulingSpec{},
		}
	}
	nodeLocalCreate := validPodLocalMPBackend()
	nodeLocalCreate.Name = "nodelocal-create"
	nodeLocalCreate.Namespace = "team-a"
	nodeLocalCreate.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "nodelocal"}
	toNodeLocal(nodeLocalCreate)
	if err := k8s.Create(ctx, nodeLocalCreate); err != nil {
		t.Fatalf("valid NodeLocal CREATE should be admitted: %v", err)
	}
	var persistedNodeLocal cachev1alpha1.CacheBackend
	if err := live.Get(ctx, client.ObjectKeyFromObject(nodeLocalCreate), &persistedNodeLocal); err != nil {
		t.Fatalf("get back NodeLocal CR: %v", err)
	}
	persistedNodeLocal.Spec.LMCache.NodeLocal.IdleRetentionSeconds++
	if err := k8s.Update(ctx, &persistedNodeLocal); err != nil {
		t.Fatalf("NodeLocal operational setting should remain mutable: %v", err)
	}
	persistedNodeLocal.Spec.LMCache.NodeLocal.Server.HTTPPort = persistedNodeLocal.Spec.LMCache.NodeLocal.Server.Port
	if err := k8s.Update(ctx, &persistedNodeLocal); err == nil {
		t.Fatal("NodeLocal UPDATE with colliding host ports should be rejected")
	}

	// A second CacheBackend cannot own the same namespace-scoped cache domain.
	overlap := validPodLocalMPBackend()
	overlap.Name = "overlapping-selector"
	overlap.Namespace = "team-a"
	overlap.Spec.EngineSelector.MatchLabels = map[string]string{cachev1alpha1.CacheBackendDomainLabel: "nodelocal"}
	if err := k8s.Create(ctx, overlap); err == nil {
		t.Fatal("CacheBackend CREATE with overlapping engineSelector should be rejected")
	}

	// Ownership is intentionally independent of ordinary Pod labels: the
	// canonical selector contains only cache-domain, even when those Pods carry
	// app/model/environment labels for other Kubernetes consumers.
	extraSelector := validPodLocalMPBackend()
	extraSelector.Name = "extra-selector-label"
	extraSelector.Namespace = "team-a"
	extraSelector.Spec.EngineSelector.MatchLabels = map[string]string{
		cachev1alpha1.CacheBackendDomainLabel: "extra-selector-label",
		"app":                                 "vllm",
	}
	if err := k8s.Create(ctx, extraSelector); err == nil {
		t.Fatal("CacheBackend CREATE with a second ownership selector label should be rejected")
	}
}
