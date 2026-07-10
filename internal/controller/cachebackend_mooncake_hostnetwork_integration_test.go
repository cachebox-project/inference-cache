package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// TestIntegrationMooncakeHostNetworkAndHeadlessService exercises the Mooncake data
// plane's provisioning contract against a REAL apiserver. Two behaviors here cannot
// be covered by the fake client, which skips allocation and validation:
//
//  1. clusterIP allocation — the apiserver assigns a virtual IP unless the Service
//     explicitly asks for headless, so "did None actually survive the apply?" is
//     only a real question against envtest. The renderer can be perfect while
//     applyService silently drops the field.
//  2. clusterIP immutability — an in-place headless migration is rejected by the
//     apiserver. That rejection is precisely what applyService must sidestep by
//     recreating the Service instead of updating it.
//
// If either regresses, a Mooncake backend reconciles Ready and transfers zero KV.
func TestIntegrationMooncakeHostNetworkAndHeadlessService(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	t.Run("MooncakeRendersHostNetworkPodAndHeadlessService", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, mooncakeBackend("cache", ns)); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		dep := getDeployment(t, r, "cache", ns)
		if !dep.Spec.Template.Spec.HostNetwork {
			t.Fatal("master pod is not hostNetwork; mooncake's transfer engine cannot use overlay pod IPs")
		}
		if got := dep.Spec.Template.Spec.DNSPolicy; got != corev1.DNSClusterFirstWithHostNet {
			t.Fatalf("dnsPolicy = %q, want %q (hostNetwork must keep cluster DNS)",
				got, corev1.DNSClusterFirstWithHostNet)
		}
		if got := dep.Spec.Strategy.Type; got != appsv1.RecreateDeploymentStrategyType {
			t.Fatalf("strategy = %q, want %q (a rolling surge collides on the node's ports)",
				got, appsv1.RecreateDeploymentStrategyType)
		}

		// The apiserver would have allocated a virtual IP had the adapter not asked
		// for headless — an assertion that is meaningless against a fake client.
		svc := getService(t, r, "cache", ns)
		if svc.Spec.ClusterIP != corev1.ClusterIPNone {
			t.Fatalf("svc.Spec.ClusterIP = %q, want %q — a virtual IP forwards only the declared ports and strands mooncake's dynamic ones",
				svc.Spec.ClusterIP, corev1.ClusterIPNone)
		}
	})

	t.Run("LMCacheKeepsPodNetworkAndVirtualClusterIP", func(t *testing.T) {
		// Blast radius: the portable, non-privileged default must not move.
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, lmcacheBackend("cache", ns)); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		if dep := getDeployment(t, r, "cache", ns); dep.Spec.Template.Spec.HostNetwork {
			t.Fatal("lmcache server became hostNetwork; it must stay on the pod network")
		}
		svc := getService(t, r, "cache", ns)
		if svc.Spec.ClusterIP == corev1.ClusterIPNone || svc.Spec.ClusterIP == "" {
			t.Fatalf("lmcache svc.Spec.ClusterIP = %q, want an apiserver-allocated virtual IP", svc.Spec.ClusterIP)
		}
	})

	t.Run("MigratesExistingDeploymentOntoHostNetworkAndRecreate", func(t *testing.T) {
		// applyDeployment overwrites the whole Spec only on CREATE; on UPDATE it
		// reconciles a hand-picked subset. A Mooncake backend provisioned before this
		// fix therefore owns an overlay Deployment with the API-server-defaulted
		// RollingUpdate strategy, and both must migrate — otherwise the upgrade is a
		// silent no-op: the master stays unreachable for the mesh, and a rolling
		// surge would collide on the node's ports.
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, mooncakeBackend("cache", ns)); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		// Rewind the live object to its pre-fix shape.
		dep := getDeployment(t, r, "cache", ns)
		dep.Spec.Template.Spec.HostNetwork = false
		dep.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
		dep.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType}
		if err := k8s.Update(ctx, dep); err != nil {
			t.Fatalf("simulate pre-fix Deployment: %v", err)
		}
		// Precondition: the apiserver populates a rollingUpdate block, which is
		// exactly what would reject a naive .Type-only flip to Recreate.
		if pre := getDeployment(t, r, "cache", ns); pre.Spec.Strategy.RollingUpdate == nil {
			t.Fatal("precondition: expected an apiserver-populated rollingUpdate block")
		}

		reconcile(t, r, "cache", ns)

		got := getDeployment(t, r, "cache", ns)
		if !got.Spec.Template.Spec.HostNetwork {
			t.Fatal("existing Deployment did not migrate onto hostNetwork; the fix would be a no-op on upgrade")
		}
		if want := corev1.DNSClusterFirstWithHostNet; got.Spec.Template.Spec.DNSPolicy != want {
			t.Fatalf("dnsPolicy = %q, want %q after migration", got.Spec.Template.Spec.DNSPolicy, want)
		}
		if got.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
			t.Fatalf("strategy = %q, want %q after migration", got.Spec.Strategy.Type, appsv1.RecreateDeploymentStrategyType)
		}
		if got.Spec.Strategy.RollingUpdate != nil {
			t.Fatal("stale rollingUpdate block not cleared; the apiserver rejects it alongside Recreate")
		}
	})

	t.Run("RecreatesServiceStuckOnAnImmutableVirtualClusterIP", func(t *testing.T) {
		// A Mooncake backend provisioned before this fix owns a Service carrying an
		// allocated virtual IP. clusterIP is immutable, so an in-place update can
		// never make it headless: applyService must delete it and let the next
		// reconcile recreate it. Without this the upgrade is a no-op and the backend
		// stays Ready while transferring nothing.
		ns := freshNS(t, k8s)
		cb := mooncakeBackend("cache", ns)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		if err := k8s.Get(ctx, types.NamespacedName{Name: "cache", Namespace: ns}, cb); err != nil {
			t.Fatalf("get CacheBackend (for owner ref UID): %v", err)
		}

		stale := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: ns},
			Spec: corev1.ServiceSpec{
				// No ClusterIP set -> the apiserver allocates a virtual IP.
				Type:     corev1.ServiceTypeClusterIP,
				Selector: selectorLabels("cache"),
				Ports:    []corev1.ServicePort{{Name: "rpc", Port: 50051, Protocol: corev1.ProtocolTCP}},
			},
		}
		if err := controllerutil.SetControllerReference(cb, stale, scheme); err != nil {
			t.Fatalf("set controller reference: %v", err)
		}
		if err := k8s.Create(ctx, stale); err != nil {
			t.Fatalf("create pre-fix service: %v", err)
		}
		if stale.Spec.ClusterIP == corev1.ClusterIPNone || stale.Spec.ClusterIP == "" {
			t.Fatalf("precondition: pre-fix Service should carry an allocated virtual IP, got %q", stale.Spec.ClusterIP)
		}

		// First pass must delete the divergent Service rather than fail forever on
		// the immutable field.
		reconcile(t, r, "cache", ns)
		var gone corev1.Service
		switch err := k8s.Get(ctx, types.NamespacedName{Name: "cache", Namespace: ns}, &gone); {
		case err == nil:
			t.Fatalf("pre-fix Service still present with clusterIP %q; applyService must delete it", gone.Spec.ClusterIP)
		case !apierrors.IsNotFound(err):
			t.Fatalf("get service: %v", err)
		}

		// The next pass recreates it headless.
		reconcile(t, r, "cache", ns)
		svc := getService(t, r, "cache", ns)
		if svc.Spec.ClusterIP != corev1.ClusterIPNone {
			t.Fatalf("recreated svc.Spec.ClusterIP = %q, want %q", svc.Spec.ClusterIP, corev1.ClusterIPNone)
		}
	})
}
