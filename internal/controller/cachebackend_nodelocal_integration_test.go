// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	builtinruntime "github.com/cachebox-project/inference-cache/internal/adapters/builtin/runtime"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

// TestIntegrationCacheBackendNodeLocalServerPod exercises the engine-demanded
// child lifecycle against a real apiserver. Envtest has no scheduler or kubelet,
// so same-node health is GPU tested separately; this pins CREATE, exact-node
// affinity, operational UPDATE, and ownership.
func TestIntegrationCacheBackendNodeLocalServerPod(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, APIReader: k8s, Scheme: scheme, Log: logr.Discard()}
	configureTestRegistries(r)
	ctx := context.Background()
	ns := freshNS(t, k8s)
	backend := nodeLocalBackend("node-cache", ns)
	backend.UID = ""
	if err := k8s.Create(ctx, backend); err != nil {
		t.Fatalf("create NodeLocal CacheBackend: %v", err)
	}
	key := client.ObjectKey{Name: backend.Name, Namespace: ns}
	var live cachev1alpha1.CacheBackend
	if err := k8s.Get(ctx, key, &live); err != nil {
		t.Fatalf("get NodeLocal CacheBackend: %v", err)
	}
	engine := nodeLocalEngine(&live, "engine-a", "node-a")
	engine.Spec.Containers[0].Image = "example.invalid/engine:test"
	if err := k8s.Create(ctx, engine); err != nil {
		t.Fatalf("create scheduled engine: %v", err)
	}
	reconcile(t, r, backend.Name, ns)

	serverKey := client.ObjectKey{Name: builtinruntime.NodeLocalServerPodName(backend.Name, "node-a"), Namespace: ns}
	var server corev1.Pod
	if err := k8s.Get(ctx, serverKey, &server); err != nil {
		t.Fatalf("get NodeLocal server Pod: %v", err)
	}
	if !server.Spec.HostNetwork || server.Spec.HostIPC || server.Spec.DNSPolicy != "ClusterFirstWithHostNet" || server.Spec.NodeName != "" {
		t.Fatalf("NodeLocal host/scheduler boundary = %+v", server.Spec)
	}
	if server.Spec.Affinity == nil || server.Spec.Affinity.NodeAffinity == nil {
		t.Fatalf("server exact-node affinity missing: %+v", server.Spec.Affinity)
	}
	for _, port := range server.Spec.Containers[0].Ports {
		if port.HostPort != port.ContainerPort || port.HostPort == 0 {
			t.Fatalf("listener is not declared as hostPort: %+v", port)
		}
	}
	if got := server.Labels[enginebinding.LabelCacheBackendUID]; got == "" {
		t.Fatal("server Pod is missing CacheBackend UID identity")
	}
	wantShmPath, err := builtinruntime.NodeLocalServerShmHostPath(&live)
	if err != nil {
		t.Fatal(err)
	}
	if !nodeLocalServerHasRuntimeIdentity(&server, wantShmPath) {
		t.Fatalf("server Pod lacks managed CUDA runtime identity: volumes=%v args=%v", server.Spec.Volumes, server.Spec.Containers[0].Args)
	}

	if err := k8s.Get(ctx, key, &live); err != nil {
		t.Fatalf("get NodeLocal CacheBackend: %v", err)
	}
	live.Spec.LMCache.NodeLocal.Server.Port = 16556
	if err := k8s.Update(ctx, &live); err != nil {
		t.Fatalf("update NodeLocal CacheBackend: %v", err)
	}
	reconcile(t, r, live.Name, live.Namespace)
	if err := k8s.Get(ctx, serverKey, &server); err != nil {
		t.Fatalf("get updated NodeLocal server Pod: %v", err)
	}
	if args := strings.Join(server.Spec.Containers[0].Args, " "); !strings.Contains(args, "--port 16556") {
		t.Fatalf("server Pod args did not update: %s", args)
	}

}
