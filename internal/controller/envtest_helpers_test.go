// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func skipWithoutEnvtest(t *testing.T) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with `KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test` for envtest")
	}
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(os.Stderr)))
}

func startEnv(t *testing.T) (client.Client, *runtime.Scheme, *rest.Config) {
	t.Helper()
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	return k8s, scheme, cfg
}

var integrationNamespaceCounter int64

func freshNS(t *testing.T, k8s client.Client) string {
	t.Helper()
	name := fmt.Sprintf("it-%d", atomic.AddInt64(&integrationNamespaceCounter, 1))
	if err := k8s.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	return name
}

func ptrBool(v bool) *bool { return &v }

func pollDeployment(t *testing.T, k8s client.Client, key client.ObjectKey, what string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var dep appsv1.Deployment
		if err := k8s.Get(context.Background(), key, &dep); err == nil {
			return string(dep.UID)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for deployment to %s", what)
	return ""
}
