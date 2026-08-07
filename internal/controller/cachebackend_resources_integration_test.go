// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestIntegrationCacheBackendResources exercises provider resources
// end-to-end against a real apiserver. A managed provider with no explicit
// resource block receives bounded renderer defaults without persisting those
// defaults, while an operator-supplied typed resource block is threaded verbatim
// into the rendered Deployment container.
//
// The unit-level renderer test
// (TestVLLMLMCacheResolveCacheServerHonorsProviderResources) constructs
// CacheBackend objects directly; this test also guards the persisted API shape
// after real-apiserver admission.
func TestIntegrationCacheBackendResources(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	newCanonicalBackend := func(namespace string) *cachev1alpha1.CacheBackend {
		cb := lmcacheBackend("cache", namespace)
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
		cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
			Provider:      cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
			Ownership:     cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
			LMCacheServer: &cachev1alpha1.LMCacheServerRemoteStorageSpec{},
		}
		return cb
	}

	t.Run("DefaultBoundsProviderWithoutPersistingResources", func(t *testing.T) {
		// Provider defaults belong to the renderer, not the persisted API.
		// The child container MUST still carry memory requests
		// and limits so heavy T2 write load cannot leave it unbounded.
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, newCanonicalBackend(ns)); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		if cb.Spec.RemoteStorage.LMCacheServer.Resources != nil {
			t.Fatalf("renderer default leaked into spec.remoteStorage.lmCacheServer.resources: %+v",
				cb.Spec.RemoteStorage.LMCacheServer.Resources)
		}

		wantReq := resource.MustParse("4Gi")
		wantLim := resource.MustParse("8Gi")
		dep := getDeployment(t, r, "cache", ns)
		container := dep.Spec.Template.Spec.Containers[0]
		if got := container.Resources.Requests[corev1.ResourceMemory]; got.Cmp(wantReq) != 0 {
			t.Fatalf("container Requests[memory] = %v, want %v (renderer default)", got.String(), wantReq.String())
		}
		if got := container.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantLim) != 0 {
			t.Fatalf("container Limits[memory] = %v, want %v (renderer default)", got.String(), wantLim.String())
		}
	})

	t.Run("OperatorOverrideHonored", func(t *testing.T) {
		// A typed provider resource block is the canonical tuning knob; the
		// rendered container MUST reflect it byte-for-byte.
		ns := freshNS(t, k8s)
		cb := newCanonicalBackend(ns)
		cb.Spec.RemoteStorage.LMCacheServer.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("12Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create CacheBackend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		dep := getDeployment(t, r, "cache", ns)
		container := dep.Spec.Template.Spec.Containers[0]
		wantReq := resource.MustParse("12Gi")
		if got := container.Resources.Requests[corev1.ResourceMemory]; got.Cmp(wantReq) != 0 {
			t.Fatalf("container Requests[memory] = %v, want operator-supplied %v", got.String(), wantReq.String())
		}
		wantLim := resource.MustParse("16Gi")
		if got := container.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantLim) != 0 {
			t.Fatalf("container Limits[memory] = %v, want operator-supplied %v", got.String(), wantLim.String())
		}
	})
}
