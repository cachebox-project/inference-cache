// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestIntegrationCacheBackendSchemaTrim exercises the v1alpha1 schema trim at
// the real apiserver layer (not just the generated-schema unit assertions). It
// confirms the three removed fields are genuinely gone from the served schema:
//
//   - a CacheBackend with the trimmed shape is accepted; and
//   - a manifest that still populates one of the removed fields
//     (spec.integration.lookupTimeoutMs, spec.integration.minimumPrefixTokens,
//     status.indexEntries) has that field dropped on write and never
//     round-trips — proving an operator setting it gets nothing.
//
// Note on behavior: the apiserver *prunes* fields absent from a structural CRD
// schema rather than rejecting the request, so the assertion is "field does not
// persist" rather than "create errors". (status.* additionally requires the
// status subresource to round-trip, so it is written via a status update.)
//
// Skipped unless KUBEBUILDER_ASSETS is set (see skipWithoutEnvtest).
func TestIntegrationCacheBackendSchemaTrim(t *testing.T) {
	skipWithoutEnvtest(t)
	c, _, _ := startEnv(t)
	ctx := context.Background()
	ns := freshNS(t, c)

	// The trimmed shape applies cleanly.
	trimmed := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "trimmed", Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			Runtime: cachev1alpha1.CacheBackendRuntimeVLLM,
			Type:    cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app.kubernetes.io/name": "vllm"},
			},
		},
	}
	if err := c.Create(ctx, trimmed); err != nil {
		t.Fatalf("create trimmed CacheBackend: %v", err)
	}

	gvk := trimmed.GroupVersionKind()
	if gvk.Empty() {
		gvk.Group, gvk.Version, gvk.Kind = "inferencecache.io", "v1alpha1", "CacheBackend"
	}

	// newManaged returns a minimally-valid managed CacheBackend (selector set so
	// it is otherwise admissible).
	newManaged := func(name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("inferencecache.io/v1alpha1")
		u.SetKind("CacheBackend")
		u.SetNamespace(ns)
		u.SetName(name)
		if err := unstructured.SetNestedField(u.Object, "VLLM", "spec", "runtime"); err != nil {
			t.Fatalf("set spec.runtime: %v", err)
		}
		if err := unstructured.SetNestedField(u.Object, "LMCache", "spec", "type"); err != nil {
			t.Fatalf("set spec.type: %v", err)
		}
		if err := unstructured.SetNestedStringMap(u.Object, map[string]string{"app.kubernetes.io/name": "vllm"}, "spec", "engineSelector", "matchLabels"); err != nil {
			t.Fatalf("set spec.engineSelector: %v", err)
		}
		return u
	}

	get := func(name string) *unstructured.Unstructured {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(gvk)
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, got); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		return got
	}

	for _, tc := range []struct {
		name        string
		backendType string
	}{
		{name: "legacy-mooncake", backendType: "Mooncake"},
		{name: "legacy-external", backendType: "External"},
		{name: "unsupported-aibrix", backendType: "AIBrix"},
		{name: "unsupported-nixl", backendType: "NIXL"},
		{name: "unknown", backendType: "Unknown"},
	} {
		t.Run("reject-type-"+tc.name, func(t *testing.T) {
			u := newManaged("reject-type-" + tc.name)
			if err := unstructured.SetNestedField(u.Object, tc.backendType, "spec", "type"); err != nil {
				t.Fatalf("set spec.type: %v", err)
			}
			if err := c.Create(ctx, u); !apierrors.IsInvalid(err) {
				t.Fatalf("create with spec.type=%q error = %v, want Invalid from CRD enum", tc.backendType, err)
			}
		})
	}

	for _, provider := range []string{"LMCacheServer", "Mooncake"} {
		t.Run("reject-provider-"+provider, func(t *testing.T) {
			u := newManaged("reject-provider-" + strings.ToLower(provider))
			if err := unstructured.SetNestedMap(u.Object, map[string]any{
				"provider": provider, "ownership": "Managed",
			}, "spec", "remoteStorage"); err != nil {
				t.Fatalf("set spec.remoteStorage: %v", err)
			}
			if err := c.Create(ctx, u); !apierrors.IsInvalid(err) {
				t.Fatalf("create with remoteStorage.provider=%q error = %v, want Invalid from CRD enum", provider, err)
			}
		})
	}

	// Removed spec fields are pruned on create and never round-trip. obj is a
	// separate RFC-1123 object name (the field name is mixed-case and cannot be
	// used as metadata.name).
	specCases := []struct {
		name  string
		obj   string
		path  []string
		value any
	}{
		{"lookupTimeoutMs", "retired-spec-lookup-timeout", []string{"spec", "integration", "lookupTimeoutMs"}, int64(7)},
		{"minimumPrefixTokens", "retired-spec-min-prefix-tokens", []string{"spec", "integration", "minimumPrefixTokens"}, int64(7)},
		{"deploymentKind", "retired-deployment-kind", []string{"spec", "deploymentKind"}, "Deployment"},
		{"replicas", "retired-replicas", []string{"spec", "replicas"}, int64(1)},
		{"autoscaling", "retired-autoscaling", []string{"spec", "autoscaling"}, map[string]any{"maxReplicas": int64(2)}},
		{"template", "retired-template", []string{"spec", "template"}, map[string]any{"schedulerName": "default-scheduler"}},
		{"hostMemory", "retired-host-memory", []string{"spec", "lmCache", "hostMemory"}, map[string]any{"capacity": "1Gi"}},
		{"workerImage", "retired-worker-image", []string{"spec", "lmCache", "workerImage"}, "example.invalid/worker:v1"},
		{"workerPort", "retired-worker-port", []string{"spec", "lmCache", "workerPort"}, int64(5555)},
		{"remoteSerde", "retired-remote-serde", []string{"spec", "lmCache", "remoteSerde"}, "cachegen"},
	}
	for _, tc := range specCases {
		t.Run(tc.name, func(t *testing.T) {
			name := tc.obj
			u := newManaged(name)
			if err := unstructured.SetNestedField(u.Object, tc.value, tc.path...); err != nil {
				t.Fatalf("set %s: %v", tc.name, err)
			}
			if err := c.Create(ctx, u); err != nil {
				t.Fatalf("create with %s: %v", tc.name, err)
			}
			if _, found, _ := unstructured.NestedFieldNoCopy(get(name).Object, tc.path...); found {
				t.Fatalf("%s persisted; want pruned (field removed from schema)", tc.name)
			}
		})
	}

	// Removed status fields are pruned too. status.* needs the status subresource
	// to round-trip at all, so write it via a status update and confirm it does
	// not persist.
	for _, fieldName := range []string{"indexEntries", "endpoint", "observedServerInstance"} {
		t.Run(fieldName, func(t *testing.T) {
			name := "retired-status-indexentries"
			if fieldName != "indexEntries" {
				name = "retired-status-" + strings.ToLower(fieldName)
			}
			if err := c.Create(ctx, newManaged(name)); err != nil {
				t.Fatalf("create: %v", err)
			}
			cur := get(name)
			value := any("removed")
			if fieldName == "indexEntries" {
				value = int64(7)
			}
			if err := unstructured.SetNestedField(cur.Object, value, "status", fieldName); err != nil {
				t.Fatalf("set status.%s: %v", fieldName, err)
			}
			if err := c.Status().Update(ctx, cur); err != nil {
				t.Fatalf("status update: %v", err)
			}
			if _, found, _ := unstructured.NestedFieldNoCopy(get(name).Object, "status", fieldName); found {
				t.Fatalf("status.%s persisted; want pruned (field removed from schema)", fieldName)
			}
		})
	}
}
