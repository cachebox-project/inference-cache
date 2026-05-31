package controller

import (
	"context"
	"testing"

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

	// The trimmed shape applies cleanly.
	trimmed := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "trimmed", Namespace: "default"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
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
		u.SetNamespace("default")
		u.SetName(name)
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
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, got); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		return got
	}

	// Removed spec fields are pruned on create and never round-trip. obj is a
	// separate RFC-1123 object name (the field name is mixed-case and cannot be
	// used as metadata.name).
	specCases := []struct {
		name string
		obj  string
		path []string
	}{
		{"lookupTimeoutMs", "retired-spec-lookup-timeout", []string{"spec", "integration", "lookupTimeoutMs"}},
		{"minimumPrefixTokens", "retired-spec-min-prefix-tokens", []string{"spec", "integration", "minimumPrefixTokens"}},
	}
	for _, tc := range specCases {
		t.Run(tc.name, func(t *testing.T) {
			name := tc.obj
			u := newManaged(name)
			if err := unstructured.SetNestedField(u.Object, int64(7), tc.path...); err != nil {
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

	// Removed status field is pruned too. status.* needs the status subresource
	// to round-trip at all, so write it via a status update and confirm it does
	// not persist.
	t.Run("indexEntries", func(t *testing.T) {
		name := "retired-status-indexentries"
		if err := c.Create(ctx, newManaged(name)); err != nil {
			t.Fatalf("create: %v", err)
		}
		cur := get(name)
		if err := unstructured.SetNestedField(cur.Object, int64(7), "status", "indexEntries"); err != nil {
			t.Fatalf("set status.indexEntries: %v", err)
		}
		if err := c.Status().Update(ctx, cur); err != nil {
			t.Fatalf("status update: %v", err)
		}
		if _, found, _ := unstructured.NestedFieldNoCopy(get(name).Object, "status", "indexEntries"); found {
			t.Fatalf("status.indexEntries persisted; want pruned (field removed from schema)")
		}
	})
}
