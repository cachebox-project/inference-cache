// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

// stubAdapter is a minimal in-test adapter for exercising Registry selection
// independently of the reference adapter. Tests configure id and supportsFn
// to make Supports return whatever the case under test needs.
type stubAdapter struct {
	id         string
	supportsFn func(runtime RuntimeID, cache *cachev1alpha1.CacheBackend) bool
}

func (s stubAdapter) Supports(r RuntimeID, c *cachev1alpha1.CacheBackend) bool {
	if s.supportsFn == nil {
		return false
	}
	return s.supportsFn(r, c)
}

func (stubAdapter) SupportsBinding(*backendadapter.Binding) bool { return true }

func (stubAdapter) ResolveCacheServer(*cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return nil, nil, nil
}

func (stubAdapter) InjectEngineConfig(*corev1.PodSpec, *backendadapter.Binding, *cachev1alpha1.CacheBackend) error {
	return nil
}

func (stubAdapter) InjectRouterConfig(*corev1.PodSpec, *backendadapter.Binding, *cachev1alpha1.CacheBackend) error {
	return nil
}

func (stubAdapter) ObservationSidecar(*cachev1alpha1.CacheBackend, *corev1.Pod) (*corev1.Container, error) {
	return nil, nil
}

func (stubAdapter) ReservedArgs() []string { return nil }
func (stubAdapter) ReservedEnv() []string  { return nil }
func (stubAdapter) EngineContainerName() string {
	return ""
}

func newCacheBackend(t cachev1alpha1.CacheBackendType, engine string) *cachev1alpha1.CacheBackend {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: t},
	}
	switch engine {
	case "vllm":
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	case "sglang":
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	}
	return cb
}

func referenceBinding(endpoint string) *backendadapter.Binding {
	return &backendadapter.Binding{Protocol: backendadapter.ProtocolLMCache, Endpoint: endpoint}
}

func TestRegistrySelectFirstMatchWins(t *testing.T) {
	r := NewRegistry()
	r.Register(stubAdapter{id: "first", supportsFn: func(RuntimeID, *cachev1alpha1.CacheBackend) bool { return true }})
	r.Register(stubAdapter{id: "second", supportsFn: func(RuntimeID, *cachev1alpha1.CacheBackend) bool { return true }})

	got, err := r.Select(RuntimeVLLM, newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "vllm"))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	stub, ok := got.(stubAdapter)
	if !ok || stub.id != "first" {
		t.Fatalf("Select returned %#v, want stub.id=\"first\" (first match wins)", got)
	}
}

func TestRegistrySelectSkipsNonMatching(t *testing.T) {
	r := NewRegistry()
	r.Register(stubAdapter{id: "vllm-only", supportsFn: func(rt RuntimeID, _ *cachev1alpha1.CacheBackend) bool {
		return rt == RuntimeVLLM
	}})
	r.Register(stubAdapter{id: "sglang-only", supportsFn: func(rt RuntimeID, _ *cachev1alpha1.CacheBackend) bool {
		return rt == RuntimeSGLang
	}})

	got, err := r.Select(RuntimeSGLang, newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang"))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got.(stubAdapter).id != "sglang-only" {
		t.Fatalf("Select picked %q, want sglang-only", got.(stubAdapter).id)
	}
}

func TestRegistrySelectNoMatchReturnsErrNoAdapter(t *testing.T) {
	r := NewRegistry()
	r.Register(stubAdapter{id: "vllm", supportsFn: func(rt RuntimeID, _ *cachev1alpha1.CacheBackend) bool {
		return rt == RuntimeVLLM
	}})

	_, err := r.Select(RuntimeSGLang, newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang"))
	if !errors.Is(err, ErrNoAdapter) {
		t.Fatalf("expected ErrNoAdapter, got %v", err)
	}
}

func TestRegistrySelectNilCacheReturnsErrNoAdapter(t *testing.T) {
	r := NewRegistry()
	r.Register(stubAdapter{id: "any", supportsFn: func(RuntimeID, *cachev1alpha1.CacheBackend) bool { return true }})

	_, err := r.Select(RuntimeVLLM, nil)
	if !errors.Is(err, ErrNoAdapter) {
		t.Fatalf("expected ErrNoAdapter for nil cache, got %v", err)
	}
}

func TestRegistryEmpty(t *testing.T) {
	r := NewRegistry()
	if r.Len() != 0 {
		t.Fatalf("Len on empty registry = %d, want 0", r.Len())
	}
	_, err := r.Select(RuntimeVLLM, newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "vllm"))
	if !errors.Is(err, ErrNoAdapter) {
		t.Fatalf("expected ErrNoAdapter from empty registry, got %v", err)
	}
}

func TestRegistryRegisterNilIsNoop(t *testing.T) {
	r := NewRegistry()
	r.Register(nil)
	if r.Len() != 0 {
		t.Fatalf("Register(nil) modified registry; Len=%d", r.Len())
	}
}

func TestReferenceAdapterSupports(t *testing.T) {
	a := NewReferenceAdapter()
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "")

	if !a.Supports(RuntimeReference, cb) {
		t.Fatalf("Supports(reference, lmcache) = false, want true")
	}
	if a.Supports(RuntimeVLLM, cb) {
		t.Fatalf("Supports(vllm, lmcache) = true, want false (reference is gated on RuntimeReference)")
	}
	if a.Supports(RuntimeReference, nil) {
		t.Fatalf("Supports(reference, nil) = true, want false")
	}
	if a.SupportsBinding(nil) {
		t.Fatal("SupportsBinding(nil) = true, want false")
	}
	if !a.SupportsBinding(referenceBinding("cache:65432")) {
		t.Fatal("SupportsBinding(remote binding) = false, want true")
	}
}

func TestReferenceAdapterInjectEngineConfigMerges(t *testing.T) {
	a := NewReferenceAdapter()
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "")
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "engine",
				Env: []corev1.EnvVar{
					{Name: "EXISTING_VAR", Value: "keep-me"},
					{Name: "OTHER_VAR", Value: "also-keep"},
				},
			},
			{
				Name: "sidecar",
				Env:  []corev1.EnvVar{{Name: "SIDECAR_VAR", Value: "untouched"}},
			},
		},
	}

	if err := a.InjectEngineConfig(pod, referenceBinding("cache.ns1.svc.cluster.local:8000"), cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}

	for _, c := range pod.Containers {
		gotEndpoint, hasEndpoint := lookupEnv(c.Env, EnvCacheEndpoint)
		if !hasEndpoint || gotEndpoint != "cache.ns1.svc.cluster.local:8000" {
			t.Fatalf("container %q: %s = (%q, %v), want (\"cache.ns1.svc.cluster.local:8000\", true)",
				c.Name, EnvCacheEndpoint, gotEndpoint, hasEndpoint)
		}
	}

	// Existing env entries on the engine container are preserved unchanged.
	if v, _ := lookupEnv(pod.Containers[0].Env, "EXISTING_VAR"); v != "keep-me" {
		t.Fatalf("EXISTING_VAR was clobbered: got %q, want keep-me", v)
	}
	if v, _ := lookupEnv(pod.Containers[0].Env, "OTHER_VAR"); v != "also-keep" {
		t.Fatalf("OTHER_VAR was clobbered: got %q, want also-keep", v)
	}
	if v, _ := lookupEnv(pod.Containers[1].Env, "SIDECAR_VAR"); v != "untouched" {
		t.Fatalf("SIDECAR_VAR on sidecar was clobbered: got %q, want untouched", v)
	}
}

func TestReferenceAdapterInjectEngineConfigIsIdempotent(t *testing.T) {
	a := NewReferenceAdapter()
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "")
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}}

	// First call writes the endpoint env.
	if err := a.InjectEngineConfig(pod, referenceBinding("first.svc:9090"), cb); err != nil {
		t.Fatalf("first InjectEngineConfig: %v", err)
	}
	// Second call updates the value in place — must not duplicate the entry.
	if err := a.InjectEngineConfig(pod, referenceBinding("second.svc:9090"), cb); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}

	envs := pod.Containers[0].Env
	matches := 0
	for _, e := range envs {
		if e.Name == EnvCacheEndpoint {
			matches++
			if e.Value != "second.svc:9090" {
				t.Fatalf("idempotent inject did not update value: got %q, want second.svc:9090", e.Value)
			}
		}
	}
	if matches != 1 {
		t.Fatalf("expected exactly 1 %s entry after second inject, got %d", EnvCacheEndpoint, matches)
	}
}

func TestReferenceAdapterInjectRejectsBadInput(t *testing.T) {
	a := NewReferenceAdapter()
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "")
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}}

	cases := []struct {
		name string
		fn   func() error
	}{
		{"nil pod", func() error { return a.InjectEngineConfig(nil, referenceBinding("x"), cb) }},
		{"nil cache", func() error { return a.InjectEngineConfig(good, referenceBinding("x"), nil) }},
		{"nil binding", func() error { return a.InjectEngineConfig(good, nil, cb) }},
		{"empty protocol", func() error {
			return a.InjectEngineConfig(good, &backendadapter.Binding{Endpoint: "x"}, cb)
		}},
		{"empty endpoint", func() error { return a.InjectEngineConfig(good, referenceBinding(""), cb) }},
		{"no containers", func() error { return a.InjectEngineConfig(&corev1.PodSpec{}, referenceBinding("x"), cb) }},
		{"router nil pod", func() error { return a.InjectRouterConfig(nil, referenceBinding("x"), cb) }},
		{"router nil cache", func() error { return a.InjectRouterConfig(good, referenceBinding("x"), nil) }},
		{"router nil binding", func() error { return a.InjectRouterConfig(good, nil, cb) }},
		{"router empty protocol", func() error {
			return a.InjectRouterConfig(good, &backendadapter.Binding{Endpoint: "x"}, cb)
		}},
		{"router empty endpoint", func() error { return a.InjectRouterConfig(good, referenceBinding(""), cb) }},
		{"router no containers", func() error { return a.InjectRouterConfig(&corev1.PodSpec{}, referenceBinding("x"), cb) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestReferenceAdapterInjectRouterConfig(t *testing.T) {
	a := NewReferenceAdapter()
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "")
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router"}}}

	if err := a.InjectRouterConfig(pod, referenceBinding("router.svc:9000"), cb); err != nil {
		t.Fatalf("InjectRouterConfig: %v", err)
	}
	v, ok := lookupEnv(pod.Containers[0].Env, EnvRouterEndpoint)
	if !ok || v != "router.svc:9000" {
		t.Fatalf("%s = (%q, %v), want (\"router.svc:9000\", true)", EnvRouterEndpoint, v, ok)
	}
	// Router inject must not also write the engine env (the two roles stay distinct).
	if _, ok := lookupEnv(pod.Containers[0].Env, EnvCacheEndpoint); ok {
		t.Fatalf("router inject also wrote %s; the two roles should stay disjoint", EnvCacheEndpoint)
	}
}

func TestRegistryResolvesReferenceAdapterByRuntime(t *testing.T) {
	r := NewRegistry()
	r.Register(NewReferenceAdapter())
	got, err := r.Select(RuntimeReference, newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, ""))
	if err != nil {
		t.Fatalf("Select(reference): %v", err)
	}
	if _, isRef := got.(referenceAdapter); !isRef {
		t.Fatalf("Select(reference) returned %T, want referenceAdapter", got)
	}
}

func TestResolveRuntimeID(t *testing.T) {
	// ResolveRuntimeID is the single rule the admission validator, the
	// reconciler, and the pod-mutating webhook all read the runtime identity
	// through — pinning it here prevents a future tweak in one layer
	// from quietly diverging from the others.
	cases := []struct {
		name string
		in   *cachev1alpha1.CacheBackend
		want RuntimeID
	}{
		{
			name: "nil cache has no runtime",
			in:   nil,
			want: "",
		},
		{
			name: "unset runtime stays empty",
			in:   &cachev1alpha1.CacheBackend{},
			want: "",
		},
		{
			name: "VLLM maps to canonical id",
			in:   &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{Runtime: cachev1alpha1.CacheBackendRuntimeVLLM}},
			want: RuntimeVLLM,
		},
		{
			name: "SGLang maps to canonical id",
			in:   &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{Runtime: cachev1alpha1.CacheBackendRuntimeSGLang}},
			want: RuntimeID("sglang"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveRuntimeID(tc.in); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestValidateConnectorDeclaration(t *testing.T) {
	requirement := LMCacheConnectorRequirement{
		Profile:       "sglang-lmcache-mp-v1",
		ClientVersion: "0.5.3",
	}
	tests := []struct {
		name        string
		annotations map[string]string
		wantErr     bool
	}{
		{
			name: "matching declaration",
			annotations: map[string]string{
				AnnotationLMCacheConnectorProfile: "sglang-lmcache-mp-v1",
				AnnotationLMCacheClientVersion:    "0.5.3",
			},
		},
		{name: "missing declaration", wantErr: true},
		{
			name: "profile mismatch",
			annotations: map[string]string{
				AnnotationLMCacheConnectorProfile: "vllm-lmcache-mp-v1",
				AnnotationLMCacheClientVersion:    "0.5.3",
			},
			wantErr: true,
		},
		{
			name: "version mismatch",
			annotations: map[string]string{
				AnnotationLMCacheConnectorProfile: "sglang-lmcache-mp-v1",
				AnnotationLMCacheClientVersion:    "0.5.2",
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
			err := ValidateConnectorDeclaration(pod, requirement)
			if tc.wantErr && err == nil {
				t.Fatal("expected declaration error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected declaration error: %v", err)
			}
		})
	}
	if err := ValidateConnectorDeclaration(nil, requirement); err == nil {
		t.Fatal("nil pod should be rejected")
	}
}

func lookupEnv(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}
