package enginewire

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// lookupInjectedEnv returns the value of the env var named name on the engine
// container and whether it was present.
func lookupInjectedEnv(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// TestInjectVLLMLMCache_InjectsEnv pins the full set of env names the engine
// wire injects, and asserts the PYTHONHASHSEED correctness invariant is
// present with value "0". PYTHONHASHSEED pins the deterministic NONE_HASH
// across the scheduler + TP worker processes so LMCache reload matches under
// TP>1 — without it the reload silently 0-hits and the engine fully
// recomputes (no crash, no error).
func TestInjectVLLMLMCache_InjectsEnv(t *testing.T) {
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: EngineContainerName}},
	}
	cache := &cachev1alpha1.CacheBackend{}

	if err := InjectVLLMLMCache(pod, "cache.example:65432", cache); err != nil {
		t.Fatalf("InjectVLLMLMCache: %v", err)
	}
	env := pod.Containers[0].Env

	// The exact set of env names the wire injects. Adding/removing one is a
	// contract change — this assertion is intentionally exact.
	wantNames := map[string]bool{
		EnvLMCacheRemoteURL:       true,
		EnvLMCacheRemoteSerde:     true,
		EnvLMCacheChunkSize:       true,
		EnvLMCacheLocalCPU:        true,
		EnvLMCacheMaxLocalCPU:     true,
		EnvVLLMUseV1:              true,
		EnvInferenceCacheFailOpen: true,
		EnvPythonHashSeed:         true,
	}
	gotNames := make(map[string]bool, len(env))
	for _, e := range env {
		gotNames[e.Name] = true
	}
	// Exact count guards against a duplicate injected entry slipping past the
	// name-set checks below (the map collapses a duplicate to one key).
	if len(env) != len(wantNames) {
		t.Errorf("injected env count = %d, want %d (duplicate or missing entry); env = %v", len(env), len(wantNames), env)
	}
	for name := range wantNames {
		if !gotNames[name] {
			t.Errorf("injected env missing %q; got %v", name, gotNames)
		}
	}
	for name := range gotNames {
		if !wantNames[name] {
			t.Errorf("injected env has unexpected entry %q; got %v", name, gotNames)
		}
	}

	// Focused assertion: the PYTHONHASHSEED correctness invariant is injected
	// with exactly "0" so every engine process derives the same NONE_HASH.
	if v, ok := lookupInjectedEnv(env, EnvPythonHashSeed); !ok || v != "0" {
		t.Fatalf("%s = (%q, %v), want 0", EnvPythonHashSeed, v, ok)
	}
}

func TestLMCacheRemoteURL_BareHostGetsScheme(t *testing.T) {
	got := LMCacheRemoteURL("cache.example:8200")
	want := "lm://cache.example:8200"
	if got != want {
		t.Fatalf("LMCacheRemoteURL(bare) = %q, want %q", got, want)
	}
}

func TestLMCacheRemoteURL_LowerCaseSchemePreserved(t *testing.T) {
	got := LMCacheRemoteURL("lm://cache.example:8200")
	want := "lm://cache.example:8200"
	if got != want {
		t.Fatalf("LMCacheRemoteURL(lower) = %q, want %q", got, want)
	}
}

func TestLMCacheRemoteURL_UpperCaseSchemeNormalised(t *testing.T) {
	// Admission lowercases the scheme during validation, so `LM://...`
	// admits. The helper must normalise to lower-case `lm://` rather
	// than passing through and producing `lm://LM://...` at injection.
	cases := []string{
		"LM://cache.example:8200",
		"Lm://cache.example:8200",
		"lM://cache.example:8200",
	}
	for _, in := range cases {
		got := LMCacheRemoteURL(in)
		want := "lm://cache.example:8200"
		if got != want {
			t.Fatalf("LMCacheRemoteURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLMCacheRemoteURL_HostCasingPreserved(t *testing.T) {
	// The case normalisation is scoped to the scheme only; the host
	// portion is preserved verbatim so we don't silently rewrite the
	// operator's typed value.
	got := LMCacheRemoteURL("lm://Cache.Example.Com:8200")
	want := "lm://Cache.Example.Com:8200"
	if got != want {
		t.Fatalf("LMCacheRemoteURL(mixed host) = %q, want %q", got, want)
	}
}

func TestLMCacheRemoteURL_ShortInputDoesNotPanic(t *testing.T) {
	// Defensive: an input shorter than the scheme length must not
	// index out of bounds. (Admission rejects this at the webhook,
	// but the helper is part of the engine wire seam and is called
	// from the External adapter without re-validating.)
	for _, in := range []string{"", "lm", "lm:", "lm:/"} {
		got := LMCacheRemoteURL(in)
		want := "lm://" + in
		if got != want {
			t.Fatalf("LMCacheRemoteURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestInjectVLLMMooncake_InjectsEnv asserts the Mooncake wire injects the SAME
// env set as the LMCache wire (Mooncake is an LMCache remote backend) but with
// LMCACHE_REMOTE_URL carrying the mooncakestore:// scheme, and the SAME
// LMCacheConnectorV1 --kv-transfer-config arg. The only on-the-wire difference
// from InjectVLLMLMCache is the remote-URL scheme.
func TestInjectVLLMMooncake_InjectsEnv(t *testing.T) {
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: EngineContainerName}},
	}
	cache := &cachev1alpha1.CacheBackend{}

	if err := InjectVLLMMooncake(pod, "mooncake.example:50051", cache); err != nil {
		t.Fatalf("InjectVLLMMooncake: %v", err)
	}
	env := pod.Containers[0].Env

	wantNames := map[string]bool{
		EnvLMCacheRemoteURL:       true,
		EnvLMCacheRemoteSerde:     true,
		EnvLMCacheChunkSize:       true,
		EnvLMCacheLocalCPU:        true,
		EnvLMCacheMaxLocalCPU:     true,
		EnvVLLMUseV1:              true,
		EnvInferenceCacheFailOpen: true,
		EnvPythonHashSeed:         true,
	}
	gotNames := make(map[string]bool, len(env))
	for _, e := range env {
		gotNames[e.Name] = true
	}
	if len(env) != len(wantNames) {
		t.Errorf("injected env count = %d, want %d; env = %v", len(env), len(wantNames), env)
	}
	for name := range wantNames {
		if !gotNames[name] {
			t.Errorf("injected env missing %q; got %v", name, gotNames)
		}
	}

	// The remote URL carries the mooncakestore:// scheme (the defining
	// difference from the LMCache wire).
	if v, ok := lookupInjectedEnv(env, EnvLMCacheRemoteURL); !ok || v != "mooncakestore://mooncake.example:50051" {
		t.Fatalf("%s = (%q, %v), want mooncakestore://mooncake.example:50051", EnvLMCacheRemoteURL, v, ok)
	}
	// PYTHONHASHSEED correctness invariant still pinned to "0".
	if v, ok := lookupInjectedEnv(env, EnvPythonHashSeed); !ok || v != "0" {
		t.Fatalf("%s = (%q, %v), want 0", EnvPythonHashSeed, v, ok)
	}
	// The connector arg is the shared LMCache connector (Mooncake is wired
	// as an LMCache remote backend).
	args := pod.Containers[0].Args
	if len(args) < 2 || args[0] != "--kv-transfer-config" || !contains(args[1], "LMCacheConnectorV1") {
		t.Fatalf("kv-transfer-config arg = %v, want LMCacheConnectorV1 connector", args)
	}
}

func TestMooncakeStoreRemoteURL_BareHostGetsScheme(t *testing.T) {
	got := MooncakeStoreRemoteURL("mooncake.example:50051")
	want := "mooncakestore://mooncake.example:50051"
	if got != want {
		t.Fatalf("MooncakeStoreRemoteURL(bare) = %q, want %q", got, want)
	}
}

func TestMooncakeStoreRemoteURL_SchemePreserved(t *testing.T) {
	got := MooncakeStoreRemoteURL("mooncakestore://mooncake.example:50051")
	want := "mooncakestore://mooncake.example:50051"
	if got != want {
		t.Fatalf("MooncakeStoreRemoteURL(prefixed) = %q, want %q (must not double the scheme)", got, want)
	}
}

func TestMooncakeStoreRemoteURL_UpperCaseSchemeNormalised(t *testing.T) {
	for _, in := range []string{
		"MOONCAKESTORE://mooncake.example:50051",
		"MooncakeStore://mooncake.example:50051",
	} {
		got := MooncakeStoreRemoteURL(in)
		want := "mooncakestore://mooncake.example:50051"
		if got != want {
			t.Fatalf("MooncakeStoreRemoteURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMooncakeStoreRemoteURL_HostCasingPreserved(t *testing.T) {
	got := MooncakeStoreRemoteURL("mooncakestore://Mooncake.Example.Com:50051")
	want := "mooncakestore://Mooncake.Example.Com:50051"
	if got != want {
		t.Fatalf("MooncakeStoreRemoteURL(mixed host) = %q, want %q", got, want)
	}
}

func TestMooncakeStoreRemoteURL_ShortInputDoesNotPanic(t *testing.T) {
	for _, in := range []string{"", "moon", "mooncakestore:", "mooncakestore:/"} {
		got := MooncakeStoreRemoteURL(in)
		want := "mooncakestore://" + in
		if got != want {
			t.Fatalf("MooncakeStoreRemoteURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateLMCacheEndpoint(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantErr   bool
		wantMatch string // substring expected in error message; empty = any error
	}{
		// Valid shapes.
		{name: "bare-host-port", input: "cache.example:8200"},
		{name: "lm-prefixed", input: "lm://cache.example:8200"},
		{name: "lm-prefixed-uppercase", input: "LM://cache.example:8200"},
		{name: "ipv4-host-port", input: "10.0.0.1:8200"},
		{name: "bracketed-ipv6", input: "[2001:db8::1]:8200"},
		{name: "bracketed-ipv6-loopback", input: "[::1]:8200"},
		{name: "leading-trailing-whitespace-trimmed", input: "  cache.example:8200  "},

		// Invalid shapes — empty.
		{name: "empty", input: "", wantErr: true, wantMatch: "endpoint is empty"},
		{name: "whitespace-only", input: "   ", wantErr: true, wantMatch: "endpoint is empty"},

		// Invalid shapes — schemes.
		{name: "https-scheme", input: "https://cache.example:443", wantErr: true, wantMatch: "scheme \"https\" is not supported"},
		{name: "http-scheme", input: "http://cache.example:80", wantErr: true, wantMatch: "scheme \"http\" is not supported"},
		{name: "tcp-scheme", input: "tcp://cache:8200", wantErr: true, wantMatch: "scheme \"tcp\" is not supported"},

		// Invalid shapes — path/query/fragment.
		{name: "lm-with-path", input: "lm://cache:8200/path", wantErr: true, wantMatch: "paths/queries/fragments"},
		{name: "lm-with-query", input: "lm://cache:8200?q=1", wantErr: true, wantMatch: "paths/queries/fragments"},
		{name: "lm-with-fragment", input: "lm://cache:8200#frag", wantErr: true, wantMatch: "paths/queries/fragments"},

		// Invalid shapes — missing host/port.
		{name: "scheme-only", input: "lm://", wantErr: true, wantMatch: "non-empty host AND port"},
		{name: "port-only", input: ":8200", wantErr: true, wantMatch: "non-empty host AND port"},
		{name: "lm-port-only", input: "lm://:8200", wantErr: true, wantMatch: "non-empty host AND port"},
		{name: "host-only-no-port", input: "cache.example", wantErr: true, wantMatch: "non-empty host AND port"},
		{name: "trailing-colon-empty-port", input: "cache.example:", wantErr: true, wantMatch: "non-empty host AND port"},
		{name: "bracketed-ipv6-no-port", input: "[::1]", wantErr: true, wantMatch: "non-empty host AND port"},

		// Invalid shapes — unbracketed IPv6.
		{name: "unbracketed-ipv6", input: "2001:db8::1", wantErr: true, wantMatch: "non-empty host AND port"},
		{name: "unbracketed-ipv6-loopback", input: "::1", wantErr: true, wantMatch: "non-empty host AND port"},

		// Invalid shapes — embedded whitespace/control chars.
		{name: "embedded-space-in-host", input: "cache example:8200", wantErr: true, wantMatch: "whitespace or control characters"},
		{name: "embedded-space-in-port", input: "cache:82 00", wantErr: true, wantMatch: "whitespace or control characters"},
		{name: "embedded-tab", input: "cache.example:82\t00", wantErr: true, wantMatch: "whitespace or control characters"},
		{name: "embedded-newline", input: "cache.example:8200\nLMCACHE_LOG_LEVEL=debug", wantErr: true, wantMatch: "whitespace or control characters"},
		{name: "embedded-null", input: "cache.example:82\x0000", wantErr: true, wantMatch: "whitespace or control characters"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLMCacheEndpoint(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ValidateLMCacheEndpoint(%q) = nil, want error containing %q", tc.input, tc.wantMatch)
				}
				if tc.wantMatch != "" && !contains(err.Error(), tc.wantMatch) {
					t.Fatalf("ValidateLMCacheEndpoint(%q) error = %q, want substring %q", tc.input, err.Error(), tc.wantMatch)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateLMCacheEndpoint(%q) = %v, want nil", tc.input, err)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
