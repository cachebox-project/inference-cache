package enginewire

import "testing"

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
