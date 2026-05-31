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
