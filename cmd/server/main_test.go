package main

import (
	"strings"
	"testing"
)

// TestValidateControllerAuthFlags pins the operator-facing startup contract for
// the controller-facing auth flag combinations that gate BOTH /snapshot and
// /policy. Each row is a real shape an operator might wire — including the
// failure modes the validation exists to catch (silent unauth,
// audience-empty bypass, whitespace-bracketed values, both knobs on at once).
//
// The diagnostic message is asserted only by substring (the user-visible cue
// the failure points at) so the wording can evolve without breaking the
// contract test.
func TestValidateControllerAuthFlags(t *testing.T) {
	const (
		ctrlSA = "system:serviceaccount:inference-cache-system:inference-cache-controller-manager"
		aud    = "inferencecache.io/controller"
	)

	cases := []struct {
		name       string
		expectedSA string
		audience   string
		insecure   bool
		wantSubstr string // "" → expect valid (empty return)
	}{
		{
			name:       "production: SA set, audience set, insecure off",
			expectedSA: ctrlSA,
			audience:   aud,
			insecure:   false,
			wantSubstr: "",
		},
		{
			name:       "local-dev: SA empty, insecure on",
			expectedSA: "",
			audience:   aud, // audience flag value is ignored when auth is off
			insecure:   true,
			wantSubstr: "",
		},
		{
			name:       "mutually exclusive: SA set AND insecure on",
			expectedSA: ctrlSA,
			audience:   aud,
			insecure:   true,
			wantSubstr: "mutually exclusive",
		},
		{
			name:       "silent unauth blocked: neither flag set",
			expectedSA: "",
			audience:   aud,
			insecure:   false,
			wantSubstr: "missing --allowed-controller-sa",
		},
		{
			name:       "audience bypass blocked: SA set, audience empty",
			expectedSA: ctrlSA,
			audience:   "",
			insecure:   false,
			wantSubstr: "--controller-audience cannot be empty",
		},
		{
			// Whitespace-only is the chart-value-pasted-with-yaml-indent
			// case. Treated identically to empty — it cannot match the
			// projected SA token's audience, so failing fast at startup
			// beats a runtime 401 storm after the controller poller has
			// been silently broken.
			name:       "audience bypass blocked: SA set, audience whitespace-only",
			expectedSA: ctrlSA,
			audience:   "   \t  ",
			insecure:   false,
			wantSubstr: "--controller-audience cannot be empty",
		},
		{
			// Leading/trailing whitespace around a real-looking value is
			// the "yaml quote pasted with extra spaces" case. The raw
			// string would be sent to TokenReviewSpec.Audiences as-is
			// and would not match the JWT-baked audience, so the runtime
			// would 401-loop while boot reported auth as enabled. Reject
			// with a fail-fast diagnostic that names the actual value
			// so the operator sees the extra whitespace.
			name:       "audience bypass blocked: SA set, audience has trailing whitespace",
			expectedSA: ctrlSA,
			audience:   "inferencecache.io/controller ",
			insecure:   false,
			wantSubstr: "leading/trailing whitespace",
		},
		{
			// Same whitespace failure mode as the audience case, but for
			// the SA flag: kube-apiserver returns a username with NO
			// whitespace ("system:serviceaccount:NS:NAME"), so a pasted
			// value with stray spaces would never match and every
			// controller scrape would 403. Fail fast at startup with
			// the value echoed so the operator sees the whitespace.
			name:       "SA bypass blocked: --allowed-controller-sa has trailing whitespace",
			expectedSA: ctrlSA + " ",
			audience:   "inferencecache.io/controller",
			insecure:   false,
			wantSubstr: "--allowed-controller-sa has leading/trailing whitespace",
		},
		{
			name:       "audience empty is fine when insecure is on (no audience check needed)",
			expectedSA: "",
			audience:   "",
			insecure:   true,
			wantSubstr: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateControllerAuthFlags(tc.expectedSA, tc.audience, tc.insecure)
			if tc.wantSubstr == "" {
				if got != "" {
					t.Fatalf("expected valid, got error: %q", got)
				}
				return
			}
			if !strings.Contains(got, tc.wantSubstr) {
				t.Fatalf("error message %q does not contain %q", got, tc.wantSubstr)
			}
		})
	}
}
