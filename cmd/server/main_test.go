package main

import (
	"strings"
	"testing"
)

// TestValidateControllerAuthFlags pins the operator-facing startup contract for
// the controller-facing auth flag combinations that gate /snapshot, /policy,
// and /probe. Each row is a real shape an operator might wire — including the
// failure modes the validation exists to catch (silent unauth, audience-empty
// bypass, whitespace-bracketed values, both knobs on at once).
//
// The diagnostic message is asserted only by substring (the user-visible cue
// the failure points at) so the wording can evolve without breaking the
// contract test.
func TestValidateControllerAuthFlags(t *testing.T) {
	const (
		ctrlSA    = "system:serviceaccount:inference-cache-system:inference-cache-controller-manager"
		ctrlAud   = "inferencecache.io/controller"
		policyAud = "inferencecache.io/policy"
	)

	cases := []struct {
		name               string
		expectedSA         string
		controllerAudience string
		policyAudience     string
		insecure           bool
		wantSubstr         string // "" → expect valid (empty return)
	}{
		{
			name:               "production: SA set, both audiences set, insecure off",
			expectedSA:         ctrlSA,
			controllerAudience: ctrlAud,
			policyAudience:     policyAud,
			insecure:           false,
			wantSubstr:         "",
		},
		{
			name:               "local-dev: SA empty, insecure on",
			expectedSA:         "",
			controllerAudience: ctrlAud,   // audience flag values are ignored when auth is off
			policyAudience:     policyAud, // audience flag values are ignored when auth is off
			insecure:           true,
			wantSubstr:         "",
		},
		{
			name:               "mutually exclusive: SA set AND insecure on",
			expectedSA:         ctrlSA,
			controllerAudience: ctrlAud,
			policyAudience:     policyAud,
			insecure:           true,
			wantSubstr:         "mutually exclusive",
		},
		{
			name:               "silent unauth blocked: neither flag set",
			expectedSA:         "",
			controllerAudience: ctrlAud,
			policyAudience:     policyAud,
			insecure:           false,
			wantSubstr:         "missing --allowed-controller-sa",
		},
		{
			name:               "controller audience bypass blocked: SA set, audience empty",
			expectedSA:         ctrlSA,
			controllerAudience: "",
			policyAudience:     policyAud,
			insecure:           false,
			wantSubstr:         "--controller-audience cannot be empty",
		},
		{
			name:               "policy audience bypass blocked: SA set, policy audience empty",
			expectedSA:         ctrlSA,
			controllerAudience: ctrlAud,
			policyAudience:     "",
			insecure:           false,
			wantSubstr:         "--policy-audience cannot be empty",
		},
		{
			// Whitespace-only is the chart-value-pasted-with-yaml-indent
			// case. Treated identically to empty — it cannot match the
			// projected SA token's audience, so failing fast at startup
			// beats a runtime 401 storm after the controller poller has
			// been silently broken.
			name:               "controller audience bypass blocked: SA set, audience whitespace-only",
			expectedSA:         ctrlSA,
			controllerAudience: "   \t  ",
			policyAudience:     policyAud,
			insecure:           false,
			wantSubstr:         "--controller-audience cannot be empty",
		},
		{
			name:               "policy audience bypass blocked: SA set, audience whitespace-only",
			expectedSA:         ctrlSA,
			controllerAudience: ctrlAud,
			policyAudience:     "   \t  ",
			insecure:           false,
			wantSubstr:         "--policy-audience cannot be empty",
		},
		{
			// Leading/trailing whitespace around a real-looking value is
			// the "yaml quote pasted with extra spaces" case. The raw
			// string would be sent to TokenReviewSpec.Audiences as-is
			// and would not match the JWT-baked audience, so the runtime
			// would 401-loop while boot reported auth as enabled. Reject
			// with a fail-fast diagnostic that names the actual value
			// so the operator sees the extra whitespace.
			name:               "controller audience bypass blocked: SA set, audience has trailing whitespace",
			expectedSA:         ctrlSA,
			controllerAudience: "inferencecache.io/controller ",
			policyAudience:     policyAud,
			insecure:           false,
			wantSubstr:         "leading/trailing whitespace",
		},
		{
			name:               "policy audience bypass blocked: SA set, audience has trailing whitespace",
			expectedSA:         ctrlSA,
			controllerAudience: ctrlAud,
			policyAudience:     "inferencecache.io/policy ",
			insecure:           false,
			wantSubstr:         "leading/trailing whitespace",
		},
		{
			// Same whitespace failure mode as the audience case, but for
			// the SA flag: kube-apiserver returns a username with NO
			// whitespace ("system:serviceaccount:NS:NAME"), so a pasted
			// value with stray spaces would never match and every
			// controller scrape would 403. Fail fast at startup with
			// the value echoed so the operator sees the whitespace.
			name:               "SA bypass blocked: --allowed-controller-sa has trailing whitespace",
			expectedSA:         ctrlSA + " ",
			controllerAudience: "inferencecache.io/controller",
			policyAudience:     policyAud,
			insecure:           false,
			wantSubstr:         "--allowed-controller-sa has leading/trailing whitespace",
		},
		{
			name:               "audiences empty are fine when insecure is on (no audience check needed)",
			expectedSA:         "",
			controllerAudience: "",
			policyAudience:     "",
			insecure:           true,
			wantSubstr:         "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateControllerAuthFlags(tc.expectedSA, tc.controllerAudience, tc.policyAudience, tc.insecure)
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
