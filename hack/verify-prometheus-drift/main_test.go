// Pure-function unit tests for the drift helper. The `make verify-prometheus`
// target exercises the happy path against the live rules files; these tests
// pin the failure-path behavior (first-divergence detection and bounded
// diff output) without needing to fork a subprocess.

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestFirstDiffLine(t *testing.T) {
	cases := []struct {
		name string
		a, b []string
		want int
	}{
		{
			name: "identical",
			a:    []string{"x", "y", "z"},
			b:    []string{"x", "y", "z"},
			want: -1,
		},
		{
			name: "diverges at middle",
			a:    []string{"x", "Y", "z"},
			b:    []string{"x", "y", "z"},
			want: 2,
		},
		{
			name: "diverges at line 1",
			a:    []string{"a"},
			b:    []string{"b"},
			want: 1,
		},
		{
			name: "a shorter than b — divergence is the first extra line on b",
			a:    []string{"x", "y"},
			b:    []string{"x", "y", "z"},
			want: 3,
		},
		{
			name: "b shorter than a — divergence is the first extra line on a",
			a:    []string{"x", "y", "z"},
			b:    []string{"x", "y"},
			want: 3,
		},
		{
			name: "both empty",
			a:    []string{},
			b:    []string{},
			want: -1,
		},
		{
			name: "one empty",
			a:    []string{"x"},
			b:    []string{},
			want: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := firstDiffLine(tc.a, tc.b); got != tc.want {
				t.Fatalf("firstDiffLine(a, b) = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCanonicalProducesStableOrdering(t *testing.T) {
	// Two maps with different insertion order should canonicalize to the
	// same string — proving the drift check is map-order-insensitive (yaml
	// unmarshal is, but JSON marshal would not be without map-key sorting,
	// which encoding/json does for us).
	a := map[string]any{"b": 1, "a": 2}
	b := map[string]any{"a": 2, "b": 1}
	ca, err := canonical(a)
	if err != nil {
		t.Fatalf("canonical(a): %v", err)
	}
	cb, err := canonical(b)
	if err != nil {
		t.Fatalf("canonical(b): %v", err)
	}
	if ca != cb {
		t.Fatalf("canonical differs across map orderings:\n  %s\n  vs\n  %s", ca, cb)
	}
}

func TestPrintDiffSkipsWhenIdentical(t *testing.T) {
	var buf bytes.Buffer
	printDiff(&buf, []string{"x", "y"}, []string{"x", "y"}, 3, 10)
	if got := buf.String(); got != "" {
		t.Fatalf("printDiff on identical inputs should be empty, got: %q", got)
	}
}

func TestPrintDiffShowsDivergence(t *testing.T) {
	var buf bytes.Buffer
	printDiff(&buf,
		[]string{"a", "b", "c", "d", "e"},
		[]string{"a", "b", "C", "d", "e"},
		1, // context lines before
		1, // tail lines after — emits the divergent line (3) plus one more (4)
	)
	got := buf.String()
	// Confirm the divergent lines from both sides appear, prefixed `-`/`+`.
	for _, want := range []string{"-c", "+C"} {
		if !strings.Contains(got, want) {
			t.Fatalf("printDiff output missing %q\nfull output:\n%s", want, got)
		}
	}
	// Bound check — with tailLines=1 and divergence at line 3, output
	// extends to line 4 (`d`) but NOT line 5 (`e`).
	if strings.Contains(got, "-e") || strings.Contains(got, "+e") {
		t.Fatalf("printDiff exceeded maxLines bound: line `e` should not appear\nfull output:\n%s", got)
	}
}
