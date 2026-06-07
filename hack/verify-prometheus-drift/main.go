// Package main is the verify-prometheus-drift CI helper. It asserts that the
// `spec.groups` body of the PrometheusRule CR YAML file is byte-equivalent
// (after canonical JSON normalization) to the `groups` body of the flat
// Prometheus rules file. The two files in config/observability/ ship the
// same alert set in two different shapes (flat for vanilla Prometheus,
// PrometheusRule CR for prometheus-operator installs); they MUST stay in
// sync, and this check is the gate that proves they do.
//
// Usage:
//
//	verify-prometheus-drift <flat-rules.yaml> <prometheus-rule.yaml>
//
// On mismatch the tool exits non-zero and prints a line-by-line unified-style
// diff so the divergent groups are immediately visible — not just a
// "they differ" message.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type flatRules struct {
	Groups []any `yaml:"groups"`
}

type prometheusRule struct {
	Spec struct {
		Groups []any `yaml:"groups"`
	} `yaml:"spec"`
}

func canonical(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// firstDiffLine reports the 1-based line index of the first divergent line
// between a and b, or -1 if identical. Used to drive the diff print so the
// operator can jump straight to the offending hunk.
func firstDiffLine(a, b []string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i + 1
		}
	}
	if len(a) != len(b) {
		// Lines all matched up to the shorter side; the divergence is at the
		// first position the longer side has and the shorter side does not.
		if len(a) < len(b) {
			return len(a) + 1
		}
		return len(b) + 1
	}
	return -1
}

// printDiff emits a minimal unified-style diff: a few lines of context
// before the first divergence on each side, then the divergent line and a
// bounded tail of lines after it, prefixed `-` (flat) / `+` (CR). Bounded
// so a fully-mangled file does not flood CI logs.
//
// `contextLines` is the number of unchanged lines printed before the
// divergence. `tailLines` is the maximum number of trailing lines printed
// AFTER (and not including) the divergent line itself — so the divergent
// line plus up to `tailLines` more print on each side.
func printDiff(w io.Writer, flatLines, crLines []string, contextLines, tailLines int) {
	diffStart := firstDiffLine(flatLines, crLines)
	if diffStart < 0 {
		return
	}
	startCtx := diffStart - contextLines
	if startCtx < 1 {
		startCtx = 1
	}
	endFlat := diffStart + tailLines
	if endFlat > len(flatLines) {
		endFlat = len(flatLines)
	}
	endCR := diffStart + tailLines
	if endCR > len(crLines) {
		endCR = len(crLines)
	}
	fmt.Fprintf(w, "  divergence first appears at canonical-JSON line %d (showing %d lines of context + divergent line + up to %d trailing lines):\n",
		diffStart, contextLines, tailLines)
	for i := startCtx; i < diffStart; i++ {
		fmt.Fprintf(w, "   %s\n", flatLines[i-1])
	}
	for i := diffStart; i <= endFlat; i++ {
		if i-1 < len(flatLines) {
			fmt.Fprintf(w, "  -%s\n", flatLines[i-1])
		}
	}
	for i := diffStart; i <= endCR; i++ {
		if i-1 < len(crLines) {
			fmt.Fprintf(w, "  +%s\n", crLines[i-1])
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: verify-prometheus-drift <flat-rules.yaml> <prometheus-rule.yaml>")
		os.Exit(2)
	}

	flatPath, crPath := os.Args[1], os.Args[2]

	flatBytes, err := os.ReadFile(flatPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", flatPath, err)
		os.Exit(2)
	}
	crBytes, err := os.ReadFile(crPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", crPath, err)
		os.Exit(2)
	}

	var flat flatRules
	if err := yaml.Unmarshal(flatBytes, &flat); err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", flatPath, err)
		os.Exit(2)
	}
	var cr prometheusRule
	if err := yaml.Unmarshal(crBytes, &cr); err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", crPath, err)
		os.Exit(2)
	}

	// Fail closed if either file accidentally gutted its rule body — a
	// PR that empties `groups:` would otherwise drift-check OK (both
	// empty) and silently ship an alertless bundle.
	if len(flat.Groups) == 0 {
		fmt.Fprintf(os.Stderr, "✗ %s has no rule groups — refusing to drift-check an empty rule set.\n", flatPath)
		os.Exit(1)
	}
	if len(cr.Spec.Groups) == 0 {
		fmt.Fprintf(os.Stderr, "✗ %s has no rule groups under spec.groups — refusing to drift-check an empty rule set.\n", crPath)
		os.Exit(1)
	}

	flatCanon, err := canonical(flat.Groups)
	if err != nil {
		fmt.Fprintf(os.Stderr, "canonicalize flat groups: %v\n", err)
		os.Exit(2)
	}
	crCanon, err := canonical(cr.Spec.Groups)
	if err != nil {
		fmt.Fprintf(os.Stderr, "canonicalize CR groups: %v\n", err)
		os.Exit(2)
	}

	if flatCanon != crCanon {
		fmt.Fprintln(os.Stderr, "✗ PrometheusRule CR spec.groups drifted from the flat rules file")
		fmt.Fprintf(os.Stderr, "  flat: %s\n  CR:   %s\n", flatPath, crPath)
		fmt.Fprintln(os.Stderr, "  Mirror every change to BOTH files. The flat file is the source of truth;")
		fmt.Fprintln(os.Stderr, "  promtool exercises it. The CR is what `kubectl apply -k config/observability` ships.")
		fmt.Fprintln(os.Stderr, "")
		printDiff(os.Stderr,
			strings.Split(flatCanon, "\n"),
			strings.Split(crCanon, "\n"),
			3,  // lines of context before
			20, // tail lines per side after the divergent line
		)
		os.Exit(1)
	}
}
