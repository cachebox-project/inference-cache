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
// Exits non-zero with a diff on mismatch.
package main

import (
	"encoding/json"
	"fmt"
	"os"

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
		// Print a brief head-of-canonical to make the diff identifiable.
		fmt.Fprintln(os.Stderr, "---- flat (canonical JSON, first 400 bytes) ----")
		fmt.Fprintln(os.Stderr, head(flatCanon, 400))
		fmt.Fprintln(os.Stderr, "---- CR   (canonical JSON, first 400 bytes) ----")
		fmt.Fprintln(os.Stderr, head(crCanon, 400))
		os.Exit(1)
	}
}

func head(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
