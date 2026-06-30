package main

import (
	"io"
	"testing"
)

// TestDoctorRejectsPositionalArgs verifies cobra.NoArgs is wired: a stray
// positional argument (e.g. a namespace typed without -n) is rejected before
// RunE runs, rather than silently ignored. Args validation happens ahead of
// RunE, so this never reaches Kubernetes client construction.
func TestDoctorRejectsPositionalArgs(t *testing.T) {
	var code int
	root := newRootCommand(&code)
	root.SetArgs([]string{"doctor", "stray-arg"})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	if err := root.Execute(); err == nil {
		t.Fatal("expected an error for a stray positional arg, got nil")
	}
}
