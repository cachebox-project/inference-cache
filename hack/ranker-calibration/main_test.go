// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunGenerateAndCheck(t *testing.T) {
	tracePath := filepath.Join("..", "..", "internal", "index", "calibration", "testdata", "c1_synthetic_trace.json")
	outPath := filepath.Join(t.TempDir(), "result.json")
	var stdout, stderr bytes.Buffer

	if code := run([]string{"-trace", tracePath, "-out", outPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("generate exit = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "wrote ranker calibration") {
		t.Fatalf("generate stdout = %q", stdout.String())
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(outPath), ".result.json.tmp-*")); err != nil || len(matches) != 0 {
		t.Fatalf("temporary results after generation = %v, err = %v", matches, err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-trace", tracePath, "-out", outPath, "-check"}, &stdout, &stderr); code != 0 {
		t.Fatalf("current check exit = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "ranker calibration is current") {
		t.Fatalf("check stdout = %q", stdout.String())
	}

	if err := os.WriteFile(outPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write stale result: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-trace", tracePath, "-out", outPath, "-check"}, &stdout, &stderr); code != 1 {
		t.Fatalf("stale check exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "is stale") {
		t.Fatalf("stale check stderr = %q", stderr.String())
	}
}

func TestRunRejectsMalformedTrace(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.json")
	if err := os.WriteFile(tracePath, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write malformed trace: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-trace", tracePath, "-out", filepath.Join(dir, "result.json")}, &stdout, &stderr); code != 1 {
		t.Fatalf("malformed trace exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "load trace") {
		t.Fatalf("malformed trace stderr = %q", stderr.String())
	}
}

func TestRunRequiresPaths(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 1 {
		t.Fatalf("missing paths exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "both -trace and -out are required") {
		t.Fatalf("missing paths stderr = %q", stderr.String())
	}
}
