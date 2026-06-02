package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHasSkipMarker(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{
			name: "no comments at all",
			body: "apiVersion: v1\nkind: ConfigMap\n",
			want: false,
		},
		{
			name: "leading comment without marker",
			body: "# just a description\napiVersion: v1\n",
			want: false,
		},
		{
			name: "marker on first line",
			body: "# verify-samples: skip\napiVersion: v1\n",
			want: true,
		},
		{
			name: "marker after blank lines + other comments",
			body: "\n\n# description\n# verify-samples: skip\n# more\napiVersion: v1\n",
			want: true,
		},
		{
			name: "marker after non-comment line is ignored",
			body: "apiVersion: v1\n# verify-samples: skip\n",
			want: false,
		},
		{
			name: "wrong marker form (extra trailing word) is not honored",
			body: "# verify-samples: skip-it\napiVersion: v1\n",
			want: false,
		},
		{
			name: "marker with surrounding whitespace is honored (TrimSpace)",
			body: "   # verify-samples: skip   \napiVersion: v1\n",
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hasSkipMarker([]byte(tc.body))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("hasSkipMarker(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

// TestHasSkipMarkerScannerError ensures that a top-of-file line larger than
// bufio.Scanner's default token buffer (64 KiB) surfaces as an error
// instead of silently treating the file as "not skipped". The gate should
// fail loudly rather than guess.
func TestHasSkipMarkerScannerError(t *testing.T) {
	// One comment line, 80 KiB of '#', no newline — exceeds bufio.Scanner's
	// default 64 KiB token limit. bufio.Scanner returns ErrTooLong.
	huge := []byte("#" + strings.Repeat("a", 80*1024))
	_, err := hasSkipMarker(huge)
	if err == nil {
		t.Fatal("expected scanner error for oversized leading line, got nil")
	}
}

func TestListSamplesFindsYAMLAndSorts(t *testing.T) {
	dir := t.TempDir()
	must := func(p string, body string) {
		t.Helper()
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	// Mix of .yaml, .yml, a nested dir, and non-YAML files. The walker
	// should pick up both YAML extensions, recurse into subdirs, and
	// ignore the README/JSON.
	must("a.yaml", "apiVersion: v1\n")
	must("z.yml", "apiVersion: v1\n")
	must("README.md", "not a sample")
	must("config.json", "{}")
	must("sub/b.yaml", "apiVersion: v1\n")

	got, err := listSamples(dir)
	if err != nil {
		t.Fatalf("listSamples: %v", err)
	}

	want := []string{
		filepath.Join(dir, "a.yaml"),
		filepath.Join(dir, "sub", "b.yaml"),
		filepath.Join(dir, "z.yml"),
	}
	if len(got) != len(want) {
		t.Fatalf("got %d files, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestListSamplesPropagatesWalkError(t *testing.T) {
	// Pointing at a non-existent path bubbles the filepath.Walk error.
	_, err := listSamples(filepath.Join(t.TempDir(), "does", "not", "exist"))
	if err == nil {
		t.Fatal("expected error for missing dir, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
}

func TestIndentNormalizesTrailingNewlines(t *testing.T) {
	for _, tc := range []struct {
		name   string
		in     string
		prefix string
		want   string
	}{
		{"empty", "", "    ", ""},
		{"single line, no trailing nl", "foo", ">>", ">>foo\n"},
		{"single line, with trailing nl", "foo\n", ">>", ">>foo\n"},
		{"multiline, multiple trailing nls", "foo\nbar\n\n", "  ", "  foo\n  bar\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := indent(tc.in, tc.prefix); got != tc.want {
				t.Fatalf("indent(%q,%q) = %q, want %q", tc.in, tc.prefix, got, tc.want)
			}
		})
	}
}
