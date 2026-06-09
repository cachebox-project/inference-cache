// Targeted tests for the pure helpers that produce the harness's reported
// numbers. The harness is operator tooling and excluded from coverage (see
// COVER_EXCLUDE in the Makefile), but the encodeHash + humanBytes + sizing
// math drive the bytes-per-entry denominators the sizing guide cites — so
// a silent regression there would publish misleading sizing numbers. These
// tests pin the cases that matter without trying to test main()'s flag-
// validation flow (that requires process-level exit assertions and is
// covered manually via the in-tree "go run ./hack/index-sizing -flag=…"
// sweeps the maintainer runs to refresh the guide).
package main

import (
	"bytes"
	"testing"
)

func TestEncodeHash(t *testing.T) {
	// Distinct inputs must produce distinct outputs across the harness's
	// realistic key range; a collision would silently inflate the entry
	// counter and deflate bytes/entry — exactly the failure mode this test
	// guards against.
	cases := []struct {
		name string
		n    int
		size int
	}{
		{"zero-32B", 0, 32},
		{"one-32B", 1, 32},
		{"vllm-8B", 1, 8},
		{"large-index-32B", 1_000_000, 32},
		{"min-hash-8B", 0xff, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := encodeHash(tc.n, tc.size)
			if len(h) != tc.size {
				t.Fatalf("encodeHash(%d, %d) len=%d, want %d", tc.n, tc.size, len(h), tc.size)
			}
		})
	}

	// Distinctness sweep: the first 2^14 (16384) indices must produce 2^14
	// distinct 8-byte hashes — small enough to run fast, large enough that
	// a wrong pack (e.g. truncating before bytes ran out, or zeroing the
	// wrong byte position) would collide within the sweep.
	seen := make(map[string]struct{}, 1<<14)
	for i := 0; i < 1<<14; i++ {
		h := encodeHash(i, 8)
		k := string(h)
		if _, dup := seen[k]; dup {
			t.Fatalf("encodeHash collision at index %d", i)
		}
		seen[k] = struct{}{}
	}

	// Padding above 8 bytes must be zero — the leading 8 bytes carry the
	// index, the rest stay zero. A regression that filled the tail with
	// random bytes would change measured per-entry cost on wider hashes.
	tail := encodeHash(0xdeadbeef, 32)[8:]
	zero := make([]byte, 24)
	if !bytes.Equal(tail, zero) {
		t.Fatalf("encodeHash(_, 32) tail not zero-padded: %x", tail)
	}
}

func TestHumanBytes(t *testing.T) {
	// humanBytes shows up in every harness report line — wrong units here
	// would mis-quote the guide's measurements. Test the boundary jumps
	// (B → KiB, KiB → MiB, MiB → GiB) plus a "just below the next unit"
	// case to catch off-by-one regressions on the threshold checks.
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.00 KiB"},
		{1024*1024 - 1, "1024.00 KiB"}, // just under MiB threshold
		{1024 * 1024, "1.00 MiB"},
		{500 * 1024 * 1024, "500.00 MiB"},
		{1024 * 1024 * 1024, "1.00 GiB"},
		{2 * 1024 * 1024 * 1024, "2.00 GiB"},
	}
	for _, tc := range cases {
		got := humanBytes(tc.in)
		if got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
