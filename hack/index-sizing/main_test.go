// Targeted tests for the helpers that produce the harness's reported numbers.
// The harness is operator tooling and excluded from coverage (see COVER_EXCLUDE
// in the Makefile), but encodeHash, humanBytes, and planRun drive the
// bytes-per-entry denominators and the cap input the sizing guide cites — a
// silent regression here would publish misleading sizing numbers. Tests pin
// the cases that matter; the main()-level glue (flag.Parse + exit codes) is
// covered manually via the in-tree "go run ./hack/index-sizing -flag=…"
// sweeps the maintainer runs to refresh the guide.
package main

import (
	"bytes"
	"strings"
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

func TestPlanRunHappyPaths(t *testing.T) {
	// Each case exercises a different combinator: pure single-tenant single-
	// replica, multi-tenant split, multi-replica fan-out, and the warning-
	// without-error truncated-divisor case. Verifies that the derived
	// numbers (ingestedKeys + totalEntries) match what the harness loop
	// would actually produce — these are the per-entry denominators the
	// guide publishes.
	cases := []struct {
		name                            string
		keys, replicas, tenants, models int
		wantKeysPerBucket, wantIngested int
		wantTotalEntries                int
		wantTruncated                   bool
	}{
		{"single-tenant", 1_000_000, 1, 1, 1, 1_000_000, 1_000_000, 1_000_000, false},
		{"multi-tenant-clean", 1_000_000, 1, 4, 1, 250_000, 1_000_000, 1_000_000, false},
		{"multi-replica", 500_000, 3, 1, 1, 500_000, 500_000, 1_500_000, false},
		{"truncated", 1_000_000, 1, 3, 1, 333_333, 999_999, 999_999, true},
		{"tenants-models-cross-product", 600, 1, 5, 3, 40, 600, 600, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planRun(tc.keys, tc.replicas, 32, tc.tenants, tc.models, 1_000)
			if err != nil {
				t.Fatalf("planRun(...) returned error: %v", err)
			}
			if plan.KeysPerBucket != tc.wantKeysPerBucket {
				t.Errorf("KeysPerBucket = %d, want %d", plan.KeysPerBucket, tc.wantKeysPerBucket)
			}
			if plan.IngestedKeys != tc.wantIngested {
				t.Errorf("IngestedKeys = %d, want %d", plan.IngestedKeys, tc.wantIngested)
			}
			if plan.TotalEntries != tc.wantTotalEntries {
				t.Errorf("TotalEntries = %d, want %d", plan.TotalEntries, tc.wantTotalEntries)
			}
			if plan.Truncated != tc.wantTruncated {
				t.Errorf("Truncated = %v, want %v", plan.Truncated, tc.wantTruncated)
			}
		})
	}
}

func TestPlanRunRejections(t *testing.T) {
	// Each case targets a distinct guard inside planRun. The asserted
	// substring lets the test catch a regression where the right branch
	// is taken but the wrong error is reported.
	cases := []struct {
		name                              string
		keys, replicas, hashSize, tenants int
		models, batchSize                 int
		wantSubstr                        string
	}{
		{"zero-keys", 0, 1, 32, 1, 1, 1_000, "strictly positive"},
		{"zero-replicas", 100, 0, 32, 1, 1, 1_000, "strictly positive"},
		{"zero-tenants", 100, 1, 32, 0, 1, 1_000, "strictly positive"},
		{"zero-models", 100, 1, 32, 1, 0, 1_000, "strictly positive"},
		{"zero-batch", 100, 1, 32, 1, 1, 0, "strictly positive"},
		{"negative-keys", -1, 1, 32, 1, 1, 1_000, "strictly positive"},
		{"narrow-hash", 100, 1, 4, 1, 1, 1_000, "hash-bytes must be >= 8"},
		{"keys-less-than-buckets", 5, 1, 32, 10, 2, 1_000, "need at least one key per bucket"},
		{"overflow-replicas", 1_000_000, 1 << 62, 32, 1, 1, 1_000, "would overflow int"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := planRun(tc.keys, tc.replicas, tc.hashSize, tc.tenants, tc.models, tc.batchSize)
			if err == nil {
				t.Fatalf("planRun(...) succeeded, want error containing %q", tc.wantSubstr)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("planRun error = %q, want substring %q", err.Error(), tc.wantSubstr)
			}
		})
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
