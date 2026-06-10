package fingerprint

import "testing"

// Golden values generated from the SMG construction via Python `xxhash`
// (xxh3_64, seed 1337) — see /tmp/gen_golden.py. Go's zeebo/xxh3 reproducing
// these proves cross-language (subscriber ↔ proxy) and cross-impl (↔ SMG)
// parity on the canonical XXH3-64.

func TestContentHashGolden(t *testing.T) {
	cases := []struct {
		name   string
		tokens []uint32
		want   uint64
	}{
		{"block[1..16]", seq(1, 16), 16863443419780771464},
		{"block[17..32]", seq(17, 16), 2287610619914608821},
		{"block[100..115]", seq(100, 16), 10823191264391160519},
	}
	for _, c := range cases {
		if got := ContentHash(c.tokens); got != c.want {
			t.Errorf("ContentHash(%s) = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestPrefixHashesGolden(t *testing.T) {
	cases := []struct {
		name      string
		tokens    []uint32
		blockSize int
		want      []uint64
	}{
		{"seq_1_32_bs16", seq(1, 32), 16, []uint64{16863443419780771464, 12466389667045779788}},
		{"seq_100_119_bs16", seq(100, 20), 16, []uint64{10823191264391160519}}, // 4 tokens discarded
		{"seq_0_63_bs16", seq(0, 64), 16, []uint64{
			15310707395893867146, 13769157705258532664, 11879827756109914528, 3888788807566526800,
		}},
		{"partial_only", seq(0, 10), 16, nil},
		{"blocksize_zero", seq(0, 32), 0, nil},
	}
	for _, c := range cases {
		got := PrefixHashes(c.tokens, c.blockSize)
		if len(got) != len(c.want) {
			t.Errorf("%s: len %d (%v), want %d (%v)", c.name, len(got), got, len(c.want), c.want)
			continue
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Errorf("%s[%d] = %d, want %d", c.name, i, got[i], c.want[i])
			}
		}
	}
}

// prefix_hash[0] must equal content_hash[0] (fresh sequence, no parent).
func TestPrefixZeroEqualsContent(t *testing.T) {
	toks := seq(7, 16)
	if PrefixHashes(toks, 16)[0] != ContentHash(toks) {
		t.Fatal("prefix_hash[0] != content_hash[0]")
	}
}

// A parent chain must make block 0 differ from the fresh case (positionality).
func TestParentChainShiftsHash(t *testing.T) {
	toks := seq(50, 16)
	fresh := PrefixHashes(toks, 16)[0]
	chained := PrefixHashesFrom(toks, 16, 0xDEADBEEF, true)[0]
	if fresh == chained {
		t.Fatal("parent chaining did not change block-0 hash; positionality broken")
	}
}

// seq returns [start, start+1, ..., start+n-1] as uint32.
func seq(start, n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(start + i)
	}
	return out
}
