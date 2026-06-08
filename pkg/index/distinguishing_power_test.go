package index

import (
	"math"
	"testing"
)

// distinguishingPower folds the cluster-cardinality observation into the
// LookupRoute score (matched_tokens × freshness × distinguishing_power). The
// helper is the math leaf — its four boundary branches are tested here in
// isolation so the lookup-path tests can stay focused on the scoring
// composition rather than re-asserting the formula. See docs/design/
// lookuproute-ranking.md §2.7 (the replica-distinguishing-power factor) for
// the design rationale.

func TestDistinguishingPowerSingleReplica(t *testing.T) {
	// total <= 1: ratio is ill-defined; degrade gracefully to 1.0 so a
	// single-replica deployment keeps its baseline ranking exactly
	// (matched_tokens × freshness with no extra factor). Without this
	// carve-out distinguishingPower(1, 1) = 0 would zero EVERY score on a
	// single-replica deployment and downgrade every hint to NO_HINT — a
	// hard regression on the simplest possible cluster shape.
	if got := distinguishingPower(1, 1); got != 1.0 {
		t.Fatalf("distinguishingPower(1, 1) = %v, want 1.0", got)
	}
	if got := distinguishingPower(0, 1); got != 1.0 {
		t.Fatalf("distinguishingPower(0, 1) = %v, want 1.0 (edge: total=1 always degrades to 1)", got)
	}
	if got := distinguishingPower(0, 0); got != 1.0 {
		t.Fatalf("distinguishingPower(0, 0) = %v, want 1.0 (edge: total<=1 always degrades to 1)", got)
	}
}

func TestDistinguishingPowerAllMatchedYieldsZero(t *testing.T) {
	// The signature case the ticket calls out: every replica holds the
	// prefix (chat template, RAG header, system prompt). The overlap does
	// not distinguish anyone, so the factor collapses to zero so the
	// post-score floor downgrades the response to NO_HINT.
	if got := distinguishingPower(3, 3); got != 0.0 {
		t.Fatalf("distinguishingPower(3, 3) = %v, want 0.0", got)
	}
	if got := distinguishingPower(10, 10); got != 0.0 {
		t.Fatalf("distinguishingPower(10, 10) = %v, want 0.0", got)
	}
}

func TestDistinguishingPowerOneOfThree(t *testing.T) {
	// Specific RAG context held by only one of three replicas: maximally
	// useful for routing — factor must be 1 - 1/3 ≈ 0.667.
	got := distinguishingPower(1, 3)
	want := 2.0 / 3.0
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("distinguishingPower(1, 3) = %v, want %v", got, want)
	}
}

func TestDistinguishingPowerTwoOfThree(t *testing.T) {
	// Partial diffusion: 2 of 3 replicas have it → factor 1/3 ≈ 0.333. The
	// ranker still surfaces the match (above the floor for non-trivial
	// matched_tokens) but weighted less than a unique match.
	got := distinguishingPower(2, 3)
	want := 1.0 / 3.0
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("distinguishingPower(2, 3) = %v, want %v", got, want)
	}
}

func TestDistinguishingPowerClampsMatchingOverTotal(t *testing.T) {
	// Defensive: a buggy caller passing matching > total would yield a
	// NEGATIVE factor and silently invert ranking. Clamp to 0 — the same
	// conservative interpretation as "every replica has it" rather than a
	// negative weight that would surface inverted hints.
	if got := distinguishingPower(5, 3); got != 0.0 {
		t.Fatalf("distinguishingPower(5, 3) = %v, want 0.0 (clamp)", got)
	}
}

func TestDistinguishingPowerNegativeInputsClampedToOne(t *testing.T) {
	// A negative `matching` count can only come from a buggy caller (the
	// production paths derive it from len(...) which is non-negative).
	// Treat it as "no matches" and return the maximum factor 1.0 — same
	// shape as total<=1 — rather than amplifying scores above their
	// matched_tokens × freshness baseline.
	if got := distinguishingPower(-1, 3); got != 1.0 {
		t.Fatalf("distinguishingPower(-1, 3) = %v, want 1.0 (clamp)", got)
	}
}
