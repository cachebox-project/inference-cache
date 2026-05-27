#!/usr/bin/env bash
# Demonstrate a prefix-cache hit: fire the same long (~8K-token) system prefix
# twice and show the second request is faster (prefill skipped) plus the cache
# counters move. Satisfies the DoD: "second identical-prefix request shows a
# cache hit (lower TTFT, prefill skipped)".
#
#   ./prefix_cache_hit_test.sh                       # defaults to localhost:30080
#   BASE=http://localhost:8000 MODEL=meta-llama/Llama-3.1-8B-Instruct ./prefix_cache_hit_test.sh
#
# Reads vLLM's own Prometheus counters before/after for the authoritative signal:
#   vllm:prefix_cache_queries_total / vllm:prefix_cache_hits_total
set -euo pipefail

BASE="${BASE:-http://localhost:30080}"
MODEL="${MODEL:-meta-llama/Llama-3.1-8B-Instruct}"

# A long, shared prefix. Repeat a sentence to push past the prefix-cache block
# threshold; the actual question is appended after it.
PREFIX="$(python3 - <<'PY'
print(("You are a meticulous assistant. Follow the rules precisely. " * 700).strip())
PY
)"

hits() { curl -s "$BASE/metrics" | awk '/^vllm:prefix_cache_hits_total/{s+=$2} END{print s+0}'; }
queries() { curl -s "$BASE/metrics" | awk '/^vllm:prefix_cache_queries_total/{s+=$2} END{print s+0}'; }

fire() {
  local q="$1"
  curl -s -o /dev/null -w '%{time_total}' "$BASE/v1/chat/completions" \
    -H 'Content-Type: application/json' \
    -d "$(python3 - "$PREFIX" "$q" "$MODEL" <<'PY'
import json, sys
prefix, q, model = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({
  "model": model,
  "max_tokens": 16,
  "temperature": 0,
  "messages": [{"role": "system", "content": prefix},
               {"role": "user", "content": q}],
}))
PY
)"
}

echo "base=$BASE model=$MODEL"
h0=$(hits); q0=$(queries)
echo "== request 1 (cold prefix) =="
t1=$(fire "Summarize the rules in one word.")
echo "  wall time: ${t1}s"
echo "== request 2 (same prefix, should hit) =="
t2=$(fire "Now summarize them in two words.")
echo "  wall time: ${t2}s"
h1=$(hits); q1=$(queries)

echo
echo "prefix_cache_queries delta: $((q1 - q0))"
echo "prefix_cache_hits    delta: $((h1 - h0))"
echo "(expect hits delta > 0 on request 2, and t2 < t1)"
