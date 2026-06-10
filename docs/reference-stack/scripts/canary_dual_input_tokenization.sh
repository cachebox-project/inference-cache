#!/usr/bin/env bash
# By-construction canary for server-side tokenization: prove a vLLM engine
# prefix-caches exactly the token-ID prompt it is handed, so a routing
# fingerprint computed over those same tokens is guaranteed to match the engine's
# cached prefix.
#
# This isolates the ENGINE half of the guarantee. The other halves are covered
# by their own tests: the tokenizer (pkg/tokenize, verified against a real HF
# tokenizer) and the content fingerprint (pkg/fingerprint, golden tests). The
# Go test TestPrefixCacheCanaryLive drives pkg/adapters/engineclient: it sends a long
# token-ID prompt to /v1/completions twice and asserts vLLM's
# prefix_cache_hits_total rises on the warm (identical) request.
#
#   ./canary_dual_input_tokenization.sh
#   ENGINE_PORT=8000 MODEL=Qwen/Qwen2.5-0.5B-Instruct ./canary_dual_input_tokenization.sh
#
# Note: vLLM CPU image pulls fail under a VPN that does TLS interception
# (x509: certificate signed by unknown authority) — disconnect the VPN to pull.
set -euo pipefail

arch="$(uname -m)"
case "$arch" in
  arm64|aarch64) IMAGE_TAG="${IMAGE_TAG:-latest-arm64}" ;;
  *)             IMAGE_TAG="${IMAGE_TAG:-latest-x86_64}" ;;
esac
IMAGE="${IMAGE:-vllm/vllm-openai-cpu:$IMAGE_TAG}"
MODEL="${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}"
ENGINE_PORT="${ENGINE_PORT:-8000}"
KVCACHE_SPACE="${KVCACHE_SPACE:-4}"
ENGINE_TIMEOUT="${ENGINE_TIMEOUT:-600}"
PROMPT_TOKENS="${PROMPT_TOKENS:-2048}"
CONTAINER="${CONTAINER:-vllm-cpu-dualinput-canary}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

log() { echo "[canary] $*"; }
fail() { echo "[canary] FAIL: $*" >&2; exit 1; }

# Reuse an already-running engine if IC_ENGINE_URL is set; otherwise start one.
started_engine=""
cleanup() {
  if [ -n "$started_engine" ]; then docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

if [ -n "${IC_ENGINE_URL:-}" ]; then
  log "using existing engine at $IC_ENGINE_URL"
else
  log "starting vLLM CPU engine ($IMAGE) with prefix caching"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER" \
    -p "$ENGINE_PORT:8000" \
    -e VLLM_CPU_KVCACHE_SPACE="$KVCACHE_SPACE" --shm-size=4g \
    "$IMAGE" \
    --model "$MODEL" --dtype bfloat16 --max-model-len 8192 --enforce-eager --enable-prefix-caching \
    >/dev/null
  started_engine="1"

  log "waiting for engine /health (up to ${ENGINE_TIMEOUT}s; CPU model load is slow)"
  deadline=$(( $(date +%s) + ENGINE_TIMEOUT ))
  until curl -sf -o /dev/null "http://localhost:$ENGINE_PORT/health"; do
    docker ps --filter "name=$CONTAINER" --format '{{.Names}}' | grep -q "$CONTAINER" \
      || { docker logs --tail 25 "$CONTAINER" || true; fail "engine container exited during startup"; }
    [ "$(date +%s)" -lt "$deadline" ] || fail "engine not healthy within ${ENGINE_TIMEOUT}s"
    sleep 4
  done
  log "engine healthy"
  export IC_ENGINE_URL="http://localhost:$ENGINE_PORT"
  export IC_ENGINE_MODEL="$MODEL"
fi

: "${IC_ENGINE_MODEL:=$MODEL}"
export IC_ENGINE_MODEL
export IC_ENGINE_PROMPT_TOKENS="$PROMPT_TOKENS"

log "running the by-construction canary (token-ID prompt prefix-cache hit)"
go test ./pkg/adapters/engineclient/ -run TestPrefixCacheCanaryLive -count=1 -v \
  || fail "canary failed: the engine did not prefix-cache the token-ID prompt"

log "PASS: engine prefix-cached the token-ID prompt — routing fingerprint matches by construction"
