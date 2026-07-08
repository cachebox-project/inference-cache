#!/usr/bin/env bash
# Full-chain CPU canary for the cache substrate, BINARY data-path edition.
# Brings up the engine + server + subscriber as host processes (no Kubernetes
# admission in the loop) and asserts the binaries wire correctly:
#
#   vLLM engine --ZMQ KV events--> kvevent-subscriber --gRPC--> policy server --> index
#
# The subscriber is hand-launched on purpose here: this canary's job is to
# pin the binaries' wire protocol. The in-cluster auto-attach path (the
# webhook injects the subscriber sidecar onto labeled engine pods) is gated
# by the webhook envtest (internal/webhook/pod/envtest_integration_test.go) —
# see docs/design/kvevent-subscriber-wiring.md for the shape decision.
#
# Checks (exit 0 = PASS, 1 = FAIL):
#   1. Engine prefix-cache hit      — vllm:prefix_cache_hits_total increases
#   2. Subscriber -> server -> index — inferencecache_index_entries{model=...} > 0
#
# On-demand canary (not a blocking CI gate): it needs Docker, pulls the vLLM CPU
# image (multi-GB), and loads a model (~1-2 min). The Docker VM needs enough RAM —
# the vLLM CPU runtime baseline is ~5 GiB before the KV cache; ~10+ GiB total is
# comfortable for the 0.5B model with a 4 GiB KV cache. See VERSIONS.md.
#
# Usage:  docs/reference-stack/scripts/canary_e2e.sh
# Tunables via env: IMAGE, MODEL, MODEL_ID, *_PORT, KVCACHE_SPACE, ENGINE_TIMEOUT.
set -euo pipefail

# --- config -----------------------------------------------------------------
arch="$(uname -m)"
case "$arch" in
  arm64|aarch64) IMAGE_TAG="${IMAGE_TAG:-latest-arm64}" ;;
  *)             IMAGE_TAG="${IMAGE_TAG:-latest-x86_64}" ;;
esac
IMAGE="${IMAGE:-vllm/vllm-openai-cpu:$IMAGE_TAG}"
MODEL="${MODEL:-Qwen/Qwen2.5-0.5B-Instruct}"
MODEL_ID="${MODEL_ID:-canary}"        # the model label the subscriber tags + we assert on
HASH_SCHEME="${HASH_SCHEME:-vllm}"
REPLICA_ID="${REPLICA_ID:-canary-vllm-0}"

ENGINE_PORT="${ENGINE_PORT:-8000}"
ZMQ_PORT="${ZMQ_PORT:-5557}"
ZMQ_REPLAY_PORT="${ZMQ_REPLAY_PORT:-5558}"
SERVER_GRPC_PORT="${SERVER_GRPC_PORT:-9090}"
SERVER_HTTP_PORT="${SERVER_HTTP_PORT:-8080}"
KVCACHE_SPACE="${KVCACHE_SPACE:-4}"
ENGINE_TIMEOUT="${ENGINE_TIMEOUT:-600}"   # seconds to wait for engine /health
CONTAINER="${CONTAINER:-vllm-cpu-canary}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

server_pid=""
sub_pid=""
log() { echo "[canary] $*"; }
fail() { echo "[canary] FAIL: $*" >&2; exit 1; }

cleanup() {
  if [ -n "$sub_pid" ]; then kill "$sub_pid" 2>/dev/null || true; fi
  if [ -n "$server_pid" ]; then kill "$server_pid" 2>/dev/null || true; fi
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- build the binaries from source ----------------------------------------
# Use `make build` so the bin/ dir + ldflags are handled consistently with the
# rest of the repo (a fresh checkout doesn't have bin/, which is git-ignored).
log "building server + kvevent-subscriber"
make build

# --- start the CPU engine ---------------------------------------------------
log "starting vLLM CPU engine ($IMAGE)"
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" \
  -p "$ENGINE_PORT:8000" -p "$ZMQ_PORT:5557" -p "$ZMQ_REPLAY_PORT:5558" \
  -e VLLM_CPU_KVCACHE_SPACE="$KVCACHE_SPACE" --shm-size=4g \
  "$IMAGE" \
  --model "$MODEL" --dtype bfloat16 --max-model-len 8192 --enforce-eager --enable-prefix-caching \
  --kv-events-config "{\"enable_kv_cache_events\":true,\"publisher\":\"zmq\",\"endpoint\":\"tcp://*:5557\",\"replay_endpoint\":\"tcp://*:5558\",\"buffer_steps\":10000,\"topic\":\"kv-events\"}" \
  >/dev/null

log "waiting for engine /health (up to ${ENGINE_TIMEOUT}s; model load is slow on CPU)"
deadline=$(( $(date +%s) + ENGINE_TIMEOUT ))
until curl -sf -o /dev/null "http://localhost:$ENGINE_PORT/health"; do
  docker ps --filter "name=$CONTAINER" --format '{{.Names}}' | grep -q "$CONTAINER" \
    || { docker logs --tail 25 "$CONTAINER" || true; fail "engine container exited during startup"; }
  [ "$(date +%s)" -lt "$deadline" ] || fail "engine did not become healthy within ${ENGINE_TIMEOUT}s"
  sleep 4
done
log "engine healthy"

# --- start the policy server + subscriber ----------------------------------
# The server refuses to start unless controller-facing auth is configured:
# either --allowed-controller-sa (production, needs an apiserver to run
# TokenReview against) or the named --insecure-disable-auth escape hatch.
# This canary runs the server as a bare host process with no apiserver, and
# only drives the gRPC ingest + index path, so it uses the escape hatch.
log "starting policy server"
./bin/server --grpc-bind-address=":$SERVER_GRPC_PORT" --http-bind-address=":$SERVER_HTTP_PORT" \
  --insecure-disable-auth \
  >/tmp/canary-server.log 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://localhost:$SERVER_HTTP_PORT/readyz" && break
  sleep 1
done
curl -sf -o /dev/null "http://localhost:$SERVER_HTTP_PORT/readyz" || fail "server did not become ready"

log "starting kvevent-subscriber"
./bin/kvevent-subscriber \
  --engine-endpoint "tcp://127.0.0.1:$ZMQ_PORT" --topic kv-events \
  --server "127.0.0.1:$SERVER_GRPC_PORT" \
  --replica-id "$REPLICA_ID" --model-id "$MODEL_ID" --hash-scheme "$HASH_SCHEME" --window 50ms \
  >/tmp/canary-subscriber.log 2>&1 &
sub_pid=$!
sleep 3   # let the SUB socket connect before traffic

# --- drive prefix traffic ---------------------------------------------------
engine_hits() { curl -s "http://localhost:$ENGINE_PORT/metrics" | awk '/^vllm:prefix_cache_hits_total/{s+=$2} END{print s+0}'; }
index_entries() { curl -s "http://localhost:$SERVER_HTTP_PORT/metrics" | awk -v m="$MODEL_ID" '$0 ~ "inferencecache_index_entries.*model=\""m"\"" {s+=$NF} END{print s+0}'; }

PREFIX="$(python3 -c 'print(("You are a meticulous canary assistant. Follow the rules precisely. " * 200).strip())')"
fire() {
  curl -s -o /dev/null -w '%{http_code}' "http://localhost:$ENGINE_PORT/v1/chat/completions" \
    -H 'Content-Type: application/json' \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"model":sys.argv[3],"max_tokens":8,"temperature":0,"messages":[{"role":"system","content":sys.argv[1]},{"role":"user","content":sys.argv[2]}]}))' "$PREFIX" "$1" "$MODEL")"
}

h0=$(engine_hits)
log "request 1 (cold prefix): HTTP $(fire 'summarize in one word')"
log "request 2 (same prefix):  HTTP $(fire 'summarize in two words')"
sleep 3   # allow the subscriber to flush adds to the server
h1=$(engine_hits)
entries=$(index_entries)

# --- assert -----------------------------------------------------------------
log "prefix_cache_hits: $h0 -> $h1   |   index_entries{model=$MODEL_ID}: $entries"
[ "$h1" -gt "$h0" ] || fail "no engine prefix-cache hit (hits did not increase)"
[ "$entries" -gt 0 ] || { tail -5 /tmp/canary-subscriber.log || true; fail "index not populated (no events reached the server)"; }

log "PASS — engine prefix-cache hit observed and the index was populated end-to-end"
