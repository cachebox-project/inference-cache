#!/usr/bin/env python3
"""Subscribe to a vLLM KV-cache event stream over ZMQ and print/capture events.

This is the by-hand stand-in for what the C1 subscriber will do: connect to the
engine's ZMQ PUB endpoint, decode the msgpack EventBatch frames, and surface
BlockStored / BlockRemoved / AllBlocksCleared with their hashes.

Usage:
    pip install -r requirements.txt
    # kind (NodePort from kind/cluster.yaml) :
    python kv_events_subscriber.py --endpoint tcp://localhost:30557 --topic kv-events
    # or port-forward:
    #   kubectl -n cache-substrate port-forward svc/vllm-lmcache-llama-8b 5557:5557
    #   python kv_events_subscriber.py --endpoint tcp://localhost:5557 --topic kv-events

Capture a sample (DoD: "captured sample of the ZMQ event stream"):
    python kv_events_subscriber.py --endpoint tcp://localhost:30557 \
        --topic kv-events --max 200 --json > ../captures/kv-events-sample.jsonl

Wire format (vLLM ZmqEventPublisher): each message is a multipart frame
[topic, seq(8-byte big-endian), msgpack(EventBatch)]. EventBatch.events is a
tagged union of the three event structs (msgspec, array_like).
"""
import argparse
import json
import sys
from typing import Union

import zmq

try:
    import msgspec

    class _Event(msgspec.Struct, array_like=True, omit_defaults=True, tag=True):
        pass

    class BlockStored(_Event):
        block_hashes: list[int] = []
        parent_block_hash: "int | None" = None
        token_ids: list[int] = []
        block_size: int = 0
        lora_id: "int | None" = None

    class BlockRemoved(_Event):
        block_hashes: list[int] = []

    class AllBlocksCleared(_Event):
        pass

    # The events list is a tagged union; the field MUST be typed as the union so
    # msgspec dispatches on the tag (the struct name) rather than yielding raw lists.
    _KVEvent = Union[BlockStored, BlockRemoved, AllBlocksCleared]

    class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True):
        ts: float = 0.0
        events: list[_KVEvent] = []

    _DECODER = msgspec.msgpack.Decoder(type=EventBatch)
    _HAVE_MSGSPEC = True
except Exception:  # pragma: no cover - fallback path
    _HAVE_MSGSPEC = False


def _redact(fields: dict) -> dict:
    """Enforce the metadata-only contract: never surface prompt-derived content.

    vLLM's BlockStored carries token_ids (the actual prompt tokens). We keep only
    the count — hashes/counts/metadata are fine; token content is not (it would
    otherwise be persisted into the committed capture sample). Mirrors the
    server-side rule: CacheStateUpdate/PrefixEntry carry metadata only.
    """
    if "token_ids" in fields:
        fields["token_count"] = len(fields.pop("token_ids") or [])
    return fields


def _decode(payload: bytes):
    """Return (ts, [(event_type, fields_dict)]). Token content is redacted."""
    if _HAVE_MSGSPEC:
        try:
            batch = _DECODER.decode(payload)
            out = []
            for ev in batch.events:
                out.append((type(ev).__name__, _redact({f: getattr(ev, f) for f in ev.__struct_fields__})))
            return batch.ts, out
        except Exception:
            pass
    # Typed decode failed: emit shape only, never the raw (potentially
    # content-bearing) payload, so the metadata-only contract still holds.
    return None, [("UNDECODED", {"bytes": len(payload)})]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="tcp://localhost:30557")
    ap.add_argument("--topic", default="kv-events")
    ap.add_argument("--max", type=int, default=0, help="stop after N events (0 = run forever)")
    ap.add_argument("--json", action="store_true", help="emit one JSON object per event (JSONL)")
    args = ap.parse_args()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(args.endpoint)
    sock.setsockopt_string(zmq.SUBSCRIBE, args.topic)
    print(f"# subscribed to {args.endpoint} topic={args.topic!r}", file=sys.stderr)

    seen = 0
    try:
        while True:
            frames = sock.recv_multipart()
            payload = frames[-1]
            ts, events = _decode(payload)
            for name, fields in events:
                seen += 1
                if args.json:
                    print(json.dumps({"ts": ts, "type": name, **_jsonable(fields)}))
                else:
                    print(f"[{seen}] {name}: {fields}")
                sys.stdout.flush()
                if args.max and seen >= args.max:
                    return 0
    except KeyboardInterrupt:
        return 0
    finally:
        sock.close()
        ctx.term()


def _jsonable(d: dict) -> dict:
    return {k: (list(v) if isinstance(v, (tuple,)) else v) for k, v in d.items()}


if __name__ == "__main__":
    raise SystemExit(main())
