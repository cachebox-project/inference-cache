#!/usr/bin/env python3
"""Emit synthetic vLLM-shaped KV-cache events over ZMQ.

Local fallback when the real engine can't start (e.g. no GPU / amd64 image under
emulation). Lets you validate the ZMQ plumbing + the subscriber's decode path
end-to-end without vLLM. It mirrors vLLM's ZmqEventPublisher framing
([topic, seq, msgpack(EventBatch)]); it is NOT a substitute for capturing real
engine events for the DoD.

    python kv_events_synthetic_publisher.py --bind tcp://*:5557 --topic kv-events
    # in another shell:
    python kv_events_subscriber.py --endpoint tcp://localhost:5557 --topic kv-events
"""
import argparse
import struct
import time

import msgspec
import zmq


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


class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True):
    ts: float = 0.0
    events: list = []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bind", default="tcp://*:5557")
    ap.add_argument("--topic", default="kv-events")
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind(args.bind)
    enc = msgspec.msgpack.Encoder()
    topic = args.topic.encode()
    seq = 0
    print(f"# publishing on {args.bind} topic={args.topic!r}")
    try:
        while True:
            base = seq * 4
            batch = EventBatch(
                ts=time.time(),
                events=[
                    BlockStored(block_hashes=[base, base + 1], parent_block_hash=None,
                                token_ids=list(range(16)), block_size=16, lora_id=None),
                    BlockRemoved(block_hashes=[base - 4]) if seq else BlockStored(),
                ],
            )
            sock.send_multipart([topic, struct.pack(">Q", seq), enc.encode(batch)])
            seq += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0
    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    raise SystemExit(main())
