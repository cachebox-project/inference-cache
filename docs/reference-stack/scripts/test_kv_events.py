#!/usr/bin/env python3
"""Reproducible checks for the KV-event decode + token redaction.

No broker, no pytest — just run it:
    pip install -r requirements.txt
    python test_kv_events.py        # exits non-zero on failure

Guards the metadata-only contract: decoded events must never carry token_ids.
"""
import msgspec

import kv_events_subscriber as sub


def _roundtrip(batch) -> list:
    payload = msgspec.msgpack.Encoder().encode(batch)
    _, events = sub._decode(payload)
    return events


def test_token_ids_redacted_to_count():
    batch = sub.EventBatch(ts=1.0, events=[
        sub.BlockStored(block_hashes=[10, 11], parent_block_hash=None,
                        token_ids=[7, 8, 9], block_size=16, lora_id=None),
    ])
    (name, fields), = _roundtrip(batch)
    assert name == "BlockStored", name
    assert "token_ids" not in fields, "token content must be redacted"
    assert fields["token_count"] == 3, fields
    assert fields["block_hashes"] == [10, 11], fields  # hashes preserved


def test_block_removed_passes_through():
    (name, fields), = _roundtrip(sub.EventBatch(ts=2.0, events=[sub.BlockRemoved(block_hashes=[4])]))
    assert name == "BlockRemoved" and fields == {"block_hashes": [4]}, (name, fields)


def test_redact_is_idempotent_and_safe_on_missing_field():
    assert sub._redact({"block_hashes": [1]}) == {"block_hashes": [1]}
    assert sub._redact({"token_ids": []}) == {"token_count": 0}


def test_undecodable_payload_reports_error_not_content():
    _, events = sub._decode(b"\xff\xff not msgpack \x00")
    (name, fields), = events
    assert name == "UNDECODED" and "error" in fields, events
    assert "token_ids" not in fields


def main() -> int:
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    print(f"\n{'PASS' if not failures else 'FAIL'} ({failures} failure(s))")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
