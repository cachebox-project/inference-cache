#!/usr/bin/env python3
"""Regenerate the golden constants in ../fingerprint_test.go.

These pin the content fingerprint construction (XXH3-64, seed 1337) byte-for-byte
against an independent implementation, so the Go `zeebo/xxh3` package and the
inference-cache-benchmark proxy (Python `xxhash`) and SMG (Rust `xxhash_rust`)
all agree on the same routing keys. Mirrors SMG's event_tree.rs:

    seed              = 1337
    content_hash(blk) = XXH3_64(seed) over concat(token.to_le_bytes() as u32)
    prefix_hash[0]    = content_hash[0]
    prefix_hash[i]    = XXH3_64(seed) over prev.le8 ++ content_hash[i].le8

Run:  python3 gen_golden.py   (requires `pip install xxhash`)
"""
import struct
import xxhash

SEED = 1337


def content_hash(tokens):
    buf = b"".join(struct.pack("<I", t & 0xFFFFFFFF) for t in tokens)
    return xxhash.xxh3_64_intdigest(buf, seed=SEED)


def next_seq(prev, content):
    return xxhash.xxh3_64_intdigest(struct.pack("<Q", prev) + struct.pack("<Q", content), seed=SEED)


def prefixes(tokens, block_size):
    n_full = (len(tokens) // block_size) * block_size
    chs = [content_hash(tokens[i:i + block_size]) for i in range(0, n_full, block_size)]
    out, prev = [], None
    for i, ch in enumerate(chs):
        ph = ch if prev is None else next_seq(prev, ch)
        out.append(ph)
        prev = ph
    return chs, out


# Sanity: XXH3 streaming (what SMG uses) == one-shot (what Go/this use).
def content_streaming(tokens):
    h = xxhash.xxh3_64(seed=SEED)
    for t in tokens:
        h.update(struct.pack("<I", t & 0xFFFFFFFF))
    return h.intdigest()


if __name__ == "__main__":
    for toks in ([1, 2, 3], list(range(16)), list(range(40))):
        assert content_hash(toks) == content_streaming(toks)
    print("OK streaming == one-shot (canonical XXH3)\n")

    cases = {
        "seq_1_32_bs16": (list(range(1, 33)), 16),
        "seq_100_119_bs16": (list(range(100, 120)), 16),
        "seq_0_63_bs16": (list(range(64)), 16),
    }
    for name, (toks, bs) in cases.items():
        chs, phs = prefixes(toks, bs)
        print(f"{name}: content={chs}")
        print(f"{' ' * len(name)}  prefix ={phs}")
