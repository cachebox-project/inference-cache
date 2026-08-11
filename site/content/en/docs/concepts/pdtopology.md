---
title: "PDTopology"
linkTitle: "PDTopology"
weight: 7
description: >
  Declare prefill/decode topology for phase-disaggregated serving.
---

## What is a PDTopology?

A `PDTopology` declares the prefill and decode pools for **phase-disaggregated (P/D)
serving** — a topology where the prefill phase (prompt processing) and the decode phase
(token generation) run on separate pools of replicas, so the KV cache produced by prefill is
transferred to a decode replica.

`PDTopology` is namespaced, short name `pdt`.

{{% alert title="Declarative today" color="info" %}}
The reconciler that acts on `PDTopology` is Phase-2 disaggregation work. The type is
intentionally declarative scaffolding for the `LookupPDRoute` gRPC RPC, which returns a
`(prefill_replica, decode_replica, transport_hint)` triple.
{{% /alert %}}

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: PDTopology
metadata:
  name: llama3-pd
  namespace: serving
spec:
  prefillPools:
    - name: prefill-a
      matchLabels:
        role: prefill
      replicas: 4
      acceleratorType: h100
  decodePools:
    - name: decode-a
      matchLabels:
        role: decode
      replicas: 8
      acceleratorType: h100
  acceleratorTypes:
    - name: h100
      vendor: nvidia
      model: H100
      matchLabels:
        accelerator: nvidia-h100
```

## Spec

| Field | Meaning |
|---|---|
| `prefillPools[]` / `decodePools[]` | Each pool has `name`, `matchLabels`, `replicas`, `acceleratorType`. |
| `acceleratorTypes[]` | Each entry has `name`, `vendor`, `model`, `matchLabels`. |

Printer columns: `Prefill`, `Decode`, `Age`.

## How it connects to routing

`PDTopology` backs the `LookupPDRoute` RPC. That RPC returns a prefill replica, a decode
replica, and a `transport_hint` (`Mooncake`, `NIXL`, or `Direct`) describing how KV should
move between them. Today `LookupPDRoute` is a fail-open stub that returns no hint; the CRD
defines the shape the full implementation will consume.

## Related pages

- [The gRPC contract]({{< relref "/docs/concepts/grpc-contract/" >}}) — the `LookupPDRoute` RPC.
- [CacheBackend]({{< relref "/docs/concepts/cachebackend/" >}}) — current cache integration profiles.
