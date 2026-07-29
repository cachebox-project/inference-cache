---
title: "PromptTemplate"
linkTitle: "PromptTemplate"
weight: 6
description: >
  Declare a cache-aware prompt template and its stable/mutable slots.
---

## What is a PromptTemplate?

A `PromptTemplate` declares a prompt template and marks which of its slots are **stable**
(part of the cacheable prefix) versus **mutable** (vary per request). This backs the
mutable-slot render pipeline — the "wedge" that lets a mostly-fixed prompt keep a stable,
cacheable prefix even as small parts of it change.

`PromptTemplate` is namespaced, short name `pt`.

{{% alert title="Declarative today" color="info" %}}
The render controller that consumes `PromptTemplate` is future work. The type is
intentionally declarative scaffolding — it defines the shape that the `RenderTemplate` gRPC
handler and the render pipeline will use.
{{% /alert %}}

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: PromptTemplate
metadata:
  name: support-agent
  namespace: serving
spec:
  body: |
    You are a helpful support agent for {{ product }}.
    Conversation so far:
    {{ history }}
  slots:
    - name: product
      type: Stable
      required: true
      description: Product name — fixed for the deployment.
    - name: history
      type: Mutable
      description: Per-request conversation history.
```

## Planned stable vs mutable semantics

When the render controller and `RenderTemplate` RPC are implemented, stable slots will
contribute to the **stable prefix hash** and mutable slots will not. Holding the stable
framing constant while isolating the parts that change will let a template retain a long
cacheable prefix across requests.

## Spec

| Field | Meaning |
|---|---|
| `body` | The template body (required). |
| `slots[]` | Each slot has `name`, `type` (`Stable` or `Mutable`), `required`, and `description`. |

## Status

| Field | Meaning |
|---|---|
| `templateRevision` | Reserved for the future render controller's stable cache-invalidation revision. It is not populated today. |
| `conditions`, `observedGeneration` | Reserved controller bookkeeping; no PromptTemplate controller writes these fields today. |

Printer columns: `Revision`, `Age`.

## Related pages

- [The gRPC contract]({{< relref "/docs/concepts/grpc-contract/" >}}) — the `RenderTemplate` RPC.
