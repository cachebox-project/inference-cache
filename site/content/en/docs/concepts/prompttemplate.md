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

## Why stable vs mutable matters

The stable slots contribute to the **stable prefix hash**; the mutable slots do not. By
holding the stable framing constant and isolating the parts that change, a template keeps a
long cacheable prefix that many requests can reuse — the render pipeline produces the same
stable prefix hash whenever the stable slots are unchanged, so the engine's prefix cache and
inference-cache's routing hint both stay warm.

## Spec

| Field | Meaning |
|---|---|
| `body` | The template body (required). |
| `slots[]` | Each slot has `name`, `type` (`Stable` or `Mutable`), `required`, and `description`. |

## Status

| Field | Meaning |
|---|---|
| `templateRevision` | A stable revision identifier used for cache invalidation — it flows through to `RenderTemplateResponse.template_revision`. |
| `conditions`, `observedGeneration` | Standard bookkeeping. |

Printer columns: `Revision`, `Age`.

## Related pages

- [The gRPC contract]({{< relref "/docs/concepts/grpc-contract/" >}}) — the `RenderTemplate` RPC.
