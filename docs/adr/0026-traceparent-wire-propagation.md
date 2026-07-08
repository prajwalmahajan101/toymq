# 0026 — TRACEPARENT wire propagation

**Status:** Accepted
**Date:** 2026-07-08
**Scope:** `internal/proto/`, `internal/tracing/`, `internal/server/`, `internal/broker/`, `pkg/client`
**Supersedes:** the "no cross-process trace propagation" limitation recorded in [ADR 0015](./0015-observability-stack.md)

## Context

ADR 0015 wired OpenTelemetry spans into the broker (`broker.publish`,
`broker.subscribe`, `wal.append`) but left one limitation explicit: **the wire
protocol carried no trace context**, so a broker span was always a *root*. A
producer's trace and the broker's trace were disconnected — you could not follow
a request from client → broker in a single Jaeger/Tempo trace.

v2 M7 closes that gap. The constraint is the same one every M-series wire change
has honoured since M4: it must be **additive and opt-in**, so it needs no major
version bump and a pre-M7 client is byte-for-byte unaffected. It must also not
force OpenTelemetry onto `pkg/client`, which ADR 0013 keeps stdlib-only.

## Decision

### Wire: an optional `TRACEPARENT` prefix line

A connection may send

```
TRACEPARENT <w3c-traceparent> [TRACESTATE <w3c-tracestate>]
```

as the line immediately **before** a `PUB` or `SUB` frame. It is parsed as a
member of the sealed `Command` union (`TraceparentCommand`) but is **not a
request**: the session stashes it and applies it to the *next* PUB/SUB, then
clears it — one traceparent scopes exactly one following command. A connection
that never sends it is processed exactly as pre-M7.

The traceparent value itself is not validated by the parser; a malformed value
simply fails to extract a parent downstream and the span degrades to a fresh
root, rather than erroring the connection. This keeps the parser dumb and the
W3C spec the single source of truth for the format.

### Extraction: the W3C propagator sets the remote parent

`tracing.ContextWithTraceparent` wraps `propagation.TraceContext.Extract` over a
`MapCarrier`. The session builds a context from the stashed line and passes it
to `PublishCtx` / `Subscribe`, so `broker.publish` / `broker.subscribe` are
started as **children** of the caller's span — same trace id, parented on the
client span. `tracing.TraceparentFromContext` is the inverse (Inject), used by
clients to serialise their active span.

### Client: OTel-free opt-in via `WithTraceparentFunc`

`pkg/client` must not import OpenTelemetry (ADR 0013). So propagation is opt-in
through a **callback**, not a hard otel dependency:

```go
client.WithTraceparentFunc(func(ctx) (traceparent, tracestate string))
```

Before each PUB/SUB the client calls it with the call's context; a non-empty
traceparent is written as the prefix line (under the same write lock, so it
immediately precedes the frame). OTel users wire
`tracing.TraceparentFromContext` here; the default (nil) sends no line and
behaves as pre-M7. This keeps otel out of the client's dependency closure while
still enabling end-to-end propagation for callers who want it.

### Deferred: the MSG continuation (broker → consumer)

The symmetric direction — stamping a `TRACEPARENT` continuation onto delivered
`MSG` frames so a consumer stitches to the producer — is **deferred**. Two
reasons:

1. The client's MSG parser requires exactly 5 fields; a 6th token would break a
   pre-M7 client, so it would need a per-subscription opt-in gate to stay safe.
2. Genuine producer→consumer stitching across the durable log requires
   **persisting** the producer's trace context in the WAL record (delivery can
   happen minutes later, or after a restart). That is a record-format change
   with its own determinism concerns (ADR 0018), out of scope for an additive
   v2 milestone.

Producer→broker propagation (this ADR) is the tested, high-value half; the
broker→consumer half is recorded here as a future item.

## Consequences

**Positive**
- A producer's span and the broker's span join into one trace — the headline
  observability win, verified by an owned integration test
  (`TestTraceparentPropagatesToBrokerSpan`).
- Fully additive: no major bump, no change to any existing client. `TRACEPARENT`
  is off unless the caller opts in.
- `pkg/client` stays stdlib-only; otel is a caller concern, injected via a func.

**Negative / trade-offs**
- Only the client→broker direction stitches; broker→consumer is deferred (see
  above), so a consumer's processing span is not yet linked to the producer.
- One extra line + one propagator Extract per traced PUB/SUB (negligible; zero
  on the un-traced path).
- The traceparent is trusted verbatim — a client can forge a parent. Acceptable
  for a telemetry signal (no security decision rides on it).

## v3 / toyraft forward-compat

Extraction happens at the edge (session), before `Propose`, so the trace context
is a materialised-view concern and never enters `StateMachine.Apply` — consistent
with ADR 0018. When the MSG continuation lands, persisting the traceparent in the
record would travel through the replicated log like `TsNs`/`VisibleAtNs`, so the
deferral does not paint v3 into a corner.

## Usage

Raw wire (additive):

```
TRACEPARENT 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
PUB orders - - 5
hello
```

Client:

```go
c, _ := client.Dial(ctx, addr,
    client.WithTraceparentFunc(tracing.TraceparentFromContext))
ctx, span := tracer.Start(ctx, "place-order")
c.Pub(ctx, "orders", key, "", payload) // broker.publish is now a child of "place-order"
span.End()
```
