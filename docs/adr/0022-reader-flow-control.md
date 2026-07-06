# 0022 — Reader backpressure: receive window + PAUSE/RESUME

**Status:** Accepted
**Date:** 2026-07-06
**Scope:** `internal/broker/`, `internal/server/`, `internal/proto/`, `internal/config/`, `pkg/client`, `cmd/toymq`

## Context

Delivery works one goroutine per `(partition, consumer)`: `runDelivery` reads
the next WAL record, records it in the consumer's `inflight` map, then blocks
sending an `Inflight` snapshot onto the session's `sendCh` (buffer 64), which
the writer drains to the socket (ADR 0008, 0021).

The only backpressure was that chain — `sendCh` plus the socket's TCP buffer.
It bounds memory **only when the client stops reading the socket**. A client
that reads eagerly but **acks slowly** drains `sendCh` fast while `runDelivery`
keeps producing, so `inflight` grows without bound and broker memory climbs
with the backlog. That is the v2 M5 risk: *inflight memory under a slow
consumer must be bounded.*

## Decision

Two layers, both landing here.

### 1. Receive window (automatic, the memory bound)

A per-`(partition, consumer)` credit window `W` (`--recv-window`, default 256,
min 1). `runDelivery` calls `Consumer.awaitCredit(ctx)` **before** reading the
next record; it blocks while `paused || len(inflight) >= W`. `Ack` frees a slot
and signals, re-opening delivery. Because the gate runs before the WAL read,
`len(inflight) ≤ W` always holds — the memory bound.

`awaitCredit` uses a buffered-1 **coalescing wake channel**, not a `sync.Cond`,
because the gate must also honour `ctx` cancellation (subscription teardown).
The buffer + a re-check loop closes the check-then-block gap: a signal that
arrives between the condition test and the receive is retained and delivered to
the next receive, so no wake-up is missed.

**Redelivery and Nack bypass the gate.** Both re-push an *already-counted*
inflight record; they push to `sendCh` non-blocking and never consume a new
credit, so they cannot stall behind a full window and the count stays `≤ W`.

Scope is per-`(partition, consumer)`, not per-session: `Ack` already lands on
the `Consumer`, so releasing a credit is a local operation with no new lock
ordering. The consequence — a `SUB #*` across N partitions is bounded by
**N×W**, not W — is accepted (still bounded) over the cross-boundary
bookkeeping a true per-session cap would need.

### 2. PAUSE / RESUME (explicit, client-driven)

Argument-less, session-scoped wire verbs added to the sealed `Command` union
(ADR 0004). They flip a `paused` flag on every `Consumer` of the connection's
current subscription (all N partitions of a `SUB #*`), which the same
`awaitCredit` gate checks. The session reaches exactly the subscribed
partitions via a `consumer` back-reference on each `Subscription` (`SetPaused`)
— it never touches the consumer registry, so PAUSE can't accidentally create
consumers on unsubscribed partitions. `PAUSE`/`RESUME` without a prior `SUB`
returns `ERR NO_SUB`.

The **redelivery sweep also honours `paused`** (`collectExpired` returns early):
an explicit PAUSE stops resends too, not just first delivery — otherwise the
visibility-timeout ticker would keep pushing MSGs the client asked to stop.
Visibility timers are unaffected (`DeliveredAt` is untouched), so RESUME
resumes normal redelivery.

Paused state is **ephemeral and per-connection**: it lives on the in-memory
`Consumer`, is never persisted, and a reconnect starts unpaused.

### PAUSE precision

PAUSE takes effect at the next `awaitCredit` check. If the delivery goroutine
is already parked reading the WAL (`reader.Next`) when PAUSE lands, the one
record it is awaiting may still be delivered before the gate is re-evaluated —
an at-most-one in-flight "slip." This is standard for cooperative flow control
and acceptable; when the window is already full the goroutine is parked in the
gate, so there is no slip at all.

## Consequences

**Positive**
- Inflight memory is bounded by `W` per consumer regardless of backlog — the
  M5 exit criterion, proven by `TestReceiveWindowBoundsInflight`.
- PAUSE/RESUME give a consumer explicit throttle control independent of acks,
  for when its own downstream is saturated.
- Additive wire change (new verbs, new flag); existing PUB/SUB/ACK/NACK frames
  and default behaviour are unchanged. The default window (256) is high enough
  that normal consumers never notice the gate.

**Negative / trade-offs**
- `SUB #*` is bounded by N×W, not W (documented above).
- A misconfigured tiny window throttles a fast, well-behaved consumer; the
  default is deliberately generous.
- PAUSE is best-effort to within one in-flight message (the slip above).

## v3 / toyraft forward-compat

Flow control is a purely local client-plane concern: the window gates delivery
out of the local materialised log, and `paused` is per-connection state. It
does not touch `raft.StateMachine.Apply` determinism (ADR 0018) — replication
replays the log identically on every node regardless of any one consumer's
window. No code owed now.

## Usage

```
toymq --recv-window 256     # default; ≤256 un-acked messages per consumer
toymq --recv-window 1       # strict lock-step: one message at a time
```

Wire (additive):

```
PAUSE       # suspend delivery for this connection's subscription -> OK 0
RESUME      # resume delivery                                     -> OK 0
```

`pkg/client` exposes `Client.Pause(ctx)` / `Client.Resume(ctx)`.
