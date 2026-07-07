# 0024 — Dead-letter queue

**Status:** Accepted
**Date:** 2026-07-07
**Scope:** `internal/broker/`, `internal/config/`, `cmd/toymq`

## Context

A message that a consumer repeatedly fails to process (NACK, or repeated
visibility-timeout redelivery) redelivered forever. One poison message could
wedge a partition — every redelivery cycle re-pushes it, ahead of or alongside
healthy traffic, with no way to quarantine it. v2 M6 adds a **dead-letter queue**
so a message that has failed too many times is set aside for inspection instead
of blocking the stream.

The v3/toyraft constraint (ADR 0018) applies: the *trigger* to dead-letter a
message is state the leader observes locally (an attempt count), but the
*effect* — moving the payload — must be a deterministic transition every node
can replay identically.

## Decision

### Trigger: `--dlq-after-nacks N`

A message is dead once it has been delivered `N` times without a successful ack.
The check `inf.Attempts >= N` runs at both failure points:

- **`Nack`** (`Consumer.nackOrKill`): the client explicitly rejected it.
- **The visibility-timeout sweep** (`collectExpired`): it silently timed out.

Both treat a failure identically — `Attempts` starts at 1 on first delivery and
increments on each nack/timeout, so `N` is the total number of delivery
attempts before dead-lettering. `N == 0` disables the DLQ (default), leaving the
pre-M6 redeliver-forever behaviour.

### Effect: `dlqMove` — the deterministic seam

`dlqMove(srcTopic, payload)` is the single dead-letter operation
(`internal/broker/dlq.go`). It:

1. **Synthetically acks** the dead message out of the source consumer's
   `inflight` (done under the consumer lock by `nackOrKill` / `collectExpired`
   before the move), so it stops redelivering and frees a receive-window slot.
2. **Republishes** the payload onto the auto-created topic `<srcTopic>.dlq`
   (lazily created with a single partition), reusing the entire normal
   topic/partition/publish path. Consumers `SUB <topic>.dlq` and inspect dead
   messages with all the existing machinery — no new wire verbs.

**Loop guard:** a topic already ending in `.dlq` has an effective threshold of 0
(`dlqThreshold`), so a dead-letter topic is never itself dead-lettered — there is
no `.dlq.dlq`.

### Best-effort in v2

The attempt count lives in the in-memory `Inflight.Attempts` and **resets on
restart** — the same model as the existing redelivery counter. A message
mid-way to the DLQ before a crash starts its count over on recovery. This is
accepted for the single-node v2: the DLQ is a convenience quarantine, not a
durable exactly-N guarantee. The `dlqMove` append itself is durable (it is a
normal WAL publish); only the trigger input is ephemeral. A failed `dlqMove`
append is logged, not retried inline, and does not fail the client's NACK.

## Consequences

**Positive**
- A poison message is quarantined after `N` attempts instead of wedging its
  partition forever; operators inspect `<topic>.dlq` with normal consumers.
- Zero new wire surface — DLQ is a normal topic. Additive and off by default.
- `dlqMove` is a clean, single-call seam, which is exactly what v3 needs to
  propose (below).

**Negative / trade-offs**
- The attempt count is in-memory and resets on restart (best-effort N).
- `<topic>.dlq` is created with one partition regardless of the source topic's
  partition count — dead-letter ordering/scale is intentionally simple.
- A message dead-lettered from a partition is gone from the source; a consumer
  that later recovers will not see it on the source topic (by design).

## v3 / toyraft forward-compat

The trigger (an attempt count crossing `N`) is **leader-side detection** over
local delivery state — it never enters `Apply`. When the leader detects a dead
message it **proposes** the `dlqMove` as a log entry; every node then applies the
same deterministic transition (append to `<topic>.dlq` + drop the source
inflight). Because `dlqMove` is already isolated as one function with explicit
inputs (source topic + payload), it drops into the propose/apply split without
change. The in-memory `Attempts` is the v2 trigger input only — v3 replaces it
with the leader's replicated view.

## Usage

```
toymq --dlq-after-nacks 5     # a message dead-letters after 5 failed deliveries
toymq                          # default: DLQ off, redeliver forever
```

Consume dead letters with a normal subscription:

```
SUB orders.dlq                 # inspect messages that failed on `orders`
```
