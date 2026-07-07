# 0025 — Delayed messages

**Status:** Accepted
**Date:** 2026-07-07
**Scope:** `internal/wal/`, `internal/proto/`, `internal/broker/`, `pkg/client`, `cmd/toymqctl`

## Context

The third v2 M6 pattern: **scheduled delivery** — publish a message now but have
it become deliverable only after a delay (retry backoff, deferred jobs, TTL-style
reminders). ToyMQ had no notion of a future delivery time; every record was
deliverable the instant it was durable.

The v3/toyraft constraint (ADR 0018) is the governing one. A delay is inherently
about wall-clock time, and wall-clock must not enter `StateMachine.Apply` — the
replicated transition has to be deterministic. So the design must resolve the
time-dependent part **at propose time** and keep only a local, non-replicated
timer on the delivery side.

## Decision

### Resolve the delay at the proposer, store an absolute time

Add an append-only `VisibleAtNs uint64` field to `wal.Record` (ADR 0023's
segment format; the field trails the payload and is CRC-guarded). The
**producer/proposer** computes `VisibleAtNs = now + delay` in
`partition.publishCtx` and stores it verbatim in the record — exactly the
treatment `TsNs` already gets (ADR 0018). `0` means "visible immediately", so
every pre-M6 record and every non-delayed PUB is unchanged and decodes
identically.

Because the release time is fixed in the record at propose time, applying the
record is deterministic: every node stores the same `VisibleAtNs`. No wall-clock
enters the state transition.

### Wire: additive `DELAY <ms>` token

`PUB <topic> <dedupe> <routing> <len> [DELAY <ms>]` — `parsePub` accepts an
optional trailing `DELAY <ms>` (a 7-field frame); `PubCommand.DelayMs` carries
it; the publish path threads it to `publishCtx`. The 5-field frame and all
existing clients are unaffected. `pkg/client` gains `PubDelay`; `toymqctl pub`
gains `--delay-ms`.

### Delivery gate: a local timed park

In `runDelivery`, when the reader returns a record whose `VisibleAtNs` is in the
future, the delivery goroutine **parks** (`awaitVisible`: a `time.Timer` wait,
cancellable by ctx) until that instant, then delivers. This is purely local
delivery-side logic — the timer never affects Apply.

**Persistence is free.** The release time lives in the WAL record, so a restart
simply re-reads it and re-parks; there is no separate delay store to replay.

**Head-of-line by design.** Delivery is a single in-order goroutine per
`(partition, consumer)`, so a delayed record holds that partition's delivery
until it fires — a later record cannot overtake it. This preserves per-partition
order (the core ToyMQ guarantee) at the cost of head-of-line blocking. The escape
hatch is to route delayed traffic to a dedicated partition or topic. This
tradeoff is *why* the in-log `visible-at` approach was chosen over a side
scheduler: a side queue would deliver out of log order and would need its own
replicated, deterministic reconciliation under v3 — the in-log form is the
toyraft-forward-compatible one.

### Retention interaction

Retention (ADR 0023) drops whole sealed segments by size/age. It must not drop a
segment that still holds an **un-fired** delayed record, or the message is lost
before it fires. Each segment tracks `maxVisibleAtNs`; `retentionKeepIndex`
clamps the drop floor down to the oldest segment whose `maxVisibleAtNs` is still
in the future, overriding size/age. Once the record fires, the guard lifts and
the segment becomes reclaimable.

## Consequences

**Positive**
- Scheduled delivery with zero new storage machinery — the WAL record *is* the
  schedule, so it is durable and survives restart for free.
- Additive wire + record change; non-delayed traffic is untouched.
- The whole feature is Apply-deterministic by construction (propose-time
  resolution), so it needs no rework under v3.

**Negative / trade-offs**
- Head-of-line blocking: a delayed record stalls its partition's delivery for
  that consumer until it fires. Documented; mitigated by dedicating a partition
  to delayed traffic.
- A far-future delay pins its segment against retention until it fires (bounded
  disk cost for the guarantee).
- The delay is wall-clock on the delivering node; there is no monotonic-clock or
  drift correction (acceptable for a single-node broker).

## v3 / toyraft forward-compat

`VisibleAtNs` is resolved by the proposer and stored in the record, so it is
already in the exact shape v3 needs: the leader stamps `now+delay` when it
proposes the entry, every node applies the same value, and the local delivery
timer on each node is a materialised-view concern outside Apply — identical to
how `TsNs` and the retention sweep are handled (ADR 0018, 0023).

## Usage

```
# Deliver 5 seconds from now:
toymqctl pub --delay-ms 5000 orders '{"job":"email"}'
```

Wire (additive):

```
PUB orders - - 11 DELAY 5000
hello world
```

`pkg/client` exposes `Client.PubDelay(ctx, topic, dedupeKey, routingKey, payload, delayMs)`.
