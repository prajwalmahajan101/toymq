# 0007 — Visibility-timeout redelivery via background ticker, snapshot-on-send for Inflight

**Status:** Accepted
**Date:** 2026-06-08
**Scope:** `internal/broker/redelivery.go`, `internal/broker/topic.go`, `internal/broker/consumer.go`, `internal/broker/broker.go`

## Context

A consumer that receives a message but never acks it must eventually get
it again — otherwise a crashed or stuck subscriber would silently sink
messages forever. This is the at-least-once contract: every published
message reaches a consumer one or more times until it is acked.

We need a redelivery mechanism that:

- Detects "delivered but not acked within X seconds" without per-message
  bookkeeping the consumer-facing API has to know about.
- Survives a subscription churn: if the original subscription dies and a
  new one takes over, in-flight messages must still reappear.
- Does not deadlock or contend with the hot paths (`Publish`, `Ack`,
  `runDelivery`) under load.

A second, related problem surfaced as soon as the redelivery loop
mutated the in-map `*Inflight`: the race detector tripped on every test.
The original delivery path (`runDelivery`) and `Nack` both sent the same
pointer that lived in `c.inflight`. Once the ticker bumped `Attempts`
and `DeliveredAt` on that pointer, any subscriber still holding the
earlier delivery raced with the writer.

## Decision

### Background-ticker redelivery

A single `runRedeliverLoop` goroutine per broker ticks every
`redeliverInterval` (1 s default). Each tick:

1. Snapshots the topics map under `broker.mu.RLock`.
2. For each topic, snapshots the consumers map under
   `topic.consumersMu.RLock`.
3. For each consumer, briefly holds `consumer.mu` to walk
   `c.inflight`. Any entry whose `DeliveredAt + visibilityTimeout` is in
   the past gets its `Attempts` incremented, `DeliveredAt` reset to
   `now`, and a value snapshot collected into a local task slice. The
   `sub.sendCh` to use is captured from `c.sub` while the lock is held.
4. After releasing `consumer.mu`, the loop does a non-blocking send
   (`select { case ch <- inf: default: }`) for each collected task.
   Channel full → skip; the next tick retries.

Consumers with `c.sub == nil` are skipped entirely. The next
`Subscribe` for that consumer starts a new `runDelivery` from
`lastAcked + 1`, which will naturally re-stream every in-flight
message it had — no special path needed.

`visibilityTimeout` defaults to 30 s. Both timings are package
constants but settable via the unexported `newBroker` constructor for
tests that need short intervals.

### Snapshot-on-send for Inflight

Every channel send of an `*Inflight` is a value copy:

```go
inf := /* the canonical record stored in c.inflight */
snapshot := *inf
sendCh <- &snapshot
```

`runDelivery` does this after the initial map insert. `Nack` does it
after bumping `Attempts`/`DeliveredAt`. `collectExpired` does it after
the same bump. Subscribers therefore receive an immutable point-in-time
view; only the broker mutates the canonical record under `consumer.mu`.

### Shutdown ordering

`Broker.Close` cancels `redeliverCtx` and drains `redeliverDone`
**before** cancelling the persist context. Any `Attempts` bumps the
final tick produces (via the `persistDirty` flag inside `Ack`/`Nack`)
land in the persist loop's final synchronous `flushDirty`. Reversing
the order would lose those bumps on shutdown.

## Consequences

**Positive**
- One goroutine, not one timer per inflight. Scales with consumer count,
  not message count.
- Subscription churn is invisible to redelivery: replacing `c.sub`
  swaps the channel pointer atomically (under `consumer.mu`), and the
  next tick picks up the new sub.
- The `Subscription.sendCh` field gives the ticker a single, lifecycle-
  correct way to reach the right channel. Stale subs are gone the
  moment they're swapped out, so the ticker never pushes into a dead
  channel.
- The snapshot rule means subscribers, the writer goroutine
  (eventually), and the network session can read `Inflight` fields with
  zero locking — they own their copy.
- Non-blocking send means a slow/full subscriber cannot stall the
  ticker for any other consumer.

**Negative**
- Visibility expiry is granular to the tick interval. A message can be
  redelivered up to one full `redeliverInterval` *after* its
  `visibilityTimeout`, never before. Default 1 s tick + 30 s
  visibility → worst-case 31 s redelivery latency. Acceptable for the
  v1 broker; tighten by shortening the interval if needed.
- Snapshotting on every send costs one `Inflight` struct copy per
  delivery. Tiny — five fields, one of which is a slice header — but
  not zero. The race fix is non-negotiable; the cost is the price.
- `Inflight.Payload` is a `[]byte` whose backing array is shared across
  snapshots. Safe today because payloads are immutable after publish,
  but anyone who mutates a slice element later breaks this invariant.
  Worth a comment if such code is ever introduced.
- The `default:` arm of the non-blocking send is a silent drop *for
  this tick*. The next tick retries, so no message is permanently
  lost, but a continually-full channel produces no visible signal.
  Acceptable while we lack a metric layer; revisit when one lands.

## Edge cases

- **Subscriber swap during sweep.** The ticker captures `sub.sendCh`
  under `consumer.mu`. If a new `Subscribe` swaps `c.sub` *after* that
  capture, the ticker sends into the old channel. The non-blocking send
  either succeeds (old subscriber drains it) or drops (channel
  unbuffered/full). Either way the new subscriber's `runDelivery` will
  re-stream from `lastAcked + 1` and pick up the inflight. No
  duplicates beyond what at-least-once already permits.
- **Multiple in-flight messages expiring in the same tick.** All
  collected, all sent in the same outer loop, all subject to the same
  non-blocking-send drop policy. No ordering guarantee between them
  beyond map-iteration order — which the broker contract has never
  promised.
- **Empty `c.inflight`.** Early-returned in `collectExpired` to avoid
  paying the iteration cost.
- **`c.sub == nil`.** Skipped entirely; nothing to send to. The next
  `Subscribe` is the recovery path.

## Usage

- Production callers get the defaults via `broker.New(dataDir,
  dedupeCap)`.
- Tests call `newBroker(dir, cap, visibility, redeliverInterval)`
  directly with short timings (e.g. 100 ms / 20 ms) to keep the suite
  fast.
- Anyone adding a new field to `Inflight` must consider whether it is
  safe to value-copy. Pointer/map/channel fields would break the
  snapshot guarantee and need to be either deeply copied or kept
  immutable post-creation.
