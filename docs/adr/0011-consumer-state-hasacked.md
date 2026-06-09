# 0011 — Consumer state: explicit hasAcked flag

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `internal/broker/consumer.go`, `internal/broker/offsets.go`, `internal/broker/topic.go`

## Context

`Consumer.lastAcked` is a `uint64` that records "the last contiguous
MsgID this consumer has acked." In code that decides where a
re-subscribing consumer should start reading the WAL, the value `0`
was being used to mean two distinct things:

1. The consumer has never acked anything (fresh state).
2. The consumer has acked exactly MsgID 0 and nothing above it.

These two states are observationally identical when persisted —
`lastAcked: 0, above_last: [], inflight: {}` — but they require
opposite behaviour from `runDelivery`:

- Case 1 should open the WAL reader at MsgID 0 (re-deliver from the
  beginning).
- Case 2 should open the WAL reader at MsgID 1 (don't re-deliver
  what's already been acked).

The existing code (`topic.go`, "fresh consumer" branch) collapsed both
into case 1, so the first acked message of every consumer was silently
re-delivered after any restart. The first ToyMQ integration test
covering "publish 1 → ack → restart → re-sub → no redelivery" exposed
this immediately.

## Decision

Add an explicit `hasAcked bool` field to `Consumer`. It flips to
`true` the first time `Consumer.Ack` succeeds and never resets. The
flag is persisted in `offsets.json` as `has_acked` and restored on
startup by `loadOffsets`.

```go
type Consumer struct {
    // ...
    hasAcked  bool
    lastAcked uint64
    // ...
}
```

Three call sites change:

- **`Consumer.Ack`** — the contiguous-advance branch fires when
  `(!hasAcked && msgID == 0)` or `(hasAcked && msgID == c.lastAcked+1)`.
  Out-of-order acks (anything not matching the contiguous branch) land
  in `aboveLast` regardless of `hasAcked`. On the contiguous branch we
  set `hasAcked = true` and `lastAcked = msgID`.
- **`Topic.runDelivery`** — `startID = 0` when `!hasAcked`, else
  `lastAcked + 1`. No special-case on inflight or aboveLast emptiness.
- **`offsets`** — `consumerOffsets` gains a `HasAcked` JSON field;
  `snapshotOffsets` reads it under the consumer mutex,
  `loadOffsets` writes it back into the consumer.

Rejected alternatives:

- **Sentinel `int64` with `-1`** — uglier (an unsigned count
  represented as signed), and the sentinel propagates through every
  `startID = lastAcked + 1` arithmetic site.
- **Make MsgIDs start at 1 in the WAL** — huge blast radius across
  WAL recovery (PROGRESS bug #3 was about the exact zero-vs-one
  off-by-one in recovery), the framed record format, and every test
  that asserts on MsgID values.

## Consequences

**Positive**
- "Never acked" and "acked MsgID 0" become distinguishable in memory
  and on disk.
- `runDelivery`'s start-position derivation is one line and reads as
  prose: "if you've ever acked, resume above lastAcked; else start
  from the beginning."
- Out-of-order acks no longer have to special-case the lastAcked=0
  state — they unconditionally go into `aboveLast` when not
  contiguous.
- Note: this means `lastAcked` advances only over a contiguous
  prefix of acknowledged ids. Acking a high id while earlier ids
  remain un-acked leaves `lastAcked` at the gap — the un-acked
  earlier ids will be redelivered on the next consumer takeover
  (the visibility timeout puts them back into pending). This is
  the property `toymqctl ack <high-id>` relies on, and the reason
  it warns users about leftover un-acked ids.

**Negative**
- One extra field per consumer (1 byte in memory, one JSON key on
  disk). Negligible.
- The on-disk format changed: pre-fix `offsets.json` files have no
  `has_acked` key. JSON decoding defaults absent fields to the zero
  value (`false`), so a pre-fix file with `lastAcked: 7` will load as
  `{hasAcked: false, lastAcked: 7}` and the consumer will re-deliver
  from MsgID 0. ToyMQ is a learning project with no production
  deployment, so this migration cost is zero in practice. If that
  ever changes, the fix is to treat any `lastAcked > 0` on load as
  evidence of past acks and force `hasAcked = true`.
- The `Ack` boolean expression got slightly busier. The compensating
  win is that the branch now spells out exactly which state
  transitions are valid.

## Edge cases

- **First message ack (MsgID 0).** Before: `lastAcked=0` →
  indistinguishable from fresh. After: `hasAcked=true, lastAcked=0`
  → distinguishable. Next subscribe resumes at MsgID 1.
- **Out-of-order ack before any contiguous ack.** Consumer receives
  MsgIDs 0 and 1, acks 1 first. Branch: `!hasAcked && msgID == 0` is
  false (msgID=1), so we fall through to `aboveLast[1] = {}`.
  `hasAcked` stays false. Then ack 0: contiguous branch
  (`!hasAcked && msgID == 0`) fires, sets `hasAcked=true`,
  `lastAcked=0`, then the drain loop advances through aboveLast → 1.
  Final state: `hasAcked=true, lastAcked=1, aboveLast={}`. Correct.
- **Restart after the above.** loadOffsets restores
  `hasAcked=true, lastAcked=1, aboveLast=[]`. runDelivery starts at
  MsgID 2. No re-delivery of 0 or 1. Correct.
- **Brand new topic, first subscriber, no messages yet.** `hasAcked`
  is false, no records in WAL. runDelivery opens reader at MsgID 0,
  blocks in `reader.Next` until a publish broadcasts. Identical to
  pre-fix behaviour for this case.

## Usage

- Test that demonstrates the fix:
  `internal/integration/persistence_test.go:TestRoundTripAckSurvivesRestart`
  — publish 1 → ack → restart → re-subscribe → assert no MSG arrives
  within 250 ms.
- New consumer code reads `hasAcked` to make any "resume vs. start
  fresh" decision; do not infer that from `lastAcked == 0`.
- Persisted `offsets.json` schema is now:
  ```json
  {
    "consumers": {
      "consumer-1": {
        "has_acked": true,
        "last_acked": 17,
        "above_last": [20, 22]
      }
    }
  }
  ```
