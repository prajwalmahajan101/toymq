# 0006 — Debounced atomic offset persistence with dirty flags

**Status:** Accepted
**Date:** 2026-06-08
**Scope:** `internal/broker/offsets.go`, `internal/broker/broker.go`

## Context

Consumer offsets (`lastAcked` plus the `aboveLast` set for out-of-order
acks) must survive a broker restart, otherwise every ACK is for nothing
— consumers would replay from the start of the log. Two competing
requirements:

- **Durability:** acks must be on disk before the consumer is told
  "you've successfully acked." Otherwise a crash between ACK and flush
  causes redelivery.
- **Throughput:** writing `offsets.json` and `fsync`-ing it after
  every single ACK would dominate the ACK latency. With thousands of
  acks per second this is fatal.

We need to amortise fsync cost across many acks without losing them on
shutdown.

## Decision

Three pieces, intentionally separated:

1. **Dirty flag per Consumer.** `persistDirty atomic.Bool`. `Ack` sets
   it `true`. Lock-free; doesn't add cost to the ACK hot path.
2. **Background ticker on Broker.** Every 100 ms (`AckPersistDebounce`),
   `flushDirty` iterates topics, skipping any whose consumers are all
   clean, and calls `flushOffsets(dir)` on the rest. Errors are
   structured-logged; the next sweep retries.
3. **Atomic write per file.** `flushOffsets` writes to
   `offsets.json.tmp`, `fsync`s it, then `os.Rename`s over
   `offsets.json`. POSIX rename on the same filesystem is atomic —
   readers see either the old file or the new file, never a partial.

The dirty flag is cleared **before** the snapshot. A concurrent ACK
that races with the flush re-sets the flag and gets caught on the next
sweep. We pay an extra (idempotent) flush rather than lose state.

`Broker.Close` cancels the persist context, waits for `persistDone`,
then closes WAL handles. The persist loop runs one final
`flushDirty()` before exiting. So `Close` is the synchronous flush;
producers that need a durability fence on shutdown have one.

## Consequences

**Positive**
- Ack hot path takes one atomic store. Zero filesystem I/O.
- Flushes coalesce: bursts of N acks land in one JSON write +
  one fsync + one rename instead of N of each.
- Acked state is durable by the time `Close` returns. No "lost ACK"
  window on graceful shutdown.
- A crash mid-flush is safe by construction: the tmp file is either
  not renamed (old offsets stand) or fully renamed (new offsets
  durable). No partially-written `offsets.json` is ever observable.

**Negative**
- A crash *between* ack and the next sweep (worst case ~100 ms) loses
  those acks. The trade-off is explicit; per-ack fsync was the only
  alternative and was rejected on throughput grounds. Consumers will
  re-receive those messages on restart, which is fine because they
  must be idempotent anyway (at-least-once delivery is the broker's
  contract).
- The "clear-dirty-then-snapshot" race re-flushes on contention. In
  practice the extra flush is rare and cheap; correctness wins.
- JSON is human-readable but not the fastest format. Acceptable while
  the offsets file is small. A binary format becomes worthwhile if a
  topic ever holds thousands of out-of-order msg_ids in `aboveLast`.

## Edge cases

- **Missing `offsets.json` at startup.** Treated as "fresh consumer,"
  not an error. The first ack will create it.
- **Multiple consumers on one topic.** All persisted in one file under
  a `consumers` object keyed by consumer ID.
- **Empty `aboveLast`.** Serialised as `[]`, not omitted, so the file
  shape is stable and easy to test.
- **Out-of-order `aboveLast` entries.** Sorted before serialisation
  (`slices.Sort`) so the same logical state always produces the same
  byte output. Critical for diffability and snapshot tests.

## Usage

- `Ack` calls `c.persistDirty.Store(true)` at the end. `Nack` does not
  (no offset state changes).
- The session loop never calls flush directly; the ticker handles it.
- Future code that wants synchronous flushes (e.g., a test) calls
  `Broker.Close` or could call `Topic.flushOffsets(dataDir)` directly
  if exposed.
