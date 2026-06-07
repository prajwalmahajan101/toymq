# 0002 — Per-message fsync with atomic committed offset

**Status:** Accepted
**Date:** 2026-06-06
**Scope:** `internal/wal/log.go`

## Context

ToyMQ promises that once a producer receives `OK` for a `PUB`, the message
survives a `kill -9`. That requires the bytes to be on stable storage — not in
the kernel page cache — before we acknowledge.

A separate question is how *readers* (tailing consumers) know which bytes are
safe to read. A naive reader could observe a partial write if the writer is
mid-flush. We need a fence between "in flight" and "durable + visible."

## Decision

**One write, one fsync, then advance state.**

In `Log.Append`, in this order, under the writer mutex:

1. Encode the record into a local `bytes.Buffer`.
2. Single `f.Write` of the full encoded frame. `O_APPEND` guarantees the bytes
   land contiguously at end-of-file.
3. `f.Sync()` — issues `fsync(2)`. Until this returns, no state advances.
4. Increment `nextMsgID`.
5. Atomically advance `committed` by the encoded byte length.

`committed` is a `sync/atomic.Uint64`. Readers (tailing consumers — see future
ADR) load it without taking the writer mutex, so reads never block writes.

If `f.Sync` returns an error we surface it and do not advance `nextMsgID`.
The next `Append` will re-use the same MsgID — the previous append's bytes
may or may not be on disk, and recovery on next startup will either accept
them (CRC valid) or truncate them (CRC invalid). The producer sees an error
and retries with the same dedupe key, so this is at-most-one-delivery for
the affected message — the contract holds.

## Consequences

**Positive**
- Durability is honest: `OK` means `fsync` returned.
- Single writer mutex; readers are lock-free via the atomic.
- Recovery is unambiguous: any record whose bytes are present and pass CRC
  is safe to surface; everything past the last good record is truncated.
- The mutex is only held across one encode + one write + one fsync. Concurrent
  publishers serialize cleanly without lock contention on the read side.

**Negative**
- Throughput is bound by single-disk fsync latency. Typical NVMe: ~10–50 µs;
  spinning rust: 5–20 ms. v1 prioritizes durability over throughput; a future
  "interval fsync" mode is planned but not implemented.
- The fsync per record makes batching impossible without a configuration knob.
  Acceptable; we will revisit when benchmarks justify the complexity.

## Usage

- Anything that needs to read the WAL safely (tailing readers, recovery)
  MUST cap its position at `committed.Load()`. Bytes past that offset are
  not yet durable and may not even be visible to other goroutines yet.
- The writer mutex MUST be acquired for any operation that mutates
  `nextMsgID` or the file (Append, Close, future Truncate).
- Future code paths that want batched fsync (e.g., a config option for
  higher throughput at the cost of durability) must still respect the
  invariant that `committed` only advances after `fsync` returns.
