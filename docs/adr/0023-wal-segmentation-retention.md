# 0023 — WAL segmentation + retention

**Status:** Accepted
**Date:** 2026-07-07
**Scope:** `internal/wal/`, `internal/broker/`, `internal/config/`, `internal/server/`, `cmd/toymq`

## Context

Each partition's WAL was a single hard-coded file (`<partition>/000000.log`).
It only ever grew: a long-running topic filled the disk with no way to reclaim
space short of stopping the broker and deleting data by hand. v2 M6 adds the
three production patterns the roadmap calls for — **retention** (bound disk),
dead-letter queue, and delayed messages. Retention as worded ("oldest WAL
segments dropped past the limit") presumes a WAL split into droppable pieces,
which did not exist.

Two constraints shaped the design:

1. **Retention needs a unit of reclaim.** You cannot free the head of an
   append-only file without rewriting it. The natural unit is a *segment*: a
   rolling series of numbered files, where whole sealed segments below a floor
   are deleted outright.
2. **v3/toyraft forward-compat (ADR 0018).** Under v3 the Raft log is the source
   of truth and the WAL becomes a local materialised view. `StateMachine.Apply`
   must stay deterministic — no wall-clock, no I/O-dependent branching in the
   replicated transition. So the age/size *trigger* for dropping data must live
   on the local materialisation side, never inside Apply.

## Decision

Turn the single WAL file into a rolling set of numbered segments and add a
background retention sweeper on the broker.

### Segmented `wal.Log`

A `Log` owns an ordered `[]*segment`; the last is the active (writable) one, the
rest are sealed and immutable. Each `segment` carries `index`, `path`,
`baseMsgID` (first record's MsgID), `baseByteOffset` (where it starts in the
logical byte stream), and `maxTsNs` (newest record time, for age retention).

**Logical offsets stay global on the `Log`.** `written`, `committed`, and
`nextMsgID` describe the whole byte stream and remain the coordination fields
the group committer and readers already use; a segment's length is derived from
`baseByteOffset` rather than duplicated as mutable per-segment state. This kept
the T1 extraction behaviour-preserving — the single-segment path (`000000.log`,
recovery, reader) is byte-for-byte unchanged, so pre-M6 data dirs open exactly
as before.

- **Rotation** (`WithSegmentBytes(n)`): when the active segment already holds a
  record and the next one would carry it past `n` bytes, `Append` seals it on
  the **record boundary** and rolls to a fresh segment. MsgIDs stay monotonic
  across the boundary; a lone record larger than `n` still lands whole in its
  own segment. `n == 0` disables rotation (one ever-growing segment) — the
  pre-M6 default.
- **Rotation is durable-before-seal.** `rotate` fsyncs the active segment and
  advances `committed` before sealing, so a sealed segment is always fully
  durable regardless of `SyncMode`. The sealed segment **keeps its fd open**
  until Close/retention: a concurrent group-commit `flush` may have snapshotted
  the old segment's `Sync`, and closing the fd out from under it would turn a
  harmless stray fsync into a spurious durability failure. `flush` snapshots
  `syncFn` under the lock since rotation repoints it.
- **Multi-segment recovery**: Open discovers `NNNNNN.log` files (ascending;
  the prefix need not start at 0 after retention) and `recover` scans them in
  order, rebuilding each segment's `baseMsgID`/`baseByteOffset`/`maxTsNs` and
  emitting the recovery visitor once per record in global MsgID order (ADR 0018
  unaffected). **Torn-tail truncation applies only to the active segment**; a
  bad frame in a sealed segment is a hard error, since sealed segments are
  fsynced in full before sealing and cannot carry an interrupted write.
- **Reader spans segments**: `NewReader(fromMsgID)` locates the segment whose
  MsgID range contains `fromMsgID`, opens its own fd, and scans to the target.
  `Next` decodes within a segment, rolls to the successor at the end of a sealed
  one, and tails the active segment on the cond-var — so a reader crosses
  rotation boundaries and follows a live rotation transparently. A start MsgID
  below the oldest retained segment's base returns `ErrOutOfRange`.

### Retention sweeper (broker)

`runRetentionLoop` is a background peer of `runRedeliverLoop`. Each tick, per
partition, it computes a **drop floor** from the size/age policy and asks the
WAL to drop every sealed segment below it (`DropSegmentsBefore` deletes files,
closes fds, returns the new retained-floor MsgID). A segment is evicted if
**either** policy would drop it (keep only what both retain); the active segment
is never touched. It runs on the **local materialised WAL only** — the sweep is
a non-deterministic local trigger, not part of Apply.

`RetentionConfig` (`SegmentBytes` / `RetainBytes` / `RetainDuration` /
`Interval`) threads through the broker constructors; the zero value keeps pre-M6
behaviour. CLI: `--segment-bytes`, `--retain-bytes`, `--retain-duration` (the
retain-* flags require `--segment-bytes > 0` — there must be sealed segments to
reclaim).

### Consumer start vs the retained floor

Retention can drop data a consumer has not read. On SUB the start point is
resolved against the floor:

- A **fresh** consumer (never acked) starts **at the floor** (earliest
  retained), so a trimmed partition stays subscribable.
- A **resuming** consumer whose next offset (`lastAcked+1`) fell **below** the
  floor lost un-consumed data and gets wire **`ERR OUT_OF_RANGE`** rather than a
  silent skip. The session runs `SubStartCheck` synchronously before the SUB
  `OK` so the error frame precedes any delivery; a rare trim-after-check race
  falls back to the strict `NewReader` guard (delivery simply does not start).

## Consequences

**Positive**
- Disk use per partition is bounded by size and/or age; old segments are
  reclaimed automatically while the broker runs.
- Segmentation is opt-in and additive: omit `--segment-bytes` and the WAL is
  the pre-M6 single file, recovering and serving identically.
- A consumer that outran retention learns so explicitly (`OUT_OF_RANGE`).

**Negative / trade-offs**
- Retention can drop unacked data by design; the consumer then gets
  `OUT_OF_RANGE` on resume. The one carve-out (an un-fired delayed record's
  segment) is handled in PR3.
- Sealed segments keep an open fd until dropped; fd count is bounded by the
  number of retained segments (retention keeps it small).
- Rotation adds a per-boundary fsync + file create; negligible against the
  amortised append cost.

## v3 / toyraft forward-compat

Segment boundaries are the natural **snapshot / compaction reference** for
toyraft (roadmap v3 UP-1): a sealed segment is a candidate snapshot artefact,
and the retained floor is a compaction low-water mark. Crucially, the reclaim
*trigger* (age/size sweep) lives entirely in the local sweeper over the
materialised view — it never enters `StateMachine.Apply`, so replication stays
deterministic (ADR 0018). No determinism debt is added.

## Usage

```
# Cap segments at 64 MiB and keep at most 512 MiB (or 24h) per partition:
toymq --segment-bytes 67108864 --retain-bytes 536870912
toymq --segment-bytes 67108864 --retain-duration 24h

# Both together: a segment is dropped when either bound would evict it.
toymq --segment-bytes 67108864 --retain-bytes 536870912 --retain-duration 24h
```

Wire (additive): a SUB whose start MsgID is below the retained floor →
`ERR OUT_OF_RANGE`.
