# 0018 — Dedupe LRU recovery from the WAL

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `internal/wal/`, `internal/broker/`
**Closes:** the dedupe durability gap flagged in [ADR 0013](./0013-pkg-client-architecture.md)
**Related:** [ADR 0006](./0006-debounced-atomic-offsets.md) (offsets persistence — the pattern we chose *not* to mirror here)

## Context

The per-topic dedupe LRU (`internal/broker/dedupe.go`) maps a producer's
dedupe key to the `MsgID` of the original publish, so a re-publish of the
same key returns the original id instead of appending a second record. It was
in-memory only: on restart it came back **empty**. A producer that retried a
publish across a broker restart (e.g. after its own crash) therefore got a
**fresh** `MsgID` and a new WAL append — the consumer saw a duplicate. This is
the durability gap ADR 0013 called out.

The v2 roadmap originally proposed a sidecar `dedupe.json`, debounced and
atomically swapped like `offsets.json` (the ADR 0006 pattern). We rejected
that in favour of rebuilding the LRU from the WAL during recovery. Two
independent reasons.

### 1. A sidecar cannot actually close the gap

Every publish already writes its `DedupeKey` into the WAL record (ADR 0001
frame format), and recovery (ADR 0003) already decodes **every** record on
`Open`. The WAL is thus the durable source of truth for key→MsgID. A sidecar
is necessarily a *lossy cache* of that truth: it lags the WAL by its debounce
window, so a crash between a dedupe insert and the next flush drops the key —
and a re-publish after restart produces exactly the duplicate we set out to
prevent. To reach "zero duplicates" a sidecar would have to reconcile against
the WAL on load regardless, at which point it earns nothing but a second fsync
path and a duplicate persistence subsystem (dirty flag, debounce loop, atomic
write).

### 2. Forward-compatibility with `toyraft` (v3)

v3 makes `toymq` a multi-node broker on top of `toyraft`, whose
`raft.StateMachine` model puts the **replicated log as the source of truth**:
each node materialises local state by `Apply`-ing committed entries in index
order, and reconstructs it via `Snapshot()`/`Restore()`. A per-node sidecar
`dedupe.json` is state Raft never replicates — a follower rebuilding from the
log (or a leader-shipped snapshot) must reconstruct dedupe **from the log**,
not from a file that was never part of the replicated stream. Rebuild-from-WAL
is exactly the seam v3 needs; a sidecar would be torn out.

## Decision

Rebuild the dedupe index from the WAL during the recovery scan that already
happens on `Open`. No new file, no debounce, no dirty flag, no atomic write,
no new fsync.

- **WAL visitor.** `wal.Open` accepts `wal.WithRecoveryVisitor(func(Record))`,
  invoked once per **valid** record during `recover()`, in ascending `MsgID`
  order. Records beyond a torn tail (which get truncated) are never visited.
  The default (no option) is a behavioural no-op — existing callers are
  unaffected. The WAL package stays ignorant of dedupe semantics; it only
  offers "visit each recovered record."
- **Broker rebuild seam.** `getOrCreateTopic` builds the `DedupeIndex` first,
  then passes a visitor that funnels each record through
  `rebuildIndexes(dedupe, rec)` — a single, deterministic, I/O-free
  materialisation function. Recovery runs inside `broker.New`, which completes
  before `server.Serve` accepts connections (`cmd/toymq/main.go`), so the
  index is fully populated before any client can publish.

Because records replay in ascending `MsgID` order, rebuilding a cap-`N` index
from more than `N` keyed records naturally retains the most-recent `N` — the
same set the live LRU would hold. Eviction is correct by construction.

## Consequences

**Positive**
- Zero-gap correctness: anything durably in the WAL is deduped after restart.
  There is no debounce window in which a key can be lost.
- Far less code than a sidecar: one option + one visitor + one replay function.
  No second persistence subsystem to keep in sync with offsets.
- Near-zero added startup cost: the recovery scan already decodes every
  record; we stop discarding the keys.
- The `rebuildIndexes` seam is the reuse point for a future
  `raft.StateMachine.Restore` / snapshot application in v3.

**Negative / trade-offs**
- LRU *recency* ordering after restart is WAL insertion order, not true
  pre-crash access order. This only changes which key is evicted first *going
  forward* — never correctness. Every rebuilt entry maps to its real original
  `MsgID`.
- Startup cost is O(records in the segment). Acceptable today (single segment
  per topic, ADR 0002/0003 already pay a full scan). If log segmentation or
  compaction lands later, the rebuild should read only the tail / latest
  snapshot — noted, not needed yet.

## Forward-compat note (v3 / toyraft) — recorded, not built

While touching the publish/recovery path, one determinism hazard was found and
is captured here so v3 does not inherit it silently:

- `internal/broker/topic.go` sets `rec.TsNs = time.Now().UnixNano()` inside the
  publish path, and `MsgID` is assigned by a local monotonic counter in
  `wal.Append`. If `Publish` becomes `raft.StateMachine.Apply` in v3, both
  fields must be **deterministic across nodes** — decided by the proposer/
  leader and carried in the proposed command `Data`, never generated inside
  `Apply` (toyraft requires `Apply` to be wall-clock-free and deterministic).
  Out of scope for M1; owned by v3 M1 (embed toyraft; pick the WAL ↔ Raft-log
  invariant).

## Edge cases

- **Missing / empty segment at startup.** Recovery visits nothing; the index
  starts empty. Identical to a fresh topic.
- **Unkeyed records.** `rebuildIndexes` skips records with an empty
  `DedupeKey`; only keyed publishes populate the index.
- **Torn trailing frame.** The truncated record is not visited, so a partially
  written publish never seeds a dedupe entry — matching the WAL's own
  truncate-on-recovery contract (ADR 0003).
- **More keyed records than `cap`.** Ascending-`MsgID` replay evicts the oldest
  first; the retained set equals the live LRU's, so eviction survives restart.

## Usage

```go
dedupe := NewDedupeIndex(cap)
log, err := wal.Open(topicDir, wal.WithRecoveryVisitor(func(rec wal.Record) {
    rebuildIndexes(dedupe, rec)
}))
```

Wire impact: **none**. This is a pure durability fix behind the existing
protocol.
