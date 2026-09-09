# 0028 — Raft embedding, command envelope & the WAL↔Raft-log invariant

**Status:** Accepted
**Date:** 2026-09-10
**Scope:** `internal/replication/`, `internal/broker/`, `internal/wal/`, `cmd/toymq/`
**Related:** [ADR 0018](./0018-dedupe-recovery-from-wal.md) (dedupe/WAL recovery — this ADR corrects its MsgID-move note), [ADR 0021](./0021-partitions-single-node.md) (partitions), [ADR 0025](./0025-delayed-messages.md) (VisibleAtNs append-only field)
**Milestone:** v3 M1 — single-node replicated path

## Context

v3.0 makes toymq a replicated broker on top of
[`toyraft`](https://github.com/prajwalmahajan101/toyraft), pinned at
`v1.0.0-rc.2`. M1 builds the foundational seam: `toymq --replicate` routes every
*mutating* command through `raft.Propose → StateMachine.Apply`, while
standalone mode stays byte-for-byte identical to v2. M1 owns the determinism,
apply-once, and idempotent-replay guarantees; multi-node, election, and peer
transport are M2.

The core model (from ADR 0018): **the raft log is the source of truth; the WAL
is local applied-state durability.** A mutating command becomes
`Propose(envelope) → toyraft replicates → Apply mutates broker state + appends
WAL → ack`.

## Decision

### 1. A deterministic apply seam, split from non-deterministic resolve

`Partition.publishCtx` mixed three concerns: routing, wall-clock stamping, and
the deterministic dedupe+`wal.Append` mutation. We split the deterministic tail
into `applyPublish(ctx, tsNs, visibleAtNs, raftIndex, dedupeKey, payload, m)` —
no clock, no routing. The leader resolves the non-deterministic values
(partition via `topic.route`, `TsNs` from the clock, `VisibleAtNs` from
`delayMs`) once, before `Propose`; every node's `Apply` then calls the same
`applyPublish` with those pinned values. Ack/Nack get the same treatment:
`applyAck`/`applyNack` carry the durable state mutation, while the per-session
`sendCh` redelivery push stays local to the leader. Standalone and replicated
paths call the *same* apply body, so the emitted WAL bytes are identical.

### 2. MsgID stays WAL-assigned at Apply

The MsgID is a per-partition `wal.Log.nextMsgID` counter. Because `Apply` runs
entries in log-index order on every node, the counter advances identically
everywhere — deterministic for free. **This corrects the ADR 0018 note** that
MsgID assignment "must move to the proposer": only the wall-clock and the
keyless round-robin are non-deterministic and move to the proposer; the MsgID
does not.

### 3. A nonce-result registry returns the MsgID

`raft.Node.Propose` returns `(Index, Term, error)` and **discards** `Apply`'s
`any` result, so the WAL-assigned MsgID cannot come back through `Propose`. The
leader stamps a nonce in the envelope, registers a buffered channel, Proposes,
and — because `Propose` blocks until `Apply` has run on this node — reads the
MsgID back from the channel that `Apply` resolves. On any non-leader node (or a
replay) the nonce matches no waiter and the result is dropped. ACK/NACK/CREATE
carry nonce 0 (no result needed).

### 4. A binary command envelope in `internal/replication`

The command travels in `raft.Entry.Data` as a version-prefixed, length-framed
binary `Envelope` (mirroring the `internal/wal` codec style), decoded by a
`brokerSM` implementing `raft.StateMachine`. The package consumes a small
`ApplySurface` interface the broker satisfies, keeping `raft` out of the
broker's core files and `broker` out of `replication` (no import cycle). A
decode failure panics (fatal log corruption per the `Apply` contract); a broker
mutation error is returned to `Propose` without poisoning the node.

### 5. Idempotent restart replay via a WAL-embedded raft index

toyraft rc.2 keeps its applied index in memory only and stubs snapshots, so on
restart it replays the **entire** committed log through `Apply`. The broker WAL
*also* recovers independently, so a naive replay double-writes every message. We
close this by stamping each replicated record with its committing
`RaftIndex` — an append-only `wal.Record` field written **only when non-zero**,
so standalone records (index 0) stay byte-identical to v2. On recovery the
broker takes the maximum `RaftIndex` across all partitions as an applied
high-water; `ApplyPublish` skips any PUB at or below it. Sequential apply under
ordered fsync makes the global maximum a safe prefix high-water.

### 6. Snapshot/Restore stay stubbed

`brokerSM.Snapshot`/`Restore` return `raft.ErrSnapshotUnsupported` per the rc.2
v1 contract. Real broker-state serialization is deferred to v4 (toyraft UP-1);
`rebuildIndexes` (`internal/broker/partition.go`) is the recorded reuse point.

### 7. Single-node transport is a local no-op

Neither shipped toyraft transport can build a self-only cluster (see
Consequences), so M1 supplies `replication.NewSingleNodeTransport` — a no-op
`raft.Transport`. A single-node cluster never Sends to a peer, so Send/Register
are no-ops. M2 replaces it with `pkg/transport/http` and real peers.

## Consequences

- Standalone mode is provably unchanged: the WAL bytes and the full integration
  matrix are identical (`RaftIndex` is omitted when zero).
- Determinism, apply-once/ordering, and idempotent replay are covered by tests
  under `-race` (`internal/broker`, `internal/replication`).
- **Two upstream toyraft blockers surfaced during integration** (tracked in
  [`TOYRAFT-MIGRATION-REPORT.md`](../TOYRAFT-MIGRATION-REPORT.md)):
  1. **Single-node transport unbuildable.** `inproc.HubConfig.Clock` is typed
     against toyraft's `internal/clock` (an external module cannot construct
     it), and `http.Config.Validate` rejects both an empty `PeerURLs` and
     self-in-`PeerURLs` — so a `peers=[self]` cluster has no shipped transport.
     Worked around with a local no-op transport.
  2. **Log not restored on restart.** `newNode` starts with an empty in-memory
     log and loads only `HardState`, so after restart `LastIndex()==0` while
     `commitIndex` is recovered. Replay-apply works (it reads Storage directly),
     but a new `Propose` appends at index 1 ≤ commitIndex and is dropped
     (`ErrProposalDropped`). A restarted single node can therefore recover and
     replay but **cannot accept new writes**. This blocks the full
     SIGKILL-and-keep-serving crash-durability criterion in rc.2; toymq's
     idempotent-replay fix (Decision 5) is correct and verified against replay,
     but end-to-end write-after-restart waits on an upstream fix.
- The nonce registry is a direct consequence of `Propose` discarding the
  `Apply` result — a candidate upstream feature request (return the apply
  result, or expose a typed result channel).
