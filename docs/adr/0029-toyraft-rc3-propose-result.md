# 0029 — toyraft rc.3 bump: Propose returns the apply result; nonce registry removed

**Status:** Accepted
**Date:** 2026-09-11
**Scope:** `internal/replication/`, `internal/broker/`, `cmd/toymq/`, `go.mod`
**Supersedes:** [ADR 0028](./0028-raft-embedding-command-envelope.md) §Decision 3 (the nonce-result registry)
**Related:** [ADR 0028](./0028-raft-embedding-command-envelope.md) (raft embedding)

## Context

toyraft `v1.0.0-rc.3` shipped, closing all six findings toymq's v3 M1 filed
(`docs/TOYRAFT-MIGRATION-REPORT.md`). Two change what the embedding needs:

- **B1 — the in-memory log is restored on restart** (`restoreLogFromStorage`).
  On rc.2 a restarted node's `LastIndex()` read 0 while `commitIndex` recovered,
  so it replayed but dropped every new `Propose` (`ErrProposalDropped`). rc.3
  restores the log, so a restarted node accepts writes again.
- **F3 — `Propose` now returns the apply result.** The signature is
  `Propose(ctx, data) (Index, Term, any, error)`, where `any` is
  `StateMachine.Apply`'s return value, delivered once the entry is applied on
  this node.

ADR 0028 §3 built a nonce→channel `ResultRegistry` precisely because rc.2's
`Propose` discarded the `Apply` result, leaving no way to return a PUB's
WAL-assigned MsgID. rc.3's F3 removes that need.

## Decision

1. **Bump the pin** `v1.0.0-rc.2 → v1.0.0-rc.3`.
2. **Read the MsgID from `Propose`'s result; delete the nonce registry.**
   `proposePublish` reads the third return and asserts it to
   `replication.ApplyResult` (PUB's `Apply` returns one); `proposeMutation`
   (ACK/NACK/CREATE, whose `Apply` returns `nil`) discards it. Deleted:
   `internal/replication/registry.go` (+test), the `Envelope.Nonce` field, the
   `BrokerSM.reg`/`Registry()` plumbing, and the broker's `resultReg`.
   `AttachRaft(node)` loses its registry parameter. `ApplyResult` moved to
   `statemachine.go`.
3. **Keep the WAL-embedded `RaftIndex` idempotence guard** (ADR 0028 §5).
   rc.3's B2 durable checkpoint calls `StateMachine.Snapshot()`, but a SM that
   returns `ErrSnapshotUnsupported` opts out and recovers by full-log replay
   (`pkg/raft/apply.go` `checkpoint`, `node_public.go` New). toymq's `brokerSM`
   still stubs Snapshot/Restore — real broker-state serialization is v4 (ADR
   0028 §6) — so on restart it still replays the whole committed log and would
   double-apply without the guard. The guard is therefore **not** dead code; it
   goes only when broker snapshots land.
4. **Keep the no-op single-node transport** (ADR 0028 §7). rc.3 fixed the
   inproc nil-clock (F4) and http self-only-cluster (F5) validation, but for a
   single node the no-op transport is still the zero-dependency choice (no
   clock, no listener, no port). M2 swaps to `pkg/transport/http` with peers.

## Consequences

- The replicated MsgID return path is now a direct `Propose` return — one fewer
  moving part, no per-command nonce allocation, no result-channel bookkeeping.
- **Full crash-durability now passes** for single-node `--replicate`: a node
  restarts, replays without duplicating (guard), and **accepts new writes**
  (B1). `TestReplicatedRestartRecovery` asserts a post-restart publish returns
  the correct next MsgID — the criterion that was blocked on rc.2 and deferred
  in M1.
- The `Envelope` wire format dropped its `Nonce` field. This is safe: the
  envelope lives only inside a raft `Entry` within one running cluster — it is
  never persisted across versions, and it was never part of `wal.Record`, so
  WAL bytes are unchanged.
- v3 M2 (multi-node) is unblocked: its kill-leader / partition-heal criteria
  depend on B1, now fixed.
- Remaining upstream dependency: broker-side `Snapshot`/`Restore` (v4) to retire
  the `RaftIndex` guard and gain log compaction.
