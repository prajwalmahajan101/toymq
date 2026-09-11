# 0031 — Partition-heal + linearizability harness (v3 M2b)

**Status:** Accepted
**Date:** 2026-09-11
**Scope:** `internal/broker/` (test-only)
**Related:** [ADR 0030](./0030-cluster-mode-peer-transport.md) (M2a distributed core), [ADR 0028](./0028-raft-embedding-command-envelope.md) (raft embedding)

## Context

v3 M2a (ADR 0030) shipped the distributed core and proved failover with a leader
**kill** (`node.Stop()`). Two M2 exit items were deferred to M2b
(`docs/ROADMAP.md:458`): **partition-heal** (reconcile divergent tails with zero
acked-write loss) and a **Jepsen/Porcupine-style linearizability harness** on
`PUB`/consume under partition churn, run under `-race`. Pass criterion: no acked
PUB lost, no double-ack accepted as a unique consume.

A kill and a partition are not the same fault. `node.Stop()` removes one side, so
it can never produce **split-brain**: two live sides, one of which lacks quorum.
Only a partition — both sides alive, cross-group traffic dropped — exercises the
minority-rejects-writes invariant and the divergent-tail reconciliation that heal
must repair. M2b therefore needs a fault the M2a harness could not express.

## Decision

1. **The partition seam is test-only, not a production transport method.** A
   `partitionableTransport` decorator (`internal/broker/cluster_partition_test.go`)
   wraps the transport `replication.NewHTTPTransport` returns, with an atomic
   blocked-peer set. `Send` drops to a blocked `msg.To`; `Register` wraps the
   inbound step to drop from a blocked `msg.From` — a **bidirectional** partition
   (a one-way drop would let the isolated node still hear the majority and never
   campaign). Both drops are silent returns, honouring raft's best-effort `Send`
   contract exactly like a lost heartbeat. Fault injection is a test concern; a
   production `Block/Unblock` API would add surface no shipping code consumes, so
   the seam lives in the test package. `newCluster` wraps every node and stores
   the handle on `clusterNode.part`; the decorator is a pass-through when nothing
   is blocked, so the M2a election/replication path is byte-identical.

2. **Split-brain is proven by an isolated-leader write that must fail.**
   `TestClusterPartitionHealNoLoss` isolates the current leader alone (minority
   of 1) from the majority of 2. A `PublishCtx` on the isolated leader must
   **not** succeed: its `Propose` appends locally but never reaches quorum, so it
   blocks until the context fires and returns `ctx.Err()` (toyraft
   `node_public.go` selects on `<-ctx.Done()`). On heal the minority's
   uncommitted tail is truncated by raft log-matching and it converges on the
   majority head — the phantom write vanishes and was never acked, so zero acked
   writes are lost.

3. **Linearizability is checked over writes; reads drain post-heal.** The harness
   models the single partition as a message-queue register: each MsgID is
   published (leader-acked) exactly once and consumed exactly once. Concurrent
   publishers run while a churn goroutine isolates **one** node at a time and
   heals — a single isolation always leaves a majority, so the cluster keeps
   making progress. Only leader-acked publishes enter the history; after the
   churn the cluster converges and the acked set is drained as the consume
   sequence. `porcupine.CheckOperations` (porcupine `v1.0.3`) must find the
   history linearizable — a duplicate MsgID assignment or a double-consume makes
   it fail. **No acked PUB lost** is proven separately by `waitConverged` to the
   highest acked MsgID on every node. Reads are the leader-local, non-linearizable
   surface by design (ADR 0030 §3), so the harness consumes from the converged
   log rather than asserting live read linearizability.

4. **A ctx-cancelled publish is not a lost write.** A `Propose` that returns
   `ctx.Err()` may still commit later (toyraft: "entry MAY still commit"). Such an
   id is in the log but not in the acked set, so the harness checks the acked set
   is a **subset** of the converged log (no acked write lost), never strict
   equality — a committed-after-timeout write is at-least-once from the client's
   view, a documented raft property, not a bug.

## Consequences

- v3 M2 exit criteria are met: `TestClusterPartitionHealNoLoss` and
  `TestClusterLinearizablePubConsume` are green under `-race`; the M2a suite
  (`TestClusterReplicatesToAllFollowers`, `…FollowerRejectsWrite`,
  `…SurvivesLeaderKill`) is unchanged.
- `porcupine v1.0.3` becomes a direct (test) dependency. It was already pinned in
  `go.sum`; `go mod tidy` promotes it out of `// indirect`.
- No production code changed. Standalone and `--replicate` runtime paths are
  untouched; the decorator only exists in `_test.go`.
- The partition seam is reusable by any future cluster fault test (e.g. M4 `WAIT`
  against a partitioned follower) without touching `internal/replication`.
- Multi-partition linearizability and a full concurrent-consumer live history stay
  out of scope — single-writer replication plus drain-after-heal meets the stated
  M2 pass criteria; the broader harness is a v4 concern.
