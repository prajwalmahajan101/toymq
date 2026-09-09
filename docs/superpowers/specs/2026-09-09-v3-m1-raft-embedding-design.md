# v3 M1 — Raft embedding + single-node replicated path (design)

**Milestone:** v3 M1 (`docs/ROADMAP.md` §"v3 M1")
**Branch:** `feat/toyraft-embed`
**ADR:** new **0028** — Raft embedding, command envelope & WAL↔Raft-log invariant (references/extends 0018)
**Status:** design approved 2026-09-09, pending implementation plan

## Goal

Single-node replicated path: `toymq --replicate` flows every *mutating* command
through `raft.Propose → StateMachine.Apply`. Standalone (non-`--replicate`) mode
stays byte-for-byte identical to v2. This is the foundational seam; it owns the
determinism + apply-once + crash-durability tests. No multi-node, no election,
no peer transport wire-up beyond what a single-node cluster needs (that is M2).

## Locked decisions

1. **Raft log = source of truth; WAL = local applied-state durability + (future)
   snapshot artefact.** A mutating command becomes
   `Propose(envelope) → toyraft replicates → Apply mutates broker state + appends
   WAL → ack`.
2. **toyraft pinned at `v1.0.0-rc.2`.** rc.1→rc.2 diff is `pkg/transport/http`
   only (nil-clock fix); zero `pkg/raft` API change. Roadmap's `rc.1` references
   updated to `rc.2` as part of this milestone.
3. **MsgID stays WAL-assigned at Apply.** It is a per-partition counter
   (`wal.Log.nextMsgID`, `log.go:214`); under Raft, Apply runs entries in
   log-index order on every node, so the counter advances identically everywhere
   — deterministic for free. **This corrects the ADR 0018 note** ("assigns MsgID
   from a local counter — must move to the proposer"): only the wall-clock
   (`TsNs`) and the keyless round-robin (`Topic.rr`) are truly non-deterministic
   and must move to the proposer.
4. **PUB recovers its MsgID via a nonce-result registry**, not a proposer-side
   MsgID reservation. `raft.Propose` returns `(Index, Term, error)` and
   **discards** `Apply`'s `any` result (`node_public.go` `proposeResult.res` is
   never returned to the caller), so the assigned MsgID cannot come back through
   `Propose`. The leader stamps a nonce in the envelope; `Apply` writes
   `{msgID, dup}` into a per-nonce channel; the handler reads it after `Propose`
   returns (guaranteed present — `Propose` blocks until applied).
5. **Snapshot/Restore stay stubbed** (`ErrSnapshotUnsupported`, per the rc.2 v1
   contract). No broker-state serialization is built in M1 — real snapshots are
   toyraft UP-1 / toymq v4. `rebuildIndexes` (`partition.go:82`) is recorded as
   the future reuse point.
6. **ADR form: new 0028**, referencing 0018, rather than editing 0018 in place
   (ADRs are immutable records; matches the existing supersede pattern).

## The integration seam

Today's mutation chokepoint (publish):

```
server session → broker.PublishCtx → topic.route → partition.publishCtx → wal.Log.Append
```

`partition.publishCtx` (`partition.go:92`) mixes three concerns:
routing (via `topic.route`), non-deterministic stamping (`time.Now()`,
`partition.go:102`), and the deterministic mutation (dedupe-check + `wal.Append`).

**Refactor — split resolve from apply.** Extract the deterministic tail:

```go
// deterministic: no clock, no routing. Runs on every replica in Apply,
// and inline on the standalone path.
func applyPublish(p *Partition, tsNs, visibleAtNs uint64,
    dedupeKey string, payload []byte, m *metrics.Metrics) (msgID uint64, dup bool, err error)
```

- `applyPublish` does: dedupe `Lookup` (early-return original MsgID on hit),
  build `wal.Record{TsNs, DedupeKey, Payload, VisibleAtNs}`, `wal.Append`
  (assigns MsgID), dedupe `Insert`, metrics. **No `time.Now`.**
- The leader-side `resolve()` does the non-deterministic work: `topic.route`
  (partition selection incl. `rr`), `now := time.Now()`, resolve `VisibleAtNs`
  from `delayMs`.

Two entry points, one mutation body:

| Path | Flow |
|---|---|
| standalone (`b.raft == nil`) | `resolve()` → `applyPublish()` inline (identical bytes to v2) |
| replicated (`b.raft != nil`) | leader `resolve()` → `Envelope` → `Propose` → `Apply` → `applyPublish()` on every node |

`wal` needs **no API change**: `Record.TsNs` / `VisibleAtNs` are already
caller-populated fields; `applyPublish` sets them from the envelope instead of
the clock. Ack/Nack get the same treatment — extract the deterministic state
mutation (offset advance / attempts bump / DLQ append) from the local delivery
action (the `sendCh` redelivery push, which stays local).

## New package: `internal/replication`

Isolated so the broker depends on a small, testable surface and `raft` types
stay out of the broker's core files.

- **`envelope.go`** — `Envelope` struct + `Encode`/`Decode` (length-prefixed
  binary, version byte first, mirroring `internal/wal` codec style). Round-trip
  unit-tested.
- **`statemachine.go`** — `brokerSM` implementing `raft.StateMachine`:
  - `Apply(entry)`: decode envelope → dispatch on `Kind` → call the matching
    deterministic broker apply-method (`applyPublish` / `applyAck` / `applyNack`
    / `applyCreateTopic`) → if a waiter is registered for `entry`'s nonce, send
    the result. Deterministic, wall-clock-free. Panics on decode failure
    (per the `Apply` contract: Apply errors are fatal unless app-level rejects).
  - `Snapshot`/`Restore`: return `raft.ErrSnapshotUnsupported`.
- **`registry.go`** — `resultRegistry` = `sync.Map[uint64]chan applyResult`.
  `register(nonce) chan`, `resolve(nonce, res)` (no-op if absent — followers /
  replay). Nonce source: a monotonic `atomic.Uint64` on the leader.

The broker holds `raft raft.Node` (nil in standalone) and a `*resultRegistry`.

## Envelope

```go
type Kind uint8
const ( KindPublish Kind = iota; KindAck; KindNack; KindCreateTopic )

type Envelope struct {
    Version     uint8
    Kind        Kind
    Nonce       uint64
    Topic       string
    Partition   int      // leader-resolved (replaces Topic.rr non-determinism)
    DedupeKey   string
    TsNs        uint64   // leader-stamped
    VisibleAtNs uint64   // leader-resolved from delayMs
    ConsumerID  string   // Ack / Nack
    MsgID       uint64   // Ack / Nack target
    Payload     []byte
    Partitions  int      // CreateTopic
}
```

**Command classification.**

| Command | Replicated? | Rationale |
|---|---|---|
| `PUB` | Propose | mutates the log |
| `ACK` | Propose | advances the durable consumer offset (failover-visible state) |
| `NACK` | Propose (state only) | attempts bump / DLQ append are state; the `sendCh` redelivery push stays local |
| `CREATE` (topic) | Propose | topic-admin; deterministic partition count |
| `SUB` | local | delivery, per-session |
| `HELLO`/`AUTH`/`PING`/`INFO`/`PAUSE`/`RESUME` | local | handshake / read / per-connection control |

## MsgID return flow (nonce registry)

```
leader PUB handler:
  nonce := reg.next()
  ch    := reg.register(nonce)
  env   := Envelope{Kind:Publish, Nonce:nonce, Partition:resolved, TsNs:now, ...}
  _, _, err := node.Propose(ctx, Encode(env))   // blocks until Apply returns
  if err != nil { return err }                   // ErrNotLeader → MOVED (M3); single-node never hits
  res := <-ch                                     // present: Propose returned post-Apply
  return OK res.MsgID (dup=res.Dup)

Apply (every node):
  env := Decode(entry.Data)
  id, dup, _ := applyPublish(part(env), env.TsNs, env.VisibleAtNs, env.DedupeKey, env.Payload)
  reg.resolve(env.Nonce, {id, dup})   // no-op when no waiter (follower / replay)
```

## `cmd/toymq` wiring (single-node)

New flags (config.go): `--replicate` (bool, default false), `--node-id`
(string, default `n1`), `--raft-dir` (string, default `<data-dir>/raft`).

When `--replicate`, assemble like `toyraftd/main.go`:

```go
store, _ := filestorage.New(cfg.RaftDir)          // pkg/storage/file
tr       := inproc.New(nodeID)                     // pkg/transport/inproc — self only
sm       := replication.NewBrokerSM(b)             // broker adapter
node, _  := raft.New(raft.Config{
    NodeID: raft.NodeID(cfg.NodeID),
    Peers:  []raft.NodeID{raft.NodeID(cfg.NodeID)}, // [self] → trivially leader
    Storage: store, Transport: tr, StateMachine: sm,
})
node.Start(ctx)
b.AttachRaft(node)                                 // broker switches mutating methods to Propose
```

`inproc` is enough single-node (no peer plane); M2 swaps in
`pkg/transport/http` + the async transport wrapper. Standalone path: none of
this runs, `b.raft == nil`.

## Standalone preservation

A single nil-check branch at the top of each mutating broker method
(`PublishCtx`/`AckCtx`/`NackCtx`/`CreateTopic`) selects direct vs Propose. No
strategy interface, no factory — three branches (ponytail: an interface with
one real alternative is not worth it). The standalone branch is the current
code with `resolve()`+`applyPublish()` substituted for the inline body, so the
emitted WAL bytes are unchanged.

## Testing (M1 owned risk)

1. **Determinism** (`internal/replication` unit or `internal/integration`) —
   feed the same mutating-command stream to two fresh brokers via Apply; assert
   byte-identical WAL segments + dedupe LRU contents + consumer offsets.
2. **Apply-once / ordering** — single-node `--replicate`: each mutating command
   round-trips `Propose→Apply` exactly once, in strict `entry.Index` order; no
   double-apply; the returned MsgID matches the WAL record.
3. **Crash-durability** — existing v1/v2 durability suite + `test/chaos`
   (`CHAOS_*`) run green with `--replicate` (SM in the loop): every `OK`'d PUB
   survives SIGKILL + restart-recover.
4. **Standalone unchanged** — the full `internal/integration` matrix
   (`m8_matrix_test.go` etc.) passes with no `--replicate`, proving the refactor
   is byte-for-byte transparent.

All under `-race`.

## Exit criteria

- `toymq --replicate` (single node) serves every mutating command via
  `Propose→Apply`; broker state matches standalone.
- Determinism + apply-once + crash-durability tests green under `-race`.
- Standalone integration matrix unchanged.
- ADR 0028 written; roadmap `rc.1`→`rc.2` references updated; migration-report
  scaffold gets its first M1 findings entry (API friction: `Propose` discards
  the Apply result → forced the nonce registry; a candidate upstream feature
  request).

## Out of scope for M1 (later milestones)

- Multi-node, election, peer transport, async transport wrapper → M2.
- `MOVED`/redirect, read model → M3.
- `WAIT`, `INFO replication` → M4.
- Snapshot/compaction serialization → v4 (toyraft UP-1).
