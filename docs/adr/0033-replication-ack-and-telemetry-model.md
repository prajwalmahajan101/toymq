# 0033 — Replication acknowledgement (`WAIT`) & cluster telemetry model (v3 M4)

**Status:** Accepted
**Date:** 2026-09-13
**Scope:** `internal/proto/`, `internal/broker/`, `internal/server/`, `internal/metrics/`, `pkg/client/`, `cmd/`
**Related:** [ADR 0030](./0030-cluster-mode-peer-transport.md) (distributed core + `NOTLEADER`), [ADR 0031](./0031-partition-heal-linearizability-harness.md) (partition harness, reused here), [ADR 0032](./0032-client-routing-read-model.md) (client routing), [ADR 0028](./0028-raft-embedding-command-envelope.md) (raft embedding), [ADR 0027](./0027-correlated-telemetry.md) (telemetry surface)

## Context

After v3 M2/M3 a write returns `OK` the moment toyraft commits the entry — i.e.
once a **quorum** holds it (`Propose` blocks until committed + locally applied).
Two gaps remained:

1. **No stronger-than-quorum durability knob.** A caller cannot ask "don't ack
   until N followers have this write" (e.g. wait for *all* replicas before
   treating a write as safe). Quorum is the floor; there was no ceiling knob.
2. **No visibility into replication state.** Role, current leader, per-follower
   lag, and commit/apply/log offsets were invisible over the wire and absent from
   the Grafana/LGTM stack (ADR 0027). Operating or debugging a cluster meant
   reading logs.

The owned risk: a replication-ack feature that **lies** — reports N replicas when
fewer truly hold the write — is worse than none, because it gives false
durability confidence. The ack must be driven by real replication state and must
never over-report.

## Decision

### 1. `PUB … WAIT <n> <timeout-ms>` — a truthful barrier above quorum

The PUB frame gains an optional trailing `WAIT <n> <timeout-ms>` token,
composable in any order with the existing `DELAY <ms>` (ADR 0025). The parser
loops over trailing tokens rather than switching on field count.

- **Semantics: `n` counts followers, excluding the leader** (Redis `WAIT`-style).
  `WAIT 0` (or token absent) = leader-only = the pre-M4 behaviour. On an
  `N`-node cluster the write already reaches a quorum (`⌊N/2⌋+1` nodes =
  leader + `⌊N/2⌋` followers) before `Propose` returns, so **`WAIT ⌊N/2⌋`** is
  the strong/all-committed-quorum barrier (`WAIT 1` on a 3-node cluster). This
  supersedes the roadmap's contradictory "`WAIT N/2+1` = strong" wording, which
  double-counted the leader.
- **Driven by `MatchIndex`, never over-counts.** After `Propose` returns the
  entry's `Index`, the leader polls `Status().MatchIndex` and counts followers
  whose match `>= Index`. `MatchIndex` only advances on a durable follower ack,
  so the count is truthful by construction. **The leader's own entry is excluded
  from the count** — `MatchIndex` includes self (a toyraft quirk, see
  Consequences).
- **On timeout the write is not lost.** The entry is already committed and
  quorum-durable; only the *extra* replication factor was not met in time. The
  server returns `ERR WAIT_TIMEOUT <msgid>` (the assigned id, not a failure
  code), and the client surfaces a typed `*WaitTimeoutError{MsgID}` so callers
  know the write landed. A duplicate PUB skips the barrier (no new entry).

Rejected alternative — count total replicas *including* the leader: it matches
the literal roadmap exit-line but contradicts the prose ("n followers acknowledge")
and the familiar Redis semantics. Follower-count was chosen and the exit line
corrected.

### 2. `INFO [replication]` — a self-delimiting status block

A new `INFO` verb (default section `replication`) returns
`INFO <numlines>\n` followed by exactly `numlines` `key:value\n` lines. The
explicit count keeps the multi-line block self-delimiting on the line-oriented
wire, so a client reads a fixed number of follow-up lines with no sentinel.
Fields: `role`, `leader`, `term`, `commit_index`, `apply_index`,
`last_log_index`, `connected_replicas`, and per-peer `replica_<id>_match_index`
/ `replica_<id>_lag_entries`. Standalone (`raft == nil`) returns just
`role:standalone`. INFO is a **local read served on any node** (leader or
follower) — no `NOTLEADER` redirect — because it reports *this node's* view.

Peer rows appear only on a leader: `Status().MatchIndex` is leader-only (nil on
a follower), so a follower reports role/offsets but no peers.

### 3. Telemetry — scrape-time collector + one counter

Raft gauges (`toymq_raft_role`, `toymq_raft_commit_index`,
`toymq_raft_apply_index`, `toymq_raft_last_log_index`,
`toymq_replication_lag_entries{peer}`) are exported by a **custom
`prometheus.Collector`** (`broker.RaftCollector`) that reads `Status()` at scrape
time — always fresh, no sampler goroutine — registered in `cmd/toymq` only when
replication *and* metrics are both on. `toymq_wait_total{result}` (satisfied|
timeout) is a plain counter incremented inline in the barrier. A `broker.publish`
span already exists (ADR 0027); the barrier is covered by that span's lifetime.

### 4. Client + `toymqctl`

`pkg/client` gains `PubWait(...)` and `Info(ctx)` (on both `Client` and the
redirect-following `ClusterClient`); `toymqctl` gains `pub --wait N
--wait-timeout-ms` and an `info` subcommand. These feed the M5 cluster TUI, which
consumes `INFO replication`.

## Consequences

- **`MatchIndex` includes the leader itself.** Contrary to a first reading of the
  toyraft docs, the leader's own id is a key in `Status().MatchIndex`. A naive
  ack/lag count is off by one, silently satisfying `WAIT n` with `n-1` real
  followers — exactly the "durability lie" this milestone had to avoid. The
  broker captures its own `NodeID` at `AttachRaft` and excludes it everywhere it
  reads `MatchIndex`. There is no `NodeID()` accessor on the `Node` interface, so
  the id is threaded in from config. Recorded as FRICTION-06 / DOC-02 in the
  migration report.
- **Byte-level lag is not shipped.** The roadmap asked for lag in *bytes +
  entries*. toyraft exposes log **indices**, not a log-index→WAL-byte-offset map,
  so byte-lag would require toymq to maintain its own translation table. INFO and
  the metrics ship **entries-lag only** (`LastLogIndex − MatchIndex[peer]`),
  which is exact and free. Byte-lag is deferred (FEAT/scope note in the migration
  report).
- **`WAIT` polls, it does not subscribe.** toyraft has no commit/match-advance
  notification, so the barrier polls `Status()` on a 5ms ticker
  (`Broker.waitForReplication`). Correct and cheap at toy scale; a notification
  API is requested as a low-priority efficiency item (FRICTION-07).
- **Consistency model, documented:** `WAIT 0` = leader-local ack (quorum-durable
  but no extra-replica guarantee); `WAIT ⌊N/2⌋` = strong (every quorum member has
  it); `WAIT N-1` = fully replicated. This is the surface the README's
  consistency section describes.
- **Wire is additive.** A client that never sends `WAIT`/`INFO` is byte-identical
  to M3. The PUB parser change is backward-compatible (the 5-field frame still
  parses).

## Usage

```
PUB orders - - 5 WAIT 1 2000      # ack once 1 follower holds it, 2s timeout
INFO replication                  # -> INFO <n> then n key:value lines
toymqctl pub --wait 1 --wait-timeout-ms 2000 orders "hello"
toymqctl info
```
