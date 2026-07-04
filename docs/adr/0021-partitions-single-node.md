# 0021 — Single-node partitions

**Status:** Accepted
**Date:** 2026-07-05
**Scope:** `internal/broker/`, `internal/proto/`, `internal/server/`,
`internal/config/`, `pkg/client/`, `cmd/*`
**Wire impact:** **breaking** (gated behind the v2.0 major bump, pre-release) —
`PUB` gains a routing-key field; `MSG`/`ACK`/`NACK` gain a partition field; a new
`CREATE` verb is added.

## Context

Through M3 a topic was exactly one WAL (`data/topics/<name>/000000.log`) with a
single topic-wide MsgID counter, dedupe LRU, and offsets file (ADR 0005/0006/0018).
All publishes to a topic serialised on one `pubMu` and one append lock — a single
ordered log with no parallelism.

v2 M4 introduces **single-node partitions**: a topic holds `N` independent
partitions, each its own ordered log. Producers spread load across partitions;
consumers parallelise by subscribing to one partition or fanning in from all.
This is the last single-node scaling primitive before v3's distributed work, and
it is deliberately built so the per-partition log is exactly the unit v3 will
place a Raft group around.

## Decision

### Partition is the log-owning unit; Topic becomes a router

Everything the old `Topic` owned — `*wal.Log`, `*DedupeIndex`, the consumer
registry, `pubMu`, and the offsets file — moves to a new `Partition` type
(`internal/broker/partition.go`). `Topic` becomes a thin router holding
`partitions []*Partition` and a round-robin cursor. The publish/deliver/ack/offsets
logic is reused verbatim, re-scoped topic → partition.

Because MsgID is assigned by each `wal.Log` (`log.go` `Append`), it stays
**monotonic per partition**. That *is* the M4 ordering guarantee: total order
within a partition, **no** cross-partition order. Consumer ack state, inflight,
and offsets are all per-partition — a `(partition, msgID)` pair is the message
identity.

### On-disk layout — 1 partition stays byte-for-byte today's layout

```
data/topics/orders/          # N > 1
  meta.json                  #   {"partitions": 4}
  0/000000.log  0/offsets.json
  1/000000.log  1/offsets.json  ...
data/topics/logs/            # N == 1 (or legacy pre-M4) → FLAT, unchanged
  000000.log    offsets.json
```

- `meta.json` is written **only when N > 1**. Its presence means "partitioned,
  read the count"; its absence means "flat, 1 partition". A pre-M4 data dir
  (flat, no `meta.json`) therefore recovers unchanged as a 1-partition topic.
- `wal.Open(dir, ...)` is already per-directory, so the WAL needs **no change** —
  each partition just points it at its own subdir. Same for `offsets.json`
  (flush/load now take the partition dir).

### Partition count is declared two ways (both)

1. **Server default** — `--default-partitions N` (default `1`) applies to any
   topic auto-created by a first `PUB`/`SUB`.
2. **Explicit** — `CREATE <topic> PARTITIONS <n>` (and `toymqctl create`) creates
   a topic with an exact count. Idempotent: re-creating with the same count is
   `OK`; a different count is `ERR`. A topic's count is fixed at creation (no
   repartitioning in M4).

The default only affects *new* topics; existing on-disk topics keep their
recovered count.

### Routing — explicit partition, else hash the routing key, else round-robin

`PUB` carries a **routing key separate from the dedupe key** (a deliberate
decision — the two concerns were conflated in the old single-`key` field):

```
PUB <topic>   <dedupe-key> <routing-key> <len>\n<payload>\n   # hash routing-key → partition
PUB <topic>#<n> <dedupe-key> <routing-key> <len>\n<payload>\n # explicit partition n (routing-key ignored)
```

- Explicit `<topic>#<n>` wins; `n` is range-checked against the count.
- Else a non-`-` routing key → `fnv1a(routing-key) % count` (stdlib `hash/fnv`,
  deterministic and stable across restarts — required so a keyed producer always
  lands on the same partition, and v3-safe).
- Else (`-`, no routing key) → round-robin via an atomic cursor. Round-robin is
  **non-deterministic** and is the one thing v3 must move into the proposer (with
  `TsNs`/MsgID, per ADR 0018); noted here and in the roadmap.

Dedupe stays per-partition. Since a routing key maps deterministically to one
partition, a dedupe key that travels with a stable routing key always hits the
same partition's LRU — no cross-partition dedupe gap.

### Subscription — `SUB <topic>` = all partitions

```
SUB <topic>       <consumer-id>\n   # all partitions (fan-in)
SUB <topic>#<n>   <consumer-id>\n   # partition n only
SUB <topic>#*     <consumer-id>\n   # all partitions (explicit synonym)
```

An all-partitions subscription is N delivery goroutines (one reader per
partition) fanning Inflight snapshots into the session's single `sendCh`. For a
1-partition topic all three forms collapse to today's single-reader behaviour.

### Delivery / ack carry the partition

MsgIDs collide across partitions, so the wire must identify the partition on
delivery and echo it on ack (a numeric field, no `#`-splitting on the hot path):

```
MSG  <topic> <partition> <msgID> <len>\n<payload>\n
ACK  <consumer-id> <partition> <msgID>\n
NACK <consumer-id> <partition> <msgID>\n
```

The client's `Delivery` gains a `Partition` field; its `Ack`/`Nack` closures
capture and send it. `PUB`'s `OK <msgID>` response is left unchanged — the msgID
is partition-local and documented as such; a producer that needs the partition
for a keyless publish is a noted future `OK <partition> <msgID>` follow-up, out
of M4 scope.

`CreateCommand` joins the sealed `Command` union (ADR 0004) — adding a field to
the existing commands and one new verb is lower-friction than a parallel type
hierarchy. `HELLO` versioning is untouched (no bump; that is v3 M7).

## Consequences

**Positive**
- Publish and delivery parallelise across partitions; throughput scales with
  cores and consumers (roadmap "Throughput" exit criterion).
- Per-partition log is the exact seam v3 wraps in a Raft group (one `Node` per
  `(topic, partition)`), and per-partition MsgID/offsets are what `Apply` needs.
- 1-partition / legacy topics are byte-for-byte unchanged — zero migration.

**Negative / trade-offs**
- A genuine wire break (PUB/MSG/ACK/NACK arity, new CREATE). Acceptable: v2.0 is
  unreleased and M3 already broke the wire behind the same major bump.
- Round-robin (keyless) routing is non-deterministic — fine single-node, must
  move to the proposer in v3.
- No repartitioning: a topic's count is immutable after creation.
- `OK <msgID>` is partition-local; a keyless producer can't learn its partition
  from the ack (noted follow-up).

## Non-goals (hooks noted)
- **Repartitioning / partition growth** — count is fixed at creation.
- **Cross-partition ordering or transactions** — explicitly not provided.
- **Multi-node partition placement** — that is v3 M4 (multi-Raft).

## Edge cases
- **Explicit `#<n>` out of range** → `ERR` (PUB/SUB rejected).
- **`CREATE` with a different count than on disk** → `ERR`; same count → `OK`.
- **`PUB` with `-` routing key on an N>1 topic** → round-robin.
- **Legacy flat dir + `--default-partitions 4`** → the existing topic stays
  1-partition; only new topics get 4.
- **All-partitions SUB then a partition-scoped ACK** → routed by the ACK's
  partition field to that partition's consumer.

## Usage

```
# broker: every auto-created topic gets 4 partitions
toymq --addr :6789 --default-partitions 4

# explicit create with a specific count
toymqctl create orders -partitions 8

# publish: routing key 'user-42' hashes to a partition; 'dk' dedupes
toymqctl pub -key dk -routing-key user-42 orders "hello"
# or pin the partition
toymqctl pub -partition 3 orders "hello"

# subscribe to all partitions, or one
toymqctl sub orders   c1
toymqctl sub orders#3 c1
```
