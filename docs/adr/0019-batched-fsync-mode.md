# 0019 — Batched-fsync mode (group commit)

**Status:** Accepted
**Date:** 2026-07-04
**Scope:** `internal/wal/`, `internal/config/`, `internal/broker/`, `cmd/toymq`
**Supersedes:** [ADR 0002](./0002-per-message-fsync.md) — per-message fsync is
preserved as the default; batched and none are opt-in.

## Context

ADR 0002 fsyncs every WAL append before `Append` returns, so `OK` means
durable. Correct, but it makes per-message fsync the throughput ceiling: N
publishes = N fsyncs, and fsync is the dominant cost under load. We want to
amortise fsync across many appends without weakening the durability contract
for callers who don't opt out.

The load-bearing constraint: readers deliver strictly up to the atomic
`committed` offset (`internal/wal/reader.go`), so **`committed` must only
advance after the bytes are durable** — otherwise a consumer could see a
record that a crash then loses, breaking at-least-once.

## Decision

A `SyncMode` on the WAL `Log`, selected at `Open` via
`wal.WithSyncMode(mode, interval)`; the broker threads it from the new
`--fsync` / `--fsync-interval` flags through `broker.SyncConfig`.

| Mode | `Append` behaviour | `OK` means |
|---|---|---|
| **`per-message`** (default) | write + fsync inline + advance `committed` + return | durable (unchanged, ADR 0002) |
| **`batched`** | write (no fsync); block until the group committer fsyncs this record's batch and advances `committed` past it | durable — the ack still fences on a completed fsync |
| **`none`** | write + advance `committed` + return; never fsync | written to the OS page cache, **not** durable |

### Group committer (batched)

A ticker-driven goroutine owned by the `Log`, modelled on the broker's
`runPersistLoop`. Every `interval` (default 5 ms) it snapshots the write
offset under `mu`, **fsyncs outside the lock** so concurrent appends proceed
during the fsync, then re-takes `mu` to publish the new `committed` offset and
broadcast. Appenders write under `mu`, then wait on the shared cond until
`committed ≥ their end offset` — the same cond readers already wait on, since
both are "wait until `committed` advances." The snapshot is always on a record
boundary (appends update `written` only after a whole-record write), so
`committed` never lands mid-record. `Log.Close` stops the committer, whose
**final flush** makes any pending appends durable and releases their waiters —
graceful shutdown loses no acked data.

Alternative — *first-appender-becomes-committer* (no goroutine) — was weighed
and rejected: the ticker maps directly onto "collect for up to interval,"
matches an existing pattern, and keeps the `Close`/lifecycle obvious.

### Durability contract per mode (precise)

- **`per-message`:** `OK` ⟹ fsync'd. Survives power loss.
- **`batched`:** `OK` ⟹ fsync'd (in its group). A crash loses only writes that
  had **not** yet been `OK`'d (un-fsynced). `OK` latency now includes up to one
  interval. Same at-least-once contract; the ack is the durability fence.
- **`none`:** `OK` ⟹ handed to the OS, **not** fsync'd. It **survives a process
  `SIGKILL`** (the page cache outlives the process) but **NOT power loss or a
  kernel panic**. This is a deliberate footgun for throughput benchmarking and
  non-durable workloads — never the default, and called out loudly here.

On an fsync failure the committer records the error and releases blocked
appenders with `ErrSyncFailed` rather than hanging them; the `Log` is no longer
durable at that point.

## Consequences

**Positive**
- N appends within one window coalesce into a single fsync — the throughput
  win, measured by `cmd/toymq-bench` (which gains an `fsync=` run label so
  per-message vs batched runs are self-documenting and tabulatable).
- The default is byte-for-byte ADR 0002 behaviour; callers who omit `--fsync`
  are unaffected.
- The `committed`-after-fsync invariant is preserved in every mode, so
  delivery never exposes un-durable data (except `none`, by explicit design).

**Negative / trade-offs**
- `batched` adds up to one interval of latency to each `OK`, and a background
  goroutine + a final-flush step to the `Log` lifecycle.
- `none` can lose acked data on power loss — mitigated only by documentation
  and by never being the default.
- Holding correctness under `-race` required care: the committer fsyncs
  outside `mu` but publishes `committed` under it, and appenders recheck the
  predicate under `mu` so a broadcast between unlock/relock is never missed.

## v3 / toyraft forward-compat

Batching changes only **when** bytes hit disk, not **what** or **in what
order** — it is orthogonal to `raft.StateMachine.Apply` determinism (the
timestamp/MsgID determinism concern is owned by [ADR 0018](./0018-dedupe-recovery-from-wal.md)).
In v3 the WAL is each node's local materialised view of the Raft log, so a
per-node fsync policy is a purely local durability knob — batched is even more
attractive there. No code owed now.

## Edge cases

- **Recovery unchanged.** The committer only affects the write path;
  `recover()` (ADR 0003) still truncates a torn tail on `Open`, and `written`
  is initialised to the recovered `committed` length.
- **Empty window.** A tick with `written == committed` does nothing (no fsync).
- **Close with a pending batch.** The final flush fsyncs and releases waiters
  before the fd closes; the record is durable and recovered on reopen.
- **`none` + crash.** Process `SIGKILL` keeps page-cache writes (recovered on
  restart); power loss may drop them — the contract above.

## Usage

```
toymq --fsync batched --fsync-interval 5ms   # group commit
toymq --fsync per-message                    # default; OK = fsync'd
toymq --fsync none                           # best-effort; not power-safe
```

Wire impact: **none** — this is a local durability/throughput knob behind the
existing protocol.
