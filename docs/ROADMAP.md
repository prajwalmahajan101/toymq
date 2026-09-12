# toymq — Roadmap

Forward-looking execution plan from the **current `v1.3.0`** state to a
multi-node `v3.0.0`. v1.x already shipped (broker, WAL, client lib,
TUI, observability — see [CHANGELOG.md](../CHANGELOG.md) and the
[Releases page](https://github.com/prajwalmahajan101/toymq/releases)).
This document covers what comes **next**.

Branch off `main`; merge via PR; **no direct commits to `main`**.

```
v1.x  (shipped) ──► v2.0.0  "Useful"  ──► v3.0.0  "Distributed (toyraft)"
   broker + WAL +      AUTH/TLS, batched-       Raft replication (embed
   client + TUI +      fsync, dedupe persist,   toyraft rc.3), quorum acks,
   observability       partitions, DLQ          client routing, cluster TUI
```

## Current state — confirmation

**You are on `v1.3.0`, not `v1.0.0`.** Tags in the repo:

| Tag | Theme |
|---|---|
| `v1.0.0` | First stable release — broker + WAL + client + ctl + bench + Docker + CI |
| `v1.1.0` | `cmd/toymq-tui` (Bubble Tea), ADR 0014 |
| `v1.2.0` | State-change logging across broker / server / client |
| `v1.3.0` | Observability stack — Prometheus metrics + OpenTelemetry tracing (ADR 0015), CI lint matrix (ADR 0016), release automation (ADR 0017) |

`CHANGELOG.md` currently only documents `1.0.0` — backfilling 1.1–1.3
is a separate housekeeping task tracked in `IDEA.md`.

---

## Why this order — risk-first, additive-first

Each milestone owns the crash / concurrency / wire-compat tests for the
surface it introduces. Wire-breaking changes are deferred as long as
possible, and clustered behind the v3.0 major bump.

| Risk | Severity | Owned by |
|---|---|---|
| Dedupe LRU loss across restart (re-delivery of acked PUBs) | **Critical** | v2 M1 — chaos soak survives restart with dedupe state intact |
| Per-message fsync latency ceiling under load | High | v2 M2 — batched-fsync mode, group-commit bench |
| First wire-protocol break (HELLO + AUTH/TLS) | High | v2 M3 — ADR superseding the line-protocol contract |
| Slow consumer → unbounded inflight memory | Medium | v2 M5 — reader backpressure / flow control |
| `StateMachine.Apply` determinism / apply-once (silent replica divergence) | **Critical** | v3 M1 — determinism + apply-once test through the new seam |
| Raft log ↔ WAL invariant (which is source of truth) | **Critical** | v3 M1 — pick once, test with crash + leader-loss matrix |
| Acked-write loss across leader failover / partition heal | **Critical** | v3 M2 — kill-leader + partition linearizability harness |
| A write reaching a follower silently dropped | High | v3 M3 — redirect test |

Two ordering decisions worth calling out:

1. **Dedupe persistence (v2 M1) before batched-fsync (v2 M2).** Dedupe
   persistence closes the one chaos-suite limitation flagged in ADR
   0013 — a durability gap. Batched-fsync is a throughput win.
   Correctness gaps land before performance wins.

2. **HELLO/AUTH/TLS (v2 M3) before partitions (v2 M4).** AUTH lifts the
   localhost-only deployment ceiling. Once the broker is reachable
   off-host, the partitioning work has real users to validate against.
   Partitions before AUTH would mean shipping a multi-partition broker
   nobody can safely run.

---

# v2.0.0 — "Useful" (single-node)

Make toymq usable for small real workloads. Each milestone is
self-contained — no item doubles project scope. Wire-additive where
possible; one breaking bump (HELLO frame) gated by major version.

Branch convention: `feat/<milestone-slug>`.

## v2 M1 — Dedupe LRU persistence ✅ *(shipped — [PR #5](https://github.com/prajwalmahajan101/toymq/pull/5))*
**Branch:** `feat/dedupe-persistence` (merged) · **ADR:** [0018](./adr/0018-dedupe-recovery-from-wal.md)
- **Approach chosen: rebuild the LRU from the WAL, not a sidecar file.**
  The WAL already stores every `DedupeKey` and already scans every record
  on `Open`; a sidecar `dedupe.json` would lag the WAL by its debounce
  window (a residual duplicate hole) *and* would be a per-node artefact
  `toyraft` never replicates in v3. Rebuild-from-WAL is zero-gap and is the
  same materialisation seam v3's `raft.StateMachine.Restore` reuses. The
  original sidecar sketch (mirroring [ADR 0006](./adr/0006-debounced-atomic-offsets.md))
  is superseded by [ADR 0018](./adr/0018-dedupe-recovery-from-wal.md).
- `wal.Open` gains `WithRecoveryVisitor`; the broker funnels each recovered
  record through `rebuildIndexes` during recovery — which runs inside
  `broker.New`, before `server.Serve` accepts connections.
- **Owned risk test:** integration restart-dedupe — publish with a stable
  key, restart over the same data dir, re-publish the same key, verify it
  returns the original `MsgID` (DUP, no new append) and the consumer sees
  exactly one message. Plus a unit LRU-cap-boundary test across restart.
  (Confirmed failing on `main`, passing on the branch.)
- **Wire impact:** none.
- **v3 forward-compat note:** `topic.go` reads `time.Now()` for `TsNs` and
  assigns `MsgID` from a local counter — both must move to the proposer for
  deterministic `Apply` under toyraft. Recorded in ADR 0018 + v3 M1; not
  built here.
- **Exit:** closes the limitation in
  [ADR 0013](./adr/0013-pkg-client-architecture.md).

## v2 M2 — Batched-fsync mode ✅ *(shipped — [PR #7](https://github.com/prajwalmahajan101/toymq/pull/7))*
**Branch:** `feat/batched-fsync` (merged) · **ADR:** [0019](./adr/0019-batched-fsync-mode.md)
- `--fsync` flag: `per-message` (default, today's behaviour) | `batched` |
  `none`, plus `--fsync-interval` (default 5ms).
- Group commit lives in the WAL (`wal.WithSyncMode`): a ticker-driven
  committer collects appends for up to `--fsync-interval`, fsyncs once
  (outside the write lock), then advances `committed` and releases all
  waiters. `committed` still advances only after fsync, so consumers never
  see un-durable data.
- ADR 0019 supersedes [ADR 0002](./adr/0002-per-message-fsync.md) —
  per-message stays the default; batched/none are opt-in.
- **Owned risk test:** `test/chaos` gains a `CHAOS_FSYNC=batched` variant —
  the SIGKILL soak runs in group-commit mode and asserts every `OK`'d PUB
  survives (only un-acked, un-fsynced writes may be lost).
- **`cmd/toymq-bench`** gains an `fsync=` run label so per-message vs
  batched runs are self-documenting (the reserved batched column).
- **`none` durability caveat:** survives a process SIGKILL (page cache),
  **not** power loss / kernel panic — documented loudly in ADR 0019.
- **Exit:** batched group-commit shipped; `--fsync=per-message` durability
  contract unchanged.

## v2 M3 — HELLO frame + AUTH + TLS ✅ *(shipped — [PR #9](https://github.com/prajwalmahajan101/toymq/pull/9); wire-breaking, gated behind v2.0)*
**Branch:** `feat/hello-auth-tls` (merged) · **ADR:** [0020](./adr/0020-hello-auth-tls.md)
- `HELLO <version> [AUTH <token>]` is the first line on every connection;
  server replies `HELLO 1 OK` (negotiated version, leaving room for
  `HELLO 2` in v3 M7) or `ERR HELLO`/`ERR AUTH` and closes. The handshake
  is synchronous (before the async writer starts) so a rejection is never
  dropped. HELLO is a handshake phase, not a `Command`.
- **`--require-hello` toggle (default on):** `false` opens a plaintext
  migration window — a non-HELLO first line is processed as a command.
- Bearer-token auth via `--auth-token-file` (one token/line), matched with
  `crypto/subtle.ConstantTimeCompare`, never logged. Per-topic ACLs (v2
  M3.5) deferred; the token set is the hook.
- TLS via `crypto/tls` — `--tls-cert`/`--tls-key` on a **separate
  `--tls-addr`** listener that runs side-by-side with plain `--addr` (two
  `Server` instances sharing one broker). `pkg/client` gains `WithAuth`,
  `WithTLS`, a `TLSConfig` helper, and `ErrHandshake`/`ErrAuth`; the three
  CLIs gain `--auth-token`/`--tls`/`--tls-ca`/`--tls-insecure`.
- **Owned risk test:** the `{plain, tls} × {no-auth, auth} × {good, bad,
  missing token}` matrix end-to-end via `pkg/client`, plus a
  `--require-hello=false` compat test and a `toymqctl` auth round-trip.
- **Client-plane only:** peer-plane (Raft) TLS is a separate v3 prerequisite.
- **Exit:** broker safely reachable off-host; raw line-oriented scripts get
  the one-line `HELLO 1` prepend recipe in the README.

## v2 M4 — Partitions (single-node) ✅ *(shipped — [PR #11](https://github.com/prajwalmahajan101/toymq/pull/11))*
**Branch:** `feat/partitions` · **ADR:** [0021](./adr/0021-partitions-single-node.md)
- A topic holds `N` partitions, each an independent ordered log.
  **`Topic` became a thin router over a new `Partition` type** that owns
  the WAL, dedupe LRU, consumers, and offsets — MsgID stays monotonic
  *per partition* (the ordering guarantee), with no cross-partition order.
- **Two ways to set the count:** `--default-partitions N` for auto-created
  topics **and** an explicit `CREATE <topic> PARTITIONS <n>` verb /
  `toymqctl create`. Count is fixed at creation.
- **`PUB` carries a routing key distinct from the dedupe key:** an explicit
  `<topic>#<n>` wins; else the routing key hashes (`fnv1a % N`); else the
  keyless publish round-robins. `SUB <topic>` / `<topic>#*` = all partitions
  (fan-in), `<topic>#<n>` = one. `MSG`/`ACK`/`NACK` gained a partition field.
- One WAL per `(topic, partition)`: `topics/<name>/<p>/000000.log` + `meta.json`
  for N>1; **1-partition topics keep the pre-M4 flat layout byte-for-byte**
  (no `meta.json`, WAL at the topic root), so old data dirs recover unchanged.
- **Owned risk test (shipped):** `TestAllPartitionsFanIn` — producers fan
  across partitions while a `#*` consumer reads all; asserts per-partition
  MsgID monotonicity, exact per-partition counts, and zero cross-partition
  leakage under `-race`. Plus routing-key determinism across restart,
  per-partition offset persistence, and flat-layout back-compat.
- **Wire:** breaking (PUB/MSG/ACK/NACK arity, new CREATE) — gated behind the
  v2.0 major bump; ADR 0021 supersedes the ADR 0001 frames it touches.
- **Exit:** bench gains `-partitions N` (keyless publishes round-robin to
  spread load); `toymqctl create` / `pub -routing-key` / `sub <topic>#*`.

## v2 M5 — Reader backpressure + flow control ✅ *(shipped — [PR #12](https://github.com/prajwalmahajan101/toymq/pull/12))*
**Branch:** `feat/flow-control` · **ADR:** [0022](./adr/0022-reader-flow-control.md)
- **Per-`(partition, consumer)` receive window** (`--recv-window`, default 256):
  `runDelivery` gates on `awaitCredit` **before** reading the next WAL record,
  so `len(inflight) ≤ W` always holds — the memory bound. `Ack` frees a slot via
  a coalescing wake channel (ctx-cancellable, unlike `sync.Cond`). A `SUB #*`
  across N partitions is bounded by **N×W**.
- **`PAUSE` / `RESUME`** added to the wire — additive, argument-less,
  session-scoped verbs suspending/resuming delivery for the whole subscription
  regardless of the window. The redelivery ticker honours the paused flag;
  redelivery/Nack re-push already-counted inflight and bypass the window gate.
  Ephemeral per-connection; `pkg/client` gains `Pause`/`Resume`.
- **Owned risk test (shipped):** `TestReceiveWindowBoundsInflight` — a fast
  producer floods the log while a slow consumer receives exactly `W`, delivery
  stalls, and acking one releases exactly one more (inflight bounded regardless
  of backlog). Plus `TestPauseHaltsAndResumeReleases` (deterministic PAUSE via
  window=1) and a real-binary `--recv-window 2` smoke.
- **Wire:** additive (new `PAUSE`/`RESUME` + `--recv-window`); PUB/SUB/ACK/NACK
  and default behaviour unchanged.
- **Exit:** memory ceiling proven under the slow-consumer scenario.

## v2 M6 — Retention + DLQ + delayed messages
**Branch:** `feat/retention-dlq` (stacked PRs) · **ADRs:** [0023](./adr/0023-wal-segmentation-retention.md), [0024](./adr/0024-dead-letter-queue.md), [0025](./adr/0025-delayed-messages.md)
- **PR1 — WAL segmentation + retention** *(landed on `feat/wal-segments`)*:
  the single WAL file becomes a rolling set of numbered segments
  (`--segment-bytes`, record-boundary rotation, multi-segment recovery,
  boundary-spanning reader). A background sweeper drops whole sealed segments
  past `--retain-bytes` / `--retain-duration` (dropped when **either** bound
  would evict; active segment never touched). A resuming consumer whose offset
  fell below the retained floor gets `ERR OUT_OF_RANGE`; a fresh consumer starts
  at the floor. Segment boundaries are the natural v3/toyraft snapshot reference.
- **PR2 — Dead-letter queue** *(landed on `feat/dlq`)* · **ADR:** [0024](./adr/0024-dead-letter-queue.md):
  `--dlq-after-nacks N`; a message that fails delivery N times (nack or
  visibility timeout) is synthetically acked and its payload republished onto the
  auto-created `<topic>.dlq` (1 partition) via the deterministic `dlqMove` seam.
  `.dlq` topics are loop-guarded; the attempt count is in-memory/best-effort in
  v2. In v3 the leader proposes the move.
- **PR3 — Delayed messages** *(landed on `feat/delayed-msgs`)* · **ADR:** [0025](./adr/0025-delayed-messages.md):
  `PUB … DELAY <ms>` stamps an append-only `VisibleAtNs` on the record at propose
  time; the delivery goroutine parks until it fires, preserving per-partition
  order (head-of-line by design). Persisted across restart for free (it is in the
  WAL); retention will not drop a segment holding an un-fired delayed record.
  `pkg/client.PubDelay` / `toymqctl pub --delay-ms`.
- **Owned risk test:** integration `m6_test.go` — retention drops old segments
  and reads below the floor return `OUT_OF_RANGE`; a message past
  `--dlq-after-nacks` lands on `<topic>.dlq` and stops redelivering; a `DELAY`
  message fires only after its visible-at; retention keeps an un-fired delayed
  record's segment.
- **Exit:** three real production patterns (log retention, DLQ, scheduled
  jobs) usable end-to-end.

## v2 M7 — Observability hardening: correlation + W3C traceparent ✅ *(shipped — [PR #19](https://github.com/prajwalmahajan101/toymq/pull/19))*
**Branch:** `feat/observability-m7` · **ADRs:** [0026](./adr/0026-traceparent-wire-propagation.md), [0027](./adr/0027-correlated-telemetry.md)

Scope grew from the original "traceparent + alerts" into a full **correlated
telemetry** story, so the milestone split: **M7** owns the code + wire
(instrumentation depth, log/trace correlation, TRACEPARENT propagation);
**M7.5** owns the provisioned Grafana LGTM stack (below). M7 stays wire-additive
— no new major bump.

- **TRACEPARENT wire (ADR 0026):** optional `TRACEPARENT <w3c-header> [TRACESTATE
  <...>]` line before a `PUB`/`SUB` frame. The server extracts it with the W3C
  propagator so `broker.publish` / `broker.subscribe` become **children** of the
  caller's span — the producer→broker link. Additive/opt-in; a non-TRACEPARENT
  client is byte-for-byte unchanged. `pkg/client` gets an **otel-free**
  `WithTraceparentFunc` (respects ADR 0013's stdlib-only client).
- **Log ↔ trace ↔ metric correlation (ADR 0027):** a `slog.Handler` wrapper
  injects `trace_id`/`span_id` into `*Context` log lines; a `trace_id` **exemplar**
  on `toymq_wal_append_seconds` links a latency spike to its trace.
- **Metrics depth:** +11 series incl. the **per-consumer lag exporter**
  (`toymq_consumer_lag_messages{topic,partition,consumer}` = latest − last-acked),
  ack/nack, DLQ, delayed-pending, retention reclaim, `partition_latest_msgid`,
  `wal_segments`, `command_errors`, `publish_failure`. New spans `broker.ack` /
  `broker.nack` / `broker.dlq_move`.
- **Owned risk test:** `internal/integration/m7_test.go` — propagation makes
  `broker.publish` a child of the client span; a log line carries the matching
  `trace_id`; a no-TRACEPARENT client is unchanged (root span); lag gauge = head
  − lastAcked. Passes under `-race`.
- **Deferred (recorded in ADR 0026):** the broker→consumer `MSG ... TRACEPARENT`
  continuation — the client MSG parser is fixed-arity, and true end-to-end
  stitching needs the producer traceparent **persisted in the WAL record** (a
  v3-adjacent format decision).
- **Exit:** producer→broker traces stitch; logs/metrics/traces share a
  `trace_id`. The alerts + Grafana dashboards that *display* them are M7.5.

## v2 M7.5 — Grafana LGTM stack (provisioned) ✅ *(shipped — [PR #21](https://github.com/prajwalmahajan101/toymq/pull/21))*
**Branch:** `feat/observability-stack` (config only, stacked on `feat/observability-m7`)
- `docker-compose.observability.yml`: `toymq → OTel Collector → { Tempo (traces),
  Prometheus w/ exemplar storage (metrics), Loki (logs via Grafana Alloy) } →
  Grafana`, all provisioned.
- **Log pipeline (decided, ADR 0027):** JSON stdout + Grafana Alloy → Loki, with
  a derived field mapping `trace_id` → Tempo. No OTLP log bridge in the hot path.
- Grafana datasource correlation: Tempo `tracesToLogs`/`tracesToMetrics`, Loki
  `trace_id`→Tempo derived field, Prometheus exemplar→Tempo.
- **`observability/prometheus/alerts.yml`** with SLOs: p99 WAL append latency,
  redelivery rate, inflight backlog, publish-failure rate, consumer-lag ceiling,
  DLQ rate.
- Dashboards: the existing overview plus 3 consolidated boards — broker
  internals (WAL/segments/retention/delayed/flow-control), consumers
  (lag/ack/nack/redelivery/DLQ), and traces/correlation (exemplars + logs).
  Consolidated (4 files) rather than 7 thin ones; same metric coverage.
- **Exit:** `docker compose -f docker-compose.observability.yml up` and
  producer→broker→consumer telemetry is pivotable (log→trace→metric) in one
  Grafana UI with alerts pre-loaded. *(Configs validated:
  `docker compose config`, `promtool check rules` (6 rules), metric-name
  cross-check; live `up --build` smoke is the manual step.)*

## v2 M8 — Integration matrix + bench polish + tag `v2.0.0` ✅ *(shipped — [PR #23](https://github.com/prajwalmahajan101/toymq/pull/23), [PR #24](https://github.com/prajwalmahajan101/toymq/pull/24); tagged `v2.0.0`)*
**Branch:** `feat/release-v2`
- Cross-product integration: `{per-message, batched} × {plain, TLS} ×
  {auth, no-auth} × {1, 4 partitions}`.
- Bench harness adds: batched-fsync column, per-partition throughput,
  end-to-end p99 with TLS on.
- Backfill `CHANGELOG.md` `[1.1.0]` / `[1.2.0]` / `[1.3.0]` sections
  while we're touching it.
- Goreleaser update for `v2.0.0`.
- Tag `v2.0.0`.

### v2.0 status

| Milestone | Title | Status | PR | Tag |
|---|---|---|---|---|
| v2 M1 | Dedupe LRU persistence | ✅ | [#5](https://github.com/prajwalmahajan101/toymq/pull/5) | — |
| v2 M2 | Batched-fsync mode | ✅ | [#7](https://github.com/prajwalmahajan101/toymq/pull/7) | — |
| v2 M3 | HELLO + AUTH + TLS | ✅ | [#9](https://github.com/prajwalmahajan101/toymq/pull/9) | — |
| v2 M4 | Partitions (single-node) | ✅ | [#11](https://github.com/prajwalmahajan101/toymq/pull/11) | — |
| v2 M5 | Reader backpressure | ✅ | [#12](https://github.com/prajwalmahajan101/toymq/pull/12) | — |
| v2 M6 | Retention + DLQ + delay | ✅ | [#13](https://github.com/prajwalmahajan101/toymq/pull/13), [#16](https://github.com/prajwalmahajan101/toymq/pull/16), [#17](https://github.com/prajwalmahajan101/toymq/pull/17) | — |
| v2 M7 | Observability: correlation + traceparent | ✅ | [#19](https://github.com/prajwalmahajan101/toymq/pull/19) | — |
| v2 M7.5 | Grafana LGTM stack (provisioned) | ✅ | [#21](https://github.com/prajwalmahajan101/toymq/pull/21) | — |
| v2 M8 | Integration matrix + release | ✅ | [#23](https://github.com/prajwalmahajan101/toymq/pull/23), [#24](https://github.com/prajwalmahajan101/toymq/pull/24) | `v2.0.0` |

---

# v3.0.0 — "Distributed" (multi-node, **toyraft** · committed)

This is where toymq earns the original distributed-broker framing. It embeds
[`github.com/prajwalmahajan101/toyraft`](https://github.com/prajwalmahajan101/toyraft)
(`v1.0.0-rc.3`) as a vendored consensus library. Standalone mode stays
byte-identical to v2; `--replicate` opts into a Raft-backed cluster. Same
execution discipline as v1/v2 — branch off `main`, merge via PR, **no direct
commits to `main`** — and the same governing principle: **the highest-blast-radius
surface ships and is crash/linearizability-tested earliest, and each milestone
owns its own risk test.**

> **`tinyraft` → `toyraft` (`v1.0.0-rc.3`).** The library is real: `raft.New(Config)`
> behind a **frozen** public API (`pkg/raft.Node` — `Start`/`Stop`/`Propose`/`Step`/
> `Status`/`LeaderHint`; `StateMachine`; `Storage`; `Transport`), a production
> durable log (`pkg/storage/file`), a production HTTP peer transport
> (`pkg/transport/http`), leader redirects, and a copy-paste wiring blueprint in
> `cmd/toyraftd/main.go`. The old roadmap's *"only attempt if toyraft is real"*
> precondition is met — this section is the committed plan, not an aspiration.

> **Goal:** a replicated, leader-based, **single-writer** toymq cluster. v3.0 is
> **replication only** — partition placement, cluster membership, follower reads,
> mirror maker, and push frames are deferred to [v4.0](#v40--deferred-not-committed).
> The narrowing keeps the release focused on the actual toyraft payoff, and — by
> design — **keeps every committed milestone buildable on toyraft `rc.3` as it
> stands today**, with no upstream-blocked (`⛔`) milestone in scope.

> **Mutual unblock (the dogfood gate).** toyraft is at `v1.0.0-rc.3` with a frozen
> public API; its own roadmap gates the `v1.0.0` tag on a **real consumer embedding
> it**. toymq's running cluster **is** that gate. So v3 M1–M6 and toyraft `v1.0.0`
> unlock each other — and v3 M6 ships a **migration / dogfooding report**
> ([`docs/TOYRAFT-MIGRATION-REPORT.md`](./TOYRAFT-MIGRATION-REPORT.md), written
> incrementally across M1–M5) back to toyraft (bugs, API friction, docs gaps,
> feature requests) as first-class output, not an afterthought.

> **Decisions locked (2026-09-04).**
> 1. **Scope — replication only.** Partitions / membership / follower reads /
>    mirror maker / push frames → [v4.0](#v40--deferred-not-committed).
> 2. **Reads — leader by default; opt-in stale replica reads.** No
>    linearizable-read guarantee (toyraft `v1` has no ReadIndex — documented). A
>    just-elected leader may serve a slightly stale read; strong reads wait on
>    toyraft **UP-3** and land in v4.
> 3. **Write routing — client-driven redirect.** A follower returns a
>    `MOVED <leader-addr>` error; `pkg/client` (CLI **and** TUI) retries against
>    the hint with bounded backoff.
> 4. **Compaction — ship on `rc.3` with an unbounded Raft log.**
>    `StateMachine.Snapshot/Restore` are *structured* now (broker-state
>    serialization built + tested via the ADR 0018 `rebuildIndexes` seam) but
>    stubbed `ErrSnapshotUnsupported` per the `v1` contract, so real compaction
>    lands free when toyraft `v2` ships snapshots. Deferred to v4 (**UP-1**).

## The integration seam

toymq today mutates at a single chokepoint: **mutate broker state → append WAL →
ack**. Replicated, a mutating command becomes **`raft.Propose(envelope)` → toyraft
replicates → `StateMachine.Apply` mutates broker state + appends WAL → ack**. WAL
becomes each node's *local applied-state durability*; the **Raft log is the
replication source of truth**. The existing rebuild-from-WAL materialisation
(ADR 0018 `rebuildIndexes`) is the same seam `StateMachine.Restore` reuses — which
is exactly why M1 picks WAL-rebuild over a sidecar, and why the seam is small and
single-point.

## Why this order — bottom-up + risk-first (continued)

Numbering continues toymq's `v3 M#` convention (**v3 M1–M6**); the release tag is
`v3.0.0`.

| Risk | Severity | Owned by |
|---|---|---|
| `StateMachine.Apply` determinism / apply-exactly-once (silent replica divergence) | **Critical** | v3 M1 — determinism + apply-once test |
| Raft log ↔ WAL invariant + crash durability through the new path | **Critical** | v3 M1 — pick-once ADR + durability suite with the SM in the loop |
| Acked-write loss across leader failover / partition heal | **Critical** | v3 M2 — kill-leader + partition linearizability harness |
| Split-brain / double-leader | High | v3 M2 — same |
| A write reaching a follower silently dropped | High | v3 M3 — redirect test |
| Stale/torn reads violating the chosen read model | Medium | v3 M3 — read-model test |
| `WAIT` over-reporting acks (a durability lie) | Medium | v3 M4 — ack-truth test vs a slow/partitioned follower |
| Raft peer transport is unauthenticated (toyraft threat model: trusted network only) | High (documented) | v3 M6 — security note + bind guard |

## toyraft readiness — the three upstream capabilities (now v4 unblockers)

An integration-readiness audit of `toyraft` (originally 2026-07-04, re-confirmed
against `rc.3`) found the Raft **engine embeddable today**, with three
capabilities **stubbed or absent** upstream. The v3.0 replication-only cut is
scoped **precisely to avoid all three**, so none blocks a committed v3.0
milestone — each instead gates a [v4.0](#v40--deferred-not-committed) item:

| toyraft capability | State in `rc.3` | Gates (v4) |
|---|---|---|
| **UP-1 — Snapshots / log compaction** | Stub — `Snapshot`/`Restore` return `ErrSnapshotUnsupported`; `FirstIndex()` hardcoded to `1`; no `InstallSnapshot`. Raft log grows unbounded (a toyraft PRD non-goal). `ErrCompacted` / `snapshotIndex+1` are reserved hooks, so landing it is **no API break**. | v4 — real compaction (bounds disk on long-running clusters). |
| **UP-2 — Runtime membership changes** | Absent — no `AddNode`/`RemoveNode`/`ConfChange`; `Config.Peers` fixed at startup. | v4 — cluster membership + discovery. |
| **UP-3 — ReadIndex / lease reads** | Absent — leader-only reads, no read barrier; a just-elected leader can serve a slightly stale read. | v4 — follower reads with bounded staleness + linearizable leader reads. |

The detailed upstream deliverables (API shape, tests, toymq consumption) are
specified in [v4.0 → Upstream work items](#upstream-work-items-detailed--land-in-toyraft-first).

**Softer integration items — toymq owns these, no toyraft change required:**

- **Async transport wrapper.** toyraft calls `Transport.Send` synchronously inside
  the tick loop, so a frozen peer stalls heartbeats. The fix
  (`cmd/toyraftd/asynctransport.go`, a per-peer buffered drain) is **daemon code,
  not a library export** — toymq copies it (or we promote it upstream). Required
  for cluster liveness. Owned by v3 M2. *(Recorded in the migration report as API
  friction — a candidate to promote upstream for toyraft `v1.0.0`.)*
- **Odd cluster size.** `Config.Peers` must be odd (even N is rejected) — toymq
  cluster docs say 3 / 5 / 7, never 2 or 4.

## v3 M1 — Raft embedding + single-node replicated path *(the state-machine seam)*
**Branch:** `feat/toyraft-embed` · **Depends on:** nothing new · **ADR:** 0018 (extend) — Raft embedding, command envelope & WAL↔Raft-log invariant
*(The foundational, highest-blast-radius milestone — it owns the determinism + durability tests.)*
- Import `github.com/prajwalmahajan101/toyraft@v1.0.0-rc.3`.
- **Pick once, ADR it:** is the **Raft log** the durability source of truth (WAL
  becomes a local materialised view + snapshot device), or is **WAL** still
  authoritative and Raft just replicates entries? Default proposal (mirrors the
  toykv seam): **Raft log is the source of truth; WAL = local applied-state
  durability + snapshot artefact.**
- Deterministic **command envelope** (encode the mutating command → `[]byte`,
  version byte); only *mutating* commands are replicated. **Classification table:**
  mutating `PUB`/`ACK`/`NACK`/topic-admin → `Propose`; reads → local/leader;
  local-admin never replicated (`HELLO`, `AUTH`, `PING`, `INFO`).
- **Determinism prerequisite (from v2 M1 / ADR 0018):** `topic.go`'s
  `TsNs = time.Now()` and the local `MsgID` counter must move to the **proposer**
  and travel in the proposed command `Data` — `Apply` runs on every node and must
  be byte-identical. **First task of M1.**
- Implement the broker `raft.StateMachine`: `Apply(Entry)` decodes the envelope →
  advances broker state (WAL append + offset/dedupe advance), deterministic and
  wall-clock-free. `Snapshot`/`Restore` return `ErrSnapshotUnsupported` per the
  `v1` contract, but the broker-state serialization they will call is **built and
  unit-tested now** via the ADR 0018 `rebuildIndexes` seam (forward-compat for
  toyraft `v2`).
- Run a **single-node cluster** (`Peers=[self]`, self trivially leader):
  `toymq --replicate` flows every mutating command `Propose → Apply`. Standalone
  (non-`--replicate`) path untouched, byte-for-byte.
- **Owned risk test:** (1) determinism — the same command stream applied twice
  yields byte-identical broker state; (2) apply-once & ordering — single-node
  propose→apply round-trips every mutating command in strict index order; (3)
  crash durability preserved through the new path (the v1/v2 durability + chaos
  suite green with the SM in the loop).
- **Exit:** single-node `--replicate` serves every mutating command via
  `Propose→Apply`; broker state matches standalone; durability suite green.

## v3 M2 — Multi-node replication + leader election *(the distributed core)*
**Branch:** `feat/cluster` · **Depends on:** v3 M1 · **ADR:** [0030](./adr/0030-cluster-mode-peer-transport.md) (M2a — peer transport, membership, NOTLEADER gate), [0031](./adr/0031-partition-heal-linearizability-harness.md) (M2b — partition-heal + linearizability harness)
- `toymq --replicate --peers <id@host:raftport,…> --raft-addr --raft-dir` for
  3 / 5 / 7-node clusters (odd N — toyraft rejects even N). Wire toyraft
  `pkg/transport/http` (peer plane, distinct from the client plane) + copy the
  **async transport wrapper** (required for liveness); `pkg/storage/file` for the
  Raft log. Standalone mode unchanged.
- N-node cluster; role tracking; `LeaderHint()` surfaced; leader-gate writes.
- **Owned risk test:** 3-node cluster — a leader-acked `PUB` appears on all
  followers; **kill the leader → new leader elected → no acked PUB lost**;
  partition heal reconciles divergent tails. Plus a **Jepsen/Porcupine-style
  linearizability harness** on `PUB`/`ACK` under partition churn, run under
  `-race`. Pass criterion: no acked PUB lost, no double-ack accepted as a unique
  consume.
- **Exit:** 3-node cluster replicates all writes; survives leader kill + partition
  with zero acked-write loss; linearizability harness green. **✅ met** — M2a
  (kill + replicate + NOTLEADER gate) and M2b (`TestClusterPartitionHealNoLoss` +
  `TestClusterLinearizablePubConsume`, porcupine, `-race`) both green on
  `feat/cluster`.

## v3 M3 — Client routing: write redirect + read model
**Branch:** `feat/cluster-routing` · **Depends on:** v3 M2 · **ADR:** [0032](./adr/0032-client-routing-read-model.md) — client routing: write redirect + leader-default read model
- Follower write → leader-hint redirect error (`MOVED <leader-addr>` /
  `ERR NOTLEADER <host:port>`). `pkg/client` (CLI **and** TUI) auto-retry against
  the hint with bounded backoff.
- Reads: leader by default (follower reads redirect); an **opt-in** flag lets a
  follower serve stale local reads (documented non-linearizable — toyraft `rc.3`
  has no ReadIndex). The bounded-staleness `MAXLAG` contract is v4 (**UP-3**).
- **Owned risk test:** a client hitting a random node completes every write via
  redirect and reads consistently from the leader; opt-in stale reads return
  follower-local state; redirect storms during an election converge under bounded
  retry.
- **Exit:** any-node client completes all writes; leader/replica read model behaves
  as specified; CLI/TUI follow redirects transparently.
- **Status:** ✅ shipped — `ClusterClient` redirecting wrapper (`pkg/client/cluster.go`)
  with bounded-jitter backoff; typed `*NotLeaderError`; `SUB … STALE` leader-default
  read model (`IsLeader()` gate); `NotLeaderHint` extended to `ErrProposalDropped`/
  `ErrStopped`; `-cluster`/`--stale` on `toymqctl` + `toymq-tui`. Integration
  (`internal/server/cluster_routing_test.go`, `-race`): follower-seeded client
  completes writes/reads via redirect, follower `SubStale` serves local state, and
  writes converge after a mid-run leader kill.

## v3 M4 — `WAIT` + INFO replication + cluster observability
**Branch:** `feat/wait-info-repl` · **Depends on:** v3 M2 (reads `Status().MatchIndex`) · **ADR:** [0033](./adr/0033-replication-ack-and-telemetry-model.md) — replication acknowledgement & telemetry model
- `PUB … WAIT <numreplicas> <timeout-ms>` — leader returns `OK` only after
  `numreplicas` followers acknowledge the write's log index (driven by leader
  `MatchIndex`); truthful, never over-reports. `WAIT 0` = leader-only (today's
  behaviour).
- `INFO replication` section — role, leader addr, connected replicas, per-replica
  lag (bytes + entries), commit/apply/log offsets.
- OTel (the v2 M7 surface) extended: spans for propose→commit→apply,
  replication-lag & role gauges → the existing LGTM stack.
- **Owned risk test:** `WAIT N t` returns only once ≥N replicas truly hold the
  index (verified against a partitioned/slow follower it does **not** over-count);
  `INFO replication` fields match live cluster state.
- **Exit:** documented consistency model (`WAIT 0` = leader-local, `WAIT N/2+1` =
  strong); `INFO replication` accurate; raft signals visible in Grafana.

## v3 M5 — TUI v3: cluster view
**Branch:** `feat/tui-v3` · **Depends on:** v3 M3, v3 M4 · **ADR:** *(none expected — consumes M3/M4; revisit only if a real contract emerges)*
- Cluster pane: replicas, current leader, per-node role, lag, log offset (fed by
  `INFO replication`); AUTH + redirect-aware connect.
- **Owned risk test:** `teatest` cluster-view smoke against a running 3-node
  cluster; a leader change is reflected in the view.
- **Exit:** the TUI renders live cluster topology and follows leadership changes;
  all v2 keybindings still pass.

## v3 M6 — Bench + dogfood report + polish + `v3.0.0`
**Branch:** `feat/release-v3` · **Depends on:** v3 M1–M5 all merged
- Re-run `cmd/toymq-bench` in cluster mode (replication cost vs standalone);
  README records the numbers.
- **Migration / dogfooding report ([`docs/TOYRAFT-MIGRATION-REPORT.md`](./TOYRAFT-MIGRATION-REPORT.md)) — a release deliverable.**
  The report is **authored incrementally as the integration happens** — each of
  M1–M5 appends its findings (confirmed bugs with repros, API friction, docs gaps,
  feature requests) as they surface against running code — and is finalized here at
  M6, then delivered to toyraft as its `v1.0.0` dogfood-gate feedback. This is the
  reciprocal half of the mutual unblock. *(Not pre-written: the report starts as an
  empty scaffold and findings are recorded only once observed against running code
  in integration — the toykv precedent. Analysis-derived expectations stay in the
  readiness section above, not in the report.)*
- **Security note & bind guard:** the toyraft peer transport is
  unauthenticated/plaintext (toyraft threat model = trusted network). Document it;
  extend v2 M3's protected-mode posture to refuse an untrusted-network raft bind
  without an explicit override.
- Docs: README cluster quickstart; a `docker-compose.cluster.yml` for a local
  3-node cluster; goreleaser produces leader/follower-aware images.
- ADR reconciliation: 0018 (extended) / 0019 / 0020 / 0021 land after their owning
  milestones (M1/M2/M3/M4); M6 verifies all files exist and the index note is
  current.
- **Coordinate toyraft `v1.0.0`:** bump the dependency `rc.3 → v1.0.0` once toyraft
  tags it off this integration.
- **Release-hardening gate (all must pass before the tag):** linearizability
  harness green on `-race`; leader-kill / partition-heal suite green;
  standalone-mode benchmarks unchanged vs v2; crash-durability suite green with the
  SM in the loop; migration report delivered.
- Tag `v3.0.0`.

### v3.0 status

| Milestone | Title | Status | PR | Tag |
|---|---|---|---|---|
| v3 M1 | Raft embedding + single-node replicated path | 📋 Planned (buildable now) | — | — |
| v3 M2 | Multi-node replication + leader election | 🚧 M2a+M2b done (`feat/cluster`, ADR 0030/0031) | — | — |
| v3 M3 | Client routing: write redirect + read model | ✅ Done (`feat/cluster-routing`, ADR 0032) | — | — |
| v3 M4 | `WAIT` + INFO replication + cluster observability | ✅ Done (`feat/wait-info-repl`, ADR 0033) | — | — |
| v3 M5 | TUI v3: cluster view | 📋 Planned | — | — |
| v3 M6 | Bench + dogfood report + polish + v3.0.0 | 📋 Planned | — | `v3.0.0` |

Every committed milestone is **buildable on toyraft `rc.3` as it stands today** —
the replication-only cut deliberately avoids the three upstream gaps (UP-1/UP-2/
UP-3), which now gate [v4.0](#v40--deferred-not-committed) instead.

### Out of scope even at v3

- Kafka-style consumer-group rebalancing protocol (cooperative-sticky etc.) —
  `SUB` semantics stay session-scoped; rebalancing is the client's job.
- Schema registry — payloads remain opaque bytes.
- `MULTI` / transactions / Lua — explicitly rejected; spec stays small.
- Geo-replication beyond the (v4) mirror maker.

**Breaking risk in v3.0:** minimal. Replication is opt-in (`--replicate`);
standalone mode is byte-identical to v2. The only behavioural addition to the
deployment contract is the v3 M6 raft-bind guard, overridable. WAL format is
untouched by v3.0.

**Cut criteria:** 3-node cluster passes the linearizability harness on `PUB`/`ACK`
under `-race`; leader-kill + partition-heal lose no acked write; `WAIT` /
`INFO replication` / redirect / opt-in stale reads all shipped with ADRs;
standalone benchmarks unchanged; the migration report is delivered and **toyraft is
tagged `v1.0.0`**.

---

# v4.0 — deferred (not committed)

v4 is **deliberately not committed**. v3.0 delivers the original distributed-broker
thesis — a replicated single-writer cluster that proves the toyraft integration.
Everything past that rides the v3 architecture but is a **new mission**, not a
continuation, and several items are **upstream-blocked** on toyraft capabilities
that do not exist in `v1.0.0`. This section tracks the genuinely-major deferrals so
they are mapped, not forgotten — each with the gate that must clear first. **Nothing
here is committed;** "ship v3.0, stop" is the default terminal state, exactly as
"ship v1, stop" was for the v1 line.

| Theme | Item | Gate / blocker |
|---|---|---|
| **Compaction** | Real Raft-log compaction via `StateMachine.Snapshot/Restore` (bounds disk) | toyraft **UP-1** snapshots; the wiring is already structured in v3 M1 (broker-state serialization built) |
| **Membership** | `CLUSTER NODES` / `CLUSTER ADD <addr>` / `CLUSTER REMOVE <id>`, `--join <seed>` bootstrap, rolling churn | toyraft **UP-2** runtime membership (single-server reconfiguration) |
| **Partitions** | Partition placement across nodes — one Raft group per `(topic, partition)`, per-partition leaders, `CLUSTER REBALANCE` | Buildable, but carries the **multi-Raft** design cost (toymq owns group fan-out + shared `/raft/message` demux; toyraft offers no group multiplexing) |
| **Follower reads** | `SUB <topic> FROM <node-id> MAXLAG <ms>` — bounded-staleness follower reads; linearizable leader reads | toyraft **UP-3** ReadIndex/lease (the `MAXLAG` freshness contract layers on top) |
| **Mirror maker** | `cmd/toymq-mirror` — one-way cross-cluster replication for active/standby DR | Rides v3 replication; own staleness-bound test + runbook |
| **Push frames** | Server-pushed events (`EVENT topic.created`, `EVENT consumer.lag`) via a `HELLO 2` bump | Additive wire work; useful for the cluster TUI + dashboards without polling |
| **Peer security** | mTLS + auth on the Raft peer transport (lift the trusted-network-only ceiling) | toyraft peer-plane TLS/auth (or a toymq transport wrapper) |

**Spec-rejected even at v4** (listed for completeness): `MULTI` / transactions /
Lua; schema registry; Kafka-style cooperative rebalancing. Revisit only on an
explicit mission change from "learning artefact" to "run at scale."

### Upstream work items (detailed) — land in `toyraft` first

These are `toyraft`-side deliverables that unblock the v4 items above. Each
preserves the frozen public API (append-only message types, reserved storage
hooks) so it lands **without** a breaking change to the `Node` / `StateMachine` /
`Storage` interfaces already in use by v3. **Tracking: open these as `toyraft`
issues (UP-1..UP-3);** the v3 M6 migration report delivers them as the concrete
feature-request half of the dogfood feedback.

#### UP-1 — Snapshots + log compaction *(unblocks v4 compaction + bounds disk)*
- **toyraft API:** make `StateMachine.Snapshot() ([]byte, Index, error)` /
  `Restore([]byte) error` real (stop returning `ErrSnapshotUnsupported`). Add
  `MsgInstallSnapshot` as the next append-only `MessageType` (`4`) plus the
  leader→follower snapshot-transfer path. Implement `Storage.Snapshot`/`Restore`;
  make `FirstIndex()` return `snapshotIndex+1` (today hardcoded to `1`); have
  `Entries`/`Term` return the already-reserved `ErrCompacted` below the floor.
- **Compaction trigger:** new `Config.SnapshotThreshold` (entries since last
  snapshot). Leader snapshots the SM, truncates the log prefix, ships the snapshot
  to any follower whose `nextIndex` precedes the floor.
- **toymq consumption:** the broker's `Snapshot` serialises its materialised state
  (per-topic WAL offsets + dedupe LRU, or a WAL-segment reference); `Restore`
  rebuilds through the ADR 0018 `rebuildIndexes` seam. This is exactly why v3 M1
  chose WAL-rebuild over a sidecar.

#### UP-2 — Runtime membership changes *(unblocks v4 membership)*
- **toyraft API:** single-server reconfiguration (add-one / remove-one — the
  correct, simpler subset of Raft §6). `Node.AddNode(ctx, NodeID, addr)` /
  `Node.RemoveNode(ctx, NodeID)`, returning after the config entry commits. Add a
  `ConfChange` entry kind applied to the **core's own peer set**, never the user
  SM. Persist the active configuration alongside `HardState`. Enforce **one
  in-flight change at a time**; run quorum math on the committed config; handle
  leader-removes-self (step down after the change commits).
- **toymq consumption:** `CLUSTER ADD` / `CLUSTER REMOVE` map to `AddNode` /
  `RemoveNode`; `INFO replication` reads `Status()` + the active config.

#### UP-3 — ReadIndex / lease reads *(unblocks v4 follower reads + linearizable leader reads)*
- **toyraft API:** `Node.ReadIndex(ctx) (Index, error)` — confirms the node is
  still leader via a heartbeat round (or an optional clock-bound leader lease gated
  by a new `Config.LeaderLease`), and returns the commit index the caller must have
  applied before serving a linearizable read. No log entry is appended.
- **toymq consumption:** leader linearizable reads call `ReadIndex` then wait for
  `Status().ApplyIndex >= idx` before answering. Followers compare their
  `ApplyIndex` lag against the `SUB … MAXLAG <ms>` bound and either serve locally or
  return `MOVED <leader>`.

---

## Honest framing — pick one trajectory

The original toymq spec was explicit about scope: a single-node learning artefact.
Three honest paths were on the table — recorded so the reasoning behind the choice
stays legible:

| Option | Trajectory | When this is right |
|---|---|---|
| **A — stay at v1.x** | Keep polishing v1; v2/v3 stay aspirational | Spec-faithful. Project remains the long-weekend artefact it was meant to be |
| **B — v1 → v2** | Make it usable single-node; stop at AUTH + partitions + DLQ + observability | Realistic if v1 sees real (personal/test) usage and the gaps annoy |
| **C — v1 → v2 → v3** | Embrace the distributed-broker trajectory — the actual downstream payoff | Only once `toyraft` ships as a vendorable library and needs a real state machine to validate against |

**Updated 2026-09-04: Option C is now active — v2 → v3.** v2.0.0 shipped, and
toyraft has reached `v1.0.0-rc.3` with a frozen public API — the exact precondition
Option C named. The committed [v3.0 arc (v3 M1–M6)](#v300--distributed-multi-node-toyraft--committed)
is now the active plan. The trajectory is the full A → B → C sequence, not a jump:
v1 shipped, v2 shipped, v3 is the real downstream payoff that motivated the whole
project. v3.0 is scoped to **replication only** (the toyraft thesis); everything
else is [v4](#v40--deferred-not-committed) deferral.

---

## Changes from prior planning

- **Roadmap committed for the first time.** Previously
  [`README.md` § Roadmap](../README.md#roadmap) pointed at [`IDEA.md`](../IDEA.md)
  and called the roadmap "intentionally not committed". This file scopes the
  **forward** path for v2/v3 where ordering and risk matter.
- **`tinyraft` → `toyraft`.** Renamed to align with the `toy*` family (`toymq`,
  `toykv`, `toyraft`). All references use `toyraft`.
- **Each milestone owns its risk tests.** Same principle as the toykv roadmap:
  crash injection and concurrency stress live in the milestone that introduces the
  risk, not in a catch-all integration pass at the end.

**v3 refresh (2026-09-04):**

- **Re-anchored to toyraft `v1.0.0-rc.3`.** The old v3 section (written 2026-07-04,
  pre-release) hedged *"only attempt if toyraft is real"* and marked two milestones
  `⛔ upstream-blocked`. toyraft is now real with a **frozen** public API, so the
  section is the committed plan and the hedge is gone.
- **v3.0 narrowed to replication only** (mirrors the toykv v3 cut). Committed set is
  now **six** milestones — embed/determinism seam, multi-node replication+election,
  client routing, `WAIT`+INFO replication, cluster TUI, release. The narrowing is
  deliberate: it keeps **every committed milestone buildable on `rc.3` today**, so
  **no committed milestone is upstream-blocked**.
- **Membership, partition placement, follower reads, mirror maker, and push frames
  moved to a new `v4.0 — deferred` section**, each tagged with its upstream gate
  (UP-1/UP-2/UP-3) — rather than sitting as `⛔`-marked milestones inside the
  committed v3 line. The detailed UP-1..UP-3 upstream specs moved with them.
- **Mutual dogfood gate made explicit.** toyraft gates its own `v1.0.0` on a real
  consumer embedding it; toymq's cluster is that consumer. v3 M6 ships a
  **[migration / dogfooding report](./TOYRAFT-MIGRATION-REPORT.md)** back to toyraft
  as a first-class release deliverable, authored incrementally across M1–M5, and
  bumps the dependency `rc.3 → v1.0.0`.
- **Four architecture decisions locked (2026-09-04):** replication-only scope;
  leader reads + opt-in stale replica reads (no ReadIndex in `rc.3`); client-driven
  write redirect; ship on `rc.3` with an unbounded Raft log (compaction structured
  now, deferred to v4 pending toyraft `v2` snapshots).
- **Per-milestone dependency + ADR ownership** continues the v2 pattern: embed /
  determinism seam → v3 M1 (ADR 0018 extended), cluster/transport/storage →
  v3 M2 (ADR 0019), write-redirect + read model → v3 M3 (ADR 0020), `WAIT` +
  replication telemetry → v3 M4 (ADR 0021). v3 M5 (TUI v3) consumes M3/M4 with no
  new ADR expected.
- **`Option C` marked active** in the Honest-framing table; the top-of-file risk
  table and the v3 status table were refit to the six-milestone set.
