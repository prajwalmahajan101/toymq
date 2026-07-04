# toymq — Roadmap

Forward-looking execution plan from the **current `v1.3.0`** state to a
multi-node `v3.0.0`. v1.x already shipped (broker, WAL, client lib,
TUI, observability — see [CHANGELOG.md](../CHANGELOG.md) and the
[Releases page](https://github.com/prajwalmahajan101/toymq/releases)).
This document covers what comes **next**.

Branch off `main`; merge via PR; **no direct commits to `main`**.

```
v1.x  (shipped) ──► v2.0.0  "Useful"  ──► v3.0.0  "Distributed (toyraft)"
   broker + WAL +      AUTH/TLS, batched-       Raft replication, quorum
   client + TUI +      fsync, dedupe persist,   acks, partitions, follower
   observability       partitions, DLQ          reads, mirror maker
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
| Raft log ↔ WAL invariant (which is source of truth) | **Critical** | v3 M1 — pick once, test with crash + leader-loss matrix |
| Split-brain on partition / leader election storms | **Critical** | v3 M2 — Jepsen-style linearizability harness |
| Partition placement across nodes (rebalance during writes) | High | v3 M4 — own concurrent-rebalance crash test |
| Cross-cluster mirror maker drift | Medium | v3 M6 — own staleness-bound test |

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

## v2 M5 — Reader backpressure + flow control
**Branch:** `feat/flow-control`
- Per-subscriber receive window (`PAUSE` / `RESUME` lines added to the
  wire — additive).
- Broker stops delivering past the window; redelivery ticker honours it.
- **Owned risk test:** slow-consumer scenario — a consumer sleeping
  100ms between ACKs must not let inflight grow unbounded; assert
  bounded memory under a fast producer.
- **Exit:** memory ceiling proven under the slow-consumer scenario.

## v2 M6 — Retention + DLQ + delayed messages
**Branch:** `feat/retention-dlq`
- **Retention:** per-topic `--retain-bytes` / `--retain-duration`;
  oldest WAL segments dropped past the limit (preserves consumer
  offsets but they return `OUT_OF_RANGE` on read past the floor).
- **Dead-letter queue:** per-topic `--dlq-after-nacks N`; messages
  exceeding N redeliveries land on `<topic>.dlq`.
- **Delayed messages:** `PUB <topic> <key> <payload> DELAY <ms>` —
  message held until the timer fires; persisted across restart.
- **Owned risk test:** time-travel — drive the clock forward in
  integration tests; verify retention drops, DLQ moves, and delayed
  releases all fire deterministically.
- **Exit:** three real production patterns (log retention, DLQ, scheduled
  jobs) usable end-to-end.

## v2 M7 — Observability hardening + W3C traceparent
**Branch:** `feat/traceparent-wire`
- Optional `TRACEPARENT <w3c-header>` line before any `PUB` /
  `SUB` frame. Server links its span to the parent; consumer reads it
  back via a new `MSG ... TRACEPARENT <...>` continuation.
- `observability/prometheus/alerts.yml` with SLOs: p99 WAL append
  latency, redelivery rate, inflight backlog, publish-failure rate.
- Per-consumer lag exporter — gauge of `(latest msgID - last acked
  msgID)` per `(topic, consumer)`.
- New ADR — wire-additive but spec'd.
- **Exit:** Grafana dashboard ships with the alerts pre-loaded; producer
  → broker → consumer traces stitch in Jaeger.

## v2 M8 — Integration matrix + bench polish + tag `v2.0.0`
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
| v2 M5 | Reader backpressure | ⬜ | — | — |
| v2 M6 | Retention + DLQ + delay | ⬜ | — | — |
| v2 M7 | traceparent + alerts | ⬜ | — | — |
| v2 M8 | Integration matrix + release | ⬜ | — | `v2.0.0` |

---

# v3.0.0 — "Distributed" (multi-node, **toyraft**)

This is where toymq earns the original distributed-broker framing. It
depends on **`toyraft`** (renamed from the placeholder `tinyraft` —
naming aligned with the `toy*` family: `toymq`, `toykv`, `toyraft`)
existing as an independently released library. Only attempt if
`toyraft` is real and ready to be embedded.

Branch convention: `feat/<milestone-slug>`. Each milestone owns the
crash / partition / consensus tests for the surface it introduces.

## toyraft readiness — upstream prerequisites (gate for v3)

Integration-readiness audit of `toyraft` (2026-07-04) against the v3
milestones. The Raft **engine is embeddable today**: `raft.New(Config)`
behind a frozen `Node` API (`Start`/`Stop`/`Propose`/`Step`/`Status`),
`Propose` blocks until *applied* (not just committed), a production
durable log (`pkg/storage/file.New`), a production HTTP peer transport
(`pkg/transport/http.New`), leader redirects, and a copy-paste wiring
blueprint in `cmd/toyraftd/main.go`. Conformance suites
(`storagetest.RunConformance`, `transporttest.RunConformance`) let toymq
validate any impls it swaps in.

Three capabilities a durable, elastic broker needs are **stubbed or
absent in toyraft** and must land upstream (or be designed around) before
the dependent milestone can meet its exit criteria:

| toyraft gap | State today | Blocks | Resolution |
|---|---|---|---|
| **Snapshots / log compaction** | Stub — `Snapshot`/`Restore` return `ErrSnapshotUnsupported` at every layer (kvsm, storage/file, storage/memory); `FirstIndex()` hardcoded to `1`; no `InstallSnapshot` message. The Raft log **grows unbounded** (toyraft PRD non-goal). | **v3 M1** (its crash matrix lists "kill during snapshot transfer" — impossible today) and any long-running cluster (disk fills). | Land snapshot + compaction upstream — `ErrCompacted` / `snapshotIndex+1` are already reserved v2 hooks, so no API break. Until then, v3 M1 **drops the snapshot-transfer case** and segment size is capped operationally. |
| **Runtime membership changes** | Absent — no `AddNode` / `RemoveNode` / `ConfChange` / joint consensus anywhere; `Config.Peers` is **fixed at startup**. | **v3 M3** (`CLUSTER ADD/REMOVE`, rolling-churn test). | Add single-server reconfiguration (or joint consensus) upstream. v3 M3 cannot start until it exists. |
| **Follower / linearizable reads** | Absent — leader-only reads, **no ReadIndex/lease**; a just-elected leader can serve a slightly stale read. Reference 307-redirects every read to the leader. | **v3 M5** (bounded-staleness follower reads) and the strong-read half of **v3 M2**. | Add ReadIndex (+ optional lease) upstream; the `MAXLAG` freshness contract layers on top. |

Softer integration items — toymq owns these, **no toyraft change required**:

- **Async transport wrapper.** toyraft calls `Transport.Send`
  synchronously inside the tick loop, so a frozen peer stalls heartbeats.
  The fix (`cmd/toyraftd/asynctransport.go`, a per-peer buffered drain) is
  **daemon code, not a library export** — toymq copies it (or we promote
  it upstream). Effectively required for N≥5 liveness. Owned by v3 M1.
- **Odd cluster size.** `Config.Peers` must be odd (even N is rejected) —
  toymq cluster docs say 3 / 5 / 7, never 2 or 4.
- **Peer-plane TLS/auth.** Inter-node traffic is plaintext; v2 M3's TLS
  covers only the client plane. Secure multi-host needs peer-plane TLS
  (upstream, or a toymq transport wrapper).
- **Multi-Raft for partitions.** toyraft is single-group. v3 M4
  (per-partition leaders) means one `Node` per `(topic, partition)` — many
  independent Raft groups. toyraft runs many `Node` instances fine but
  offers no group multiplexing; toymq owns the fan-out and the shared
  `/raft/message` demux.

**Sequencing consequence:** v3 M1 and v3 M2 are buildable on toyraft **as
it stands today** (minus the snapshot-transfer crash case). **v3 M3 and v3
M5 are upstream-blocked** — marked ⛔ in the status table. v3 M4 is
buildable but carries the multi-Raft design cost. These upstream items are
tracked as `toyraft` issues, not toymq milestones — toymq's v3 depends on
their release the same way it depends on toyraft existing at all.

### Upstream work items (detailed) — land in `toyraft` first

These are `toyraft`-side deliverables. Each preserves the frozen public
API (append-only message types, reserved storage hooks) so it lands
without a breaking change to the `Node`/`StateMachine`/`Storage`
interfaces already in use.

#### UP-1 — Snapshots + log compaction *(unblocks v3 M1 snapshot case + bounds disk)*
- **toyraft API:** make `StateMachine.Snapshot() ([]byte, Index, error)` /
  `Restore([]byte) error` real (stop returning `ErrSnapshotUnsupported`).
  Add `MsgInstallSnapshot` as the next append-only `MessageType` (values
  `0..3` are taken; use `4`) plus the leader→follower snapshot-transfer
  path in the driver. Implement `Storage.Snapshot`/`Restore`; make
  `FirstIndex()` return `snapshotIndex+1` (today hardcoded to `1`) and
  have `Entries`/`Term` return the already-reserved `ErrCompacted` below
  the compaction floor.
- **Compaction trigger:** new `Config.SnapshotThreshold` (entries since
  last snapshot, e.g. 10 000). Leader snapshots the SM, truncates the log
  prefix, and ships the snapshot to any follower whose `nextIndex`
  precedes the floor.
- **toymq consumption:** the broker's `Snapshot` serialises its
  materialised state (per-topic WAL offsets + dedupe LRU, or a WAL-segment
  reference); `Restore` rebuilds through the ADR 0018 `rebuildIndexes`
  seam. This is exactly why M1 chose WAL-rebuild over a sidecar.
- **toyraft tests:** `storagetest` gains compaction invariants; new
  driver tests for snapshot round-trip, `InstallSnapshot` to a lagging
  follower, and restart-from-snapshot.

#### UP-2 — Runtime membership changes *(unblocks v3 M3)*
- **toyraft API:** single-server reconfiguration (add-one / remove-one —
  the correct, simpler subset of Raft §6). `Node.AddNode(ctx, NodeID,
  addr)` / `Node.RemoveNode(ctx, NodeID)`, returning after the config
  entry commits. Add a `ConfChange` entry kind (a reserved `Entry`
  discriminator applied to the **core's own peer set**, never the user
  SM). Persist the active configuration alongside `HardState` so it
  survives restart. Enforce **one in-flight change at a time**; run quorum
  math on the committed config; handle leader-removes-self (step down
  after the change commits).
- **toymq consumption:** `CLUSTER ADD <addr>` / `CLUSTER REMOVE <id>` map
  to `AddNode`/`RemoveNode`; `INFO replication` reads `Status()` + the
  active config for role / leader / peers / lag.
- **toyraft tests:** add & remove under sustained write load, leader
  self-removal step-down, node rejoin after restart using the persisted
  config, and a rejected concurrent-change assertion.

#### UP-3 — ReadIndex / lease reads *(unblocks v3 M5 + the strong-read half of v3 M2)*
- **toyraft API:** `Node.ReadIndex(ctx) (Index, error)` — confirms the
  node is still leader via a heartbeat round (or an optional clock-bound
  leader lease gated by a new `Config.LeaderLease`), and returns the
  commit index the caller must have applied before serving a linearizable
  read. No log entry is appended.
- **toymq consumption:** leader linearizable reads call `ReadIndex` then
  wait for `Status().ApplyIndex >= idx` before answering. Followers
  compare their `ApplyIndex` lag against the `SUB ... MAXLAG <ms>` bound
  and either serve locally or return `MOVED <leader>`.
- **toyraft tests:** partitioned stale leader must **fail** `ReadIndex`
  (no stale linearizable read), lease-expiry correctness, and
  read-your-writes under partition churn.

> Tracking: open these as `toyraft` issues/roadmap entries (UP-1..UP-3).
> toymq v3 M1/M2 proceed against current toyraft; M3 waits on UP-2, M5 on
> UP-3, and M1's snapshot-transfer crash case on UP-1.

## v3 M1 — Embed toyraft; pick the WAL ↔ Raft-log invariant
**Branch:** `feat/toyraft-embed`
- Import `github.com/prajwalmahajan101/toyraft`.
- **Pick once, ADR it:** is the **Raft log** the durability source of
  truth (WAL becomes a snapshot device), or is **WAL** still
  authoritative and Raft just replicates entries? Reference toykv ADR
  on the same question. Default proposal: Raft log is the source of
  truth; WAL = local materialised view + snapshot artefact.
- **toyraft wiring (blueprint: `cmd/toyraftd/main.go`):** implement
  `raft.StateMachine` on the broker — `Apply(Entry)` = WAL append +
  offset/dedupe advance, deterministic and wall-clock-free (see the
  determinism note below); `Snapshot`/`Restore` return
  `ErrSnapshotUnsupported` until the upstream snapshot gate lands. Wire
  `pkg/storage/file` (or a WAL-backed `Storage`), `pkg/transport/http`
  (peer plane), and copy the **async transport wrapper** from
  `cmd/toyraftd/asynctransport.go` (required for liveness). Stand up a
  **second** listener for the peer/consensus plane, separate from the
  existing producer/consumer client plane; leader-gate writes and
  307/`MOVED`-redirect non-leaders to the leader's client address.
- **Determinism prerequisite (from v2 M1 / ADR 0018):** `topic.go`'s
  `TsNs = time.Now()` and the local `MsgID` counter must move to the
  **proposer** and travel in the proposed command `Data` — `Apply` runs
  on every node and must be byte-identical. This is the first task of M1.
- `toymq --replicate --peers <list>` for 3/5/7-node clusters (odd N —
  toyraft rejects even N); single mode preserved by default (no peers →
  today's behaviour, byte-for-byte).
- **Owned risk test:** crash matrix — kill the leader mid-write, kill a
  follower mid-replication; verify every acked PUB survives on the
  surviving quorum. *(The "kill during snapshot transfer" case is
  deferred until toyraft **UP-1** (snapshots) lands — snapshots are a
  stub today; see prerequisites.)*
- **Exit:** 3-node cluster passes the crash matrix; single-node mode is
  unchanged.

## v3 M2 — Quorum acks + leader redirect
**Branch:** `feat/quorum-acks`
- `PUB ... WAIT <numreplicas> <timeout-ms>` — leader returns `OK` only
  after `numreplicas` followers acknowledge the log entry. `WAIT 0`
  = leader-only (today's behaviour).
- Writes to followers return `MOVED <leader-addr>` (or transparent
  redirect in `pkg/client` behind an opt-in flag).
- **Owned risk test:** Jepsen-style linearizability harness on
  `PUB`/`ACK` under partition churn. Pass criterion: no acked PUB lost,
  no double-ack accepted as a unique consume.
- **toyraft note:** write-side quorum via `Propose` (which blocks until
  applied) works today. Strong *reads* need toyraft **UP-3** (ReadIndex/lease, shared with v3
  M5) — until then a just-elected leader may serve a
  slightly stale read; document this in the consistency model.
- **Exit:** documented consistency model (`WAIT 0` = leader-local,
  `WAIT N/2+1` = strong); harness green.

## v3 M3 — Cluster membership + discovery ⛔ *upstream-blocked*
**Branch:** `feat/cluster-membership`
- ⛔ **Blocked on toyraft:** requires runtime membership changes
  (single-server reconfiguration) — absent today, `Config.Peers` is fixed
  at startup. Blocked on toyraft **UP-2**; cannot start until it lands
  (see prerequisites).
- `CLUSTER NODES` / `CLUSTER ADD <addr>` / `CLUSTER REMOVE <id>` wire
  surface.
- Bootstrap via static peers list or a single seed `--join <addr>`.
- `INFO replication` — role, leader addr, lag (bytes + entries), peers.
- **Owned risk test:** rolling membership churn — add/remove nodes
  during a sustained write load; verify quorum is preserved and lag
  stays bounded.
- **Exit:** rolling restart of all peers under load loses zero acked
  writes.

## v3 M4 — Partition placement across nodes
**Branch:** `feat/partition-placement`
- ⚠️ **toyraft: multi-Raft.** Each `(topic, partition)` is its own Raft
  group → one `toyraft.Node` per partition, all multiplexed over one
  peer transport / `/raft/message` endpoint. toyraft runs many `Node`s
  fine but provides no group multiplexing; toymq owns the fan-out,
  per-group storage dirs, and the shared-transport demux.
- Each `(topic, partition)` has a leader replica; placement balanced
  across the cluster.
- Rebalance command: `CLUSTER REBALANCE` (manual at first; automatic
  later).
- **Owned risk test:** concurrent rebalance + writes — move a partition
  leader mid-stream; verify zero acked PUB loss and bounded delivery
  pause.
- **Exit:** linear throughput scaling across cluster size, demonstrated
  in `cmd/toymq-bench`.

## v3 M5 — Follower reads with bounded staleness ⛔ *upstream-blocked*
**Branch:** `feat/follower-reads`
- ⛔ **Blocked on toyraft:** requires ReadIndex (+ optional leader lease)
  — absent today (leader-only reads, no read barrier). Blocked on toyraft
  **UP-3**; the `MAXLAG` contract layers on top (see prerequisites).
- `SUB <topic> FROM <node-id> MAXLAG <ms>` — follower may serve reads
  if its lag is within the bound; otherwise redirects to leader.
- New ADR — defines the freshness contract.
- **Owned risk test:** stale-read bound under induced lag; verify the
  client sees `MOVED` rather than a stale message past `MAXLAG`.
- **Exit:** read fan-out works in the TUI cluster view.

## v3 M6 — Mirror maker (cross-cluster replication)
**Branch:** `feat/mirror-maker`
- `cmd/toymq-mirror` — daemon that subscribes to one cluster's topics
  and republishes to another (one-way, eventually consistent).
- Configurable filter (regex topic include/exclude); preserves dedupe
  keys end-to-end.
- **Owned risk test:** staleness bound — under steady load, prove cross-cluster
  delta stays within an asserted window.
- **Exit:** active/standby DR pattern usable; documented runbook.

## v3 M7 — RESP-3-style push frames + keyspace notifications
**Branch:** `feat/push-frames`
- Server-pushed events outside the request/reply pattern:
  `EVENT topic.created`, `EVENT consumer.lag`, etc.
- Useful for the TUI cluster view and external dashboards without
  polling.
- HELLO bump to `HELLO 2` to opt in (additive; HELLO 1 clients keep
  working).
- **Exit:** TUI cluster view runs entirely off pushed events.

## v3 M8 — Release `v3.0.0`
**Branch:** `feat/release-v3`
- Full Jepsen-style harness as a CI nightly.
- Goreleaser produces leader/follower-aware Docker images;
  `docker-compose.cluster.yml` spins a 3-node demo.
- README "Distributed" section.
- Tag `v3.0.0`.

### v3.0 status

| Milestone | Title | Status | PR | Tag |
|---|---|---|---|---|
| v3 M1 | Embed toyraft + invariant ADR | ⬜ buildable now¹ | — | — |
| v3 M2 | Quorum acks + leader redirect | ⬜ buildable now¹ | — | — |
| v3 M3 | Cluster membership + discovery | ⛔ upstream-blocked² | — | — |
| v3 M4 | Partition placement across nodes | ⬜ multi-Raft³ | — | — |
| v3 M5 | Follower reads with bounded staleness | ⛔ upstream-blocked⁴ | — | — |
| v3 M6 | Mirror maker | ⬜ | — | — |
| v3 M7 | Push frames + keyspace events | ⬜ | — | — |
| v3 M8 | Release v3.0.0 | ⬜ | — | `v3.0.0` |

¹ On toyraft as it stands today (M1 minus the snapshot-transfer crash
case). ² Needs upstream runtime membership changes. ³ Buildable, but
carries the multi-Raft (one `Node` per partition) design cost. ⁴ Needs
upstream ReadIndex/lease. See **toyraft readiness — upstream
prerequisites** above.

### Out of scope even at v3

- Kafka-style consumer-group rebalancing protocol (cooperative-sticky
  etc.) — `SUB` semantics stay session-scoped; rebalancing is the
  client's job.
- Schema registry — payloads remain opaque bytes.
- `MULTI` / transactions / Lua — explicitly rejected; spec stays small.
- Geo-replication beyond mirror maker.

---

## Honest framing — pick one trajectory

The original toymq spec was explicit about scope: a single-node
learning artefact. Three honest paths forward — **no commitment yet,
recorded so future-me remembers the choice is live**:

| Option | Trajectory | When this is right |
|---|---|---|
| **A — stay at v1.x** | Keep polishing v1; v2/v3 stay aspirational | Spec-faithful. Project remains the long-weekend artefact it was meant to be |
| **B — v1 → v2** | Make it usable single-node; stop at AUTH + partitions + DLQ | Realistic if v1 sees real (personal/test) usage and the gaps annoy |
| **C — v1 → v2 → v3** | Embrace the distributed-broker trajectory | Only if `toyraft` is happening and needs a real state machine to validate against |

**Default unless explicitly chosen: Option A.** Decision is reviewed
after each major tag ships, not before.

---

## Changes from prior planning

- **Roadmap committed for the first time.** Previously
  [`README.md` § Roadmap](../README.md#roadmap) pointed at
  [`IDEA.md`](../IDEA.md) and called the roadmap "intentionally not
  committed". That's the right call for v1 (and IDEA.md still owns the
  unscoped backlog) — this file scopes the **forward** path for v2/v3
  only, where ordering and risk matter.
- **`tinyraft` → `toyraft`.** Renamed to align with the `toy*` family
  (`toymq`, `toykv`, `toyraft`). All future references use `toyraft`.
- **Each milestone owns its risk tests.** Same principle as the toykv
  roadmap: crash injection and concurrency stress live in the
  milestone that introduces the risk, not in a catch-all integration
  pass at the end.
