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

## v2 M1 — Dedupe LRU persistence
**Branch:** `feat/dedupe-persistence` · **ADR:** [0018](./adr/0018-dedupe-recovery-from-wal.md)
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

## v2 M2 — Batched-fsync mode
**Branch:** `feat/batched-fsync`
- New `--fsync` flag: `per-message` (default, today's behaviour) |
  `batched` | `none`.
- Group commit: collect appends for up to `--fsync-interval` (default
  5ms), fsync once, ack all waiters.
- New ADR superseding [ADR 0002](./adr/0002-per-message-fsync.md) — the
  per-message decision is preserved as the default; batched is opt-in.
- **Owned risk test:** crash injection under `--fsync=batched` — SIGKILL
  during a group-commit window; verify only **acked** PUBs survive (the
  ones whose `OK` was sent after their batch's fsync returned).
- **`cmd/toymq-bench`** gains the batched column the README has been
  reserving since v1.0.
- **Exit:** measurable throughput win in the bench harness; durability
  contract unchanged for `--fsync=per-message`.

## v2 M3 — HELLO frame + AUTH + TLS  *(wire-breaking, gated behind v2.0)*
**Branch:** `feat/hello-auth-tls`
- New `HELLO <version> [AUTH token]` frame, must be the first line on
  every connection. Servers respond `HELLO 1 OK` or `ERR`.
- Bearer-style token auth — `--auth-token-file` flag points at a file
  of one-token-per-line. Future ADR for per-topic ACLs (v2 M3.5
  optional).
- TLS via `crypto/tls` — `--tls-cert` / `--tls-key`. TLS-or-plain
  selectable per listener; both can run side-by-side.
- New ADR for the wire bump; `pkg/client` gains `WithAuth`, `WithTLS`
  options.
- **Owned risk test:** matrix — `{plain, tls} × {no-auth, auth} × {good,
  bad token}`; integration suite gains a TLS scenario; `toymqctl` honours
  the new flags.
- **Exit:** broker safely reachable off-host; existing line-oriented
  `redis-cli`-style scripts get a one-line `HELLO 1` prepend recipe in
  the README.

## v2 M4 — Partitions (single-node)
**Branch:** `feat/partitions`
- A topic can be created with `N` partitions. `PUB <topic> <key> <payload>`
  hashes `key` to a partition; explicit `PUB <topic>#<partition> ...`
  is also accepted.
- One WAL segment file per `(topic, partition)`; offsets file extended.
- Consumers subscribe to a partition (or `<topic>#*` for all).
- New ADR for the segment layout — preserves the current single-WAL
  default (1 partition = today's behaviour).
- **Owned risk test:** concurrent stress — N producers fan out across
  partitions while M consumers subscribe; verify per-partition ordering
  and zero cross-partition leakage under `-race`.
- **Exit:** README "Throughput" section shows linear scaling up to GOMAXPROCS;
  TUI partition view.

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
| v2 M1 | Dedupe LRU persistence | 🔄 branch | — | — |
| v2 M2 | Batched-fsync mode | ⬜ | — | — |
| v2 M3 | HELLO + AUTH + TLS | ⬜ | — | — |
| v2 M4 | Partitions (single-node) | ⬜ | — | — |
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

## v3 M1 — Embed toyraft; pick the WAL ↔ Raft-log invariant
**Branch:** `feat/toyraft-embed`
- Vendor / import `github.com/prajwalmahajan101/toyraft`.
- **Pick once, ADR it:** is the **Raft log** the durability source of
  truth (WAL becomes a snapshot device), or is **WAL** still
  authoritative and Raft just replicates entries? Reference toykv ADR
  on the same question. Default proposal: Raft log is the source of
  truth; WAL = local materialised view + snapshot artefact.
- `toymq --replicate --peers <list>` for 3-node clusters; single mode
  preserved by default (no peers → today's behaviour, byte-for-byte).
- **Owned risk test:** crash matrix — kill the leader mid-write, kill a
  follower mid-replication, kill during snapshot transfer; verify every
  acked PUB survives on the surviving quorum.
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
- **Exit:** documented consistency model (`WAIT 0` = leader-local,
  `WAIT N/2+1` = strong); harness green.

## v3 M3 — Cluster membership + discovery
**Branch:** `feat/cluster-membership`
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
- Each `(topic, partition)` has a leader replica; placement balanced
  across the cluster.
- Rebalance command: `CLUSTER REBALANCE` (manual at first; automatic
  later).
- **Owned risk test:** concurrent rebalance + writes — move a partition
  leader mid-stream; verify zero acked PUB loss and bounded delivery
  pause.
- **Exit:** linear throughput scaling across cluster size, demonstrated
  in `cmd/toymq-bench`.

## v3 M5 — Follower reads with bounded staleness
**Branch:** `feat/follower-reads`
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
| v3 M1 | Embed toyraft + invariant ADR | ⬜ | — | — |
| v3 M2 | Quorum acks + leader redirect | ⬜ | — | — |
| v3 M3 | Cluster membership + discovery | ⬜ | — | — |
| v3 M4 | Partition placement across nodes | ⬜ | — | — |
| v3 M5 | Follower reads with bounded staleness | ⬜ | — | — |
| v3 M6 | Mirror maker | ⬜ | — | — |
| v3 M7 | Push frames + keyspace events | ⬜ | — | — |
| v3 M8 | Release v3.0.0 | ⬜ | — | `v3.0.0` |

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
