# ToyRaft Migration & Dogfooding Report

> **What this is.** toymq's v3.0 embeds
> [`toyraft`](https://github.com/prajwalmahajan101/toyraft) `v1.0.0-rc.3` as its
> consensus library (see [ROADMAP § v3.0](./ROADMAP.md#v300--distributed-multi-node-toyraft--committed)).
> toyraft's own roadmap gates its `v1.0.0` tag on a **real consumer embedding it**;
> toymq's cluster is that consumer. This document is the **reciprocal half of that
> mutual unblock** — the structured feedback (confirmed bugs, API friction, docs
> gaps, feature requests, and what worked) that flows back to toyraft to be triaged
> into its `v1.0.0` release.

> **Status:** 🟡 in progress — **v3 M1 complete** (single-node replicated path).
> The findings below are recorded from running embedded code, each with a repro.
> **All six M1 findings were fixed upstream in `toyraft v1.0.0-rc.3`; toymq is
> bumped to rc.3** (ADR 0029). M2–M6 are not yet run. It is **authored incrementally, not
> pre-written** — findings are recorded here **only once observed against running
> embedded code**, each with a repro. Analysis-derived expectations (the readiness
> audit, known upstream gaps) live in the roadmap's
> [toyraft-readiness section](./ROADMAP.md#toyraft-readiness--the-three-upstream-capabilities-now-v4-unblockers)
> and the [Upstream work items](./ROADMAP.md#upstream-work-items-detailed--land-in-toyraft-first),
> **not** here — this file carries only what integration actually surfaces.

## Authoring protocol

Each v3 milestone appends what it **actually observed** (not what it expected).
Nothing is written into the findings tables until it is reproduced against running
code.

| Milestone | Appends its observed findings on |
|---|---|
| v3 M1 — embed + determinism seam | `Apply` determinism, `StateMachine`/`Storage` interface fit, envelope encoding, snapshot-stub ergonomics |
| v3 M2 — multi-node + election | transport behaviour under load, election/heartbeat, crash-matrix results |
| v3 M3 — client routing | `LeaderHint`/`ErrNotLeader` redirect ergonomics |
| v3 M4 — `WAIT` + INFO repl | `Status()` / `MatchIndex` fitness for ack-truth + lag reporting |
| v3 M5 — cluster TUI | `Status()` polling ergonomics for a live view |
| **v3 M6 — release** | **finalize**; dedupe; file the toyraft issues; deliver |

**Discipline (from the toykv precedent):** a finding lands only when it is observed
in integration with a repro. Findings are **not** invented from analysis to pad the
report — if M1–M5 never hit a predicted problem, it never appears here.

## Legend

**Status** — `[confirmed-in-integration]` · `[fixed-upstream rc.3]` ·
`[wontfix / by-design]`

**Severity** — 🔴 blocker (integration cannot meet an exit criterion) · 🟠 friction
(workable, but costs code or clarity) · 🟡 papercut (minor) · 🟢 praise (worked well)

---

## 1. Confirmed bugs (with repro)

Reproducible defects in toyraft observed during integration.

| ID | Severity | Status | Summary | Repro | toyraft area |
|---|---|---|---|---|---|
| BUG-01 | 🔴 | [fixed-upstream rc.3] | **In-memory log not restored on restart.** `newNode` constructs `log: &Log{}` (empty) and loads only `HardState`, so after restart `log.LastIndex()==0` while `commitIndex` is recovered from `HardState`. Replay-apply works (the driver reads committed entries straight from `Storage`), but a fresh `Propose` appends at `LastIndex()+1 == 1`, which is `≤ commitIndex`, and the entry is dropped with `ErrProposalDropped`. A restarted single node therefore recovers and replays but **cannot accept new writes**. | Publish 3 via `--replicate`, `node.Stop()` + broker `Close()`, reopen + re-attach raft: `Status()` reports `Role=Leader, Term=2, CommitIndex=3, ApplyIndex=3, LastLogIndex=0`; the next `Publish` returns `raft: proposal dropped (leadership lost before commit)`. | `pkg/raft` (`node.go` `newNode`) |

---

## 2. API friction

Places where the frozen public API is workable but cost toymq extra code or clarity
— observed in integration.

| ID | Severity | Status | Finding |
|---|---|---|---|
| FRICTION-01 | 🟠 | [fixed-upstream rc.3] | **`Propose` discards the `Apply` result.** `raft.Node.Propose` returns `(Index, Term, error)` and drops `Apply`'s `any` return, so a PUB's WAL-assigned MsgID cannot come back through `Propose`. toymq had to build a nonce-result registry (`internal/replication/registry.go`): the leader stamps a nonce, registers a channel, and `Apply` resolves it. The rc.2 reference `kvsm` hits the same wall — it exposes a separate `Get` for exactly this reason. **Request:** return the apply result from `Propose`, or expose a typed result channel. `pkg/raft`. |
| FRICTION-02 | 🟠 | [fixed-upstream rc.3] | **`inproc` transport unusable by external embedders.** `inproc.HubConfig.Clock` is typed `internal/clock.Clock` — an external module cannot construct it — and `NewHub` hard-errors on a nil Clock. So the in-process transport (ideal for a single-node embed and for tests) cannot be built outside the toyraft module. `pkg/transport/inproc`. |
| FRICTION-03 | 🟠 | [fixed-upstream rc.3] | **No transport expresses a `peers=[self]` cluster.** `http.Config.Validate` rejects an empty `PeerURLs` *and* rejects this node's own ID in `PeerURLs`. A single-node cluster (peers == [self], so `PeerURLs` excludes self → empty) satisfies neither. Combined with FRICTION-02, **neither shipped transport can build a single-node cluster**; toymq supplies a local no-op `raft.Transport` (`internal/replication/transport.go`). `pkg/transport/http`. |
| FRICTION-05 | 🟡 | [confirmed-in-integration] | **No single predicate for "cannot serve as leader."** Redirecting a client around a failing leader (v3 M3, ADR 0032) must treat three distinct errors identically: `*raft.ErrNotLeader` (write hit a follower), `raft.ErrProposalDropped` (leadership lost mid-propose), and `raft.ErrStopped` (node stopping). Each is correct behaviour, but every embedder must rediscover that all three mean "redirect elsewhere"; toymq matches all three in `Broker.NotLeaderHint`. **Request:** a documented predicate (`raft.IsNotLeader(err) bool`) or shared sentinel so consumers don't hand-roll the classification. Behaviour is fine — only the ergonomics. `pkg/raft`. |
| FRICTION-04 | 🟠 | [confirmed-in-integration] | **The driver calls `Transport.Send` synchronously, but the http `Send` blocks → an embedder must write an async wrapper for liveness.** `pkg/raft/driver.go` sends outbound messages inline in its single driver goroutine (`for _, m := range msgs { Transport.Send(ctx, m) }`), and `pkg/transport/http` `client.Send` is a blocking POST with retries + `SendTimeout`. So one dead or slow peer stalls the driver: no ticks, no commits, no election — a cluster-wide liveness failure, not just a lost message. Every embedder using the http transport must therefore decorate it with a per-peer async queue. toymq ships `internal/replication.asyncTransport` (buffered per-peer send + pump goroutine, drop-on-full, matching the best-effort `Send` contract). **Request:** ship an async transport option in-tree, or make the driver fan out Sends off the tick loop, so embedders get liveness by default. `pkg/raft` (`driver.go`) + `pkg/transport/http`. |

---

## 3. Missing capabilities / feature requests

Capabilities toymq needed that `rc.2` does not provide — surfaced during
integration. *(The pre-known upstream gaps — snapshots / membership / ReadIndex —
are tracked in the roadmap's [Upstream work items](./ROADMAP.md#upstream-work-items-detailed--land-in-toyraft-first);
they land here only if integration confirms toymq actually hits them.)*

| ID | Severity | Status | Request |
|---|---|---|---|
| FEAT-01 | 🔴 | [fixed-upstream rc.3] | **No persistent applied index; snapshots stubbed → replay double-applies.** With no durable applied index and `Snapshot`/`Restore` returning `ErrSnapshotUnsupported`, a restart replays the **entire** committed log through `Apply`. An embedder that keeps its own durable applied-state store (toymq's WAL) therefore double-applies every command. **Confirmed:** publish 3 with `--replicate`, restart → partition head 5 instead of 2 (six records instead of three). toymq worked around duplication with a WAL-embedded raft-index high-water (ADR 0028 §5), but that only prevents *duplication* — it cannot restore write capability (see BUG-01). **Request:** persist the applied index (or ship working snapshots) so an embedder can resume without re-applying, confirming the UP-1 line. This is the crux of the M1 crash-durability gap. |

---

## 4. Docs gaps

Things that were true and important but not obvious from the toyraft docs/README —
each cost integration time or risked a correctness bug.

| ID | Severity | Status | Gap |
|---|---|---|---|
| DOC-01 | 🟡 | [fixed-upstream rc.3] | **No guidance on transport choice for an external embedder.** The constraints that rule out both shipped transports for a single node (FRICTION-02/03) are only discoverable by hitting the `NewHub`/`Validate` errors at runtime. A short "embedding toyraft: transports" note — or an exported single-node/no-op transport — would have saved the round trip. `pkg/transport`. |

---

## 5. What worked well (praise — dogfooding isn't only complaints)

API decisions that made embedding smooth, recorded so they are kept, not
accidentally regressed.

| ID | Status | Note |
|---|---|---|
| PRAISE-01 | [confirmed-in-integration] | **Nil-clock defaulting is genuinely external-embed-friendly.** Both `raft.Config` and `http.Config` default a nil `Clock` to the real clock, with a comment naming the exact reason — "external consumers who cannot construct `internal/clock`." rc.3 extended the same affordance to `inproc` (FRICTION-02); where it exists, embedding is frictionless. |
| PRAISE-02 | [confirmed-in-integration] | **In-order single-goroutine `Apply` made determinism free.** `Apply` runs on one goroutine in strict index order, so a per-partition WAL counter advances identically on every node with no coordination. The two-broker byte-identical-WAL determinism test passed on the first run. |
| PRAISE-03 | [confirmed-in-integration] | **`Propose` blocks until locally applied**, so the applied result is available the instant `Propose` returns — race-free. rc.3 makes this even cleaner by returning `Apply`'s value directly from `Propose` (FRICTION-01), retiring toymq's nonce registry. |

---

## 6. Per-milestone integration log

Appended as each milestone runs. Each entry: what was integrated, what surfaced
(cross-referenced to §1–§5), and what got worked around.

### v3 M1 — embed + determinism seam
**Integrated:** `toyraft v1.0.0-rc.2` embedded behind `toymq --replicate`. Every
mutating command (PUB/ACK/NACK/CREATE) flows `Propose → StateMachine.Apply`
through a new `internal/replication` package (command envelope, `brokerSM`,
nonce-result registry, single-node no-op transport). Standalone mode is
byte-identical to v2 (the full integration matrix + `internal/wal` codec tests
pass with `RaftIndex` omitted when zero).

**Surfaced:**
- **BUG-01** (§1) — restart does not restore the in-memory log; a restarted node
  recovers/replays but cannot accept new writes.
- **FEAT-01** (§3) — no persistent applied index + stubbed snapshots → the whole
  committed log re-applies on restart (double-apply confirmed). This is the
  crux of the crash-durability gap.
- **FRICTION-01/02/03** (§2) — `Propose` discards the `Apply` result (forced the
  nonce registry); neither shipped transport can build a `peers=[self]` cluster
  (forced a local no-op transport).
- **DOC-01** (§4), **PRAISE-01/02/03** (§5).

**Worked around:** WAL-embedded `RaftIndex` high-water makes replay idempotent
(no duplication); local no-op transport for the single-node plane; nonce
registry for the MsgID return path. Determinism + apply-once + idempotent-replay
verified under `-race`.

**Blocked on rc.2 → resolved in rc.3:** on rc.2, full SIGKILL-and-keep-serving
crash-durability could not pass (BUG-01 + FEAT-01) — the idempotent-replay half
was done, but write-after-restart was blocked.

**M1 closeout (rc.3 bump, ADR 0029):** toyraft `v1.0.0-rc.3` shipped fixes for
all six findings. toymq bumped to rc.3 and:
- **BUG-01 fixed** (`restoreLogFromStorage`) → a restarted node accepts writes;
  `TestReplicatedRestartRecovery` now asserts a post-restart publish returns the
  correct next MsgID. **Full single-node crash-durability passes.**
- **FRICTION-01 fixed** (`Propose` returns `Apply`'s result) → the nonce
  registry was deleted; the MsgID comes straight back from `Propose`.
- **FRICTION-02/03 + DOC-01 fixed** (inproc nil-clock, http self-only cluster) →
  the no-op transport is now a *choice*, not a necessity; kept for zero
  dependencies at N=1.
- **FEAT-01 partially fixed** — rc.3 adds a durable applied-index checkpoint
  (B2), but it engages only for a StateMachine that implements `Snapshot`.
  toymq's `brokerSM` still stubs snapshots (real broker-state serialization is
  **v4**), so it opts into full-log replay and **keeps the `RaftIndex`
  idempotence guard**. Retiring the guard + log compaction remain a v4 item.

### v3 M2 — multi-node + election

**M2a — distributed core** (ADR 0030). Swapped the single-node no-op transport
for a real N-node cluster over `pkg/transport/http`: `toymq --replicate --peers
id@url,… --raft-addr host:port`. Every mutating command replicates
`Propose→Apply` to all followers; a write on a follower is leader-gated
(`*raft.ErrNotLeader` → wire `NOTLEADER <leader-id>`); killing the leader elects
a new one among survivors with the prior acked writes intact. Verified in-process
over the http transport on loopback under `-race` (`internal/broker/cluster_test.go`).

**Surfaced:**
- **FRICTION-04** (§2) — the driver's synchronous `Transport.Send` + the http
  transport's blocking `Send` mean a dead peer stalls the whole driver (no ticks,
  no commits, no election). This forced an async send decorator
  (`internal/replication.asyncTransport`, per-peer buffered queue + pump,
  drop-on-full) — mandatory for liveness, not an optimization. This is the M2a
  finding delivered back upstream.

**Confirmed working (rc.3):** multi-peer `http.Config.PeerURLs`; `LeaderHint()`
on the node API; `node.Start` wiring `Transport.Register(n.Step)` for the inbound
plane; `*raft.ErrNotLeader{LeaderHint}` as a typed, `errors.As`-able rejection —
all behaved as documented, so the leader-gate and redirect-hint were a thin
mapping with no workaround.

**Worked around:** async transport decorator (FRICTION-04); free-loopback-port
helper in tests because the http transport binds via `ListenAndServe` and gives
no way to read back a `:0`-assigned port (minor — noted, not filed).

**Deferred:** NodeID→client-addr resolution + client auto-retry + read redirect (M3).

**M2b — partition-heal + linearizability harness** (ADR 0031). Added a test-only
bidirectional partition seam (`partitionableTransport`, drops Send-to and
Step-from a blocked peer) — the fault `node.Stop()` cannot express (a kill has no
live minority, so no split-brain). Two tests, green under `-race`:
`TestClusterPartitionHealNoLoss` (isolate the leader alone → it cannot ack a write
→ majority elects and serves → heal → minority truncates its uncommitted tail and
converges, zero acked-write loss) and `TestClusterLinearizablePubConsume` (a
porcupine message-queue model over concurrent PUBs under partition churn; no acked
PUB lost, no double-consume).

**Confirmed working (rc.3):** `Propose` on a partitioned leader blocks on quorum
and returns `ctx.Err()` on the caller's deadline (`node_public.go` selects on
`<-ctx.Done()`), so a bounded `PublishCtx` cleanly rejects a minority write —
no hang, no phantom ack. On heal, raft log-matching truncates the isolated
leader's uncommitted tail with no embedder involvement — divergent-tail
reconciliation is automatic.

**Worked around (M2b):** none — the partition test is pure fault injection at the
transport boundary; no toyraft API gap surfaced.

**Noted (not filed):** a `ctx`-cancelled `Propose` may still commit later, so the
write is at-least-once from the client's view. Expected raft semantics; the
harness treats the acked set as a subset of the converged log, never asserts
equality. A `Propose` that could report "definitely not committed" on ctx-cancel
would let a client distinguish lost from delayed — a possible future API note, not
a bug.

### v3 M3 — client routing

Client routing is entirely client-side (ADR 0032): resolving the `ERR NOTLEADER`
hint to a member address and retrying lives in `pkg/client.ClusterClient`, not in
toyraft. As expected, this surfaced **no toyraft friction** — the redirect
contract shipped in M2 (`*raft.ErrNotLeader` carrying a `LeaderHint`) was
sufficient.

**One ergonomic observation (not a bug).** Routing around a *shutting-down* leader
needs more than `*raft.ErrNotLeader`. When a node is stopping, `Propose` returns
`raft.ErrStopped`; when leadership is lost mid-propose it returns
`raft.ErrProposalDropped`. Both are the correct raft behaviour, but from the
embedding server's view they are the same actionable condition as `ErrNotLeader`:
"this node cannot commit your write as leader — go elsewhere." toymq handles this
by matching all three in `Broker.NotLeaderHint` and surfacing `NOTLEADER` for each.

- **Feedback to toyraft:** these three are distinct error *types* but a single
  *class* for a client-facing consumer. A documented predicate (e.g.
  `raft.IsNotLeader(err) bool`) or a shared sentinel would save every embedder
  from rediscovering that `ErrStopped`/`ErrProposalDropped` must be treated like
  `ErrNotLeader` for redirect purposes. Filed as an API-friction item, not a bug —
  behaviour is correct, only the classification is left to the consumer.
- **`LeaderHint` on a stopped node** returns a stale/empty value, as one would
  expect. toymq tolerates this: an empty or self-referential hint makes the client
  sweep its member set round-robin, so redirect still converges. No change
  requested — this is a reasonable contract for a stopped node.

### v3 M4 — WAIT + INFO replication
_Not started._

### v3 M5 — cluster TUI
_Not started._

### v3 M6 — finalize & deliver
_Not started._ Dedupe findings, open the toyraft issues, deliver as the `v1.0.0`
dogfood-gate feedback.

---

## 7. Delivery — how this feeds toyraft `v1.0.0`

At v3 M6 this report is finalized and delivered as toyraft's dogfood-gate feedback:

1. **Bugs (§1)** → toyraft issues, each with the repro/test name.
2. **API friction (§2)** → toyraft issues (promote-to-library or document-as-required).
3. **Feature requests (§3)** → toyraft roadmap entries (UP-1/UP-2/UP-3 confirmed or
   revised against real integration experience), sequenced against toyraft's `v2`
   line — these unblock toymq **v4**, not v3.0.
4. **Docs gaps (§4)** → toyraft README / `StateMachine` doc PRs.
5. **Praise (§5)** → the decisions that worked, kept on record.
6. **Dependency bump** → once toyraft tags `v1.0.0` off this feedback, toymq bumps
   `rc.3 → v1.0.0` (v3 M6) and the mutual unblock closes.
