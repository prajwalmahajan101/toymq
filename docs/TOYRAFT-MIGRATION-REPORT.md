# ToyRaft Migration & Dogfooding Report

> **What this is.** toymq's v3.0 embeds
> [`toyraft`](https://github.com/prajwalmahajan101/toyraft) `v1.0.0-rc.1` as its
> consensus library (see [ROADMAP § v3.0](./ROADMAP.md#v300--distributed-multi-node-toyraft--committed)).
> toyraft's own roadmap gates its `v1.0.0` tag on a **real consumer embedding it**;
> toymq's cluster is that consumer. This document is the **reciprocal half of that
> mutual unblock** — the structured feedback (confirmed bugs, API friction, docs
> gaps, feature requests, and what worked) that flows back to toyraft to be triaged
> into its `v1.0.0` release.

> **Status:** ⬜ not started. The integration (v3 M1–M6) has **not yet run**, so this
> report is **intentionally empty**. It is **authored incrementally, not
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

**Status** — `[confirmed-in-integration]` · `[fixed-upstream]` ·
`[wontfix / by-design]`

**Severity** — 🔴 blocker (integration cannot meet an exit criterion) · 🟠 friction
(workable, but costs code or clarity) · 🟡 papercut (minor) · 🟢 praise (worked well)

---

## 1. Confirmed bugs (with repro)

Reproducible defects in toyraft observed during integration.

| ID | Severity | Status | Summary | Repro | toyraft area |
|---|---|---|---|---|---|
| _(none yet)_ | | | | | |

_Template — one row per reproduced defect:_
`| BUG-01 | 🔴 | [confirmed-in-integration] | one line | minimal steps / test name | pkg/… |`

---

## 2. API friction

Places where the frozen public API is workable but cost toymq extra code or clarity
— observed in integration.

| ID | Severity | Status | Finding |
|---|---|---|---|
| _(none yet)_ | | | |

---

## 3. Missing capabilities / feature requests

Capabilities toymq needed that `rc.1` does not provide — surfaced during
integration. *(The pre-known upstream gaps — snapshots / membership / ReadIndex —
are tracked in the roadmap's [Upstream work items](./ROADMAP.md#upstream-work-items-detailed--land-in-toyraft-first);
they land here only if integration confirms toymq actually hits them.)*

| ID | Severity | Status | Request |
|---|---|---|---|
| _(none yet)_ | | | |

---

## 4. Docs gaps

Things that were true and important but not obvious from the toyraft docs/README —
each cost integration time or risked a correctness bug.

| ID | Severity | Status | Gap |
|---|---|---|---|
| _(none yet)_ | | | |

---

## 5. What worked well (praise — dogfooding isn't only complaints)

API decisions that made embedding smooth, recorded so they are kept, not
accidentally regressed.

| ID | Status | Note |
|---|---|---|
| _(none yet)_ | | |

---

## 6. Per-milestone integration log

Appended as each milestone runs. Each entry: what was integrated, what surfaced
(cross-referenced to §1–§5), and what got worked around.

### v3 M1 — embed + determinism seam
_Not started._

### v3 M2 — multi-node + election
_Not started._

### v3 M3 — client routing
_Not started._

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
   `rc.1 → v1.0.0` (v3 M6) and the mutual unblock closes.
