# 0030 — Cluster mode: peer transport, membership flags & the NOTLEADER write gate

**Status:** Accepted
**Date:** 2026-09-11
**Scope:** `internal/config/`, `internal/replication/`, `internal/broker/`, `internal/proto/`, `internal/server/`, `cmd/toymq/`
**Related:** [ADR 0028](./0028-raft-embedding-command-envelope.md) (raft embedding), [ADR 0029](./0029-toyraft-rc3-propose-result.md) (rc.3 bump)

## Context

v3 M1 embedded toyraft behind `toymq --replicate` as a **single-node** cluster
(`Peers=[self]`, no-op transport). This ADR covers **v3 M2a** — the distributed
core: a real N-node cluster that replicates every mutating command, gates writes
to the leader, and survives a leader kill with zero acked-write loss. M2b
(partition-heal + a linearizability harness) and M3 (client redirect routing +
read model) are separate.

toyraft rc.3's http transport now accepts a multi-peer `PeerURLs` and a
`LeaderHint()` is on the node API — the two pieces M2a needs. Two rc.3 facts
shaped the design:

- `Propose` on a follower returns `*raft.ErrNotLeader{LeaderHint NodeID}`
  (`pkg/raft/node_public.go`) — a typed, `errors.As`-able rejection.
- The raft driver calls `Transport.Send` **synchronously** in its single driver
  goroutine (`pkg/raft/driver.go`), and the http transport's `Send` is a
  blocking POST (retries + `SendTimeout`). A dead/slow peer therefore stalls the
  whole driver — no ticks, no commits, no election. This is a **liveness bug**
  for any embedder using the http transport directly.

## Decision

1. **Membership on the CLI, not a config file.** `--peers id@baseURL,…` is the
   full membership *including self*; `--raft-addr host:port` is this node's raft
   transport listen address. Empty `--peers` keeps the M1 single-node path
   byte-for-byte. `internal/config` parses `--peers` into a **string-keyed**
   `map[id]url` (`ParsePeers`, `ClusterPeers`, `PeerURLsExcludingSelf`) so the
   config package stays free of a toyraft import; `cmd` converts to `raft.NodeID`
   at assembly. Validation (only when `Replicate && Peers!=""`): self ∈ peers,
   odd N, every entry `id@absolute-url`, no duplicate id, `--raft-addr` set.

2. **An async send decorator wraps the http transport — mandatory, not
   optional.** `internal/replication.asyncTransport` gives each peer a buffered
   queue + pump goroutine; `Send` enqueues and returns immediately, dropping on a
   full queue (raft's `Send` is best-effort — a dropped heartbeat/append is
   retransmitted next tick). Without it the synchronous driver blocks on a dead
   peer and the cluster loses liveness. `NewHTTPTransport` builds the http
   transport and wraps it. The M1 no-op `singleNodeTransport` stays for the
   empty-peers branch.

3. **Writes are leader-gated; reads stay local.** In multi-node mode `attachRaft`
   does **not** wait for self-leadership (a follower never self-leads) — it starts
   the node and lets the gate handle not-yet-leader writes. A mutating command
   proposed on a follower returns `*raft.ErrNotLeader`, which the broker exposes
   via `NotLeaderHint(err) (hint, ok)` (keeping the toyraft type inside broker).
   The session maps it to a new wire code **`NOTLEADER <leader-id>`** on
   PUB/ACK/NACK/CREATE. Reads (SUB/consume) are served locally on any node —
   documented non-linearizable; the read model is M3.

4. **The NOTLEADER hint is a NodeID, not an address.** M2a surfaces the leader's
   raft NodeID only. Resolving it to a client address and transparent client
   (CLI/TUI) auto-retry is M3 (`feat/cluster-routing`); doing it now would need a
   second NodeID→client-addr map with no consumer yet. The hint is best-effort —
   a freshly-elected follower may report an empty hint for a heartbeat or two;
   the client simply retries another node.

## Consequences

- `toymq --replicate --peers … --raft-addr …` forms a 3/5/7-node cluster; a
  leader-acked PUB replicates to every follower via `Propose→Apply`; killing the
  leader elects a new one among the survivors with the prior acked writes intact.
  Verified in-process over the http transport on loopback under `-race`
  (`internal/broker/cluster_test.go`: replicate-to-all, follower-rejects,
  survive-leader-kill).
- The async decorator is a permanent part of the embed as long as the driver
  sends synchronously — filed upstream as FRICTION-04
  (`docs/TOYRAFT-MIGRATION-REPORT.md`); the ideal upstream fix is a shipped async
  transport option or a driver that fans out Sends.
- New wire contract: `NOTLEADER` is a distinct ERR code from the command-specific
  `PUB_FAILED`/`ACK_FAILED`/… — a client can special-case redirects without
  string-matching. The contract is stable; only the hint's *resolution* changes
  in M3.
- Standalone and single-node `--replicate` paths are unchanged (empty `--peers`);
  the M1 regression suite stays green.

## Addendum (v3 M6) — unauthenticated transport & the public-bind guard

The toyraft peer transport is **unauthenticated and plaintext**: its threat model
is a trusted network (there is no per-peer auth, no TLS on the raft plane). That is
acceptable for a toy cluster on a private network or loopback, but binding
`--raft-addr` to a public/wildcard interface silently exposes an unauthenticated
Propose/Step surface to anyone who can reach the port.

**Decision:** `config.validate()` refuses a public raft bind by default. When
`--peers` is set, `isPublicBind(RaftAddr)` rejects an empty/wildcard host
(`0.0.0.0`, `::`) or a literal IP that is neither loopback nor private; a non-IP
hostname is left to the operator (cannot be classified without DNS). The escape
hatch is the explicit `--raft-allow-public-bind` flag — the operator affirms the
network is trusted. This mirrors the redis "protected mode" posture and keeps the
default safe without adding auth to the raft plane (real peer auth/TLS is out of
scope for v3, tracked for v4).

**Consequence:** `docker-compose.cluster.yml` binds the wildcard `0.0.0.0:7000`
inside its private compose network, so it passes `--raft-allow-public-bind`
deliberately — the one intended, documented use of the override.
