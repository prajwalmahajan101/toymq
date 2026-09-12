# 0032 — Client routing: write redirect + leader-default read model (v3 M3)

**Status:** Accepted
**Date:** 2026-09-12
**Scope:** `pkg/client/`, `internal/broker/`, `internal/proto/`, `internal/server/`, `cmd/`
**Related:** [ADR 0030](./0030-cluster-mode-peer-transport.md) (M2a distributed core + NOTLEADER gate), [ADR 0031](./0031-partition-heal-linearizability-harness.md) (M2b partition-heal), [ADR 0028](./0028-raft-embedding-command-envelope.md) (raft embedding)

## Context

v3 M2 shipped a replicated cluster: a mutating command (PUB/ACK/NACK/CREATE) that
reaches a follower is proposed through raft, fails with `*raft.ErrNotLeader`, and
the server surfaces `ERR NOTLEADER <node-id>` (the hint is a raft NodeID). Reads
(SUB) were served locally on any node — documented non-linearizable. ADR 0030 §4
deferred two client-facing pieces to M3:

1. **Resolving the NOTLEADER hint and retrying.** Until M3 a CLI/TUI user who hit
   a follower just got an error — there was no client that followed the redirect.
2. **A leader-default read model.** SUB served follower-local state unconditionally,
   so a read after a write on a different node could miss it.

The goal: an any-node client transparently completes every write and reads
consistently from the leader, with an opt-in stale-read escape hatch.

## Decision

1. **Client-side hint resolution — no new server surface.** The wire keeps
   `ERR NOTLEADER <node-id>` (shipped in M2). The client is given the cluster's
   client addresses and resolves the hint itself. Members are seeded as
   `nodeID@host:port` (or bare `host:port`); a `NotLeaderError` whose hint matches
   a known id jumps straight to that member, otherwise the client sweeps the
   member set round-robin. Rejected alternative: a server `MOVED <host:port>` that
   advertises client addresses — it would require membership/gossip of client
   endpoints the broker does not otherwise need, coupling the raft layer to the
   client-facing address space. Resolution is purely a client concern, so it lives
   in the client.

2. **A redirecting wrapper, not a smarter Client.** `pkg/client.Client` stays a
   dumb single-connection transport. A new `ClusterClient` holds one live
   connection to the believed leader and mirrors the request surface
   (`Pub`/`PubDelay`/`Ack`/`Nack`/`Create`/`Sub`/`SubStale`). On a redirectable
   error it reconnects and replays the op. This keeps the single-conn path
   byte-identical for standalone users and isolates all routing in one type.

3. **NOTLEADER is a typed error.** `pkg/client` classifies an `ERR NOTLEADER`
   frame into `*NotLeaderError{Hint}` (matched with `errors.As`) instead of
   string-parsing `ErrServer`. A single `serverErr(frame)` helper does this for
   every request path so classification is identical everywhere.

4. **Redirect and mid-write failover are both retryable, bounded.** The wrapper
   retries on `*NotLeaderError` **and** on a transport/closed error (the connected
   leader died under an in-flight write), advancing to the next member each time.
   Retries are capped by a bounded exponential backoff with full jitter
   (`pkg/client/backoff.go`): a redirect storm during an election terminates with
   a surfaced error rather than spinning. rand + sleep are injectable for
   deterministic tests.

5. **Broker classifies all "cannot serve as leader" conditions as redirectable.**
   `Broker.NotLeaderHint` now matches not only `*raft.ErrNotLeader` but also
   `raft.ErrProposalDropped` (leadership lost mid-propose) and `raft.ErrStopped`
   (node stopping — a dying leader). Without this a client cannot route around a
   leader that is shutting down: its propose fails with `ErrStopped`, and the
   server would otherwise emit a command-specific `PUB_FAILED` the client would
   not retry. The two hint-less errors fall back to the broker's best-known leader,
   and an empty hint makes the client sweep — so redirect converges regardless.

6. **Leader-default reads, opt-in stale.** `Broker.IsLeader()` (standalone always
   true) gates SUB: in a replicated cluster a default SUB on a follower is
   redirected with `ERR NOTLEADER`, mirroring the write path. An explicit
   `SUB <topic> <consumer> STALE` (parsed into `SubCommand.Stale`) opts into a
   follower-local read — the documented non-linearizable path, since toyraft rc.3
   has no ReadIndex. ACK/NACK stay leader-gated always. `ClusterClient.Sub` follows
   redirects to the leader; `SubStale` subscribes against the connected node and
   does **not** redirect.

7. **Mid-stream failover after Sub is caller-driven.** If the leader dies after a
   Sub, the delivery channel closes and `ClusterClient.Err()` wraps `ErrTransport`;
   the caller re-subscribes. Automatic re-subscription is deferred to v4. The
   bounded-staleness `MAXLAG` contract for stale reads is also v4 (UP-3).

## Consequences

- An any-node CLI/TUI client (`toymqctl -cluster id@h:p,… ` / `toymq-tui -cluster …`)
  completes every write and lands SUB on the leader transparently. `--stale` on the
  consume path routes to a follower-local read.
- Standalone (`-addr`, no `-cluster`) is unchanged: `IsLeader()` is always true, a
  bare `SUB` is byte-identical, and the single-connection `Client` is untouched.
- `NotLeaderHint` matching three raft errors means a shutting-down or
  leadership-losing node redirects cleanly instead of leaking a `*_FAILED`; this
  also tightens the M2 write path.
- The wrapper's one-live-connection model means concurrent ops share a connection
  and a redirect; this suits the CLI/TUI (effectively serial). A connection pool
  is not needed at this scale and is not built.

## Usage

- Cluster client: `client.DialCluster(ctx, []string{"n1@host:6789", "n2@…"}, opts…)`.
- Stale read: `cc.SubStale(ctx, topic, consumer)` or `toymqctl sub --stale`.
- Detecting a redirect on a bare `Client`: `errors.As(err, &client.NotLeaderError{})`.
- The redirect/retry cap and backoff bounds are internal
  (`newBackoff(20ms, 500ms, 10)`); expose as options only if a real workload needs it.
