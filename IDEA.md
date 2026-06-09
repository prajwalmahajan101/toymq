# Ideas

Open-ended improvements that don't yet warrant an ADR. Promote to
[`docs/adr/`](./docs/adr/) when a direction is chosen; demote from here
when shipped or rejected.

Each entry is short on purpose — a launching pad, not a spec. If an idea
grows past a few lines, that's a signal it's ready to become an ADR.

## Legend

- **★** — high impact, low effort (start here if poking around)
- **△** — needs an ADR before code lands
- **◇** — experimental; may be rejected

---

## Durability & correctness

- **★ Persist dedupe LRU to disk.** The dedupe index
  (`internal/broker/dedupe.go`) lives in memory only — survives one
  broker lifetime, lost on SIGKILL. A sidecar file (`dedupe.json`,
  atomically swapped like the offsets file in
  [ADR 0006](./docs/adr/0006-debounced-atomic-offsets.md)) closes the
  one chaos-test limitation flagged in
  [ADR 0013](./docs/adr/0013-pkg-client-architecture.md).
- **△ Batched-fsync mode.** [ADR 0002](./docs/adr/0002-per-message-fsync.md)
  picked per-message; the benchmark column in the README is reserved
  for batched. Group-commit with a configurable interval bound trades
  per-publish durability latency for throughput. Will need an ADR
  superseding 0002.
- **△ Replication (raft or chain).** The current line protocol has no
  framing for multi-node consensus. Either Raft (single leader,
  log-replicated WAL) or chain replication (head-tail). Big design
  surface; ADR first.
- **◇ Idempotent consumer offsets.** Currently consumer state is
  best-effort. Stricter idempotency keys on the ack path would close
  exactly-once for cooperative clients.

## Observability

- **★ Prometheus alerting rules.** The metrics stack
  ([ADR 0015](./docs/adr/0015-observability-stack.md)) exposes 11 series
  but ships no alerts. Add `observability/prometheus/alerts.yml` with
  SLOs: p99 WAL append latency, redelivery rate, inflight backlog,
  publish-failure rate.
- **△ W3C `traceparent` propagation on the wire.** Today broker spans
  are always root spans. Adding an optional `TRACEPARENT` line before
  PUB/SUB frames would correlate producer → broker → consumer traces.
  Needs an ADR — wire-protocol change.
- **◇ Per-consumer lag exporter.** A gauge of `(latest msgID - last acked
  msgID)` per `(topic, consumer)` makes "is this consumer falling
  behind?" answerable from Grafana.

## Performance & scale

- **△ Reader backpressure / flow control.** The delivery channel
  (`internal/broker/topic.go`, send-channel buffer) absorbs ~64 entries
  before blocking. A slow consumer holding many topics grows inflight
  unbounded. A read-side window or PAUSE/RESUME command would cap
  memory. Protocol-touching; ADR required.
- **◇ Redelivery benchmark suite.** `cmd/toymq-bench` measures publish
  latency only. The redelivery ticker is the second hot path — add
  scenarios for visibility-timeout expiry, NACK storms, and concurrent
  subscriptions.
- **◇ Zero-copy WAL replay.** `sendfile(2)` / `splice(2)` for handing WAL
  bytes straight to a socket on consumer catch-up. Premature until
  profiling shows replay is bottlenecked.

## Security

- **△ HELLO frame + TLS.** Currently no auth, no encryption. A HELLO
  frame negotiating protocol version + TLS would be the foundation.
  Listed in README as not-implemented; promote to ADR when prioritized.
- **△ Per-topic ACLs (capability tokens).** Bearer-style tokens on
  CONNECT scoping which topics a session may publish/subscribe.

## Code-review follow-ups (small, well-scoped)

These came out of the post-v1.3 review. Each is a one- or two-file fix.

- **Ack/Nack inflight gauge race** — `internal/broker/broker.go` ~L311–333.
  Gauge is updated after the consumer lock is released; a concurrent
  redelivery tick can produce stale cardinality. Move the metric write
  inside the locked section or switch to an atomic counter.
- **Defensive consumer snapshot in `flushDirty`** —
  `internal/broker/broker.go:166`. Iteration over consumers dereferences
  without a nil-check; a mid-iteration delete could panic. Snapshot the
  consumer list under the lock first.
- **Redelivery ticker lock scope** — `internal/broker/redelivery.go`.
  README "What surprised me" describes a past `-race` fix here; re-verify
  the scan holds `inflightMu` for the full window, not per-entry.

## Tooling / DX

- **◇ Cosign release-archive signing.** Deferred per
  [ADR 0017](./docs/adr/0017-release-automation.md). Keyless via GitHub
  OIDC; revisit when there's an external consumer.
- **◇ GHCR image push on tag.** CI already builds the Docker image on
  every push; tag-triggered push to `ghcr.io/prajwalmahajan101/toymq` is
  mechanical. Adds a registry to maintain.
- **◇ Helm chart.** A `deploy/helm/toymq/` chart for Kubernetes. Pairs
  naturally with the GHCR image push above.
- **◇ Homebrew tap.** Tap formula for `brew install toymq`. Low-effort
  but adds another release destination to keep in sync.

## Rejected / parked

_(empty — populated as we say "no" to ideas)_
