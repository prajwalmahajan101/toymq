# Changelog

All notable changes to ToyMQ are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] — unreleased

The **"Useful" single-node** line (v2 M1–M7.5). Makes ToyMQ usable for
small real workloads: durable dedupe, tunable fsync, off-host auth/TLS,
partitions, flow control, retention/DLQ/delayed messages, and a correlated
observability stack. Two wire-breaking changes (M3 HELLO frame, M4 partition
arity) are clustered behind this major bump. Date/tag pending manual
verification (v2 M8).

### Added

- **Dedupe-LRU persistence** — the dedupe index is rebuilt from the WAL on
  `Open` (no sidecar file), closing the restart durability gap flagged in
  ADR 0013 (v2 M1, [ADR 0018](docs/adr/0018-dedupe-recovery-from-wal.md)).
- **Batched-fsync mode** — `--fsync per-message|batched|none` +
  `--fsync-interval`; a WAL group committer coalesces appends into one
  fsync while `committed` still advances only after durability (v2 M2,
  [ADR 0019](docs/adr/0019-batched-fsync-mode.md)).
- **HELLO handshake + bearer-token auth + TLS** — `HELLO <version> [AUTH
  <token>]` first line, `--auth-token-file`, a side-by-side `--tls-addr`
  listener, and `pkg/client` `WithAuth` / `WithTLS`. `--require-hello`
  toggles a plaintext migration window (v2 M3,
  [ADR 0020](docs/adr/0020-hello-auth-tls.md)).
- **Partitions (single-node)** — a topic is `N` independent ordered logs;
  `--default-partitions` + a `CREATE <topic> PARTITIONS <n>` verb; routing
  key hashing (`fnv1a`) / explicit `<topic>#<n>` / keyless round-robin;
  `SUB <topic>#*` fan-in. MsgID monotonic per partition (v2 M4,
  [ADR 0021](docs/adr/0021-partitions-single-node.md)).
- **Reader backpressure** — per-`(partition, consumer)` receive window
  (`--recv-window`, default 256) bounding inflight, plus session-scoped
  `PAUSE` / `RESUME` verbs (v2 M5,
  [ADR 0022](docs/adr/0022-reader-flow-control.md)).
- **Retention, DLQ, delayed messages** — rolling WAL segments with a
  size/duration sweeper; `--dlq-after-nacks` republishing to `<topic>.dlq`;
  `PUB … DELAY <ms>` visible-at scheduling (v2 M6,
  [ADR 0023](docs/adr/0023-wal-segmentation-retention.md),
  [0024](docs/adr/0024-dead-letter-queue.md),
  [0025](docs/adr/0025-delayed-messages.md)).
- **Correlated observability** — W3C `TRACEPARENT` wire propagation
  (producer→broker span linkage), `trace_id`/`span_id` in logs, a metric
  exemplar, +11 metric series incl. per-consumer lag, and a provisioned
  Grafana LGTM stack (`docker-compose.observability.yml`, dashboards,
  Prometheus SLO alerts) (v2 M7 / M7.5,
  [ADR 0026](docs/adr/0026-traceparent-wire-propagation.md),
  [0027](docs/adr/0027-correlated-telemetry.md)).

### Changed (wire-breaking — gated behind this major bump)

- **HELLO is now the first line on every connection** (default
  `--require-hello=true`). Raw line-oriented scripts prepend `HELLO 1`; the
  migration window (`--require-hello=false`) processes a non-HELLO first
  line as a command (v2 M3).
- **`PUB` / `MSG` / `ACK` / `NACK` carry a partition/routing field** and a
  new `CREATE` verb exists; `PUB` separates the routing key from the dedupe
  key. 1-partition topics keep the pre-M4 flat on-disk layout byte-for-byte
  (v2 M4).

## [1.3.0] — 2026-06-09

### Added

- **Observability** — Prometheus metrics + OpenTelemetry tracing
  ([ADR 0015](docs/adr/0015-observability-stack.md)).
- **CI lint matrix** ([ADR 0016](docs/adr/0016-ci-lint-and-matrix.md)) and
  **release automation** with goreleaser
  ([ADR 0017](docs/adr/0017-release-automation.md)).

## [1.2.0] — 2026-06-09

### Added

- **State-change logging** across broker / server / client — structured
  `slog` lines at each lifecycle transition.

## [1.1.0] — 2026-06-09

### Added

- **`cmd/toymq-tui`** — interactive Bubble Tea client; the first binary
  with a third-party runtime dependency
  ([ADR 0014](docs/adr/0014-tui-framework-choice.md)).

## [1.0.0] — 2026-06-09

First stable release. Single-node persistent message broker, stdlib
only, ~5k lines of Go.

### Added

- **WAL** (Write-Ahead Log) with CRC32-framed records, per-message
  fsync, and torn-tail recovery on Open
  ([ADR 0001](docs/adr/0001-framed-record-format.md),
  [0002](docs/adr/0002-per-message-fsync.md),
  [0003](docs/adr/0003-recovery-by-scan.md)).
- **Wire protocol** — line-oriented TCP, four verbs (`PUB` / `SUB` /
  `ACK` / `NACK`) and four responses (`OK` / `DUP` / `ERR` / `MSG`).
  Sealed Command interface
  ([ADR 0004](docs/adr/0004-proto-sealed-types.md)).
- **Broker** — lazy topic registry with double-checked locking,
  per-topic dedupe LRU, debounced atomic offset persistence, and
  visibility-timeout redelivery
  ([ADRs 0005](docs/adr/0005-broker-lazy-topic-registry.md),
  [0006](docs/adr/0006-debounced-atomic-offsets.md),
  [0007](docs/adr/0007-visibility-timeout-redelivery.md),
  [0011](docs/adr/0011-consumer-state-hasacked.md)).
- **Server** — per-connection Session with the four-channel
  concurrency model (reader / handler / writer / done) and graceful
  shutdown
  ([ADR 0008](docs/adr/0008-session-concurrency-model.md)).
- **`cmd/toymq`** — broker binary with `--addr`, `--data-dir`,
  `--log-level`, `--log-format`, `--shutdown-timeout`, `--dedupe-cap`
  ([ADR 0009](docs/adr/0009-cmd-wiring-and-config.md)).
- **`pkg/client`** — reusable Go client library. Single read
  goroutine, mutex-serialised writes, head-of-queue pending FIFO for
  every request-shaped verb, caller-owned reconnect
  ([ADR 0013](docs/adr/0013-pkg-client-architecture.md)).
- **`cmd/toymqctl`** — operator CLI with `pub`, `sub`, and `ack`
  subcommands. Quoted-payload output for `sub`; auto-ACK by default;
  signal-driven shutdown.
- **`cmd/toymq-bench`** — stdlib-only throughput + latency harness
  reporting min / p50 / p95 / p99 / max via sorted-slice percentiles
  (no histogram library).
- **Tests** — integration suite covering 8 scenarios
  ([ADR 0010](docs/adr/0010-integration-test-architecture.md)) and a
  chaos SIGKILL soak test
  ([ADR 0012](docs/adr/0012-chaos-test-architecture.md)).
- **Docker** — multi-stage build producing a ~3 MB `FROM scratch`
  image. `Dockerfile`, `.dockerignore`, `docker-compose.yml`.
- **CI** — GitHub Actions with three parallel jobs
  (`go vet` + `gofmt` + `go test -race`, chaos smoke, docker build).
- **Docs** — five mermaid-illustrated architecture docs
  ([ARCHITECTURE](docs/ARCHITECTURE.md),
  [FLOWS](docs/FLOWS.md),
  [REDELIVERY](docs/REDELIVERY.md),
  [PERSISTENCE](docs/PERSISTENCE.md),
  [CONCURRENCY](docs/CONCURRENCY.md)) plus 13 ADRs.

### Guarantees

- **Durability:** `PUB` returns `OK` only after a successful
  `fsync(2)`. The message survives any unclean exit at that point.
- **At-least-once delivery:** every `PUB` that returned `OK` will be
  delivered to at least one consumer per topic. Duplicates allowed.
- **Idempotent producer:** dedupe key + in-memory LRU returns `DUP`
  for repeats within the LRU window. **Limitation:** the LRU does
  not persist across restarts.
- **Crash recovery:** WAL scan on `Broker.Open` truncates torn tails
  (length-bounded read + CRC32 verification) and rebuilds consumer
  state from `offsets.json`.

### Public surface (stable from this release)

- The wire protocol — verbs, responses, and the line/length framing
  documented in [`README.md` § Wire protocol](README.md#wire-protocol).
- `pkg/client.Client` and its `Dial` / `Pub` / `Sub` / `Ack` /
  `Nack` / `Close` methods, plus the sentinel errors
  (`ErrClosed` / `ErrTransport` / `ErrServer` / `ErrSubInUse`).
- `cmd/toymq` flags listed above.
- `cmd/toymqctl` subcommands and the 0 / 1 / 2 exit code convention.

Wire-incompatible changes from here on will bump the major version.
Additive `pkg/client` surface area (e.g. a `Retrying` wrapper) ships
as minor versions.

### Out of scope (deferred to future milestones)

- `cmd/toymq-tui` — interactive Bubble Tea client. First binary
  with a third-party runtime dependency; requires `ADR 0014`.
- Batched-fsync mode — group commits with a configurable interval
  bound. Lifts the per-publish durability latency ceiling.
- Dedupe-LRU persistence across restarts — closes the chaos-suite
  limitation flagged in ADR 0013.
- Replication, authentication, TLS, observability hooks. See
  [`README.md` § Roadmap](README.md#roadmap).

<!-- Compare links. The 2.0.0 target resolves once the tag is cut (v2 M8). -->

[2.0.0]: https://github.com/prajwalmahajan101/toymq/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/prajwalmahajan101/toymq/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/prajwalmahajan101/toymq/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/prajwalmahajan101/toymq/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/prajwalmahajan101/toymq/releases/tag/v1.0.0
