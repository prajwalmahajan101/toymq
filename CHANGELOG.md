# Changelog

All notable changes to ToyMQ are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
