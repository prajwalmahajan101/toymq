# Architecture Decision Records

> For the structural overview — system layers, command flow diagrams,
> redelivery semantics, persistence model, and goroutine maps — see
> [`../ARCHITECTURE.md`](../ARCHITECTURE.md),
> [`../FLOWS.md`](../FLOWS.md),
> [`../REDELIVERY.md`](../REDELIVERY.md),
> [`../PERSISTENCE.md`](../PERSISTENCE.md), and
> [`../CONCURRENCY.md`](../CONCURRENCY.md). ADRs cover *why*; those
> docs cover *what*.


| #    | Title                                                | Status   |
| ---- | ---------------------------------------------------- | -------- |
| 0001 | [Framed record format for the WAL](./0001-framed-record-format.md) | Accepted |
| 0002 | [Per-message fsync with atomic committed offset](./0002-per-message-fsync.md) | Accepted |
| 0003 | [Crash recovery by full segment scan](./0003-recovery-by-scan.md) | Accepted |
| 0004 | [Sealed Command interface for the wire protocol](./0004-proto-sealed-types.md) | Accepted |
| 0005 | [Lazy topic registry with double-checked locking](./0005-broker-lazy-topic-registry.md) | Accepted |
| 0006 | [Debounced atomic offset persistence](./0006-debounced-atomic-offsets.md) | Accepted |
| 0007 | [Visibility-timeout redelivery and Inflight snapshot rule](./0007-visibility-timeout-redelivery.md) | Accepted |
| 0008 | [Per-connection Session: four-channel concurrency model](./0008-session-concurrency-model.md) | Accepted |
| 0009 | [Binary entry point: testable `run`, stdlib `flag`, and a config package](./0009-cmd-wiring-and-config.md) | Accepted |
| 0010 | [Integration test architecture](./0010-integration-test-architecture.md) | Accepted |
| 0011 | [Consumer state: explicit hasAcked flag](./0011-consumer-state-hasacked.md) | Accepted |
| 0012 | [Chaos test architecture](./0012-chaos-test-architecture.md) | Accepted |
| 0013 | [`pkg/client` architecture](./0013-pkg-client-architecture.md) | Accepted |
| 0014 | [TUI framework choice: Bubble Tea](./0014-tui-framework-choice.md) | Accepted |
| 0015 | [Observability stack: Prometheus + OpenTelemetry](./0015-observability-stack.md) | Accepted |
| 0016 | [CI lint + Go version matrix + local hook](./0016-ci-lint-and-matrix.md) | Accepted |
| 0017 | [Release automation with GoReleaser](./0017-release-automation.md) | Accepted |
| 0018 | [Dedupe LRU recovery from the WAL](./0018-dedupe-recovery-from-wal.md) | Accepted |

Each ADR is a short snapshot of *why* a decision was made, captured at the
time the decision landed in code. They are not living docs — when a decision
is overturned, write a new ADR that supersedes the old one rather than
editing history.
