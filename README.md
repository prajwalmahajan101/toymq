# ToyMQ

A single-node persistent message broker written in Go as a learning
project. Stdlib only, ~5k lines of code, 13 ADRs documenting every
non-obvious decision. Not production software.

What it does:

- **PUB / SUB / ACK / NACK** over a line-oriented TCP protocol.
- **Per-message fsync** durability — every `OK` is preceded by a
  successful `fsync(2)`.
- **At-least-once** delivery with visibility-timeout redelivery.
- **Idempotent producer** support via in-memory dedupe LRU.
- **Crash recovery** by full WAL scan with torn-tail truncation.

What it doesn't:

- No replication, no multi-node, no cluster.
- No authentication, no TLS, no authorization.
- No dynamic topic management (topics auto-create on first publish).
- No metrics, no tracing, no logger hooks.
- No batched fsync (mode exists in plans but not implemented in v1).

---

## 30-second quickstart

Two terminals, one publish, one subscribe.

```bash
# Build.
go install ./cmd/toymq ./cmd/toymqctl

# Terminal A — start the broker.
toymq --data-dir /tmp/toymq

# Terminal B — subscribe.
toymqctl sub orders consumer-1

# Terminal C — publish.
toymqctl pub orders "hello"
toymqctl pub orders "world"
```

Terminal B prints:

```
MSG topic=orders id=0 payload="hello"
MSG topic=orders id=1 payload="world"
```

Auto-ACK is on by default; kill terminal B and resume:

```bash
toymqctl sub orders consumer-1
# (silent — both messages already acked, nothing replays)
```

To see redelivery in action, use `--no-auto-ack`:

```bash
toymqctl sub orders consumer-2 --no-auto-ack --max-msgs 1
# MSG topic=orders id=0 payload="hello"
# (consumer exits without acking)

toymqctl sub orders consumer-2 --max-msgs 1
# MSG topic=orders id=0 payload="hello"   ← redelivered
```

---

## Wire protocol

Line-oriented, length-prefixed for binary payloads. The wire is
deliberately small: four verbs, four response shapes. Every frame
ends with `\n`. Bytes between framing are arbitrary.

### Commands (client → broker)

| Verb | Wire shape | Notes |
|---|---|---|
| `PUB` | `PUB <topic> <key> <len>\n<payload>\n` | `<key>` is `-` for no dedupe key. `<len>` is the byte length of `<payload>`. |
| `SUB` | `SUB <topic> <consumer-id>\n` | Registers the consumer; broker starts replaying from `lastAcked + 1`. |
| `ACK` | `ACK <consumer-id> <msg-id>\n` | Confirms delivery; advances `lastAcked` when the prefix is contiguous. |
| `NACK` | `NACK <consumer-id> <msg-id>\n` | Returns the msg to pending; redelivered on the next `runDelivery` iteration. |

### Responses (broker → client)

| Response | Wire shape | When |
|---|---|---|
| `OK` | `OK <msg-id>\n` | Success. For PUB the id is freshly assigned; for SUB it is `0` (placeholder); for ACK/NACK it echoes the target id. |
| `DUP` | `DUP <msg-id>\n` | Dedupe LRU hit; the original assignment for this key is returned. No new WAL write. |
| `ERR` | `ERR <code> <reason>\n` | Server error. `<code>` is a stable token (`PUB_FAILED`, `NO_SUB`, etc.); `<reason>` is human text. |
| `MSG` | `MSG <topic> <msg-id> <len>\n<payload>\n` | Async push from a subscription. Order matches `msg-id`. |

Source of truth: [`internal/proto/parser.go`](./internal/proto/parser.go),
[`internal/proto/response.go`](./internal/proto/response.go).

### What's on the wire?

Skip the client and talk to the broker directly with `nc`:

```bash
# In one terminal, start the broker.
toymq --data-dir /tmp/toymq-nc

# In another:
printf 'PUB orders - 5\nhello\n' | nc -q1 localhost 6789
# OK 0

printf 'SUB orders raw-consumer\nACK raw-consumer 0\n' | nc -q1 localhost 6789
# OK 0
# MSG orders 0 5
# hello
# OK 0
```

The `-q1` is `ncat`'s "quit after 1 second of EOF on stdin" — needed
because the broker keeps the conn open for streaming.

---

## Durability semantics

ToyMQ's durability contract, in three sentences:

1. **PUB returns `OK` only after `fsync(2)` returns success.** The
   message survives any unclean exit at that point — kernel panic,
   SIGKILL, power loss.
2. **At-least-once.** Every PUB that returned `OK` will be delivered
   to at least one consumer per topic. Duplicate deliveries are
   allowed; the chaos suite verifies the no-loss invariant under
   SIGKILL.
3. **ACKs are debounced to disk every ≤100 ms.** An ack lost in that
   window is recovered by redelivery — the WAL is the source of truth,
   `offsets.json` is a cache.

Deeper material:

- [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) — layered system,
  data model, WAL framing.
- [`docs/FLOWS.md`](./docs/FLOWS.md) — sequence diagrams for every
  command verb.
- [`docs/REDELIVERY.md`](./docs/REDELIVERY.md) — at-least-once,
  visibility timeout, gap-aware `lastAcked`.
- [`docs/PERSISTENCE.md`](./docs/PERSISTENCE.md) — WAL append,
  `offsets.json` atomic swap, recovery scan, graceful shutdown.
- [`docs/CONCURRENCY.md`](./docs/CONCURRENCY.md) — goroutine map,
  channels, lock hierarchy.

---

## Architecture at a glance

One process, five layers, ~5k lines of Go. Every TCP byte passes
through:

```
Client → Listener → Session → Broker → Topic → WAL → Disk
```

Two background goroutine kinds run alongside the request path: a
**redelivery ticker** per topic (returns expired Inflight entries to
pending — see [`docs/REDELIVERY.md`](./docs/REDELIVERY.md)) and a
**single offset-persist debouncer** per broker (writes `offsets.json`
atomically every ≤100 ms). The full goroutine census, with a count
formula in terms of conns / subs / topics, lives in
[`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md#3-runtime-goroutine-census).

The structural docs in `docs/` cover the system *what*; the ADRs in
`docs/adr/` cover the design *why*.

---

## Architecture Decision Records

Thirteen ADRs in [`docs/adr/`](./docs/adr/README.md) — each captures
why a non-obvious decision was made at the time it landed in code.
ADRs are not living docs; if a decision is overturned, a new ADR
supersedes the old one.

| # | Title |
|---|---|
| [0001](./docs/adr/0001-framed-record-format.md) | Framed record format for the WAL |
| [0002](./docs/adr/0002-per-message-fsync.md) | Per-message fsync with atomic committed offset |
| [0003](./docs/adr/0003-recovery-by-scan.md) | Crash recovery by full segment scan |
| [0004](./docs/adr/0004-proto-sealed-types.md) | Sealed Command interface for the wire protocol |
| [0005](./docs/adr/0005-broker-lazy-topic-registry.md) | Lazy topic registry with double-checked locking |
| [0006](./docs/adr/0006-debounced-atomic-offsets.md) | Debounced atomic offset persistence |
| [0007](./docs/adr/0007-visibility-timeout-redelivery.md) | Visibility-timeout redelivery and Inflight snapshot rule |
| [0008](./docs/adr/0008-session-concurrency-model.md) | Per-connection Session: four-channel concurrency model |
| [0009](./docs/adr/0009-cmd-wiring-and-config.md) | Binary entry point: testable `run`, stdlib `flag`, and a config package |
| [0010](./docs/adr/0010-integration-test-architecture.md) | Integration test architecture |
| [0011](./docs/adr/0011-consumer-state-hasacked.md) | Consumer state: explicit `hasAcked` flag |
| [0012](./docs/adr/0012-chaos-test-architecture.md) | Chaos test architecture |
| [0013](./docs/adr/0013-pkg-client-architecture.md) | `pkg/client` architecture |

---

## Benchmarks

**TODO: filled in after Step 14c (`toymq-bench` harness).**

The benchmark table will compare per-message fsync vs batched fsync
(when implemented) across throughput and p50 / p95 / p99 latency.
Schema:

| Mode | Throughput (msg/s) | p50 (µs) | p95 (µs) | p99 (µs) |
|---|---|---|---|---|
| per-msg fsync | — | — | — | — |
| interval fsync (10 ms) | — | — | — | — |
| interval fsync (100 ms) | — | — | — | — |

Numbers will be measured on a single host (specs documented inline)
and committed alongside the `cmd/toymq-bench` binary.

---

## What surprised me

Five things from building this that I didn't expect going in.

**Fsync latency dominates everything.** Per-message `fsync(2)` runs
~1–2 ms on a commodity SSD — orders of magnitude slower than any other
operation in the broker. There's almost no point optimising elsewhere
until you've decided whether to batch fsyncs. ADR 0002 picked
per-message; the cost shows up as a hard per-connection publish ceiling
that no Go-side cleverness can move.

**The first SIGKILL surfaced a torn-tail bug.** Integration tests
shut down cleanly. The chaos suite SIGKILLs. On the first chaos run,
recovery parsed a partial trailing record, picked up garbage, and
rejected the WAL. The fix — length-bounded read + CRC verification
with truncation on either failure — is now ADR 0003. Clean-shutdown
tests are not a substitute for chaos tests.

**`-race` caught a redelivery-ticker race I'd "proven" safe.** The
ticker scanned Inflight without holding `inflightMu`, on the wrong
assumption that the map snapshot was stable mid-tick. `-race` fired
within seconds; without it, the test ran clean for minutes. Three
lines moved the lock above the scan. "Small loops look safe" — they
don't.

**The `hasAcked` flag is non-obvious until it isn't.** First draft
used `lastAcked uint64` with the convention "0 means never acked."
Then a consumer acked msg 0 — "never acked" and "acked msg 0" had the
same on-disk representation, and resume silently replayed. ADR 0011
introduces the explicit boolean. One byte of state turns an ambiguous
representation into an unambiguous one.

**A single FIFO collapsed `pkg/client`'s response routing.** First
sketch was `map[verbID]chan response` keyed by a per-request token.
The wire protocol's strict response ordering makes that map
unnecessary — a slice with cancel-tombstones covers every case. The
simpler version survived the chaos refactor without changes (ADR
0013). Read the protocol contract before designing the data structure.

---

## Roadmap

Out-of-scope for v1. Each is its own future milestone.

- **`cmd/toymq-bench`** — throughput + latency harness with p50 /
  p95 / p99 reporting. Fills the benchmark table above.
- **`cmd/toymq-tui`** — interactive Bubble Tea client. First binary
  with a third-party runtime dependency; needs an ADR before it
  lands.
- **Replication** — multi-node, raft or chain-replication. Would
  require a network framing layer beyond the current line protocol.
- **Batched fsync mode** — group commits with configurable interval
  bound. Trades per-publish durability latency for throughput; the
  benchmark mode is already scaffolded in the spec.
- **Dedupe LRU persistence** — currently in-memory only; survives
  one broker lifetime, not a SIGKILL. Persisting it would close the
  one chaos-test limitation flagged in ADR 0013.
- **Authentication / TLS** — neither exists. The line protocol would
  need a HELLO frame; out of scope for a learning project but listed
  here so it's not forgotten.
- **Metrics / tracing / structured logging** — the broker emits
  slog at boot but no per-request observability. Hooks would slot
  cleanly into the Session handler.

---

## License

MIT. See [`LICENSE`](./LICENSE).
