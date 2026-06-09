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
