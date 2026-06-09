# 0012 — Chaos test architecture

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `test/chaos/`

## Context

ToyMQ's integration suite (`internal/integration/`, Step 13) drives
the broker over real TCP across eight scripted scenarios, but never
takes the broker down mid-flight. The system has been carefully built
to survive crashes — per-message fsync, WAL recovery on open,
debounced offset persistence with a final flush on shutdown — but
none of that machinery is exercised under actual crash injection.
The only way to verify the durability contract is to kill the broker
unexpectedly and check whether any acknowledged work was lost.

The contract under test is the **no-loss invariant**:

> Every `MsgID` for which the producer received `OK` must eventually
> appear in the consumer's ACK log.

A 10-minute SIGKILL soak is the cheapest way to gain confidence in
this property without a formal model.

## Decision

### Location and build tag

The suite lives at `test/chaos/`, in package `chaos`, with every file
guarded by `//go:build chaos`. `go test ./...` continues to skip it;
chaos runs are opt-in:

```
go test -tags chaos -v ./test/chaos/...
```

Putting the suite at the repo root (rather than under `internal/`)
matches BUILD_GUIDE convention and makes the "external client"
framing literal: nothing here imports `internal/broker`,
`internal/server`, or `internal/proto`. The only ToyMQ artefact the
test depends on is the built `cmd/toymq` binary.

### Subprocess broker, built once

`TestMain` runs `go build -o <tmp>/toymq ./cmd/toymq` exactly once
per test invocation and passes the binary path to the test. The
supervisor `exec.Command`s that path with each restart. Per-restart
recompilation (`go run ./cmd/toymq`) would waste several seconds of
the 10-minute budget on the Go toolchain.

`Process.Signal(syscall.SIGKILL)` is the crash injector — explicitly
not SIGINT, because SIGINT triggers the graceful shutdown path the
broker already handles cleanly. We want to prove durability under
unclean exits, where `defer b.Close()` does **not** run, the persist
loop's final flush does **not** happen, and the WAL may have a torn
tail.

### Duration and CHAOS_DURATION

Default: 10 minutes (the BUILD_GUIDE soak target). Override via
`CHAOS_DURATION` env var with any `time.ParseDuration`-accepted
string. The env var is for development iteration
(`CHAOS_DURATION=30s`) and to keep CI runs short if/when this gets
wired into CI; the `chaos` build tag is the primary gate against
accidental invocation.

### Producer with dedupe-key retry

The producer uses monotonic dedupe keys (`chaos-1`, `chaos-2`, …)
and retains the current key across reconnects. When a PUB roundtrip
fails (write error, EOF mid-OK), the same key is retried on the new
connection. This protects against in-process broker hiccups —
a slow OK, a transient EOF — but **does not** protect against
dedupe-LRU loss across a SIGKILL (see Limitations below).

The producer records every `OK <id>` and every `DUP <id>` into a
single `okMsgIDs` slice. Both count as "the producer believes this
MsgID is durable."

### Consumer with reconnect + re-SUB

The consumer subscribes with a stable ID (`chaos-consumer-1`) and
ACKs every MSG it receives. On transport failure it reconnects and
re-SUBs; the broker's `runDelivery` opens a fresh WAL reader at
`lastAcked+1` and replays anything unacked. The consumer's `seen`
map records `MsgID → delivery count` — duplicates are allowed (the
broker provides at-least-once), but loss is not.

### End-of-test drain protocol

Naive shutdown loses messages: producer's last PUB gets OK, the test
asserts before the consumer's reader sees the corresponding MSG.
The protocol is:

1. Stop the supervisor's kill loop. Broker stays up.
2. Stop the producer. Snapshot its `okMsgIDs`.
3. Poll the consumer's `seen` map every 500 ms for up to 30 s,
   waiting until it's a superset of `okMsgIDs`.
4. Stop the consumer, kill the broker subprocess, assert.

If the drain timeout fires before catch-up, the test prints the
missing MsgIDs and fails. That output is the diagnostic; without it,
"missing MsgIDs" would be reported only as a count.

## Consequences

**Positive**
- The durability contract is now exercised under actual crash
  injection, not just clean restarts.
- The test catches regressions across the entire stack: WAL torn-tail
  recovery, offset persistence, consumer resume, dedupe-LRU
  retention within a single broker lifetime.
- The build-tag gate means there is zero ongoing cost to having this
  suite in the repo. Default `go test ./...` is unaffected.

**Negative**
- A 10-minute test is expensive to run frequently. The
  `CHAOS_DURATION=30s` smoke is the practical day-to-day check; the
  10-minute soak is a pre-merge / pre-release activity.
- A flake in the chaos test is harder to diagnose than a unit-test
  flake. The merged stderr capture from the broker subprocess
  partially mitigates this.
- Building the broker binary in `TestMain` adds ~1 s of setup. Worth
  it for the per-spawn savings.

## Limitations of this test

These are properties the test does **not** prove. Documenting them
here so future readers don't over-claim what a clean run means.

- **Dedupe across restarts.** The broker's dedupe LRU
  (`internal/broker/dedupe.go`) lives only in process memory. After
  a SIGKILL, the new broker has an empty LRU. If the producer sent
  PUB with key `chaos-K`, got OK, and the broker died before that
  key made it into anyone's awareness on disk, the producer's retry
  with the *same key* on the new broker will be treated as a fresh
  publish. The same logical payload now occupies two WAL slots, two
  MsgIDs. The producer records both. The consumer eventually sees
  both. The no-loss invariant still holds; what's weaker than naive
  reading suggests is "same dedupe key returns the same MsgID
  forever." If that property is ever needed, the dedupe LRU must
  persist alongside the WAL.
- **Disk-level corruption.** The test SIGKILLs the process; it does
  not corrupt files, truncate segments mid-flight, or simulate
  disk-full. WAL torn-tail recovery is exercised, but only the
  specific shape of torn tail that an interrupted Append produces.
- **Network partition.** Producer, consumer, and broker share
  `localhost`. The test does not simulate dropped packets, latency,
  or reordered TCP segments.
- **Multi-consumer coordination.** One producer, one consumer.
  Fan-out and consumer takeover are covered by the integration
  suite, not here.
- **Performance regressions.** The test asserts correctness, not
  throughput. The producer interval is fixed; missed throughput
  targets would not fail the test.

## Edge cases

- **SIGKILL during the producer's PUB write.** Producer gets a write
  error on the next byte, closes the conn, reconnects, retries with
  the same key. The torn write is truncated by WAL recovery on the
  new broker's startup; the retry succeeds with a fresh MsgID.
- **SIGKILL between WAL fsync and OK response.** The message is
  durable but the producer did not receive OK. Producer retries
  with the same key. Empty dedupe LRU on the new broker → second
  fsync, second MsgID. Both are in the consumer's seen set. (See
  Limitations.)
- **SIGKILL during the consumer's ACK send.** ACK never reaches the
  broker. Consumer reconnects and re-SUBs; broker replays the
  message. Consumer ACKs again. `seen[MsgID]` increments to 2;
  test still passes since the contract is "≥1," not "==1."
- **Drain timeout fires.** Most likely cause is a real bug
  (consumer wedged, broker stuck), not a slow machine — the drain
  budget is 30 s for the residual in-flight at producer-stop, which
  is at most a few hundred messages. If this trips on a clean
  machine, investigate before bumping the timeout.

## Usage

- Compile-check only:
  ```
  go build -tags chaos ./test/chaos/...
  ```
- 30-second smoke run:
  ```
  CHAOS_DURATION=30s go test -tags chaos -v ./test/chaos/...
  ```
- Full 10-minute soak:
  ```
  go test -tags chaos -v -timeout 15m ./test/chaos/...
  ```
- Reading a failure: the test prints producer stats
  (`keysSent, dupHits, retries`), consumer stats
  (`uniqueSeen, totalDeliveries, errors, resubs`), supervisor
  restart count, and — on assertion failure — the list of MsgIDs in
  `producer.okMsgIDs \ consumer.seen`.
