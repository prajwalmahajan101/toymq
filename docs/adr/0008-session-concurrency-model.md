# 0008 — Per-connection Session: four-channel concurrency model

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `internal/server/session.go`

## Context

Each client TCP connection has to do three things at once:

- Read framed commands off the wire and dispatch them to the broker.
- Receive broker deliveries (`*Inflight`) and emit `MSG` frames.
- Respond to each command synchronously (`OK`, `DUP`, `ERR`).

A single goroutine cannot do all three — reading is a blocking call,
delivery arrival is asynchronous, and the two write paths (responses
and `MSG` frames) must not interleave or the wire format is corrupted.

Two goroutines is the minimum: one reads the conn, one writes to it.
But the moment you have two goroutines sharing the conn, you have to
answer how each tells the other "I'm done," how the writer is told
what to write, and how shutdown sequencing works without losing the
last in-flight frame to a race.

## Decision

A `Session` owns four channels and runs two goroutines:

- `respCh chan func(*bufio.Writer) error` (buf 8) — reader pushes
  response closures; writer invokes them.
- `sendCh chan *broker.Inflight` (buf 64) — broker pushes deliveries
  from the active subscription; writer emits them as `MSG` frames.
- `quit chan struct{}` — closed by `Run` after the reader exits to
  signal the writer to leave its `select`.
- `writerDone chan struct{}` — closed by the writer on its way out;
  `Run` waits on it and `sendResp` watches it to escape a deadlock
  if the writer died before the reader noticed.

`Run(ctx)` spawns the writer, runs the reader inline, then on reader
return: cancels the active subscription, closes `quit`, waits on
`writerDone`, and closes the conn.

### Single-writer invariant

`runWriter` is the only goroutine that calls `bufio.Writer.Write`,
`Flush`, or `proto.Write*`. Both response closures and delivery
frames pass through this one goroutine's `select`. This eliminates
the class of bug where a partial `MSG` frame interleaves with an
`OK` and produces an unparseable byte stream.

### Single-owner reader state

`currentTopic`, `currentSub`, and `currentCancel` are touched only
by the reader goroutine. No mutex protects them because no second
goroutine can reach them. `SUB`, `ACK`, and `NACK` all run on the
reader's stack.

### Response payload: closures, not types

Items on `respCh` are `func(*bufio.Writer) error`. The reader captures
its arguments and builds the closure; the writer just calls it. No
parallel response-type hierarchy mirroring `proto.Command`. Per-type
arg sets are encoded in the closure body. The cost — closures are
opaque for logging — is accepted for now and revisited when
middleware is on the horizon.

### Subscription lifecycle

Each `SUB` derives a cancellable context from `Run`'s `ctx`:

```
subCtx, cancel := context.WithCancel(ctx)
sub, _ := broker.Subscribe(subCtx, topic, consumerID, sendCh)
```

A second `SUB` on the same session cancels the prior `subCtx` before
creating the new one; the broker's `Topic.Subscribe` independently
handles the swap on its side. Session exit (reader return) calls the
current `cancel`, which terminates the broker's delivery goroutine
cleanly.

The session does not call `<-sub.done` — the broker owns that
synchronization and the session does not need it for correctness.
Orphaned subscriptions whose `cancel` has been called will see the
broker's `runDelivery` exit on its next `ctx.Done()` poll.

### Shutdown ordering

On reader return:

1. `currentCancel()` if a subscription is live — broker delivery
   goroutine winds down, stops pushing into `sendCh`.
2. `close(quit)` — writer's `select` picks the quit arm and returns.
3. `<-writerDone` — wait for the writer to fully exit, including any
   in-flight `Flush()`.
4. `conn.Close()` — final cleanup.

If the writer hit a wire-write error before this sequence began, it
calls `conn.Close()` itself before exiting. That makes the reader's
next `bufio.ReadString` return an error, which unblocks the reader
into the same shutdown sequence above. `Close` is idempotent on the
conn so the final `conn.Close()` above is a safe no-op.

### Topic inference for ACK/NACK

The wire `AckCommand` and `NackCommand` carry `(ConsumerID, MsgID)`
but no topic. The session remembers the topic from the last `SUB`
and uses it for all subsequent `ACK`/`NACK`. An `ACK` before any
`SUB` returns `ERR NO_SUB`.

This is a deliberate consequence of the "one session, one active
subscription" model. The same client can switch topics by issuing a
new `SUB`; from that point on, ACKs are interpreted against the new
topic. Messages in flight from the prior subscription (still in
`sendCh` or already written) will fail their ACK against the new
topic — that's correct behavior, and clients are expected to
either ACK before switching or accept the broker's redelivery.

## Consequences

**Positive**
- Zero shared mutable state across goroutines. No mutex in the file.
- The race detector has nothing to find; the design is correct by
  construction.
- Goroutine accounting is tight: two per session, both observable
  via `runtime.NumGoroutine()`. A leak surfaces immediately in
  tests.
- Shutdown is deterministic: reader return → subscription cancel →
  writer drain-and-quit → conn close. No race window where a frame
  is half-written when the conn closes.
- The writer pre-closes the conn on wire errors, which kicks the
  reader out of its blocking read without requiring a separate
  failure-notification channel.

**Negative**
- Closures on `respCh` are opaque. A future logging or metrics layer
  that wants to inspect responses will need to either move to a
  sealed-type model or wrap every closure with metadata. Tracked.
- If the writer's `case <-quit` arm is ever dropped (it was, in the
  first cut), `Run` deadlocks at `<-writerDone`. The test suite
  catches this immediately via the 1-second `time.After` guards;
  worth keeping those guards strict.
- `sendResp` watches `writerDone` as a deadlock escape. That means
  if the writer dies due to a wire error, in-flight responses
  silently drop. Acceptable: the conn is already broken, the client
  isn't getting any response anyway.
- The "topic from last SUB" rule means a client that pipelines
  `SUB topic1 c1`, `SUB topic2 c1`, then `ACK c1 N` will have its
  ACK interpreted against `topic2`. Documented; clients that need
  multi-topic ACK ordering should open multiple connections.

## Edge cases

- **`SUB` followed by another `SUB`.** Old `subCtx` is canceled, new
  one is created. Broker's `Topic.Subscribe` swaps `c.sub` and
  cancels the prior delivery goroutine. Messages already in
  `sendCh` (delivered to the old subscription) still get written to
  the wire — the writer doesn't know which subscription delivered
  them.
- **`ACK` for an `MsgID` not in flight.** Broker returns
  `"ack: msg N not in inflight for consumer X"`. Session wraps it
  as `ERR ACK_FAILED <reason>`. Wire-level UX is honest about the
  failure.
- **Invalid command in a well-framed line** (e.g. `WAT garbage\n`).
  Reader emits `ERR INVALID <reason>` and *continues*. The byte
  stream is still in sync.
- **Bad framing** (line over `MaxLineLength`, missing trailing
  newline, short body). Reader returns without responding. The
  conn is no longer trustworthy as a command stream.
- **Client closes mid-command.** Reader's `ReadCommand` returns the
  underlying I/O error; reader returns; standard shutdown sequence
  fires.

## Usage

- Production: `NewSession(conn, broker).Run(ctx)`. The conn is
  typically a `net.Conn` from the accept loop; `io.ReadWriteCloser`
  is the type accepted so tests can pass `net.Pipe()` pairs.
- Tests: `net.Pipe()` gives two synchronous in-memory `net.Conn`s.
  Writes to one block until reads on the other consume them, so
  test goroutines must split read and write across goroutines.
- A new wire verb: add to `proto.ReadCommand`, add a case to
  `handleCommand`, write a `handle<Verb>` that pushes a response
  closure. Single contained extension point.
