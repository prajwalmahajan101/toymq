# 0013 — `pkg/client` architecture

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `pkg/client/`

## Context

ToyMQ has accumulated three flavours of wire-protocol client over its
build-out:

- `internal/integration/client.go` — a `*testing.T`-bound helper that
  fails fast on any protocol drift.
- `test/chaos/client.go` — an error-returning twin used by the chaos
  producer/consumer so reconnect loops can survive transport errors.
- Hand-rolled framing inside the chaos producer and consumer, plus
  upcoming consumers (`cmd/toymqctl`, `cmd/toymq-bench`, eventual TUI).

Each flavour reimplements the same parsing, the same write framing,
the same OK/DUP/ERR interpretation. Keeping them in sync as the wire
protocol evolves is busywork that will eventually drift. ToyMQ also
ships no public Go library yet — every external consumer would have
to rewrite this code.

`pkg/client` is the canonical client: one TCP connection per `Client`,
typed `Pub` / `Sub` / `Ack` / `Nack` methods, no third-party
dependencies. It is the layer all four upcoming binaries
(`toymqctl`, `toymq-bench`, the TUI, the refactored chaos
producer/consumer) sit on top of.

## Decision

### One read goroutine, mutex-serialised writes

A single `readLoop` goroutine owns `c.r` and is the only goroutine
that calls `readFrame`. Every write path acquires `writeMu` for the
entire frame (header + payload + trailer + flush). This mirrors the
server's single-writer invariant established in ADR 0008 and removes
any need for a write-side framing lock or per-call buffering.

The read loop is also the **sole closer of `deliveryCh`**, via a
`defer`. Closing it from `Close()` would race with the loop's send
into the same channel; making the loop own its lifecycle eliminates
the race without extra locks.

### Pending response FIFO for all four verbs

All four request-shaped verbs — PUB, SUB, ACK, NACK — block until the
broker answers. The server sends responses in strict request order
(see `internal/server/session.go`). A single FIFO of `pendingReq`
entries is therefore sufficient: each `Pub`/`Sub`/`Ack`/`Nack` push
an entry, take `writeMu`, write the frame, release, and wait on a
one-shot response channel. The read loop pops the head entry per OK,
DUP, or ERR frame.

Cancellation does **not** remove the entry from the queue; it marks
it `cancelled = true`. The next response for that slot pops and
discards it, and ordering after that is preserved naturally. This
avoids an O(n) scan on cancel and keeps the data structure a plain
slice.

ACK and NACK responses (`OK <msgID>`) are checked against the target
`msgID` and a mismatch returns `ErrServer`. This is defensive — under
correct broker behaviour the IDs always match — but cheap, and it
turns a future protocol mistake into a loud error instead of silent
drift.

ACK and NACK are reachable via two surfaces: `Client.Ack` /
`Client.Nack` for callers that already know the consumer ID and
msg-id (e.g. `cmd/toymqctl ack`), and `Delivery.Ack` / `Delivery.Nack`
closures for the streaming case. Both paths route through the same
`sendAckLike` helper and the same pending FIFO.

### Single subscription per Client

`Sub` claims `subActive`; a second `Sub` on the same Client returns
`ErrSubInUse`. Multi-consumer use is multiple Clients. This trades
flexibility for a simple invariant: there is exactly one
`consumerID` and one `deliveryCh` per Client, so the read loop's MSG
dispatcher needs no per-consumer demuxing.

The delivery channel is buffered (64 slots). A slow consumer applies
TCP backpressure — when the channel fills, the read loop blocks,
which stalls the broker's writer. That is the intended behaviour:
the broker's outbound buffer absorbs short stalls; sustained slowness
pauses delivery rather than spilling unbounded memory in the client.

### Caller-owned reconnect

Transport failures permanently close the Client and surface as
errors wrapping `ErrTransport`. There is no in-library reconnect, no
exponential backoff, no `Retrying(...)` wrapper. The caller is
expected to wrap `Dial` in its own loop. This matches the chaos
producer/consumer, which already manage their own backoff because
the soak test needs to count reconnects as a signal.

Adding a `client.Retrying` helper later — a thin wrapper that catches
`ErrTransport` and redials — is straightforward, but doing it
speculatively before any caller wants it would impose a reconnect
policy on every consumer. Defer until a real caller needs it.

### `Close` always returns nil; `Err()` exposes transport error

`Close()` returns `nil` on every call, matching `io.Closer`
implementations like `net.Conn` and `os.File`. To distinguish
"caller closed it" from "transport blew up," `Err() error` returns
the recorded transport error if any, else nil. Callers that need the
distinction (e.g. a chaos loop deciding whether to count a
disconnect as a flake) call `Err()` after they notice the channel
close or the next call returns `ErrClosed`.

### Sentinel errors

Four exported sentinels, all wrappable with `%w`:

- `ErrClosed` — Client closed (by caller or transport).
- `ErrTransport` — net-layer error; wraps the underlying err.
- `ErrServer` — broker returned `ERR <code> <reason>`; the formatted
  message carries the code and reason.
- `ErrSubInUse` — second `Sub` on a Client that already owns one.

Callers compose with `errors.Is`. `ErrClosed` and `ErrTransport`
overlap at the boundary (a transport blow-up closes the Client) —
the public contract is that **either** check is sufficient to decide
"connection unusable."

### Stdlib only

Zero third-party dependencies. The package compiles with `go build`
straight out of the box on a stock Go install. This is a load-bearing
constraint until Step 17 (TUI), which is the first binary expected to
add an external dep (Bubble Tea), and which will require ADR 0014
before it lands.

## Consequences

**Positive**
- One canonical client, ~600 LOC, replaces three drift-prone flavours.
- The chaos producer/consumer shed their hand-rolled framing and
  shrink by ~150 LOC each.
- New CLI/TUI surfaces can be built on a typed API instead of
  reaching for `fmt.Fprintf` and `bufio.Reader.ReadString`.
- The single-FIFO request/response model is simple enough to reason
  about for `-race` correctness: lock order is `pending.mu` → never
  held across writes; `writeMu` → never held across reads.
- Buffered delivery channel composes with `context.Context`
  cancellation in the canonical Go way (`select { case d := <-ch:
  case <-ctx.Done(): }`), so callers compose naturally with
  cancellation trees.

**Negative**
- `pkg/client` is now a public surface area that future protocol
  changes have to honour, or break callers explicitly. The lack of an
  external user today softens this — there are no SemVer obligations
  yet — but as soon as `cmd/toymqctl` ships, behavioural changes need
  thinking through.
- Single-sub-per-Client means a TUI that wants to subscribe to two
  topics simultaneously needs two Clients, two TCP connections. For
  v1 this is a non-issue; for a future high-fan-out consumer it would
  be wasteful.
- Caller-owned reconnect means every binary repeats backoff/jitter
  logic. The chaos suite already has it; toymqctl will probably skip
  it; the TUI will reinvent it. A `Retrying` helper should land once
  the second caller asks for one.

## Limitations of this design

These are properties the library does **not** provide. Documenting
them so future readers don't over-claim what `pkg/client` does.

- **No automatic reconnect.** A transport blow-up means subsequent
  calls return `ErrClosed`. The caller must dial a fresh Client.
- **No write-side flow control beyond TCP.** A burst of concurrent
  `Pub` calls buffers entirely in the kernel send buffer. There is no
  application-level back-pressure on the producer side.
- **No retry on `ErrServer`.** A `PUB_FAILED` or other server-side
  error is returned to the caller verbatim; the library does not
  treat it as transient.
- **No multi-subscription per Client.** Workaround: open multiple
  Clients.
- **No metrics, no tracing, no logger.** The Client is silent; if it
  closes due to a transport error, the caller learns via `Err()` or
  the next call's `ErrClosed`. Adding hooks would be additive but is
  not in v1.
- **Wire-protocol ID semantics are echoed back to the caller.** PUB
  returns `(MsgID, dup)`; ACK/NACK validate the echo against their
  target. Callers that want richer semantics (delivery attempts,
  redelivery counters) need to track them externally.

## Edge cases

- **`Close` during in-flight `Pub`.** `Close` triggers `closeOnce`,
  closes the conn, which unblocks `readLoop`'s `ReadString`. The
  loop drains the pending FIFO with a synthetic `ERR CLOSED` frame.
  The in-flight `Pub` selects on `<-c.done` first (it is checked
  before `<-p.resp`) and returns `ErrClosed`.
- **Context cancel during `Pub` wait.** The pending entry is marked
  `cancelled`. When the real response arrives, `deliver` skips the
  cancelled entry and the response is lost — but the next live entry
  in queue order is the one that should receive the next response,
  which it does. Correctness preserved.
- **Transport blow-up during `Sub`.** The read loop notices the EOF,
  records `readErr`, drains pending with `ERR TRANSPORT`, and closes
  the Client. The `Sub` caller's `select` either fires on `p.resp`
  (with the transport error) or on `c.done` (with `ErrClosed`),
  whichever wins. Both produce a hard error; `Sub` returns it and
  `rollbackSub` clears `subActive` so a fresh Client+Sub can be
  retried.
- **Slow consumer.** `deliveryCh` fills at 64 entries. The read loop
  blocks on its send. The broker's per-session writer eventually
  stalls on TCP send-buffer back-pressure. Producers on other
  Clients are unaffected.
- **`ACK`/`NACK` echo mismatch.** Defensive check: if the broker's
  `OK <id>` carries a different `MsgID` than the verb targeted,
  `sendAckLike` returns `ErrServer` rather than silently accepting.
  Under the current server this branch is dead code, but the cost is
  one integer compare.

## Usage

```go
ctx := context.Background()
c, err := client.Dial(ctx, "127.0.0.1:6789")
if err != nil { /* ... */ }
defer c.Close()

id, dup, err := c.Pub(ctx, "orders", "k1", []byte("hello"))
// ...

ch, err := c.Sub(ctx, "orders", "consumer-1")
for d := range ch {
	process(d.Payload)
	_ = d.Ack(ctx)
}
```

Reconnect (caller-owned) follows the pattern used in
`test/chaos/producer.go` and `test/chaos/consumer.go`: catch
`errors.Is(err, client.ErrTransport)` (or the closed channel from
`Sub`), redial, resume.

## Addendum (2026-06-09): `WithLogger` Option

The "no metrics, no tracing, no logger" line under *Limitations of
this design* originally meant the Client emitted nothing at all. As
of v1.2, the Client gains an opt-in logger via
`client.WithLogger(*slog.Logger)`.

**What changed:**

- A `logger *slog.Logger` field on the resolved `config`, set by
  the new `WithLogger` Option.
- A single `Client.log(level, msg, args...)` helper that no-ops when
  the logger is nil — keeps the nil-check in one place.
- State-change call sites: `Dial` → Debug "dialed", `Sub` → Debug
  "subscribed", `Close` → Debug "closed", `readLoop` transport
  failure → Warn "transport lost".

**What did not change:**

- **Silent by default.** A Client constructed without `WithLogger`
  emits nothing. Existing callers (`internal/integration`,
  `test/chaos`, `cmd/toymqctl`, `cmd/toymq-bench`, `cmd/toymq-tui`)
  continue to see zero log output unless they opt in.
- **No metrics or tracing.** Logger is the only hook added; metrics
  and OTel remain explicitly out of scope for v1.

The escape hatch matches what the original ADR called out as
"additive but not in v1": a single new Option, no signature changes
to `Dial`/`Pub`/`Sub`/`Ack`/`Nack`, and no behavioural change for
callers who don't reach for it.
