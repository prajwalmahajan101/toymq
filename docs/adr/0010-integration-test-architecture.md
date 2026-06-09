# 0010 — Integration test architecture

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `internal/integration/`, `internal/broker/broker.go`

## Context

ToyMQ has unit tests in every package (>90% line coverage), but every
test runs in-package against `net.Pipe` or in-process broker handles.
Nothing proves that broker + server + WAL + protocol assemble into a
working system when dialed over real TCP. End-to-end tests close that
gap and become the regression net for future protocol or broker
changes.

Four design choices shape the suite:

1. Where the tests live (which constrains how they reach test seams).
2. Whether the system-under-test is the binary (subprocess) or the
   in-process broker+server.
3. How the redelivery sweep interval is shortened from the 30 s
   production default to something a test can tolerate.
4. What "redelivery after a mid-MSG disconnect" actually means in the
   current code, so tests assert the contract instead of guessing.

## Decision

### Location: `internal/integration/`

Tests live in `internal/integration/`, a non-importable test-only
package. The package imports `internal/broker` and `internal/server`
directly so it can construct the system under test in-process.

The build-guide originally suggested `test/` at the repo root, which
would force the suite to use only the TCP surface. That framing is
attractive — it makes the tests look like an external client — but it
trades a real production constraint (the broker only exposes a 30 s
visibility timeout) for one of two costs: either add a
`-visibility-timeout` flag to the binary purely so tests can shorten
it, or accept 30+ s per redelivery test. Neither is worth the framing
benefit. Putting the suite under `internal/` keeps the
"client-shaped" feel (every test dials TCP; nothing calls broker
methods directly) without forcing a production-API surface to
accommodate test ergonomics.

### System-under-test: in-process

Each test spins up `broker.NewWithTimings(...)` + `server.New(addr,
broker)` + `Serve(ctx)` on a goroutine in the test process, binding
on `127.0.0.1:0`. The test then dials the bound address and exercises
the protocol.

Subprocess testing (`go run ./cmd/toymq`) was rejected because:

- It is slow (compile + boot per test).
- It is brittle to flag parsing and binary path resolution.
- The binary wiring is already covered by
  `cmd/toymq/main_test.go:TestRunStartsAndShutsDown`, which spawns
  `run`, signals it, and confirms exit + goroutine baseline. Adding a
  subprocess scenario at the integration layer would re-test the same
  cmd-wiring code with worse ergonomics.

### Visibility-timeout seam: promote `newBroker` to `NewWithTimings`

The previously-unexported `newBroker(dataDir, dedupeCap, visibility,
redeliverInterval)` was the broker package's test seam. Integration
tests live in a *different* package and cannot reach unexported
identifiers, so the seam is renamed and exported as `NewWithTimings`,
documented for explicit-timing construction. `broker.New` continues to
delegate to it with the 30 s / 1 s production defaults.

This narrows the surface change to one rename. It also accidentally
becomes useful production API: operators who eventually want a tuned
visibility timeout can call `NewWithTimings` directly without waiting
for a flag.

### Scenario 8 — close-mid-MSG redelivery semantics

The build-guide spec for scenario 8 is "Close conn mid-MSG; no ACK;
reopen; receive the message again." Walking through the current code:

1. Server writes `MSG` (header + payload, one `Write+Flush`).
2. Client closes the conn after the header is buffered by the kernel
   but before reading the payload.
3. Server's `runReader.ReadCommand` gets `io.EOF`. `Session.Run`
   cancels the current subscription (`currentCancel()`), waits for
   the writer, closes the conn. The inflight stays in
   `c.inflight` because no ACK arrived.
4. The redelivery sweep ticks. It iterates consumers, finds the
   expired inflight, but the consumer's subscription is now `nil`
   (the cancel above tore it down). The sweep **skips** consumers
   with `sub == nil`. `Attempts` is *not* bumped.
5. Client reconnects. New `SUB <topic> <consumerID>` causes
   `runDelivery` to open a WAL reader at `lastAcked+1`, sees the
   un-acked record, sends an `Inflight` with `Attempts: 1`. Message
   is re-delivered.

So the asserted contract is: **after close-mid-MSG + reconnect, the
client receives the same MsgID with `Attempts == 1` (not 2)**. The
visibility timeout is irrelevant in this path — the redelivery is
driven by the fresh SUB's WAL tail, not by the sweep.

If we later wanted `Attempts >= 2` we'd have to change the sweep to
queue redeliveries for the next subscriber (a per-consumer pending
list) rather than skip-when-`sub==nil`. That is a behaviour change,
not a test issue, and it is not in scope for Step 13.

### Branch hygiene

The current `feat/integration-tests` branch already carries three
coverage-push commits. Either:

- Rename to `feat/coverage-push`, merge, cut a fresh
  `feat/integration-tests` off main (clean history).
- Stack integration commits on top (one bigger PR; branch name
  misleading until merge).

The plan picks the rename for honest history, but the choice doesn't
affect any code; it's mentioned here only because reviewers reading
the PR history later may wonder why coverage-push commits sit on a
branch named "integration-tests".

## Consequences

**Positive**
- Tests run in <2 s end-to-end thanks to in-process binding and
  short timing knobs.
- No production-only flag exists purely to accommodate tests.
- `NewWithTimings` is a documented test seam and a real operator
  knob.
- The Scenario 8 contract is explicit, so future changes that alter
  Attempts semantics will fail the test loudly.

**Negative**
- The suite is not literally a separate-process client. A bug that
  only manifests when the binary is launched via the real entry
  point (e.g. argv parsing, logger setup, signal handling) is not
  caught here — it is caught by `cmd/toymq/main_test.go`. Two layers
  by design.
- `NewWithTimings` widens the broker public API. The cost is small;
  the name advertises its purpose; the production constructor
  remains the default.
- Helpers in `internal/integration/` duplicate some
  bytes-on-the-wire knowledge (`MSG topic id len\n`, etc.) for
  readability. The risk of drift is bounded by the protocol's
  stability — the verbs and separators have been frozen since ADR
  0004.

## Edge cases

- **Listener race on Serve startup.** The harness polls `srv.Addr()`
  until non-empty before letting the test dial. Without this, the
  test races against `Serve`'s `Listen`+`mu.Lock`+assignment, and
  the dial fails intermittently.
- **OK / MSG interleaving on the wire.** A SUB causes the server to
  emit an `OK 0` *and* potentially a stream of `MSG ...` lines if
  there is backlog. The client helper buffers any `MSG` lines that
  arrive while the test was waiting for an `OK`, so test assertions
  can be written in their intent order. Same pattern as
  `internal/server/session_test.go:TestSessionSubMsgNack`.
- **Restart preserves data dir.** The `restart(t)` helper shuts the
  server, closes the broker, then re-opens both on the *same*
  `dataDir`. The harness does not delete the temp dir between
  restarts; `t.TempDir()` cleans up at the end of the test.
- **Goroutine leak detection.** One harness-level test wraps
  `startBroker` + `restart` + shutdown in a `runtime.NumGoroutine()`
  baseline check, mirroring `cmd/toymq/main_test.go`. Future
  scenarios get leak coverage for free by reusing the harness.

## Usage

- Start a broker for a scenario:
  ```go
  h := startBroker(t)                 // defaults: 100ms vis, 20ms tick
  c := dial(t, h.addr)
  defer c.close()
  c.pub("orders", "", []byte("hello"))
  id := c.expectOK(t)
  ```
- Restart across the same data dir:
  ```go
  h.restart(t)
  c2 := dial(t, h.addr)
  c2.sub("orders", "consumer-1")
  c2.expectOK(t)
  ```
- Tune timings for a redelivery test:
  ```go
  h := startBroker(t, withVisibility(50*time.Millisecond))
  ```
- Run the suite:
  ```
  go test -race ./internal/integration/...
  ```
