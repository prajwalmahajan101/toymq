# 0009 — Binary entry point: testable `run`, stdlib `flag`, and a config package

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `cmd/toymq/main.go`, `internal/config/`

## Context

ToyMQ needs a runnable binary that wires the broker and the server
together, parses runtime configuration, configures logging, and
handles signal-driven shutdown. Several conventions about *how* this
is structured affect every subsequent operational and testing decision:

- Where does configuration live and get validated?
- Which flag library handles parsing?
- Can `main`'s behavior be unit-tested?
- How is logger setup propagated to library code?
- How does graceful shutdown integrate with signal handling?

A naive `main()` that does everything inline works, but it's
untestable, hard to reuse, and grows unbounded as flags accumulate.

## Decision

### Dedicated `internal/config` package

`Config` is a struct in `internal/config`, populated by
`Parse(args []string, stderr io.Writer) (*Config, error)`. Parsing
constructs a `flag.NewFlagSet(..., flag.ContinueOnError)`, binds six
flags, parses `args`, and runs a separate `validate()` method.

Defaults live as exported constants (`DefaultAddr`,
`DefaultDataDir`, etc.) so tests assert against them and future
`cmd/toymqctl` / `cmd/toymq-bench` tools can share them.

Validation rules:

- `addr`, `data-dir` non-empty.
- `log-level` ∈ {`debug`, `info`, `warn`, `error`}.
- `log-format` ∈ {`text`, `json`}.
- `shutdown-timeout` > 0.
- `dedupe-cap` > 0.

### Stdlib `flag`

No third-party flag library. POSIX-ish single-dash syntax. The six
flags fit comfortably; no subcommands are planned. If subcommands
ever appear, `flag.NewFlagSet` per subcommand is the migration
bridge.

### Testable `run` helper

`main()` is five lines: build a `signal.NotifyContext`, call
`run(ctx, os.Args[1:], os.Stdout, os.Stderr)`, exit with the
returned error. `run` takes all its IO and arguments as parameters
so tests can drive it with synthetic args and discarded writers.

This isolates `os.Exit`, `os.Args`, `os.Stdout`, and `os.Stderr` to
`main()` alone. Every other startup path — parse, validate, broker
open, server lifecycle, signal-driven shutdown — runs inside `run`
and is unit-testable.

### Logger format selectable, default text

Two handler choices: `slog.NewTextHandler` (default) and
`slog.NewJSONHandler` (opt-in via `-log-format=json`). Level is
controlled by `-log-level`. `slog.SetDefault` is called inside
`run` so library code (`broker.flushDirty`, `server.Serve`) uses
the configured logger without taking it as a parameter.

### Shutdown sequencing

`run` spawns `srv.Serve(ctx)` on a goroutine that pushes its return
value onto a buffered `serveErr` channel. The main path selects
between `serveErr` and `ctx.Done()`:

- If `Serve` returns first (bind failure, fatal Accept error), the
  error propagates out.
- If `ctx.Done()` fires first (signal arrived), log it, derive a
  fresh `context.WithTimeout(background, cfg.ShutdownTimeout)`,
  and call `srv.Shutdown(shutCtx)`. Then `<-serveErr` collects
  Serve's return value (now `nil`, because the listener was closed).

Broker shutdown is `defer b.Close()` — runs on any return path.
Errors from Close are logged, not propagated, because the function
is already returning.

### `flag.ErrHelp` is a clean exit

`config.Parse` returns `flag.ErrHelp` when the user passed `-h` or
`-help`. `run` detects this with `errors.Is(err, flag.ErrHelp)` and
returns `nil`. `main` exits with status 0, matching standard CLI
convention.

## Consequences

**Positive**
- `main()` is irreducibly small. All logic is in `run`, which is
  exercised by `cmd/toymq/main_test.go` (help, invalid flag,
  start+stop with goroutine accounting).
- Config validation lives in one place, with one test file. Adding
  a flag is: declare in `Parse`, validate, default constant, test.
- Logger configuration cascades to library code via
  `slog.SetDefault` — no need to thread `*slog.Logger` through every
  constructor.
- Shutdown sequencing is deterministic and matches the build-guide
  verify ("Broker should exit within Shutdown deadline (5s)"). The
  drain budget is a single named knob.
- Stdlib-only: ToyMQ still has zero third-party runtime dependencies.

**Negative**
- Six flags is the upper end of what `flag` reads cleanly. The
  seventh flag is the moment to evaluate `pflag` or `cobra`.
- The "exported defaults as constants" pattern duplicates the
  values across `Parse` and the test. Acceptable; the tests catch
  drift.
- `slog.SetDefault` mutates global state inside `run`. Tests that
  parallelize `run` would race on the default logger; we don't
  parallelize, but it's a constraint worth knowing.
- `run`'s `serveErr` channel and outer `select` are a small piece
  of choreography. Easy to misread on first encounter. The comment
  in code is the primary defense.

## Edge cases

- **Bind failure.** `Serve` returns the wrapped `listen` error
  immediately. `run` receives it via `<-serveErr` in the first
  select arm and returns. No shutdown work needed; listener never
  bound.
- **Signal received before bind completes.** `ctx.Done()` fires;
  the shutdown path runs. `srv.Shutdown` closes the (already-bound
  or in-flight) listener; `<-serveErr` collects either a `nil` or
  a bind-failure error. Both paths exit cleanly.
- **`-h` / `-help`.** `flag.ErrHelp` propagates through `config.Parse`;
  `run` returns nil; `main` exits 0. The flag package writes the
  usage text to the configured stderr.
- **Unknown flag.** `flag.ContinueOnError` returns the error to
  `Parse`. `run` returns it wrapped in `"config: ..."`. `main`
  prints and exits 1.
- **`flag.ErrHelp` while validation would fail.** The flag package
  short-circuits on `-h` before our `validate()` runs, so help text
  is shown regardless of other invalid values.

## Usage

- Production: `toymq -addr :6789 -data-dir /var/lib/toymq
  -log-format json`. SIGINT or SIGTERM triggers graceful shutdown.
- Local dev: `go run ./cmd/toymq` uses all defaults; logs to stdout
  in text format.
- Tests: `run(ctx, args, &out, &errOut)` drives the binary in-process.
  Use `-addr 127.0.0.1:0` and `t.TempDir()` for `-data-dir` to keep
  tests isolated and parallel-safe (at the test-binary level, not
  within `run`).
- New flag: declare in `config.Parse`, add a validation rule, add a
  default constant, write the assertion in `TestParseDefaults` and
  `TestParseOverrides`, propagate through `run` to whoever needs it.
