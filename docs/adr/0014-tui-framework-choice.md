# 0014 — TUI framework choice: Bubble Tea

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `cmd/toymq-tui/` and `go.mod`

## Context

Step 17 of the build guide is an interactive TUI client that lets a
human exercise the broker — `PUB / SUB / ACK / NACK`, watch deliveries
arrive on a live subscription, toggle auto-ack, and nack the last
message — without running two `toymqctl` terminals or hand-rolling
protocol frames. It sits on top of `pkg/client` (ADR 0013) and is the
last item on the v1 build guide.

The TUI has four non-negotiables:

1. **Modal input.** Pub and Sub each open a form with two or three
   text fields. Esc cancels, Enter submits.
2. **Async event injection.** A subscription goroutine reads
   `<-chan client.Delivery` and the UI must redraw when a new message
   lands, without the input loop polling.
3. **A scrollback pane** of recent events that survives across
   pub/sub operations (last ~1000 lines).
4. **Tests on state transitions** without a real terminal. The pure
   `Update` function should be drivable from `*testing.T`.

Up through v1.0.0, `go.mod` declares **zero third-party runtime
dependencies**. ADR 0013 calls this out as a load-bearing property for
the broker and client, and it remains so — every package outside
`cmd/toymq-tui/` must keep its stdlib-only stance. The TUI is the
first place where the cost of staying stdlib-only outweighs the
benefit.

## Decision

### Bubble Tea + Lipgloss + Bubbles

The TUI uses the Charm stack:

- `github.com/charmbracelet/bubbletea` — the Elm-pattern runtime
  (`tea.Model`, `Update`, `View`, `tea.Cmd`).
- `github.com/charmbracelet/lipgloss` — declarative styling for the
  three-pane layout (header, scrollback, footer + modal overlay).
- `github.com/charmbracelet/bubbles/textinput` — the modal text
  fields.

### Why not stdlib + raw escape codes

Possible but expensive. We'd reimplement:

- TTY raw mode, alternate screen buffer, cursor save/restore.
- Key parsing for the escape-sequence dialect (Esc vs Alt+key
  ambiguity, function keys, paste bracketing).
- A redraw scheduler that batches input + async events without
  tearing.
- A focus/modal stack.

That is a second project. ToyMQ's whole point is the message broker;
spending a weekend on a hand-rolled terminal framework for the demo
client is a misallocation.

### Why not tview

`tview` is mature and stdlib-shaped (callback-driven), but it does
**not** separate state from render. Tests on the UI logic have to
spin up its `tview.Application` and assert against widget state by
reference. The Elm pattern Bubble Tea enforces — a pure
`Update(msg) (Model, Cmd)` — is directly testable: feed synthetic
`tea.KeyMsg` and `msgArrivedMsg` values, assert the next Model. This
matches the "drive the public surface from `*testing.T`" pattern used
across the rest of the repo (cf. `internal/integration/harness.go`
and `pkg/client/client_test.go`).

### Why not raw `tcell`

`tcell` is the layer Bubble Tea and tview both build on. Going to
`tcell` directly avoids one transitive dep but reintroduces every
batteries-included concern from the "Why not stdlib" point — focus
stacks, modal overlays, text inputs. Bubble Tea pays for itself the
moment you need a `textinput` widget.

### Dependency boundary

The Charm dependencies are confined to `cmd/toymq-tui/`. They never
appear in `go.mod` as a dependency of anything under `internal/` or
`pkg/`. To make this enforceable rather than aspirational:

- `pkg/client` and every `internal/` package continue to build with
  `go build ./pkg/... ./internal/...` against a stdlib-only Go.
- The TUI imports `pkg/client` (and `internal/config` for the addr
  default, like the other `cmd/` binaries), but `pkg/client` does not
  import `cmd/toymq-tui/` — that direction would be a cycle in any
  case.
- Future TUI screens or sub-models live under `cmd/toymq-tui/`, not
  in a shared `internal/tui/` package. If a second TUI binary ever
  appears, *then* we extract — but not preemptively.

### Async client calls go through `tea.Cmd`, not direct method calls

Inside `Update`, no method on `*pkg/client.Client` is called
synchronously. Every PUB / SUB / ACK / NACK is wrapped in a `tea.Cmd`
that runs the blocking call on a background goroutine and returns a
result message (`pubResultMsg`, `subStartedMsg`, `ackResultMsg`,
etc.). The redraw loop stays responsive even if the broker is slow or
the conn is wedged. This mirrors Bubble Tea's documented pattern for
I/O and is the single hardest discipline to keep right — `Update`
must remain pure.

### Subscription pump

The subscription is a long-lived goroutine, not a `tea.Cmd`. It owns
the `<-chan client.Delivery` returned by `Client.Sub` and forwards
each delivery into the Bubble Tea program via `program.Send(...)`.
When the channel closes (transport blow-up or caller `Close`), the
pump sends a `transportLostMsg` and exits. The TUI renders
"disconnected, press q to quit" rather than freezing.

### No in-app reconnect (v1)

`pkg/client` is caller-owned-reconnect (ADR 0013). The TUI inherits
that: on `transportLostMsg`, render the disconnected banner and
require the user to relaunch. Auto-reconnect would need a fresh
`Client` and a re-Subscribe with the previous consumer ID; the
state-management cost (which messages were in flight? what's the
new `lastAcked`?) is not worth it for a demo client. If we add it
later, an addendum ADR records the policy.

## Consequences

**Positive**
- A working TUI in a few hundred lines, with testable `Update`
  transitions and a recognisable Elm-pattern shape that future
  contributors can navigate.
- Modal pub/sub forms come for free via `bubbles/textinput`.
- The "TUI as the demo client" loop in the README quickstart finally
  has a single binary to point at instead of two `toymqctl` shells.

**Negative**
- `go.mod` grows a transitive dependency tree (~10 modules under the
  Charm umbrella: `bubbletea`, `lipgloss`, `bubbles`,
  `termenv`, `lucasb-eyer/go-colorful`, `mattn/go-runewidth`,
  `muesli/cancelreader`, etc.). `go mod vendor` is now non-trivial
  and `Dockerfile` builds will need to fetch them.
- The "zero deps" line in the README has to be qualified: the
  broker, the client library, and the non-TUI `cmd/` binaries
  remain stdlib-only; only `cmd/toymq-tui` pulls externals. README
  text needs adjusting.
- Bubble Tea's Elm pattern is unfamiliar to contributors coming from
  callback-style frameworks. The trade is testability; the cost is
  one paragraph of onboarding in the TUI's package doc.
- Bubble Tea pre-1.0 has had API churn. We pin a specific minor
  version in `go.mod` and accept that an upgrade is its own commit
  with its own diff to review.

## Alternatives considered

| Option            | Verdict | Why                                                        |
| ----------------- | ------- | ---------------------------------------------------------- |
| stdlib + raw VT   | No      | Re-implements a TUI framework; not the project's purpose.  |
| `tview`           | No      | Callback model resists pure-function `Update` tests.       |
| raw `tcell`       | No      | Same widget shortfall as stdlib; no real saving over Charm.|
| **Bubble Tea**    | **Yes** | Elm pattern → testable; widgets + styling included.        |
| `gocui`           | No      | Limited maintenance; not a step up over `tview`.           |
| Web UI in browser | No      | Out of scope; project is "terminal toy broker."            |

## Usage pattern (for future TUI screens)

When adding a new screen or sub-model:

1. Add a new `state` enum value in `model.go` (e.g.
   `stateRetainModal`).
2. Add the `tea.Model` for the new sub-model in its own file
   (`retain_modal.go`).
3. Route key events to the sub-model from the parent's `Update`
   while the state is active.
4. Any client call goes through a new `tea.Cmd` factory in
   `commands.go`; results come back as a new typed message handled
   in `update.go`.
5. Add a `model_test.go` case feeding the relevant `tea.Msg` and
   asserting the next `Model` state.

`cmd/toymq-tui/` is the place. Do not move shared TUI helpers into
`internal/tui/` until a second TUI binary actually exists.

## Limitations of this design

- **Pre-1.0 framework.** Bubble Tea has not committed to SemVer. Pin
  the minor version and treat upgrades as audited.
- **Real-TTY-required tests.** Visual / golden-frame tests are not in
  v1. We test `Update` (pure) and skip `View`. The render layer is
  validated by manual smoke and the Docker e2e in the build plan.
- **Dependency budget.** Adding more Charm modules
  (`bubbles/list`, `bubbles/viewport`, etc.) is fine inside
  `cmd/toymq-tui/`; pulling unrelated TUI libraries is not. Stay
  inside the Charm stack to keep the transitive set manageable.
- **No reconnect** as decided above; documented here so future readers
  do not assume it was an oversight.
