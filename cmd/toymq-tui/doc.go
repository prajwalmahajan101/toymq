// Command toymq-tui is an interactive terminal client for the ToyMQ
// broker. It connects over TCP via pkg/client and exposes PUB / SUB /
// ACK / NACK through modal forms and key bindings.
//
// Architecture follows the Bubble Tea Elm pattern: the model in
// model.go is mutated only by Update in update.go, and View in view.go
// reads (never mutates) model state. All blocking I/O against
// pkg/client is wrapped in tea.Cmd factories (commands.go) so the
// redraw loop never blocks. Subscription deliveries are pumped by a
// recursive tea.Cmd (subscription.go) — when one delivery arrives, the
// Cmd returns the next read-delivery Cmd, keeping at most one pump
// goroutine alive at a time.
//
// See docs/adr/0014-tui-framework-choice.md for the framework
// rationale (Bubble Tea over tview / raw tcell) and the dependency
// boundary (Charm modules confined to this directory).
package main
