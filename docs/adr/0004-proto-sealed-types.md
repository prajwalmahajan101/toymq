# 0004 — Sealed `Command` interface for the wire protocol

**Status:** Accepted
**Date:** 2026-06-08
**Scope:** `internal/proto/command.go`

## Context

The wire protocol has a small fixed set of commands (`PUB`, `SUB`, `ACK`,
`NACK`) and responses (`OK`, `MSG`, `ERR`, `DUP`). Downstream code — the
session loop, the broker — needs to dispatch on which variant it's
holding. We need a Go-idiomatic shape that gives us:

- exhaustiveness in spirit (no surprise new variants from outside the
  package),
- a single dispatch path callers can type-switch on,
- room for typed per-variant fields without losing static checks.

Go has no native sum types or enums-with-data, so we choose between:

1. An interface with concrete struct types.
2. A single tagged struct (`type Command struct { Kind, Topic, Payload, ... }`)
   where most fields are unused per variant.
3. A `any` payload plus a string tag.

## Decision

Use a **sealed interface**:

```go
type Command interface {
    isCommand()
}

type PubCommand  struct { ... }
type SubCommand  struct { ... }
type AckCommand  struct { ... }
type NackCommand struct { ... }

func (PubCommand) isCommand()  {}
func (SubCommand) isCommand()  {}
func (AckCommand) isCommand()  {}
func (NackCommand) isCommand() {}
```

The `isCommand()` marker method is **unexported**, so only types in the
`proto` package can ever satisfy `Command`. Adding a new variant
requires editing `command.go` — exactly where the parser and the
session-loop dispatch live.

Responses (`OK`/`MSG`/`ERR`/`DUP`) intentionally do *not* share an
interface. They are emitted by independent `Write*` functions because
the session loop always knows which response it wants to send; it never
needs to "hold a Response and dispatch later." If that changes we can
introduce a sealed `Response` interface in a follow-up ADR.

## Consequences

**Positive**
- Type switches in callers are total: `switch c := cmd.(type) { case
  PubCommand: ... }` covers every variant the parser can return.
- Each variant gets exactly the fields it needs (e.g. PUB has Payload,
  ACK does not).
- The compiler catches typos — `PubCommnd` is a build error, not a
  silent runtime mismatch like a string tag.

**Negative**
- Slight ceremony per variant (a marker method) and a small allocation
  per parsed command (interface boxing). Both are negligible at the
  scale we operate.
- No exhaustiveness check from the compiler (Go does not flag a missing
  `case`). We pay attention via tests and code review.

## Usage

- Parsers return `Command` (the interface) plus an error.
- Callers type-switch on the concrete struct types in the same package.
- New variants land via PRs that touch `command.go` and update every
  type-switch by hand.
