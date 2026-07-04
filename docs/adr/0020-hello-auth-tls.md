# 0020 — HELLO handshake, bearer-token AUTH, and TLS

**Status:** Accepted
**Date:** 2026-07-05
**Scope:** `internal/proto/`, `internal/server/`, `internal/config/`,
`pkg/client/`, `cmd/*`
**Wire impact:** **breaking** — the first line on every connection is now a
`HELLO` frame (gated behind the v2.0 major bump), with a `--require-hello=false`
migration escape hatch.

## Context

Through v1 the wire protocol had no handshake: a connection's first line was a
`PUB`/`SUB`/`ACK`/`NACK` verb (ADR 0004/0008). There was no authentication and
no transport encryption, so the broker was effectively localhost-only — the
last thing pinning toymq to a single trusted host.

v2 M3 adds a mandatory `HELLO <version> [AUTH <token>]` handshake, bearer-token
auth, and TLS, lifting the off-host deployment ceiling. It is the one
deliberate wire break in v2, and it must precede partitions (v2 M4) so a
multi-partition broker has a safe way to be reached off-host.

## Decision

### Wire

```
C→S: HELLO 1 [AUTH <token>]\n      (first line; mandatory when --require-hello)
S→C: HELLO 1 OK\n                  (negotiated version = min(clientMax, serverMax))
S→C: ERR HELLO <reason>\n | ERR AUTH <reason>\n   then the server closes
```

- **HELLO is a handshake phase, not a `Command`.** `ReadCommand` was split into
  `ParseCommandLine(line, br, maxPayload)` + a line read, so the session peeks
  the first line for HELLO and, in compat mode, still processes a non-HELLO
  first line as a command without re-reading it. HELLO is deliberately kept out
  of the sealed `Command` union.
- **Version negotiation.** The client sends its max supported version (`1`);
  the server replies `HELLO <min(clientMax, serverMax)> OK`. Today that is
  always `1`. This is the seam for `HELLO 2` (v3 M7 push frames) — a future
  feature opts in by version, with no further wire break.
- New `ERR` codes: `HELLO` (missing/malformed handshake or unsupported
  version) and `AUTH` (missing/invalid token).

### Handshake is synchronous (no dropped-response race)

The session performs the handshake **inline before starting the async writer
goroutine**, writing the `HELLO … OK` / `ERR …` response directly to the
connection's buffered writer. An earlier design routed the response through the
session's `respCh` and hit a real race: on rejection, `close(quit)` could win
the writer's `select` over the queued `ERR`, dropping it (~40% under load) and
handing the client an EOF instead of a reason. Doing the handshake before the
writer exists removes the race by construction. The client mirrors this: `Dial`
writes `HELLO` and reads the one-line response synchronously **before**
starting `readLoop` (which then owns the reader).

### `--require-hello` migration toggle

Required by default. `--require-hello=false` opens a plaintext migration
window: a non-HELLO first line is processed as a normal command, so
un-migrated raw-line clients keep working while callers upgrade. A migrated raw
client only needs to prepend one `HELLO 1` line.

### AUTH — bearer tokens, constant-time, file-sourced

`--auth-token-file` holds one token per line (blank lines and `#` comments
skipped), loaded once at startup. A HELLO's `AUTH <token>` is matched with
`crypto/subtle.ConstantTimeCompare` against every configured token with no
early exit, so neither a match nor its position leaks through timing. Tokens
are never logged and never live in code. Auth is off when no file is given.

### TLS — side-by-side listeners

`--tls-addr` runs a TLS listener (via `tls.NewListener`, `MinVersion` TLS 1.2)
**alongside** the plain `--addr` listener; both are separate `server.Server`
instances sharing one broker, each on its own `Serve` goroutine, both drained
on shutdown. This lets operators migrate clients to the TLS port one at a time
rather than a hard cutover. `--tls-cert`/`--tls-key` are both-or-neither;
`--tls-addr` requires them.

### Client surface

`pkg/client` gains `WithAuth(token)` and `WithTLS(*tls.Config)` options
(additive to `Dial`, per ADR 0013), a `TLSConfig(caFile, insecure)` helper for
CLIs, and sentinels `ErrHandshake` (wraps `ErrAuth` on an AUTH rejection). The
three CLI consumers (`toymqctl`, `toymq-bench`, `toymq-tui`) expose
`--auth-token`, `--tls`, `--tls-ca`, `--tls-insecure`.

## Consequences

**Positive**
- The broker can be exposed off-host with auth + TLS.
- Version negotiation makes future wire features (HELLO 2) additive.
- The `--require-hello` toggle turns a hard wire break into a staged migration.
- The zero-value server policy (no hello, no auth) equals pre-M3 behaviour, so
  the change is opt-in at the server too.

**Negative / trade-offs**
- A genuine wire break: every client must speak HELLO once `--require-hello` is
  on. Mitigated by the toggle and the one-line prepend recipe.
- Bearer tokens are coarse (no per-topic ACLs yet) and static (no hot-reload).
- Client-plane only — see below.

## Client-plane vs peer-plane (v3)

M3's AUTH/TLS secure the **client plane** (producer/consumer connections). The
v3 **peer plane** (Raft inter-node traffic) is a separate concern with its own
TLS/auth, already tracked as a v3 prerequisite in the roadmap. Nobody should
assume M3 secured replication.

## Non-goals (hooks noted)
- **Per-topic ACLs** — the optional v2 M3.5 follow-up; the token set is the hook.
- **mTLS / client-cert auth** — bearer token only for v2.
- **Token hot-reload** — loaded once at startup.

## Edge cases
- **Malformed HELLO** (bad arity / non-numeric / zero version) → `ERR HELLO`.
- **HELLO with a higher version** → downgraded to the server max, `HELLO 1 OK`.
- **Auth enabled, no token in HELLO** → `ERR AUTH` (same path as a bad token).
- **Compat mode, first line is a valid HELLO** → still handled as a handshake.
- **EOF before handshake** → connection closed, no response.

## Usage

```
# secured broker: plain + TLS side by side, token auth
toymq --addr :6789 --tls-addr :6790 --tls-cert cert.pem --tls-key key.pem \
      --auth-token-file tokens.txt

# client
toymqctl --addr host:6790 --tls --tls-ca cert.pem --auth-token "$TOKEN" \
         pub orders "hello"

# raw line-oriented script (migration): prepend one HELLO line
printf 'HELLO 1\nPUB orders - 5\nhello\n' | nc host 6789

# migration window (accept un-migrated plaintext clients)
toymq --require-hello=false ...
```
