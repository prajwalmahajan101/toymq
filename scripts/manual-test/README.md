# toymq manual-test lab

A one-command **tmux lab** for manually exercising every ToyMQ feature shipped
from v1.0.0 through the v2 milestones (M1–M8) — across real terminal sessions,
including the interactive TUI that automated tests can't drive.

This is human-driven acceptance testing. The automated coverage lives in
`internal/integration/` and `test/chaos/`; this lab is for a hands-on pass
before cutting the `v2.0.0` tag.

## Prerequisites

- **tmux** and the **Go toolchain** (1.25+) — required.
- **Docker** — optional, only for the observability stack (step 11).
- `curl` and `nc` (netcat) — used by a few steps; optional.

## Run

```bash
./scripts/manual-test/tmux-lab.sh
```

It builds the four binaries, generates a self-signed cert + a token file under
`/tmp/toymq-lab` (override with `TOYMQ_LAB_DIR`), starts the broker with the full
v2 feature set, and drops you into a 5-pane tmux session:

```
+----------------------+----------------------+
| 0 broker             | 1 consumer           |
+----------------------+----------------------+
| 2 producer           | 3 TUI                |
+----------------------+----------------------+
| 4 bench + notes (full width)                |
+---------------------------------------------+
```

Then follow **[WALKTHROUGH.md](WALKTHROUGH.md)** step by step (also open in
pane 4 via `less`). Each step names the feature/milestone, the exact command,
which pane to run it in, and the expected output.

## Broker configuration

Pane 0 runs (`broker.sh`):

- auth (`--auth-token-file`) + TLS (`--tls-addr :6889`) alongside plain `:6789`
- `--default-partitions 4`, `--fsync batched`, `--recv-window 8`
- segmentation + retention, `--dlq-after-nacks 3`, metrics on `:6790`

Restart it (from any pane) with the `labbroker` helper:
`labbroker` (batched), `labbroker per-message`, or `labbroker none`.

## tmux basics

- **Detach** (leave everything running): `Ctrl-b` then `d`.
- **Re-attach**: re-run `./tmux-lab.sh` (it attaches instead of rebuilding).
- **Switch panes**: `Ctrl-b` then an arrow key.
- **Scroll a pane**: `Ctrl-b` then `[`, arrows/PageUp, `q` to exit scroll.

## Tear down

```bash
./scripts/manual-test/teardown.sh            # kill tmux session + broker
./scripts/manual-test/teardown.sh --docker   # + observability compose stack
./scripts/manual-test/teardown.sh --purge    # + delete the lab dir
```

## Files

| File | Role |
|---|---|
| `tmux-lab.sh` | Build binaries, gen cert/token, spawn the tmux panes. |
| `env.sh` | Shared paths/ports/creds sourced by every pane. |
| `broker.sh` | Launch the broker with the full v2 feature set. |
| `gen-cert.go` | Throwaway self-signed cert generator (`go run`, not part of the build). |
| `WALKTHROUGH.md` | The guided feature-by-feature manual test. |
| `teardown.sh` | Stop and optionally purge the lab. |
