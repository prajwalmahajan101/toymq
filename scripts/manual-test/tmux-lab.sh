#!/usr/bin/env bash
# tmux-lab.sh — one-command manual-test lab for toymq (v1 -> v2 features).
#
# Builds the four binaries, generates a self-signed cert + token file, then
# spawns a tmux session with five pre-wired panes:
#
#   +----------------------+----------------------+
#   | 0 broker             | 1 consumer           |
#   +----------------------+----------------------+
#   | 2 producer           | 3 TUI                |
#   +----------------------+----------------------+
#   | 4 bench + notes (full width)                |
#   +---------------------------------------------+
#
# Re-running attaches to the existing session instead of rebuilding.
# Follow scripts/manual-test/WALKTHROUGH.md step by step. Tear down with
# scripts/manual-test/teardown.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TOYMQ_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

command -v tmux >/dev/null || { echo "tmux-lab: tmux is required (install tmux)." >&2; exit 1; }
command -v go   >/dev/null || { echo "tmux-lab: Go toolchain is required." >&2; exit 1; }

# Already running? Attach and stop.
if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
  echo "tmux-lab: session '$TMUX_SESSION' already exists — attaching. (teardown.sh to reset)"
  exec tmux attach -t "$TMUX_SESSION"
fi

mkdir -p "$BIN" "$DATA"

echo "==> building binaries into $BIN"
( cd "$TOYMQ_REPO" && for c in toymq toymqctl toymq-bench toymq-tui; do
    echo "    go build $c"
    go build -o "$BIN/$c" "./cmd/$c"
  done )

if [[ ! -f "$CERT" || ! -f "$KEY" ]]; then
  echo "==> generating self-signed cert"
  ( cd "$TOYMQ_REPO" && go run "$SCRIPT_DIR/gen-cert.go" "$CERT" "$KEY" )
fi

if [[ ! -f "$TOKENS" ]]; then
  echo "==> writing token file $TOKENS"
  printf '# toymq lab bearer tokens (one per line; blank + # lines ignored)\n%s\n' "$TOKEN" > "$TOKENS"
fi

echo "==> spawning tmux session '$TMUX_SESSION'"

# One full pane, then carve the layout. Splitting the full-width pane 0
# vertically first makes the bottom strip span the whole window.
tmux new-session -d -s "$TMUX_SESSION" -x 230 -y 56
BROKER=$(tmux display-message -p -t "$TMUX_SESSION" '#{pane_id}')

tmux split-window -v -l 26% -t "$BROKER"          # full-width bottom = notes
NOTES=$(tmux display-message -p '#{pane_id}')

tmux split-window -h -t "$BROKER"                  # top-right = consumer
CONSUMER=$(tmux display-message -p '#{pane_id}')

tmux split-window -v -t "$BROKER"                  # below broker = producer
PRODUCER=$(tmux display-message -p '#{pane_id}')

tmux split-window -v -t "$CONSUMER"               # below consumer = TUI
TUI=$(tmux display-message -p '#{pane_id}')

# Title each pane and source the shared env (with repo + lab dir) in its shell.
# The pane shell may be zsh, so we export the two vars env.sh needs, then source
# it — env.sh is written to be bash/zsh-safe and defines the mq/mqs helpers.
PANE_INIT="export TOYMQ_REPO='$TOYMQ_REPO' TOYMQ_LAB_DIR='$LAB_DIR'; source '$SCRIPT_DIR/env.sh'; clear"
init_pane() {
  local pane="$1" title="$2"
  tmux select-pane -t "$pane" -T "$title"
  tmux send-keys -t "$pane" "$PANE_INIT" Enter
}
tmux set -g pane-border-status top >/dev/null 2>&1 || true

init_pane "$BROKER"   "0 broker"
init_pane "$CONSUMER" "1 consumer"
init_pane "$PRODUCER" "2 producer"
init_pane "$TUI"      "3 TUI"
init_pane "$NOTES"    "4 bench + notes"

# --- pane 0: broker (auto-start) ---
tmux send-keys -t "$BROKER" "bash '$SCRIPT_DIR/broker.sh'" Enter

# --- pane 1: consumer (seeded, press Enter to run) ---
tmux send-keys -t "$CONSUMER" \
  "echo '[consumer] fan-in over all partitions. Press Enter to subscribe:'" Enter
tmux send-keys -t "$CONSUMER" "mq sub 'orders#*' c1"

# --- pane 2: producer cheatsheet ---
tmux send-keys -t "$PRODUCER" "cat <<'CHEAT'
[producer] the mq/mqs helpers inject auth + correct flag order for you:
  mq create -partitions 4 events              # explicit topic create
  mq pub orders 'hello world'                 # basic publish
  mq pub -partition 0 -key k1 orders 'idem'   # repeat SAME cmd -> DUP (per-partition)
  mq pub -routing-key user-42 orders 'routed' # deterministic partition (fnv1a)
  mq pub -partition 2 orders 'pinned'         # pin to partition 2
  mq pub -delay-ms 5000 orders 'later'        # delivered ~5s later
  mqs pub orders 'over-tls'                    # TLS listener (mqs = TLS variant)
Full script: less '$SCRIPT_DIR/WALKTHROUGH.md'
CHEAT" Enter

# --- pane 3: TUI (seeded, press Enter to launch) ---
tmux send-keys -t "$TUI" \
  "echo '[TUI] press Enter to launch. Keys: p=pub s=sub a=auto-ack n=nack q=quit'" Enter
tmux send-keys -t "$TUI" "mqtui"

# --- pane 4: bench + notes ---
tmux send-keys -t "$NOTES" "cat <<'CHEAT'
[bench + notes] walkthrough: less '$SCRIPT_DIR/WALKTHROUGH.md'
  mqbench  -topic bench -producers 8 -msgs 50000 -partitions 4
  mqbenchs -topic bench -producers 8 -msgs 50000 -partitions 4 -fsync batched
  curl -s \$METRICS/healthz ; echo
  curl -s \$METRICS/metrics | grep -E 'toymq_(inflight|consumer_lag|retention|dlq)'
CHEAT" Enter

tmux select-pane -t "$BROKER"
echo "==> attaching. Detach with 'Ctrl-b d'; reset with scripts/manual-test/teardown.sh"
exec tmux attach -t "$TMUX_SESSION"
