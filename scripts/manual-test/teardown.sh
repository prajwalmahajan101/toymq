#!/usr/bin/env bash
# teardown.sh — stop the manual-test lab.
#
#   teardown.sh            # kill the tmux session (and any broker it hosts)
#   teardown.sh --docker   # also tear down the observability compose stack
#   teardown.sh --purge    # also delete $LAB_DIR (binaries, certs, data)
#   teardown.sh --docker --purge
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TOYMQ_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

DO_DOCKER=0
DO_PURGE=0
for arg in "$@"; do
  case "$arg" in
    --docker) DO_DOCKER=1 ;;
    --purge)  DO_PURGE=1 ;;
    *) echo "teardown: unknown flag '$arg' (want --docker|--purge)" >&2; exit 2 ;;
  esac
done

if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
  echo "==> killing tmux session '$TMUX_SESSION'"
  tmux kill-session -t "$TMUX_SESSION"
else
  echo "==> no tmux session '$TMUX_SESSION'"
fi

# Belt-and-suspenders: the lab broker, if it outlived tmux. broker.sh `exec`s
# toymq so its argv starts with "toymq" (not "$BIN/toymq"); match instead on the
# lab's unique token-file path, which always appears in the broker's argv.
if pgrep -f "$TOKENS" >/dev/null 2>&1; then
  echo "==> stopping stray lab broker"
  pkill -f "$TOKENS" || true
fi

if [[ "$DO_DOCKER" == 1 ]]; then
  echo "==> docker compose down (observability stack)"
  ( cd "$TOYMQ_REPO" && docker compose -f docker-compose.observability.yml down -v ) || true
fi

if [[ "$DO_PURGE" == 1 ]]; then
  echo "==> removing $LAB_DIR"
  rm -rf "$LAB_DIR"
fi

echo "done."
