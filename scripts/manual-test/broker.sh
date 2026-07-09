#!/usr/bin/env bash
# broker.sh — launch the lab broker with the full v2 feature surface enabled.
#
# Runs in the foreground (pane 0) so its logs stream live. Ctrl-C stops it;
# re-run to restart over the same data-dir (used by the dedupe-persistence and
# fsync-mode walkthrough steps).
#
# Usage:
#   broker.sh [fsync-mode]     # fsync-mode: per-message (default) | batched | none
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TOYMQ_REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=env.sh
source "$SCRIPT_DIR/env.sh"

FSYNC="${1:-batched}"
case "$FSYNC" in
  per-message | batched | none) ;;
  *)
    echo "broker.sh: fsync mode must be per-message|batched|none (got '$FSYNC')" >&2
    exit 2
    ;;
esac

if [[ ! -x "$BIN/toymq" ]]; then
  echo "broker.sh: $BIN/toymq not found — run tmux-lab.sh first (it builds the binaries)." >&2
  exit 1
fi

echo "=== toymq lab broker: fsync=$FSYNC, plain=$WIRE_PLAIN, tls=$WIRE_TLS, metrics=$METRICS ==="
echo "    data-dir=$DATA  (Ctrl-C to stop; re-run to restart over the same data)"
echo

exec toymq \
  --addr ":6789" \
  --tls-addr ":6889" --tls-cert "$CERT" --tls-key "$KEY" \
  --auth-token-file "$TOKENS" \
  --require-hello=true \
  --data-dir "$DATA" \
  --default-partitions 4 \
  --fsync "$FSYNC" --fsync-interval 50ms \
  --recv-window 8 \
  --segment-bytes 1048576 --retain-bytes 4194304 --retain-duration 24h \
  --dlq-after-nacks 3 \
  --dedupe-cap 4096 \
  --metrics-addr ":6790" \
  --log-format text --log-level info
