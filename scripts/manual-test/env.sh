#!/usr/bin/env bash
# env.sh — shared context for the toymq manual-test lab.
#
# Sourced by tmux-lab.sh / broker.sh / teardown.sh (bash) AND by every tmux
# pane's login shell (which may be zsh). It must therefore be BOTH bash- and
# zsh-safe: no BASH_SOURCE, no reliance on unquoted word-splitting.
#
# Everything the lab generates (binaries, cert/key, token file, data-dir) lives
# under $LAB_DIR — outside the repo — so nothing here touches version control.

# Repo root: callers (the bash scripts, and tmux-lab.sh when seeding panes)
# export TOYMQ_REPO. Fall back to the current directory for direct sourcing.
export TOYMQ_REPO="${TOYMQ_REPO:-$PWD}"

# Lab workspace (override with TOYMQ_LAB_DIR).
export LAB_DIR="${TOYMQ_LAB_DIR:-/tmp/toymq-lab}"
export BIN="$LAB_DIR/bin"
export DATA="$LAB_DIR/data"
export CERT="$LAB_DIR/cert.pem"
export KEY="$LAB_DIR/key.pem"
export CA="$CERT" # self-signed: the cert is its own CA
export TOKENS="$LAB_DIR/tokens.txt"

# Credentials / addresses.
export TOKEN="lab-secret-token"
export WIRE_PLAIN="localhost:6789"
export WIRE_TLS="localhost:6889"
export METRICS="localhost:6790"

# Put the built binaries first on PATH so panes call bare toymq/toymqctl/etc.
export PATH="$BIN:$PATH"

export TMUX_SESSION="toymq-lab"

# Absolute path to broker.sh so a restart works from any pane's cwd.
export BROKER_SH="$TOYMQ_REPO/scripts/manual-test/broker.sh"

# --- Wrapper functions (bash + zsh compatible) ----------------------------
# These inject the auth/addr (and TLS) flags in the CORRECT position: Go's flag
# package stops parsing at the first positional arg, so every flag must precede
# the topic/payload. The wrappers put the injected flags first, then "$@" (your
# extra flags + positionals). Use them instead of raw toymqctl so ordering and
# auth are always right, in any shell.
#
#   mq  pub orders 'hi'                 # plain listener
#   mq  pub -partition 0 -key k1 orders dup
#   mq  sub 'orders#*' c1
#   mq  create -partitions 4 events
#   mqs pub orders 'over tls'           # TLS listener
#   mqbench  -topic bench -producers 8 -msgs 50000 -partitions 4
#   mqbenchs -topic bench -producers 8 -msgs 50000 -partitions 4
#   mqtui                               # plain TUI
#   mqtuis                              # TLS TUI

mq() {
  local verb="$1"; shift
  toymqctl "$verb" -addr "$WIRE_PLAIN" -auth-token "$TOKEN" "$@"
}
mqs() {
  local verb="$1"; shift
  toymqctl "$verb" -addr "$WIRE_TLS" -tls -tls-ca "$CA" -auth-token "$TOKEN" "$@"
}
mqbench()  { toymq-bench -addr "$WIRE_PLAIN" -auth-token "$TOKEN" "$@"; }
mqbenchs() { toymq-bench -addr "$WIRE_TLS" -tls -tls-ca "$CA" -auth-token "$TOKEN" "$@"; }
mqtui()  { toymq-tui -addr "$WIRE_PLAIN" -auth-token "$TOKEN" "$@"; }
mqtuis() { toymq-tui -addr "$WIRE_TLS" -tls -tls-ca "$CA" -auth-token "$TOKEN" "$@"; }

# labbroker (re)starts the broker from anywhere: `labbroker`, `labbroker none`.
labbroker() { bash "$BROKER_SH" "$@"; }
