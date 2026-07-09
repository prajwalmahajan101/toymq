# ToyMQ manual-test walkthrough (v1 → v2)

A guided, feature-by-feature manual test of everything shipped from v1.0.0
through the v2 milestones (M1–M8). Run `./tmux-lab.sh` first — it builds the
binaries, generates a self-signed cert + token file, and lays out the panes.

**Pane map**

```
0 broker      1 consumer
2 producer    3 TUI
4 bench + notes (full width)
```

**Conventions**

Every pane sources `env.sh`, which defines helper functions that inject auth and
the **correct flag order** (Go's `flag` stops parsing at the first positional, so
flags must precede the topic/payload — the helpers handle that):

| Helper | Expands to |
|---|---|
| `mq <verb> …` | `toymqctl <verb> -addr localhost:6789 -auth-token <tok> …` (plain) |
| `mqs <verb> …` | `… -addr localhost:6889 -tls -tls-ca <cert> -auth-token <tok> …` (TLS) |
| `mqbench …` / `mqbenchs …` | `toymq-bench …` (plain / TLS) |
| `mqtui` / `mqtuis` | `toymq-tui …` (plain / TLS) |

The lab broker (pane 0) runs with **auth + TLS + 4 partitions + batched fsync +
recv-window 8 + segmentation/retention + DLQ-after-3-nacks + metrics on :6790**.
Expected output follows each command; IDs/partitions vary by run.

---

## 0. Sanity — is the broker up?

Pane **4**:

```bash
curl -s $METRICS/healthz; echo
```

Expected: `ok`. If not, check pane 0 for `msg="metrics listening"` and two
`msg=listening` lines (`:6789` plaintext, `:6889` TLS).

---

## 1. HELLO handshake + AUTH + TLS  *(M3)*

**Auth is enforced.** Pane **2** — a bad token is rejected (raw `toymqctl`, flags
first):

```bash
toymqctl pub -addr localhost:6789 -auth-token wrong-token orders 'nope'
```

Expected: a non-zero exit with `authentication failed: invalid or missing token`.

Good token over the **plain** listener, then over **TLS**:

```bash
mq  pub orders 'hello over plain'
mqs pub orders 'hello over tls'
```

Expected: each returns `OK <id>` (exit 0). Same broker — TLS terminates on `:6889`.

**Raw wire** (optional, no client lib) — HELLO must be the first line:

```bash
printf 'HELLO 1 AUTH lab-secret-token\nPUB orders - - 3\nraw\n' | nc -q1 localhost 6789
```

Expected:

```
HELLO 1 OK
OK <id>
```

Drop the `AUTH` token to see the rejection:

```bash
printf 'HELLO 1\nPUB orders - - 3\nraw\n' | nc -q1 localhost 6789
```

Expected: `ERR AUTH ...`, then the connection closes.

---

## 2. Basic PUB / SUB + at-least-once delivery  *(v1)*

Pane **1** — press **Enter** to run the seeded fan-in subscribe:

```bash
mq sub 'orders#*' c1
```

Pane **2** — publish:

```bash
mq pub orders 'first message'
```

Expected in pane 1:

```
MSG topic=orders partition=<n> id=<id> payload="first message"
```

(The consumer auto-acks by default; earlier step-1 messages show up too.)

---

## 3. Idempotent producer + dedupe **persistence**  *(v1 + M1)*

Dedupe is **per-partition**, so pin the partition (or reuse a routing key) or the
two publishes round-robin to different partitions and both succeed. Pane **2**:

```bash
mq pub -partition 0 -key k1 orders 'idempotent payload'
mq pub -partition 0 -key k1 orders 'idempotent payload'
```

The **second** returns `DUP <id>` (same id, no new append). Raw-wire equivalent,
pinning partition 0 via `orders#0`:

```bash
printf 'HELLO 1 AUTH lab-secret-token\nPUB orders#0 k1 - 4\ndupe\n' | nc -q1 localhost 6789
```

Expected: `DUP <id>` (the id from the first publish of `k1`).

**Now prove it survives a restart (the M1 guarantee).** Pane **0**: press
`Ctrl-C`, then re-run the broker with `labbroker` (a cwd-independent helper) —
or press **Up-arrow**, Enter. Pane **2**:

```bash
mq pub -partition 0 -key k1 orders 'idempotent payload'
```

Expected: still `DUP` — the dedupe LRU was **rebuilt from the WAL** on startup,
not lost. (Pre-M1 this would have returned a fresh `OK`.)

---

## 4. Partitions  *(M4)*

`orders` already has 4 partitions (`--default-partitions 4`). Create an explicit
one to see the verb:

```bash
mq create -partitions 4 events     # idempotent for the same count
```

Pane **2** — three routing modes:

```bash
mq pub -routing-key user-42 orders 'routed'    # fnv1a hash -> fixed partition
mq pub -partition 2         orders 'pinned'    # forced to partition 2
for i in 1 2 3 4 5 6; do mq pub orders "rr-$i"; done   # keyless -> round-robin
```

Watch pane 1 (`orders#*`) fan them in; note the `partition=` field. **Per-partition
`id` is monotonic**; there is no order across partitions.

Single-partition view — pane **2** (new consumer id):

```bash
mq sub 'orders#2' only-p2
```

Expected: only messages whose `partition=2` (includes the `-partition 2` publish).
`Ctrl-C` to stop. Re-running `-routing-key user-42` always lands on the **same**
partition (deterministic, stable across restarts).

---

## 5. Batched fsync mode  *(M2)*

The broker runs `--fsync batched --fsync-interval 50ms` (group commit). Every
`OK` is still durable (`committed` advances only after the group fsync). To
contrast modes, restart pane **0**:

```bash
# pane 0: Ctrl-C, then:
labbroker per-message     # fsync every PUB (v1 default)
# ...or:
labbroker none            # no fsync (survives clean shutdown only)
```

Publish under each and confirm `OK`s still return. See
`docs/adr/0019-batched-fsync-mode.md`. Restart back to `labbroker` (batched)
before continuing.

---

## 6. Delayed messages  *(M6)*

Keep the pane-1 `orders#*` subscription running. Pane **2**:

```bash
mq pub orders 'immediate'
mq pub -delay-ms 5000 orders 'delayed 5s'
```

Expected: `immediate` shows at once; `delayed 5s` appears ~5 seconds later, in
per-partition order. The delay is persisted in the WAL (survives restart).

---

## 7. Dead-letter queue  *(M6)*

The broker moves a message to `<topic>.dlq` after **3** failed deliveries
(`--dlq-after-nacks 3`). `toymqctl` has no `nack`, so drive this from the **TUI**
(pane 3), whose `n` key nacks the last message.

1. Pane **3** — press **Enter** to launch `mqtui`.
2. Press `a` to turn **auto-ack off** (header shows `auto-ack=false`).
3. Press `s`, subscribe: topic `dlqtest`, consumer `d1`, Enter.
4. Pane **2** — publish one message:
   ```bash
   mq pub dlqtest 'poison'
   ```
5. In the TUI, press `n` (nack) on the delivered message. It is redelivered; nack
   it again, and once more — **3 nacks total**.
6. After the 3rd failure the broker republishes it to `dlqtest.dlq`. Pane **2**:
   ```bash
   mq sub dlqtest.dlq dlq-reader
   ```
   Expected: the `poison` payload appears on the `.dlq` topic. `Ctrl-C` to stop.

> Alternative without the TUI: a `-no-auto-ack` subscriber that never acks lets
> the **visibility timeout** count as the failed deliveries — slower, but hands-off.

---

## 8. Reader backpressure / flow control  *(M5)*

The broker runs `--recv-window 8`: at most 8 un-acked messages in flight per
`(partition, consumer)`. Pane **2** — a slow consumer that never acks:

```bash
mq sub -no-auto-ack orders slow-consumer
```

Pane **4** — flood, then read the inflight gauge:

```bash
mqbench -topic orders -producers 4 -msgs 5000 -partitions 4
curl -s $METRICS/metrics | grep toymq_inflight_messages
```

Expected: `toymq_inflight_messages{...consumer="slow-consumer"...}` stays **bounded
at the window (8)** regardless of the 5000-message backlog. `Ctrl-C` the slow consumer.

> `PAUSE`/`RESUME` are wire-only verbs (no ctl/TUI flag). To see them:
> ```bash
> printf 'HELLO 1 AUTH lab-secret-token\nSUB orders p1\nPAUSE\nRESUME\n' | nc -q2 localhost 6789
> ```

---

## 9. Retention  *(M6)*

Segments roll at 1 MiB; retention keeps ~4 MiB per partition
(`--segment-bytes 1048576 --retain-bytes 4194304`).

> **Speed note:** the default broker runs `--fsync batched --fsync-interval 50ms`,
> which caps publish throughput at roughly `producers / 0.05s` (~80 msg/s with 4
> producers) — a 200k flood would take ~40 min. For this step, restart the broker
> in no-fsync mode first (pane 0: `Ctrl-C`, then `labbroker none`) so the flood
> finishes in seconds. Restart back to `labbroker` (batched) afterwards.

Pane **4** — overflow the 4 MiB/partition floor with many producers:

```bash
mqbench -topic flood -producers 16 -msgs 60000 -size 512
curl -s $METRICS/metrics | grep -E 'toymq_retention_(segments|bytes)_reclaimed_total'
```

Expected: the reclaim counters climb above 0 as sealed segments past the floor
are dropped. A consumer resuming below the floor gets `ERR OUT_OF_RANGE`; a fresh
consumer starts at the floor.

---

## 10. Benchmark harness + report polish  *(M8)*

Pane **4** — plain then TLS:

```bash
mqbench  -topic bench -producers 8 -msgs 50000 -partitions 4
mqbenchs -topic bench -producers 8 -msgs 50000 -partitions 4 -fsync batched
```

Confirm the M8 additions in the report header:

```
toymq-bench  addr=...  producers=8  msgs=50000  size=256  partitions=4  fsync=batched  tls=true
throughput  <n> msg/s   <n> MiB/s
per-part    ~<n> msg/s across 4 partition(s)      <- new per-partition line
latency     min=... p50=... p95=... p99=... max=...   <- p99 renders with TLS on
errors      0
```

---

## 11. Observability  *(M7 / M7.5)*

**Metrics/health (already on):**

```bash
curl -s $METRICS/healthz; echo
curl -s $METRICS/metrics | grep -E 'toymq_(publish|consumer_lag|ack|nack|wal_append)' | head
```

**Full LGTM stack** (needs Docker; a separate broker on the compose network):

```bash
cd "$TOYMQ_REPO"
docker compose -f docker-compose.observability.yml up -d --build
# Grafana:    http://localhost:3000  (dashboards pre-provisioned)
# Prometheus: http://localhost:9090
```

Publish some traffic against the compose broker, then in Grafana pivot
**log → trace → metric** by `trace_id` (Loki → Tempo → Prometheus exemplar).
Tear down:

```bash
docker compose -f docker-compose.observability.yml down -v
```

---

## 12. TUI tour  *(v1.1)*

Pane **3** — `mqtui` (relaunch if you quit it). Header shows
`connected @ localhost:6789`. Keys:

- `p` — publish modal (topic, payload, dedupe key; `Tab`/`Shift-Tab` move,
  `Enter` sends, `Esc` cancels).
- `s` — subscribe modal (topic, consumer-id).
- `a` — toggle auto-ack.
- `n` — nack the last MSG.
- `q` / `Ctrl-C` — quit.

Publish from the TUI and watch it land in the pane-1 consumer; subscribe from the
TUI and publish from pane 2 to watch it arrive here.

---

## Done

You have exercised: HELLO/auth/TLS, PUB/SUB/at-least-once, dedupe + persistence,
partitions, batched fsync, delayed messages, DLQ, flow control, retention, the
bench, the metrics/observability stack, and the TUI.

Tear the lab down with:

```bash
./teardown.sh            # stop tmux + broker
./teardown.sh --docker   # + observability compose
./teardown.sh --purge    # + delete /tmp/toymq-lab
```
