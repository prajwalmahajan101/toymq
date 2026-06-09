# 0015 — Observability stack: Prometheus + OpenTelemetry

**Status:** Accepted
**Date:** 2026-06-09
**Scope:** `internal/metrics/`, `internal/tracing/`, `cmd/toymq/`,
`go.mod`

## Context

After v1.2.0 the broker emits structured logs at state-change
points (broker open / close, listener bound, consumer subscribed,
redelivery fired, offsets flushed, session open / close). Logs
answer *"what just happened?"* but they cannot answer the questions
a Grafana dashboard exists to answer:

- *How many publishes per second per topic, sustained over the
  last 15 minutes?*
- *What is the p99 of `fsync` latency, and is it correlated with
  redelivery rate?*
- *How deep is the inflight queue for consumer X right now?*
- *Did a publish that took 800ms also fail to flush offsets, and
  in what order did the spans run inside the broker?*

The `WithLogger` opt-in from v1.2.0 was the first crack in the
"silent by default" stance that ADR 0013 established for the
client; ADR 0014 then narrowed the "stdlib only" rule to "except
`cmd/toymq-tui`". This ADR widens the crack further by accepting
metrics and tracing as first-class broker concerns and accepting
their transitive dependency footprint.

## Decision

### Metrics — Prometheus `client_golang`

`github.com/prometheus/client_golang` is the canonical Go metrics
library, scrapeable by every Prometheus-compatible TSDB. We use
its `*prometheus.Registry` directly (not the global default) so
tests cannot leak state between runs.

A small `internal/metrics` package owns one `Metrics` struct
holding every counter / gauge / histogram the broker uses (names
in the table in Commit 2 of the plan). A nil `*Metrics` pointer is
a valid "metrics off" state — call sites go through a helper
method whose first statement is `if m == nil { return }`, the same
discipline as `Client.log` from ADR 0013's addendum.

Why not Stdlib `expvar`:

- No histogram support; we need p50/p95/p99 for fsync and Publish
  latency.
- The Prometheus exposition format is a de-facto standard;
  `expvar` requires a translator for every consumer.

Why not OpenTelemetry metrics:

- The OTel metrics API in Go is still maturing (multiple SDK
  rewrites in the last 18 months). Prom `client_golang` has been
  stable for years.
- We get an OTel-compatible scrape for free via the OTel
  Prometheus receiver, so picking Prom does not block future OTel
  metrics migration.

### Tracing — OpenTelemetry SDK

`go.opentelemetry.io/otel` plus the OTLP gRPC exporter. The OTel
trace API is stable; OTLP is the open standard accepted by Jaeger,
Tempo, Honeycomb, Datadog, Lightstep, and every collector in
between.

`internal/tracing` exports a `Tracer` wrapper. When the configured
endpoint is empty, the package installs a no-op TracerProvider so
every `tracer.Start(ctx, ...)` call returns the same noop span. The
hot path therefore pays one interface dispatch per span — measured
at ~10 ns in independent benchmarks — and zero allocations.

Sampling defaults to `ParentBased(TraceIDRatio(0.05))` — 5% of
root spans. `--trace-sample-ratio` (0..1) is the operator knob.

Spans are added at:

- `broker.Publish` → `"broker.publish"` (attrs: topic,
  payload_bytes, duplicate).
- `broker.Subscribe` → `"broker.subscribe"` (attrs: topic,
  consumer_id, from_msg_id).
- `wal.Log.Append` → `"wal.append"` (attrs: msg_id, bytes).
- `broker.sweepRedelivery` → `"broker.redelivery_sweep"` (attrs:
  count) — one span per tick, not per message, to keep cardinality
  bounded.

### Exposure — separate `--metrics-addr` HTTP port

A dedicated `--metrics-addr` flag (default `:6790`) binds an HTTP
server that serves `/metrics` (Prometheus exposition) and
`/healthz`. Empty value disables the HTTP listener entirely. The
broker's TCP wire-protocol port is unaffected.

Why a separate port:

- Conceptually orthogonal: the wire protocol is not HTTP. Muxing
  would risk feeding HTTP-shaped first bytes to the line parser.
- Operationally orthogonal: scrape policies, firewall rules, and
  rate limits for `/metrics` differ from those for the broker
  port.
- One flag, easy to disable in environments that don't want the
  endpoint.

### Boundary update

The previous boundary was: *"stdlib only outside
`cmd/toymq-tui/`"* (per ADR 0014). The new boundary is:

> Stdlib only inside `pkg/client`, `cmd/toymqctl`, and
> `cmd/toymq-bench`. The Charm stack stays confined to
> `cmd/toymq-tui/`. The Prometheus + OTel stacks may appear in
> `cmd/toymq`, `internal/metrics`, `internal/tracing`, and any
> `internal/` package the broker hot path crosses
> (`internal/broker`, `internal/server`, `internal/wal`).

`pkg/client` stays stdlib-only by design — it remains a "drop-in
client library for any Go consumer" with no surprise transitive
weight. Consumers that want client-side metrics or traces wrap the
public API at their layer.

### Off-by-default contract

- `--metrics-addr=""` skips the HTTP listener and the
  `*prometheus.Registry`. The Go process holds no Prom collectors.
- `--otlp-endpoint=""` installs the OTel noop TracerProvider. No
  gRPC client, no exporter goroutine, no batching queue.
- A user upgrading to v1.3.0 who does not set either flag sees
  zero behavioural change vs v1.2.0.

## Consequences

**Positive**

- A Grafana dashboard that answers the four questions in Context.
- OTLP traces consumable by any modern collector.
- Hot-path cost remains negligible: counter `.Inc()` is one
  atomic add; histogram `.Observe()` is a buckets walk; noop span
  start is one interface call.

**Negative**

- `go.mod` grows by ~20 modules: `prometheus/client_golang`,
  `prometheus/client_model`, `prometheus/common`,
  `prometheus/procfs`, `cespare/xxhash`, `beorn7/perks`,
  `golang/protobuf`, plus the OTel module tree
  (`go.opentelemetry.io/otel`, `.../sdk`, `.../trace`,
  `.../exporters/otlp/otlptrace/otlptracegrpc`,
  `google.golang.org/grpc`, etc.).
- The broker binary's scratch Docker image grows from ~10 MB to
  ~25-30 MB. Still tiny.
- CI build time bumps by ~3-5s due to extra compile units.
- Future protocol additions (W3C `traceparent` in the wire format,
  metrics over the wire) will need their own ADRs.

## Limitations of this design

- **No cross-process trace propagation.** The wire protocol does
  not carry a W3C `traceparent` line yet, so broker spans are
  always root spans. A client that traces its own `Pub` and the
  resulting broker span will appear as two unrelated traces. A
  future ADR + wire-protocol revision is required to wire them
  together.
- **No alerting rules shipped.** The dashboard panel queries are
  the dashboard's contract; alert rules are a follow-up once
  SLOs are defined.
- **No per-`msg_id` labels.** Prometheus cardinality blows up
  with high-cardinality labels; metric labels stay at topic +
  consumer level. For per-message investigation, use traces.
- **`pkg/client` stays silent.** No `WithMetrics`, no
  `WithTracer` — consumers wrap the API at their layer if they
  need client-side observability.

## Usage

```go
// cmd/toymq main.go (sketch)
reg := metrics.NewRegistry()
m := metrics.New(reg)

tp, _ := tracing.New(ctx, cfg.OTLPEndpoint, cfg.ServiceVersion, cfg.TraceSampleRatio)
defer tp.Shutdown(shutdownCtx)
tr := tracing.NewTracer(tp)

b, _ := broker.NewWithObservability(cfg.DataDir, cfg.DedupeCap, m, tr)
srv := server.NewWithObservability(cfg.Addr, b, m)

if cfg.MetricsAddr != "" {
    go runMetricsHTTP(cfg.MetricsAddr, reg)
}
```

Operator commands:

```bash
# Local: full stack via compose (broker + Prometheus + Grafana)
docker compose up -d
open http://localhost:3000   # Grafana, anonymous viewer

# Local binary, metrics only
toymq --metrics-addr :6790
curl localhost:6790/metrics

# Add tracing pointing at a Jaeger all-in-one
toymq --metrics-addr :6790 \
      --otlp-endpoint http://localhost:4317 \
      --trace-sample-ratio 0.1
```
