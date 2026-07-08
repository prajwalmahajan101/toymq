# 0027 — Correlated telemetry (logs ↔ traces ↔ metrics)

**Status:** Accepted
**Date:** 2026-07-08
**Scope:** `internal/logging/`, `internal/metrics/`, `internal/broker/`, `internal/server/`, `cmd/toymq/`
**Extends:** [ADR 0015](./0015-observability-stack.md); pairs with [ADR 0026](./0026-traceparent-wire-propagation.md)

## Context

After ADR 0015 the three observability channels were **independent**: slog logs,
Prometheus metrics, and OTel traces shared no join key, so an operator could not
pivot from a slow trace to its log lines, or from a latency-spike metric to the
trace that caused it. ADR 0026 added a `trace_id` that now flows through the
broker; this ADR makes logs and metrics *carry* it, and broadens the metric
surface from the 11 v1.3 series to real RED/USE coverage.

The governing constraint is unchanged from ADR 0015: **off-by-default**. Every
signal stays disabled unless its flag/endpoint is set, and the un-traced path is
byte-for-byte identical to pre-M7.

## Decision

### Logs carry the trace context

A `logging.CorrelationHandler` decorates the slog handler chain. On each record
it reads the active span from the record's context
(`trace.SpanContextFromContext`) and, when valid, injects `trace_id` and
`span_id` attributes. It is a no-op when there is no recording span, so
un-traced logs are unchanged.

Correlation only applies to `*Context` log calls (`slog.InfoContext`, …) — a
bare `slog.Info` has no context to read a span from. The traced hot-path log
sites (`consumer subscribed`, dead-lettering) were converted to the `*Context`
forms so the join actually fires; background-loop logs without a span context
are left as-is.

### Metrics: RED/USE depth + exemplars

Eleven series are added, all following the existing nil-safe helper pattern
(`if m == nil { return }`) and registered on the same private registry:

- **Delivery outcomes:** `ack_total`, `nack_total`, `dlq_total{trigger}`.
- **Lag (the roadmap exporter):** `consumer_lag_messages{topic,partition,consumer}`
  = partition head msgID − consumer lastAcked, set on each ack.
- **Backlog / scheduling:** `delayed_pending{topic,partition}`,
  `partition_latest_msgid`, `wal_segments`.
- **Retention:** `retention_segments_reclaimed_total`, `retention_bytes_reclaimed_total`.
- **Errors:** `command_errors_total{verb,code}`, `publish_failure_total`.

`toymq_wal_append_seconds` gains a **`trace_id` exemplar** (via
`ExemplarObserver`), so a latency spike in Grafana links straight to the trace
that produced it — the metric→trace pivot.

`delayed_pending` is explicitly **best-effort, in-process** (like the DLQ
attempt count in ADR 0024): it resets to 0 on restart, and its decrement clamps
at 0 so a delayed record published before a restart fires without underflowing.

### Log pipeline: JSON stdout + Grafana Alloy (target)

For log→trace correlation in Grafana we chose **structured JSON stdout scraped
by Grafana Alloy into Loki**, rather than an OTLP log bridge. Rationale: it adds
no dependency to the broker's hot path (slog stays; otel-log stays out), and a
Grafana *derived field* maps `trace_id` → Tempo. This decision is recorded here;
the Alloy/Loki/Tempo/Prometheus **provisioning** (docker-compose, dashboards,
`alerts.yml`, datasource correlation) is the M7.5 config milestone and is not
part of this code change.

## Consequences

**Positive**
- One `trace_id` joins a log line → its span → a metric exemplar: the "pivot
  between all three" story, provable end-to-end once M7.5 provisions the stack.
- Metric coverage moves from thin (publish/subscribe/inflight) to RED/USE
  (acks, nacks, lag, DLQ, retention, errors), all partition-aware.
- Still off-by-default and nil-safe; existing deployments are unchanged.

**Negative / trade-offs**
- Log correlation only fires on `*Context` calls; a bare `slog.Info` in a traced
  path silently misses the ids (must remember to use the `*Context` form).
- `delayed_pending` is best-effort across restarts (documented).
- `command_errors_total`'s `verb` label is recovered from the parse-error text
  (the Command is nil on a parse failure), so it is a low-cardinality
  approximation, not a guaranteed-exact verb.

## v3 / toyraft forward-compat

Correlation is a materialised-view concern (handlers, exemplars, gauges) computed
outside `StateMachine.Apply`, so nothing here constrains v3. The lag gauge reads
the same per-partition head/lastAcked state the delivery path already maintains;
under v3 it becomes a per-replica local view, needing no protocol change.

## Usage

Run the broker with observability on and JSON logs:

```
toymq --metrics-addr :6790 --otlp-endpoint localhost:4317 --log-format json
```

Then a traced request emits a log line, a span, and a metric exemplar that all
share the same `trace_id`.
