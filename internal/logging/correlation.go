// Package logging provides a slog.Handler wrapper that correlates log
// records with the active OpenTelemetry span, injecting trace_id and
// span_id fields so a log line joins to its trace in Grafana/Loki
// (ADR 0027).
//
// The wrapper is a no-op when the record's context carries no recording
// span (the observability-off path), so un-traced logs are byte-for-byte
// unchanged from the bare TextHandler/JSONHandler. Correlation only
// applies to *Context log calls (slog.InfoContext, logger.Log, …) — a
// bare slog.Info has no context to read a span from.
package logging

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// CorrelationHandler decorates an inner slog.Handler with trace/span id
// injection. Construct via NewCorrelationHandler.
type CorrelationHandler struct {
	inner slog.Handler
}

// NewCorrelationHandler wraps inner so every *Context log call whose
// context has a valid span emits trace_id (and span_id) attributes.
func NewCorrelationHandler(inner slog.Handler) *CorrelationHandler {
	return &CorrelationHandler{inner: inner}
}

// Enabled defers to the inner handler.
func (h *CorrelationHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle injects trace_id/span_id from the context's span context when
// one is present, then delegates to the inner handler.
func (h *CorrelationHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		r = r.Clone()
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
		if sc.HasSpanID() {
			r.AddAttrs(slog.String("span_id", sc.SpanID().String()))
		}
	}
	return h.inner.Handle(ctx, r)
}

// WithAttrs re-wraps so the correlation stays in effect on derived
// loggers.
func (h *CorrelationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &CorrelationHandler{inner: h.inner.WithAttrs(attrs)}
}

// WithGroup re-wraps so the correlation stays in effect on grouped
// loggers.
func (h *CorrelationHandler) WithGroup(name string) slog.Handler {
	return &CorrelationHandler{inner: h.inner.WithGroup(name)}
}
