package integration

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toymq/internal/broker"
	"github.com/prajwalmahajan101/toymq/internal/logging"
	"github.com/prajwalmahajan101/toymq/internal/metrics"
	"github.com/prajwalmahajan101/toymq/internal/server"
	"github.com/prajwalmahajan101/toymq/internal/tracing"
	"github.com/prajwalmahajan101/toymq/pkg/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// m7Harness is a purpose-built integration rig for the M7 observability
// milestone: unlike the shared startBroker helper it wires a real
// span-recording TracerProvider and a live metrics registry so the tests
// can assert on emitted spans and gauge values.
type m7Harness struct {
	addr     string
	metrics  *metrics.Metrics
	tracer   oteltrace.Tracer
	recorder *tracetest.SpanRecorder
}

func startM7Broker(t *testing.T) *m7Harness {
	t.Helper()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	tracer := tp.Tracer("m7-test")

	reg := metrics.NewRegistry()
	m := metrics.New(reg)

	b, err := broker.NewWithObservability(
		t.TempDir(), defaultDedupeCap, 1, defaultRecvWindow,
		defaultVisibility, defaultRedeliverInterval,
		broker.SyncConfig{}, broker.RetentionConfig{}, 0, m, tracer,
	)
	if err != nil {
		t.Fatalf("broker.NewWithObservability: %v", err)
	}

	srv := server.NewWithObservability("127.0.0.1:0", b, m)
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	addr, err := waitForAddr(srv, defaultAddrPollTimeout)
	if err != nil {
		cancel()
		_ = b.Close()
		t.Fatalf("server did not bind: %v", err)
	}

	t.Cleanup(func() {
		shutCtx, cancelShut := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancelShut()
		_ = srv.Shutdown(shutCtx)
		cancel()
		<-serveErr
		_ = b.Close()
		_ = tp.Shutdown(context.Background())
	})

	return &m7Harness{addr: addr, metrics: m, tracer: tracer, recorder: rec}
}

// findSpan returns the first ended span with the given name.
func findSpan(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, s := range spans {
		if s.Name() == name {
			return s, true
		}
	}
	return nil, false
}

// TestTraceparentPropagatesToBrokerSpan verifies the ADR 0026 headline:
// a client that injects a TRACEPARENT line makes the broker's publish span
// a child of the caller's span (same trace, parented on the client span),
// rather than a fresh root.
func TestTraceparentPropagatesToBrokerSpan(t *testing.T) {
	h := startM7Broker(t)

	c, err := client.Dial(context.Background(), h.addr,
		client.WithTraceparentFunc(tracing.TraceparentFromContext))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ctx, clientSpan := h.tracer.Start(context.Background(), "client.publish")
	if _, _, err := c.Pub(ctx, "orders", "k1", "", []byte("hello")); err != nil {
		t.Fatalf("pub: %v", err)
	}
	clientSpan.End()

	spans := h.recorder.Ended()
	pub, ok := findSpan(spans, "broker.publish")
	if !ok {
		t.Fatal("no broker.publish span recorded")
	}
	wantTrace := clientSpan.SpanContext().TraceID()
	if pub.SpanContext().TraceID() != wantTrace {
		t.Fatalf("broker.publish trace id = %s, want %s (not propagated)",
			pub.SpanContext().TraceID(), wantTrace)
	}
	if !pub.Parent().IsValid() {
		t.Fatal("broker.publish is a root span; expected a remote parent")
	}
	if pub.Parent().SpanID() != clientSpan.SpanContext().SpanID() {
		t.Fatalf("broker.publish parent span id = %s, want client span %s",
			pub.Parent().SpanID(), clientSpan.SpanContext().SpanID())
	}
}

// TestLogJoinsTrace verifies log/trace correlation (ADR 0027): a log line
// emitted inside a propagated span carries the same trace_id, so Loki can
// join it to the trace in Grafana. The "consumer subscribed" line is
// emitted under the broker.subscribe span, which is parented on the
// client's SUB traceparent.
func TestLogJoinsTrace(t *testing.T) {
	h := startM7Broker(t)

	// Redirect the default logger through the correlation handler into a
	// buffer for the duration of this (non-parallel) test.
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(logging.NewCorrelationHandler(
		slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)))
	defer slog.SetDefault(prev)

	c, err := client.Dial(context.Background(), h.addr,
		client.WithTraceparentFunc(tracing.TraceparentFromContext))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ctx, span := h.tracer.Start(context.Background(), "client.subscribe")
	if _, err := c.Sub(ctx, "orders", "c1"); err != nil {
		t.Fatalf("sub: %v", err)
	}
	span.End()
	traceID := span.SpanContext().TraceID().String()

	// The "consumer subscribed" line is emitted synchronously during Sub;
	// give the server a beat to flush it.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "consumer subscribed") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	logs := buf.String()
	if !strings.Contains(logs, "consumer subscribed") {
		t.Fatalf("no 'consumer subscribed' log captured; got:\n%s", logs)
	}
	if !strings.Contains(logs, traceID) {
		t.Fatalf("log line missing trace_id %s; got:\n%s", traceID, logs)
	}
}

// TestNoTraceparentBackCompat verifies the additive/opt-in contract: a
// client that never sends TRACEPARENT publishes and consumes exactly as
// pre-M7, and the broker.publish span is a fresh root (no phantom parent).
func TestNoTraceparentBackCompat(t *testing.T) {
	h := startM7Broker(t)

	c, err := client.Dial(context.Background(), h.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	id, dup, err := c.Pub(context.Background(), "orders", "k1", "", []byte("v1"))
	if err != nil || dup {
		t.Fatalf("pub: id=%d dup=%v err=%v", id, dup, err)
	}

	ch, err := c.Sub(context.Background(), "orders", "c1")
	if err != nil {
		t.Fatalf("sub: %v", err)
	}
	select {
	case d := <-ch:
		if string(d.Payload) != "v1" {
			t.Fatalf("payload = %q, want v1", d.Payload)
		}
		if err := d.Ack(context.Background()); err != nil {
			t.Fatalf("ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery")
	}

	pub, ok := findSpan(h.recorder.Ended(), "broker.publish")
	if !ok {
		t.Fatal("no broker.publish span recorded")
	}
	if pub.Parent().IsValid() {
		t.Fatalf("broker.publish should be a root span without TRACEPARENT, got parent %s",
			pub.Parent().SpanID())
	}
}

// TestConsumerLagGauge verifies the roadmap's lag exporter: after acking a
// contiguous prefix, toymq_consumer_lag_messages equals head - lastAcked.
func TestConsumerLagGauge(t *testing.T) {
	h := startM7Broker(t)

	c, err := client.Dial(context.Background(), h.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	const n = 10
	for i := range n {
		if _, _, err := c.Pub(context.Background(), "orders", "", "", []byte("m")); err != nil {
			t.Fatalf("pub %d: %v", i, err)
		}
	}

	ch, err := c.Sub(context.Background(), "orders", "c1")
	if err != nil {
		t.Fatalf("sub: %v", err)
	}

	// Receive all n, ack the first 4 (msgIDs 0..3) contiguously.
	const ackUpTo = 4
	for i := range n {
		select {
		case d := <-ch:
			if d.MsgID < ackUpTo {
				if err := d.Ack(context.Background()); err != nil {
					t.Fatalf("ack %d: %v", d.MsgID, err)
				}
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("only received %d/%d messages", i, n)
		}
	}

	// head = n-1 = 9, lastAcked = 3 -> lag = 6. The gauge is set on the last
	// ack (msgID 3); give the broker a beat to have processed it.
	g := h.metrics.ConsumerLag.With(prometheus.Labels{
		"topic": "orders", "partition": "0", "consumer": "c1",
	})
	deadline := time.Now().Add(time.Second)
	var got float64
	for time.Now().Before(deadline) {
		got = testutil.ToFloat64(g)
		if got == float64(n-1-(ackUpTo-1)) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if want := float64(n - 1 - (ackUpTo - 1)); got != want {
		t.Fatalf("consumer_lag_messages = %v, want %v", got, want)
	}
}
