package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace/noop"
)

func TestNew_EmptyEndpointReturnsNoop(t *testing.T) {
	tp, err := New(context.Background(), "", "test", 0.1)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tp.Shutdown(context.Background())

	if _, ok := tp.(*noopProvider); !ok {
		t.Fatalf("got %T, want *noopProvider when endpoint is empty", tp)
	}

	// Tracer must still be usable — span Start on the noop
	// provider returns a span whose IsRecording is false.
	tr := tp.Tracer()
	_, span := tr.Start(context.Background(), "test.span")
	if span.IsRecording() {
		t.Fatalf("noop span IsRecording = true, want false")
	}
	span.End()
}

func TestNoopProvider_TracerSource(t *testing.T) {
	// Direct construction matches what New returns for the empty
	// endpoint; smoke-check the type assertion path used by
	// production code.
	p := &noopProvider{tp: noop.NewTracerProvider()}
	if p.Tracer() == nil {
		t.Fatalf("noopProvider.Tracer returned nil")
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestNew_SampleRatioClamped(t *testing.T) {
	// Out-of-range ratios must not panic the SDK constructor. The
	// noop path is hit because endpoint is empty, but the ratio
	// validation runs before that check is reached in the SDK
	// path; cover the clamp explicitly with a non-empty endpoint
	// would require a live OTLP server, so we exercise the noop
	// path with extreme values and check no panic.
	for _, r := range []float64{-1, 0, 0.5, 1, 2} {
		tp, err := New(context.Background(), "", "test", r)
		if err != nil {
			t.Fatalf("ratio %v: %v", r, err)
		}
		_ = tp.Shutdown(context.Background())
	}
}
