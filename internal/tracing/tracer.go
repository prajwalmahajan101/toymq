// Package tracing wires the OpenTelemetry TracerProvider for the
// broker. When the configured OTLP endpoint is empty, New returns a
// no-op TracerProvider so production code can call tracer.Start
// unconditionally and pay only one interface dispatch per span.
//
// Spans are added at broker.Publish, broker.Subscribe, wal.Append,
// and the redelivery sweep — see ADR 0015 for the full list and
// the cross-process traceparent limitation.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerProvider abstracts what tracing.New returns so cmd/toymq can
// defer Shutdown without caring whether the underlying provider is
// the real SDK or the noop.
type TracerProvider interface {
	Tracer() trace.Tracer
	Shutdown(ctx context.Context) error
}

// New returns a TracerProvider. When endpoint is empty, the noop
// provider is returned and no exporter is created. Otherwise the
// OTLP gRPC exporter is wired to endpoint and the SDK provider is
// configured with a ParentBased(TraceIDRatio(sampleRatio)) sampler.
//
// serviceVersion is attached as the otel.service.version attribute
// so dashboards can break panels down by deploy.
func New(ctx context.Context, endpoint, serviceVersion string, sampleRatio float64) (TracerProvider, error) {
	if endpoint == "" {
		return &noopProvider{tp: noop.NewTracerProvider()}, nil
	}

	exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("toymq"),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	if sampleRatio < 0 {
		sampleRatio = 0
	}
	if sampleRatio > 1 {
		sampleRatio = 1
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return &sdkProvider{tp: tp}, nil
}

const tracerName = "github.com/prajwalmahajan101/toymq"

type noopProvider struct{ tp trace.TracerProvider }

func (n *noopProvider) Tracer() trace.Tracer            { return n.tp.Tracer(tracerName) }
func (n *noopProvider) Shutdown(context.Context) error { return nil }

type sdkProvider struct{ tp *sdktrace.TracerProvider }

func (s *sdkProvider) Tracer() trace.Tracer { return s.tp.Tracer(tracerName) }
func (s *sdkProvider) Shutdown(ctx context.Context) error {
	return s.tp.Shutdown(ctx)
}

// Convenience attribute keys used by call sites in broker / wal.
// Keeping the keys here lets the dashboard JSON stay in sync with
// the code.
var (
	AttrTopic        = attribute.Key("topic")
	AttrConsumerID   = attribute.Key("consumer_id")
	AttrFromMsgID    = attribute.Key("from_msg_id")
	AttrMsgID        = attribute.Key("msg_id")
	AttrPayloadBytes = attribute.Key("payload_bytes")
	AttrDuplicate   = attribute.Key("duplicate")
	AttrCount        = attribute.Key("count")
)
