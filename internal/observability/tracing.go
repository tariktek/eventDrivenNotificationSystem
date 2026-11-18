package observability

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
)

// Propagator is the W3C trace context format used on every boundary:
// HTTP headers, the provider call, and the traceparent stored with each
// notification.
var Propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// SetupTracing installs the global tracer provider. With tracing disabled
// the no-op provider stays in place and instrumentation costs next to
// nothing. The returned function flushes buffered spans; call it on exit.
func SetupTracing(ctx context.Context, cfg config.TracingConfig, service, version string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(Propagator)
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	// The config holds the collector's base URL; OTLP/HTTP posts traces
	// to its /v1/traces path.
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(strings.TrimSuffix(cfg.Endpoint, "/")+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("service.version", version),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(workOnly{ratio: sdktrace.TraceIDRatioBased(cfg.SampleRatio)})),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// workOnly decides for spans that would start a new trace: only units of
// work do (an incoming HTTP request, a delivery picked from the queue), at
// the configured ratio. The DB and Redis instrumentation would otherwise
// turn every background poll (claims, heartbeats, the outbox relay ten
// times a second) into a trace of its own and bury the useful ones. Those
// queries still appear inside the traces of the work that runs them.
type workOnly struct{ ratio sdktrace.Sampler }

func (s workOnly) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Kind == trace.SpanKindServer || p.Kind == trace.SpanKindConsumer {
		return s.ratio.ShouldSample(p)
	}
	return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState()}
}

func (s workOnly) Description() string { return "WorkOnly(" + s.ratio.Description() + ")" }

// TraceParent returns ctx's span context as a W3C traceparent value, or ""
// when there is no sampled span.
func TraceParent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, c)
	return c.Get("traceparent")
}

// LinkFrom returns a link to the span a traceparent value names, for work
// that continues asynchronously what another trace started (a delivery
// continues the API request that created the notification). A link, not a
// parent: the delivery may happen much later, retry many times, or never.
func LinkFrom(ctx context.Context, traceparent string) []trace.Link {
	if traceparent == "" {
		return nil
	}
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []trace.Link{{SpanContext: sc, Attributes: []attribute.KeyValue{attribute.String("link.reason", "created by")}}}
}
