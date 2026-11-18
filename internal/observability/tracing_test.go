package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func provider() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.ParentBased(workOnly{ratio: sdktrace.AlwaysSample()})),
	)
	return tp, rec
}

func names(rec *tracetest.SpanRecorder) []string {
	var out []string
	for _, s := range rec.Ended() {
		out = append(out, s.Name())
	}
	return out
}

// TestWorkOnlySampler: background queries start no trace; work does, and
// everything it runs is part of it.
func TestWorkOnlySampler(t *testing.T) {
	tp, rec := provider()
	tr := tp.Tracer("test")
	ctx := context.Background()

	_, poll := tr.Start(ctx, "SELECT claim") // a background poll: client span, no parent
	poll.End()

	work, req := tr.Start(ctx, "POST /api/v1/notifications", trace.WithSpanKind(trace.SpanKindServer))
	_, q := tr.Start(work, "INSERT notifications", trace.WithSpanKind(trace.SpanKindClient))
	q.End()
	req.End()

	_, del := tr.Start(ctx, "deliver notification", trace.WithSpanKind(trace.SpanKindConsumer))
	del.End()

	assert.ElementsMatch(t, []string{"INSERT notifications", "POST /api/v1/notifications", "deliver notification"}, names(rec))
}

func TestTraceParentAndLink(t *testing.T) {
	tp, _ := provider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "req", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	tp1 := TraceParent(ctx)
	require.Regexp(t, `^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`, tp1)
	links := LinkFrom(context.Background(), tp1)
	require.Len(t, links, 1)
	assert.Equal(t, span.SpanContext().TraceID(), links[0].SpanContext.TraceID())

	assert.Empty(t, TraceParent(context.Background()), "no span, nothing to store")
	assert.Nil(t, LinkFrom(context.Background(), ""))
	assert.Nil(t, LinkFrom(context.Background(), "garbage"))
}

func TestLogsCarryTraceIDs(t *testing.T) {
	tp, _ := provider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "req", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	ctx = WithCorrelationID(ctx, "corr-1")

	var buf bytes.Buffer
	log := slog.New(newContextHandler(slog.NewJSONHandler(&buf, nil)))
	log.InfoContext(ctx, "hello")

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
	assert.Equal(t, "corr-1", line["correlation_id"])
	assert.Equal(t, span.SpanContext().TraceID().String(), line["trace_id"])
	assert.Equal(t, span.SpanContext().SpanID().String(), line["span_id"])
}
