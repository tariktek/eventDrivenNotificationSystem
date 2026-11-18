package observability

import (
	"context"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/trace"
)

type correlationKey struct{}

// WithCorrelationID returns a context carrying the request's correlation ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the correlation ID in ctx, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// contextAttrs are the request-scoped IDs carried by ctx: the correlation
// ID, and the trace and span IDs when a sampled span is active, so a log
// line leads to its trace in Jaeger.
func contextAttrs(ctx context.Context) []slog.Attr {
	var attrs []slog.Attr
	if id := CorrelationID(ctx); id != "" {
		attrs = append(attrs, slog.String("correlation_id", id))
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && sc.IsSampled() {
		attrs = append(attrs, slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return attrs
}

// contextHandler adds request-scoped attributes from the context to every
// record, so code only has to log with *Context methods to be traceable:
//
//	log.InfoContext(ctx, "notification created", "id", n.ID)
//	→ {..., "msg":"notification created", "id":"...", "correlation_id":"..."}
//
// The ID is always a top-level field, even on loggers derived with
// WithGroup, so a single query on correlation_id finds every line.
type contextHandler struct {
	inner slog.Handler // everything applied: what records normally go to
	// beforeGroup is the handler as it was before the first WithGroup, and
	// afterGroup replays every With* call made since. Only used to place
	// the correlation ID outside the groups.
	beforeGroup slog.Handler
	afterGroup  []func(slog.Handler) slog.Handler
}

func newContextHandler(h slog.Handler) contextHandler {
	return contextHandler{inner: h, beforeGroup: h}
}

func (h contextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := contextAttrs(ctx)
	if len(attrs) == 0 {
		return h.inner.Handle(ctx, r)
	}
	if len(h.afterGroup) == 0 {
		r.AddAttrs(attrs...)
		return h.inner.Handle(ctx, r)
	}
	// Grouped logger: attach the IDs before the groups, then rebuild them.
	// Grouped loggers are rare, so the extra allocation is acceptable.
	out := h.beforeGroup.WithAttrs(attrs)
	for _, apply := range h.afterGroup {
		out = apply(out)
	}
	return out.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := contextHandler{inner: h.inner.WithAttrs(attrs), beforeGroup: h.beforeGroup}
	if len(h.afterGroup) == 0 {
		next.beforeGroup = next.inner
		return next
	}
	next.afterGroup = withOp(h.afterGroup, func(x slog.Handler) slog.Handler { return x.WithAttrs(attrs) })
	return next
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{
		inner:       h.inner.WithGroup(name),
		beforeGroup: h.beforeGroup,
		afterGroup:  withOp(h.afterGroup, func(x slog.Handler) slog.Handler { return x.WithGroup(name) }),
	}
}

// withOp returns ops plus op in a new slice: sibling loggers derived from
// the same parent must never share (and overwrite) one backing array.
func withOp(ops []func(slog.Handler) slog.Handler, op func(slog.Handler) slog.Handler) []func(slog.Handler) slog.Handler {
	return append(slices.Clip(ops), op)
}
