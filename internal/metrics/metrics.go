// Package metrics defines the Prometheus metrics of the API and worker.
//
// Where each family is measured:
//
//   - Status counters (created, sent, failed, cancelled) are counted by the
//     outbox relay as it publishes events (ObserveEvents). Every status
//     change, whoever makes it (API, worker, reaper, a redrive, manual SQL),
//     becomes exactly one event and the relay publishes each one once, so
//     summing the counters over all workers gives exact system-wide totals
//     without instrumenting a dozen code paths.
//   - Delivery mechanics (provider latency, retries, rate-limit waits,
//     breaker state, lease expiries) are measured by the worker.
//   - Queue depth and outbox lag are sampled from the database every few
//     seconds (package stats) rather than counted on every request.
//   - HTTP traffic is measured by the API's middleware.
package metrics

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
)

// Metrics holds every collector. A process registers all of them; the
// labelled ones only appear once observed, so each process exposes what
// it actually measures.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec

	Created   *prometheus.CounterVec
	Sent      *prometheus.CounterVec
	Failed    *prometheus.CounterVec
	Cancelled *prometheus.CounterVec
	Retries   *prometheus.CounterVec

	QueueDepth     *prometheus.GaugeVec
	QueueOldestAge *prometheus.GaugeVec
	OutboxLag      prometheus.Gauge
	OutboxBacklog  prometheus.Gauge

	DeliveryLatency  *prometheus.HistogramVec
	ProviderDuration *prometheus.HistogramVec
	RateLimitWait    *prometheus.HistogramVec
	BreakerState     *prometheus.GaugeVec
	LeaseExpired     prometheus.Counter

	CallbackDeliveries *prometheus.CounterVec
	WebSocketClients   prometheus.Gauge
}

// New creates the collectors on a fresh registry, with Go runtime and
// process metrics.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := factory{reg}

	// Delivery times run from tens of milliseconds to minutes (retries,
	// backlogs). Quantiles are interpolated within a bucket, so the range
	// where most deliveries land is finely divided.
	latencyBuckets := []float64{.05, .1, .25, .5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 45, 60, 120, 300, 900}

	return &Metrics{
		Registry: reg,

		HTTPRequests: f.counterVec("http_requests_total", "HTTP requests handled.", "route", "method", "status"),
		HTTPDuration: f.histogramVec("http_request_duration_seconds", "HTTP request latency.",
			prometheus.DefBuckets, "route", "method", "status"),

		Created:   f.counterVec("notifications_created_total", "Notifications created.", "channel", "priority"),
		Sent:      f.counterVec("notifications_sent_total", "Notifications accepted by the provider.", "channel"),
		Failed:    f.counterVec("notifications_failed_total", "Notifications that ended in failed (dead letter).", "channel", "reason"),
		Cancelled: f.counterVec("notifications_cancelled_total", "Notifications cancelled before delivery.", "channel"),
		Retries:   f.counterVec("notification_retries_total", "Delivery attempts that failed transiently and were rescheduled.", "channel"),

		QueueDepth: f.gaugeVec("notification_queue_depth", "Notifications waiting or in flight, sampled.",
			"channel", "priority", "status"),
		QueueOldestAge: f.gaugeVec("notification_queue_oldest_age_seconds",
			"How long the oldest due pending notification has been waiting, sampled.", "channel"),
		OutboxLag:     f.gauge("outbox_lag_seconds", "Age of the oldest unpublished outbox event, sampled."),
		OutboxBacklog: f.gauge("outbox_backlog_events", "Unpublished outbox events, sampled."),

		DeliveryLatency: f.histogramVec("notification_delivery_latency_seconds",
			"From creation (or scheduled time) to acceptance by the provider.", latencyBuckets, "channel", "priority"),
		ProviderDuration: f.histogramVec("provider_request_duration_seconds", "Provider call latency.",
			[]float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}, "channel", "status_class"),
		RateLimitWait: f.histogramVec("rate_limit_wait_seconds", "Wait for a send slot.",
			[]float64{0, .001, .005, .01, .05, .1, .25, .5, 1}, "channel"),
		BreakerState: f.gaugeVec("circuit_breaker_state", "0 closed, 1 half-open, 2 open.", "channel"),
		LeaseExpired: f.counter("worker_lease_expired_total", "Claims recovered by the reaper after their worker stopped heartbeating."),

		CallbackDeliveries: f.counterVec("callback_deliveries_total", "Status callback attempts by result.", "result"),
		WebSocketClients:   f.gauge("websocket_clients", "Connected WebSocket clients."),
	}
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

// ObserveEvents counts status transitions from published events.
func (m *Metrics) ObserveEvents(evs []events.Event) {
	for i := range evs {
		e := &evs[i]
		if e.Type != events.TypeNotificationCreated && e.Type != events.TypeNotificationStatusChanged {
			continue
		}
		var p events.NotificationPayload
		if json.Unmarshal(e.Payload, &p) != nil {
			continue
		}
		switch {
		case e.Type == events.TypeNotificationCreated:
			m.Created.WithLabelValues(p.Channel, p.Priority).Inc()
		case p.Status == "sent":
			m.Sent.WithLabelValues(p.Channel).Inc()
		case p.Status == "failed":
			m.Failed.WithLabelValues(p.Channel, failureReason(&p)).Inc()
		case p.Status == "cancelled":
			m.Cancelled.WithLabelValues(p.Channel).Inc()
		}
	}
}

// failureReason buckets why a notification failed, with few values so the
// label stays cheap.
func failureReason(p *events.NotificationPayload) string {
	switch {
	case p.ErrorCode != nil && *p.ErrorCode == "lease_expired":
		return "lease_expired"
	case p.Attempts >= p.MaxAttempts:
		return "attempts_exhausted"
	default:
		return "permanent_error"
	}
}

// StatusClass groups an HTTP status for labels: "2xx", "4xx", "5xx", or
// "error" when there was no response.
func StatusClass(code int) string {
	switch {
	case code <= 0:
		return "error"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

// Seconds converts a duration for Observe.
func Seconds(d time.Duration) float64 { return d.Seconds() }

type factory struct{ reg prometheus.Registerer }

func (f factory) counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(c)
	return c
}

func (f factory) counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	f.reg.MustRegister(c)
	return c
}

func (f factory) gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(g)
	return g
}

func (f factory) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	f.reg.MustRegister(g)
	return g
}

func (f factory) histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	f.reg.MustRegister(h)
	return h
}
