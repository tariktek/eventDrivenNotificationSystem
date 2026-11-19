// Package health implements liveness and readiness probes.
//
// Liveness answers "is the process alive?" and never touches dependencies:
// restarting a process because its database is down only makes an outage
// worse. Readiness answers "should this instance receive work?" and checks
// every dependency. Dependencies are either critical (the instance cannot do
// its job without them, e.g. PostgreSQL) or non-critical (the instance keeps
// working in a reduced mode, e.g. without Redis the API still accepts work
// while deliveries and live events wait), which reports "degraded" instead
// of "down".
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Check probes one dependency and returns nil when it is healthy.
type Check func(ctx context.Context) error

// Status is the health state of a component or of the whole instance.
type Status string

// Possible statuses.
const (
	StatusOK       Status = "ok"
	StatusDegraded Status = "degraded"
	StatusDown     Status = "down"
)

// ComponentResult is the outcome of a single check.
type ComponentResult struct {
	Status    Status `json:"status"`
	Critical  bool   `json:"critical"`
	LatencyMS int64  `json:"latency_ms"`
	Error     string `json:"error,omitempty"`
}

// Report is the readiness response body.
type Report struct {
	Status Status                     `json:"status"`
	Reason string                     `json:"reason,omitempty"`
	Checks map[string]ComponentResult `json:"checks,omitempty"`
}

type component struct {
	name     string
	critical bool
	check    Check
}

// Registry holds the readiness checks of one process.
type Registry struct {
	timeout    time.Duration
	components []component
	draining   atomic.Bool
}

// NewRegistry returns a registry whose checks each get the given timeout.
func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{timeout: timeout}
}

// Register adds a named dependency check. It must be called before the
// registry starts serving requests.
func (r *Registry) Register(name string, critical bool, check Check) {
	r.components = append(r.components, component{name: name, critical: critical, check: check})
}

// SetDraining marks the instance as shutting down. From then on readiness
// reports "down" so load balancers stop routing new traffic to it while
// in-flight requests finish.
func (r *Registry) SetDraining() {
	r.draining.Store(true)
}

// Check runs all checks concurrently and aggregates the result.
func (r *Registry) Check(ctx context.Context) Report {
	if r.draining.Load() {
		return Report{Status: StatusDown, Reason: "shutting down"}
	}

	results := make(map[string]ComponentResult, len(r.components))
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, c := range r.components {
		wg.Go(func() {
			res := r.run(ctx, c)
			mu.Lock()
			results[c.name] = res
			mu.Unlock()
		})
	}
	wg.Wait()

	overall := StatusOK
	for _, res := range results {
		switch {
		case res.Status == StatusDown && res.Critical:
			overall = StatusDown
		case res.Status == StatusDown && overall == StatusOK:
			overall = StatusDegraded
		}
	}
	return Report{Status: overall, Checks: results}
}

func (r *Registry) run(ctx context.Context, c component) ComponentResult {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	start := time.Now()
	err := c.check(ctx)
	res := ComponentResult{
		Status:    StatusOK,
		Critical:  c.critical,
		LatencyMS: time.Since(start).Milliseconds(),
	}
	if err != nil {
		res.Status = StatusDown
		res.Error = err.Error()
	}
	return res
}

// LivenessHandler always answers 200 while the process can serve HTTP.
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, Report{Status: StatusOK})
	}
}

// ReadinessHandler answers 200 when the instance can take work (ok or
// degraded) and 503 when a critical dependency is down or it is draining.
func (r *Registry) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		report := r.Check(req.Context())
		code := http.StatusOK
		if report.Status == StatusDown {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, report)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
