// Package mockprovider is a stand-in for the external notification
// provider, used for local runs, end-to-end tests and load tests (the free
// webhook.site endpoint caps requests and expires).
//
// It speaks the same contract as the real provider and adds two things:
//
//   - fault injection: configurable rates of 503, 429 and 400 responses
//     and latency, changeable at runtime (PUT /admin/config), to exercise
//     retries and the circuit breaker;
//   - independent measurement (GET /admin/stats): the highest number of
//     messages received in any one second per channel, and how many
//     deliveries repeated an already-accepted notification. These verify
//     the rate limit and delivery guarantees from the receiving side.
package mockprovider

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Config controls fault injection. Rates are probabilities in [0, 1],
// applied in the order permanent, rate limit, failure.
type Config struct {
	FailureRate   float64  `json:"failure_rate"`    // → 503
	RateLimitRate float64  `json:"rate_limit_rate"` // → 429, Retry-After: 1
	PermanentRate float64  `json:"permanent_rate"`  // → 400
	Latency       Duration `json:"latency"`         // e.g. "20ms"
	LatencyJitter Duration `json:"latency_jitter"`  // e.g. "30ms"
}

// Duration is a time.Duration that reads and writes JSON as "20ms" rather
// than integer nanoseconds, so the admin API is usable by hand.
type Duration time.Duration

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"20ms\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Stats is what the mock has observed since start or the last reset.
type Stats struct {
	Received      int            `json:"received"`
	Accepted      int            `json:"accepted"`
	ByStatus      map[string]int `json:"by_status"`
	ByChannel     map[string]int `json:"by_channel"`
	MaxPerSecond  map[string]int `json:"max_per_second"`
	Duplicates    int            `json:"duplicates"` // accepted again after an earlier accept
	Config        Config         `json:"config"`
	LastMessageAt *time.Time     `json:"last_message_at,omitempty"`
}

// Server is the mock provider.
type Server struct {
	now func() time.Time

	mu         sync.Mutex
	cfg        Config
	stats      Stats
	secCounts  map[string]map[int64]int // channel → unix second → count
	acceptedID map[string]bool          // X-Idempotency-Key values accepted so far
}

// New returns a mock provider. now may be nil (time.Now).
func New(cfg Config, now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}
	s := &Server{now: now, cfg: cfg}
	s.reset()
	return s
}

func (s *Server) reset() {
	s.stats = Stats{ByStatus: map[string]int{}, ByChannel: map[string]int{}, MaxPerSecond: map[string]int{}}
	s.secCounts = map[string]map[int64]int{}
	s.acceptedID = map[string]bool{}
}

// Handler exposes the provider and admin endpoints.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /send", s.send)
	mux.HandleFunc("GET /admin/stats", s.getStats)
	mux.HandleFunc("PUT /admin/config", s.putConfig)
	mux.HandleFunc("POST /admin/reset", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		s.reset()
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

type sendRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	var req sendRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil ||
		req.To == "" || req.Channel == "" || req.Content == "" {
		s.record("", "", http.StatusBadRequest)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "to, channel and content are required"})
		return
	}

	s.mu.Lock()
	cfg := s.cfg
	s.mu.Unlock()

	if d := time.Duration(cfg.Latency) + jitter(time.Duration(cfg.LatencyJitter)); d > 0 {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
	}

	key := r.Header.Get("X-Idempotency-Key")
	switch roll := rand.Float64(); { //nolint:gosec // fault injection, not security
	case roll < cfg.PermanentRate:
		s.record(req.Channel, key, http.StatusBadRequest)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "recipient rejected (injected)"})
	case roll < cfg.PermanentRate+cfg.RateLimitRate:
		s.record(req.Channel, key, http.StatusTooManyRequests)
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited (injected)"})
	case roll < cfg.PermanentRate+cfg.RateLimitRate+cfg.FailureRate:
		s.record(req.Channel, key, http.StatusServiceUnavailable)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable (injected)"})
	default:
		s.record(req.Channel, key, http.StatusAccepted)
		writeJSON(w, http.StatusAccepted, map[string]string{
			"messageId": uuid.NewString(),
			"status":    "accepted",
			"timestamp": s.now().UTC().Format(time.RFC3339Nano),
		})
	}
}

// record counts a request; channel is "" for malformed ones.
func (s *Server) record(channel, key string, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.stats.Received++
	s.stats.ByStatus[strconv.Itoa(status)]++
	s.stats.LastMessageAt = &now
	if channel == "" {
		return
	}
	s.stats.ByChannel[channel]++

	sec := now.Unix()
	if s.secCounts[channel] == nil {
		s.secCounts[channel] = map[int64]int{}
	}
	s.secCounts[channel][sec]++
	if c := s.secCounts[channel][sec]; c > s.stats.MaxPerSecond[channel] {
		s.stats.MaxPerSecond[channel] = c
	}
	delete(s.secCounts[channel], sec-5) // keep memory bounded

	if status == http.StatusAccepted {
		s.stats.Accepted++
		if key != "" {
			if s.acceptedID[key] {
				s.stats.Duplicates++
			}
			s.acceptedID[key] = true
		}
	}
}

func (s *Server) getStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	st := s.stats
	st.Config = s.cfg
	b, err := json.Marshal(st) // marshal under the lock: the maps are shared
	s.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (s *Server) putConfig(w http.ResponseWriter, r *http.Request) {
	var cfg Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, cfg)
}

func jitter(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(limit))) //nolint:gosec // fault injection, not security
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
