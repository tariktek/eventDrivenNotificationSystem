package mockprovider

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/callback"
)

// CallbackReceiver plays a client's webhook endpoint: it verifies each
// status callback's signature exactly as a client would (callback.Verify)
// and keeps what it received for inspection, so the callback flow can be
// demonstrated and tested end to end with docker compose.
type CallbackReceiver struct {
	secret []byte
	now    func() time.Time

	mu       sync.Mutex
	received []ReceivedCallback
	invalid  int
}

// ReceivedCallback is one verified callback.
type ReceivedCallback struct {
	EventID    string          `json:"event_id"`
	Type       string          `json:"type"`
	ReceivedAt time.Time       `json:"received_at"`
	Body       json.RawMessage `json:"body"`
}

// NewCallbackReceiver returns a receiver that verifies with secret.
func NewCallbackReceiver(secret []byte, now func() time.Time) *CallbackReceiver {
	if now == nil {
		now = time.Now
	}
	return &CallbackReceiver{secret: secret, now: now}
}

// Register adds POST /callbacks and GET /admin/callbacks to mux.
func (c *CallbackReceiver) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /callbacks", c.receive)
	mux.HandleFunc("GET /admin/callbacks", c.list)
}

func (c *CallbackReceiver) receive(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	err = callback.Verify(c.secret, r.Header.Get(callback.HeaderSignature), r.Header.Get(callback.HeaderTimestamp),
		body, c.now(), 5*time.Minute)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil || !json.Valid(body) {
		c.invalid++
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	c.received = append(c.received, ReceivedCallback{
		EventID: r.Header.Get(callback.HeaderEventID), Type: r.Header.Get(callback.HeaderEventType),
		ReceivedAt: c.now(), Body: body,
	})
	if len(c.received) > 1000 { // keep memory bounded under load tests
		c.received = c.received[len(c.received)-1000:]
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *CallbackReceiver) list(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"valid": len(c.received), "invalid": c.invalid, "callbacks": c.received})
}
