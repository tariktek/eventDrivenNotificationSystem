package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

// maxResponseBytes caps how much of a response body is read: a
// misbehaving provider must not be able to exhaust worker memory.
const maxResponseBytes = 1 << 20

// HTTPConfig configures HTTPClient.
type HTTPConfig struct {
	// URLs maps each channel to its provider endpoint.
	URLs map[domain.Channel]string
	// Timeout bounds a whole attempt: connect, send, wait, read.
	Timeout time.Duration
}

// HTTPClient sends messages as JSON POSTs:
//
//	POST <url>  {"to": "+905551234567", "channel": "sms", "content": "..."}
//	→ 202       {"messageId": "...", "status": "accepted", "timestamp": "..."}
type HTTPClient struct {
	cfg  HTTPConfig
	http *http.Client
	now  func() time.Time
}

// NewHTTPClient builds a client with connection pooling sized for the
// worker's concurrency.
func NewHTTPClient(cfg HTTPConfig) *HTTPClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 200
	transport.MaxIdleConnsPerHost = 100 // one provider host, many concurrent sends
	transport.IdleConnTimeout = 90 * time.Second

	return &HTTPClient{
		cfg: cfg,
		http: &http.Client{
			// A client span per call, and the traceparent header, so the
			// provider's side can join the trace.
			Transport: otelhttp.NewTransport(transport),
			Timeout:   cfg.Timeout,
			// A redirect from a delivery endpoint means misconfiguration;
			// following it could resend the message somewhere unexpected.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now: time.Now,
	}
}

type sendRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Subject string `json:"subject,omitempty"`
	Content string `json:"content"`
}

type sendResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	// Timestamp is informational; it is not validated, so a provider that
	// formats it differently does not turn successful sends into failures.
	Timestamp string `json:"timestamp"`
}

// Send implements Sender.
func (c *HTTPClient) Send(ctx context.Context, msg *Message) Result {
	start := c.now()
	res := c.send(ctx, msg)
	res.Duration = c.now().Sub(start)
	return res
}

func (c *HTTPClient) send(ctx context.Context, msg *Message) Result {
	url, ok := c.cfg.URLs[msg.Channel]
	if !ok {
		return permanent(0, "no_provider", fmt.Sprintf("no provider URL configured for channel %q", msg.Channel))
	}
	body, err := json.Marshal(sendRequest{To: msg.To, Channel: string(msg.Channel), Subject: msg.Subject, Content: msg.Content})
	if err != nil {
		return permanent(0, "encode_error", err.Error())
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return permanent(0, "bad_request", err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// Lets a provider that supports it drop a resend of the same
	// notification: our at-least-once delivery can resend after a crash.
	req.Header.Set("X-Idempotency-Key", msg.NotificationID.String())
	if msg.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", msg.CorrelationID)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return classifyTransportError(err)
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reused

	return classifyResponse(resp, raw, readErr)
}

// maxMessageIDBytes bounds a stored provider message id.
const maxMessageIDBytes = 255

// unexpectedResponse is the error code noted on a success whose response
// did not follow the provider contract.
const unexpectedResponse = "unexpected_provider_response"

func classifyResponse(resp *http.Response, body []byte, readErr error) Result {
	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return accepted(code, body, readErr)

	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests, code >= 500:
		return transient(code, "http_"+strconv.Itoa(code), snippet(body), retryAfter(resp.Header.Get("Retry-After"), time.Now()))

	case code >= 300 && code < 400:
		return permanent(code, "unexpected_redirect", "provider redirected to "+resp.Header.Get("Location"))

	default: // remaining 4xx: the request itself is wrong
		return permanent(code, "http_"+strconv.Itoa(code), snippet(body))
	}
}

// accepted classifies a 2xx response. The status code is what says the
// provider took the message; the body only adds its message id. So a 2xx
// is always a success: treating an unexpected body as an error would send
// the message again on every retry (the recipient gets it several times)
// and count a healthy provider against its circuit breaker. A body that is
// not {"messageId": ..., "status": "accepted"} (unreadable, not JSON,
// another status, an id that cannot be stored) is kept on the attempt as
// a note in ErrorCode/Error; the worker logs it.
func accepted(code int, body []byte, readErr error) Result {
	res := Result{Outcome: domain.OutcomeSuccess, StatusCode: code}
	note := func(msg string) Result {
		res.ErrorCode, res.Error = unexpectedResponse, msg
		return res
	}
	if readErr != nil {
		return note("accepted, but reading the response failed: " + readErr.Error())
	}
	var r sendResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return note("accepted, but the response is not the expected JSON: " + snippet(body))
	}
	switch id := r.MessageID; {
	case id == "":
		return note("accepted without a messageId")
	case !storableID(id):
		// PostgreSQL text rejects NUL and invalid UTF-8; storing such an
		// id would roll back the whole outcome and the message would be
		// sent again. Keep the delivery, drop the id.
		return note(fmt.Sprintf("accepted, but the messageId cannot be stored (%d bytes, NUL, or invalid UTF-8): %s", len(id), snippet([]byte(id))))
	default:
		res.MessageID = id
	}
	if r.Status != "accepted" {
		return note(fmt.Sprintf("accepted with status %q instead of \"accepted\"", snippet([]byte(r.Status))))
	}
	return res
}

// storableID reports whether a provider message id fits a text column.
func storableID(id string) bool {
	return len(id) <= maxMessageIDBytes && utf8.ValidString(id) && !strings.ContainsRune(id, 0)
}

func classifyTransportError(err error) Result {
	var netErr net.Error
	switch {
	case errors.Is(err, context.Canceled):
		// Shutdown mid-send: we cannot know whether the provider got it.
		return transient(0, "cancelled", err.Error(), 0)
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return transient(0, "timeout", err.Error(), 0)
	default:
		return transient(0, "connection_error", err.Error(), 0)
	}
}

// retryAfter parses a Retry-After header: delay-seconds or an HTTP date.
func retryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func transient(code int, errCode, msg string, after time.Duration) Result {
	return Result{Outcome: domain.OutcomeTransientError, StatusCode: code, ErrorCode: errCode, Error: msg, RetryAfter: after}
}

func permanent(code int, errCode, msg string) Result {
	return Result{Outcome: domain.OutcomePermanentError, StatusCode: code, ErrorCode: errCode, Error: msg}
}

// snippet makes a provider response body safe to store and log: short,
// valid UTF-8 (truncation may split a multi-byte character) and free of NUL
// bytes, which PostgreSQL text columns reject. Without this, an odd error
// page from the provider could make recording the attempt itself fail.
func snippet(b []byte) string {
	const limit = 512
	truncated := len(b) > limit
	if truncated {
		b = b[:limit]
	}
	s := strings.ToValidUTF8(strings.ReplaceAll(string(b), "\x00", ""), "\uFFFD")
	if truncated {
		s += "…"
	}
	return s
}
