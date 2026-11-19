package middleware

import (
	"bytes"
	"net/http"
)

// ErrorWriter renders an error as an API error response.
type ErrorWriter func(w http.ResponseWriter, r *http.Request, err error)

// statusWriter records the status code and size of a response while
// passing everything through.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// Status is the response status; 200 if the handler wrote nothing explicit.
func (w *statusWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// responseBuffer holds a whole response in memory instead of sending it,
// so it can be stored before the client sees any of it. Responses that
// pass through it are small JSON documents (the routes that take an
// Idempotency-Key).
type responseBuffer struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newResponseBuffer() *responseBuffer { return &responseBuffer{header: http.Header{}} }

func (b *responseBuffer) Header() http.Header { return b.header }

func (b *responseBuffer) WriteHeader(code int) {
	if b.status == 0 {
		b.status = code
	}
}

func (b *responseBuffer) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.body.Write(p)
}

// Status is the response status; 200 if the handler wrote nothing explicit.
func (b *responseBuffer) Status() int {
	if b.status == 0 {
		return http.StatusOK
	}
	return b.status
}

// writeTo sends the buffered response.
func (b *responseBuffer) writeTo(w http.ResponseWriter) {
	for k, v := range b.header {
		w.Header()[k] = v
	}
	w.WriteHeader(b.Status())
	_, _ = w.Write(b.body.Bytes())
}
