package middleware

import (
	"bytes"
	"net/http"
)

// ErrorWriter renders an error as an API error response.
type ErrorWriter func(w http.ResponseWriter, r *http.Request, err error)

// statusWriter records the status code and size of a response, and
// optionally a copy of its body, while passing everything through.
type statusWriter struct {
	http.ResponseWriter
	status  int
	bytes   int
	capture *bytes.Buffer // nil unless the body must be kept
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
	if w.capture != nil {
		w.capture.Write(b[:n])
	}
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
