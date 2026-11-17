// Package validation checks client input against the business rules for
// each channel. It reports every problem at once, each tied to the JSON path
// of the offending field (e.g. "items[3].recipient"), so a client can fix a
// whole request in one round trip.
package validation

import (
	"fmt"
	"strings"
)

// maxReportedErrors caps the error list so a 1000-item batch full of
// mistakes cannot produce a multi-megabyte response.
const maxReportedErrors = 100

// FieldError describes one invalid field.
type FieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// Errors is a list of field errors. It implements error; callers detect it
// with errors.As and translate it into a 422 response.
type Errors []FieldError

func (e Errors) Error() string {
	parts := make([]string, len(e))
	for i, fe := range e {
		parts[i] = fe.Field + ": " + fe.Reason
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// collector accumulates errors under a field-path prefix.
type collector struct {
	prefix    string
	errs      *Errors
	truncated *bool
}

func newCollector(prefix string) collector {
	return collector{prefix: prefix, errs: &Errors{}, truncated: new(bool)}
}

// at returns a collector for a nested path sharing the same error list.
func (c collector) at(prefix string) collector {
	return collector{prefix: c.path(prefix), errs: c.errs, truncated: c.truncated}
}

func (c collector) path(field string) string {
	switch {
	case c.prefix == "":
		return field
	case field == "":
		return c.prefix
	case strings.HasPrefix(field, "["):
		return c.prefix + field
	default:
		return c.prefix + "." + field
	}
}

func (c collector) add(field, format string, args ...any) {
	if len(*c.errs) >= maxReportedErrors {
		*c.truncated = true
		return
	}
	*c.errs = append(*c.errs, FieldError{Field: c.path(field), Reason: fmt.Sprintf(format, args...)})
}

func (c collector) len() int { return len(*c.errs) }

// err returns nil when nothing was collected, so callers can write
// `if err := v.err(); err != nil`.
func (c collector) err() error {
	if len(*c.errs) == 0 {
		return nil
	}
	errs := *c.errs
	if *c.truncated {
		errs = append(errs, FieldError{Field: "", Reason: fmt.Sprintf("too many errors; only the first %d are reported", maxReportedErrors)})
	}
	return errs
}
