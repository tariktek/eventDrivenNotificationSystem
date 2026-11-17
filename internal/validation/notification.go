package validation

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

// Request limits.
const (
	MaxScheduleAhead     = 30 * 24 * time.Hour
	scheduleClockSkew    = time.Minute // tolerated client clock drift into the past
	idempotencyKeyMaxLen = 255
	metadataMaxBytes     = 4 * 1024
	templateVarsMaxBytes = 16 * 1024
)

// Options tune environment-dependent rules.
type Options struct {
	// AllowInsecureCallbacks permits http:// callback URLs (local development).
	AllowInsecureCallbacks bool
	// AllowPrivateCallbacks permits callbacks to private/loopback hosts
	// (local development and tests).
	AllowPrivateCallbacks bool
}

// NotificationRequest is client input as received, before any parsing.
type NotificationRequest struct {
	Channel        string
	Priority       string
	Recipient      string
	Subject        string
	Content        string
	TemplateID     string
	TemplateVars   map[string]any
	ScheduledAt    *time.Time
	IdempotencyKey string
	MaxAttempts    *int
	CallbackURL    string
	Metadata       map[string]any
}

// ParsedNotification is a request that passed validation, with typed fields.
//
// When TemplateID is set, Subject and Content are empty: the template is
// rendered later and the result must go through Content before storage.
// Otherwise Content has already been checked and Segments is set.
type ParsedNotification struct {
	Channel        domain.Channel
	Priority       domain.Priority
	Recipient      string
	Subject        string
	Content        string
	Segments       int
	TemplateID     *uuid.UUID
	TemplateVars   map[string]any
	ScheduledAt    *time.Time
	IdempotencyKey string
	MaxAttempts    int
	CallbackURL    string
	Metadata       map[string]any
}

// Notification validates a single notification request. field prefixes
// error paths ("" for a single request, "items[3]" inside a batch).
func Notification(req *NotificationRequest, now time.Time, opts Options, field string) (ParsedNotification, error) {
	c := newCollector(field)
	p := checkNotification(c, req, now, opts)
	if err := c.err(); err != nil {
		return ParsedNotification{}, err
	}
	return p, nil
}

func checkNotification(c collector, req *NotificationRequest, now time.Time, opts Options) ParsedNotification {
	var p ParsedNotification
	start := c.len()

	ch, err := domain.ParseChannel(req.Channel)
	if err != nil {
		if req.Channel == "" {
			c.add("channel", "is required")
		} else {
			c.add("channel", "must be one of sms, email, push")
		}
	} else {
		p.Channel = ch
		checkRecipient(c, ch, req.Recipient)
	}
	p.Recipient = req.Recipient

	if prio, err := domain.ParsePriority(req.Priority); err != nil {
		c.add("priority", "must be one of high, normal, low")
	} else {
		p.Priority = prio
	}

	switch {
	case req.TemplateID != "" && (req.Content != "" || req.Subject != ""):
		c.add("template_id", "cannot be combined with subject or content; the template provides them")
	case req.TemplateID != "":
		id, err := uuid.Parse(req.TemplateID)
		if err != nil {
			c.add("template_id", "must be a UUID")
		} else {
			p.TemplateID = &id
		}
		checkJSONObject(c, "template_vars", req.TemplateVars, templateVarsMaxBytes)
		p.TemplateVars = req.TemplateVars
	case req.TemplateVars != nil:
		c.add("template_vars", "requires template_id")
	case p.Channel != "":
		// Content rules depend on the channel; checkContent reports a
		// missing content and any subject problem together.
		p.Segments = checkContent(c, p.Channel, req.Subject, req.Content)
		p.Subject, p.Content = req.Subject, req.Content
	case req.Content == "":
		c.add("content", "is required unless template_id is given")
	}

	if req.ScheduledAt != nil {
		at := req.ScheduledAt.UTC()
		switch {
		case at.Before(now.Add(-scheduleClockSkew)):
			c.add("scheduled_at", "must not be in the past")
		case at.After(now.Add(MaxScheduleAhead)):
			c.add("scheduled_at", "must be at most %d days ahead", int(MaxScheduleAhead.Hours()/24))
		default:
			p.ScheduledAt = &at
		}
	}

	if k := req.IdempotencyKey; k != "" {
		if len(k) > idempotencyKeyMaxLen || !isPrintableASCII(k) {
			c.add("idempotency_key", "must be 1-%d printable ASCII characters without spaces", idempotencyKeyMaxLen)
		}
		p.IdempotencyKey = k
	}

	p.MaxAttempts = domain.DefaultMaxAttempts
	if req.MaxAttempts != nil {
		if m := *req.MaxAttempts; m < domain.MinAttempts || m > domain.MaxAttemptsLimit {
			c.add("max_attempts", "must be between %d and %d", domain.MinAttempts, domain.MaxAttemptsLimit)
		} else {
			p.MaxAttempts = m
		}
	}

	if req.CallbackURL != "" {
		checkCallbackURL(c, req.CallbackURL, opts)
		p.CallbackURL = req.CallbackURL
	}

	if req.Metadata != nil {
		checkJSONObject(c, "metadata", req.Metadata, metadataMaxBytes)
	}
	p.Metadata = req.Metadata

	if c.len() > start {
		return ParsedNotification{}
	}
	return p
}

// Batch validates a batch request: its size, every item, and that no
// idempotency key repeats within the batch (which would otherwise abort the
// whole insert with a unique-constraint error).
func Batch(items []NotificationRequest, now time.Time, opts Options) ([]ParsedNotification, error) {
	c := newCollector("")
	if len(items) == 0 || len(items) > domain.MaxBatchSize {
		c.add("items", "must contain between 1 and %d notifications", domain.MaxBatchSize)
		return nil, c.err()
	}

	parsed := make([]ParsedNotification, len(items))
	firstSeen := make(map[string]int)
	for i := range items {
		item := c.at(fmt.Sprintf("items[%d]", i))
		parsed[i] = checkNotification(item, &items[i], now, opts)

		if k := items[i].IdempotencyKey; k != "" {
			if j, dup := firstSeen[k]; dup {
				item.add("idempotency_key", "duplicates items[%d].idempotency_key", j)
			} else {
				firstSeen[k] = i
			}
		}
	}
	if err := c.err(); err != nil {
		return nil, err
	}
	return parsed, nil
}

func isPrintableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// Metadata validates a free-form metadata object (e.g. batch metadata).
func Metadata(m map[string]any, field string) error {
	if m == nil {
		return nil
	}
	c := newCollector("")
	checkJSONObject(c, field, m, metadataMaxBytes)
	return c.err()
}

// TemplateVars validates template variables supplied outside a
// notification request (e.g. a preview).
func TemplateVars(m map[string]any, field string) error {
	c := newCollector("")
	checkJSONObject(c, field, m, templateVarsMaxBytes)
	return c.err()
}

// checkJSONObject validates a free-form object destined for a jsonb column.
func checkJSONObject(c collector, field string, v map[string]any, maxBytes int) {
	b, err := json.Marshal(v)
	if err != nil || len(b) > maxBytes {
		c.add(field, "must be a JSON object of at most %d bytes", maxBytes)
		return
	}
	if containsNUL(v) {
		c.add(field, "must not contain NUL characters")
	}
}
