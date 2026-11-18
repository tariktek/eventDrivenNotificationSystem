package observability

import (
	"log/slog"
	"regexp"
	"strings"
)

// Personal data must not reach the logs: logs are copied to more systems,
// kept longer and read by more people than the database. Code is written
// not to log recipients or content, and the logger masks anything that
// slips through anyway (a provider error echoing "invalid number
// +905551234567", for example).

// sensitiveKeys are attribute names whose whole value is personal data.
var sensitiveKeys = map[string]bool{
	"recipient": true, "to": true, "phone": true, "email": true, "device_token": true,
}

var (
	// E.164 numbers, as the API accepts them. Requiring the "+" keeps
	// UUIDs, timestamps and counters from being mistaken for phones.
	phonePattern = regexp.MustCompile(`\+\d{7,15}`)
	emailPattern = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
)

// MaskPhone keeps enough of a number to tell numbers apart in a support
// case: "+905551234567" → "+90555***4567".
func MaskPhone(s string) string {
	if len(s) <= 8 {
		return "***"
	}
	return s[:6] + "***" + s[len(s)-4:]
}

// MaskEmail keeps the first letter and the domain: "ada@example.com" →
// "a***@example.com".
func MaskEmail(s string) string {
	at := strings.LastIndexByte(s, '@')
	if at < 1 {
		return "***"
	}
	return s[:1] + "***" + s[at:]
}

// MaskRecipient masks a recipient of any channel: phone and email keep a
// hint, anything else (push tokens) is hidden.
func MaskRecipient(s string) string {
	switch {
	case phonePattern.MatchString(s) && strings.HasPrefix(s, "+"):
		return MaskPhone(s)
	case strings.Contains(s, "@"):
		return MaskEmail(s)
	default:
		return "***"
	}
}

// ScrubPII masks phone numbers and emails inside free text.
func ScrubPII(s string) string {
	if !strings.ContainsAny(s, "+@") { // fast path: nearly every value
		return s
	}
	s = phonePattern.ReplaceAllStringFunc(s, MaskPhone)
	return emailPattern.ReplaceAllStringFunc(s, MaskEmail)
}

// maskAttr is the logger's ReplaceAttr hook.
func maskAttr(_ []string, a slog.Attr) slog.Attr {
	if sensitiveKeys[strings.ToLower(a.Key)] {
		return slog.String(a.Key, MaskRecipient(a.Value.Resolve().String()))
	}
	switch a.Value.Kind() {
	case slog.KindString:
		if s := a.Value.String(); strings.ContainsAny(s, "+@") {
			return slog.String(a.Key, ScrubPII(s))
		}
	case slog.KindAny:
		if err, ok := a.Value.Any().(error); ok {
			return slog.String(a.Key, ScrubPII(err.Error()))
		}
	default:
	}
	return a
}
