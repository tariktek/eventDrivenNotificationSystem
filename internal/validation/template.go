package validation

import (
	"errors"
	"regexp"
	"strings"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/msgtemplate"
)

// Template limits.
const (
	templateNameMaxLen  = 100
	templateBodyMaxSize = 64 * 1024
	templateSubjMaxSize = 1024
)

var templateName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// TemplateInput is a template create/update request as received.
type TemplateInput struct {
	Name    string
	Channel string
	Subject string
	Body    string
}

// ParsedTemplate is a template that passed validation and compiled.
type ParsedTemplate struct {
	Name     string
	Channel  domain.Channel
	Subject  string
	Body     string
	Compiled *msgtemplate.Template
}

// Template validates and compiles a template definition.
func Template(in *TemplateInput) (ParsedTemplate, error) {
	c := newCollector("")

	switch {
	case in.Name == "":
		c.add("name", "is required")
	case len(in.Name) > templateNameMaxLen || !templateName.MatchString(in.Name):
		c.add("name", "must be 1-%d lowercase letters, digits, '-' or '_', starting with a letter or digit", templateNameMaxLen)
	}

	ch, err := domain.ParseChannel(in.Channel)
	if err != nil {
		c.add("channel", "must be one of sms, email, push")
	}

	if checkText(c, "subject", in.Subject) {
		switch {
		case len(in.Subject) > templateSubjMaxSize:
			c.add("subject", "must be at most %d bytes", templateSubjMaxSize)
		case ch == domain.ChannelEmail && in.Subject == "":
			c.add("subject", "is required for email templates")
		case ch == domain.ChannelSMS && in.Subject != "":
			c.add("subject", "is not supported for sms")
		case strings.ContainsAny(in.Subject, "\r\n"):
			c.add("subject", "must not contain line breaks")
		}
	}
	if checkText(c, "body", in.Body) {
		switch {
		case in.Body == "":
			c.add("body", "is required")
		case len(in.Body) > templateBodyMaxSize:
			c.add("body", "must be at most %d bytes", templateBodyMaxSize)
		}
	}
	if err := c.err(); err != nil {
		return ParsedTemplate{}, err
	}

	compiled, err := msgtemplate.Parse(in.Subject, in.Body)
	if err != nil {
		var pe *msgtemplate.ParseError
		if errors.As(err, &pe) {
			c.add(pe.Part, "%s", pe.Err.Error())
		} else {
			c.add("body", "%s", err.Error())
		}
		return ParsedTemplate{}, c.err()
	}

	return ParsedTemplate{Name: in.Name, Channel: ch, Subject: in.Subject, Body: in.Body, Compiled: compiled}, nil
}
