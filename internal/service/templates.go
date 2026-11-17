package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/msgtemplate"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// Preview is a rendered template.
type Preview struct {
	Subject  string
	Content  string
	Segments int
}

// CreateTemplate validates, compiles and stores a template.
func (s *Service) CreateTemplate(ctx context.Context, in *validation.TemplateInput) (*domain.Template, error) {
	p, err := validation.Template(in)
	if err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate template id: %w", err)
	}
	return s.store.InsertTemplate(ctx, &domain.Template{
		ID:        id,
		Name:      p.Name,
		Channel:   p.Channel,
		Subject:   p.Subject,
		Body:      p.Body,
		Variables: p.Compiled.Variables,
		CreatedAt: s.clock(),
	})
}

// UpdateTemplate replaces a template and bumps its version.
func (s *Service) UpdateTemplate(ctx context.Context, id uuid.UUID, in *validation.TemplateInput) (*domain.Template, error) {
	p, err := validation.Template(in)
	if err != nil {
		return nil, err
	}
	return s.store.UpdateTemplate(ctx, &domain.Template{
		ID:        id,
		Name:      p.Name,
		Channel:   p.Channel,
		Subject:   p.Subject,
		Body:      p.Body,
		Variables: p.Compiled.Variables,
	})
}

// GetTemplate returns a live template.
func (s *Service) GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	return s.store.GetTemplate(ctx, id)
}

// ListTemplates returns one page of live templates.
func (s *Service) ListTemplates(ctx context.Context, channel *domain.Channel, limit int, cursor string) (postgres.Page[*domain.Template], error) {
	return s.store.ListTemplates(ctx, channel, limit, cursor)
}

// DeleteTemplate soft-deletes a template.
func (s *Service) DeleteTemplate(ctx context.Context, id uuid.UUID) error {
	return s.store.DeleteTemplate(ctx, id, s.clock())
}

// PreviewTemplate renders a template with the given variables, applying the
// same checks as sending would, without storing anything.
func (s *Service) PreviewTemplate(ctx context.Context, id uuid.UUID, vars map[string]any) (Preview, error) {
	if err := validation.TemplateVars(vars, "template_vars"); err != nil {
		return Preview{}, err
	}
	tpl, compiled, err := newTemplateCache(s.store.Repo).load(ctx, id)
	if err != nil {
		return Preview{}, err // a missing template is a 404 here, not a field error
	}
	subject, content, segments, err := render(compiled, tpl.Channel, vars, "")
	if err != nil {
		return Preview{}, err
	}
	return Preview{Subject: subject, Content: content, Segments: segments}, nil
}

// templateCache loads and compiles each template at most once per request,
// so a 1000-item batch using one template hits the database once.
type templateCache struct {
	repo    *postgres.Repo
	entries map[uuid.UUID]cachedTemplate
}

type cachedTemplate struct {
	tpl      *domain.Template
	compiled *msgtemplate.Template
	err      error
}

func newTemplateCache(repo *postgres.Repo) *templateCache {
	return &templateCache{repo: repo, entries: map[uuid.UUID]cachedTemplate{}}
}

// get returns the template, or a validation error on field when it does
// not exist (a notification referencing a missing template is bad input).
func (c *templateCache) get(ctx context.Context, id uuid.UUID, field string) (tpl *domain.Template, compiled *msgtemplate.Template, err error) {
	tpl, compiled, err = c.load(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil, validation.Field(field, "template %s does not exist", id)
	}
	return tpl, compiled, err
}

// load loads and compiles a template once per cache; domain.ErrNotFound
// passes through unchanged.
func (c *templateCache) load(ctx context.Context, id uuid.UUID) (tpl *domain.Template, compiled *msgtemplate.Template, err error) {
	e, ok := c.entries[id]
	if !ok {
		e.tpl, e.err = c.repo.GetTemplate(ctx, id)
		if e.err == nil {
			e.compiled, e.err = msgtemplate.Parse(e.tpl.Subject, e.tpl.Body)
			if e.err != nil {
				e.err = fmt.Errorf("stored template %s no longer compiles: %w", id, e.err)
			}
		}
		c.entries[id] = e
	}
	return e.tpl, e.compiled, e.err
}
