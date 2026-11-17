package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// InsertTemplate stores a new template. It returns domain.ErrAlreadyExists
// if a live template already uses the name.
func (r *Repo) InsertTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error) {
	vars, err := json.Marshal(nonNilStrings(t.Variables))
	if err != nil {
		return nil, fmt.Errorf("encode template variables: %w", err)
	}
	row, err := r.q.InsertTemplate(ctx, sqlcgen.InsertTemplateParams{
		ID:        t.ID,
		Name:      t.Name,
		Channel:   string(t.Channel),
		Subject:   nilIfZero(t.Subject),
		Body:      t.Body,
		Variables: vars,
		CreatedAt: t.CreatedAt,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("template name %q: %w", t.Name, domain.ErrAlreadyExists)
		}
		return nil, fmt.Errorf("insert template: %w", err)
	}
	return toTemplate(&row)
}

// GetTemplate returns a live (not deleted) template.
func (r *Repo) GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	row, err := r.q.GetTemplate(ctx, id)
	if err != nil {
		return nil, notFound(err, "get template")
	}
	return toTemplate(&row)
}

// UpdateTemplate replaces a live template's content and bumps its version.
func (r *Repo) UpdateTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error) {
	vars, err := json.Marshal(nonNilStrings(t.Variables))
	if err != nil {
		return nil, fmt.Errorf("encode template variables: %w", err)
	}
	row, err := r.q.UpdateTemplate(ctx, sqlcgen.UpdateTemplateParams{
		ID:        t.ID,
		Name:      t.Name,
		Channel:   string(t.Channel),
		Subject:   nilIfZero(t.Subject),
		Body:      t.Body,
		Variables: vars,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("template name %q: %w", t.Name, domain.ErrAlreadyExists)
		}
		return nil, notFound(err, "update template")
	}
	return toTemplate(&row)
}

// DeleteTemplate soft-deletes a template so existing notifications keep a
// valid reference.
func (r *Repo) DeleteTemplate(ctx context.Context, id uuid.UUID, now time.Time) error {
	n, err := r.q.SoftDeleteTemplate(ctx, sqlcgen.SoftDeleteTemplateParams{ID: id, Now: &now})
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("delete template: %w", domain.ErrNotFound)
	}
	return nil
}

// ListTemplates returns one page of live templates, newest first.
func (r *Repo) ListTemplates(ctx context.Context, channel *domain.Channel, limit int, cursorToken string) (Page[*domain.Template], error) {
	after, err := decodeCursor(cursorToken)
	if err != nil {
		return Page[*domain.Template]{}, err
	}
	limit = clampLimit(limit)

	params := sqlcgen.ListTemplatesParams{RowLimit: int32(limit + 1)} //nolint:gosec // clampLimit caps limit at MaxPageSize
	if channel != nil {
		params.Channel = new(string(*channel))
	}
	if after != nil {
		params.AfterCreatedAt, params.AfterID = &after.CreatedAt, &after.ID
	}
	rows, err := r.q.ListTemplates(ctx, params)
	if err != nil {
		return Page[*domain.Template]{}, fmt.Errorf("list templates: %w", err)
	}
	items := make([]*domain.Template, len(rows))
	for i := range rows {
		if items[i], err = toTemplate(&rows[i]); err != nil {
			return Page[*domain.Template]{}, err
		}
	}
	return paginate(items, limit, func(t *domain.Template) cursor {
		return cursor{CreatedAt: t.CreatedAt, ID: t.ID}
	}), nil
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
