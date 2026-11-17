// Package msgtemplate renders message templates with variable substitution,
// e.g. "Hello {{.name}}, your order {{.order_id}} has shipped."
//
// Templates are written by API clients, so they are treated as untrusted
// code and checked when parsed:
//   - only allowlisted functions may be called. text/template's builtins
//     printf, call, index, slice, ... are rejected: printf alone can allocate
//     gigabytes ({{printf "%0999999999d" 1}}) before any output limit applies;
//   - {{define}} and {{template}} are rejected (no recursion);
//   - {{range}} may not be nested and may not iterate over a literal number,
//     so the work a template can do is bounded by the size of its variables
//     (which the API caps), even when the loop body writes nothing;
//   - rendered output is capped in size;
//   - a missing or null variable is an error, never a silent "<no value>".
package msgtemplate

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"text/template"
	"text/template/parse"
)

// MaxOutputBytes caps the rendered size of a subject or body. Channel limits
// are lower; this only guards against runaway expansion.
const MaxOutputBytes = 256 * 1024

// funcs are the helpers added for template authors.
var funcs = template.FuncMap{
	"upper": strings.ToUpper,
	"lower": strings.ToLower,
	"trim":  strings.TrimSpace,
	// default returns fallback when value is empty, e.g. {{default "there" .nickname}}.
	// The variable itself must still be supplied (it may be "").
	"default": func(fallback string, value any) any {
		if value == "" {
			return fallback
		}
		return value
	},
}

// allowedFuncs are the only identifiers a template may call: the helpers
// above plus side-effect-free, constant-cost builtins.
var allowedFuncs = func() map[string]bool {
	m := map[string]bool{
		"and": true, "or": true, "not": true, "len": true,
		"eq": true, "ne": true, "lt": true, "le": true, "gt": true, "ge": true,
	}
	for name := range funcs {
		m[name] = true
	}
	return m
}()

// Template is a parsed subject/body pair.
type Template struct {
	subject *template.Template // nil when there is no subject
	body    *template.Template

	// Variables lists the top-level variables referenced, sorted. They must
	// all be supplied, non-null, when rendering.
	Variables []string
}

// MissingVariablesError lists variables the template needs but were not
// supplied or were null.
type MissingVariablesError struct {
	Names []string
}

func (e *MissingVariablesError) Error() string {
	return "missing or null template variables: " + strings.Join(e.Names, ", ")
}

// ErrOutputTooLarge is returned when rendering exceeds MaxOutputBytes.
var ErrOutputTooLarge = fmt.Errorf("rendered template exceeds %d bytes", MaxOutputBytes)

// Parse compiles and checks a template. subject may be empty.
func Parse(subject, body string) (*Template, error) {
	t := &Template{}
	vars := map[string]bool{}

	if subject != "" {
		st, err := compile("subject", subject, vars)
		if err != nil {
			return nil, err
		}
		t.subject = st
	}
	bt, err := compile("body", body, vars)
	if err != nil {
		return nil, err
	}
	t.body = bt

	for v := range vars {
		t.Variables = append(t.Variables, v)
	}
	slices.Sort(t.Variables)
	return t, nil
}

func compile(name, text string, vars map[string]bool) (*template.Template, error) {
	t, err := template.New(name).Funcs(funcs).Option("missingkey=error").Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parse %s template: %w", name, err)
	}
	// Each {{define}} adds an associated template.
	if len(t.Templates()) > 1 {
		return nil, fmt.Errorf("parse %s template: {{define}} is not allowed", name)
	}
	w := walker{vars: vars}
	if err := w.walk(t.Root, scope{}); err != nil {
		return nil, fmt.Errorf("parse %s template: %w", name, err)
	}
	return t, nil
}

// scope tracks where in the tree the walker is.
type scope struct {
	dotRebound bool // inside {{range}}/{{with}}: "." is no longer the variables map
	inRange    bool
}

type walker struct {
	vars map[string]bool
}

// walk enforces the rules in the package comment and collects top-level
// variable names, both from ".name" and from "$.name".
func (w walker) walk(node parse.Node, s scope) error {
	switch n := node.(type) {
	case nil:
		return nil
	case *parse.ListNode:
		if n == nil {
			return nil
		}
		for _, c := range n.Nodes {
			if err := w.walk(c, s); err != nil {
				return err
			}
		}
	case *parse.TemplateNode:
		return errors.New("{{template}} is not allowed")
	case *parse.ActionNode:
		return w.walk(n.Pipe, s)
	case *parse.PipeNode:
		if n == nil {
			return nil
		}
		for _, cmd := range n.Cmds {
			for _, arg := range cmd.Args {
				if err := w.walk(arg, s); err != nil {
					return err
				}
			}
		}
	case *parse.ChainNode:
		return w.walk(n.Node, s)
	case *parse.IdentifierNode:
		if !allowedFuncs[n.Ident] {
			return fmt.Errorf("function %q is not allowed", n.Ident)
		}
	case *parse.FieldNode:
		if !s.dotRebound && len(n.Ident) > 0 {
			w.vars[n.Ident[0]] = true
		}
	case *parse.VariableNode:
		// $ always refers to the variables map, whatever the scope.
		if len(n.Ident) > 1 && n.Ident[0] == "$" {
			w.vars[n.Ident[1]] = true
		}
	case *parse.IfNode:
		return w.branch(&n.BranchNode, s, s)
	case *parse.WithNode:
		return w.branch(&n.BranchNode, s, scope{dotRebound: true, inRange: s.inRange})
	case *parse.RangeNode:
		if s.inRange {
			return errors.New("nested {{range}} is not allowed")
		}
		if rangesOverLiteral(n.Pipe) {
			return errors.New("{{range}} over a literal number is not allowed")
		}
		return w.branch(&n.BranchNode, s, scope{dotRebound: true, inRange: true})
	}
	return nil
}

func (w walker) branch(b *parse.BranchNode, outer, body scope) error {
	if err := w.walk(b.Pipe, outer); err != nil {
		return err
	}
	if err := w.walk(b.List, body); err != nil {
		return err
	}
	// The else branch of range/with runs with the outer dot.
	return w.walk(b.ElseList, scope{dotRebound: outer.dotRebound, inRange: body.inRange})
}

func rangesOverLiteral(p *parse.PipeNode) bool {
	if p == nil {
		return false
	}
	for _, cmd := range p.Cmds {
		for _, arg := range cmd.Args {
			if _, ok := arg.(*parse.NumberNode); ok {
				return true
			}
		}
	}
	return false
}

// Render executes the template. Every variable in t.Variables must be
// present and non-null in vars; all offending names are reported together.
func (t *Template) Render(vars map[string]any) (subject, body string, err error) {
	var missing []string
	for _, v := range t.Variables {
		if val, ok := vars[v]; !ok || val == nil {
			missing = append(missing, v)
		}
	}
	if len(missing) > 0 {
		return "", "", &MissingVariablesError{Names: missing}
	}
	if vars == nil {
		vars = map[string]any{}
	}

	if t.subject != nil {
		if subject, err = execute(t.subject, vars); err != nil {
			return "", "", err
		}
	}
	if body, err = execute(t.body, vars); err != nil {
		return "", "", err
	}
	return subject, body, nil
}

func execute(t *template.Template, vars map[string]any) (string, error) {
	w := &limitedWriter{limit: MaxOutputBytes}
	if err := t.Execute(w, vars); err != nil {
		if errors.Is(err, ErrOutputTooLarge) {
			return "", ErrOutputTooLarge
		}
		return "", fmt.Errorf("render %s template: %w", t.Name(), err)
	}
	return w.buf.String(), nil
}

// limitedWriter fails writes beyond limit, which aborts template execution.
// The buffer is a named field, not embedded, so no promoted method (such as
// WriteString) can bypass the check.
type limitedWriter struct {
	buf   bytes.Buffer
	limit int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.buf.Len()+len(p) > w.limit {
		return 0, ErrOutputTooLarge
	}
	return w.buf.Write(p)
}
