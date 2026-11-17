//go:build integration

package router_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apispec "github.com/tariktek/eventDrivenNotificationSystem/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/handler"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/router"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/service"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/testutil/pgtest"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

const (
	maxBody = 64 * 1024 // small, so the 413 test stays cheap
	apiKey  = "integration-test-key"
)

var (
	pool       *pgxpool.Pool
	app        http.Handler
	specRouter routers.Router
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := pgtest.Start(ctx)
	if err == nil {
		err = pgtest.MigrateUp(pg.DSN)
	}
	if err == nil {
		pool, err = pgxpool.New(ctx, pg.DSN)
	}
	var doc *openapi3.T
	if err == nil {
		doc, err = openapi3.NewLoader().LoadFromData(apispec.YAML)
	}
	if err == nil {
		specRouter, err = gorillamux.NewRouter(doc)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	store := postgres.NewStore(pool)
	svc := service.New(store, time.Now, validation.Options{})
	meta := func(ctx context.Context) service.RequestMeta {
		return service.RequestMeta{CorrelationID: observability.CorrelationID(ctx)}
	}
	app = router.New(router.Deps{
		Health:         health.NewRegistry(time.Second),
		API:            handler.New(svc, meta),
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes:   maxBody,
		APIKeys:        []string{apiKey},
		Idempotency:    store.Idempotency(),
		IdempotencyTTL: time.Hour,
		Now:            time.Now,
	})

	code := m.Run()
	pool.Close()
	_ = pg.Terminate()
	os.Exit(code)
}

func reset(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE notifications, batches, templates, notification_attempts, idempotency_keys CASCADE`)
	require.NoError(t, err)
}

type response struct {
	Code   int
	Body   []byte
	Header http.Header
}

func (r response) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(r.Body, &m), "body: %s", r.Body)
	return m
}

// call sends a request through the real router and checks the response
// against the OpenAPI spec: the status must be documented for the operation
// and the body must match its schema. That is the contract test.
func call(t *testing.T, method, path string, body any) response {
	t.Helper()
	return send(t, method, path, body, map[string]string{"X-API-Key": apiKey}, true)
}

// callWith is call with explicit headers (no API key unless given).
func callWith(t *testing.T, method, path string, body any, headers map[string]string) response {
	t.Helper()
	return send(t, method, path, body, headers, true)
}

// callUndocumented is call for paths deliberately absent from the spec.
func callUndocumented(t *testing.T, method, path string) response {
	t.Helper()
	return send(t, method, path, nil, map[string]string{"X-API-Key": apiKey}, false)
}

func send(t *testing.T, method, path string, body any, headers map[string]string, documented bool) response {
	t.Helper()
	var raw []byte
	switch b := body.(type) {
	case nil:
	case string:
		raw = []byte(b)
	default:
		var err error
		raw, err = json.Marshal(b)
		require.NoError(t, err)
	}

	url := "http://localhost:8080" + path
	req := httptest.NewRequestWithContext(t.Context(), method, url, bytes.NewReader(raw))
	if raw != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	res := response{Code: rec.Code, Body: rec.Body.Bytes(), Header: rec.Header()}

	// Validate the response against the spec when the path is in it.
	vreq := httptest.NewRequestWithContext(t.Context(), method, url, http.NoBody)
	if !documented {
		return res
	}
	// Failing here (rather than skipping) guarantees the contract check
	// cannot silently stop running, e.g. after a server URL change.
	route, params, err := specRouter.FindRoute(vreq)
	require.NoError(t, err, "%s %s is not in the spec", method, path)
	in := &openapi3filter.RequestValidationInput{
		Request: vreq, PathParams: params, Route: route,
		Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
	}
	err = openapi3filter.ValidateResponse(t.Context(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: in,
		Status:                 rec.Code,
		Header:                 rec.Header(),
		Body:                   io.NopCloser(bytes.NewReader(res.Body)),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true},
	})
	require.NoError(t, err, "%s %s -> %d does not match the spec; body: %s", method, path, rec.Code, res.Body)
	return res
}

func sms(extra ...map[string]any) map[string]any {
	m := map[string]any{"channel": "sms", "recipient": "+905551234567", "content": "Your code is 4821"}
	for _, e := range extra {
		for k, v := range e {
			m[k] = v
		}
	}
	return m
}

func errorCode(t *testing.T, r response) string {
	t.Helper()
	return r.json(t)["error"].(map[string]any)["code"].(string)
}

func errorFields(t *testing.T, r response) []string {
	t.Helper()
	details, _ := r.json(t)["error"].(map[string]any)["details"].([]any)
	var out []string
	for _, d := range details {
		out = append(out, d.(map[string]any)["field"].(string))
	}
	return out
}

// ---- notifications ---------------------------------------------------------

func TestCreateAndGetNotification(t *testing.T) {
	reset(t)
	res := call(t, http.MethodPost, "/api/v1/notifications", sms(map[string]any{"priority": "high", "metadata": map[string]any{"campaign": "otp"}}))
	require.Equal(t, http.StatusCreated, res.Code, "%s", res.Body)

	created := res.json(t)
	assert.Equal(t, "pending", created["status"])
	assert.Equal(t, "high", created["priority"])
	assert.EqualValues(t, 1, created["segments"])
	assert.EqualValues(t, 5, created["max_attempts"])
	assert.Equal(t, "otp", created["metadata"].(map[string]any)["campaign"])
	assert.NotContains(t, created, "subject", "absent optional fields are omitted, not null")

	id := created["id"].(string)
	res = call(t, http.MethodGet, "/api/v1/notifications/"+id, nil)
	require.Equal(t, http.StatusOK, res.Code)
	got := res.json(t)
	assert.Equal(t, id, got["id"])
	assert.Equal(t, []any{}, got["delivery_attempts"])
}

func TestCreateNotification_Scheduled(t *testing.T) {
	reset(t)
	at := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	res := call(t, http.MethodPost, "/api/v1/notifications", sms(map[string]any{"scheduled_at": at}))
	require.Equal(t, http.StatusCreated, res.Code)
	assert.Equal(t, "scheduled", res.json(t)["status"])
}

func TestCreateNotification_IdempotencyKeyInBody(t *testing.T) {
	reset(t)
	body := sms(map[string]any{"idempotency_key": "otp-42"})
	first := call(t, http.MethodPost, "/api/v1/notifications", body).json(t)
	second := call(t, http.MethodPost, "/api/v1/notifications", body).json(t)
	assert.Equal(t, first["id"], second["id"])
}

func TestCreateNotification_ValidationErrors(t *testing.T) {
	reset(t)
	res := call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
		"channel": "email", "recipient": "not-an-email", "content": "hi", "max_attempts": 99,
	})
	require.Equal(t, http.StatusUnprocessableEntity, res.Code)
	assert.Equal(t, "validation_failed", errorCode(t, res))
	assert.ElementsMatch(t, []string{"recipient", "subject", "max_attempts"}, errorFields(t, res))
}

func TestCreateNotification_BadRequests(t *testing.T) {
	reset(t)
	tests := map[string]string{
		"malformed json": `{"channel": "sms",`,
		"unknown field":  `{"channel":"sms","recipent":"+905551234567","content":"hi"}`,
		"wrong type":     `{"channel":"sms","recipient":12345,"content":"hi"}`,
		"trailing data":  `{"channel":"sms","recipient":"+905551234567","content":"hi"} {}`,
		"empty body":     ``,
		// "Şifre" encoded as Windows-1254, not UTF-8: must be refused, not
		// silently stored as "�ifre".
		"invalid utf8": "{\"channel\":\"sms\",\"recipient\":\"+905551234567\",\"content\":\"\xdeifre\"}",
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			res := call(t, http.MethodPost, "/api/v1/notifications", body)
			require.Equal(t, http.StatusBadRequest, res.Code, "%s", res.Body)
			assert.Equal(t, "bad_request", errorCode(t, res))
		})
	}
}

func TestCreateNotification_UnicodeRoundTrip(t *testing.T) {
	reset(t)
	res := call(t, http.MethodPost, "/api/v1/notifications", sms(map[string]any{"content": "Şifreniz: 4821 çğıöü 😀"}))
	require.Equal(t, http.StatusCreated, res.Code)
	n := res.json(t)
	assert.Equal(t, "Şifreniz: 4821 çğıöü 😀", n["content"])
	assert.EqualValues(t, 1, n["segments"], "UCS-2, 23 units: one segment")
}

func TestBodyTooLarge(t *testing.T) {
	res := call(t, http.MethodPost, "/api/v1/notifications", sms(map[string]any{"content": strings.Repeat("x", maxBody)}))
	require.Equal(t, http.StatusRequestEntityTooLarge, res.Code)
	assert.Equal(t, "payload_too_large", errorCode(t, res))
}

func TestGetNotification_NotFoundAndBadID(t *testing.T) {
	res := call(t, http.MethodGet, "/api/v1/notifications/0192f5e4-7c1a-7b3e-9a41-2f0c8d6b1e77", nil)
	assert.Equal(t, http.StatusNotFound, res.Code)
	assert.Equal(t, "not_found", errorCode(t, res))

	res = call(t, http.MethodGet, "/api/v1/notifications/not-a-uuid", nil)
	assert.Equal(t, http.StatusBadRequest, res.Code)
}

func TestCancelAndRetry(t *testing.T) {
	reset(t)
	id := call(t, http.MethodPost, "/api/v1/notifications", sms()).json(t)["id"].(string)

	res := call(t, http.MethodPost, "/api/v1/notifications/"+id+"/retry", nil)
	require.Equal(t, http.StatusConflict, res.Code, "pending cannot be retried")
	assert.Equal(t, "invalid_status_transition", errorCode(t, res))

	res = call(t, http.MethodPost, "/api/v1/notifications/"+id+"/cancel", nil)
	require.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "cancelled", res.json(t)["status"])

	res = call(t, http.MethodPost, "/api/v1/notifications/"+id+"/cancel", nil)
	require.Equal(t, http.StatusConflict, res.Code)
	assert.Contains(t, res.json(t)["error"].(map[string]any)["message"], "cancelled")

	_, err := pool.Exec(t.Context(), `UPDATE notifications SET status='failed', failed_at=now(), cancelled_at=NULL WHERE id=$1`, id)
	require.NoError(t, err)
	res = call(t, http.MethodPost, "/api/v1/notifications/"+id+"/retry", nil)
	require.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "pending", res.json(t)["status"])
}

func TestListNotifications(t *testing.T) {
	reset(t)
	for range 3 {
		call(t, http.MethodPost, "/api/v1/notifications", sms())
	}
	call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
		"channel": "push", "recipient": "device-token", "content": "hi",
	})

	page := call(t, http.MethodGet, "/api/v1/notifications?limit=2", nil).json(t)
	assert.Len(t, page["items"], 2)
	assert.Equal(t, true, page["has_more"])

	next := call(t, http.MethodGet, "/api/v1/notifications?limit=2&cursor="+page["next_cursor"].(string), nil).json(t)
	assert.Len(t, next["items"], 2)
	assert.Equal(t, false, next["has_more"])
	assert.NotContains(t, next, "next_cursor")

	push := call(t, http.MethodGet, "/api/v1/notifications?channel=push", nil).json(t)
	assert.Len(t, push["items"], 1)

	t.Run("invalid parameters", func(t *testing.T) {
		for query, field := range map[string]string{
			"status=done":  "status",
			"channel=fax":  "channel",
			"limit=0":      "limit",
			"limit=101":    "limit",
			"cursor=xyz!!": "cursor",
			"created_from=2026-02-01T00:00:00Z&created_to=2026-01-01T00:00:00Z": "created_to",
		} {
			res := call(t, http.MethodGet, "/api/v1/notifications?"+query, nil)
			require.Equal(t, http.StatusUnprocessableEntity, res.Code, query)
			assert.Contains(t, errorFields(t, res), field, query)
		}
		res := call(t, http.MethodGet, "/api/v1/notifications?created_from=yesterday", nil)
		assert.Equal(t, http.StatusBadRequest, res.Code, "type errors are 400")
	})
}

// ---- batches ---------------------------------------------------------------

func TestBatch_Lifecycle(t *testing.T) {
	reset(t)
	res := call(t, http.MethodPost, "/api/v1/notifications/batch", map[string]any{
		"metadata": map[string]any{"campaign": "bf"},
		"items": []any{
			sms(),
			sms(map[string]any{"idempotency_key": "dup"}),
			map[string]any{"channel": "push", "recipient": "tok", "subject": "Sale", "content": "50% off"},
		},
	})
	require.Equal(t, http.StatusCreated, res.Code, "%s", res.Body)
	created := res.json(t)
	assert.EqualValues(t, 3, created["total"])
	batchID := created["batch_id"].(string)

	batch := call(t, http.MethodGet, "/api/v1/batches/"+batchID, nil).json(t)
	assert.EqualValues(t, 3, batch["counts"].(map[string]any)["pending"])
	assert.EqualValues(t, 0, batch["progress"])

	list := call(t, http.MethodGet, "/api/v1/batches/"+batchID+"/notifications", nil).json(t)
	assert.Len(t, list["items"], 3)

	cancelled := call(t, http.MethodPost, "/api/v1/batches/"+batchID+"/cancel", nil).json(t)
	assert.EqualValues(t, 3, cancelled["cancelled"])

	batch = call(t, http.MethodGet, "/api/v1/batches/"+batchID, nil).json(t)
	assert.EqualValues(t, 1, batch["progress"])

	// A second batch reusing the key reports it as a duplicate.
	res = call(t, http.MethodPost, "/api/v1/notifications/batch", map[string]any{
		"items": []any{sms(map[string]any{"idempotency_key": "dup"})},
	})
	require.Equal(t, http.StatusCreated, res.Code)
	assert.Equal(t, true, res.json(t)["notifications"].([]any)[0].(map[string]any)["duplicate"])
}

func TestBatch_AllOrNothing(t *testing.T) {
	reset(t)
	res := call(t, http.MethodPost, "/api/v1/notifications/batch", map[string]any{
		"items": []any{sms(), sms(map[string]any{"recipient": "0555"}), sms(map[string]any{"channel": "fax"})},
	})
	require.Equal(t, http.StatusUnprocessableEntity, res.Code)
	assert.ElementsMatch(t, []string{"items[1].recipient", "items[2].channel"}, errorFields(t, res))

	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM notifications`).Scan(&count))
	assert.Zero(t, count, "nothing is stored when any item is invalid")
}

func TestBatch_NotFound(t *testing.T) {
	const id = "0192f5e4-7c1a-7b3e-9a41-2f0c8d6b1e77"
	assert.Equal(t, http.StatusNotFound, call(t, http.MethodGet, "/api/v1/batches/"+id, nil).Code)
	assert.Equal(t, http.StatusNotFound, call(t, http.MethodGet, "/api/v1/batches/"+id+"/notifications", nil).Code)
	assert.Equal(t, http.StatusNotFound, call(t, http.MethodPost, "/api/v1/batches/"+id+"/cancel", nil).Code)
}

// ---- templates -------------------------------------------------------------

func TestTemplates_EndToEnd(t *testing.T) {
	reset(t)
	res := call(t, http.MethodPost, "/api/v1/templates", map[string]any{
		"name": "order-shipped", "channel": "email",
		"subject": "Order {{.order_id}} shipped",
		"body":    "Hi {{.name}}, order {{.order_id}} ships via {{upper .carrier}}.",
	})
	require.Equal(t, http.StatusCreated, res.Code, "%s", res.Body)
	tpl := res.json(t)
	assert.Equal(t, []any{"carrier", "name", "order_id"}, tpl["variables"])
	tplID := tpl["id"].(string)

	t.Run("preview", func(t *testing.T) {
		res := call(t, http.MethodPost, "/api/v1/templates/"+tplID+"/preview", map[string]any{
			"template_vars": map[string]any{"name": "Ada", "order_id": "A-1", "carrier": "ups"},
		})
		require.Equal(t, http.StatusOK, res.Code, "%s", res.Body)
		p := res.json(t)
		assert.Equal(t, "Order A-1 shipped", p["subject"])
		assert.Equal(t, "Hi Ada, order A-1 ships via UPS.", p["content"])

		res = call(t, http.MethodPost, "/api/v1/templates/"+tplID+"/preview", map[string]any{"template_vars": map[string]any{"name": "Ada"}})
		require.Equal(t, http.StatusUnprocessableEntity, res.Code)
		assert.Equal(t, []string{"template_vars"}, errorFields(t, res))
	})

	t.Run("notification from template", func(t *testing.T) {
		res := call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
			"channel": "email", "recipient": "ada@example.com", "template_id": tplID,
			"template_vars": map[string]any{"name": "Ada", "order_id": "A-1", "carrier": "ups"},
		})
		require.Equal(t, http.StatusCreated, res.Code, "%s", res.Body)
		n := res.json(t)
		assert.Equal(t, "Order A-1 shipped", n["subject"])
		assert.Equal(t, "Hi Ada, order A-1 ships via UPS.", n["content"])
		assert.EqualValues(t, 1, n["template_version"])
	})

	t.Run("template channel must match", func(t *testing.T) {
		res := call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
			"channel": "sms", "recipient": "+905551234567", "template_id": tplID,
			"template_vars": map[string]any{"name": "Ada", "order_id": "A-1", "carrier": "ups"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, res.Code)
		assert.Equal(t, []string{"template_id"}, errorFields(t, res))
	})

	t.Run("unknown template in batch", func(t *testing.T) {
		res := call(t, http.MethodPost, "/api/v1/notifications/batch", map[string]any{"items": []any{
			sms(),
			map[string]any{"channel": "sms", "recipient": "+905551234567", "template_id": "0192f5e4-7c1a-7b3e-9a41-2f0c8d6b1e77"},
		}})
		require.Equal(t, http.StatusUnprocessableEntity, res.Code)
		assert.Equal(t, []string{"items[1].template_id"}, errorFields(t, res))
	})

	t.Run("invalid templates", func(t *testing.T) {
		for name, body := range map[string]map[string]any{
			"unsafe function":  {"name": "x", "channel": "sms", "body": `{{printf "%0999d" 1}}`},
			"bad name":         {"name": "Has Spaces", "channel": "sms", "body": "hi"},
			"email no subject": {"name": "y", "channel": "email", "body": "hi"},
		} {
			res := call(t, http.MethodPost, "/api/v1/templates", body)
			assert.Equal(t, http.StatusUnprocessableEntity, res.Code, name)
		}
	})

	t.Run("update, duplicate name, delete", func(t *testing.T) {
		res := call(t, http.MethodPut, "/api/v1/templates/"+tplID, map[string]any{
			"name": "order-shipped", "channel": "email", "subject": "Shipped", "body": "Hi {{.name}}",
		})
		require.Equal(t, http.StatusOK, res.Code)
		assert.EqualValues(t, 2, res.json(t)["version"])

		res = call(t, http.MethodPost, "/api/v1/templates", map[string]any{"name": "order-shipped", "channel": "sms", "body": "x"})
		assert.Equal(t, http.StatusConflict, res.Code)

		list := call(t, http.MethodGet, "/api/v1/templates?channel=email", nil).json(t)
		assert.Len(t, list["items"], 1)

		assert.Equal(t, http.StatusNoContent, call(t, http.MethodDelete, "/api/v1/templates/"+tplID, nil).Code)
		assert.Equal(t, http.StatusNotFound, call(t, http.MethodGet, "/api/v1/templates/"+tplID, nil).Code)
	})
}

// ---- middleware ------------------------------------------------------------

func TestAuth(t *testing.T) {
	for name, headers := range map[string]map[string]string{
		"missing key": {},
		"wrong key":   {"X-API-Key": "nope"},
	} {
		t.Run(name, func(t *testing.T) {
			res := callWith(t, http.MethodGet, "/api/v1/notifications", nil, headers)
			require.Equal(t, http.StatusUnauthorized, res.Code)
			assert.Equal(t, "unauthorized", errorCode(t, res))
		})
	}

	res := callWith(t, http.MethodGet, "/health", nil, nil)
	assert.Equal(t, http.StatusOK, res.Code, "probes need no key")
}

func TestCorrelationID(t *testing.T) {
	reset(t)
	res := callWith(t, http.MethodPost, "/api/v1/notifications", sms(), map[string]string{
		"X-API-Key": apiKey, "X-Correlation-ID": "checkout-7781",
	})
	require.Equal(t, http.StatusCreated, res.Code)
	assert.Equal(t, "checkout-7781", res.Header.Get("X-Correlation-ID"))

	var stored string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT correlation_id FROM notifications`).Scan(&stored))
	assert.Equal(t, "checkout-7781", stored, "saved so the worker can continue the trail")

	res = call(t, http.MethodGet, "/api/v1/notifications/0192f5e4-7c1a-7b3e-9a41-2f0c8d6b1e77", nil)
	generated := res.Header.Get("X-Correlation-ID")
	assert.NotEmpty(t, generated)
	assert.Equal(t, generated, res.json(t)["correlation_id"], "error bodies carry the same id")
}

func TestIdempotencyKey(t *testing.T) {
	reset(t)
	withKey := func(key string) map[string]string {
		return map[string]string{"X-API-Key": apiKey, "Idempotency-Key": key}
	}

	first := callWith(t, http.MethodPost, "/api/v1/notifications", sms(), withKey("req-1"))
	second := callWith(t, http.MethodPost, "/api/v1/notifications", sms(), withKey("req-1"))
	require.Equal(t, http.StatusCreated, first.Code)
	require.Equal(t, http.StatusCreated, second.Code)
	assert.Equal(t, first.json(t)["id"], second.json(t)["id"])
	assert.Equal(t, "true", second.Header.Get("Idempotent-Replayed"))

	reused := callWith(t, http.MethodPost, "/api/v1/notifications", sms(map[string]any{"content": "other"}), withKey("req-1"))
	require.Equal(t, http.StatusUnprocessableEntity, reused.Code)
	assert.Equal(t, "idempotency_key_reused", errorCode(t, reused))

	batch := map[string]any{"items": []any{sms(), sms()}}
	b1 := callWith(t, http.MethodPost, "/api/v1/notifications/batch", batch, withKey("batch-1"))
	b2 := callWith(t, http.MethodPost, "/api/v1/notifications/batch", batch, withKey("batch-1"))
	assert.Equal(t, b1.json(t)["batch_id"], b2.json(t)["batch_id"], "a retried batch is not created twice")

	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM notifications`).Scan(&count))
	assert.Equal(t, 3, count, "one single + two batch items")
}

// TestIdempotencyKey_Concurrent fires the same request 20 times at once, as
// an aggressive retry policy might. Exactly one notification may exist
// afterwards; every response is either the 201 or a 409 "in progress".
func TestIdempotencyKey_Concurrent(t *testing.T) {
	reset(t)
	const n = 20
	codes := make(chan int, n)
	ids := make(chan any, n)

	start := make(chan struct{})
	done := make(chan struct{}, n)
	for range n {
		go func() {
			defer func() { done <- struct{}{} }()
			<-start
			res := send(t, http.MethodPost, "/api/v1/notifications", sms(),
				map[string]string{"X-API-Key": apiKey, "Idempotency-Key": "burst"}, true)
			codes <- res.Code
			if res.Code == http.StatusCreated {
				ids <- res.json(t)["id"]
			}
		}()
	}
	close(start)
	for range n {
		<-done
	}
	close(codes)
	close(ids)

	for c := range codes {
		assert.Contains(t, []int{http.StatusCreated, http.StatusConflict}, c)
	}
	unique := map[any]bool{}
	for id := range ids {
		unique[id] = true
	}
	assert.Len(t, unique, 1, "every 201 describes the same notification")

	var count int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM notifications`).Scan(&count))
	assert.Equal(t, 1, count)
}

// ---- routing ---------------------------------------------------------------

func TestUnknownRouteIsJSON(t *testing.T) {
	res := callUndocumented(t, http.MethodGet, "/api/v1/nope")
	assert.Equal(t, http.StatusNotFound, res.Code)
	assert.Equal(t, "not_found", errorCode(t, res))
}
