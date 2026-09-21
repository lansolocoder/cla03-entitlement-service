package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// validationHandler returns a handler whose database is never reached because
// every case below fails request validation first.
func validationHandler() http.Handler {
	return New(func(context.Context) error { return nil }, nil)
}

func TestPutPoolValidation(t *testing.T) {
	const path = "/v1/tenants/t1/quota-pools/p1"
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty body", ``},
		{"not json", `{`},
		{"missing limit", `{"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`},
		{"zero limit", `{"limit":0,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`},
		{"negative limit", `{"limit":-3,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`},
		{"fractional limit", `{"limit":1.5,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`},
		{"missing validFrom", `{"limit":10,"validUntil":"2026-12-31T00:00:00Z"}`},
		{"missing validUntil", `{"limit":10,"validFrom":"2026-01-01T00:00:00Z"}`},
		{"bad time format", `{"limit":10,"validFrom":"2026/01/01","validUntil":"2026-12-31T00:00:00Z"}`},
		{"from equals until", `{"limit":10,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-01-01T00:00:00Z"}`},
		{"from after until", `{"limit":10,"validFrom":"2026-12-31T00:00:00Z","validUntil":"2026-01-01T00:00:00Z"}`},
		{"unknown field", `{"limit":10,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z","extra":1}`},
		{"wrong type", `{"limit":"10","validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`},
		{"trailing content", `{"limit":10,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}garbage`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			validationHandler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("got %d, want 400, body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPostRequiresIdempotencyKey(t *testing.T) {
	paths := []string{
		"/v1/tenants/t1/quota-pools/p1/reservations",
		"/v1/tenants/t1/quota-pools/p1/reservations/r1/confirm",
		"/v1/tenants/t1/quota-pools/p1/reservations/r1/release",
	}
	body := `{"reservationId":"r1","amount":1,"expiresAt":"2099-01-01T00:00:00Z"}`
	for _, path := range paths {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		validationHandler().ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s without key: got %d, want 400", path, rec.Code)
		}
	}
}

func TestCreateReservationValidation(t *testing.T) {
	const path = "/v1/tenants/t1/quota-pools/p1/reservations"
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty body", ``},
		{"malformed json", `{"amount":`},
		{"missing reservationId", `{"amount":1,"expiresAt":"2099-01-01T00:00:00Z"}`},
		{"missing amount", `{"reservationId":"r1","expiresAt":"2099-01-01T00:00:00Z"}`},
		{"zero amount", `{"reservationId":"r1","amount":0,"expiresAt":"2099-01-01T00:00:00Z"}`},
		{"negative amount", `{"reservationId":"r1","amount":-5,"expiresAt":"2099-01-01T00:00:00Z"}`},
		{"fractional amount", `{"reservationId":"r1","amount":2.5,"expiresAt":"2099-01-01T00:00:00Z"}`},
		{"missing expiresAt", `{"reservationId":"r1","amount":1}`},
		{"bad expiresAt", `{"reservationId":"r1","amount":1,"expiresAt":"soon"}`},
		{"unknown field", `{"reservationId":"r1","amount":1,"expiresAt":"2099-01-01T00:00:00Z","x":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
			req.Header.Set("Idempotency-Key", "k-"+tc.name)
			validationHandler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("got %d, want 400, body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestActionEndpointsRejectBody(t *testing.T) {
	for _, action := range []string{"confirm", "release"} {
		path := "/v1/tenants/t1/quota-pools/p1/reservations/r1/" + action
		// Any real content is a malformed request (an empty body or {} is
		// accepted; that path is covered against a real database in the
		// integration tests).
		for _, body := range []string{`{"amount":1}`, `garbage`, `[]`} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			req.Header.Set("Idempotency-Key", "k-"+body)
			validationHandler().ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s with %s: got %d, want 400", action, body, rec.Code)
			}
		}
	}
}
