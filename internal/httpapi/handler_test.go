package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockBackend is a scriptable backend for handler-level tests.
type mockBackend struct {
	pingErr error

	poolIn         poolInput
	poolCreated    bool
	poolErr        error
	poolCalls      int
	reservationIn  reservationInput
	resResult      operationResult
	resErr         error
	resCalls       int
	resKey         string
	resHash        string
	resTenant      string
	resPool        string
	decision       string
	decResult      operationResult
	decErr         error
	decCalls       int
	getResult      reservationResponse
	getErr         error
	getCalls       int
	allocIn        allocationInput
	allocResult    operationResult
	allocErr       error
	allocCalls     int
	allocKey       string
	allocHash      string
	allocTeam      string
	getAllocResult allocationResponse
	getAllocErr    error
	getAllocCalls  int
}

func (m *mockBackend) Ping(context.Context) error { return m.pingErr }

func (m *mockBackend) createPool(_ context.Context, tenantID, poolID string, in poolInput) (poolResponse, bool, error) {
	m.poolCalls++
	m.poolIn = in
	if m.poolErr != nil {
		return poolResponse{}, false, m.poolErr
	}
	if !m.poolCreated {
		return poolResponse{}, false, nil
	}
	return poolResponse{TenantID: tenantID, PoolID: poolID, Limit: in.limit,
		ValidFrom: in.validFrom, ValidUntil: in.validUntil}, true, nil
}

func (m *mockBackend) createReservation(_ context.Context, tenantID, poolID, key, hash string, in reservationInput) (operationResult, error) {
	m.resCalls++
	m.resTenant, m.resPool, m.resKey, m.resHash, m.reservationIn = tenantID, poolID, key, hash, in
	return m.resResult, m.resErr
}

func (m *mockBackend) decideReservation(_ context.Context, tenantID, poolID, reservationID, key, hash, decision string) (operationResult, error) {
	m.decCalls++
	m.resTenant, m.resPool, m.resKey, m.resHash, m.decision = tenantID, poolID, reservationID, key, decision
	return m.decResult, m.decErr
}

func (m *mockBackend) getReservation(_ context.Context, tenantID, poolID, reservationID string) (reservationResponse, error) {
	m.getCalls++
	m.resTenant, m.resPool = tenantID, poolID
	return m.getResult, m.getErr
}

func (m *mockBackend) setAllocation(_ context.Context, tenantID, poolID, teamID, key, hash string, in allocationInput) (operationResult, error) {
	m.allocCalls++
	m.resTenant, m.resPool, m.allocTeam, m.allocKey, m.allocHash, m.allocIn = tenantID, poolID, teamID, key, hash, in
	return m.allocResult, m.allocErr
}

func (m *mockBackend) getAllocation(_ context.Context, tenantID, poolID, teamID string) (allocationResponse, error) {
	m.getAllocCalls++
	m.resTenant, m.resPool, m.allocTeam = tenantID, poolID, teamID
	return m.getAllocResult, m.getAllocErr
}

func request(method, target string, body string) *http.Request {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	return r
}

func withKey(r *http.Request, key string) *http.Request {
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	return r
}

func TestHealthAndReadiness(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		dbError                  error
		status                   int
		calls                    int
	}{
		{"liveness", "GET", "/healthz", "ok", errors.New("database offline"), 200, 0},
		{"ready", "GET", "/readyz", "ready", nil, 200, 1},
		{"unavailable", "GET", "/readyz", "unavailable", errors.New("private connection details"), 503, 1},
		{"unknown path", "GET", "/entitlements", "404", nil, 404, 0},
		{"wrong method", "POST", "/readyz", "Method Not Allowed", nil, 405, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			mb := &mockBackend{pingErr: tc.dbError}
			handler := New(&pingCountingBackend{mb, &calls})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request(tc.method, tc.path, ""))
			if response.Code != tc.status || !strings.Contains(response.Body.String(), tc.body) || calls != tc.calls {
				t.Fatalf("status=%d body=%q database calls=%d", response.Code, response.Body.String(), calls)
			}
			if strings.Contains(response.Body.String(), "private connection details") {
				t.Fatal("database error leaked into public response")
			}
		})
	}
}

// pingCountingBackend wraps mockBackend to assert the readiness deadline.
type pingCountingBackend struct {
	*mockBackend
	calls *int
}

func (p *pingCountingBackend) Ping(ctx context.Context) error {
	*p.calls++
	if _, ok := ctx.Deadline(); !ok {
		panic("readiness database check must have a deadline")
	}
	return p.mockBackend.pingErr
}

func TestPutPool(t *testing.T) {
	base := "/v1/tenants/t1/quota-pools/p1"
	validBody := `{"limit":10,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`

	for _, tc := range []struct {
		name   string
		body   string
		status int
	}{
		{"missing limit", `{"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`, 400},
		{"zero limit", `{"limit":0,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`, 400},
		{"negative limit", `{"limit":-3,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`, 400},
		{"fractional limit", `{"limit":1.5,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z"}`, 400},
		{"bad from", `{"limit":10,"validFrom":"soon","validUntil":"2026-12-31T00:00:00Z"}`, 400},
		{"bad until", `{"limit":10,"validFrom":"2026-01-01T00:00:00Z","validUntil":"never"}`, 400},
		{"from equals until", `{"limit":10,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-01-01T00:00:00Z"}`, 400},
		{"from after until", `{"limit":10,"validFrom":"2026-12-31T00:00:00Z","validUntil":"2026-01-01T00:00:00Z"}`, 400},
		{"unknown field", `{"limit":10,"validFrom":"2026-01-01T00:00:00Z","validUntil":"2026-12-31T00:00:00Z","extra":1}`, 400},
		{"malformed json", `{not json`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mb := &mockBackend{}
			h := New(mb)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, request(http.MethodPut, base, tc.body))
			if rec.Code != tc.status {
				t.Fatalf("status=%d want %d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if mb.poolCalls != 0 {
				t.Fatal("invalid request must not reach storage")
			}
		})
	}

	t.Run("created", func(t *testing.T) {
		mb := &mockBackend{poolCreated: true}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodPut, base, validBody))
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"limit":10`) || mb.poolIn.limit != 10 {
			t.Fatalf("unexpected body/input: %s %+v", rec.Body.String(), mb.poolIn)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		mb := &mockBackend{poolCreated: false}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodPut, base, validBody))
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d", rec.Code)
		}
	})

	t.Run("storage error hidden", func(t *testing.T) {
		mb := &mockBackend{poolErr: errors.New("secret schema detail")}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodPut, base, validBody))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "secret") {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func TestPostReservationValidation(t *testing.T) {
	target := "/v1/tenants/t1/quota-pools/p1/reservations"
	validBody := `{"reservationId":"r1","amount":5,"expiresAt":"2026-12-31T00:00:00Z"}`

	t.Run("missing idempotency key", func(t *testing.T) {
		mb := &mockBackend{}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodPost, target, validBody))
		if rec.Code != http.StatusBadRequest || mb.resCalls != 0 {
			t.Fatalf("status=%d calls=%d", rec.Code, mb.resCalls)
		}
	})

	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing reservationId", `{"amount":5,"expiresAt":"2026-12-31T00:00:00Z"}`},
		{"blank reservationId", `{"reservationId":"  ","amount":5,"expiresAt":"2026-12-31T00:00:00Z"}`},
		{"zero amount", `{"reservationId":"r1","amount":0,"expiresAt":"2026-12-31T00:00:00Z"}`},
		{"fractional amount", `{"reservationId":"r1","amount":2.5,"expiresAt":"2026-12-31T00:00:00Z"}`},
		{"bad expiry", `{"reservationId":"r1","amount":5,"expiresAt":"tomorrow"}`},
		{"unknown field", `{"reservationId":"r1","amount":5,"expiresAt":"2026-12-31T00:00:00Z","x":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mb := &mockBackend{}
			rec := httptest.NewRecorder()
			New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, tc.body), "k1"))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if mb.resCalls != 0 {
				t.Fatal("invalid request must not reach storage")
			}
		})
	}

	t.Run("success passes parsed input and key", func(t *testing.T) {
		mb := &mockBackend{resResult: operationResult{status: http.StatusCreated, body: []byte(`{"reservationId":"r1"}`)}}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, validBody), "key-1"))
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d", rec.Code)
		}
		if mb.resKey != "key-1" || mb.reservationIn.reservationID != "r1" || mb.reservationIn.amount != 5 {
			t.Fatalf("backend got %+v", mb)
		}
		if mb.resHash == "" {
			t.Fatal("request hash must be supplied")
		}
	})

	t.Run("different content yields different hash", func(t *testing.T) {
		mb := &mockBackend{resResult: operationResult{status: http.StatusCreated, body: []byte(`{}`)}}
		h := New(mb)
		r1 := httptest.NewRecorder()
		h.ServeHTTP(r1, withKey(request(http.MethodPost, target, validBody), "k"))
		hash1 := mb.resHash
		r2 := httptest.NewRecorder()
		h.ServeHTTP(r2, withKey(request(http.MethodPost, target, `{"reservationId":"r1","amount":6,"expiresAt":"2026-12-31T00:00:00Z"}`), "k"))
		if hash1 == mb.resHash {
			t.Fatal("requests with different amounts must hash differently")
		}
	})

	t.Run("storage error hidden", func(t *testing.T) {
		mb := &mockBackend{resErr: errors.New("deadlock detail")}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, validBody), "k"))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "deadlock") {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func TestDecisionRouting(t *testing.T) {
	for _, tc := range []struct {
		name, path, decision string
	}{
		{"confirm suffix", "/v1/tenants/t1/quota-pools/p1/reservations/r1/confirm", "confirmed"},
		{"release suffix", "/v1/tenants/t1/quota-pools/p1/reservations/r1/release", "released"},
		{"confirm action", "/v1/tenants/t1/quota-pools/p1/reservations/r1:confirm", "confirmed"},
		{"release action", "/v1/tenants/t1/quota-pools/p1/reservations/r1:release", "released"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mb := &mockBackend{decResult: operationResult{status: http.StatusOK, body: []byte(`{"status":"` + tc.decision + `"}`)}}
			rec := httptest.NewRecorder()
			New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, tc.path, ""), "k1"))
			if rec.Code != http.StatusOK || mb.decCalls != 1 || mb.decision != tc.decision {
				t.Fatalf("status=%d calls=%d decision=%q body=%s", rec.Code, mb.decCalls, mb.decision, rec.Body.String())
			}
		})
	}

	t.Run("missing key is rejected", func(t *testing.T) {
		mb := &mockBackend{}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodPost, "/v1/tenants/t1/quota-pools/p1/reservations/r1/confirm", ""))
		if rec.Code != http.StatusBadRequest || mb.decCalls != 0 {
			t.Fatalf("status=%d calls=%d", rec.Code, mb.decCalls)
		}
	})

	t.Run("backend conflict surfaces", func(t *testing.T) {
		mb := &mockBackend{decResult: operationResult{status: http.StatusConflict, body: []byte(`{"error":"reservation is not pending"}`)}}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, "/v1/tenants/t1/quota-pools/p1/reservations/r1/release", ""), "k"))
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d", rec.Code)
		}
	})
}

func TestGetReservation(t *testing.T) {
	target := "/v1/tenants/t1/quota-pools/p1/reservations/r1"

	t.Run("found", func(t *testing.T) {
		mb := &mockBackend{getResult: reservationResponse{ReservationID: "r1", Status: "pending"}}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodGet, target, ""))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"pending"`) {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("missing and cross-tenant both 404", func(t *testing.T) {
		mb := &mockBackend{getErr: errNotFound}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodGet, target, ""))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d", rec.Code)
		}
	})

	t.Run("storage error 503", func(t *testing.T) {
		mb := &mockBackend{getErr: errors.New("connection reset internals")}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodGet, target, ""))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "reset") {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func TestPostAllocationValidation(t *testing.T) {
	target := "/v1/tenants/t1/quota-pools/p1/teams/team1/allocation"
	validBody := `{"amount":10,"expectedVersion":0}`

	t.Run("missing idempotency key", func(t *testing.T) {
		mb := &mockBackend{}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodPost, target, validBody))
		if rec.Code != http.StatusBadRequest || mb.allocCalls != 0 {
			t.Fatalf("status=%d calls=%d", rec.Code, mb.allocCalls)
		}
	})

	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing amount", `{"expectedVersion":0}`},
		{"negative amount", `{"amount":-1,"expectedVersion":0}`},
		{"fractional amount", `{"amount":1.5,"expectedVersion":0}`},
		{"missing expectedVersion", `{"amount":10}`},
		{"negative expectedVersion", `{"amount":10,"expectedVersion":-1}`},
		{"fractional expectedVersion", `{"amount":10,"expectedVersion":0.5}`},
		{"unknown field", `{"amount":10,"expectedVersion":0,"x":1}`},
		{"malformed json", `{not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mb := &mockBackend{}
			rec := httptest.NewRecorder()
			New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, tc.body), "k1"))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if mb.allocCalls != 0 {
				t.Fatal("invalid request must not reach storage")
			}
		})
	}

	t.Run("valid request reaches storage", func(t *testing.T) {
		mb := &mockBackend{allocResult: operationResult{status: http.StatusOK,
			body: []byte(`{"teamId":"team1","allocated":10,"used":0,"available":10,"version":1}`)}}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, validBody), "k1"))
		if rec.Code != http.StatusOK || mb.allocCalls != 1 {
			t.Fatalf("status=%d calls=%d body=%s", rec.Code, mb.allocCalls, rec.Body.String())
		}
		if mb.allocTeam != "team1" || mb.allocKey != "k1" || mb.allocIn.amount != 10 || mb.allocIn.expectedVersion != 0 {
			t.Fatalf("backend got %+v team=%q key=%q", mb.allocIn, mb.allocTeam, mb.allocKey)
		}
		if mb.allocHash == "" {
			t.Fatal("request hash must be supplied")
		}
	})

	t.Run("different content yields different hash", func(t *testing.T) {
		mb := &mockBackend{allocResult: operationResult{status: http.StatusOK, body: []byte(`{}`)}}
		h := New(mb)
		r1 := httptest.NewRecorder()
		h.ServeHTTP(r1, withKey(request(http.MethodPost, target, validBody), "k"))
		hash1 := mb.allocHash
		r2 := httptest.NewRecorder()
		h.ServeHTTP(r2, withKey(request(http.MethodPost, target, `{"amount":11,"expectedVersion":0}`), "k"))
		if hash1 == mb.allocHash {
			t.Fatal("different allocation requests must hash differently")
		}
	})

	t.Run("storage error hidden", func(t *testing.T) {
		mb := &mockBackend{allocErr: errors.New("secret schema detail")}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, validBody), "k1"))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "secret") {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func TestGetAllocation(t *testing.T) {
	target := "/v1/tenants/t1/quota-pools/p1/teams/team1/allocation"

	t.Run("found", func(t *testing.T) {
		mb := &mockBackend{getAllocResult: allocationResponse{TeamID: "team1", Allocated: 10, Used: 3, Available: 7, Version: 2}}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodGet, target, ""))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"available":7`) {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if mb.allocTeam != "team1" {
			t.Fatalf("team=%q", mb.allocTeam)
		}
	})

	t.Run("missing is 404", func(t *testing.T) {
		mb := &mockBackend{getAllocErr: errNotFound}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodGet, target, ""))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d", rec.Code)
		}
	})

	t.Run("storage error 503", func(t *testing.T) {
		mb := &mockBackend{getAllocErr: errors.New("connection reset internals")}
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, request(http.MethodGet, target, ""))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "reset") {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func TestPostReservationWithTeam(t *testing.T) {
	target := "/v1/tenants/t1/quota-pools/p1/reservations"

	t.Run("teamId passed through", func(t *testing.T) {
		mb := &mockBackend{resResult: operationResult{status: http.StatusCreated, body: []byte(`{}`)}}
		body := `{"reservationId":"r1","teamId":"team1","amount":5,"expiresAt":"2026-12-31T00:00:00Z"}`
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, body), "k1"))
		if rec.Code != http.StatusCreated || mb.reservationIn.teamID != "team1" {
			t.Fatalf("status=%d input=%+v", rec.Code, mb.reservationIn)
		}
	})

	t.Run("blank teamId rejected", func(t *testing.T) {
		mb := &mockBackend{}
		body := `{"reservationId":"r1","teamId":"  ","amount":5,"expiresAt":"2026-12-31T00:00:00Z"}`
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, body), "k1"))
		if rec.Code != http.StatusBadRequest || mb.resCalls != 0 {
			t.Fatalf("status=%d calls=%d", rec.Code, mb.resCalls)
		}
	})

	t.Run("absent teamId stays empty", func(t *testing.T) {
		mb := &mockBackend{resResult: operationResult{status: http.StatusCreated, body: []byte(`{}`)}}
		body := `{"reservationId":"r1","amount":5,"expiresAt":"2026-12-31T00:00:00Z"}`
		rec := httptest.NewRecorder()
		New(mb).ServeHTTP(rec, withKey(request(http.MethodPost, target, body), "k1"))
		if rec.Code != http.StatusCreated || mb.reservationIn.teamID != "" {
			t.Fatalf("status=%d input=%+v", rec.Code, mb.reservationIn)
		}
	})
}
