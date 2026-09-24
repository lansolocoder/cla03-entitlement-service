package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func doRequest(t *testing.T, h http.Handler, method, target, body string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(body)))
	parsed := map[string]any{}
	if rec.Body.Len() > 0 {
		// The default mux 404/405 responses are plain text; that is fine.
		if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
			parsed = nil
		}
	}
	return rec.Code, parsed
}

const validGrant = `{
	"grantId":"g-1","feature":"seats","amount":10,
	"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"
}`

func TestCreateGrantSuccess(t *testing.T) {
	h := New(newFakeStore(), nil)

	// Offset and sub-second input is normalized to whole-second UTC Z.
	body := `{"grantId":"g-1","feature":"seats","amount":10,` +
		`"effectiveAt":"2026-01-01T02:00:00.25+02:00","expiresAt":"2026-02-01T01:00:00.25+01:00"}`
	status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements", body)
	if status != http.StatusCreated {
		t.Fatalf("status=%d body=%v", status, got)
	}
	want := map[string]any{
		"tenant": "acme", "grantId": "g-1", "feature": "seats",
		"amount": float64(10), "state": "active",
		"effectiveAt": "2026-01-01T00:00:00Z", "expiresAt": "2026-02-01T00:00:00Z",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("field %s = %v, want %v", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("unexpected response fields: %v", got)
	}
}

func TestCreateGrantIdempotentRetry(t *testing.T) {
	h := New(newFakeStore(), nil)

	status1, _ := doRequest(t, h, "POST", "/tenants/acme/entitlements", validGrant)
	if status1 != http.StatusCreated {
		t.Fatalf("first status=%d, want 201", status1)
	}

	// Same four fields expressed equivalently in another timezone: 200.
	equivalent := `{"grantId":"g-1","feature":"seats","amount":10,` +
		`"effectiveAt":"2026-01-01T05:00:00+05:00","expiresAt":"2026-02-01T05:30:00+05:30"}`
	status2, got := doRequest(t, h, "POST", "/tenants/acme/entitlements", equivalent)
	if status2 != http.StatusOK {
		t.Fatalf("retry status=%d, want 200", status2)
	}
	if got["grantId"] != "g-1" || got["state"] != "active" {
		t.Fatalf("retry returned unexpected body: %v", got)
	}
}

func TestCreateGrantConflict(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"feature differs", buildVariant("feature")},
		{"amount differs", buildVariant("amount")},
		{"effectiveAt differs", buildVariant("effectiveAt")},
		{"expiresAt differs", buildVariant("expiresAt")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			h := New(store, nil)
			if status, _ := doRequest(t, h, "POST", "/tenants/acme/entitlements", validGrant); status != 201 {
				t.Fatalf("initial status=%d", status)
			}
			status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements", tc.body)
			if status != http.StatusConflict {
				t.Fatalf("status=%d body=%v, want 409", status, got)
			}
			if _, ok := got["error"]; !ok {
				t.Fatal("409 response must carry an error string")
			}
			// No new data: exactly one grant visible, the original.
			status, got = doRequest(t, h, "GET", "/tenants/acme/entitlements?at=2026-01-15T00:00:00Z", "")
			if status != 200 {
				t.Fatalf("list status=%d", status)
			}
			grantsList, _ := got["grants"].([]any)
			if len(grantsList) != 1 {
				t.Fatalf("conflict created data: %v", grantsList)
			}
			first := grantsList[0].(map[string]any)
			if first["amount"] != float64(10) || first["feature"] != "seats" {
				t.Fatalf("original grant was mutated: %v", first)
			}
		})
	}
}

func buildVariant(changed string) string {
	fields := map[string]string{
		"feature":     `"feature":"quota"`,
		"amount":      `"amount":11`,
		"effectiveAt": `"effectiveAt":"2026-01-02T00:00:00Z"`,
		"expiresAt":   `"expiresAt":"2026-03-01T00:00:00Z"`,
	}
	set := map[string]string{
		"feature":     `"feature":"seats"`,
		"amount":      `"amount":10`,
		"effectiveAt": `"effectiveAt":"2026-01-01T00:00:00Z"`,
		"expiresAt":   `"expiresAt":"2026-02-01T00:00:00Z"`,
	}
	set[changed] = fields[changed]
	return `{"grantId":"g-1",` + set["feature"] + `,` + set["amount"] + `,` +
		set["effectiveAt"] + `,` + set["expiresAt"] + `}`
}

func TestCreateGrantValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty body", ``},
		{"malformed json", `{not json`},
		{"missing grantId", `{"feature":"seats","amount":10,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"empty grantId", `{"grantId":"","feature":"seats","amount":10,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"missing feature", `{"grantId":"g","amount":10,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"invalid feature", `{"grantId":"g","feature":"seat","amount":10,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"missing amount", `{"grantId":"g","feature":"seats","effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"zero amount", `{"grantId":"g","feature":"seats","amount":0,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"negative amount", `{"grantId":"g","feature":"seats","amount":-3,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"fractional amount", `{"grantId":"g","feature":"seats","amount":1.5,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"exponent amount", `{"grantId":"g","feature":"seats","amount":1e3,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"string amount", `{"grantId":"g","feature":"seats","amount":"10","effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"missing effectiveAt", `{"grantId":"g","feature":"seats","amount":10,"expiresAt":"2026-02-01T00:00:00Z"}`},
		{"missing expiresAt", `{"grantId":"g","feature":"seats","amount":10,"effectiveAt":"2026-01-01T00:00:00Z"}`},
		{"bad effectiveAt", `{"grantId":"g","feature":"seats","amount":10,"effectiveAt":"not-a-time","expiresAt":"2026-02-01T00:00:00Z"}`},
		{"bad expiresAt", `{"grantId":"g","feature":"seats","amount":10,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-02-01"}`},
		{"interval equal", `{"grantId":"g","feature":"seats","amount":10,"effectiveAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:00:00Z"}`},
		{"interval reversed", `{"grantId":"g","feature":"seats","amount":10,"effectiveAt":"2026-02-01T00:00:00Z","expiresAt":"2026-01-01T00:00:00Z"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New(newFakeStore(), nil)
			status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements", tc.body)
			if status != http.StatusBadRequest {
				t.Fatalf("status=%d body=%v, want 400", status, got)
			}
			if _, ok := got["error"].(string); !ok {
				t.Fatal("400 response must carry an error string")
			}
		})
	}
}

func TestCreateGrantQuotaAccepted(t *testing.T) {
	h := New(newFakeStore(), nil)
	body := strings.Replace(validGrant, `"feature":"seats"`, `"feature":"quota"`, 1)
	status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements", body)
	if status != 201 || got["feature"] != "quota" {
		t.Fatalf("status=%d body=%v", status, got)
	}
}

func TestCreateGrantStoreFailure(t *testing.T) {
	store := newFakeStore()
	store.failCreate = true
	h := New(store, nil)
	status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements", validGrant)
	if status != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", status)
	}
	if got["error"] != "internal error" {
		t.Fatalf("body=%v", got)
	}
}

func TestListGrantsIntervals(t *testing.T) {
	store := newFakeStore()
	h := New(store, nil)
	body := `{"grantId":"g-1","feature":"seats","amount":5,` +
		`"effectiveAt":"2026-01-01T00:00:00+02:00","expiresAt":"2026-02-01T00:00:00Z"}`
	if status, _ := doRequest(t, h, "POST", "/tenants/acme/entitlements", body); status != 201 {
		t.Fatal("seed failed")
	}
	// effectiveAt normalizes to 2025-12-31T22:00:00Z.
	for _, tc := range []struct {
		name string
		at   string
		want int
	}{
		{"before interval", "2025-12-31T21:59:59Z", 0},
		{"at effectiveAt (closed left)", "2025-12-31T22:00:00Z", 1},
		{"inside", "2026-01-15T12:00:00Z", 1},
		{"at expiresAt (open right)", "2026-02-01T00:00:00Z", 0},
		{"after interval", "2026-03-01T00:00:00Z", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, got := doRequest(t, h, "GET", "/tenants/acme/entitlements?at="+tc.at, "")
			if status != 200 {
				t.Fatalf("status=%d", status)
			}
			if got["tenant"] != "acme" {
				t.Errorf("tenant=%v", got["tenant"])
			}
			if got["asOf"] != tc.at {
				t.Errorf("asOf=%v, want %s", got["asOf"], tc.at)
			}
			list, _ := got["grants"].([]any)
			if len(list) != tc.want {
				t.Fatalf("grants=%v want %d", list, tc.want)
			}
			if tc.want == 1 {
				g := list[0].(map[string]any)
				for _, field := range []string{"tenant", "grantId", "feature", "amount", "effectiveAt", "expiresAt", "state"} {
					if _, ok := g[field]; !ok {
						t.Errorf("grant missing field %s: %v", field, g)
					}
				}
				if g["state"] != "active" {
					t.Errorf("state=%v", g["state"])
				}
			}
		})
	}
}

func TestListGrantsEmptyAndTenantIsolation(t *testing.T) {
	h := New(newFakeStore(), nil)
	status, got := doRequest(t, h, "GET", "/tenants/nobody/entitlements?at=2026-01-01T00:00:00Z", "")
	if status != 200 {
		t.Fatalf("status=%d", status)
	}
	list, ok := got["grants"].([]any)
	if !ok || len(list) != 0 {
		t.Fatalf("grants must be an empty array, got %v", got["grants"])
	}

	if status, _ = doRequest(t, h, "POST", "/tenants/acme/entitlements", validGrant); status != 201 {
		t.Fatal("seed failed")
	}
	status, got = doRequest(t, h, "GET", "/tenants/other/entitlements?at=2026-01-15T00:00:00Z", "")
	if status != 200 {
		t.Fatalf("status=%d", status)
	}
	if list, _ = got["grants"].([]any); len(list) != 0 {
		t.Fatalf("tenant isolation failed: %v", list)
	}
}

func TestListGrantsValidationAndFailure(t *testing.T) {
	h := New(newFakeStore(), nil)
	for _, target := range []string{
		"/tenants/acme/entitlements",
		"/tenants/acme/entitlements?at=",
		"/tenants/acme/entitlements?at=soon",
	} {
		status, got := doRequest(t, h, "GET", target, "")
		if status != http.StatusBadRequest {
			t.Fatalf("GET %s status=%d body=%v, want 400", target, status, got)
		}
	}

	store := newFakeStore()
	store.failList = true
	h = New(store, nil)
	status, got := doRequest(t, h, "GET", "/tenants/acme/entitlements?at=2026-01-01T00:00:00Z", "")
	if status != http.StatusInternalServerError || got["error"] != "internal error" {
		t.Fatalf("status=%d body=%v", status, got)
	}
}

func TestCancelGrant(t *testing.T) {
	store := newFakeStore()
	h := New(store, nil)
	if status, _ := doRequest(t, h, "POST", "/tenants/acme/entitlements", validGrant); status != 201 {
		t.Fatal("seed failed")
	}

	status, got := doRequest(t, h, "POST",
		"/tenants/acme/entitlements/g-1/cancel",
		`{"cancelledAt":"2026-01-10T00:00:00Z"}`)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, got)
	}
	if got["state"] != "cancelled" || got["grantId"] != "g-1" {
		t.Fatalf("body=%v", got)
	}

	// No longer visible for any asOf, including inside the interval.
	status, got = doRequest(t, h, "GET", "/tenants/acme/entitlements?at=2026-01-15T00:00:00Z", "")
	if status != 200 {
		t.Fatalf("list status=%d", status)
	}
	if list, _ := got["grants"].([]any); len(list) != 0 {
		t.Fatalf("cancelled grant still listed: %v", list)
	}

	// Idempotent: cancelling again still succeeds and changes nothing.
	status, _ = doRequest(t, h, "POST", "/tenants/acme/entitlements/g-1/cancel",
		`{"cancelledAt":"2026-01-11T00:00:00Z"}`)
	if status != http.StatusOK {
		t.Fatalf("second cancel status=%d, want 200", status)
	}
}

func TestCancelUnknownGrant(t *testing.T) {
	store := newFakeStore()
	h := New(store, nil)
	status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements/missing/cancel",
		`{"cancelledAt":"2026-01-10T00:00:00Z"}`)
	if status != http.StatusNotFound {
		t.Fatalf("status=%d body=%v, want 404", status, got)
	}
	if _, ok := got["error"].(string); !ok {
		t.Fatal("404 response must carry an error string")
	}
}

func TestCancelValidationAndFailure(t *testing.T) {
	h := New(newFakeStore(), nil)
	for _, body := range []string{``, `{}`, `{"cancelledAt":"nope"}`} {
		status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements/g-1/cancel", body)
		if status != http.StatusBadRequest {
			t.Fatalf("body=%q status=%d body=%v, want 400", body, status, got)
		}
	}

	store := newFakeStore()
	store.failCancel = true
	h = New(store, nil)
	status, got := doRequest(t, h, "POST", "/tenants/acme/entitlements/g-1/cancel",
		`{"cancelledAt":"2026-01-10T00:00:00Z"}`)
	if status != http.StatusInternalServerError || got["error"] != "internal error" {
		t.Fatalf("status=%d body=%v", status, got)
	}
}

func TestEntitlementRouting(t *testing.T) {
	h := New(newFakeStore(), nil)
	for _, tc := range []struct {
		name, method, target, body string
		status                     int
	}{
		{"cancel wrong method", "GET", "/tenants/acme/entitlements/g-1/cancel", "", http.StatusMethodNotAllowed},
		{"create/list wrong method", "DELETE", "/tenants/acme/entitlements", "", http.StatusMethodNotAllowed},
		{"unknown route", "GET", "/tenants/acme/other", "", http.StatusNotFound},
		{"unknown deeper route", "POST", "/tenants/acme/entitlements/g-1/renew", validGrant, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _ := doRequest(t, h, tc.method, tc.target, tc.body)
			if status != tc.status {
				t.Fatalf("%s %s: status=%d want %d", tc.method, tc.target, status, tc.status)
			}
		})
	}
}
