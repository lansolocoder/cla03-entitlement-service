package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

var noopPing = func(context.Context) error { return nil }

func doRequest(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, reader))
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid JSON %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestCreateEntitlement(t *testing.T) {
	fake := newFakeService()
	h := New(noopPing, fake)

	t.Run("success", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements",
			`{"teamId":"team-1","type":"seat","total":5,"expiresAt":"2027-01-01T00:00:00Z"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		for field, want := range map[string]any{
			"teamId": "team-1", "type": "seat", "total": float64(5),
			"status": "active", "used": float64(0), "version": float64(1),
			"expiresAt": "2027-01-01T00:00:00Z",
		} {
			if body[field] != want {
				t.Errorf("%s = %v, want %v", field, body[field], want)
			}
		}
		if body["id"] == "" || body["id"] == nil {
			t.Error("id must be returned")
		}
	})

	t.Run("quota type accepted", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements",
			`{"teamId":"team-1","type":"quota","total":100,"expiresAt":"2027-06-01T12:00:00Z"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	for _, tc := range []struct {
		name, body, code string
	}{
		{"missing teamId", `{"type":"seat","total":5,"expiresAt":"2027-01-01T00:00:00Z"}`, "invalid_teamId"},
		{"missing type", `{"teamId":"t","total":5,"expiresAt":"2027-01-01T00:00:00Z"}`, "invalid_type"},
		{"bad type", `{"teamId":"t","type":"tokens","total":5,"expiresAt":"2027-01-01T00:00:00Z"}`, "invalid_type"},
		{"missing total", `{"teamId":"t","type":"seat","expiresAt":"2027-01-01T00:00:00Z"}`, "invalid_total"},
		{"zero total", `{"teamId":"t","type":"seat","total":0,"expiresAt":"2027-01-01T00:00:00Z"}`, "invalid_total"},
		{"negative total", `{"teamId":"t","type":"seat","total":-1,"expiresAt":"2027-01-01T00:00:00Z"}`, "invalid_total"},
		{"missing expiresAt", `{"teamId":"t","type":"seat","total":5}`, "invalid_expiresAt"},
		{"bad expiresAt", `{"teamId":"t","type":"seat","total":5,"expiresAt":"2027-01-01"}`, "invalid_expiresAt"},
		{"non-UTC offset", `{"teamId":"t","type":"seat","total":5,"expiresAt":"2027-01-01T08:00:00+08:00"}`, "invalid_expiresAt"},
		{"malformed json", `{not json`, "invalid_json"},
		{"unknown field", `{"teamId":"t","type":"seat","total":5,"expiresAt":"2027-01-01T00:00:00Z","x":1}`, "invalid_json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, "POST", "/entitlements", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			body := decodeBody(t, rec)
			errObj, _ := body["error"].(map[string]any)
			if errObj["code"] != tc.code {
				t.Fatalf("code=%v want %s", errObj["code"], tc.code)
			}
		})
	}

	t.Run("duplicate returns 409 and existing record", func(t *testing.T) {
		payload := `{"teamId":"dup","type":"seat","total":3,"expiresAt":"2027-02-02T00:00:00Z"}`
		first := doRequest(t, h, "POST", "/entitlements", payload)
		if first.Code != http.StatusCreated {
			t.Fatalf("first create status=%d", first.Code)
		}
		firstID := decodeBody(t, first)["id"].(string)

		second := doRequest(t, h, "POST", "/entitlements", payload)
		if second.Code != http.StatusConflict {
			t.Fatalf("second create status=%d body=%s", second.Code, second.Body.String())
		}
		body := decodeBody(t, second)
		errObj, _ := body["error"].(map[string]any)
		if errObj["code"] != "entitlement_already_exists" {
			t.Fatalf("code=%v", errObj["code"])
		}
		existing, _ := body["entitlement"].(map[string]any)
		if existing["id"] != firstID {
			t.Fatalf("existing id=%v want %s", existing["id"], firstID)
		}
	})

	t.Run("different expiry for same team and type is allowed", func(t *testing.T) {
		first := doRequest(t, h, "POST", "/entitlements",
			`{"teamId":"renew","type":"seat","total":1,"expiresAt":"2027-03-01T00:00:00Z"}`)
		second := doRequest(t, h, "POST", "/entitlements",
			`{"teamId":"renew","type":"seat","total":1,"expiresAt":"2028-03-01T00:00:00Z"}`)
		if first.Code != 201 || second.Code != 201 {
			t.Fatalf("statuses=%d,%d", first.Code, second.Code)
		}
	})
}

func createEntitlement(t *testing.T, h http.Handler, teamID, typ string, total int, expiresAt string) string {
	t.Helper()
	rec := doRequest(t, h, "POST", "/entitlements",
		`{"teamId":"`+teamID+`","type":"`+typ+`","total":`+strconv.Itoa(total)+`,"expiresAt":"`+expiresAt+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	return decodeBody(t, rec)["id"].(string)
}

func TestAllocate(t *testing.T) {
	fake := newFakeService()
	h := New(noopPing, fake)
	id := createEntitlement(t, h, "alloc", "seat", 5, "2027-01-01T00:00:00Z")

	t.Run("success increments used and version", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/allocations",
			`{"memberId":"m-1","quantity":2}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		if body["used"] != float64(2) || body["version"] != float64(2) || body["status"] != "active" {
			t.Fatalf("unexpected body %v", body)
		}
	})

	t.Run("additional allocation accumulates per member", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/allocations",
			`{"memberId":"m-1","quantity":2}`)
		if rec.Code != http.StatusOK || decodeBody(t, rec)["used"] != float64(4) {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("over total is 409 and record unchanged", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/allocations",
			`{"memberId":"m-2","quantity":2}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		// The failed allocation must not appear in details.
		detail := doRequest(t, h, "GET", "/entitlements/"+id, "")
		members := decodeBody(t, detail)["members"].([]any)
		if len(members) != 1 {
			t.Fatalf("members=%v, failed allocation must not persist", members)
		}
	})

	t.Run("invalid payloads are 400", func(t *testing.T) {
		for _, body := range []string{
			`{"quantity":1}`,
			`{"memberId":"m-9"}`,
			`{"memberId":"m-9","quantity":0}`,
			`{"memberId":"","quantity":1}`,
		} {
			rec := doRequest(t, h, "POST", "/entitlements/"+id+"/allocations", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("body=%s status=%d want 400", body, rec.Code)
			}
		}
	})

	t.Run("unknown entitlement is 404", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/missing/allocations",
			`{"memberId":"m-1","quantity":1}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d", rec.Code)
		}
	})

	t.Run("allocation after cancel is 409", func(t *testing.T) {
		cancelled := createEntitlement(t, h, "cancel-alloc", "quota", 10, "2027-01-01T00:00:00Z")
		if rec := doRequest(t, h, "POST", "/entitlements/"+cancelled+"/cancel", ""); rec.Code != 200 {
			t.Fatalf("cancel status=%d", rec.Code)
		}
		rec := doRequest(t, h, "POST", "/entitlements/"+cancelled+"/allocations",
			`{"memberId":"m-1","quantity":1}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("allocation after expiry is 409", func(t *testing.T) {
		expired := createEntitlement(t, h, "expired-alloc", "quota", 10, "2026-06-01T00:00:00Z")
		// Fake clock starts 2026-01-01; move it past 2026-06-01.
		fake.advance(160 * 24 * time.Hour)
		detail := doRequest(t, h, "GET", "/entitlements/"+expired, "")
		if decodeBody(t, detail)["status"] != "expired" {
			t.Fatalf("expected expired, body=%s", detail.Body.String())
		}
		rec := doRequest(t, h, "POST", "/entitlements/"+expired+"/allocations",
			`{"memberId":"m-1","quantity":1}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func TestUse(t *testing.T) {
	fake := newFakeService()
	h := New(noopPing, fake)
	id := createEntitlement(t, h, "use", "quota", 100, "2027-01-01T00:00:00Z")
	alloc := doRequest(t, h, "POST", "/entitlements/"+id+"/allocations",
		`{"memberId":"m-1","quantity":10}`)
	if alloc.Code != 200 {
		t.Fatalf("alloc status=%d", alloc.Code)
	}

	t.Run("first use accumulates", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/usages",
			`{"memberId":"m-1","quantity":4,"usageKey":"key-1"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		if body["used"] != float64(14) || body["version"] != float64(3) {
			t.Fatalf("body=%v", body)
		}
		if body["replayed"] != false {
			t.Fatalf("first use must not be a replay")
		}
	})

	t.Run("same usageKey replays without accumulating", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/usages",
			`{"memberId":"m-1","quantity":4,"usageKey":"key-1"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d", rec.Code)
		}
		body := decodeBody(t, rec)
		if body["used"] != float64(14) {
			t.Fatalf("used=%v, replay must not accumulate", body["used"])
		}
		if body["replayed"] != true {
			t.Fatalf("repeated usageKey must be flagged as replay")
		}
	})

	t.Run("same key different params is 409", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/usages",
			`{"memberId":"m-2","quantity":4,"usageKey":"key-1"}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := decodeBody(t, rec)
		existing := body["existingUsage"].(map[string]any)
		if existing["memberId"] != "m-1" || existing["usageKey"] != "key-1" {
			t.Fatalf("existingUsage=%v", existing)
		}
	})

	t.Run("usage beyond member remaining is 409 and unchanged", func(t *testing.T) {
		// m-1 has 10 allocated, 4 used -> 6 remaining
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/usages",
			`{"memberId":"m-1","quantity":7,"usageKey":"key-over"}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		detail := doRequest(t, h, "GET", "/entitlements/"+id, "")
		body := decodeBody(t, detail)
		if body["used"] != float64(14) {
			t.Fatalf("used=%v, rejected usage must not accumulate", body["used"])
		}
	})

	t.Run("usage without allocation is rejected", func(t *testing.T) {
		rec := doRequest(t, h, "POST", "/entitlements/"+id+"/usages",
			`{"memberId":"m-unknown","quantity":1,"usageKey":"key-x"}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("invalid payloads are 400", func(t *testing.T) {
		for _, body := range []string{
			`{"quantity":1,"usageKey":"k"}`,
			`{"memberId":"m-1","usageKey":"k"}`,
			`{"memberId":"m-1","quantity":1}`,
			`{"memberId":"m-1","quantity":-1,"usageKey":"k"}`,
		} {
			rec := doRequest(t, h, "POST", "/entitlements/"+id+"/usages", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("body=%s status=%d", body, rec.Code)
			}
		}
	})
}

func TestCancel(t *testing.T) {
	h := New(noopPing, newFakeService())
	id := createEntitlement(t, h, "cancel", "seat", 5, "2027-01-01T00:00:00Z")

	rec := doRequest(t, h, "POST", "/entitlements/"+id+"/cancel", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["status"] != "cancelled" {
		t.Fatalf("status=%v", body["status"])
	}

	// Historical reads still work.
	detail := doRequest(t, h, "GET", "/entitlements/"+id, "")
	if detail.Code != 200 || decodeBody(t, detail)["status"] != "cancelled" {
		t.Fatalf("detail status=%d body=%s", detail.Code, detail.Body.String())
	}

	// Repeat cancel is a state conflict.
	again := doRequest(t, h, "POST", "/entitlements/"+id+"/cancel", "")
	if again.Code != http.StatusConflict {
		t.Fatalf("repeat cancel status=%d want 409", again.Code)
	}
}

func TestDetail(t *testing.T) {
	h := New(noopPing, newFakeService())
	id := createEntitlement(t, h, "detail", "quota", 100, "2027-01-01T00:00:00Z")

	doRequest(t, h, "POST", "/entitlements/"+id+"/allocations", `{"memberId":"alice","quantity":10}`)
	doRequest(t, h, "POST", "/entitlements/"+id+"/allocations", `{"memberId":"bob","quantity":5}`)
	doRequest(t, h, "POST", "/entitlements/"+id+"/usages", `{"memberId":"alice","quantity":3,"usageKey":"a1"}`)
	doRequest(t, h, "POST", "/entitlements/"+id+"/usages", `{"memberId":"alice","quantity":2,"usageKey":"a2"}`)
	doRequest(t, h, "POST", "/entitlements/"+id+"/usages", `{"memberId":"alice","quantity":3,"usageKey":"a1"}`) // replay

	rec := doRequest(t, h, "GET", "/entitlements/"+id, "")
	if rec.Code != 200 {
		t.Fatalf("status=%d", rec.Code)
	}
	body := decodeBody(t, rec)
	members := body["members"].([]any)
	byMember := map[string]map[string]any{}
	for _, m := range members {
		mu := m.(map[string]any)
		byMember[mu["memberId"].(string)] = mu
	}
	alice := byMember["alice"]
	if alice["used"] != float64(5) {
		t.Fatalf("alice used=%v want 5 (replay not double counted)", alice["used"])
	}
	keys := alice["usageKeys"].([]any)
	if len(keys) != 2 || keys[0] != "a1" || keys[1] != "a2" {
		t.Fatalf("alice keys=%v", keys)
	}
	bob := byMember["bob"]
	if bob["used"] != float64(0) {
		t.Fatalf("bob used=%v", bob["used"])
	}
	bobKeys := bob["usageKeys"].([]any)
	if len(bobKeys) != 0 {
		t.Fatalf("bob keys should be empty, got %v", bobKeys)
	}
}

func TestUnknownEntitlementAndRoutes(t *testing.T) {
	h := New(noopPing, newFakeService())
	if rec := doRequest(t, h, "GET", "/entitlements/nope", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET missing entitlement status=%d", rec.Code)
	}
	if rec := doRequest(t, h, "POST", "/entitlements/nope/usages",
		`{"memberId":"m","quantity":1,"usageKey":"k"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("use missing entitlement status=%d", rec.Code)
	}
	if rec := doRequest(t, h, "GET", "/entitlements", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET collection status=%d, want 404 (no list endpoint)", rec.Code)
	}
	if rec := doRequest(t, h, "DELETE", "/entitlements/x", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE status=%d want 405", rec.Code)
	}
}
