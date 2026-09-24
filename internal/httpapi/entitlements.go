package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

var keyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,31}$`)

// parseQuantity accepts only a plain JSON integer literal in [1, 1000000];
// strings, fractions and exponents are rejected.
func parseQuantity(raw json.RawMessage) (int64, error) {
	text := string(raw)
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, errors.New("not an integer")
		}
	}
	quantity, err := strconv.ParseInt(text, 10, 64)
	if err != nil || quantity < 1 || quantity > 1000000 {
		return 0, errors.New("quantity out of range")
	}
	return quantity, nil
}

const maxBodyBytes = 1 << 20

type entitlementJSON struct {
	ID        string `json:"id"`
	Tenant    string `json:"tenant"`
	Key       string `json:"key"`
	Quantity  int64  `json:"quantity"`
	Consumed  int64  `json:"consumed"`
	Reserved  int64  `json:"reserved"`
	Status    string `json:"status"`
	CreatedAt string `json:"createdAt"`
	ExpiresAt string `json:"expiresAt"`
}

func toJSON(ent store.Entitlement) entitlementJSON {
	return entitlementJSON{
		ID:        ent.ID,
		Tenant:    ent.Tenant,
		Key:       ent.Key,
		Quantity:  ent.Quantity,
		Consumed:  ent.Consumed,
		Reserved:  ent.Reserved,
		Status:    ent.Status,
		CreatedAt: ent.CreatedAt.UTC().Format(time.RFC3339Nano),
		ExpiresAt: ent.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}
}

type createRequest struct {
	Key       string          `json:"key"`
	Quantity  json.RawMessage `json:"quantity"`
	ExpiresAt string          `json:"expiresAt"`
}

func createEntitlement(s Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant := r.PathValue("tenant")
		if tenant == "" {
			respondError(w, http.StatusNotFound, "not found")
			return
		}
		var req createRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err := decoder.Decode(&req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			respondError(w, http.StatusBadRequest, "request body must be a single JSON object")
			return
		}
		if !keyPattern.MatchString(req.Key) {
			respondError(w, http.StatusBadRequest, "key must match ^[a-z][a-z0-9_-]{1,31}$")
			return
		}
		quantity, err := parseQuantity(req.Quantity)
		if err != nil {
			respondError(w, http.StatusBadRequest, "quantity must be an integer between 1 and 1000000")
			return
		}
		expiresAt, err := time.Parse(time.RFC3339, req.ExpiresAt)
		if err != nil {
			respondError(w, http.StatusBadRequest, "expiresAt must be an RFC3339 timestamp")
			return
		}
		if _, offset := expiresAt.Zone(); offset != 0 {
			respondError(w, http.StatusBadRequest, "expiresAt must be in UTC")
			return
		}
		if !expiresAt.After(time.Now()) {
			respondError(w, http.StatusBadRequest, "expiresAt must be in the future")
			return
		}
		ent, created, err := s.CreateEntitlement(r.Context(), tenant, req.Key, quantity, expiresAt)
		if errors.Is(err, store.ErrConflict) {
			respondError(w, http.StatusConflict, "entitlement already exists with different parameters")
			return
		}
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		respondJSON(w, status, toJSON(ent))
	}
}

func listEntitlements(s Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant := r.PathValue("tenant")
		if tenant == "" {
			respondError(w, http.StatusNotFound, "not found")
			return
		}
		if r.URL.RawQuery != "" {
			respondError(w, http.StatusBadRequest, "query parameters are not supported")
			return
		}
		ents, err := s.ListEntitlements(r.Context(), tenant)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		items := make([]entitlementJSON, 0, len(ents))
		for _, ent := range ents {
			items = append(items, toJSON(ent))
		}
		respondJSON(w, http.StatusOK, map[string]any{"items": items})
	}
}

func getEntitlement(s Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant := r.PathValue("tenant")
		key := r.PathValue("key")
		if tenant == "" || !keyPattern.MatchString(key) {
			respondError(w, http.StatusNotFound, "not found")
			return
		}
		ent, err := s.GetEntitlement(r.Context(), tenant, key)
		if errors.Is(err, store.ErrNotFound) {
			respondError(w, http.StatusNotFound, "not found")
			return
		}
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		respondJSON(w, http.StatusOK, toJSON(ent))
	}
}
