// Package httpapi exposes the quota pool and reservation HTTP API.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lansolocoder/cla03-entitlement-service/internal/store"
)

// maxBodyBytes caps request bodies to keep malformed uploads bounded.
const maxBodyBytes = 1 << 16

// dbTimeout bounds every database call made while serving a request.
const dbTimeout = 5 * time.Second

type api struct {
	store   *store.Store
	checkDB func(context.Context) error
}

// New builds the HTTP handler. checkDatabase powers the readiness probe and
// st provides quota persistence.
func New(checkDatabase func(context.Context) error, st *store.Store) http.Handler {
	a := &api{store: st, checkDB: checkDatabase}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("GET /readyz", a.ready)

	mux.HandleFunc("PUT /v1/tenants/{tenantId}/quota-pools/{poolId}", a.putPool)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations", a.createReservation)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{reservationId}/confirm", a.confirmReservation)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{reservationId}/release", a.releaseReservation)
	mux.HandleFunc("GET /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{reservationId}", a.getReservation)

	return mux
}

func (a *api) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if a.checkDB == nil {
		respond(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
		return
	}
	if err := a.checkDB(ctx); err != nil {
		log.Printf("readiness check failed: %v", err)
		respond(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
		return
	}
	respond(w, http.StatusOK, `{"status":"ready"}`)
}

type putPoolRequest struct {
	Limit      *int64 `json:"limit"`
	ValidFrom  string `json:"validFrom"`
	ValidUntil string `json:"validUntil"`
}

func (a *api) putPool(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")

	var req putPoolRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.Limit == nil || *req.Limit <= 0 ||
		req.ValidFrom == "" || req.ValidUntil == "" {
		fail(w, http.StatusBadRequest)
		return
	}
	validFrom, err := time.Parse(time.RFC3339, req.ValidFrom)
	if err != nil {
		fail(w, http.StatusBadRequest)
		return
	}
	validUntil, err := time.Parse(time.RFC3339, req.ValidUntil)
	if err != nil {
		fail(w, http.StatusBadRequest)
		return
	}
	if !validFrom.Before(validUntil) {
		fail(w, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), dbTimeout)
	defer cancel()
	pool, err := a.store.PutPool(ctx, store.Pool{
		TenantID:   tenantID,
		PoolID:     poolID,
		Limit:      int(*req.Limit),
		ValidFrom:  validFrom.UTC(),
		ValidUntil: validUntil.UTC(),
	})
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, pool)
}

type createReservationRequest struct {
	ReservationID string `json:"reservationId"`
	Amount        *int64 `json:"amount"`
	ExpiresAt     string `json:"expiresAt"`
}

func (a *api) createReservation(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")

	// Idempotency-Key is mandatory for every POST and is enforced before the
	// request body is interpreted.
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}

	var req createReservationRequest
	raw, ok := readStrictBody(w, r, &req)
	if !ok {
		return
	}
	// Syntactic/field legality is 400 and never touches the database; the
	// idempotency claim is taken only for well-formed requests, so business
	// preconditions are decided strictly after idempotency.
	if req.ReservationID == "" || req.Amount == nil || *req.Amount <= 0 || req.ExpiresAt == "" {
		fail(w, http.StatusBadRequest)
		return
	}
	expiresAt, err := time.Parse(time.RFC3339, req.ExpiresAt)
	if err != nil {
		fail(w, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), dbTimeout)
	defer cancel()
	result, replayed, err := a.store.WithIdempotency(ctx, tenantID, key, r.Method, r.URL.Path, raw,
		func(tx pgx.Tx, now time.Time) (store.Result, error) {
			return a.doCreateReservation(ctx, tx, now, tenantID, poolID, req, expiresAt.UTC())
		})
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	// An identical replay always answers 200 with the original result, even
	// though the first response was 201 (or a 409).
	respondReplay(w, result, replayed)
}

func (a *api) doCreateReservation(ctx context.Context, tx pgx.Tx, now time.Time, tenantID, poolID string, req createReservationRequest, expiresAt time.Time) (store.Result, error) {
	created, err := store.CreateReservation(ctx, tx, now, store.Reservation{
		ReservationID: req.ReservationID,
		TenantID:      tenantID,
		PoolID:        poolID,
		Amount:        int(*req.Amount),
		ExpiresAt:     expiresAt,
	})
	if err != nil {
		return businessResult(err)
	}
	return jsonResult(http.StatusCreated, created)
}

func (a *api) confirmReservation(w http.ResponseWriter, r *http.Request) {
	a.transition(w, r, store.ConfirmReservation)
}

func (a *api) releaseReservation(w http.ResponseWriter, r *http.Request) {
	a.transition(w, r, store.ReleaseReservation)
}

type transitionFunc func(context.Context, pgx.Tx, time.Time, string, string, string) (store.Reservation, error)

func (a *api) transition(w http.ResponseWriter, r *http.Request, do transitionFunc) {
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")
	reservationID := r.PathValue("reservationId")

	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	// Action endpoints carry no request body; anything else is a malformed
	// request and never reaches storage.
	raw, ok := readStrictBody(w, r, nil)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), dbTimeout)
	defer cancel()
	result, replayed, err := a.store.WithIdempotency(ctx, tenantID, key, r.Method, r.URL.Path, raw,
		func(tx pgx.Tx, now time.Time) (store.Result, error) {
			updated, err := do(ctx, tx, now, tenantID, poolID, reservationID)
			if err != nil {
				return businessResult(err)
			}
			return jsonResult(http.StatusOK, updated)
		})
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	respondReplay(w, result, replayed)
}

func (a *api) getReservation(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")
	reservationID := r.PathValue("reservationId")

	ctx, cancel := context.WithTimeout(r.Context(), dbTimeout)
	defer cancel()
	reservation, err := a.store.GetReservation(ctx, tenantID, poolID, reservationID)
	if err != nil {
		writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reservation)
}

// businessResult converts a business-rule outcome into a stored idempotent
// Result (so an identical replay returns the same answer). Only genuine
// storage failures are returned as errors, rolling the transaction back and
// surfacing as 503.
func businessResult(err error) (store.Result, error) {
	if status, ok := businessStatus(err); ok {
		return errorResult(status)
	}
	return store.Result{}, err
}

// businessStatus maps a business-rule error to its public status code. The
// second result is false for errors that are not business outcomes (storage
// failures), which must surface as 503.
func businessStatus(err error) (int, bool) {
	switch {
	case errors.Is(err, store.ErrReservationNotFound):
		return http.StatusNotFound, true
	case errors.Is(err, store.ErrPoolNotFound),
		errors.Is(err, store.ErrPoolExists),
		errors.Is(err, store.ErrReservationExists),
		errors.Is(err, store.ErrPoolInactive),
		errors.Is(err, store.ErrInsufficientQuota),
		errors.Is(err, store.ErrInvalidExpiry),
		errors.Is(err, store.ErrNotPending):
		return http.StatusConflict, true
	default:
		return 0, false
	}
}

func jsonResult(status int, v any) (store.Result, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return store.Result{}, err
	}
	return store.Result{Status: status, Body: body}, nil
}

func errorResult(status int) (store.Result, error) {
	messages := map[int]string{
		http.StatusNotFound: "not found",
		http.StatusConflict: "conflict",
	}
	body, err := json.Marshal(map[string]string{"error": messages[status]})
	if err != nil {
		return store.Result{}, err
	}
	return store.Result{Status: status, Body: body}, nil
}

// writeStoreError maps errors that escaped a store call to public status
// codes. Business-rule errors map to 404/409; anything else is a storage
// failure (503). Internal details are logged but never sent to clients.
func writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	if status, ok := businessStatus(err); ok {
		fail(w, status)
		return
	}
	if errors.Is(err, store.ErrIdempotencyMismatch) {
		fail(w, http.StatusConflict)
		return
	}
	log.Printf("%s %s: storage failure: %v", r.Method, r.URL.Path, err)
	fail(w, http.StatusServiceUnavailable)
}

// idempotencyKey extracts and validates the mandatory Idempotency-Key header.
func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 255 {
		fail(w, http.StatusBadRequest)
		return "", false
	}
	return key, true
}

// readStrictBody reads a JSON request body, rejecting oversized payloads,
// malformed JSON, unknown fields and trailing content. It returns the
// canonicalized bytes used for idempotency fingerprinting. dst may be nil for
// endpoints that require an empty body.
func readStrictBody(w http.ResponseWriter, r *http.Request, dst any) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		fail(w, http.StatusBadRequest)
		return nil, false
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		if dst != nil {
			fail(w, http.StatusBadRequest)
			return nil, false
		}
		return nil, true
	}
	if dst == nil {
		// Bodyless action endpoints accept an empty object as a synonym for
		// no payload; any actual content is a malformed request.
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &obj); err != nil || len(obj) != 0 {
			fail(w, http.StatusBadRequest)
			return nil, false
		}
		return nil, true
	}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		fail(w, http.StatusBadRequest)
		return nil, false
	}
	if dec.More() {
		fail(w, http.StatusBadRequest)
		return nil, false
	}
	canonical, err := store.CanonicalPayload(raw)
	if err != nil {
		fail(w, http.StatusBadRequest)
		return nil, false
	}
	return canonical, true
}

// decodeStrict decodes a mandatory JSON body, rejecting unknown fields and
// trailing content. It writes a 400 itself and returns false on any problem.
func decodeStrict(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		fail(w, http.StatusBadRequest)
		return false
	}
	if dec.More() {
		fail(w, http.StatusBadRequest)
		return false
	}
	return true
}

func fail(w http.ResponseWriter, status int) {
	messages := map[int]string{
		http.StatusBadRequest:         "invalid request",
		http.StatusNotFound:           "not found",
		http.StatusConflict:           "conflict",
		http.StatusServiceUnavailable: "service unavailable",
	}
	writeJSON(w, status, map[string]string{"error": messages[status]})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		log.Printf("failed to encode response: %v", err)
		respond(w, http.StatusServiceUnavailable, `{"error":"service unavailable"}`)
		return
	}
	respond(w, status, string(body))
}

func respondReplay(w http.ResponseWriter, result store.Result, replayed bool) {
	status := result.Status
	if replayed {
		// A replay of an idempotent request always carries 200, along with
		// the original result body, regardless of the first status code.
		status = http.StatusOK
	}
	respond(w, status, string(result.Body))
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
