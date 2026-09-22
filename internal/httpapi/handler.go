package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// API types -------------------------------------------------------------

type poolInput struct {
	limit      int64
	validFrom  time.Time
	validUntil time.Time
}

type reservationInput struct {
	reservationID string
	teamID        string // empty means the reservation is not attributed to a team
	amount        int64
	expiresAt     time.Time
}

type allocationInput struct {
	amount          int64
	expectedVersion int64
}

type poolResponse struct {
	TenantID   string    `json:"tenantId"`
	PoolID     string    `json:"poolId"`
	Limit      int64     `json:"limit"`
	ValidFrom  time.Time `json:"validFrom"`
	ValidUntil time.Time `json:"validUntil"`
	CreatedAt  time.Time `json:"createdAt"`
}

type reservationResponse struct {
	TenantID      string    `json:"tenantId"`
	PoolID        string    `json:"poolId"`
	ReservationID string    `json:"reservationId"`
	TeamID        *string   `json:"teamId,omitempty"`
	Amount        int64     `json:"amount"`
	Status        string    `json:"status"`
	ExpiresAt     time.Time `json:"expiresAt"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

type allocationResponse struct {
	TeamID    string `json:"teamId"`
	Allocated int64  `json:"allocated"`
	Used      int64  `json:"used"`
	Available int64  `json:"available"`
	Version   int64  `json:"version"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// operationResult is the outcome of a store operation. status is the HTTP
// status to return; body is the already-serialized response. Replays of a
// completed idempotent operation carry status 200 and the original body.
type operationResult struct {
	status int
	body   []byte
}

type backend interface {
	Ping(ctx context.Context) error
	createPool(ctx context.Context, tenantID, poolID string, in poolInput) (poolResponse, bool, error)
	createReservation(ctx context.Context, tenantID, poolID, idempotencyKey, requestHash string, in reservationInput) (operationResult, error)
	decideReservation(ctx context.Context, tenantID, poolID, reservationID, idempotencyKey, requestHash, decision string) (operationResult, error)
	getReservation(ctx context.Context, tenantID, poolID, reservationID string) (reservationResponse, error)
	setAllocation(ctx context.Context, tenantID, poolID, teamID, idempotencyKey, requestHash string, in allocationInput) (operationResult, error)
	getAllocation(ctx context.Context, tenantID, poolID, teamID string) (allocationResponse, error)
}

// Routing ---------------------------------------------------------------

func New(b backend) http.Handler {
	mux := http.NewServeMux()
	api := &api{backend: b}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := b.Ping(ctx); err != nil {
			respond(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
			return
		}
		respond(w, http.StatusOK, `{"status":"ready"}`)
	})

	mux.HandleFunc("PUT /v1/tenants/{tenantId}/quota-pools/{poolId}", api.putPool)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations", api.postReservation)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{reservationId}/confirm", api.confirmReservation)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{reservationId}/release", api.releaseReservation)
	// Alternate "R/{reservationId}:confirm|release" action syntax.
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{idAction}", api.reservationAction)
	mux.HandleFunc("GET /v1/tenants/{tenantId}/quota-pools/{poolId}/reservations/{reservationId}", api.getReservation)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/quota-pools/{poolId}/teams/{teamId}/allocation", api.postAllocation)
	mux.HandleFunc("GET /v1/tenants/{tenantId}/quota-pools/{poolId}/teams/{teamId}/allocation", api.getAllocation)

	return mux
}

type api struct {
	backend backend
}

// Request bodies --------------------------------------------------------

type putPoolRequest struct {
	Limit      *int64  `json:"limit"`
	ValidFrom  *string `json:"validFrom"`
	ValidUntil *string `json:"validUntil"`
}

type postReservationRequest struct {
	ReservationID *string `json:"reservationId"`
	TeamID        *string `json:"teamId"`
	Amount        *int64  `json:"amount"`
	ExpiresAt     *string `json:"expiresAt"`
}

type postAllocationRequest struct {
	Amount          *int64 `json:"amount"`
	ExpectedVersion *int64 `json:"expectedVersion"`
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		badRequest(w, "request body must be a valid JSON object")
		return false
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		badRequest(w, "request body must contain a single JSON object")
		return false
	}
	return true
}

func parseRFC3339(value *string) (time.Time, bool) {
	if value == nil || strings.TrimSpace(*value) == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func (a *api) idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		badRequest(w, "Idempotency-Key header is required")
		return "", false
	}
	return key, true
}

func requestHash(r *http.Request, canonical []byte) string {
	sum := sha256.Sum256([]byte(r.Method + "\n" + r.URL.Path + "\n" + string(canonical)))
	return hex.EncodeToString(sum[:])
}

// Handlers --------------------------------------------------------------

func (a *api) putPool(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")

	var req putPoolRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Limit == nil || *req.Limit < 1 {
		badRequest(w, "limit must be a positive integer")
		return
	}
	validFrom, ok := parseRFC3339(req.ValidFrom)
	if !ok {
		badRequest(w, "validFrom must be an RFC3339 timestamp")
		return
	}
	validUntil, ok := parseRFC3339(req.ValidUntil)
	if !ok {
		badRequest(w, "validUntil must be an RFC3339 timestamp")
		return
	}
	if !validFrom.Before(validUntil) {
		badRequest(w, "validFrom must be earlier than validUntil")
		return
	}

	pool, created, err := a.backend.createPool(r.Context(), tenantID, poolID, poolInput{
		limit:      *req.Limit,
		validFrom:  validFrom,
		validUntil: validUntil,
	})
	if err != nil {
		storageFailed(w, err)
		return
	}
	if !created {
		fail(w, http.StatusConflict, errorResponse{Error: "quota pool already exists"})
		return
	}
	writeJSON(w, http.StatusCreated, pool)
}

func (a *api) postReservation(w http.ResponseWriter, r *http.Request) {
	key, ok := a.idempotencyKey(w, r)
	if !ok {
		return
	}
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")

	var req postReservationRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.ReservationID == nil || strings.TrimSpace(*req.ReservationID) == "" {
		badRequest(w, "reservationId is required")
		return
	}
	if req.Amount == nil || *req.Amount < 1 {
		badRequest(w, "amount must be a positive integer")
		return
	}
	expiresAt, ok := parseRFC3339(req.ExpiresAt)
	if !ok {
		badRequest(w, "expiresAt must be an RFC3339 timestamp")
		return
	}
	teamID := ""
	if req.TeamID != nil {
		teamID = strings.TrimSpace(*req.TeamID)
		if teamID == "" {
			badRequest(w, "teamId must not be blank")
			return
		}
	}

	in := reservationInput{
		reservationID: strings.TrimSpace(*req.ReservationID),
		teamID:        teamID,
		amount:        *req.Amount,
		expiresAt:     expiresAt,
	}
	canonical, _ := json.Marshal(struct {
		ReservationID string `json:"reservationId"`
		TeamID        string `json:"teamId,omitempty"`
		Amount        int64  `json:"amount"`
		ExpiresAt     string `json:"expiresAt"`
	}{in.reservationID, in.teamID, in.amount, in.expiresAt.Format(time.RFC3339Nano)})

	result, err := a.backend.createReservation(r.Context(), tenantID, poolID, key, requestHash(r, canonical), in)
	if err != nil {
		storageFailed(w, err)
		return
	}
	writeJSON(w, result.status, result.body)
}

func (a *api) confirmReservation(w http.ResponseWriter, r *http.Request) {
	a.decide(w, r, r.PathValue("reservationId"), "confirmed")
}

func (a *api) releaseReservation(w http.ResponseWriter, r *http.Request) {
	a.decide(w, r, r.PathValue("reservationId"), "released")
}

func (a *api) reservationAction(w http.ResponseWriter, r *http.Request) {
	// Accept the "R/{reservationId}:confirm" and ":release" action syntax.
	tail := r.PathValue("idAction")
	reservationID, action, ok := strings.Cut(tail, ":")
	if !ok || reservationID == "" {
		respond(w, http.StatusNotFound, `{"error":"not found"}`)
		return
	}
	switch action {
	case "confirm":
		a.decide(w, r, reservationID, "confirmed")
	case "release":
		a.decide(w, r, reservationID, "released")
	default:
		respond(w, http.StatusNotFound, `{"error":"not found"}`)
	}
}

func (a *api) decide(w http.ResponseWriter, r *http.Request, reservationID, decision string) {
	key, ok := a.idempotencyKey(w, r)
	if !ok {
		return
	}
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")
	hash := requestHash(r, nil)

	result, err := a.backend.decideReservation(r.Context(), tenantID, poolID, reservationID, key, hash, decision)
	if err != nil {
		storageFailed(w, err)
		return
	}
	writeJSON(w, result.status, result.body)
}

func (a *api) getReservation(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")
	reservationID := r.PathValue("reservationId")

	reservation, err := a.backend.getReservation(r.Context(), tenantID, poolID, reservationID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			fail(w, http.StatusNotFound, errorResponse{Error: "reservation not found"})
			return
		}
		storageFailed(w, err)
		return
	}
	writeJSON(w, http.StatusOK, reservation)
}

func (a *api) postAllocation(w http.ResponseWriter, r *http.Request) {
	key, ok := a.idempotencyKey(w, r)
	if !ok {
		return
	}
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")
	teamID := r.PathValue("teamId")

	var req postAllocationRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Amount == nil || *req.Amount < 0 {
		badRequest(w, "amount must be a non-negative integer")
		return
	}
	if req.ExpectedVersion == nil || *req.ExpectedVersion < 0 {
		badRequest(w, "expectedVersion must be a non-negative integer")
		return
	}

	in := allocationInput{amount: *req.Amount, expectedVersion: *req.ExpectedVersion}
	canonical, _ := json.Marshal(struct {
		Amount          int64 `json:"amount"`
		ExpectedVersion int64 `json:"expectedVersion"`
	}{in.amount, in.expectedVersion})

	result, err := a.backend.setAllocation(r.Context(), tenantID, poolID, teamID, key, requestHash(r, canonical), in)
	if err != nil {
		storageFailed(w, err)
		return
	}
	writeJSON(w, result.status, result.body)
}

func (a *api) getAllocation(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantId")
	poolID := r.PathValue("poolId")
	teamID := r.PathValue("teamId")

	allocation, err := a.backend.getAllocation(r.Context(), tenantID, poolID, teamID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			fail(w, http.StatusNotFound, errorResponse{Error: "team allocation not found"})
			return
		}
		storageFailed(w, err)
		return
	}
	writeJSON(w, http.StatusOK, allocation)
}

// Response helpers ------------------------------------------------------

func badRequest(w http.ResponseWriter, message string) {
	fail(w, http.StatusBadRequest, errorResponse{Error: message})
}

func storageFailed(w http.ResponseWriter, err error) {
	// Internal details go to the server log only, never to the client.
	log.Printf("storage error: %v", err)
	fail(w, http.StatusServiceUnavailable, errorResponse{Error: "service temporarily unavailable"})
}

func fail(w http.ResponseWriter, status int, v any) {
	writeJSON(w, status, v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var body []byte
	switch b := v.(type) {
	case []byte:
		body = b
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			status = http.StatusServiceUnavailable
			body = []byte(`{"error":"service temporarily unavailable"}`)
			break
		}
		body = encoded
	}
	respond(w, status, string(body))
}

func respond(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body + "\n"))
}
