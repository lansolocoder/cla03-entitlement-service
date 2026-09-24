package httpapi

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lansolocoder/cla03-entitlement-service/internal/entitlement"
)

// fakeService is an in-memory entitlement.Service used by the HTTP tests.
// It enforces the same lifecycle rules as the PostgreSQL store so routing,
// status codes and response bodies can be exercised without a database.
type fakeService struct {
	mu           sync.Mutex
	entitlements map[string]*entitlement.Entitlement
	allocations  map[string]map[string]int // id -> member -> allocated
	usages       map[string][]fakeUsage    // id -> usage records
	keys         map[string]string         // id|usageKey -> "member|qty"
	nextID       int
	now          time.Time
}

type fakeUsage struct {
	memberID string
	quantity int
	usageKey string
}

func newFakeService() *fakeService {
	return &fakeService{
		entitlements: map[string]*entitlement.Entitlement{},
		allocations:  map[string]map[string]int{},
		usages:       map[string][]fakeUsage{},
		keys:         map[string]string{},
		now:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (f *fakeService) status(e *entitlement.Entitlement) entitlement.Status {
	if e.Status == entitlement.Active && !f.now.Before(e.ExpiresAt) {
		return entitlement.Expired
	}
	return e.Status
}

func (f *fakeService) snapshot(e *entitlement.Entitlement) *entitlement.Entitlement {
	c := *e
	c.Status = f.status(e)
	return &c
}

func (f *fakeService) Create(_ context.Context, params entitlement.CreateParams) (*entitlement.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entitlements {
		if e.TeamID == params.TeamID && e.Type == params.Type && e.ExpiresAt.Equal(params.ExpiresAt) {
			return nil, &entitlement.ConflictError{
				APIError: entitlement.APIError{Kind: entitlement.KindConflict, Code: "entitlement_already_exists",
					Message: "an entitlement with the same team, type and expiry already exists"},
				Existing: f.snapshot(e),
			}
		}
	}
	f.nextID++
	e := &entitlement.Entitlement{
		ID:        fmt.Sprintf("ent_%d", f.nextID),
		TeamID:    params.TeamID,
		Type:      params.Type,
		Total:     params.Total,
		Status:    entitlement.Active,
		ExpiresAt: params.ExpiresAt,
		Used:      0,
		Version:   1,
	}
	f.entitlements[e.ID] = e
	f.allocations[e.ID] = map[string]int{}
	return f.snapshot(e), nil
}

func (f *fakeService) Get(_ context.Context, id string) (*entitlement.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entitlements[id]
	if !ok {
		return nil, notFoundErr()
	}
	return f.snapshot(e), nil
}

func notFoundErr() error {
	return &entitlement.APIError{Kind: entitlement.KindNotFound, Code: "entitlement_not_found", Message: "entitlement does not exist"}
}

func (f *fakeService) requireOperable(id string) (*entitlement.Entitlement, error) {
	e, ok := f.entitlements[id]
	if !ok {
		return nil, notFoundErr()
	}
	switch f.status(e) {
	case entitlement.Cancelled:
		return nil, &entitlement.APIError{Kind: entitlement.KindConflict, Code: "entitlement_cancelled", Message: "cancelled"}
	case entitlement.Expired:
		return nil, &entitlement.APIError{Kind: entitlement.KindConflict, Code: "entitlement_expired", Message: "expired"}
	}
	return e, nil
}

func (f *fakeService) Allocate(_ context.Context, id, memberID string, quantity int) (*entitlement.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, err := f.requireOperable(id)
	if err != nil {
		return nil, err
	}
	if e.Used+quantity > e.Total {
		return nil, &entitlement.APIError{Kind: entitlement.KindConflict, Code: "allocation_exceeds_total", Message: "exceeds total"}
	}
	f.allocations[id][memberID] += quantity
	e.Used += quantity
	e.Version++
	return f.snapshot(e), nil
}

func (f *fakeService) Use(_ context.Context, id, memberID string, quantity int, usageKey string) (*entitlement.Entitlement, entitlement.UsageRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, exists := f.entitlements[id]
	if !exists {
		return nil, entitlement.UsageRecord{}, false, notFoundErr()
	}
	key := id + "\x00" + usageKey
	if first, ok := f.keys[key]; ok {
		parts := strings.SplitN(first, "\x00", 2)
		firstMember := parts[0]
		firstQty, _ := strconv.Atoi(parts[1])
		if firstMember != memberID || firstQty != quantity {
			return nil, entitlement.UsageRecord{}, false, &entitlement.UsageKeyConflictError{
				APIError: entitlement.APIError{Kind: entitlement.KindConflict, Code: "usage_key_reused", Message: "key reused"},
				Existing: entitlement.UsageRecord{MemberID: firstMember, Quantity: firstQty, UsageKey: usageKey},
			}
		}
		for _, u := range f.usages[id] {
			if u.usageKey == usageKey {
				return f.snapshot(e),
					entitlement.UsageRecord{MemberID: u.memberID, Quantity: u.quantity, UsageKey: u.usageKey},
					true, nil
			}
		}
	}
	if _, err := f.requireOperable(id); err != nil {
		return nil, entitlement.UsageRecord{}, false, err
	}
	consumed := 0
	for _, u := range f.usages[id] {
		if u.memberID == memberID {
			consumed += u.quantity
		}
	}
	remaining := f.allocations[id][memberID] - consumed
	if remaining < quantity {
		return nil, entitlement.UsageRecord{}, false,
			&entitlement.APIError{Kind: entitlement.KindConflict, Code: "usage_exceeds_member_remaining", Message: "exceeds remaining"}
	}
	f.usages[id] = append(f.usages[id], fakeUsage{memberID, quantity, usageKey})
	f.keys[key] = memberID + "\x00" + strconv.Itoa(quantity)
	e.Used += quantity
	e.Version++
	return f.snapshot(e), entitlement.UsageRecord{MemberID: memberID, Quantity: quantity, UsageKey: usageKey}, false, nil
}

func (f *fakeService) Cancel(_ context.Context, id string) (*entitlement.Entitlement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, err := f.requireOperable(id)
	if err != nil {
		return nil, err
	}
	e.Status = entitlement.Cancelled
	e.Version++
	return f.snapshot(e), nil
}

func (f *fakeService) Detail(_ context.Context, id string) (*entitlement.Detail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entitlements[id]
	if !ok {
		return nil, notFoundErr()
	}
	detail := &entitlement.Detail{Entitlement: *f.snapshot(e), Members: []entitlement.MemberUsage{}}
	for memberID := range f.allocations[id] {
		mu := entitlement.MemberUsage{MemberID: memberID, UsageKeys: []string{}}
		for _, u := range f.usages[id] {
			if u.memberID == memberID {
				mu.Used += u.quantity
				mu.UsageKeys = append(mu.UsageKeys, u.usageKey)
			}
		}
		detail.Members = append(detail.Members, mu)
	}
	return detail, nil
}

// advance moves the fake clock and returns the new time.
func (f *fakeService) advance(d time.Duration) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	return f.now
}
