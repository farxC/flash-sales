package order

import (
	"context"
	"errors"
)

var (
	ErrOrderNotFound      = errors.New("order: not found")
	ErrDuplicateRequestID = errors.New("order: request id already used")
)

// Repository persists Orders and drives their status transitions,
// keyed by RequestID rather than primary key -- see the request_id
// column comment in schema.sql for why.
type Repository interface {
	Create(ctx context.Context, o *Order) error
	MarkReserved(ctx context.Context, requestID string) error
	MarkApproved(ctx context.Context, requestID string) error
	MarkRejected(ctx context.Context, requestID, reason string) error
	FindByRequestID(ctx context.Context, requestID string) (*Order, error)
}

// InMemoryRepository is a Repository backed by a map keyed by
// RequestID. Like product.InMemoryRepository, it's a lightweight test
// double and is NOT safe for concurrent callers.
type InMemoryRepository struct {
	orders map[string]*Order
}

func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{orders: make(map[string]*Order)}
}

func (r *InMemoryRepository) Create(ctx context.Context, o *Order) error {
	if _, exists := r.orders[o.RequestID()]; exists {
		return ErrDuplicateRequestID
	}
	r.orders[o.RequestID()] = o
	return nil
}

func (r *InMemoryRepository) FindByRequestID(ctx context.Context, requestID string) (*Order, error) {
	o, ok := r.orders[requestID]
	if !ok {
		return nil, ErrOrderNotFound
	}
	return o, nil
}

func (r *InMemoryRepository) MarkReserved(ctx context.Context, requestID string) error {
	o, err := r.FindByRequestID(ctx, requestID)
	if err != nil {
		return err
	}
	return o.MarkReserved()
}

func (r *InMemoryRepository) MarkApproved(ctx context.Context, requestID string) error {
	o, err := r.FindByRequestID(ctx, requestID)
	if err != nil {
		return err
	}
	return o.MarkApproved()
}

func (r *InMemoryRepository) MarkRejected(ctx context.Context, requestID, reason string) error {
	o, err := r.FindByRequestID(ctx, requestID)
	if err != nil {
		return err
	}
	return o.MarkRejected(reason)
}
