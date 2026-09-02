package product

import (
	"context"
	"errors"
)

var ErrProductNotFound = errors.New("product: not found")

// Repository provides access to the product catalog. DecrementStock
// and ReleaseStock are expected to be atomic -- safe to call from any
// number of concurrent callers without an external lock.
type Repository interface {
	List(ctx context.Context) ([]*Product, error)
	FindByID(ctx context.Context, id string) (*Product, error)
	DecrementStock(ctx context.Context, id string, qty int) error
	ReleaseStock(ctx context.Context, id string, qty int) error
}

// InMemoryRepository is a Repository backed by a fixed in-memory
// slice, seeded once at construction. Useful as a lightweight test
// double -- the running application uses PostgresRepository, whose
// atomic SQL statements are what actually enforce the invariant
// under concurrency. This implementation is NOT safe for concurrent
// callers.
type InMemoryRepository struct {
	products []*Product
}

func NewInMemoryRepository(products []*Product) *InMemoryRepository {
	return &InMemoryRepository{products: products}
}

func (r *InMemoryRepository) List(ctx context.Context) ([]*Product, error) {
	return r.products, nil
}

func (r *InMemoryRepository) FindByID(ctx context.Context, id string) (*Product, error) {
	for _, p := range r.products {
		if p.ID() == id {
			return p, nil
		}
	}
	return nil, ErrProductNotFound
}

func (r *InMemoryRepository) DecrementStock(ctx context.Context, id string, qty int) error {
	p, err := r.FindByID(ctx, id)
	if err != nil {
		return err
	}
	return p.DecrementStock(qty)
}

func (r *InMemoryRepository) ReleaseStock(ctx context.Context, id string, qty int) error {
	p, err := r.FindByID(ctx, id)
	if err != nil {
		return err
	}
	return p.ReleaseStock(qty)
}
