package product

import (
	"context"
	"errors"
)

var ErrProductNotFound = errors.New("product: not found")

// Repository provides access to the product catalog.
type Repository interface {
	List(ctx context.Context) ([]*Product, error)
	FindByID(ctx context.Context, id string) (*Product, error)
	Save(ctx context.Context, p *Product) error
}

// InMemoryRepository is a Repository backed by a fixed in-memory
// slice, seeded once at construction. Useful as a lightweight test
// double -- the running application uses PostgresRepository.
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

func (r *InMemoryRepository) Save(ctx context.Context, p *Product) error {
	for i, existing := range r.products {
		if existing.ID() == p.ID() {
			r.products[i] = p
			return nil
		}
	}
	return ErrProductNotFound
}
