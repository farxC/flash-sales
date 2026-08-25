package product

import "errors"

var ErrProductNotFound = errors.New("product: not found")

// Repository provides read access to the product catalog.
type Repository interface {
	List() ([]*Product, error)
	FindByID(id string) (*Product, error)
}

// InMemoryRepository is a Repository backed by a fixed in-memory
// slice, seeded once at construction. It exists to stand in for a
// real datastore until a later lesson introduces one.
type InMemoryRepository struct {
	products []*Product
}

func NewInMemoryRepository(products []*Product) *InMemoryRepository {
	return &InMemoryRepository{products: products}
}

func (r *InMemoryRepository) List() ([]*Product, error) {
	return r.products, nil
}

func (r *InMemoryRepository) FindByID(id string) (*Product, error) {
	for _, p := range r.products {
		if p.ID() == id {
			return p, nil
		}
	}
	return nil, ErrProductNotFound
}
