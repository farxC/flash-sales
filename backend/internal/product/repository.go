package product

// Repository provides read access to the product catalog.
type Repository interface {
	List() ([]*Product, error)
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
