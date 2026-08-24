package product

import "errors"

var (
	ErrEmptyName     = errors.New("product: name must not be empty")
	ErrNegativePrice = errors.New("product: value in cents must not be negative")
	ErrNegativeStock = errors.New("product: stock must not be negative")
)

// Product is the aggregate root for the flash-sale catalog. Stock
// mutation is deliberately not exposed here yet — the purchase flow
// (a dedicated worker serializing access) will own that invariant
// when it's introduced.
type Product struct {
	id           string
	name         string
	description  string
	valueInCents int64
	stock        int
}

func NewProduct(id, name, description string, valueInCents int64, stock int) (*Product, error) {
	if name == "" {
		return nil, ErrEmptyName
	}
	if valueInCents < 0 {
		return nil, ErrNegativePrice
	}
	if stock < 0 {
		return nil, ErrNegativeStock
	}

	return &Product{
		id:           id,
		name:         name,
		description:  description,
		valueInCents: valueInCents,
		stock:        stock,
	}, nil
}

func (p *Product) ID() string { return p.id }

func (p *Product) Name() string { return p.name }

func (p *Product) Description() string { return p.description }

func (p *Product) ValueInCents() int64 { return p.valueInCents }

func (p *Product) Stock() int { return p.stock }
