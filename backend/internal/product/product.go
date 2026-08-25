package product

import "errors"

var (
	ErrEmptyName         = errors.New("product: name must not be empty")
	ErrNegativePrice     = errors.New("product: value in cents must not be negative")
	ErrNegativeStock     = errors.New("product: stock must not be negative")
	ErrInvalidQuantity   = errors.New("product: quantity must be positive")
	ErrInsufficientStock = errors.New("product: insufficient stock")
)

// Product is the aggregate root for the flash-sale catalog.
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

// DecrementStock applies a reservation of qty units.
//
// This method holds no lock of its own. It is safe under concurrency
// only because exactly one goroutine (the checkout package's stock
// worker) is ever allowed to call it for a given Product — the
// invariant is protected by serializing access at the caller, not by
// synchronizing here. Do not call this from more than one goroutine.
func (p *Product) DecrementStock(qty int) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	if qty > p.stock {
		return ErrInsufficientStock
	}
	p.stock -= qty
	return nil
}

// ReleaseStock returns qty previously-reserved units back to the
// pool -- the compensating action for a reservation that was later
// rejected downstream (e.g. a failed order confirmation).
//
// Like DecrementStock, this method holds no lock of its own and is
// safe under concurrency only because exactly one goroutine ever
// calls it for a given Product.
func (p *Product) ReleaseStock(qty int) error {
	if qty <= 0 {
		return ErrInvalidQuantity
	}
	p.stock += qty
	return nil
}
