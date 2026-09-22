package order

import "errors"

var (
	ErrEmptyRequestID    = errors.New("order: request id must not be empty")
	ErrEmptyConsumerID   = errors.New("order: consumer id must not be empty")
	ErrNoItems           = errors.New("order: must have at least one item")
	ErrEmptyProductID    = errors.New("order: item product id must not be empty")
	ErrInvalidQuantity   = errors.New("order: item quantity must be positive")
	ErrNegativeItemValue = errors.New("order: item value in cents must not be negative")
	ErrInvalidTransition = errors.New("order: invalid status transition")
)

// GuestConsumerID is the one fixed consumer every order is placed
// under -- buyer identity and auth are deliberately out of scope for
// this project (the focus is event-driven architecture, not
// e-commerce). Must match the seed row in backend/db/schema.sql.
const GuestConsumerID = "00000000-0000-0000-0000-000000000001"

// Status is where an Order sits in the checkout pipeline's lifecycle.
type Status string

const (
	StatusPending  Status = "pending"
	StatusReserved Status = "reserved"
	StatusApproved Status = "approved"
	StatusRejected Status = "rejected"
)

// Item is an internal entity of the Order aggregate, not a separate
// aggregate root. valueInCents is a unit-price snapshot taken at
// order-creation time, independent of the product's price moving
// later.
type Item struct {
	productID    string
	quantity     int
	valueInCents int64
}

func NewItem(productID string, quantity int, valueInCents int64) (Item, error) {
	if productID == "" {
		return Item{}, ErrEmptyProductID
	}
	if quantity <= 0 {
		return Item{}, ErrInvalidQuantity
	}
	if valueInCents < 0 {
		return Item{}, ErrNegativeItemValue
	}
	return Item{productID: productID, quantity: quantity, valueInCents: valueInCents}, nil
}

func (i Item) ProductID() string   { return i.productID }
func (i Item) Quantity() int       { return i.quantity }
func (i Item) ValueInCents() int64 { return i.valueInCents }

// Order is the aggregate root for a checkout attempt's persisted
// state. A fresh Order has no id until Repository.Create persists it
// (Postgres mints the primary key) -- nothing downstream needs it
// back, since the whole pipeline correlates by RequestID instead (see
// the request_id column comment in schema.sql).
type Order struct {
	id                string
	requestID         string
	consumerID        string
	items             []Item
	totalValueInCents int64
	status            Status
}

func NewOrder(requestID, consumerID string, items []Item) (*Order, error) {
	if requestID == "" {
		return nil, ErrEmptyRequestID
	}
	if consumerID == "" {
		return nil, ErrEmptyConsumerID
	}
	if len(items) == 0 {
		return nil, ErrNoItems
	}

	return &Order{
		requestID:         requestID,
		consumerID:        consumerID,
		items:             items,
		totalValueInCents: sumItems(items),
		status:            StatusPending,
	}, nil
}

// RehydrateOrder reconstructs an Order from an already-persisted row,
// including its DB-assigned id and current status.
func RehydrateOrder(id, requestID, consumerID string, items []Item, status Status) (*Order, error) {
	o, err := NewOrder(requestID, consumerID, items)
	if err != nil {
		return nil, err
	}
	o.id = id
	o.status = status
	return o, nil
}

func sumItems(items []Item) int64 {
	var total int64
	for _, it := range items {
		total += int64(it.quantity) * it.valueInCents
	}
	return total
}

func (o *Order) ID() string               { return o.id }
func (o *Order) RequestID() string        { return o.requestID }
func (o *Order) ConsumerID() string       { return o.consumerID }
func (o *Order) Items() []Item            { return o.items }
func (o *Order) TotalValueInCents() int64 { return o.totalValueInCents }
func (o *Order) Status() Status           { return o.status }

// MarkReserved transitions a pending order to reserved -- called once
// StockWorker successfully decrements stock.
func (o *Order) MarkReserved() error {
	if o.status != StatusPending {
		return ErrInvalidTransition
	}
	o.status = StatusReserved
	return nil
}

// MarkApproved transitions a reserved order to approved -- called
// once EventConsumer's simulated confirmation step succeeds.
func (o *Order) MarkApproved() error {
	if o.status != StatusReserved {
		return ErrInvalidTransition
	}
	o.status = StatusApproved
	return nil
}

// MarkRejected transitions pending or reserved to rejected -- covers
// both the immediate out-of-stock case (pending, no reservation ever
// happened) and a confirmation-stage rejection (reserved). reason is
// accepted for parity with ReservationEvent/OrderStatusEvent's Reason
// field but is not stored on the Order -- it only ever flows through
// Kafka/SSE, same as today.
func (o *Order) MarkRejected(reason string) error {
	if o.status != StatusPending && o.status != StatusReserved {
		return ErrInvalidTransition
	}
	o.status = StatusRejected
	return nil
}
