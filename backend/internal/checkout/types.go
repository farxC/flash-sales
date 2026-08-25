package checkout

// Request is a checkout attempt waiting to be processed by the stock
// worker. It carries only what's needed to decide whether a
// reservation can be made.
type Request struct {
	ID        string
	ProductID string
	Quantity  int
}

// ReservationStatus is the outcome of processing a Request.
type ReservationStatus string

const (
	StatusReserved ReservationStatus = "reserved"
	StatusRejected ReservationStatus = "rejected"
)

// ReservationEvent is the outcome published after a Request has been
// processed. It's the unit of data that crosses from the stock
// worker (producer) to Kafka to whatever consumes it downstream.
type ReservationEvent struct {
	RequestID string            `json:"requestId"`
	ProductID string            `json:"productId"`
	Quantity  int               `json:"quantity"`
	Status    ReservationStatus `json:"status"`
	Reason    string            `json:"reason,omitempty"`
}

// ReleaseRequest asks the stock worker to return previously-reserved
// units back to the pool -- the compensating action for a
// reservation that was rejected downstream, after stock had already
// been decremented.
type ReleaseRequest struct {
	RequestID string
	ProductID string
	Quantity  int
}

// OrderStatus is the final outcome of an order, decided downstream
// of a successful stock reservation.
type OrderStatus string

const (
	OrderStatusApproved OrderStatus = "approved"
	OrderStatusRejected OrderStatus = "rejected"
)

// OrderStatusEvent is published to the order.status topic once an
// order's fate -- approved or rejected -- has been decided.
type OrderStatusEvent struct {
	RequestID string      `json:"requestId"`
	ProductID string      `json:"productId"`
	Quantity  int         `json:"quantity"`
	Status    OrderStatus `json:"status"`
	Reason    string      `json:"reason,omitempty"`
}
