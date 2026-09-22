package checkout

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"flash-sales/backend/internal/order"
	"flash-sales/backend/internal/product"
)

// fakePublisher records every message published to it -- a test
// double for Publisher, so StockWorker/EventConsumer tests never
// need a live Kafka connection.
type fakePublisher struct {
	mu        sync.Mutex
	published []fakePublished
}

type fakePublished struct {
	key   string
	value []byte
}

func (f *fakePublisher) Publish(ctx context.Context, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, fakePublished{key: key, value: value})
	return nil
}

func (f *fakePublisher) last(t *testing.T) fakePublished {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.published) == 0 {
		t.Fatal("expected a message to have been published, none were")
	}
	return f.published[len(f.published)-1]
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func newWorkerTestProduct(t *testing.T, stock int) (*product.Product, *product.InMemoryRepository) {
	t.Helper()
	p, err := product.NewProduct("prod-1", "Widget", "test widget", 1000, stock)
	if err != nil {
		t.Fatalf("failed to build product: %v", err)
	}
	return p, product.NewInMemoryRepository([]*product.Product{p})
}

// seedTestOrder creates a pending order for requestID/productID via
// orderRepo.Create, standing in for the order checkout.Handler would
// normally have created before StockWorker/EventConsumer ever see the
// request -- these tests call handleReservation/process directly,
// bypassing the Handler.
func seedTestOrder(t *testing.T, orderRepo order.Repository, requestID, productID string, quantity int, valueInCents int64) {
	t.Helper()

	item, err := order.NewItem(productID, quantity, valueInCents)
	if err != nil {
		t.Fatalf("failed to build order item: %v", err)
	}
	ord, err := order.NewOrder(requestID, order.GuestConsumerID, []order.Item{item})
	if err != nil {
		t.Fatalf("failed to build order: %v", err)
	}
	if err := orderRepo.Create(context.Background(), ord); err != nil {
		t.Fatalf("failed to seed order: %v", err)
	}
}

func decodeReservationEvent(t *testing.T, body []byte) ReservationEvent {
	t.Helper()
	var evt ReservationEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		t.Fatalf("failed to decode ReservationEvent: %v", err)
	}
	return evt
}

func TestStockWorker_HandleReservation_Success(t *testing.T) {
	p, repo := newWorkerTestProduct(t, 5)
	pub := &fakePublisher{}
	orderRepo := order.NewInMemoryRepository()
	seedTestOrder(t, orderRepo, "req-1", p.ID(), 2, p.ValueInCents())
	w := NewStockWorker(repo, orderRepo, nil, nil, pub)

	w.handleReservation(context.Background(), 0, Request{ID: "req-1", ProductID: p.ID(), Quantity: 2})

	evt := decodeReservationEvent(t, pub.last(t).value)
	if evt.Status != StatusReserved {
		t.Errorf("status = %q, want %q", evt.Status, StatusReserved)
	}
	if evt.RequestID != "req-1" || evt.ProductID != p.ID() || evt.Quantity != 2 {
		t.Errorf("unexpected event fields: %+v", evt)
	}

	updated, err := repo.FindByID(context.Background(), p.ID())
	if err != nil {
		t.Fatalf("failed to find product: %v", err)
	}
	if updated.Stock() != 3 {
		t.Errorf("stock = %d, want 3", updated.Stock())
	}

	ord, err := orderRepo.FindByRequestID(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("failed to find order: %v", err)
	}
	if ord.Status() != order.StatusReserved {
		t.Errorf("order status = %q, want %q", ord.Status(), order.StatusReserved)
	}
}

func TestStockWorker_HandleReservation_InsufficientStock(t *testing.T) {
	p, repo := newWorkerTestProduct(t, 1)
	pub := &fakePublisher{}
	orderRepo := order.NewInMemoryRepository()
	seedTestOrder(t, orderRepo, "req-1", p.ID(), 5, p.ValueInCents())
	w := NewStockWorker(repo, orderRepo, nil, nil, pub)

	w.handleReservation(context.Background(), 0, Request{ID: "req-1", ProductID: p.ID(), Quantity: 5})

	evt := decodeReservationEvent(t, pub.last(t).value)
	if evt.Status != StatusRejected {
		t.Errorf("status = %q, want %q", evt.Status, StatusRejected)
	}
	if evt.Reason == "" {
		t.Error("expected a non-empty rejection reason")
	}

	updated, _ := repo.FindByID(context.Background(), p.ID())
	if updated.Stock() != 1 {
		t.Errorf("stock = %d, want unchanged 1", updated.Stock())
	}

	ord, err := orderRepo.FindByRequestID(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("failed to find order: %v", err)
	}
	if ord.Status() != order.StatusRejected {
		t.Errorf("order status = %q, want %q", ord.Status(), order.StatusRejected)
	}
}

func TestStockWorker_HandleReservation_ProductNotFound(t *testing.T) {
	_, repo := newWorkerTestProduct(t, 5)
	pub := &fakePublisher{}
	orderRepo := order.NewInMemoryRepository()
	seedTestOrder(t, orderRepo, "req-1", "does-not-exist", 1, 1000)
	w := NewStockWorker(repo, orderRepo, nil, nil, pub)

	w.handleReservation(context.Background(), 0, Request{ID: "req-1", ProductID: "does-not-exist", Quantity: 1})

	evt := decodeReservationEvent(t, pub.last(t).value)
	if evt.Status != StatusRejected {
		t.Errorf("status = %q, want %q", evt.Status, StatusRejected)
	}
}

func TestStockWorker_HandleRelease_Success(t *testing.T) {
	p, repo := newWorkerTestProduct(t, 5)
	pub := &fakePublisher{}
	orderRepo := order.NewInMemoryRepository()
	w := NewStockWorker(repo, orderRepo, nil, nil, pub)

	// Simulate a prior reservation of 2 units before releasing them
	// back -- releasing without ever having decremented is exactly
	// the bug ReleaseStock's initial-stock guard now rejects.
	if err := repo.DecrementStock(context.Background(), p.ID(), 2); err != nil {
		t.Fatalf("failed to seed a prior reservation: %v", err)
	}

	w.handleRelease(context.Background(), 0, ReleaseRequest{RequestID: "req-1", ProductID: p.ID(), Quantity: 2})

	updated, err := repo.FindByID(context.Background(), p.ID())
	if err != nil {
		t.Fatalf("failed to find product: %v", err)
	}
	if updated.Stock() != 5 {
		t.Errorf("stock = %d, want 5", updated.Stock())
	}
	if got := pub.count(); got != 0 {
		t.Errorf("handleRelease should not publish anything, got %d messages", got)
	}
}
