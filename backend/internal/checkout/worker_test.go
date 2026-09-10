package checkout

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

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
	w := NewStockWorker(repo, nil, nil, pub)

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
}

func TestStockWorker_HandleReservation_InsufficientStock(t *testing.T) {
	p, repo := newWorkerTestProduct(t, 1)
	pub := &fakePublisher{}
	w := NewStockWorker(repo, nil, nil, pub)

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
}

func TestStockWorker_HandleReservation_ProductNotFound(t *testing.T) {
	_, repo := newWorkerTestProduct(t, 5)
	pub := &fakePublisher{}
	w := NewStockWorker(repo, nil, nil, pub)

	w.handleReservation(context.Background(), 0, Request{ID: "req-1", ProductID: "does-not-exist", Quantity: 1})

	evt := decodeReservationEvent(t, pub.last(t).value)
	if evt.Status != StatusRejected {
		t.Errorf("status = %q, want %q", evt.Status, StatusRejected)
	}
}

func TestStockWorker_HandleRelease_Success(t *testing.T) {
	p, repo := newWorkerTestProduct(t, 3)
	pub := &fakePublisher{}
	w := NewStockWorker(repo, nil, nil, pub)

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
