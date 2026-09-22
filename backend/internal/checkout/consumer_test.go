package checkout

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"flash-sales/backend/internal/order"
)

// newTestConsumer builds an EventConsumer with no Kafka reader --
// process never touches it, only Run does -- and a confirmationLatency
// of 0 plus an injectable randFloat, so tests are instant and
// deterministic instead of waiting a real 3 seconds for a "roughly
// 80%" outcome.
func newTestConsumer(orderPublisher Publisher, orderRepo order.Repository, releases chan<- ReleaseRequest, randFloat func() float64) *EventConsumer {
	return &EventConsumer{
		orderPublisher:      orderPublisher,
		orderRepo:           orderRepo,
		releases:            releases,
		confirmationLatency: 0,
		randFloat:           randFloat,
	}
}

func reservationMessage(t *testing.T, evt ReservationEvent) kafka.Message {
	t.Helper()
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to encode ReservationEvent: %v", err)
	}
	return kafka.Message{Value: body}
}

func decodeOrderStatusEvent(t *testing.T, body []byte) OrderStatusEvent {
	t.Helper()
	var evt OrderStatusEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		t.Fatalf("failed to decode OrderStatusEvent: %v", err)
	}
	return evt
}

func TestEventConsumer_Process_AlreadyRejectedPassesThrough(t *testing.T) {
	pub := &fakePublisher{}
	releases := make(chan ReleaseRequest, 1)
	orderRepo := order.NewInMemoryRepository()
	seedTestOrder(t, orderRepo, "req-1", "prod-1", 1, 1000)
	// randFloat should never even be called for this path -- panic if it is.
	c := newTestConsumer(pub, orderRepo, releases, func() float64 {
		t.Fatal("randFloat should not be called for an already-rejected reservation")
		return 0
	})

	msg := reservationMessage(t, ReservationEvent{
		RequestID: "req-1", ProductID: "prod-1", Quantity: 1,
		Status: StatusRejected, Reason: "product: insufficient stock",
	})
	c.process(context.Background(), msg)

	evt := decodeOrderStatusEvent(t, pub.last(t).value)
	if evt.Status != OrderStatusRejected {
		t.Errorf("status = %q, want %q", evt.Status, OrderStatusRejected)
	}
	if evt.Reason != "product: insufficient stock" {
		t.Errorf("reason = %q, want the original reservation-stage reason", evt.Reason)
	}

	select {
	case rel := <-releases:
		t.Fatalf("expected no release for an already-rejected reservation, got %+v", rel)
	default:
	}

	ord, err := orderRepo.FindByRequestID(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("failed to find order: %v", err)
	}
	if ord.Status() != order.StatusRejected {
		t.Errorf("order status = %q, want %q", ord.Status(), order.StatusRejected)
	}
}

func TestEventConsumer_Process_ApprovedOnLowRoll(t *testing.T) {
	pub := &fakePublisher{}
	releases := make(chan ReleaseRequest, 1)
	orderRepo := order.NewInMemoryRepository()
	seedTestOrder(t, orderRepo, "req-1", "prod-1", 2, 1000)
	if err := orderRepo.MarkReserved(context.Background(), "req-1"); err != nil {
		t.Fatalf("failed to seed reserved order: %v", err)
	}
	c := newTestConsumer(pub, orderRepo, releases, func() float64 { return 0 }) // 0 < orderApprovalRate -> approved

	msg := reservationMessage(t, ReservationEvent{
		RequestID: "req-1", ProductID: "prod-1", Quantity: 2, Status: StatusReserved,
	})
	c.process(context.Background(), msg)

	evt := decodeOrderStatusEvent(t, pub.last(t).value)
	if evt.Status != OrderStatusApproved {
		t.Errorf("status = %q, want %q", evt.Status, OrderStatusApproved)
	}

	select {
	case rel := <-releases:
		t.Fatalf("expected no release for an approved order, got %+v", rel)
	default:
	}

	ord, err := orderRepo.FindByRequestID(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("failed to find order: %v", err)
	}
	if ord.Status() != order.StatusApproved {
		t.Errorf("order status = %q, want %q", ord.Status(), order.StatusApproved)
	}
}

func TestEventConsumer_Process_RejectedOnHighRoll_ReleasesStock(t *testing.T) {
	pub := &fakePublisher{}
	releases := make(chan ReleaseRequest, 1)
	orderRepo := order.NewInMemoryRepository()
	seedTestOrder(t, orderRepo, "req-1", "prod-1", 2, 1000)
	if err := orderRepo.MarkReserved(context.Background(), "req-1"); err != nil {
		t.Fatalf("failed to seed reserved order: %v", err)
	}
	c := newTestConsumer(pub, orderRepo, releases, func() float64 { return 0.99 }) // >= orderApprovalRate -> rejected

	msg := reservationMessage(t, ReservationEvent{
		RequestID: "req-1", ProductID: "prod-1", Quantity: 2, Status: StatusReserved,
	})
	c.process(context.Background(), msg)

	evt := decodeOrderStatusEvent(t, pub.last(t).value)
	if evt.Status != OrderStatusRejected {
		t.Errorf("status = %q, want %q", evt.Status, OrderStatusRejected)
	}

	select {
	case rel := <-releases:
		if rel.RequestID != "req-1" || rel.ProductID != "prod-1" || rel.Quantity != 2 {
			t.Errorf("unexpected release request: %+v", rel)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a ReleaseRequest to be sent for a confirmation-stage rejection")
	}

	ord, err := orderRepo.FindByRequestID(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("failed to find order: %v", err)
	}
	if ord.Status() != order.StatusRejected {
		t.Errorf("order status = %q, want %q", ord.Status(), order.StatusRejected)
	}
}

// markFailingOrderRepo is a fake order.Repository whose Mark*
// methods always error, used to confirm process() still publishes
// the OrderStatusEvent even when the order-row update fails -- the
// best-effort behavior described in worker.go/consumer.go.
type markFailingOrderRepo struct{ order.Repository }

func (markFailingOrderRepo) MarkApproved(ctx context.Context, requestID string) error {
	return errors.New("boom")
}

func (markFailingOrderRepo) MarkRejected(ctx context.Context, requestID, reason string) error {
	return errors.New("boom")
}

func TestEventConsumer_Process_OrderRepoFailure_StillPublishes(t *testing.T) {
	pub := &fakePublisher{}
	releases := make(chan ReleaseRequest, 1)
	c := newTestConsumer(pub, markFailingOrderRepo{}, releases, func() float64 { return 0 })

	msg := reservationMessage(t, ReservationEvent{
		RequestID: "req-1", ProductID: "prod-1", Quantity: 1, Status: StatusReserved,
	})
	c.process(context.Background(), msg)

	if pub.count() != 1 {
		t.Fatalf("expected process to still publish despite the order-repo failure, got %d messages", pub.count())
	}
}
