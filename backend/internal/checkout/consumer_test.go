package checkout

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// newTestConsumer builds an EventConsumer with no Kafka reader --
// process never touches it, only Run does -- and a confirmationLatency
// of 0 plus an injectable randFloat, so tests are instant and
// deterministic instead of waiting a real 3 seconds for a "roughly
// 80%" outcome.
func newTestConsumer(orderPublisher Publisher, releases chan<- ReleaseRequest, randFloat func() float64) *EventConsumer {
	return &EventConsumer{
		orderPublisher:      orderPublisher,
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
	// randFloat should never even be called for this path -- panic if it is.
	c := newTestConsumer(pub, releases, func() float64 {
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
}

func TestEventConsumer_Process_ApprovedOnLowRoll(t *testing.T) {
	pub := &fakePublisher{}
	releases := make(chan ReleaseRequest, 1)
	c := newTestConsumer(pub, releases, func() float64 { return 0 }) // 0 < orderApprovalRate -> approved

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
}

func TestEventConsumer_Process_RejectedOnHighRoll_ReleasesStock(t *testing.T) {
	pub := &fakePublisher{}
	releases := make(chan ReleaseRequest, 1)
	c := newTestConsumer(pub, releases, func() float64 { return 0.99 }) // >= orderApprovalRate -> rejected

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
}
