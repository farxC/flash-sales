package checkout

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand/v2"
	"time"

	"github.com/segmentio/kafka-go"
)

const ReservationsConsumerGroup = "checkout-reservations-consumer"

// confirmationLatency is a fake delay simulating a slow downstream
// confirmation step (e.g. payment processing). It makes the async
// gap between "reserved" and "approved/rejected" actually observable
// instead of resolving near-instantly.
const confirmationLatency = 3 * time.Second

// orderApprovalRate is the probability that a successfully-reserved
// order is approved during confirmation, simulating a downstream
// step (e.g. payment) that doesn't always succeed even though stock
// was available.
const orderApprovalRate = 0.8

// EventConsumer is worker B: the downstream consumer of reservation
// outcomes. For every event with status "reserved", it simulates an
// order confirmation step by randomly approving it (80% of the time)
// or rejecting it (20%), then publishes the final OrderStatusEvent to
// order.status. A rejected confirmation releases the reserved stock
// back to the pool via a ReleaseRequest sent to the stock worker.
// Events that were already rejected at the reservation stage (e.g.
// out of stock) pass straight through as rejected orders, with no
// randomization and nothing to release.
//
// Offsets are committed manually, after an event has been fully
// processed (at-least-once delivery): if the process crashes between
// reading a message and committing it, the message is redelivered on
// restart rather than silently lost.
type EventConsumer struct {
	reader         *kafka.Reader
	orderPublisher *EventPublisher
	releases       chan<- ReleaseRequest
}

func NewEventConsumer(brokerAddr string, orderPublisher *EventPublisher, releases chan<- ReleaseRequest) *EventConsumer {
	return &EventConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{brokerAddr},
			Topic:   ReservationsTopic,
			GroupID: ReservationsConsumerGroup,
		}),
		orderPublisher: orderPublisher,
		releases:       releases,
	}
}

func (c *EventConsumer) Run(ctx context.Context) {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("checkout: failed to fetch event: %v", err)
			continue
		}

		c.process(ctx, msg)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("checkout: failed to commit offset for event: %v", err)
		}
	}
}

func (c *EventConsumer) process(ctx context.Context, msg kafka.Message) {
	var evt ReservationEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		log.Printf("checkout: failed to decode event: %v", err)
		return
	}

	log.Printf("checkout event: request=%s product=%s qty=%d status=%s reason=%q",
		evt.RequestID, evt.ProductID, evt.Quantity, evt.Status, evt.Reason)

	orderEvt := OrderStatusEvent{
		RequestID: evt.RequestID,
		ProductID: evt.ProductID,
		Quantity:  evt.Quantity,
	}

	if evt.Status == StatusRejected {
		// Already failed at the reservation stage -- nothing to
		// confirm, nothing to release.
		orderEvt.Status = OrderStatusRejected
		orderEvt.Reason = evt.Reason
	} else if !sleep(ctx, confirmationLatency) {
		// Context was canceled (shutdown) while "confirming" -- drop
		// this event rather than publish with a canceled context.
		return
	} else if rand.Float64() < orderApprovalRate {
		orderEvt.Status = OrderStatusApproved
	} else {
		orderEvt.Status = OrderStatusRejected
		orderEvt.Reason = "order rejected during confirmation"
		c.releases <- ReleaseRequest{RequestID: evt.RequestID, ProductID: evt.ProductID, Quantity: evt.Quantity}
	}

	body, err := json.Marshal(orderEvt)
	if err != nil {
		log.Printf("checkout: failed to encode order status event for request %s: %v", evt.RequestID, err)
		return
	}
	if err := c.orderPublisher.Publish(ctx, orderEvt.ProductID, body); err != nil {
		log.Printf("checkout: failed to publish order status event for request %s: %v", evt.RequestID, err)
	}
}

func (c *EventConsumer) Close() error {
	return c.reader.Close()
}

// sleep waits out d, returning false early if ctx is canceled first.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
