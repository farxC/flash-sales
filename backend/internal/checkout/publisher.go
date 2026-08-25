package checkout

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	ReservationsTopic = "checkout.reservations"
	OrderStatusTopic  = "order.status"
)

// EventPublisher publishes pre-encoded messages to a single Kafka
// topic. It's topic-agnostic by design so it can be reused for both
// checkout.reservations and order.status.
type EventPublisher struct {
	writer *kafka.Writer
}

func NewEventPublisher(brokerAddr, topic string) *EventPublisher {
	return &EventPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokerAddr),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *EventPublisher) Publish(ctx context.Context, key string, value []byte) error {
	log.Printf("checkout publishing: key=%s body=%s",
		key, value)
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

func (p *EventPublisher) Close() error {
	return p.writer.Close()
}
