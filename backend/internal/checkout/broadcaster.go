package checkout

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/segmentio/kafka-go"
)

const OrderStatusBroadcasterGroup = "order-status-broadcaster"

// OrderStatusBroadcaster is worker C: it consumes order.status and
// fans each event out, over Server-Sent Events, to every browser
// currently connected -- a different async pattern than the other
// two workers (broadcast-to-many-listeners instead of
// consume-once-from-a-queue). Clients that aren't connected when an
// event is published simply never see it; there's no replay.
type OrderStatusBroadcaster struct {
	reader *kafka.Reader

	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func NewOrderStatusBroadcaster(brokerAddr string) *OrderStatusBroadcaster {
	return &OrderStatusBroadcaster{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{brokerAddr},
			Topic:   OrderStatusTopic,
			GroupID: OrderStatusBroadcasterGroup,
		}),
		clients: make(map[chan []byte]struct{}),
	}
}

func (b *OrderStatusBroadcaster) Run(ctx context.Context) {
	for {
		msg, err := b.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("checkout: broadcaster failed to fetch event: %v", err)
			continue
		}

		b.broadcast(msg.Value)

		if err := b.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("checkout: broadcaster failed to commit offset: %v", err)
		}
	}
}

func (b *OrderStatusBroadcaster) broadcast(body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.clients {
		select {
		case ch <- body:
		default:
			// Slow client -- drop this event for them rather than
			// block the broadcast for everyone else.
		}
	}
}

func (b *OrderStatusBroadcaster) subscribe() chan []byte {
	ch := make(chan []byte, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *OrderStatusBroadcaster) unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
}

// ServeSSE streams every order.status event to the connected client
// for as long as the connection stays open.
func (b *OrderStatusBroadcaster) ServeSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := b.subscribe()
	defer b.unsubscribe(ch)

	for {
		select {
		case <-r.Context().Done():
			return
		case body := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", body)
			flusher.Flush()
		}
	}
}

func (b *OrderStatusBroadcaster) Close() error {
	return b.reader.Close()
}
