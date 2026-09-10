package checkout

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// newTestBroadcaster builds an OrderStatusBroadcaster with no Kafka
// reader -- subscribe/broadcast/unsubscribe/ServeSSE never touch it,
// only Run does, so these tests never need a live Kafka connection.
func newTestBroadcaster() *OrderStatusBroadcaster {
	return &OrderStatusBroadcaster{clients: make(map[chan []byte]struct{})}
}

func TestOrderStatusBroadcaster_SubscribeReceivesBroadcast(t *testing.T) {
	b := newTestBroadcaster()
	ch := b.subscribe()

	b.broadcast([]byte("event-1"))

	select {
	case got := <-ch:
		if string(got) != "event-1" {
			t.Errorf("got %q, want %q", got, "event-1")
		}
	default:
		t.Fatal("expected subscriber to receive the broadcast event")
	}
}

func TestOrderStatusBroadcaster_BroadcastReachesAllSubscribers(t *testing.T) {
	b := newTestBroadcaster()
	ch1 := b.subscribe()
	ch2 := b.subscribe()

	b.broadcast([]byte("event-1"))

	for i, ch := range []chan []byte{ch1, ch2} {
		select {
		case got := <-ch:
			if string(got) != "event-1" {
				t.Errorf("subscriber %d: got %q, want %q", i, got, "event-1")
			}
		default:
			t.Fatalf("subscriber %d: expected to receive the broadcast event", i)
		}
	}
}

func TestOrderStatusBroadcaster_UnsubscribeStopsDelivery(t *testing.T) {
	b := newTestBroadcaster()
	ch := b.subscribe()
	b.unsubscribe(ch)

	b.broadcast([]byte("event-1"))

	select {
	case got := <-ch:
		t.Fatalf("unsubscribed client received an event: %q", got)
	default:
		// Expected: nothing delivered.
	}
}

func TestOrderStatusBroadcaster_SlowClientDoesNotBlockBroadcast(t *testing.T) {
	b := newTestBroadcaster()
	ch := b.subscribe()

	// Fill the subscriber's buffer completely without draining it, so
	// the next broadcast has nowhere to put this client's copy.
	for range cap(ch) {
		ch <- []byte("filler")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.broadcast([]byte("event-that-should-be-dropped"))
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broadcast blocked on a full subscriber instead of dropping the event for them")
	}
}

// syncRecorder is a minimal, concurrency-safe http.ResponseWriter +
// http.Flusher. httptest.ResponseRecorder's buffer isn't safe to
// read from one goroutine while ServeSSE writes from another --
// exactly the situation this test needs, since ServeSSE loops until
// the request context is canceled.
type syncRecorder struct {
	mu     sync.Mutex
	header http.Header
	body   strings.Builder
}

func newSyncRecorder() *syncRecorder {
	return &syncRecorder{header: make(http.Header)}
}

func (r *syncRecorder) Header() http.Header { return r.header }

func (r *syncRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Write(p)
}

func (r *syncRecorder) WriteHeader(int) {}

func (r *syncRecorder) Flush() {}

func (r *syncRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.String()
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestOrderStatusBroadcaster_ServeSSE(t *testing.T) {
	b := newTestBroadcaster()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.ServeSSE(rec, req)
	}()

	// Wait until ServeSSE has actually subscribed before broadcasting
	// -- there's no replay, so an event published before the
	// subscription exists would simply be lost.
	waitUntil(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.clients) == 1
	})

	b.broadcast([]byte(`{"hello":"world"}`))

	// The exact SSE wire format ServeSSE is supposed to produce --
	// checking for this directly means there's nothing left to
	// re-verify after cancellation, just a final read of the same
	// (by-then-stable) body.
	const wantFrame = "data: {\"hello\":\"world\"}\n\n"
	waitUntil(t, func() bool {
		return strings.Contains(rec.String(), wantFrame)
	})

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ServeSSE did not return after its context was canceled")
	}

	b.mu.Lock()
	remaining := len(b.clients)
	b.mu.Unlock()
	if remaining != 0 {
		t.Errorf("client still subscribed after ServeSSE returned: %d remaining", remaining)
	}
}
