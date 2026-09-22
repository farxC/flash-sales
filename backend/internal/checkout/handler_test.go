package checkout

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"flash-sales/backend/internal/order"
	"flash-sales/backend/internal/product"
)

func seedProduct(t *testing.T) (*product.Product, *product.InMemoryRepository) {
	t.Helper()

	p, err := product.NewProduct("prod-1", "Widget", "a test widget", 1000, 5)
	if err != nil {
		t.Fatalf("failed to build seed product: %v", err)
	}
	return p, product.NewInMemoryRepository([]*product.Product{p})
}

func doCheckout(t *testing.T, handler *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/checkout", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.Checkout(rec, req)
	return rec
}

func TestCheckout_InvalidBody(t *testing.T) {
	_, repo := seedProduct(t)
	requests := make(chan Request, 1)
	handler := NewHandler(repo, order.NewInMemoryRepository(), requests)

	rec := doCheckout(t, handler, `not json`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestCheckout_InvalidQuantity(t *testing.T) {
	p, repo := seedProduct(t)
	requests := make(chan Request, 1)
	handler := NewHandler(repo, order.NewInMemoryRepository(), requests)

	rec := doCheckout(t, handler, `{"productId":"`+p.ID()+`","quantity":0}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestCheckout_ProductNotFound(t *testing.T) {
	_, repo := seedProduct(t)
	requests := make(chan Request, 1)
	handler := NewHandler(repo, order.NewInMemoryRepository(), requests)

	rec := doCheckout(t, handler, `{"productId":"does-not-exist","quantity":1}`)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestCheckout_Success(t *testing.T) {
	p, repo := seedProduct(t)
	requests := make(chan Request, 1)
	handler := NewHandler(repo, order.NewInMemoryRepository(), requests)

	rec := doCheckout(t, handler, `{"productId":"`+p.ID()+`","quantity":2}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}

	var resp checkoutResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if resp.RequestID == "" {
		t.Fatal("response requestId is empty")
	}

	select {
	case req := <-requests:
		if req.ID != resp.RequestID {
			t.Errorf("enqueued request id = %q, want %q (must match the response)", req.ID, resp.RequestID)
		}
		if req.ProductID != p.ID() {
			t.Errorf("enqueued productId = %q, want %q", req.ProductID, p.ID())
		}
		if req.Quantity != 2 {
			t.Errorf("enqueued quantity = %d, want 2", req.Quantity)
		}
	default:
		t.Fatal("expected a Request to be enqueued, channel was empty")
	}
}

func TestCheckout_Success_CreatesOrder(t *testing.T) {
	p, repo := seedProduct(t)
	requests := make(chan Request, 1)
	orderRepo := order.NewInMemoryRepository()
	handler := NewHandler(repo, orderRepo, requests)

	rec := doCheckout(t, handler, `{"productId":"`+p.ID()+`","quantity":2}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}

	var resp checkoutResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	ord, err := orderRepo.FindByRequestID(context.Background(), resp.RequestID)
	if err != nil {
		t.Fatalf("expected order to have been created: %v", err)
	}
	if ord.Status() != order.StatusPending {
		t.Errorf("status = %q, want %q", ord.Status(), order.StatusPending)
	}
	if ord.ConsumerID() != order.GuestConsumerID {
		t.Errorf("consumerID = %q, want %q", ord.ConsumerID(), order.GuestConsumerID)
	}
	if len(ord.Items()) != 1 || ord.Items()[0].ProductID() != p.ID() || ord.Items()[0].Quantity() != 2 {
		t.Errorf("unexpected items: %+v", ord.Items())
	}
	if want := p.ValueInCents() * 2; ord.TotalValueInCents() != want {
		t.Errorf("total = %d, want %d", ord.TotalValueInCents(), want)
	}
}

// createFailingOrderRepo is a fake order.Repository whose Create
// always errors, used to verify Checkout fails closed on
// order-creation failure -- unlike StockWorker/EventConsumer's later
// best-effort order-status updates, this is the one point where
// nothing else has happened yet, so refusing the whole attempt is
// cheap and correct.
type createFailingOrderRepo struct{ order.Repository }

func (createFailingOrderRepo) Create(ctx context.Context, o *order.Order) error {
	return errors.New("boom")
}

func TestCheckout_OrderRepoFailure_Returns500AndDoesNotEnqueue(t *testing.T) {
	p, repo := seedProduct(t)
	requests := make(chan Request, 1)
	handler := NewHandler(repo, createFailingOrderRepo{}, requests)

	rec := doCheckout(t, handler, `{"productId":"`+p.ID()+`","quantity":1}`)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	select {
	case req := <-requests:
		t.Fatalf("expected nothing enqueued, got %+v", req)
	default:
	}
}

func TestCheckout_RejectsWhenQueueFull(t *testing.T) {
	p, repo := seedProduct(t)
	requests := make(chan Request, 1)
	requests <- Request{ID: "already-queued"} // fill the only slot
	handler := NewHandler(repo, order.NewInMemoryRepository(), requests)

	rec := doCheckout(t, handler, `{"productId":"`+p.ID()+`","quantity":1}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
