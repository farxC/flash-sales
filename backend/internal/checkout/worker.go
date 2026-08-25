package checkout

import (
	"context"
	"encoding/json"
	"log"

	"flash-sales/backend/internal/product"
)

// StockWorker is the sole goroutine allowed to mutate product stock.
// Serializing every checkout request through this single worker is
// what guarantees the stock invariant holds under concurrency -- no
// lock is needed on Product itself because there is exactly one
// writer. After deciding the outcome of a request, it publishes a
// ReservationEvent to Kafka for whatever consumes it downstream. It
// also accepts ReleaseRequests -- the compensating action sent back
// by the order-status consumer when a reservation is rejected
// downstream -- on a separate channel, since releasing stock is a
// second kind of mutation that must go through the same sole writer.
type StockWorker struct {
	repo      product.Repository
	requests  <-chan Request
	releases  <-chan ReleaseRequest
	publisher *EventPublisher
}

func NewStockWorker(repo product.Repository, requests <-chan Request, releases <-chan ReleaseRequest, publisher *EventPublisher) *StockWorker {
	return &StockWorker{repo: repo, requests: requests, releases: releases, publisher: publisher}
}

func (w *StockWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-w.requests:
			w.handleReservation(ctx, req)
		case rel := <-w.releases:
			w.handleRelease(rel)
		}
	}
}

func (w *StockWorker) handleReservation(ctx context.Context, req Request) {
	evt := ReservationEvent{
		RequestID: req.ID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
	}

	p, err := w.repo.FindByID(req.ProductID)
	if err != nil {
		evt.Status = StatusRejected
		evt.Reason = "product not found"
	} else if err := p.DecrementStock(req.Quantity); err != nil {
		evt.Status = StatusRejected
		evt.Reason = err.Error()
	} else {
		evt.Status = StatusReserved
	}

	body, err := json.Marshal(evt)
	if err != nil {
		log.Printf("checkout: failed to encode event for request %s: %v", req.ID, err)
		return
	}
	if err := w.publisher.Publish(ctx, evt.ProductID, body); err != nil {
		log.Printf("checkout: failed to publish event for request %s: %v", req.ID, err)
	}
}

func (w *StockWorker) handleRelease(rel ReleaseRequest) {
	p, err := w.repo.FindByID(rel.ProductID)
	if err != nil {
		log.Printf("checkout: failed to release stock for request %s: %v", rel.RequestID, err)
		return
	}
	if err := p.ReleaseStock(rel.Quantity); err != nil {
		log.Printf("checkout: failed to release stock for request %s: %v", rel.RequestID, err)
	}
}
