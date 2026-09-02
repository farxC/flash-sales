package checkout

import (
	"context"
	"encoding/json"
	"log"

	"flash-sales/backend/internal/product"
)

// StockWorker processes checkout requests and releases pulled from
// shared channels. Multiple StockWorker.Run goroutines (a pool) can
// safely pull from the same channels concurrently -- the stock
// invariant is enforced by Postgres's atomic DecrementStock/
// ReleaseStock, not by serializing access through a single writer.
// After deciding the outcome of a request, it publishes a
// ReservationEvent to Kafka for whatever consumes it downstream. It
// also accepts ReleaseRequests -- the compensating action sent back
// by the order-status consumer when a reservation is rejected
// downstream -- on a separate channel.
type StockWorker struct {
	repo      product.Repository
	requests  <-chan Request
	releases  <-chan ReleaseRequest
	publisher *EventPublisher
}

func NewStockWorker(repo product.Repository, requests <-chan Request, releases <-chan ReleaseRequest, publisher *EventPublisher) *StockWorker {
	return &StockWorker{repo: repo, requests: requests, releases: releases, publisher: publisher}
}

func (w *StockWorker) Run(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-w.requests:
			w.handleReservation(ctx, workerID, req)
		case rel := <-w.releases:
			w.handleRelease(ctx, workerID, rel)
		}
	}
}

func (w *StockWorker) handleReservation(ctx context.Context, workerID int, req Request) {
	log.Printf("checkout: worker %d picked up reservation request %s", workerID, req.ID)

	evt := ReservationEvent{
		RequestID: req.ID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
	}

	if err := w.repo.DecrementStock(ctx, req.ProductID, req.Quantity); err != nil {
		evt.Status = StatusRejected
		evt.Reason = err.Error()
	} else {
		evt.Status = StatusReserved
	}

	body, err := json.Marshal(evt)
	if err != nil {
		log.Printf("checkout: worker %d failed to encode event for request %s: %v", workerID, req.ID, err)
		return
	}
	if err := w.publisher.Publish(ctx, evt.ProductID, body); err != nil {
		log.Printf("checkout: worker %d failed to publish event for request %s: %v", workerID, req.ID, err)
	}
}

func (w *StockWorker) handleRelease(ctx context.Context, workerID int, rel ReleaseRequest) {
	log.Printf("checkout: worker %d picked up release for request %s", workerID, rel.RequestID)

	if err := w.repo.ReleaseStock(ctx, rel.ProductID, rel.Quantity); err != nil {
		log.Printf("checkout: worker %d failed to release stock for request %s: %v", workerID, rel.RequestID, err)
	}
}
