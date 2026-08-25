package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"flash-sales/backend/internal/checkout"
	"flash-sales/backend/internal/product"
)

// checkoutQueueSize bounds how many checkout requests can be waiting
// for the stock worker at once. Once full, the handler rejects new
// requests with 503 instead of blocking -- see the /checkout design.
const checkoutQueueSize = 256

func main() {
	seedProduct, err := product.NewProduct(
		"prod-1",
		"Limited Edition Sneakers",
		"Only 100 pairs available in this flash sale.",
		1999900,
		100,
	)
	if err != nil {
		log.Fatal(err)
	}
	productRepo := product.NewInMemoryRepository([]*product.Product{seedProduct})
	productHandler := product.NewHandler(productRepo)

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	reservationPublisher := checkout.NewEventPublisher(kafkaBroker, checkout.ReservationsTopic)
	defer reservationPublisher.Close()

	orderPublisher := checkout.NewEventPublisher(kafkaBroker, checkout.OrderStatusTopic)
	defer orderPublisher.Close()

	requests := make(chan checkout.Request, checkoutQueueSize)
	releases := make(chan checkout.ReleaseRequest, checkoutQueueSize)

	consumer := checkout.NewEventConsumer(kafkaBroker, orderPublisher, releases)
	defer consumer.Close()

	broadcaster := checkout.NewOrderStatusBroadcaster(kafkaBroker)
	defer broadcaster.Close()

	stockWorker := checkout.NewStockWorker(productRepo, requests, releases, reservationPublisher)
	checkoutHandler := checkout.NewHandler(productRepo, requests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go stockWorker.Run(ctx)
	go consumer.Run(ctx)
	go broadcaster.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /products", productHandler.List)
	mux.HandleFunc("POST /checkout", checkoutHandler.Checkout)
	mux.HandleFunc("GET /events", broadcaster.ServeSSE)

	addr := ":8080"
	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, withCORS(mux)); err != nil {
		log.Fatal(err)
	}
}

// withCORS allows the frontend (running on a different origin) to
// call this API directly from the browser.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
