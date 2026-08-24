package main

import (
	"log"
	"net/http"

	"flash-sales/backend/internal/product"
)

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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /products", productHandler.List)

	addr := ":8080"
	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
