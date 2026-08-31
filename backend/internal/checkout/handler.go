package checkout

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"flash-sales/backend/internal/product"
)

type Handler struct {
	repo     product.Repository
	requests chan<- Request
}

func NewHandler(repo product.Repository, requests chan<- Request) *Handler {
	return &Handler{repo: repo, requests: requests}
}

type checkoutBody struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

type checkoutResponse struct {
	RequestID string `json:"requestId"`
}

func (h *Handler) Checkout(w http.ResponseWriter, r *http.Request) {
	var body checkoutBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if body.Quantity <= 0 {
		http.Error(w, "quantity must be greater than zero", http.StatusBadRequest)
		return
	}

	if _, err := h.repo.FindByID(r.Context(), body.ProductID); err != nil {
		http.Error(w, "product not found", http.StatusNotFound)
		return
	}

	requestID, err := newRequestID()
	if err != nil {
		http.Error(w, "failed to create request", http.StatusInternalServerError)
		return
	}

	select {
	case h.requests <- Request{ID: requestID, ProductID: body.ProductID, Quantity: body.Quantity}:
	default:
		http.Error(w, "system busy, try again shortly", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(checkoutResponse{RequestID: requestID})
}

func newRequestID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
