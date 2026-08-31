package product

import (
	"encoding/json"
	"net/http"
)

type Handler struct {
	repo Repository
}

func NewHandler(repo Repository) *Handler {
	return &Handler{repo: repo}
}

type productResponse struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	ValueInCents int64  `json:"valueInCents"`
	Stock        int    `json:"stock"`
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	products, err := h.repo.List(r.Context())
	if err != nil {
		http.Error(w, "failed to list products", http.StatusInternalServerError)
		return
	}

	response := make([]productResponse, 0, len(products))
	for _, p := range products {
		response = append(response, productResponse{
			ID:           p.ID(),
			Name:         p.Name(),
			Description:  p.Description(),
			ValueInCents: p.ValueInCents(),
			Stock:        p.Stock(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
