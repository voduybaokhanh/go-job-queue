package api

import (
	"net/http"
)

// NewRouter creates a new HTTP router with all API routes
func NewRouter(handler *Handler) http.Handler {
	mux := http.NewServeMux()

	// API v1 routes
	mux.HandleFunc("POST /api/v1/jobs", handler.CreateJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}", handler.GetJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", handler.CancelJob)

	// Health check endpoint
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return mux
}

