package http

import (
	"encoding/json"
	"net/http"

	"github.com/nireo/sagasu/registry"
)

// NewServer creates a new HTTP server for the registry
func NewServer(addr string, store *registry.Store) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		state, err := store.GetState()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(state)
	})
	mux.HandleFunc("POST /state", func(w http.ResponseWriter, r *http.Request) {
		var state registry.State
		if err := json.NewDecoder(r.Body).Decode(&state); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := store.SaveState(&state); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
