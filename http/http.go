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
		var req registry.ApplyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := store.AddToGroup(req.AddData.Group, req.AddData.Instance); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("DELETE /state", func(w http.ResponseWriter, r *http.Request) {
		var req registry.ApplyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := store.RemoveFromGroup(req.RemoveData.Group, req.RemoveData.InstanceID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
