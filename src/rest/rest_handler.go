package rest

import (
	"context"
	"encoding/json"
	"net/http"

	pb "market-observer/src/grpc_control"
	"market-observer/src/logger"
)

type RestHandler struct {
	Client pb.MarketObserverControlClient
	Logger *logger.Logger
}

// -----------------------------------------------------------------------------

func NewRestHandler(client pb.MarketObserverControlClient, logger *logger.Logger) *RestHandler {
	return &RestHandler{
		Client: client,
		Logger: logger,
	}
}

// -----------------------------------------------------------------------------

func (h *RestHandler) RegisterEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/api/source/list", h.handleListSources)
	mux.HandleFunc("/api/source/add", h.handleAddSource)
	mux.HandleFunc("/api/source/remove", h.handleRemoveSource)
	mux.HandleFunc("/api/source/symbols", h.handleUpdateSymbols)
	mux.HandleFunc("/api/source/start", h.handleStartSource)
	mux.HandleFunc("/api/source/stop", h.handleStopSource)
}

// -----------------------------------------------------------------------------
// Common Helpers

func (h *RestHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *RestHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]interface{}{
		"success": false,
		"message": message,
	})
}

// -----------------------------------------------------------------------------
// Request Objects

type AddSourceRequest struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Symbols []string `json:"symbols"`
}

type SourceControlRequest struct {
	SourceName string `json:"source_name"`
}

type UpdateSymbolsRequest struct {
	SourceName string   `json:"source_name"`
	Symbols    []string `json:"symbols"`
}

// -----------------------------------------------------------------------------
// Handlers

func (h *RestHandler) handleListSources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	resp, err := h.Client.ListSources(context.Background(), &pb.Empty{})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var response []map[string]interface{}
	for _, info := range resp.Sources {
		response = append(response, map[string]interface{}{
			"name":         info.Name,
			"is_running":   info.IsRunning,
			"symbol_count": info.SymbolCount,
			"is_real_time": info.IsRealTime,
			"type":         info.Type,
		})
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"sources": response,
	})
}

// -----------------------------------------------------------------------------

func (h *RestHandler) handleAddSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req AddSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	resp, err := h.Client.AddSource(context.Background(), &pb.AddSourceRequest{
		Name:    req.Name,
		Type:    req.Type,
		Symbols: req.Symbols,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if !resp.Success {
		h.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success":       false,
			"message":       resp.Message,
			"current_state": resp.CurrentState,
		})
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"message":       resp.Message,
		"current_state": resp.CurrentState,
	})
}

// -----------------------------------------------------------------------------

func (h *RestHandler) handleRemoveSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req SourceControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	resp, err := h.Client.RemoveSource(context.Background(), &pb.RemoveSourceRequest{
		Name: req.SourceName,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":       resp.Success,
		"message":       resp.Message,
		"current_state": resp.CurrentState,
	})
}

// -----------------------------------------------------------------------------

func (h *RestHandler) handleUpdateSymbols(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req UpdateSymbolsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	resp, err := h.Client.UpdateSymbols(context.Background(), &pb.UpdateSymbolsRequest{
		SourceName: req.SourceName,
		Symbols:    req.Symbols,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":      resp.Success,
		"message":      resp.Message,
		"symbol_count": resp.SymbolCount,
	})
}

// -----------------------------------------------------------------------------

func (h *RestHandler) handleStartSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req SourceControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	resp, err := h.Client.StartSource(context.Background(), &pb.SourceControlRequest{
		SourceName: req.SourceName,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":       resp.Success,
		"message":       resp.Message,
		"current_state": resp.CurrentState,
	})
}

// -----------------------------------------------------------------------------

func (h *RestHandler) handleStopSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req SourceControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	resp, err := h.Client.StopSource(context.Background(), &pb.SourceControlRequest{
		SourceName: req.SourceName,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":       resp.Success,
		"message":       resp.Message,
		"current_state": resp.CurrentState,
	})
}
