package server

import (
	"encoding/json"
	"net/http"

	"market-observer/src/models"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// -----------------------------------------------------------------------------
// Hub Pattern Implementation
// -----------------------------------------------------------------------------

// handleWebsockets is the main Hub loop
func (s *FastAPIServer) handleWebsockets() {
	for {
		select {
		case client := <-s.register:
			s.clients[client] = struct{}{}
			// Send initial state on connect
			s.stateMutex.RLock()
			if s.latestState != nil {
				// Send full initial state
				client.send <- s.latestState
			}
			s.stateMutex.RUnlock()

		case client := <-s.unregister:
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				close(client.send)
			}

		case message := <-s.broadcast:
			// Update state and broadcast
			s.stateMutex.Lock()
			s.latestState = message
			s.stateMutex.Unlock()

			// Broadcast to all clients
			for client := range s.clients {
				select {
				case client.send <- message:
					// Message sent successfully
				default:
					// Client too slow, disconnect to prevent Hub blocking
					// This ensures reliable 24/7 operation by pruning dead/slow consumers
					delete(s.clients, client)
					close(client.send)
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Data Exchange Interface Implementation
// -----------------------------------------------------------------------------

// UpdateAllDatas - updates internal state by merging new data (Deep Merge)
func (s *FastAPIServer) UpdateAllDatas(data interface{}) {
	// Parse input
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		s.Logger.Info("AllDatas expected map[string]interface{}, got %T", data)
		return
	}

	newRaw := safeStockPriceMap(dataMap, "raw_data")
	newAggs := safeAggregationsMap(dataMap, "aggregations")
	newTs := safeInt64(dataMap, "timestamp")
	newMetrics := safeProcessingMetrics(dataMap, "processing_metrics")

	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// 1. Merge Raw Data
	if s.latestState.RawData == nil {
		s.latestState.RawData = make(map[string]models.MStockPrice)
	}
	for k, v := range newRaw {
		s.latestState.RawData[k] = v
	}

	// 2. Merge Aggregations
	if s.latestState.Aggregations == nil {
		s.latestState.Aggregations = make(map[string]map[string][]models.MAggregation)
	}
	for sym, windows := range newAggs {
		if s.latestState.Aggregations[sym] == nil {
			s.latestState.Aggregations[sym] = make(map[string][]models.MAggregation)
		}
		for wName, wData := range windows {
			// 2025-12-21: Reverted to "Latest Only" snapshot per user request.
			// The server holds only the most recent aggregation for each window.
			s.latestState.Aggregations[sym][wName] = wData
		}
	}

	// 3. Update Metadata
	s.latestState.Timestamp = newTs
	s.latestState.ProcessingMetrics = newMetrics
	s.latestState.Type = "UPDATE"
}

// -----------------------------------------------------------------------------`

// Broadcast - parses data and sends to broadcast channel (Queue)
func (s *FastAPIServer) Broadcast(message interface{}) {
	// Parse input
	dataMap, ok := message.(map[string]interface{})
	if !ok {
		// Log error but don't crash
		s.Logger.Info("Broadcast expected map[string]interface{}, got %T", message)
		return
	}

	// Convert to strongly typed structure BEFORE entering the channel
	// This optimization prevents the Hub from doing data processing
	state := &models.MLatestData{
		Type:              "UPDATE",
		RawData:           safeStockPriceMap(dataMap, "raw_data"),
		Aggregations:      safeAggregationsMap(dataMap, "aggregations"),
		Timestamp:         safeInt64(dataMap, "timestamp"),
		ProcessingMetrics: safeProcessingMetrics(dataMap, "processing_metrics"),
	}

	// Non-blocking send if buffer is full (optional, but safer for "prevent lock")
	// However, if we want reliability, we might want to block briefly or drop.
	// With a large buffer, blocking is rare.
	// We'll trust the large buffer (set in NewFastAPIServer) handling.
	s.broadcast <- state
}

// -----------------------------------------------------------------------------
// Helper Methods
// -----------------------------------------------------------------------------

// SetLatestState - Thread-safe state update
func (s *FastAPIServer) SetLatestState(state *models.MLatestData) {
	s.stateMutex.Lock()
	state.Type = "UPDATE"
	s.latestState = state
	s.stateMutex.Unlock()
}

// -----------------------------------------------------------------------------
// WebSocket Handlers
// -----------------------------------------------------------------------------

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// -----------------------------------------------------------------------------

func (s *FastAPIServer) handleWebSocket(c *gin.Context) {
	// s.Logger.Info("WebSocket Handshake initiating for %s", c.ClientIP())

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		s.Logger.Info("Failed to upgrade websocket: %v", err)
		return
	}

	client := &Client{
		hub:  s,
		conn: conn,
		// Buffered channel to prevent blocking the Hub loop
		send: make(chan interface{}, 256),
	}

	s.register <- client

	// Start goroutines for reading/writing
	go client.writePump()
	go client.readPump()
}

// -----------------------------------------------------------------------------
// Client Message Handling
// -----------------------------------------------------------------------------

func (s *FastAPIServer) HandleClientMessage(client *Client, message []byte) {
	var cmd models.MSubscribeCommand
	if err := json.Unmarshal(message, &cmd); err != nil {
		s.Logger.Info("Failed to parse client command: %v, disconnecting client", err)
		client.conn.Close()
		return
	}

	if cmd.Command != "subscribe" {
		return
	}

	s.stateMutex.RLock()
	var response *models.MLatestData

	if cmd.ClientType == "dashboard" {
		response = s.dashboardResponse(cmd.Symbols, cmd.Timeframe)
	} else {
		response = s.symbolViewResponse(cmd.Symbols, cmd.Timeframe)
	}
	s.stateMutex.RUnlock()

	// Send response to client
	// Use select to avoid blocking if client's send buffer is full
	select {
	case client.send <- response:
	default:
		// Client buffer full, could drop or log.
		// Detailed handling is in the Hub loop default case for broadcast,
		// but here for direct response we just try.
	}
}

// -----------------------------------------------------------------------------
// Response Filtering
// -----------------------------------------------------------------------------

func (s *FastAPIServer) symbolViewResponse(symbols []string, timeframe string) *models.MLatestData {
	// Filter Raw Data (symbols only)
	filteredRaw := make(map[string]models.MStockPrice)
	if len(symbols) == 0 {
		filteredRaw = s.latestState.RawData
	} else {
		for sym, data := range s.latestState.RawData {
			if contains(symbols, sym) {
				filteredRaw[sym] = data
			}
		}
	}

	// Filter Aggregations (symbols AND timeframe)
	filteredAgg := make(map[string]map[string][]models.MAggregation)

	if len(symbols) == 0 {
		for sym, windowsMap := range s.latestState.Aggregations {
			if timeframe != "" {
				if wData, exists := windowsMap[timeframe]; exists {
					filteredAgg[sym] = map[string][]models.MAggregation{timeframe: wData}
				}
			} else {
				filteredAgg[sym] = windowsMap
			}
		}
	} else {
		for _, sym := range symbols {
			if windowsMap, exists := s.latestState.Aggregations[sym]; exists {
				if timeframe != "" {
					if wData, exists := windowsMap[timeframe]; exists {
						filteredAgg[sym] = map[string][]models.MAggregation{timeframe: wData}
					}
				} else {
					filteredAgg[sym] = windowsMap
				}
			}
		}
	}

	return &models.MLatestData{
		Type:              "INITIAL",
		RawData:           filteredRaw,
		Aggregations:      filteredAgg,
		Timestamp:         s.latestState.Timestamp,
		ProcessingMetrics: s.latestState.ProcessingMetrics,
	}
}

// -----------------------------------------------------------------------------

func (s *FastAPIServer) dashboardResponse(symbols []string, timeframe string) *models.MLatestData {
	filteredAgg := make(map[string]map[string][]models.MAggregation)

	if timeframe == "" {
		return &models.MLatestData{
			Type:              "INITIAL",
			RawData:           make(map[string]models.MStockPrice),
			Aggregations:      filteredAgg,
			Timestamp:         s.latestState.Timestamp,
			ProcessingMetrics: s.latestState.ProcessingMetrics,
		}
	}

	// Helper removed: We now want to return FULL history for charts

	if len(symbols) == 0 {
		for sym, windowsMap := range s.latestState.Aggregations {
			if wData, exists := windowsMap[timeframe]; exists {
				filteredAgg[sym] = map[string][]models.MAggregation{timeframe: wData}
			}
		}
	} else {
		for _, sym := range symbols {
			if windowsMap, exists := s.latestState.Aggregations[sym]; exists {
				if wData, exists := windowsMap[timeframe]; exists {
					filteredAgg[sym] = map[string][]models.MAggregation{timeframe: wData}
				}
			}
		}
	}

	return &models.MLatestData{
		Type:              "INITIAL",
		RawData:           s.latestState.RawData,
		Aggregations:      filteredAgg,
		Timestamp:         s.latestState.Timestamp,
		ProcessingMetrics: s.latestState.ProcessingMetrics,
	}
}
