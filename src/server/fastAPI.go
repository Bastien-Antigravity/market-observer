package server

import (
	"fmt"
	"strings"
	"sync"

	"market-observer/src/logger"
	"market-observer/src/models"

	"github.com/gin-gonic/gin"
)

// -----------------------------------------------------------------------------
// FastAPIServer
// -----------------------------------------------------------------------------

type FastAPIServer struct {
	Config *models.MConfig
	Logger *logger.Logger
	engine *gin.Engine

	// WebSocket clients
	clients    map[*Client]struct{}
	broadcast  chan *models.MLatestData // Strongly typed and Buffered Queue
	register   chan *Client
	unregister chan *Client

	// Local cache
	latestState *models.MLatestData
	stateMutex  sync.RWMutex
}

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------

func NewFastAPIServer(cfg *models.MConfig, logger *logger.Logger) *FastAPIServer {
	// Set Gin mode
	if cfg.LogLevel != "DEBUG" {
		gin.SetMode(gin.ReleaseMode)
	}

	s := &FastAPIServer{
		Config:  cfg,
		Logger:  logger,
		engine:  gin.Default(),
		clients: make(map[*Client]struct{}),
		// Buffered channel to prevent lock/blocking
		// Queue size of 256 ensures we can handle bursts of updates
		broadcast:  make(chan *models.MLatestData, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		latestState: &models.MLatestData{
			Type:              "INITIAL",
			RawData:           make(map[string]models.MStockPrice),
			Aggregations:      make(map[string]map[string][]models.MAggregation),
			Timestamp:         0,
			ProcessingMetrics: models.MProcessingMetrics{},
		},
	}

	// Add CORS Middleware
	s.engine.Use(func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if strings.HasPrefix(origin, "http://127.0.0.1:") {
			c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
		}
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// setup web routes
	s.setupRoutes()
	return s
}

// -----------------------------------------------------------------------------
// Route Setup (Matches Python endpoints exactly)
// -----------------------------------------------------------------------------

func (s *FastAPIServer) setupRoutes() {
	// REST API endpoints
	s.engine.GET("/api/metrics", s.getMetrics)
	s.engine.GET("/api/config", s.getConfig)
	s.engine.GET("/api/health", s.getHealth)

	// WebSocket endpoint
	s.engine.GET("/ws", s.handleWebSocket)
}

// -----------------------------------------------------------------------------
// Server Lifecycle
// -----------------------------------------------------------------------------

func (s *FastAPIServer) Start() error {
	addr := fmt.Sprintf("%s:%d", s.Config.Host, s.Config.Port)
	s.Logger.Info("Starting server on %s", addr)

	go s.handleWebsockets()

	return s.engine.Run(addr)
}

// -----------------------------------------------------------------------------

func (s *FastAPIServer) Stop() error {
	// Clean shutdown
	close(s.broadcast)
	close(s.register)
	close(s.unregister)
	return nil
}

// -----------------------------------------------------------------------------
// Route Handlers (Matches Python behavior exactly)
// -----------------------------------------------------------------------------

func (s *FastAPIServer) getMetrics(c *gin.Context) {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()

	// Return processing_metrics
	c.JSON(200, s.latestState.ProcessingMetrics)
}

// -----------------------------------------------------------------------------

func (s *FastAPIServer) getConfig(c *gin.Context) {
	// Return timeframes from config
	c.JSON(200, gin.H{
		"timeframes": s.Config.WindowsAgg,
	})
}

// -----------------------------------------------------------------------------

func (s *FastAPIServer) getHealth(c *gin.Context) {
	s.stateMutex.RLock()
	connections := len(s.clients)
	timestamp := s.latestState.Timestamp
	s.stateMutex.RUnlock()

	// Exact same response as Python
	c.JSON(200, gin.H{
		"status":        "ok",
		"connections":   connections,
		"latest_update": timestamp,
	})
}

// Methods moved to hub.go to follow Single Responsibility Principle
