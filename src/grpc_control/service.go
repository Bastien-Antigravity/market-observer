package grpc_control

import (
	"context"
	"fmt"
	"market-observer/src/config"
	datasource "market-observer/src/data_source"
	"market-observer/src/data_source/yahoo"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ControlService implements the MarketObserverControlServer interface
type ControlService struct {
	UnimplementedMarketObserverControlServer
	Config         *config.Config
	DataSource     *datasource.MultiSourceManager
	ConfigPath     string
	Logger         *logger.Logger
	NetworkManager interfaces.INetworkManager
}

// NewControlService creates a new instance of ControlService
func NewControlService(
	cfg *config.Config,
	ds *datasource.MultiSourceManager,
	cfgPath string,
	log *logger.Logger,
	netMgr interfaces.INetworkManager,
) *ControlService {
	return &ControlService{
		Config:         cfg,
		DataSource:     ds,
		ConfigPath:     cfgPath,
		Logger:         log,
		NetworkManager: netMgr,
	}
}

// -----------------------------------------------------------------------------

func (s *ControlService) ListSources(ctx context.Context, req *Empty) (*ListSourcesResponse, error) {
	sources := s.DataSource.GetAllSources()
	var response []*SourceStatus

	for _, src := range sources {
		status := &SourceStatus{
			Name:        src.Name(),
			IsRunning:   true, // Simplified, assume if in manager it's active-ish, or check Start state?
			SymbolCount: 0,    // Need to expose this via interface? For now 0.
			IsRealTime:  src.IsRealTime(),
			Type:        "unknown",
		}
		// Type check if possible
		if _, ok := src.(*yahoo.YahooFinanceSource); ok {
			status.Type = "yahoo"
		}
		response = append(response, status)
	}

	return &ListSourcesResponse{Sources: response}, nil
}

// -----------------------------------------------------------------------------

func (s *ControlService) AddSource(ctx context.Context, req *AddSourceRequest) (*SourceControlResponse, error) {
	if req.Name == "" || req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "name and type are required")
	}

	// Check if exists
	if _, err := s.DataSource.GetSource(req.Name); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "source %s already exists", req.Name)
	}

	// Create Config
	sourceCfg := models.MSourceConfig{
		Name:    req.Name,
		Symbols: req.Symbols,
	}

	var newSource interfaces.IDataSource

	switch req.Type {
	case "yahoo":
		newSource = yahoo.NewYahooFinanceSource(s.Config.MConfig, sourceCfg, s.NetworkManager)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported source type: %s", req.Type)
	}

	// Add to Manager (Starts it automatically)
	if err := s.DataSource.AddSource(newSource); err != nil {
		s.Logger.Error("Failed to add source: %v", err)
		return &SourceControlResponse{
			Success:      false,
			Message:      fmt.Sprintf("Failed to add source: %v", err),
			CurrentState: "stopped",
		}, nil
	}

	// Save to Persistent Config is NOT implemented here to avoid complexity with YAML array merging?
	// ACTUALLY, we should update config in memory at least.
	s.Config.DataSource.Sources = append(s.Config.DataSource.Sources, sourceCfg)
	s.Config.Save(s.ConfigPath)

	return &SourceControlResponse{
		Success:      true,
		Message:      fmt.Sprintf("Added source %s", req.Name),
		CurrentState: "running",
	}, nil
}

// -----------------------------------------------------------------------------

func (s *ControlService) RemoveSource(ctx context.Context, req *RemoveSourceRequest) (*SourceControlResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	if err := s.DataSource.RemoveSource(req.Name); err != nil {
		return &SourceControlResponse{
			Success:      false,
			Message:      fmt.Sprintf("Failed to remove source: %v", err),
			CurrentState: "unknown",
		}, nil
	}

	// Clean from Config
	newSources := []models.MSourceConfig{}
	for _, src := range s.Config.DataSource.Sources {
		if src.Name != req.Name {
			newSources = append(newSources, src)
		}
	}
	s.Config.DataSource.Sources = newSources
	s.Config.Save(s.ConfigPath)

	return &SourceControlResponse{
		Success:      true,
		Message:      fmt.Sprintf("Removed source %s", req.Name),
		CurrentState: "removed",
	}, nil
}

// -----------------------------------------------------------------------------

// UpdateSymbols updates the symbol list for a specific source
func (s *ControlService) UpdateSymbols(ctx context.Context, req *UpdateSymbolsRequest) (*UpdateSymbolsResponse, error) {
	sName := req.SourceName
	newSymbols := req.Symbols

	if sName == "" {
		return nil, status.Error(codes.InvalidArgument, "source_name is required")
	}
	if len(newSymbols) == 0 {
		return nil, status.Error(codes.InvalidArgument, "symbols list cannot be empty")
	}

	// Target Specific Source
	source, err := s.DataSource.GetSource(sName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "source %s not found", sName)
	}

	if err := source.UpdateSymbols(newSymbols); err != nil {
		s.Logger.Error("gRPC: Failed to update running source: %v", err)
		return &UpdateSymbolsResponse{
			Success:     false,
			Message:     fmt.Sprintf("Failed to update running source: %v", err),
			SymbolCount: 0,
		}, nil
	}

	// Update Config Persistence
	found := false
	for i, src := range s.Config.DataSource.Sources {
		if src.Name == sName {
			s.Config.DataSource.Sources[i].Symbols = newSymbols
			found = true
			break
		}
	}

	if found {
		s.Config.Save(s.ConfigPath)
	}

	s.Logger.Info("gRPC: UpdateSymbols success for %s. Count: %d", sName, len(newSymbols))
	return &UpdateSymbolsResponse{
		Success:     true,
		Message:     fmt.Sprintf("Successfully updated %s with %d symbols", sName, len(newSymbols)),
		SymbolCount: int32(len(newSymbols)),
	}, nil
}

// -----------------------------------------------------------------------------

func (s *ControlService) StartSource(ctx context.Context, req *SourceControlRequest) (*SourceControlResponse, error) {
	if req.SourceName == "" {
		return nil, status.Error(codes.InvalidArgument, "source_name is required")
	}

	if err := s.DataSource.StartSource(req.SourceName); err != nil {
		return &SourceControlResponse{Success: false, Message: err.Error(), CurrentState: "stopped"}, nil
	}
	return &SourceControlResponse{Success: true, Message: "Key Start called", CurrentState: "running"}, nil
}

// -----------------------------------------------------------------------------

func (s *ControlService) StopSource(ctx context.Context, req *SourceControlRequest) (*SourceControlResponse, error) {
	if req.SourceName == "" {
		return nil, status.Error(codes.InvalidArgument, "source_name is required")
	}

	if err := s.DataSource.StopSource(req.SourceName); err != nil {
		return &SourceControlResponse{Success: false, Message: err.Error(), CurrentState: "unknown"}, nil
	}
	return &SourceControlResponse{Success: true, Message: "Key Stop called", CurrentState: "stopped"}, nil
}

// -----------------------------------------------------------------------------

func (s *ControlService) GetStatus(ctx context.Context, req *Empty) (*StatusResponse, error) {
	// Re-use ListSources logic or better yet, just implement it here
	sources := s.DataSource.GetAllSources()
	var sourceStatuses []*SourceStatus

	for _, src := range sources {
		status := &SourceStatus{
			Name:        src.Name(),
			IsRunning:   true,
			SymbolCount: 0,
			IsRealTime:  src.IsRealTime(),
			Type:        "unknown",
		}
		if _, ok := src.(*yahoo.YahooFinanceSource); ok {
			status.Type = "yahoo"
		}
		sourceStatuses = append(sourceStatuses, status)
	}
	return &StatusResponse{Sources: sourceStatuses}, nil
}
