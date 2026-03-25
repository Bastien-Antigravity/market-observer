package grpc_control

import (
	"context"
	"fmt"

	"market-observer/src/data_source/yahoo"
	"market-observer/src/interfaces"
	"market-observer/src/models"
)

// -----------------------------------------------------------------------------
// ListSources retrieves information about all active data sources
func (s *ControlService) ListSources(ctx context.Context, req *Empty) (*ListSourcesResponse, error) {
	sourcesInfo := s.DataSource.GetAllSources()
	var response []*SourceStatus

	for _, src := range sourcesInfo {
		info := &SourceStatus{
			Name:        src.Name(),
			IsRunning:   src.IsRunning(),
			SymbolCount: 0,
			IsRealTime:  src.IsRealTime(),
			Type:        src.Type(),
		}
		response = append(response, info)
	}

	return &ListSourcesResponse{Sources: response}, nil
}

// -----------------------------------------------------------------------------

// AddSource instantiates and adds a new source based on the requested type
func (s *ControlService) AddSource(ctx context.Context, req *AddSourceRequest) (*SourceControlResponse, error) {
	if req.Name == "" || req.Type == "" {
		return &SourceControlResponse{Success: false, Message: "name and type are required", CurrentState: "stopped"}, nil
	}

	if _, err := s.DataSource.GetSource(req.Name); err == nil {
		return &SourceControlResponse{Success: false, Message: fmt.Sprintf("source %s already exists", req.Name), CurrentState: "stopped"}, nil
	}

	sourceCfg := models.MSourceConfig{
		Name:    req.Name,
		Symbols: req.Symbols,
	}

	var newSource interfaces.IDataSource

	switch req.Type {
	case "yahoo":
		newSource = yahoo.NewYahooFinanceSource(s.Config.MConfig, sourceCfg, s.NetworkManager, s.Logger)
	default:
		return &SourceControlResponse{Success: false, Message: fmt.Sprintf("unsupported source type: %s", req.Type), CurrentState: "stopped"}, nil
	}

	if err := s.DataSource.AddSource(newSource); err != nil {
		s.Logger.Error(fmt.Sprintf("gRPC AddSource failed: %v", err))
		return &SourceControlResponse{Success: false, Message: err.Error(), CurrentState: "stopped"}, nil
	}

	s.Config.DataSource.Sources = append(s.Config.DataSource.Sources, sourceCfg)
	s.Config.Save(s.ConfigPath)

	return &SourceControlResponse{
		Success:      true,
		Message:      fmt.Sprintf("Added source %s", req.Name),
		CurrentState: "running",
	}, nil
}

// -----------------------------------------------------------------------------

// RemoveSource stops and cleans up a single tracked source
func (s *ControlService) RemoveSource(ctx context.Context, req *RemoveSourceRequest) (*SourceControlResponse, error) {
	if req.Name == "" {
		return &SourceControlResponse{Success: false, Message: "name is required", CurrentState: "unknown"}, nil
	}

	if err := s.DataSource.RemoveSource(req.Name); err != nil {
		return &SourceControlResponse{
			Success:      false,
			Message:      err.Error(),
			CurrentState: "unknown",
		}, nil
	}

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

// UpdateSymbols changes the subscribed symbols for a specific source
func (s *ControlService) UpdateSymbols(ctx context.Context, req *UpdateSymbolsRequest) (*UpdateSymbolsResponse, error) {
	if req.SourceName == "" {
		return &UpdateSymbolsResponse{Success: false, Message: "source_name is required", SymbolCount: 0}, nil
	}
	if len(req.Symbols) == 0 {
		return &UpdateSymbolsResponse{Success: false, Message: "symbols list cannot be empty", SymbolCount: 0}, nil
	}

	source, err := s.DataSource.GetSource(req.SourceName)
	if err != nil {
		return &UpdateSymbolsResponse{Success: false, Message: fmt.Sprintf("source %s not found", req.SourceName), SymbolCount: 0}, nil
	}

	if err := source.UpdateSymbols(req.Symbols); err != nil {
		s.Logger.Error(fmt.Sprintf("Failed to update running source: %v", err))
		return &UpdateSymbolsResponse{Success: false, Message: err.Error(), SymbolCount: 0}, nil
	}

	found := false
	for i, src := range s.Config.DataSource.Sources {
		if src.Name == req.SourceName {
			s.Config.DataSource.Sources[i].Symbols = req.Symbols
			found = true
			break
		}
	}

	if found {
		s.Config.Save(s.ConfigPath)
	}

	s.Logger.Info(fmt.Sprintf("UpdateSymbols success for %s. Count: %d", req.SourceName, len(req.Symbols)))

	return &UpdateSymbolsResponse{
		Success:     true,
		Message:     fmt.Sprintf("Successfully updated %s with %d symbols", req.SourceName, len(req.Symbols)),
		SymbolCount: int32(len(req.Symbols)),
	}, nil
}

// -----------------------------------------------------------------------------

// StartSource restarts a source that has been stopped
func (s *ControlService) StartSource(ctx context.Context, req *SourceControlRequest) (*SourceControlResponse, error) {
	if req.SourceName == "" {
		return &SourceControlResponse{Success: false, Message: "source_name is required", CurrentState: "stopped"}, nil
	}
	if err := s.DataSource.StartSource(req.SourceName); err != nil {
		return &SourceControlResponse{Success: false, Message: err.Error(), CurrentState: "stopped"}, nil
	}
	return &SourceControlResponse{Success: true, Message: "Key Start called", CurrentState: "running"}, nil
}

// -----------------------------------------------------------------------------

// StopSource suspends data fetching for a source
func (s *ControlService) StopSource(ctx context.Context, req *SourceControlRequest) (*SourceControlResponse, error) {
	if req.SourceName == "" {
		return &SourceControlResponse{Success: false, Message: "source_name is required", CurrentState: "unknown"}, nil
	}
	if err := s.DataSource.StopSource(req.SourceName); err != nil {
		return &SourceControlResponse{Success: false, Message: err.Error(), CurrentState: "unknown"}, nil
	}
	return &SourceControlResponse{Success: true, Message: "Key Stop called", CurrentState: "stopped"}, nil
}

// -----------------------------------------------------------------------------

// GetStatus returns the status of all sources
func (s *ControlService) GetStatus(ctx context.Context, req *Empty) (*StatusResponse, error) {
	sourcesInfo := s.DataSource.GetAllSources()
	var response []*SourceStatus

	for _, src := range sourcesInfo {
		info := &SourceStatus{
			Name:        src.Name(),
			IsRunning:   src.IsRunning(),
			SymbolCount: 0,
			IsRealTime:  src.IsRealTime(),
			Type:        src.Type(),
		}
		response = append(response, info)
	}
	return &StatusResponse{Sources: response}, nil
}
