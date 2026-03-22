package grpc_control

import (
	"market-observer/src/config"
	datasource "market-observer/src/data_source"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
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

// -----------------------------------------------------------------------------

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
