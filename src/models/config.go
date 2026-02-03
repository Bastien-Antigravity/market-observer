package models

// MConfig Structure
type MConfig struct {
	Name       string            `yaml:"name"`
	Host       string            `yaml:"host"`
	Port       int               `yaml:"port"`
	LogLevel   string            `yaml:"log_level"`
	GrpcHost   string            `yaml:"grpc_host"`
	GrpcPort   int               `yaml:"grpc_port"`
	Storage    MStorageConfig    `yaml:"storage"`
	Network    MNetworkConfig    `yaml:"network"`
	DataSource MDataSourceConfig `yaml:"data_source"`
	WindowsAgg []string          `yaml:"windows_aggregation"`
}

type MStorageConfig struct {
	DBType             string `yaml:"db_type"`
	DBPath             string `yaml:"db_path"`
	DBConnectionString string `yaml:"db_connection_string"`
}

type MNetworkConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Proxies            []string `yaml:"proxies"`
	RequestTimeout     int      `yaml:"timeout"`
	MaxRetries         int      `yaml:"retries"`
	ConcurrentRequests int      `yaml:"concurrent_requests"`
	UserAgent          string   `yaml:"user_agent"`
}

type MDataSourceConfig struct {
	DataRetentionDays     int             `yaml:"data_retention_days"`
	UpdateIntervalSeconds int             `yaml:"update_interval_seconds"`
	Sources               []MSourceConfig `yaml:"sources"`
}

type MSourceConfig struct {
	Name    string   `yaml:"name"`
	Type    string   `yaml:"type"`    // Source Type (yahoo, etc.)
	Enabled bool     `yaml:"enabled"` // Enable/Disable this source
	Symbols []string `yaml:"symbols"`
	APIKey  string   `yaml:"api_key"` // Optional
}
