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
	Nats       MNATSConfig       `yaml:"nats"`
}

type MNATSConfig struct {
	Servers        []string         `yaml:"servers"`
	ClientID       string           `yaml:"client_id"`
	ClusterID      string           `yaml:"cluster_id"`
	Subject        string           `yaml:"subject"`
	SubjectPrefix  string           `yaml:"subject_prefix"`
	ConnectTimeout string           `yaml:"connect_timeout"`
	ReconnectWait  string           `yaml:"reconnect_wait"`
	MaxReconnects  int              `yaml:"max_reconnects"`
	FlushTimeout   string           `yaml:"flush_timeout"`
	JetStream      MJetStreamConfig `yaml:"jetstream"`
}

type MJetStreamConfig struct {
	Enabled         bool     `yaml:"enabled"`
	StreamName      string   `yaml:"stream_name"`
	Subjects        []string `yaml:"subjects"`
	Retention       string   `yaml:"retention"`
	Storage         string   `yaml:"storage"`
	Replicas        int      `yaml:"replicas"`
	MaxAge          string   `yaml:"max_age"`
	MaxMsgs         int      `yaml:"max_msgs"`
	MaxBytes        int64    `yaml:"max_bytes"`
	MaxMsgSize      int32    `yaml:"max_msg_size"`
	DurableConsumer string   `yaml:"durable_consumer"`
	AckWait         string   `yaml:"ack_wait"`
}

type MStorageConfig struct {
	DBType             string `yaml:"db_type"`
	DBPath             string `yaml:"db_path"`
	DBConnectionString string `yaml:"db_connection_string"`
	EnableSSL          bool   `yaml:"enable_ssl"`
	PostgresMode       string `yaml:"postgres_mode"` // "tick", "aggregated", "both"
	Reset              bool   `yaml:"reset"`         // If true, drops schema/file on boot
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
