package config

import (
	"fmt"
	"os"

	"market-observer/src/models"

	"gopkg.in/yaml.v3"
)

// -----------------------------------------------------------------------------

// Config wraps models.MConfig and provides business logic methods
type Config struct {
	*models.MConfig
}

// -----------------------------------------------------------------------------

// NewConfig creates a new MConfig instance from YAML file
func NewConfig(configPath string) (*Config, error) {
	// 1. Read the YAML file content
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	// 2. Unmarshal data into the models struct
	var modelConfig models.MConfig
	if err := yaml.Unmarshal(data, &modelConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config from YAML: %w", err)
	}

	config := &Config{MConfig: &modelConfig}

	// 3. Validate the loaded configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

// -----------------------------------------------------------------------------

// Validate performs basic configuration validation
func (c *Config) Validate() error {
	// Validate App configuration (Flattened)
	if c.Name == "" {
		return fmt.Errorf("application name cannot be empty")
	}
	// LogLevel check if needed
	if c.LogLevel == "" {
		// default or error
	}

	// Validate Server configuration (Flattened)
	if c.Host == "" {
		return fmt.Errorf("server host cannot be empty")
	}
	if c.Port <= 1024 || c.Port > 65535 {
		return fmt.Errorf("invalid server port number: %d (must be between 1025 and 65535)", c.Port)
	}

	// Validate Storage configuration
	if c.Storage.DBType == "" {
		return fmt.Errorf("database type cannot be empty")
	}
	if c.Storage.DBType == "sqlite" && c.Storage.DBPath == "" {
		return fmt.Errorf("database path cannot be empty for sqlite")
	}

	// Validate Network configuration
	if c.Network.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be greater than 0")
	}
	if c.Network.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if c.Network.ConcurrentRequests <= 0 {
		return fmt.Errorf("concurrent requests must be greater than 0")
	}
	// UserAgent might be optional or checked

	// Validate DataSource configuration
	if c.DataSource.UpdateIntervalSeconds <= 0 {
		return fmt.Errorf("update interval must be greater than 0")
	}
	if c.DataSource.DataRetentionDays <= 0 {
		return fmt.Errorf("data retention days must be greater than 0")
	}
	if len(c.DataSource.Sources) == 0 {
		return fmt.Errorf("at least one data source must be configured")
	}
	for i, src := range c.DataSource.Sources {
		if src.Name == "" {
			return fmt.Errorf("source %d must have a name", i)
		}
		if len(src.Symbols) == 0 {
			return fmt.Errorf("source '%s' must have at least one symbol", src.Name)
		}
	}

	// Validate Windows aggregation
	for i, window := range c.WindowsAgg {
		if window == "" {
			return fmt.Errorf("window aggregation %d cannot be empty", i)
		}
	}

	return nil
}

// -----------------------------------------------------------------------------

// Save persists the current configuration to the specified YAML file path
func (c *Config) Save(configPath string) error {
	// 1. Marshal the struct to YAML
	data, err := yaml.Marshal(c.MConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal config to YAML: %w", err)
	}

	// 2. Write to file (0644 permissions)
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config to file '%s': %w", configPath, err)
	}

	return nil
}
