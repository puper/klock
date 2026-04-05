package logger

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// LoggerConfig holds configuration for the zap logger
type LoggerConfig struct {
	// Output configuration
	OutputPath    string // Log file path (e.g., "logs/server.log")
	EnableConsole bool   // Also log to console

	// Level configuration
	Level string // DEBUG, INFO, WARN, ERROR, FATAL

	// Format configuration
	Format string // json, console

	// Rotation configuration
	MaxSize    int  // Max size in MB before rotation
	MaxBackups int  // Max number of old log files
	MaxAge     int  // Max days to retain old log files
	Compress   bool // Compress rotated files
}

// LoadConfigFromEnv loads logger configuration from environment variables
func LoadConfigFromEnv() LoggerConfig {
	return LoggerConfig{
		OutputPath:    getEnvOrDefault("LOCK_SERVER_LOG_OUTPUT", "logs/server.log"),
		EnableConsole: getEnvAsBool("LOCK_SERVER_LOG_CONSOLE", true),
		Level:         getEnvOrDefault("LOCK_SERVER_LOG_LEVEL", "info"),
		Format:        getEnvOrDefault("LOCK_SERVER_LOG_FORMAT", "json"),
		MaxSize:       getEnvAsInt("LOCK_SERVER_LOG_MAX_SIZE", 100),
		MaxBackups:    getEnvAsInt("LOCK_SERVER_LOG_MAX_BACKUPS", 10),
		MaxAge:        getEnvAsInt("LOCK_SERVER_LOG_MAX_AGE", 30),
		Compress:      getEnvAsBool("LOCK_SERVER_LOG_COMPRESS", true),
	}
}

// Validate validates the logger configuration
func (c *LoggerConfig) Validate() error {
	if c.OutputPath == "" {
		return fmt.Errorf("output path cannot be empty")
	}

	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
		"fatal": true,
	}
	if !validLevels[strings.ToLower(c.Level)] {
		return fmt.Errorf("invalid log level: %s (valid: debug, info, warn, error, fatal)", c.Level)
	}

	validFormats := map[string]bool{
		"json":    true,
		"console": true,
	}
	if !validFormats[strings.ToLower(c.Format)] {
		return fmt.Errorf("invalid log format: %s (valid: json, console)", c.Format)
	}

	if c.MaxSize <= 0 {
		return fmt.Errorf("max size must be positive, got: %d", c.MaxSize)
	}

	if c.MaxBackups < 0 {
		return fmt.Errorf("max backups cannot be negative, got: %d", c.MaxBackups)
	}

	if c.MaxAge < 0 {
		return fmt.Errorf("max age cannot be negative, got: %d", c.MaxAge)
	}

	return nil
}

// Helper functions for environment variable parsing

func getEnvOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback
}

func getEnvAsBool(key string, fallback bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return fallback
}
