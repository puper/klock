package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestLoadConfigFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected LoggerConfig
	}{
		{
			name:    "default values",
			envVars: map[string]string{},
			expected: LoggerConfig{
				OutputPath:    "logs/server.log",
				EnableConsole: true,
				Level:         "info",
				Format:        "json",
				MaxSize:       100,
				MaxBackups:    10,
				MaxAge:        30,
				Compress:      true,
			},
		},
		{
			name: "custom values",
			envVars: map[string]string{
				"LOCK_SERVER_LOG_OUTPUT":      "/var/log/test.log",
				"LOCK_SERVER_LOG_CONSOLE":     "false",
				"LOCK_SERVER_LOG_LEVEL":       "debug",
				"LOCK_SERVER_LOG_FORMAT":      "console",
				"LOCK_SERVER_LOG_MAX_SIZE":    "50",
				"LOCK_SERVER_LOG_MAX_BACKUPS": "5",
				"LOCK_SERVER_LOG_MAX_AGE":     "7",
				"LOCK_SERVER_LOG_COMPRESS":    "false",
			},
			expected: LoggerConfig{
				OutputPath:    "/var/log/test.log",
				EnableConsole: false,
				Level:         "debug",
				Format:        "console",
				MaxSize:       50,
				MaxBackups:    5,
				MaxAge:        7,
				Compress:      false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment
			os.Clearenv()

			// Set test environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			// Load config
			config := LoadConfigFromEnv()

			// Assert
			if config != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, config)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  LoggerConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: LoggerConfig{
				OutputPath: "logs/test.log",
				Level:      "info",
				Format:     "json",
				MaxSize:    100,
				MaxBackups: 10,
				MaxAge:     30,
			},
			wantErr: false,
		},
		{
			name: "empty output path",
			config: LoggerConfig{
				OutputPath: "",
				Level:      "info",
				Format:     "json",
				MaxSize:    100,
			},
			wantErr: true,
		},
		{
			name: "invalid log level",
			config: LoggerConfig{
				OutputPath: "logs/test.log",
				Level:      "invalid",
				Format:     "json",
				MaxSize:    100,
			},
			wantErr: true,
		},
		{
			name: "invalid format",
			config: LoggerConfig{
				OutputPath: "logs/test.log",
				Level:      "info",
				Format:     "invalid",
				MaxSize:    100,
			},
			wantErr: true,
		},
		{
			name: "negative max size",
			config: LoggerConfig{
				OutputPath: "logs/test.log",
				Level:      "info",
				Format:     "json",
				MaxSize:    -1,
			},
			wantErr: true,
		},
		{
			name: "negative max backups",
			config: LoggerConfig{
				OutputPath: "logs/test.log",
				Level:      "info",
				Format:     "json",
				MaxSize:    100,
				MaxBackups: -1,
			},
			wantErr: true,
		},
		{
			name: "negative max age",
			config: LoggerConfig{
				OutputPath: "logs/test.log",
				Level:      "info",
				Format:     "json",
				MaxSize:    100,
				MaxAge:     -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewLogger(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		config  LoggerConfig
		wantErr bool
	}{
		{
			name: "json format",
			config: LoggerConfig{
				OutputPath:    filepath.Join(tmpDir, "json.log"),
				EnableConsole: false,
				Level:         "info",
				Format:        "json",
				MaxSize:       100,
				MaxBackups:    10,
				MaxAge:        30,
				Compress:      false,
			},
			wantErr: false,
		},
		{
			name: "console format",
			config: LoggerConfig{
				OutputPath:    filepath.Join(tmpDir, "console.log"),
				EnableConsole: false,
				Level:         "info",
				Format:        "console",
				MaxSize:       100,
				MaxBackups:    10,
				MaxAge:        30,
				Compress:      false,
			},
			wantErr: false,
		},
		{
			name: "with console output",
			config: LoggerConfig{
				OutputPath:    filepath.Join(tmpDir, "both.log"),
				EnableConsole: true,
				Level:         "info",
				Format:        "json",
				MaxSize:       100,
				MaxBackups:    10,
				MaxAge:        30,
				Compress:      false,
			},
			wantErr: false,
		},
		{
			name: "invalid config",
			config: LoggerConfig{
				OutputPath: "",
				Level:      "info",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := NewLogger(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				if logger != nil {
					t.Errorf("expected nil logger, got %v", logger)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if logger == nil {
					t.Errorf("expected logger, got nil")
				} else {
					// Test logging
					logger.Info("test message", zap.String("key", "value"))

					// Sync logger (ignore "bad file descriptor" for stdout)
					err = logger.Sync()
					if err != nil && !strings.Contains(err.Error(), "bad file descriptor") && !strings.Contains(err.Error(), "invalid argument") {
						t.Errorf("sync error: %v", err)
					}
				}
			}
		})
	}
}
