---
title: "Zap Logger Integration Research"
link: "zap-logger-integration"
type: research
ontological_relations:
  - relates_to: [[server-architecture]]
  - relates_to: [[logging-implementation]]
tags: [research, logging, zap, grpc, middleware]
uuid: "550e8400-e29b-41d4-a716-446655440001"
created_at: "2026-04-05T10:30:00Z"
---

## Current State Analysis

### Existing Logging Implementation

**Location**: `server/main.go`

**Current Approach**:
- Standard library `log` package only
- No structured logging
- No log levels
- No log rotation or compression
- Simple text output to stdout

**Logging Points** (with line numbers):
- `server/main.go:10` - Import statement
- `server/main.go:1209` - Unary interceptor logging
- `server/main.go:1225` - Stream interceptor logging
- `server/main.go:1238` - Metrics reporter logging
- `server/main.go:1285, 1311, 1320, 1328, 1334` - Server lifecycle logs

**Current Log Format**:
```
grpc_unary method=/lock.LockService/Acquire code=OK duration_ms=15
grpc_stream method=/lock.LockService/WatchSession code=STREAM_ERROR duration_ms=234
metrics unary_calls=1234 unary_failures=5 stream_calls=56 stream_failures=2 auth_failures=0 rate_limited=3 rate_limiter_entries=45 active_watchers=12 session_invalidations=0
```

### Server Architecture

**Framework**: gRPC (google.golang.org/grpc v1.67.1)

**Initialization Flow** (`server/main.go:1272-1337`):
1. Environment variable configuration
2. Create hierarchical locker
3. Create lock service
4. Create rate limiter
5. Create gRPC server with interceptors
6. Register service
7. Start listening
8. Graceful shutdown handling

**Middleware Chain**:
```
Auth Interceptor → Rate Limit Interceptor → Metrics Interceptor → Handler
```

**Interceptor Registration** (`server/main.go:1296-1305`):
```go
grpc.ChainUnaryInterceptor(
    grpcServerAuthUnaryInterceptor(authToken, metrics),
    grpcServerRateUnaryInterceptor(limiter, metrics),
    grpcServerUnaryMetricsInterceptor(metrics),
)
grpc.ChainStreamInterceptor(
    grpcServerAuthStreamInterceptor(authToken, metrics),
    grpcServerRateStreamInterceptor(limiter, metrics),
    grpcServerStreamMetricsInterceptor(metrics),
)
```

### Directory Structure

```
klock/
├── server/
│   ├── main.go              # Main server (1358 lines)
│   ├── main_test.go         # Server tests
│   └── grpc_security_test.go # Interceptor tests
├── pkg/
│   ├── hierlock/            # Hierarchical lock implementation
│   ├── lockrpcpb/           # Generated protobuf code
│   └── logger/              # EMPTY - for future logging
├── client/                  # Go Client SDK
├── cmd/                     # Command-line tools
├── logs/                    # EMPTY - for log storage
├── proto/                   # Protocol definitions
└── go.mod                   # Module definition
```

### Configuration Pattern

**Environment Variables** (`server/main.go:1273-1281`):
- `LOCK_SERVER_ADDR` - Server address
- `LOCK_SERVER_SHARDS` - Shard count
- `LOCK_SERVER_DEFAULT_LEASE_MS` - Default lease
- `LOCK_SERVER_MAX_LEASE_MS` - Max lease
- `LOCK_SERVER_AUTH_TOKEN` - Auth token
- `LOCK_SERVER_RATE_LIMIT_RPS` - Rate limit
- `LOCK_SERVER_RATE_LIMIT_BURST` - Burst limit
- `LOCK_SERVER_IDEMPOTENCY_ENTRIES` - Cache size
- `LOCK_SERVER_IDEMPOTENCY_TTL_MS` - Cache TTL

**No config files** - all configuration via environment variables

### Dependencies

**Current** (`go.mod`):
- Go 1.25.0
- golang.org/x/time v0.15.0 (rate limiting)
- google.golang.org/grpc v1.67.1
- google.golang.org/protobuf v1.34.2

**Missing**:
- No logging library (uses standard library)
- No log rotation library
- No compression library

## Integration Points

### 1. Logger Initialization

**Location**: `server/main.go` main() function

**Required Changes**:
- Initialize zap logger before creating gRPC server
- Configure log output (file + stdout)
- Configure log rotation and compression
- Configure log level
- Replace all `log.Printf` calls with structured logging

### 2. Interceptor Logging

**Current Interceptors** (`server/main.go:1195-1230`):
- `grpcServerUnaryMetricsInterceptor` - logs unary calls
- `grpcServerStreamMetricsInterceptor` - logs stream calls

**Required Changes**:
- Add request/response logging
- Add structured fields (method, duration, status, peer, etc.)
- Add correlation IDs for request tracing
- Add log levels based on status codes

### 3. Metrics Reporter

**Current** (`server/main.go:1230-1260`):
- Periodic metrics logging every 30 seconds
- Simple key=value format

**Required Changes**:
- Use structured logging
- Add timestamp
- Add server ID field
- Consider Prometheus metrics export

### 4. Lifecycle Events

**Current Log Points**:
- Server startup (`server/main.go:1320`)
- Shutdown signal (`server/main.go:1328`)
- Fatal errors (`server/main.go:1285, 1311, 1334`)

**Required Changes**:
- Use appropriate log levels (Info, Error, Fatal)
- Add structured context
- Add graceful shutdown logging

## Zap Logger Requirements

### Features Needed

1. **Structured Logging**
   - JSON format for production
   - Console format for development
   - Configurable via environment variable

2. **Log Levels**
   - DEBUG, INFO, WARN, ERROR, FATAL
   - Configurable via environment variable

3. **Log Rotation**
   - Size-based rotation
   - Time-based rotation (daily)
   - Configurable max size and backup count

4. **Log Compression**
   - Compress rotated log files
   - Configurable compression (gzip/zstd)
   - Configurable compression age

5. **Log Cleanup**
   - Delete old log files based on age
   - Delete old log files based on count
   - Configurable retention policy

6. **Multiple Outputs**
   - File output (with rotation)
   - Console output (for development)
   - Configurable output targets

### Recommended Libraries

**Core Logger**:
- `go.uber.org/zap` - High-performance structured logger

**Log Rotation**:
- `gopkg.in/natefinch/lumberjack.v2` - Log rotation with compression
- Alternative: `github.com/lestrrat-go/file-rotatelogs`

### Configuration Structure

```go
type LoggerConfig struct {
    // Output configuration
    OutputPath    string // Log file path (e.g., "logs/server.log")
    EnableConsole bool   // Also log to console

    // Level configuration
    Level string // DEBUG, INFO, WARN, ERROR, FATAL

    // Format configuration
    Format string // json, console

    // Rotation configuration
    MaxSize    int // Max size in MB before rotation
    MaxBackups int // Max number of old log files
    MaxAge     int // Max days to retain old log files
    Compress   bool // Compress rotated files

    // gRPC specific
    LogRequests  bool // Log request payloads
    LogResponses bool // Log response payloads
}
```

### Environment Variables

```bash
# Logger configuration
LOCK_SERVER_LOG_LEVEL=info
LOCK_SERVER_LOG_FORMAT=json
LOCK_SERVER_LOG_OUTPUT=logs/server.log
LOCK_SERVER_LOG_CONSOLE=true
LOCK_SERVER_LOG_MAX_SIZE=100        # MB
LOCK_SERVER_LOG_MAX_BACKUPS=10
LOCK_SERVER_LOG_MAX_AGE=30          # days
LOCK_SERVER_LOG_COMPRESS=true
LOCK_SERVER_LOG_REQUESTS=false      # security consideration
LOCK_SERVER_LOG_RESPONSES=false
```

## Implementation Strategy

### Phase 1: Core Logger Setup
1. Create `pkg/logger` package
2. Implement zap logger initialization
3. Implement lumberjack rotation
4. Add configuration via environment variables
5. Add logger to server initialization

### Phase 2: Replace Standard Logging
1. Replace `log.Printf` in interceptors
2. Replace `log.Printf` in metrics reporter
3. Replace `log.Printf` in lifecycle events
4. Replace `log.Fatal` with zap Fatal

### Phase 3: Enhanced Logging
1. Add request/response logging middleware
2. Add correlation ID generation
3. Add structured fields (method, peer, duration, etc.)
4. Add log level based on status codes

### Phase 4: Testing & Documentation
1. Add unit tests for logger package
2. Add integration tests for logging
3. Update README with logging configuration
4. Add logging examples

## Key Files to Modify

1. **`pkg/logger/logger.go`** (NEW)
   - Logger initialization
   - Configuration parsing
   - Rotation setup

2. **`server/main.go`**
   - Import logger package
   - Initialize logger in main()
   - Replace all log.Printf calls
   - Pass logger to interceptors

3. **`go.mod`**
   - Add zap dependency
   - Add lumberjack dependency

4. **`server/main_test.go`**
   - Add logger initialization in tests
   - Test logging output

## Potential Issues

### 1. Performance Impact
- Structured logging has overhead
- File I/O for log rotation
- Compression CPU usage

**Mitigation**:
- Use zap's high-performance encoder
- Configure appropriate log level
- Use async logging if needed

### 2. Disk Space
- Log files can consume disk space
- Rotation and compression help

**Mitigation**:
- Configure appropriate MaxSize, MaxBackups, MaxAge
- Monitor disk usage
- Add disk space alerts

### 3. Security
- Request/response logging may expose sensitive data
- Log files may contain sensitive information

**Mitigation**:
- Make request/response logging optional (default off)
- Sanitize sensitive fields
- Secure log file permissions
- Consider log encryption

### 4. Compatibility
- Existing log parsing tools may break
- Log format changes

**Mitigation**:
- Keep console format similar to current
- Document new log format
- Provide migration guide

## Dependencies to Add

```go
require (
    go.uber.org/zap v1.27.0
    gopkg.in/natefinch/lumberjack.v2 v2.2.1
)
```

## Success Criteria

1. ✅ All server logs use structured logging
2. ✅ Log files rotate based on size
3. ✅ Rotated logs are compressed
4. ✅ Old logs are cleaned up automatically
5. ✅ Log level is configurable
6. ✅ Console output available for development
7. ✅ No performance degradation > 5%
8. ✅ All tests pass
9. ✅ Documentation updated
