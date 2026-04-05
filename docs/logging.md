# Zap Logger Integration

## Overview

The klock gRPC server now uses zap structured logging with automatic log rotation, compression, and cleanup capabilities.

## Features

- ✅ Structured logging in JSON format (production) or console format (development)
- ✅ Configurable log levels (DEBUG, INFO, WARN, ERROR, FATAL)
- ✅ Automatic log rotation based on file size
- ✅ Automatic compression of rotated log files (gzip)
- ✅ Automatic cleanup of old log files
- ✅ Multiple output targets (file + console)
- ✅ Environment variable configuration

## Configuration

All logging configuration is done via environment variables:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `LOCK_SERVER_LOG_LEVEL` | `info` | Log level (debug, info, warn, error, fatal) |
| `LOCK_SERVER_LOG_FORMAT` | `json` | Log format (json, console) |
| `LOCK_SERVER_LOG_OUTPUT` | `logs/server.log` | Log file path |
| `LOCK_SERVER_LOG_CONSOLE` | `true` | Also log to console |
| `LOCK_SERVER_LOG_MAX_SIZE` | `100` | Max size in MB before rotation |
| `LOCK_SERVER_LOG_MAX_BACKUPS` | `10` | Max number of old log files |
| `LOCK_SERVER_LOG_MAX_AGE` | `30` | Max days to retain old log files |
| `LOCK_SERVER_LOG_COMPRESS` | `true` | Compress rotated files |

## Usage Examples

### Development Mode (Console Output)

```bash
export LOCK_SERVER_LOG_FORMAT=console
export LOCK_SERVER_LOG_LEVEL=debug
export LOCK_SERVER_LOG_CONSOLE=true
./bin/server
```

### Production Mode (JSON to File)

```bash
export LOCK_SERVER_LOG_FORMAT=json
export LOCK_SERVER_LOG_LEVEL=info
export LOCK_SERVER_LOG_CONSOLE=false
export LOCK_SERVER_LOG_OUTPUT=/var/log/klock/server.log
export LOCK_SERVER_LOG_MAX_SIZE=100
export LOCK_SERVER_LOG_MAX_BACKUPS=10
export LOCK_SERVER_LOG_MAX_AGE=30
export LOCK_SERVER_LOG_COMPRESS=true
./bin/server
```

### Minimal Configuration

```bash
# All defaults are suitable for most deployments
./bin/server
```

## Log Format

### JSON Format (Production)

```json
{
  "level": "info",
  "time": "2026-04-05T12:59:35.065+0800",
  "caller": "server/main.go:1320",
  "msg": "lock grpc server started",
  "address": ":8080",
  "server_id": "srv_81488f8c3d852934",
  "protocol": "1",
  "auth_enabled": false,
  "rate_rps": 200,
  "rate_burst": 400
}
```

### Console Format (Development)

```
2026-04-05T12:59:35.065+0800	INFO	server/main.go:1320	lock grpc server started	{"address": ":8080", "server_id": "srv_81488f8c3d852934"}
```

## Log Rotation

Log files are automatically rotated when they reach the configured max size:

1. When `logs/server.log` reaches 100 MB (default)
2. It's renamed to `logs/server-2026-04-05T12-59-35.065.log`
3. A new `logs/server.log` is created
4. Old files are compressed to `.gz` format
5. Files older than 30 days (default) are deleted
6. Maximum 10 backup files (default) are kept

## Log File Structure

```
logs/
├── server.log                           # Current log file
├── server-2026-04-05T12-00-00.000.log.gz  # Rotated and compressed
├── server-2026-04-05T13-00-00.000.log.gz
└── server-2026-04-05T14-00-00.000.log.gz
```

## Structured Fields

All log messages include structured fields for easy parsing and querying:

### Server Lifecycle
- `address` - Server listen address
- `server_id` - Unique server identifier
- `protocol` - Protocol version
- `auth_enabled` - Authentication status
- `rate_rps` - Rate limit RPS
- `rate_burst` - Rate limit burst

### gRPC Calls
- `method` - gRPC method name
- `code` - Response code (OK, ERROR, etc.)
- `duration` - Request duration
- `peer` - Client peer address
- `success` - Request success status

### Metrics
- `unary_calls` - Total unary calls
- `unary_failures` - Failed unary calls
- `stream_calls` - Total stream calls
- `stream_failures` - Failed stream calls
- `auth_failures` - Authentication failures
- `rate_limited` - Rate limited requests
- `active_watchers` - Active stream watchers
- `session_invalidations` - Session invalidations

## Monitoring

### Log File Monitoring

```bash
# Watch current log file
tail -f logs/server.log

# Watch all log files
tail -f logs/*.log

# Search logs
cat logs/server.log | jq 'select(.level=="error")'
```

### Log Rotation Monitoring

```bash
# Check log file sizes
ls -lh logs/

# Check compressed files
ls -lh logs/*.gz

# Check total log size
du -sh logs/
```

## Performance

Zap is designed for high-performance logging:

- Zero-allocation JSON encoding
- Minimal CPU overhead
- Efficient log rotation
- Asynchronous compression

Expected overhead: < 5% performance impact

## Troubleshooting

### Log File Not Created

```bash
# Check directory permissions
ls -ld logs/

# Create logs directory
mkdir -p logs/
chmod 755 logs/
```

### Log Rotation Not Working

```bash
# Check current log file size
ls -lh logs/server.log

# Check rotation settings
echo $LOCK_SERVER_LOG_MAX_SIZE
echo $LOCK_SERVER_LOG_MAX_BACKUPS
echo $LOCK_SERVER_LOG_COMPRESS
```

### Too Many Log Files

```bash
# Reduce retention
export LOCK_SERVER_LOG_MAX_BACKUPS=5
export LOCK_SERVER_LOG_MAX_AGE=7

# Reduce max size
export LOCK_SERVER_LOG_MAX_SIZE=50
```

## Testing

Run logger tests:

```bash
go test ./pkg/logger -v
```

Run all tests:

```bash
go test ./... -v
```

## Implementation Details

### Package Structure

```
pkg/logger/
├── config.go       # Configuration parsing
├── logger.go       # Logger initialization
└── logger_test.go  # Unit tests
```

### Dependencies

- `go.uber.org/zap` - High-performance structured logger
- `gopkg.in/natefinch/lumberjack.v2` - Log rotation with compression

### Integration Points

1. **Server Initialization** (`server/main.go:1272`)
   - Logger created before gRPC server
   - Global logger replaced

2. **gRPC Interceptors** (`server/main.go:1195-1228`)
   - Unary and stream interceptors use structured logging
   - All request metadata logged

3. **Metrics Reporter** (`server/main.go:1231-1266`)
   - Periodic metrics logged every 30 seconds
   - All metrics fields structured

## Future Enhancements

- [ ] Request/response payload logging (with security considerations)
- [ ] Correlation ID generation for request tracing
- [ ] Prometheus metrics export
- [ ] Log forwarding to external systems (ELK, Splunk)
- [ ] Log encryption for sensitive environments
