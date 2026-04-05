---
title: "Zap Logger Integration Implementation Plan"
link: "zap-logger-integration-plan"
type: implementation_plan
ontological_relations:
  - relates_to: [[zap-logger-integration]]
tags: [plan, logging, zap, grpc, implementation]
uuid: "550e8400-e29b-41d4-a716-446655440002"
created_at: "2026-04-05T10:45:00Z"
parent_research: ".artifacts/research/2026-04-05_10-30-00_zap-logger-integration.md"
git_commit_at_plan: "7fc8d96"
---

## Goal

Integrate zap structured logging into the klock gRPC server with automatic log rotation, compression, and cleanup capabilities. Replace all standard library `log` usage with structured, leveled logging.

## Scope & Assumptions

### IN Scope
- Create logger package with zap + lumberjack integration
- Replace all `log.Printf` calls in server/main.go with structured logging
- Add log rotation based on file size
- Add log compression for rotated files
- Add automatic cleanup of old log files
- Configure logging via environment variables
- Support both JSON (production) and console (development) formats
- Support both file and console output simultaneously

### OUT of Scope
- Request/response payload logging (security consideration, can be added later)
- Correlation ID generation (can be added later)
- Prometheus metrics export
- Log encryption
- Log forwarding to external systems (ELK, Splunk, etc.)
- Performance benchmarking (beyond ensuring no >5% degradation)
- User documentation (only developer notes)

### Assumptions
- Go 1.25.0 environment
- gRPC server runs on Linux/macOS/Windows
- Logs directory is writable
- Default configuration is suitable for most deployments
- No existing log parsing tools depend on current format

## Deliverables

1. **pkg/logger/logger.go** - Logger initialization and configuration
2. **pkg/logger/config.go** - Configuration structure and parsing
3. **pkg/logger/logger_test.go** - Unit tests for logger package
4. **server/main.go** - Updated to use zap logger
5. **go.mod** - Updated dependencies

## Readiness

### Preconditions
- ✅ Go 1.25.0 installed
- ✅ gRPC server code exists and is functional
- ✅ `pkg/logger/` directory exists (empty)
- ✅ `logs/` directory exists (empty)
- ✅ No existing logging infrastructure to conflict with

### What Must Exist Before Starting
- Internet access to download dependencies
- Write permissions to project directory
- Go module support enabled

## Milestones

### M1: Logger Package Foundation
- Create logger package structure
- Implement configuration parsing
- Implement zap logger initialization with lumberjack

### M2: Server Integration
- Add logger to server initialization
- Replace standard log imports
- Replace all log.Printf calls with structured logging

### M3: Interceptor Enhancement
- Update metrics interceptors to use structured logging
- Add structured fields (method, duration, status, peer)
- Update metrics reporter to use structured logging

### M4: Testing & Validation
- Add unit tests for logger package
- Verify log rotation works
- Verify compression works
- Verify cleanup works
- Run all existing tests to ensure no regression

## Work Breakdown (Tasks)

### T001: Add zap and lumberjack dependencies

**Summary**: Add go.uber.org/zap and gopkg.in/natefinch/lumberjack.v2 to go.mod

**Files**:
- `go.mod` (modify)
- `go.sum` (generated)

**Changes**:
1. Run `go get go.uber.org/zap@v1.27.0`
2. Run `go get gopkg.in/natefinch/lumberjack.v2@v2.2.1`
3. Verify dependencies are added to go.mod

**Acceptance Test**:
- go.mod contains both dependencies
- go.sum is generated/updated
- `go mod download` succeeds without errors

**Evidence Contract**: `go mod verify && grep -E "zap|lumberjack" go.mod`

**Dependencies**: None
**Milestone**: M1
**Estimate**: 5 minutes

---

### T002: Create logger configuration structure

**Summary**: Create pkg/logger/config.go with LoggerConfig struct and environment variable parsing

**Files**:
- `pkg/logger/config.go` (new)

**Changes**:
1. Create `pkg/logger/config.go`
2. Define `LoggerConfig` struct with fields:
   - OutputPath (string)
   - EnableConsole (bool)
   - Level (string)
   - Format (string)
   - MaxSize (int)
   - MaxBackups (int)
   - MaxAge (int)
   - Compress (bool)
3. Implement `LoadConfigFromEnv() LoggerConfig` function
4. Read environment variables:
   - LOCK_SERVER_LOG_LEVEL (default: "info")
   - LOCK_SERVER_LOG_FORMAT (default: "json")
   - LOCK_SERVER_LOG_OUTPUT (default: "logs/server.log")
   - LOCK_SERVER_LOG_CONSOLE (default: "true")
   - LOCK_SERVER_LOG_MAX_SIZE (default: 100)
   - LOCK_SERVER_LOG_MAX_BACKUPS (default: 10)
   - LOCK_SERVER_LOG_MAX_AGE (default: 30)
   - LOCK_SERVER_LOG_COMPRESS (default: "true")
5. Implement `Validate() error` method for config validation
6. Add default values for all fields

**Acceptance Test**:
- Config struct is properly defined
- Environment variables are read correctly
- Default values are applied when env vars not set
- Validation catches invalid configurations

**Evidence Contract**: `go test ./pkg/logger -run TestLoadConfigFromEnv`

**Dependencies**: T001
**Milestone**: M1
**Estimate**: 30 minutes

---

### T003: Implement zap logger initialization with lumberjack

**Summary**: Create pkg/logger/logger.go with NewLogger function that initializes zap with lumberjack rotation

**Files**:
- `pkg/logger/logger.go` (new)

**Changes**:
1. Create `pkg/logger/logger.go`
2. Implement `NewLogger(config LoggerConfig) (*zap.Logger, error)` function
3. Create lumberjack.Logger with config values:
   - Filename: config.OutputPath
   - MaxSize: config.MaxSize
   - MaxBackups: config.MaxBackups
   - MaxAge: config.MaxAge
   - Compress: config.Compress
4. Create zap core with:
   - JSON encoder if format is "json"
   - Console encoder if format is "console"
   - Log level from config
5. Add file output using zapcore.AddSync(lumberjack)
6. If EnableConsole is true, add console output using zapcore.Lock(os.Stdout)
7. Combine outputs using zapcore.NewTee
8. Return configured zap.Logger
9. Implement `Sync() error` method wrapper
10. Add package-level default logger for convenience

**Acceptance Test**:
- Logger is created successfully
- Logs are written to file
- Logs are written to console when enabled
- JSON format is correct
- Console format is readable

**Evidence Contract**: `go test ./pkg/logger -run TestNewLogger`

**Dependencies**: T002
**Milestone**: M1
**Estimate**: 45 minutes

---

### T004: Add unit tests for logger package

**Summary**: Create pkg/logger/logger_test.go with comprehensive tests

**Files**:
- `pkg/logger/logger_test.go` (new)

**Changes**:
1. Create `pkg/logger/logger_test.go`
2. Test `TestLoadConfigFromEnv`:
   - Test with all env vars set
   - Test with no env vars (use defaults)
   - Test with invalid values
3. Test `TestNewLogger`:
   - Test JSON format
   - Test console format
   - Test file output only
   - Test file + console output
   - Test log levels (debug, info, warn, error)
4. Test `TestLogRotation`:
   - Create logger with small MaxSize (1 MB)
   - Write enough logs to trigger rotation
   - Verify multiple log files exist
   - Verify old files are compressed
5. Test `TestLogCleanup`:
   - Create logger with MaxBackups=2, MaxAge=1
   - Write logs to create multiple files
   - Verify old files are deleted

**Acceptance Test**:
- All tests pass
- Coverage > 80% for logger package
- No race conditions detected

**Evidence Contract**: `go test ./pkg/logger -v -race -cover`

**Dependencies**: T003
**Milestone**: M1
**Estimate**: 1 hour

---

### T005: Initialize logger in server main function

**Summary**: Update server/main.go to initialize zap logger at startup

**Files**:
- `server/main.go` (modify)

**Changes**:
1. Add import: `"github.com/puper/klock/pkg/logger"`
2. In `main()` function, after line 1272 (before creating locker):
   ```go
   // Initialize logger
   logConfig := logger.LoadConfigFromEnv()
   zapLogger, err := logger.NewLogger(logConfig)
   if err != nil {
       log.Fatalf("initialize logger: %v", err)
   }
   defer zapLogger.Sync()
   
   // Replace global logger
   zap.ReplaceGlobals(zapLogger)
   ```
3. Keep standard `log` import for now (will be removed in T006)

**Acceptance Test**:
- Server starts successfully
- Logger is initialized
- Log file is created in logs/ directory
- Startup message appears in log file

**Evidence Contract**: `timeout 5 go run ./server 2>&1 | head -5`

**Dependencies**: T004
**Milestone**: M2
**Estimate**: 15 minutes

---

### T006: Replace server lifecycle logging

**Summary**: Replace all log.Printf calls in server lifecycle events with structured logging

**Files**:
- `server/main.go` (modify)

**Changes**:
1. Replace line 1285 (locker creation error):
   ```go
   // OLD: log.Fatalf("create locker: %v", err)
   // NEW:
   zap.L().Fatal("failed to create locker", zap.Error(err))
   ```

2. Replace line 1311 (listen error):
   ```go
   // OLD: log.Fatalf("listen: %v", err)
   // NEW:
   zap.L().Fatal("failed to listen", zap.Error(err), zap.String("address", addr))
   ```

3. Replace line 1320 (startup message):
   ```go
   // OLD: log.Printf("lock grpc server listening on %s server_id=%s protocol=%s auth_enabled=%t rate_rps=%d rate_burst=%d idempotency_entries=%d idempotency_ttl_ms=%d", ...)
   // NEW:
   zap.L().Info("lock grpc server started",
       zap.String("address", addr),
       zap.String("server_id", serverID),
       zap.String("protocol", "grpc"),
       zap.Bool("auth_enabled", authToken != ""),
       zap.Int("rate_rps", rateLimitRPS),
       zap.Int("rate_burst", rateLimitBurst),
       zap.Int("idempotency_entries", idempotencyEntries),
       zap.Int("idempotency_ttl_ms", idempotencyTTLMS),
   )
   ```

4. Replace line 1328 (shutdown signal):
   ```go
   // OLD: log.Printf("shutdown signal received; draining sessions")
   // NEW:
   zap.L().Info("shutdown signal received; draining sessions")
   ```

5. Replace line 1334 (server error):
   ```go
   // OLD: log.Fatal(err)
   // NEW:
   zap.L().Fatal("server error", zap.Error(err))
   ```

**Acceptance Test**:
- All lifecycle events are logged with structured format
- Log level is appropriate (Info for normal, Fatal for errors)
- All fields are present and correctly formatted
- Server starts and stops cleanly

**Evidence Contract**: `go test ./server -run TestMain -v`

**Dependencies**: T005
**Milestone**: M2
**Estimate**: 20 minutes

---

### T007: Update unary interceptor logging

**Summary**: Replace log.Printf in grpcServerUnaryMetricsInterceptor with structured logging

**Files**:
- `server/main.go` (modify lines 1195-1212)

**Changes**:
1. Update `grpcServerUnaryMetricsInterceptor` function:
   ```go
   func grpcServerUnaryMetricsInterceptor(m *serverMetrics) grpc.UnaryServerInterceptor {
       return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
           start := time.Now()
           m.unaryCalls.Add(1)
           resp, err := handler(ctx, req)
           duration := time.Since(start)
           
           code := "OK"
           if err != nil {
               m.unaryFailures.Add(1)
               code = "TRANSPORT_ERROR"
           } else if withErr, ok := resp.(interface{ GetError() *lockrpcpb.ErrorStatus }); ok && withErr.GetError() != nil {
               m.unaryFailures.Add(1)
               code = withErr.GetError().GetCode()
           }
           
           // Structured logging with all relevant fields
           zap.L().Info("grpc_unary_call",
               zap.String("method", info.FullMethod),
               zap.String("code", code),
               zap.Duration("duration", duration),
               zap.String("peer", peerKeyFromContext(ctx)),
               zap.Bool("success", err == nil),
           )
           
           return resp, err
       }
   }
   ```

**Acceptance Test**:
- Unary calls are logged with structured format
- All fields are present (method, code, duration, peer, success)
- Log level is Info for all calls
- Metrics counters still work correctly

**Evidence Contract**: `go test ./server -run TestGRPCUnaryInterceptor -v`

**Dependencies**: T006
**Milestone**: M3
**Estimate**: 15 minutes

---

### T008: Update stream interceptor logging

**Summary**: Replace log.Printf in grpcServerStreamMetricsInterceptor with structured logging

**Files**:
- `server/main.go` (modify lines 1214-1228)

**Changes**:
1. Update `grpcServerStreamMetricsInterceptor` function:
   ```go
   func grpcServerStreamMetricsInterceptor(m *serverMetrics) grpc.StreamServerInterceptor {
       return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
           start := time.Now()
           m.streamCalls.Add(1)
           err := handler(srv, ss)
           duration := time.Since(start)
           
           code := "OK"
           if err != nil {
               m.streamFailures.Add(1)
               code = "STREAM_ERROR"
           }
           
           // Structured logging with all relevant fields
           zap.L().Info("grpc_stream_call",
               zap.String("method", info.FullMethod),
               zap.String("code", code),
               zap.Duration("duration", duration),
               zap.String("peer", peerKeyFromContext(ss.Context())),
               zap.Bool("success", err == nil),
           )
           
           return err
       }
   }
   ```

**Acceptance Test**:
- Stream calls are logged with structured format
- All fields are present (method, code, duration, peer, success)
- Log level is Info for all calls
- Metrics counters still work correctly

**Evidence Contract**: `go test ./server -run TestGRPCStreamInterceptor -v`

**Dependencies**: T007
**Milestone**: M3
**Estimate**: 15 minutes

---

### T009: Update metrics reporter logging

**Summary**: Replace log.Printf in startMetricsReporter with structured logging

**Files**:
- `server/main.go` (modify lines 1230-1260)

**Changes**:
1. Update `startMetricsReporter` function:
   ```go
   func startMetricsReporter(ctx context.Context, m *serverMetrics, limiter *rateLimiter) {
       ticker := time.NewTicker(30 * time.Second)
       defer ticker.Stop()
       
       report := func() {
           limiterSize := 0
           if limiter != nil {
               limiterSize = limiter.size()
           }
           
           zap.L().Info("server_metrics",
               zap.Int64("unary_calls", m.unaryCalls.Load()),
               zap.Int64("unary_failures", m.unaryFailures.Load()),
               zap.Int64("stream_calls", m.streamCalls.Load()),
               zap.Int64("stream_failures", m.streamFailures.Load()),
               zap.Int64("auth_failures", m.authFailures.Load()),
               zap.Int64("rate_limited", m.rateLimited.Load()),
               zap.Int("rate_limiter_entries", limiterSize),
               zap.Int64("active_watchers", m.activeWatchers.Load()),
               zap.Int64("session_invalidations", m.sessionInvalidation.Load()),
           )
       }
       
       for {
           select {
           case <-ctx.Done():
               report()
               return
           case <-ticker.C:
               report()
           }
       }
   }
   ```

**Acceptance Test**:
- Metrics are logged with structured format every 30 seconds
- All metric fields are present
- Log level is Info
- Final report is logged on shutdown

**Evidence Contract**: `go test ./server -run TestMetricsReporter -v`

**Dependencies**: T008
**Milestone**: M3
**Estimate**: 15 minutes

---

### T010: Remove standard log import and verify

**Summary**: Remove standard library log import and verify all logging uses zap

**Files**:
- `server/main.go` (modify)

**Changes**:
1. Remove import: `"log"` (line 10)
2. Search for any remaining `log.` references
3. Verify no compilation errors
4. Run all tests to ensure nothing broke

**Acceptance Test**:
- No standard log import in server/main.go
- Code compiles successfully
- All tests pass
- No log.Printf calls remain

**Evidence Contract**: `go build ./server && go test ./server -v`

**Dependencies**: T009
**Milestone**: M2
**Estimate**: 10 minutes

---

### T011: Integration test for log rotation and compression

**Summary**: Create integration test to verify log rotation and compression work correctly

**Files**:
- `server/main_test.go` (modify)

**Changes**:
1. Add test `TestLogRotation`:
   ```go
   func TestLogRotation(t *testing.T) {
       // Create temp directory for logs
       tmpDir := t.TempDir()
       
       // Set environment variables for small rotation
       os.Setenv("LOCK_SERVER_LOG_OUTPUT", filepath.Join(tmpDir, "test.log"))
       os.Setenv("LOCK_SERVER_LOG_MAX_SIZE", "1") // 1 MB
       os.Setenv("LOCK_SERVER_LOG_MAX_BACKUPS", "3")
       os.Setenv("LOCK_SERVER_LOG_COMPRESS", "true")
       
       // Initialize logger
       config := logger.LoadConfigFromEnv()
       zapLogger, err := logger.NewLogger(config)
       require.NoError(t, err)
       
       // Write enough logs to trigger rotation (> 1 MB)
       for i := 0; i < 100000; i++ {
           zapLogger.Info("test log message", zap.Int("iteration", i))
       }
       
       // Force sync
       zapLogger.Sync()
       
       // Verify rotation occurred
       files, err := os.ReadDir(tmpDir)
       require.NoError(t, err)
       assert.Greater(t, len(files), 1, "Expected multiple log files after rotation")
       
       // Verify compression
       compressedCount := 0
       for _, f := range files {
           if strings.HasSuffix(f.Name(), ".gz") {
               compressedCount++
           }
       }
       assert.Greater(t, compressedCount, 0, "Expected compressed log files")
   }
   ```

**Acceptance Test**:
- Test passes
- Multiple log files are created
- Old log files are compressed
- Backup count is respected

**Evidence Contract**: `go test ./server -run TestLogRotation -v`

**Dependencies**: T010
**Milestone**: M4
**Estimate**: 30 minutes

---

### T012: Run full test suite and verify no regression

**Summary**: Run all tests to ensure no functionality is broken by logging changes

**Files**:
- None (verification only)

**Changes**:
1. Run `go test ./...` to test all packages
2. Run `go test -race ./...` to check for race conditions
3. Run `go vet ./...` to check for common errors
4. Verify test coverage hasn't dropped significantly

**Acceptance Test**:
- All tests pass
- No race conditions detected
- No vet warnings
- Coverage is maintained or improved

**Evidence Contract**: `go test -race -cover ./...`

**Dependencies**: T011
**Milestone**: M4
**Estimate**: 15 minutes

---

## Risks & Mitigations

### Risk 1: Performance degradation from structured logging
**Impact**: Medium
**Probability**: Low
**Mitigation**: 
- Zap is designed for high performance
- Use async logging if needed (zap supports this)
- Monitor performance in production
- Keep log level at INFO or higher in production

### Risk 2: Disk space exhaustion from log files
**Impact**: High
**Probability**: Low
**Mitigation**:
- Configure reasonable MaxSize (100 MB default)
- Configure reasonable MaxBackups (10 default)
- Configure reasonable MaxAge (30 days default)
- Enable compression by default
- Add disk space monitoring in production

### Risk 3: Log file permissions issues
**Impact**: Medium
**Probability**: Low
**Mitigation**:
- Ensure logs/ directory exists and is writable
- Add clear error message if log file cannot be created
- Document permission requirements

### Risk 4: Breaking existing log parsing tools
**Impact**: Low
**Probability**: Medium
**Mitigation**:
- Document new log format clearly
- Provide migration guide if needed
- Console format remains human-readable
- JSON format is standard and well-supported

## Test Strategy

### Unit Tests
- Test logger configuration parsing
- Test logger initialization with various configs
- Test log rotation mechanism
- Test log compression
- Test log cleanup

### Integration Tests
- Test log rotation in real scenario
- Test compression of rotated files
- Test cleanup of old files
- Test server startup with logging
- Test server shutdown with logging

### Regression Tests
- Run all existing tests to ensure no functionality is broken
- Verify metrics counters still work
- Verify interceptors still function correctly
- Verify graceful shutdown still works

## References

### Research Document
- `.artifacts/research/2026-04-05_10-30-00_zap-logger-integration.md`

### Key Code Locations
- `server/main.go:10` - Standard log import (to be removed)
- `server/main.go:1195-1212` - Unary interceptor logging
- `server/main.go:1214-1228` - Stream interceptor logging
- `server/main.go:1230-1260` - Metrics reporter
- `server/main.go:1272-1337` - Main function
- `server/main.go:1285, 1311, 1320, 1328, 1334` - Lifecycle logging points

### External Documentation
- Zap documentation: https://pkg.go.dev/go.uber.org/zap
- Lumberjack documentation: https://pkg.go.dev/gopkg.in/natefinch/lumberjack.v2

## Final Gate

**Output Summary**:
- Plan path: `.artifacts/plan/2026-04-05_10-45-00_zap-logger-integration.md`
- Milestones: 4 (M1: Foundation, M2: Integration, M3: Enhancement, M4: Testing)
- Tasks: 12
- Git state: 7fc8d96
- Estimated total time: ~5 hours

**Next Step**: Proceed to execute-phase with the generated plan path
