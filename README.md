# klock

`klock` 是一个基于 Go 的两级层级锁实现，包含：

1. 本地进程内层级锁 `pkg/hierlock`
2. 单节点 gRPC 锁服务 `server`
3. Go 客户端 SDK `client`

当前实现重点是安全优先：客户端先失效、服务端后回收。

`client.Client` 含后台 goroutine（heartbeat/local sweep），业务侧使用完需要调用 `Close(ctx)` 回收资源。`Close(ctx)` 后该实例不可复用。

## 功能概览

1. 两级 Key 锁：`L1 -> L2`（如 `tenant -> resource`）
2. 父子冲突控制：锁定 L1 时阻塞该前缀下 L2
3. 分片存储：降低 map 竞争
4. 引用计数回收：动态创建锁节点并回收
5. `context` 超时：避免逻辑死锁
6. 会话租约（session lease）+ 全局 heartbeat（不是每把锁一个心跳）
7. fencing token（单调递增）
8. request id 幂等（acquire/release）
9. 客户端 `LockHandle.Done()` 被动失效通知

## 目录结构

```text
.
├── client/              # Go SDK
├── docs/                # 设计与场景分析文档
├── proto/               # gRPC/protobuf 协议定义
├── pkg/hierlock/        # 进程内层级锁
├── pkg/lockrpcpb/       # protoc 生成的 Go 代码
├── server/              # gRPC 锁服务
└── go.mod
```

详细设计与逐场景分析见：[docs/design-scenarios.md](docs/design-scenarios.md)。
gRPC 双向流迁移设计见：[docs/grpc-migration.md](docs/grpc-migration.md)。

## 快速开始

### 1) 启动服务端

```bash
go run ./server
```

默认监听 `:8080`。

可选环境变量：

1. `LOCK_SERVER_ADDR`（默认 `:8080`）
2. `LOCK_SERVER_SHARDS`（默认 `1024`）
3. `LOCK_SERVER_DEFAULT_LEASE_MS`（默认 `8000`）
4. `LOCK_SERVER_MAX_LEASE_MS`（默认 `60000`）
5. `LOCK_SERVER_AUTH_TOKEN`（可选，设置后启用 token 鉴权）
6. `LOCK_SERVER_RATE_LIMIT_RPS`（默认 `200`）
7. `LOCK_SERVER_RATE_LIMIT_BURST`（默认 `400`）
8. `LOCK_SERVER_IDEMPOTENCY_ENTRIES`（默认 `65536`）
9. `LOCK_SERVER_IDEMPOTENCY_TTL_MS`（默认 `3600000`，即 1h）

### 2) 客户端使用

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/puper/klock/client"
)

func main() {
	c := client.NewWithConfig("grpc://127.0.0.1:8080", client.Config{
		SessionLease:      8 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatTimeout:  1500 * time.Millisecond,
		LocalTTL:          3 * time.Second,
		ServerLeaseBuffer: 5 * time.Second,
		AuthToken:         "replace-with-your-token", // 对应 LOCK_SERVER_AUTH_TOKEN
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	lockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	h, err := c.Lock(lockCtx, "tenant-1", "res-1", client.LockOption{
		Timeout: 1200 * time.Millisecond, // 整次加锁调用的总等待时间（含内部重试）；0 表示一直重试直到 ctx 结束
	})
	if err != nil {
		panic(err)
	}

	select {
	case ev := <-h.Done():
		fmt.Printf("lock closed: type=%s code=%s msg=%s\n", ev.Type, ev.ErrorCode, ev.Message)
		return
	case <-time.After(500 * time.Millisecond):
	}

	unlockCtx, unlockCancel := context.WithTimeout(context.Background(), time.Second)
	defer unlockCancel()
	if err := h.Unlock(unlockCtx); err != nil {
		panic(err)
	}
}
```

## 客户端语义

`Lock/LockL1` 返回 `*LockHandle`：

1. `Token`：服务端锁 token
2. `Fence`：fencing token（单调递增）
3. `Done() <-chan LockEvent`：锁关闭/失效通知
4. `Unlock(ctx)`：主动释放
5. `UnlockWithRetry(ctx, maxAttempts, retryDelay)`：解锁失败自动重试（复用同一 request_id）

`Done()` 可能收到的事件：

1. `unlocked`：主动释放
2. `session_gone`：会话过期或会话失效
3. `server_changed`：服务端实例变化（重启/切换）
4. `protocol_mismatch`：协议版本不一致
5. `local_expired`：本地 TTL 到期

## 心跳与租约策略

默认策略（可配置）：

1. heartbeat 请求超时：`2s`
2. 连续两次 heartbeat 失败：客户端立即 fail-closed（当前 session 下所有锁本地失效）
3. 未显式指定 lock 时长：每次 heartbeat 成功后自动续本地 TTL
4. 服务端租约（若未手动指定 `SessionLease`）：
   `server lease = LocalTTL + ServerLeaseBuffer`

设计目标：

1. 客户端本地比服务端更早失效，减少异常写入窗口
2. 服务端租约兜底，最终回收锁

## 失败策略（Lock）

当 `Lock` 最终失败且原因不是服务端明确的锁冲突超时（`LOCK_TIMEOUT`）时，
客户端会自动 fail-closed 当前 session（广播 `session_gone`，错误码 `LOCK_UNCERTAIN_RESULT`），
并异步关闭远端 session，减少“服务端已成功加锁但客户端未确认”导致的挂锁窗口。

## gRPC 通信

客户端支持 `grpc://host:port` 地址，启用 gRPC unary + `WatchSession` 双向流。
当服务端优雅重启时，会向会话 watch 流广播 `SESSION_INVALIDATED`，客户端会立刻触发 `Done()` 失效事件。
服务端默认开启基础限流（可配置），并支持 `authorization: Bearer <token>` 鉴权。
服务端会定期输出指标日志，其中 `rate_limiter_entries` 表示当前限流分桶数量。

## 协议生成

协议源文件：`proto/lock.proto`。
如修改协议，请重新生成：

```bash
./scripts/gen-proto.sh
```

幂等说明（每个 session）：

1. `acquire/release` 幂等缓存支持容量与 TTL 双限制（可配置）
2. 默认容量 `65536`，默认 TTL `1h`
3. 超出容量按最旧优先淘汰，超过 TTL 的请求键会过期

## 测试

```bash
go test ./...
go test -race ./...
```

压测（示例）：

```bash
LOCK_SERVER_AUTH_TOKEN=bench-token \
LOCK_SERVER_RATE_LIMIT_RPS=5000 \
LOCK_SERVER_RATE_LIMIT_BURST=10000 \
go run ./server

go run ./cmd/loadtest \
  -addr grpc://127.0.0.1:8080 \
  -token bench-token \
  -concurrency 64 \
  -duration 30s \
  -keyspace 2048
```

客户端演示（场景日志）：

```bash
LOCK_SERVER_AUTH_TOKEN=demo-token go run ./server

go run ./cmd/clientdemo \
  -addr grpc://127.0.0.1:8080 \
  -token demo-token
```

## 适用场景与边界

适合：

1. 单节点锁服务
2. 业务侧需要层级冲突控制
3. 需要客户端可感知的锁失效通知

不适合：

1. 多副本强一致分布式锁（当前未引入共识存储）
2. 跨机房强容灾锁语义
