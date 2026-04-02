# gRPC 双向流迁移设计与落地现状（安全优先）

本文定义 `klock` 从 HTTP + heartbeat 迁移到 gRPC 的方案与当前落地状态，并保证：

1. server 优雅重启时可主动通知客户端锁失效。
2. server 非优雅中断时客户端仍 fail-closed。
3. 幂等、fencing、层级锁冲突语义保持不变。

## 1. 设计目标与约束

### 目标（已完成）

1. 用 gRPC 替换现有 HTTP 接口（客户端与服务端均已切换为纯 gRPC）。
2. 引入双向流，实现服务端事件主动推送。
3. 保持客户端“无法确认有效性就立即失效”的安全策略。

### 非目标

1. 不改变单节点锁服务定位。
2. 不引入多副本一致性协议（Raft/etcd）。

## 2. API 分层

采用“控制面 unary + 事件面 bidi stream”：

1. unary（请求/响应）
   1. `CreateSession`
   2. `Heartbeat`
   3. `Acquire`
   4. `Release`
   5. `CloseSession`
2. bidi stream
   1. `WatchSession(stream ClientMsg) returns (stream ServerEvent)`

原因：

1. `Acquire/Release` 幂等与重试语义在 unary 更稳定。
2. 事件流仅承载“状态变化通知”，职责清晰。

## 3. 协议字段（当前）

### 通用

1. `protocol_version`：协议版本
2. `server_id`：服务实例 ID（每次重启变化）
3. `session_id`：会话 ID
4. `request_id`：幂等键

### 事件类型

1. `WATCH_ESTABLISHED`
2. `SESSION_INVALIDATED`
3. `PROTOCOL_MISMATCH`

说明：`server_id` 变化通过 unary RPC 返回 `SERVER_INSTANCE_MISMATCH` 判定，不单独定义 stream 事件。

`SESSION_INVALIDATED.reason` 建议枚举：

1. `lease_expired`
2. `client_close`
3. `server_restarting`
4. `admin_close`

## 4. 服务端状态机

### session 结构（在现有 `sessionState` 基础上扩展）

1. 保留：`lease/expiresAt/timer/idempotency/locksByToken`
2. 新增：`watchers`（该 session 的订阅流）

### 关键规则

1. `expireSession(sessionID, reason)`：
   1. 删除 session
   2. 回收锁
   3. 向 watchers 广播 `SESSION_INVALIDATED(reason)`
2. 广播不能阻塞主锁：
   1. 在临界区内只拷贝 watcher 列表
   2. 在临界区外发送事件
3. watcher 必须有界缓冲（例如 64）
   1. 满队列视为慢消费者，立即踢出并关闭流

## 5. 优雅重启流程

服务进入重启流程时：

1. 标记 `draining=true`（拒绝新 `CreateSession/Acquire/WatchSession`）
2. 遍历所有活跃 session 调用 `expireSession(sessionID, "server_restarting")`
3. 等待短暂窗口（例如 200~500ms）让事件尽量送达
4. 关闭 gRPC server

说明：

1. 即使第 3 步期间部分客户端未收到事件，连接关闭后客户端仍会断流 fail-closed。

## 6. 客户端状态机

### 有效性条件

锁有效当且仅当：

1. 本地 `LockHandle` 未结束
2. `session` 未失效
3. `watch stream` 活跃且 `server_id` 匹配

### 失效触发条件（任一满足即失效）

1. 收到 `SESSION_INVALIDATED`
2. 观察到 `server_id` 变化（`SERVER_INSTANCE_MISMATCH`）
3. stream 断开且短时重连失败
4. unary 调用返回 `SESSION_GONE/SESSION_NOT_FOUND/SERVER_INSTANCE_MISMATCH`
5. 本地 TTL 到期

### 客户端执行动作

1. `invalidateSession(sessionID)`
2. 广播 `Done()` 事件给该 session 下全部句柄
3. 清理句柄映射，保证只触发一次（`sync.Once`）

## 7. 一致性与安全性

1. `fence` 单调递增语义不变。
2. `Acquire/Release` 幂等窗口策略不变（有界最旧淘汰）。
3. 对“未知状态”统一按失效处理，避免误持锁。
4. 协议或版本异常返回明确错误码，不使用隐式行为。

## 8. 推荐错误码（当前实现）

1. `SESSION_GONE`
2. `SESSION_NOT_FOUND`
3. `LOCK_TIMEOUT`
4. `IDEMPOTENCY_CONFLICT`
5. `LOCK_NOT_FOUND`
6. `SERVER_INSTANCE_MISMATCH`
7. `PROTOCOL_MISMATCH`
8. `SERVER_DRAINING`

## 9. 测试矩阵（必须）

1. 正常路径
   1. create -> watch -> acquire -> release
2. 重启通知
   1. watch 建立后触发 graceful restart，客户端收到 `SESSION_INVALIDATED(server_restarting)`
3. 崩溃路径
   1. 不发事件直接断流，客户端 fail-closed
4. 并发与竞态
   1. `Acquire` 成功与 `SESSION_INVALIDATED` 并发
   2. 慢 watcher 不阻塞主流程
5. 幂等
   1. request_id 重放返回一致结果
   2. 不同 token 复用同 release request_id 返回冲突
6. 稳定性
   1. `go test -race ./...`

## 10. 当前落地状态与后续

当前已落地：

1. 协议已切到 `proto/lock.proto` + `protoc` 生成代码
2. 客户端与服务端均为纯 gRPC
3. 已具备鉴权（Bearer token）与基础限流
4. 已提供压测工具 `cmd/loadtest`

后续上线门槛建议：

1. 重启通知与断流失效路径测试全部通过
2. race 测试通过
3. 压测确认限流阈值与尾延迟符合业务 SLO
