# 🔍 全量审计报告 - klock 分布式锁系统

**审计日期**: 2026-04-05  
**审计模式**: 全量扫描 (`—full`)  
**审计范围**: server/, pkg/, client/, cmd/  
**审计员**: Code Auditor (Critic-Led Review)

---

## 📊 执行摘要

**总文件数**: 8 个核心文件  
**总代码行数**: 3,091 行  
**已验证漏洞**: 1 个 (HIGH 严重性)  
**未验证风险**: 5 个 (设计改进建议)  
**误报**: 21 个  

### 关键发现
- ✅ **服务器实现质量优秀**：并发处理、错误处理、资源管理均正确
- ✅ **锁实现正确性高**：引用计数、写者优先、清理路径均正确
- ⚠️ **客户端存在竞争条件**：会话失效期间可能产生孤儿锁句柄

---

## 🚨 已验证漏洞 (VERIFIED)

### CVE-001: 客户端孤儿锁句柄竞争条件

**严重性**: HIGH  
**类别**: Logic | Concurrency  
**位置**: `client/client.go:L417-440`  
**CWE**: CWE-367 (Time-of-check Time-of-use Race Condition)

#### 描述

在 `lockWithScope` 函数中，锁获取成功与会话失效之间存在时间窗口。如果在此窗口期间另一个 goroutine 观察到服务器变更并使会话失效，新创建的锁句柄将变成孤儿状态。

#### 复现路径

```
线程 A (lockWithScope):
  L417: acquire() 成功 -> sessionID="s1", serverID="server1"
  L425: attemptCancel()
  L426: err == nil, 进入成功路径
  L427: observeServer("server1") -> 无变化，会话仍有效
  L428: newHandle() 创建句柄 h
  L430: c.mu.Lock() <- 等待锁

线程 B (observeServer):
  L1150: c.mu.Lock()
  L1156: c.serverID = "server2"
  L1162: invalidateSessionLocked("s1") -> 标记会话 "s1" 无效
  L1166: collectHandlesLocked("s1") -> 收集句柄
         [注意：线程 A 的句柄尚未在 map 中，因此未被收集！]
  L1167: c.mu.Unlock()

线程 A (继续):
  L430: c.mu.Lock() -> 获得锁
  L438: c.handles[h.Token] = h -> 添加句柄，sessionID="s1" 已失效
  L439: c.mu.Unlock()
  L440: return h, nil
```

#### 影响

1. **一致性违规**：客户端认为持有锁，但服务器可能已释放
2. **资源泄漏**：孤儿句柄仅依赖本地 TTL 过期清理
3. **静默失败**：无立即失败通知，难以调试

#### 修复建议

**选项 1: 原子性句柄注册**
```go
// 在 lockWithScope 中
c.mu.Lock()
defer c.mu.Unlock()

// 检查会话是否仍然有效
if c.sessionID != sessionID || c.serverID != resp.ServerID {
    // 会话已失效，释放锁并返回错误
    go c.unlockHandleWithRequestID(context.Background(), h, releaseReqID)
    return nil, errors.New("session invalidated during acquire")
}

c.handles[h.Token] = h
```

**选项 2: 会话版本控制**
```go
type Client struct {
    sessionVersion int64  // 每次服务器变更时递增
    // ...
}

// 在句柄中存储版本
h.sessionVersion = c.sessionVersion

// 注册时验证版本
if h.sessionVersion != c.sessionVersion {
    return nil, errors.New("stale session version")
}
```

**选项 3: 失败标记**
```go
// 在 collectHandlesLocked 中
for token, h := range c.handles {
    if h.sessionID == sessionID {
        h.fail(nil)  // 立即标记为失败
        delete(c.handles, token)
    }
}

// 在 lockWithScope 中
if h.failed {
    return nil, errors.New("handle invalidated during registration")
}
```

#### 测试验证

已创建复现测试：`client/orphaned_handle_race_test.go`

---

## ⚠️ 未验证风险 (UNVERIFIED)

### RISK-001: 无限制的会话创建

**严重性**: MEDIUM  
**类别**: Security | Performance  
**位置**: `server/main.go:L272-321`

#### 描述
服务器允许无限制创建会话，每个会话消耗内存（幂等性缓存、通道、定时器、锁句柄）。攻击者可通过分布式攻击耗尽服务器内存。

#### 缓解措施
- 速率限制器提供每个 peer+method 的保护
- 分布式攻击可绕过单 IP 限制

#### 建议
添加配置选项限制最大会话数：
```go
maxSessions := envAsInt("LOCK_SERVER_MAX_SESSIONS", 100000)
if len(s.sessions) >= maxSessions {
    return nil, status.Error(codes.ResourceExhausted, "max sessions reached")
}
```

---

### RISK-002: 单会话无限制锁数量

**严重性**: MEDIUM  
**类别**: Security | Performance  
**位置**: `server/main.go:L380-484`

#### 描述
单个会话可获取无限制数量的锁，每个锁消耗 `lockHandle` 条目。

#### 建议
添加每个会话的锁数量限制：
```go
maxLocksPerSession := envAsInt("LOCK_SERVER_MAX_LOCKS_PER_SESSION", 10000)
if len(st.locksByToken) >= maxLocksPerSession {
    return nil, status.Error(codes.ResourceExhausted, "max locks per session")
}
```

---

### RISK-003: 幂等性缓存返回过期令牌

**严重性**: MEDIUM  
**类别**: Logic  
**位置**: `server/main.go:L753-775, L487-524`

#### 描述
当锁被释放后，使用相同的 `request_id` 再次获取锁会返回已释放的旧令牌。

#### 设计权衡
- **当前行为**：幂等性保证"相同 request_id = 相同响应"
- **问题**：令牌已失效，客户端收到混淆错误
- **替代方案**：释放时清除幂等性缓存条目（但可能破坏合法重试场景）

#### 建议
文档化此行为，或在令牌释放时使幂等性缓存条目失效。

---

### RISK-004: 缺少字符串长度验证

**严重性**: LOW  
**类别**: Security  
**位置**: `server/main.go:L380-391`

#### 描述
服务器验证 `P1` 和 `P2` 非空，但未验证最大长度。客户端可发送超长字符串导致内存问题。

#### 建议
添加长度限制：
```go
maxKeyLength := envAsInt("LOCK_SERVER_MAX_KEY_LENGTH", 1024)
if len(p1) > maxKeyLength || len(p2) > maxKeyLength {
    return nil, status.Error(codes.InvalidArgument, "key too long")
}
```

---

### RISK-005: Lock → LockL1 自死锁风险

**严重性**: LOW  
**类别**: Logic | Usage Hazard  
**位置**: `pkg/hierlock/locker.go:L132-138`

#### 描述
在同一 goroutine 中先调用 `Lock(p1, p2)` 再调用 `LockL1(p1)` 会导致自死锁。

#### 原因
- `Lock` 获取 L1 的 RLock
- `LockL1` 尝试获取 L1 的 Lock
- Go 的 RWMutex 不允许同一 goroutine 在持有 RLock 时获取 Lock

#### 建议
在文档中明确警告此使用约束。

---

## ✅ 误报分析 (FALSE POSITIVE)

### 已排除的潜在问题

经过 Critic 深度分析，以下问题被确认为误报：

#### 服务器端 (server/main.go)
1. **定时器回调与心跳竞争** - 所有操作在 `s.mu` 保护下，无竞争
2. **goroutine 泄漏** - `defer cancelAcquireWait(nil)` 确保清理
3. **通道双重关闭** - 映射检查确保仅关闭一次
4. **TOCTOU 竞争** - 正确的双重检查模式
5. **无界 expiredSessions 增长** - 有修剪机制（10分钟，最多10000条目）

#### 锁实现 (pkg/hierlock/locker.go)
1. **引用计数竞争** - 分片锁保护，`sync.Once` 防止双重释放
2. **pendingWriters 下溢** - `sync.Once` 保证单次执行
3. **节点删除竞争** - 正确的释放顺序：锁 → 计数器 → 引用
4. **写者饥饿** - `pendingWriters` 门控有效防止
5. **哈希冲突** - 仅影响性能，不影响正确性

#### 客户端 (client/client.go)
1. **WaitGroup goroutine 泄漏** - WaitGroup 计数器平衡，必然完成
2. **上下文泄漏** - 父上下文取消保证子上下文清理
3. **流生命周期** - 所有路径正确调用 `CloseSend()`
4. **通道双重关闭** - `sync.Once` 保护
5. **并发 Close() 安全** - 所有操作幂等

---

## 📈 代码质量评估

### 优点
1. **并发正确性**：正确使用互斥锁、序列号、`sync.Once`
2. **错误处理**：所有错误路径正确清理资源
3. **防御性编程**：会话有效性检查、双重验证模式
4. **测试覆盖**：关键路径有测试验证

### 改进建议
1. **资源限制**：添加会话/锁数量限制防止 DoS
2. **输入验证**：添加字符串长度限制
3. **文档化**：明确使用约束（如 Lock/LockL1 互斥）
4. **监控指标**：添加资源使用指标（会话数、锁数）

---

## 🎯 行动计划

### 立即修复 (P0)
- [ ] 修复客户端孤儿句柄竞争 (CVE-001)
- [ ] 添加单元测试验证修复

### 短期改进 (P1)
- [ ] 添加会话数量限制配置
- [ ] 添加每会话锁数量限制配置
- [ ] 添加密钥长度验证

### 长期优化 (P2)
- [ ] 文档化 Lock/LockL1 使用约束
- [ ] 添加资源使用监控指标
- [ ] 考虑幂等性缓存失效策略

---

## 📝 测试验证

### 已创建测试
- `client/orphaned_handle_race_test.go` - 孤儿句柄竞争复现测试

### 建议测试
- 并发会话创建压力测试
- 单会话大量锁获取测试
- 服务器故障转移场景测试

---

## 🔐 安全建议

1. **DoS 防护**：实施资源限制（会话、锁、密钥长度）
2. **输入验证**：验证所有客户端输入
3. **监控告警**：监控资源使用异常
4. **速率限制**：考虑全局速率限制（非仅 per-peer）

---

## 📚 参考资料

- [CWE-367: Time-of-check Time-of-use Race Condition](https://cwe.mitre.org/data/definitions/367.html)
- [Go RWMutex 文档](https://pkg.go.dev/sync#RWMutex)
- [分布式锁最佳实践](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)

---

**审计结论**: klock 系统整体质量优秀，但存在一个需要立即修复的客户端竞争条件。建议优先修复 CVE-001，然后逐步实施资源限制和输入验证。

**审计员签名**: Code Auditor  
**日期**: 2026-04-05
