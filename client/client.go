package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puper/klock/pkg/lockrpcpb"
	"google.golang.org/grpc"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	gstatus "google.golang.org/grpc/status"
)

// Scope 表示锁粒度。
type Scope string

const (
	// ScopeL1 表示一级键锁。
	ScopeL1 Scope = "l1"
	// ScopeL2 表示二级键锁。
	ScopeL2 Scope = "l2"

	protocolVersion = "1"
)

const (
	defaultHeartbeatTimeout        = 2 * time.Second
	defaultServerLeaseBuffer       = 3 * time.Second
	defaultLocalTTLMultiplier      = 2
	defaultSweepInterval           = 200 * time.Millisecond
	defaultAcquireRetryDelay       = 100 * time.Millisecond
	defaultWatchReconnectInitial   = 100 * time.Millisecond
	defaultWatchReconnectMax       = 1600 * time.Millisecond
	defaultWatchReconnectFailAfter = 5 * time.Second
	defaultWatchStableResetAfter   = 500 * time.Millisecond
)

type LockOption struct {
	// Timeout 控制“整次加锁调用”的总等待时长（包含内部重试）。
	//
	// 语义：
	// 1. =0：表示一直重试直到成功或 ctx 结束（until 模式）。
	// 2. >0：在该总时长内自动重试；超时后返回 context deadline exceeded。
	// 3. 不影响已拿到锁后的持有时长。
	Timeout time.Duration
	// AttemptTimeout 控制“单次 acquire RPC 尝试”的超时时长。
	//
	// 语义：
	// 1. =0：单次尝试不额外限时，仅受 Timeout/ctx 约束。
	// 2. >0：每次重试都会使用该上限；若剩余总时长更短，则自动截断到剩余时长。
	// 3. 建议与 Timeout 搭配使用，形成“总时长 + 单次尝试”双层超时。
	AttemptTimeout time.Duration
	// LocalTTL 控制“本地句柄有效期”。
	//
	// 语义：
	// 1. >0：使用固定本地 TTL；到期后触发 EventLocalExpired，不会自动续期。
	// 2. <=0：使用客户端默认 TTL（Config.LocalTTL），并在每次 heartbeat 成功后自动续期。
	// 3. 这是客户端本地安全边界，不等同于服务端 session lease。
	//    典型建议：LocalTTL 小于服务端 lease，让客户端先失效、服务端后兜底回收。
	LocalTTL time.Duration
}

type EventType string

const (
	// EventServerChanged 表示服务实例切换导致本地会话失效。
	EventServerChanged EventType = "server_changed"
	// EventProtocolMismatch 表示协议版本不一致。
	EventProtocolMismatch EventType = "protocol_mismatch"
	// EventSessionGone 表示会话失效（过期/不存在/主动关闭/心跳连续失败）。
	EventSessionGone EventType = "session_gone"
	// EventUnlocked 表示主动解锁成功。
	EventUnlocked EventType = "unlocked"
	// EventLocalExpired 表示本地 TTL 到期，锁在客户端侧失效。
	EventLocalExpired EventType = "local_expired"
)

// LockEvent 是锁状态变化事件。
type LockEvent struct {
	Type       EventType
	Message    string
	OldServer  string
	NewServer  string
	SessionID  string
	Token      string
	ErrorCode  string
	OccurredAt time.Time
}

// Config 定义客户端行为参数。
type Config struct {
	// SessionLease 是“客户端请求的会话租约时长”（单位：time.Duration）。
	// 1. >0：按该值向服务端申请 lease。
	// 2. =0：客户端按 LocalTTL + ServerLeaseBuffer 推导请求值。
	// 3. 最终生效值由服务端决定，可能被服务端默认值/上限策略约束。
	SessionLease time.Duration
	// HeartbeatInterval 控制心跳发送周期。
	HeartbeatInterval time.Duration
	// HeartbeatTimeout 控制单次 heartbeat RPC 的超时时间。
	HeartbeatTimeout time.Duration
	// ServerLeaseBuffer 是在未显式设置 SessionLease 时，
	// 推导服务端 lease 请求值所附加的安全缓冲（LocalTTL + ServerLeaseBuffer）。
	ServerLeaseBuffer time.Duration
	// LocalTTL 是客户端本地锁句柄有效期的默认值。
	// 1. LockOption.LocalTTL <= 0 时，使用该默认值。
	// 2. =0 时，默认取 2 * HeartbeatInterval。
	LocalTTL time.Duration
	// LocalSweepInterval 控制本地 TTL 过期扫描频率。
	LocalSweepInterval time.Duration
	// AuthToken 会通过 gRPC metadata 的 "authorization" 头以 Bearer 方式发送。
	AuthToken string
}

// Client 是锁服务 Go SDK 主体。
//
// 生命周期约束：
// 1. NewWithConfig 创建后会启动本地 TTL 扫描协程。
// 2. 使用结束必须调用 Close 释放后台资源。
// 3. Close 后实例不可复用。
type Client struct {
	baseURL string
	grpc    lockrpcpb.LockServiceClient
	conn    *grpc.ClientConn
	cfg     Config

	mu                  sync.Mutex
	sessionID           string
	serverID            string
	heartbeatStop       context.CancelFunc
	watchStop           context.CancelFunc
	sweepStop           context.CancelFunc
	sessionInitInFlight bool
	sessionInitDone     chan struct{}
	sessionInitWG       sync.WaitGroup
	bgActive            int
	bgDone              chan struct{}
	closed              bool
	nextReq             uint64
	handles             map[string]*LockHandle
	remoteCloseInFlight map[string]chan struct{}
	initErr             error
}

// LockHandle 是一次加锁成功后的本地句柄。
type LockHandle struct {
	Token string
	Fence uint64

	client    *Client
	sessionID string
	done      chan LockEvent
	once      sync.Once
	autoRenew bool
	localTTL  time.Duration
	expireAt  int64 // unix nano
}

// Done 返回锁事件通知通道。
func (h *LockHandle) Done() <-chan LockEvent {
	return h.done
}

// Unlock 主动释放锁。
func (h *LockHandle) Unlock(ctx context.Context) error {
	if h == nil || h.client == nil {
		return errors.New("nil lock handle")
	}
	return h.client.unlockHandle(ctx, h)
}

// UnlockWithRetry 在解锁失败时按固定次数与间隔重试。
// 注意：重试过程会复用同一个 release request_id，确保服务端幂等语义生效。
func (h *LockHandle) UnlockWithRetry(ctx context.Context, maxAttempts int, retryDelay time.Duration) error {
	if h == nil || h.client == nil {
		return errors.New("nil lock handle")
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if retryDelay <= 0 {
		retryDelay = 100 * time.Millisecond
	}

	releaseReqID := h.client.nextRequestID("rel")
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := h.client.unlockHandleWithRequestID(ctx, h, releaseReqID)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetriableUnlockError(err) || attempt == maxAttempts {
			break
		}
		if err := sleepWithContext(ctx, retryDelay); err != nil {
			return err
		}
	}
	return lastErr
}

// setExpireAt 设置本地过期时间（原子写）。
func (h *LockHandle) setExpireAt(t time.Time) {
	atomic.StoreInt64(&h.expireAt, t.UnixNano())
}

// expired 判断当前句柄是否已过本地 TTL（原子读）。
func (h *LockHandle) expired(now time.Time) bool {
	exp := atomic.LoadInt64(&h.expireAt)
	if exp == 0 {
		return false
	}
	return now.UnixNano() >= exp
}

// fail 触发一次性失效事件并关闭 Done 通道。
func (h *LockHandle) fail(ev LockEvent) {
	h.once.Do(func() {
		select {
		case h.done <- ev:
		default:
		}
		close(h.done)
	})
}

// sessionResponse 是会话接口响应体。
type sessionResponse struct {
	SessionID       string `json:"session_id"`
	LeaseMS         int64  `json:"lease_ms"`
	ExpiresAt       string `json:"expires_at"`
	ServerID        string `json:"server_id"`
	ProtocolVersion string `json:"protocol_version"`
}

// lockRequest 是加锁请求体。
type lockRequest struct {
	Scope     Scope  `json:"scope"`
	P1        string `json:"p1"`
	P2        string `json:"p2,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
}

// lockResponse 是加锁响应体。
type lockResponse struct {
	Token           string `json:"token"`
	Fence           uint64 `json:"fence"`
	SessionID       string `json:"session_id"`
	ServerID        string `json:"server_id"`
	ProtocolVersion string `json:"protocol_version"`
}

// APIError 封装服务端 RPC 错误与业务错误码。
type APIError struct {
	Status          int
	Code            string
	Message         string
	ServerID        string
	ProtocolVersion string
}

// Error 实现 error 接口。
func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code != "" {
		return fmt.Sprintf("lock service status %d: %s (%s)", e.Status, e.Message, e.Code)
	}
	return fmt.Sprintf("lock service status %d: %s", e.Status, e.Message)
}

// New 使用默认配置创建客户端。
func New(baseURL string) *Client {
	return NewWithConfig(baseURL, Config{})
}

// NewWithConfig 使用给定配置创建客户端并启动本地 TTL 扫描协程。
func NewWithConfig(baseURL string, cfg Config) *Client {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 1 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if cfg.ServerLeaseBuffer <= 0 {
		cfg.ServerLeaseBuffer = defaultServerLeaseBuffer
	}
	if cfg.LocalTTL <= 0 {
		cfg.LocalTTL = time.Duration(defaultLocalTTLMultiplier) * cfg.HeartbeatInterval
	}
	if cfg.LocalSweepInterval <= 0 {
		cfg.LocalSweepInterval = defaultSweepInterval
	}

	target := strings.TrimSpace(strings.TrimPrefix(baseURL, "grpc://"))
	c := &Client{
		baseURL:             target,
		cfg:                 cfg,
		handles:             make(map[string]*LockHandle),
		bgDone:              make(chan struct{}),
		remoteCloseInFlight: make(map[string]chan struct{}),
	}

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		c.initErr = err
	} else {
		c.conn = conn
		c.grpc = lockrpcpb.NewLockServiceClient(conn)
	}
	close(c.bgDone)

	if c.initErr != nil {
		return c
	}

	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	c.sweepStop = sweepCancel
	c.markBGStart()
	go c.localExpiryLoop(sweepCtx)
	return c
}

// Lock 获取二级锁。
func (c *Client) Lock(ctx context.Context, p1, p2 string, opt LockOption) (*LockHandle, error) {
	return c.lockWithScope(ctx, ScopeL2, p1, p2, opt)
}

// LockL1 获取一级锁。
func (c *Client) LockL1(ctx context.Context, p1 string, opt LockOption) (*LockHandle, error) {
	return c.lockWithScope(ctx, ScopeL1, p1, "", opt)
}

// lockWithScope 是 Lock/LockL1 的统一实现：
// 1. 支持 Timeout=0 的 until 模式；
// 2. 支持 Timeout>0 的总时长自动重试模式。
func (c *Client) lockWithScope(ctx context.Context, scope Scope, p1, p2 string, opt LockOption) (*LockHandle, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	lastSessionID := ""
	waitCtx := ctx
	cancel := func() {}
	if opt.Timeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, opt.Timeout)
	}
	defer cancel()
	acquireReqID := c.nextRequestID("acq")

	for {
		if err := waitCtx.Err(); err != nil {
			c.failClosedOnUncertainLockFailure(lastSessionID, err)
			return nil, err
		}

		sessionID, err := c.ensureSession(waitCtx)
		if err != nil {
			if !isRetriableAcquireError(err) {
				c.failClosedOnUncertainLockFailure(lastSessionID, err)
				return nil, err
			}
			if err := sleepWithContext(waitCtx, defaultAcquireRetryDelay); err != nil {
				c.failClosedOnUncertainLockFailure(lastSessionID, err)
				return nil, err
			}
			continue
		}
		lastSessionID = sessionID

		attemptCtx := waitCtx
		attemptCancel := func() {}
		if opt.AttemptTimeout > 0 {
			attemptDur := opt.AttemptTimeout
			if dl, ok := waitCtx.Deadline(); ok {
				remain := time.Until(dl)
				if remain <= 0 {
					return nil, waitCtx.Err()
				}
				if attemptDur > remain {
					attemptDur = remain
				}
			}
			attemptCtx, attemptCancel = context.WithTimeout(waitCtx, attemptDur)
		}

		attemptTimeout := int64(0)
		if dl, ok := attemptCtx.Deadline(); ok {
			remain := time.Until(dl)
			if remain <= 0 {
				attemptCancel()
				return nil, attemptCtx.Err()
			}
			attemptTimeout = durationMS(remain)
			if attemptTimeout <= 0 {
				attemptTimeout = 1
			}
		}

		resp, err := c.acquire(attemptCtx, lockRequest{
			Scope:     scope,
			P1:        p1,
			P2:        p2,
			TimeoutMS: attemptTimeout,
			SessionID: sessionID,
			RequestID: acquireReqID,
		})
		attemptCancel()
		if err == nil {
			c.observeServer(resp.ServerID)
			h := c.newHandle(resp, opt)
			releaseReqID := c.nextRequestID("rel")
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = c.unlockHandleWithRequestID(releaseCtx, h, releaseReqID)
				releaseCancel()
				return nil, errors.New("client is closed")
			}
			c.handles[h.Token] = h
			c.mu.Unlock()
			return h, nil
		}

		c.handleAPIError(sessionID, err)
		if !isRetriableAcquireError(err) {
			c.failClosedOnUncertainLockFailure(sessionID, err)
			return nil, err
		}
		if err := sleepWithContext(waitCtx, defaultAcquireRetryDelay); err != nil {
			c.failClosedOnUncertainLockFailure(sessionID, err)
			return nil, err
		}
	}
}

func isServerDeterministicAcquireFailure(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.Code {
	case "LOCK_TIMEOUT", "IDEMPOTENCY_CONFLICT":
		return true
	default:
		return false
	}
}

func (c *Client) failClosedOnUncertainLockFailure(sessionID string, cause error) {
	if sessionID == "" || isServerDeterministicAcquireFailure(cause) {
		return
	}
	c.invalidateSession(sessionID)
	msg := "lock failed with uncertain result; session invalidated"
	if cause != nil {
		msg += ": " + cause.Error()
	}
	c.broadcastSessionEvent(sessionID, LockEvent{
		Type:       EventSessionGone,
		Message:    msg,
		SessionID:  sessionID,
		ErrorCode:  "LOCK_UNCERTAIN_RESULT",
		OccurredAt: time.Now(),
	})
	c.closeSessionRemoteBestEffort(sessionID)
}

// isRetriableAcquireError 判断是否应继续重试加锁。
func isRetriableAcquireError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if st, ok := gstatus.FromError(err); ok {
		switch st.Code() {
		case gcodes.Unavailable, gcodes.DeadlineExceeded, gcodes.ResourceExhausted:
			return true
		case gcodes.Unauthenticated, gcodes.PermissionDenied, gcodes.InvalidArgument, gcodes.NotFound:
			return false
		}
	}

	var ae *APIError
	if errors.As(err, &ae) {
		switch ae.Code {
		case "LOCK_TIMEOUT", "SESSION_GONE", "SESSION_NOT_FOUND", "SERVER_INSTANCE_MISMATCH":
			return true
		}
		if ae.Status >= 500 {
			return true
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}

	var ue *url.Error
	if errors.As(err, &ue) {
		return true
	}
	return false
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// newHandle 根据服务端响应构造本地句柄并设置初始 TTL。
func (c *Client) newHandle(resp lockResponse, opt LockOption) *LockHandle {
	ttl := opt.LocalTTL
	autoRenew := false
	if ttl <= 0 {
		ttl = c.cfg.LocalTTL
		autoRenew = true
	}
	h := &LockHandle{
		Token:     resp.Token,
		Fence:     resp.Fence,
		client:    c,
		sessionID: resp.SessionID,
		done:      make(chan LockEvent, 1),
		autoRenew: autoRenew,
		localTTL:  ttl,
	}
	h.setExpireAt(time.Now().Add(ttl))
	return h
}

// unlockHandle 执行解锁请求并更新本地句柄状态。
func (c *Client) unlockHandle(ctx context.Context, h *LockHandle) error {
	releaseReqID := c.nextRequestID("rel")
	return c.unlockHandleWithRequestID(ctx, h, releaseReqID)
}

func (c *Client) unlockHandleWithRequestID(ctx context.Context, h *LockHandle, releaseReqID string) error {
	resp, err := c.grpc.Release(c.rpcContext(ctx), &lockrpcpb.ReleaseRequest{
		SessionId:       h.sessionID,
		Token:           h.Token,
		RequestId:       releaseReqID,
		ProtocolVersion: protocolVersion,
		ExpectedServer:  c.currentServerID(),
	})
	if err != nil {
		return err
	}
	if resp.Error != nil {
		err := c.apiErrorFromRPC(resp.Error)
		c.handleAPIError(h.sessionID, err)
		return err
	}

	c.observeServer(resp.ServerId)
	c.mu.Lock()
	delete(c.handles, h.Token)
	c.mu.Unlock()

	h.fail(LockEvent{Type: EventUnlocked, Message: "lock unlocked", SessionID: h.sessionID, Token: h.Token, OccurredAt: time.Now()})
	return nil
}

func isRetriableUnlockError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if st, ok := gstatus.FromError(err); ok {
		switch st.Code() {
		case gcodes.Unavailable, gcodes.DeadlineExceeded, gcodes.ResourceExhausted:
			return true
		case gcodes.Unauthenticated, gcodes.PermissionDenied, gcodes.InvalidArgument, gcodes.NotFound:
			return false
		}
	}

	var ae *APIError
	if errors.As(err, &ae) {
		switch ae.Code {
		case "LOCK_TIMEOUT":
			return true
		case "SESSION_GONE", "SESSION_NOT_FOUND", "LOCK_NOT_FOUND", "IDEMPOTENCY_CONFLICT":
			return false
		}
		if ae.Status >= 500 {
			return true
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}

	var ue *url.Error
	if errors.As(err, &ue) {
		return true
	}
	return false
}

// Close 关闭客户端：
// 1. 标记 closed，阻止后续新加锁；
// 2. 取消 heartbeat/sweep 后台协程并等待退出；
// 3. 尝试通知服务端关闭 session。
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	sessionID := c.sessionID
	hbStop := c.heartbeatStop
	wStop := c.watchStop
	sweepStop := c.sweepStop
	bgDone := c.bgDone
	conn := c.conn
	c.sessionID = ""
	c.heartbeatStop = nil
	c.watchStop = nil
	c.sweepStop = nil
	affected := c.collectHandlesLocked(sessionID)
	c.mu.Unlock()

	for _, h := range affected {
		h.fail(LockEvent{
			Type:       EventSessionGone,
			Message:    "client closed",
			SessionID:  sessionID,
			Token:      h.Token,
			ErrorCode:  "CLIENT_CLOSE",
			OccurredAt: time.Now(),
		})
	}

	if hbStop != nil {
		hbStop()
	}
	if wStop != nil {
		wStop()
	}
	if sweepStop != nil {
		sweepStop()
	}
	if err := waitChannelWithContext(ctx, bgDone); err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}
	if err := waitWaitGroupWithContext(ctx, &c.sessionInitWG); err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}
	var closeErr error
	if sessionID != "" {
		closeErr = c.closeSessionRemoteManaged(ctx, sessionID)
	}
	if conn != nil {
		_ = conn.Close()
	}
	return closeErr
}

// ensureSession 确保客户端有可用 session。
// 并发场景下仅允许一个 goroutine 执行 session 初始化，其余等待通知。
func (c *Client) ensureSession(ctx context.Context) (string, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return "", errors.New("client is closed")
		}
		if c.sessionID != "" {
			id := c.sessionID
			c.mu.Unlock()
			return id, nil
		}
		if !c.sessionInitInFlight {
			c.sessionInitInFlight = true
			done := make(chan struct{})
			c.sessionInitDone = done
			c.sessionInitWG.Add(1)
			c.mu.Unlock()

			resp, err := c.createSession(ctx)

			c.mu.Lock()
			c.sessionInitInFlight = false
			if err != nil {
				// Wake waiters while still holding the state lock so they cannot
				// observe a cleared channel before the notification is published.
				close(done)
				if c.sessionInitDone == done {
					c.sessionInitDone = nil
				}
				c.mu.Unlock()
				c.sessionInitWG.Done()
				return "", err
			}
			if c.closed {
				// Keep the same ordering guarantees as the error path.
				close(done)
				if c.sessionInitDone == done {
					c.sessionInitDone = nil
				}
				c.mu.Unlock()
				closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = c.closeSessionRemoteManaged(closeCtx, resp.SessionID)
				cancel()
				c.sessionInitWG.Done()
				return "", errors.New("client is closed")
			}
			if c.sessionID == "" {
				c.sessionID = resp.SessionID
				if resp.ServerID != "" {
					c.serverID = resp.ServerID
				}
				hbCtx, cancel := context.WithCancel(context.Background())
				c.heartbeatStop = cancel
				c.markBGStartLocked()
				go c.heartbeatLoop(hbCtx, resp.SessionID)
				wCtx, wCancel := context.WithCancel(context.Background())
				c.watchStop = wCancel
				c.markBGStartLocked()
				go c.watchLoop(wCtx, resp.SessionID)
			}
			id := c.sessionID
			close(done)
			if c.sessionInitDone == done {
				c.sessionInitDone = nil
			}
			c.mu.Unlock()
			c.sessionInitWG.Done()
			return id, nil
		}
		done := c.sessionInitDone
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-done:
		}
	}
}

// invalidateSession 使当前 session 失效并停止 heartbeat。
func (c *Client) invalidateSession(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionID != sessionID {
		return
	}
	if c.heartbeatStop != nil {
		c.heartbeatStop()
	}
	if c.watchStop != nil {
		c.watchStop()
	}
	c.sessionID = ""
	c.heartbeatStop = nil
	c.watchStop = nil
}

// heartbeatLoop 定期续约会话。
// 连续失败达到阈值后触发 fail-closed。
func (c *Client) heartbeatLoop(ctx context.Context, sessionID string) {
	defer c.markBGDone()

	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hbCtx, cancel := context.WithTimeout(ctx, c.cfg.HeartbeatTimeout)
			err := c.heartbeat(hbCtx, sessionID)
			cancel()
			if err == nil {
				consecutiveFailures = 0
				c.renewAutoHandles(sessionID)
				continue
			}

			consecutiveFailures++
			c.handleAPIError(sessionID, err)

			var ae *APIError
			if errors.As(err, &ae) && (ae.Code == "SESSION_GONE" || ae.Code == "SESSION_NOT_FOUND") {
				return
			}

			if consecutiveFailures >= 2 {
				c.failSessionOnHeartbeat(sessionID, err)
				return
			}
		}
	}
}

func (c *Client) watchLoop(ctx context.Context, sessionID string) {
	defer c.markBGDone()
	if c.grpc == nil {
		return
	}

	backoff := defaultWatchReconnectInitial
	disconnectedSince := time.Time{}
	connectedAt := time.Time{}
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := c.grpc.WatchSession(c.rpcContext(ctx))
		if err != nil {
			if !c.retryWatchReconnect(ctx, sessionID, err, &backoff, &disconnectedSince) {
				return
			}
			continue
		}
		if err := stream.Send(&lockrpcpb.ClientWatchMessage{
			SessionId:       sessionID,
			ProtocolVersion: protocolVersion,
		}); err != nil {
			_ = stream.CloseSend()
			if !c.retryWatchReconnect(ctx, sessionID, err, &backoff, &disconnectedSince) {
				return
			}
			continue
		}
		for {
			ev, err := stream.Recv()
			if err != nil {
				_ = stream.CloseSend()
				if ctx.Err() != nil {
					return
				}
				if !connectedAt.IsZero() && time.Since(connectedAt) >= defaultWatchStableResetAfter {
					backoff = defaultWatchReconnectInitial
					disconnectedSince = time.Time{}
				}
				break
			}
			switch ev.Type {
			case "WATCH_ESTABLISHED":
				connectedAt = time.Now()
				if ev.ServerId != "" {
					c.observeServer(ev.ServerId)
				}
			case "SESSION_INVALIDATED":
				_ = stream.CloseSend()
				c.invalidateSession(sessionID)
				c.broadcastSessionEvent(sessionID, LockEvent{
					Type:       EventSessionGone,
					Message:    ev.Message,
					SessionID:  sessionID,
					ErrorCode:  ev.Code,
					OccurredAt: time.Now(),
				})
				return
			case "PROTOCOL_MISMATCH":
				_ = stream.CloseSend()
				c.broadcastEvent(LockEvent{
					Type:       EventProtocolMismatch,
					Message:    ev.Message,
					ErrorCode:  ev.Code,
					OccurredAt: time.Now(),
				})
				return
			}
		}

		connectedAt = time.Time{}
		if !c.retryWatchReconnect(ctx, sessionID, errors.New("watch stream disconnected"), &backoff, &disconnectedSince) {
			return
		}
	}
}

func (c *Client) retryWatchReconnect(ctx context.Context, sessionID string, cause error, backoff *time.Duration, disconnectedSince *time.Time) bool {
	if disconnectedSince != nil && disconnectedSince.IsZero() {
		*disconnectedSince = time.Now()
	}
	if err := sleepWithContext(ctx, *backoff); err != nil {
		return false
	}
	if time.Since(*disconnectedSince) >= defaultWatchReconnectFailAfter {
		c.failSessionOnWatch(sessionID, cause)
		return false
	}
	if *backoff < defaultWatchReconnectMax {
		*backoff *= 2
		if *backoff > defaultWatchReconnectMax {
			*backoff = defaultWatchReconnectMax
		}
	}
	return true
}

// renewAutoHandles 为自动续约句柄刷新本地 TTL。
func (c *Client) renewAutoHandles(sessionID string) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, h := range c.handles {
		if h.sessionID == sessionID && h.autoRenew {
			h.setExpireAt(now.Add(h.localTTL))
		}
	}
}

// localExpiryLoop 周期扫描本地句柄，超过 TTL 则触发 local_expired 事件。
func (c *Client) localExpiryLoop(ctx context.Context) {
	defer c.markBGDone()

	ticker := time.NewTicker(c.cfg.LocalSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			c.mu.Lock()
			expired := make([]*LockHandle, 0)
			for token, h := range c.handles {
				if h.expired(now) {
					expired = append(expired, h)
					delete(c.handles, token)
				}
			}
			c.mu.Unlock()
			for _, h := range expired {
				h.fail(LockEvent{
					Type:       EventLocalExpired,
					Message:    "local lock ttl expired",
					SessionID:  h.sessionID,
					Token:      h.Token,
					ErrorCode:  "LOCAL_TTL_EXPIRED",
					OccurredAt: now,
				})
			}
		}
	}
}

// failSessionOnHeartbeat 在 heartbeat 连续失败时触发本地失效。
func (c *Client) failSessionOnHeartbeat(sessionID string, cause error) {
	c.invalidateSession(sessionID)
	msg := "heartbeat failed twice; lock invalidated"
	if cause != nil {
		msg = msg + ": " + cause.Error()
	}
	c.broadcastSessionEvent(sessionID, LockEvent{
		Type:       EventSessionGone,
		Message:    msg,
		SessionID:  sessionID,
		ErrorCode:  "HEARTBEAT_CONSECUTIVE_FAILURE",
		OccurredAt: time.Now(),
	})
	c.closeSessionRemoteBestEffort(sessionID)
}

func (c *Client) failSessionOnWatch(sessionID string, cause error) {
	c.invalidateSession(sessionID)
	msg := "watch stream disconnected; lock invalidated"
	if cause != nil {
		msg = msg + ": " + cause.Error()
	}
	c.broadcastSessionEvent(sessionID, LockEvent{
		Type:       EventSessionGone,
		Message:    msg,
		SessionID:  sessionID,
		ErrorCode:  "WATCH_STREAM_DISCONNECTED",
		OccurredAt: time.Now(),
	})
	c.closeSessionRemoteBestEffort(sessionID)
}

// closeSessionRemote 通知服务端关闭指定 session。
func (c *Client) closeSessionRemote(ctx context.Context, sessionID string) error {
	if c.grpc == nil {
		return nil
	}
	resp, err := c.grpc.CloseSession(c.rpcContext(ctx), &lockrpcpb.CloseSessionRequest{
		SessionId:       sessionID,
		ProtocolVersion: protocolVersion,
		ExpectedServer:  c.currentServerID(),
	})
	if err != nil {
		return err
	}
	c.observeServer(resp.ServerId)
	if resp.Error == nil || resp.Error.Code == "SESSION_NOT_FOUND" || resp.Error.Code == "SESSION_GONE" {
		return nil
	}
	err = c.apiErrorFromRPC(resp.Error)
	c.handleAPIError(sessionID, err)
	return err
}

// createSession 调用服务端创建会话接口。
func (c *Client) createSession(ctx context.Context) (sessionResponse, error) {
	lease := c.effectiveServerLease()
	resp, err := c.grpc.CreateSession(c.rpcContext(ctx), &lockrpcpb.CreateSessionRequest{
		LeaseMs:         durationMS(lease),
		ProtocolVersion: protocolVersion,
		ExpectedServer:  c.currentServerID(),
	})
	if err != nil {
		return sessionResponse{}, err
	}
	if resp.Error != nil {
		err := c.apiErrorFromRPC(resp.Error)
		c.handleAPIError("", err)
		return sessionResponse{}, err
	}

	out := sessionResponse{
		SessionID:       resp.SessionId,
		LeaseMS:         resp.LeaseMs,
		ExpiresAt:       resp.ExpiresAt,
		ServerID:        resp.ServerId,
		ProtocolVersion: resp.ProtocolVersion,
	}
	if out.SessionID == "" {
		return sessionResponse{}, errors.New("lock service returned empty session id")
	}
	if out.ServerID != "" {
		c.observeServer(out.ServerID)
	}
	if out.ProtocolVersion != "" && out.ProtocolVersion != protocolVersion {
		c.broadcastEvent(LockEvent{Type: EventProtocolMismatch, Message: "protocol mismatch", ErrorCode: "PROTOCOL_MISMATCH", OccurredAt: time.Now()})
	}
	return out, nil
}

// effectiveServerLease 计算应请求的服务端 lease。
func (c *Client) effectiveServerLease() time.Duration {
	if c.cfg.SessionLease > 0 {
		return c.cfg.SessionLease
	}
	lease := c.cfg.LocalTTL + c.cfg.ServerLeaseBuffer
	if lease <= c.cfg.LocalTTL {
		lease = c.cfg.LocalTTL + time.Second
	}
	return lease
}

// heartbeat 发送一次会话续约请求。
func (c *Client) heartbeat(ctx context.Context, sessionID string) error {
	resp, err := c.grpc.Heartbeat(c.rpcContext(ctx), &lockrpcpb.HeartbeatRequest{
		SessionId:       sessionID,
		ProtocolVersion: protocolVersion,
		ExpectedServer:  c.currentServerID(),
	})
	if err != nil {
		return err
	}
	c.observeServer(resp.ServerId)
	if resp.Error == nil {
		return nil
	}
	return c.apiErrorFromRPC(resp.Error)
}

// acquire 发起加锁 RPC 请求。
func (c *Client) acquire(ctx context.Context, reqBody lockRequest) (lockResponse, error) {
	resp, err := c.grpc.Acquire(c.rpcContext(ctx), &lockrpcpb.LockRequest{
		Scope:           string(reqBody.Scope),
		P1:              reqBody.P1,
		P2:              reqBody.P2,
		TimeoutMs:       reqBody.TimeoutMS,
		SessionId:       reqBody.SessionID,
		RequestId:       reqBody.RequestID,
		ProtocolVersion: protocolVersion,
		ExpectedServer:  c.currentServerID(),
	})
	if err != nil {
		return lockResponse{}, err
	}
	c.observeServer(resp.ServerId)
	if resp.Error != nil {
		return lockResponse{}, c.apiErrorFromRPC(resp.Error)
	}
	if resp.Token == "" {
		return lockResponse{}, errors.New("lock service returned empty token")
	}
	return lockResponse{
		Token:           resp.Token,
		Fence:           resp.Fence,
		SessionID:       resp.SessionId,
		ServerID:        resp.ServerId,
		ProtocolVersion: resp.ProtocolVersion,
	}, nil
}

// observeServer 观察服务实例 ID 变化并执行本地失效处理。
func (c *Client) observeServer(serverID string) {
	if serverID == "" {
		return
	}
	c.mu.Lock()
	old := c.serverID
	if old == "" {
		c.serverID = serverID
		c.mu.Unlock()
		return
	}
	if old == serverID {
		c.mu.Unlock()
		return
	}
	sessionID := c.sessionID
	if c.heartbeatStop != nil {
		c.heartbeatStop()
	}
	if c.watchStop != nil {
		c.watchStop()
	}
	c.sessionID = ""
	c.heartbeatStop = nil
	c.watchStop = nil
	c.serverID = serverID
	affected := c.collectHandlesLocked(sessionID)
	c.mu.Unlock()

	ev := LockEvent{Type: EventServerChanged, Message: "server instance changed; local session invalidated", OldServer: old, NewServer: serverID, SessionID: sessionID, ErrorCode: "SERVER_INSTANCE_MISMATCH", OccurredAt: time.Now()}
	for _, h := range affected {
		h.fail(ev)
	}
}

// handleAPIError 根据业务错误码执行本地状态机动作。
func (c *Client) handleAPIError(sessionID string, err error) {
	var ae *APIError
	if !errors.As(err, &ae) {
		return
	}
	switch ae.Code {
	case "SESSION_GONE", "SESSION_NOT_FOUND":
		c.invalidateSession(sessionID)
		c.broadcastSessionEvent(sessionID, LockEvent{Type: EventSessionGone, Message: ae.Message, SessionID: sessionID, ErrorCode: ae.Code, OccurredAt: time.Now()})
	case "SERVER_INSTANCE_MISMATCH":
		c.observeServer(ae.ServerID)
	case "PROTOCOL_MISMATCH":
		c.broadcastEvent(LockEvent{Type: EventProtocolMismatch, Message: ae.Message, ErrorCode: ae.Code, NewServer: ae.ServerID, OccurredAt: time.Now()})
	}
}

// broadcastSessionEvent 向指定 session 下全部句柄广播事件并移除句柄。
func (c *Client) broadcastSessionEvent(sessionID string, ev LockEvent) {
	c.mu.Lock()
	affected := c.collectHandlesLocked(sessionID)
	c.mu.Unlock()
	for _, h := range affected {
		ev.Token = h.Token
		h.fail(ev)
	}
}

// broadcastEvent 向全部句柄广播事件并移除句柄。
func (c *Client) broadcastEvent(ev LockEvent) {
	c.mu.Lock()
	affected := make([]*LockHandle, 0, len(c.handles))
	for token, h := range c.handles {
		affected = append(affected, h)
		delete(c.handles, token)
	}
	c.mu.Unlock()
	for _, h := range affected {
		ev.Token = h.Token
		h.fail(ev)
	}
}

// collectHandlesLocked 在持锁状态下收集并移除受影响句柄。
func (c *Client) collectHandlesLocked(sessionID string) []*LockHandle {
	affected := make([]*LockHandle, 0)
	for token, h := range c.handles {
		if sessionID == "" || h.sessionID == sessionID {
			affected = append(affected, h)
			delete(c.handles, token)
		}
	}
	return affected
}

// nextRequestID 生成客户端请求 ID。
func (c *Client) nextRequestID(prefix string) string {
	n := atomic.AddUint64(&c.nextReq, 1)
	return prefix + "_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_" + strconv.FormatUint(n, 10)
}

func (c *Client) currentServerID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.serverID
}

func (c *Client) rpcContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(c.cfg.AuthToken) == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+strings.TrimSpace(c.cfg.AuthToken))
}

func (c *Client) apiErrorFromRPC(es *lockrpcpb.ErrorStatus) error {
	if es == nil {
		return nil
	}
	return &APIError{
		Status:          int(es.Status),
		Code:            es.Code,
		Message:         es.Message,
		ServerID:        es.ServerId,
		ProtocolVersion: es.ProtocolVersion,
	}
}

// durationMS 将 duration 转为毫秒数；<=0 返回 0。
func durationMS(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}

// waitChannelWithContext 在 context 约束下等待 done 信号。
func waitChannelWithContext(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitWaitGroupWithContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return waitChannelWithContext(ctx, done)
}

func (c *Client) closeSessionRemoteBestEffort(sessionID string) {
	if sessionID == "" {
		return
	}
	done, started := c.beginManagedRemoteClose(sessionID)
	if !started {
		_ = done
		return
	}
	go func() {
		defer c.finishManagedRemoteClose(sessionID, done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.closeSessionRemote(ctx, sessionID)
	}()
}

func (c *Client) closeSessionRemoteManaged(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return nil
	}
	done, started := c.beginManagedRemoteClose(sessionID)
	if !started {
		return waitChannelWithContext(ctx, done)
	}
	defer c.finishManagedRemoteClose(sessionID, done)
	return c.closeSessionRemote(ctx, sessionID)
}

func (c *Client) beginManagedRemoteClose(sessionID string) (chan struct{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if done, ok := c.remoteCloseInFlight[sessionID]; ok {
		return done, false
	}
	done := make(chan struct{})
	c.remoteCloseInFlight[sessionID] = done
	return done, true
}

func (c *Client) finishManagedRemoteClose(sessionID string, done chan struct{}) {
	c.mu.Lock()
	current, ok := c.remoteCloseInFlight[sessionID]
	if ok && current == done {
		delete(c.remoteCloseInFlight, sessionID)
	}
	c.mu.Unlock()
	if ok && current == done {
		close(done)
	}
}

func (c *Client) markBGStart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markBGStartLocked()
}

func (c *Client) markBGStartLocked() {
	if c.bgActive == 0 {
		c.bgDone = make(chan struct{})
	}
	c.bgActive++
}

func (c *Client) markBGDone() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bgActive--
	if c.bgActive == 0 {
		close(c.bgDone)
	}
}
