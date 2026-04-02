package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Scope 表示锁粒度。
type Scope string

const (
	// ScopeL1 表示一级键锁。
	ScopeL1 Scope = "l1"
	// ScopeL2 表示二级键锁。
	ScopeL2 Scope = "l2"

	protocolVersion = "1"
	headerServerID  = "X-Lock-Server-ID"
	headerProtocol  = "X-Lock-Protocol-Version"
)

const (
	defaultHeartbeatTimeout   = 2 * time.Second
	defaultServerLeaseBuffer  = 3 * time.Second
	defaultLocalTTLMultiplier = 2
	defaultSweepInterval      = 200 * time.Millisecond
)

type LockOption struct {
	Timeout time.Duration
	// LocalTTL sets a fixed local validity duration for this lock handle.
	// If zero, the lock uses client default local TTL and auto-renews on each heartbeat success.
	LocalTTL time.Duration
}

type EventType string

const (
	// EventServerChanged 表示服务实例切换导致本地会话失效。
	EventServerChanged    EventType = "server_changed"
	// EventProtocolMismatch 表示协议版本不一致。
	EventProtocolMismatch EventType = "protocol_mismatch"
	// EventSessionGone 表示会话失效（过期/不存在/主动关闭/心跳连续失败）。
	EventSessionGone      EventType = "session_gone"
	// EventUnlocked 表示主动解锁成功。
	EventUnlocked         EventType = "unlocked"
	// EventLocalExpired 表示本地 TTL 到期，锁在客户端侧失效。
	EventLocalExpired     EventType = "local_expired"
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
	// SessionLease, when set, is used directly for server-side lease.
	// When zero, server lease is computed as LocalTTL + ServerLeaseBuffer.
	SessionLease time.Duration
	// HeartbeatInterval controls heartbeat period.
	HeartbeatInterval time.Duration
	// HeartbeatTimeout controls per-heartbeat request timeout.
	HeartbeatTimeout time.Duration
	// ServerLeaseBuffer is fixed safety buffer added to local TTL when SessionLease is not set.
	ServerLeaseBuffer time.Duration
	// LocalTTL is client-side lock validity window.
	// If zero, defaults to 2 * HeartbeatInterval.
	LocalTTL time.Duration
	// LocalSweepInterval controls local TTL expiration scan frequency.
	LocalSweepInterval time.Duration
}

// Client 是锁服务 Go SDK 主体。
//
// 生命周期约束：
// 1. NewWithConfig 创建后会启动本地 TTL 扫描协程。
// 2. 使用结束必须调用 Close 释放后台资源。
// 3. Close 后实例不可复用。
type Client struct {
	baseURL string
	http    *http.Client
	cfg     Config

	mu                  sync.Mutex
	sessionID           string
	serverID            string
	heartbeatStop       context.CancelFunc
	sweepStop           context.CancelFunc
	sessionInitInFlight bool
	sessionInitDone     chan struct{}
	heartbeatWG         sync.WaitGroup
	sweepWG             sync.WaitGroup
	closed              bool
	nextReq             uint64
	handles             map[string]*LockHandle
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

// createSessionRequest 是创建会话请求体。
type createSessionRequest struct {
	LeaseMS int64 `json:"lease_ms,omitempty"`
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

// errorResponse 是服务端错误响应体。
type errorResponse struct {
	Error           string `json:"error"`
	Code            string `json:"code,omitempty"`
	ServerID        string `json:"server_id,omitempty"`
	ProtocolVersion string `json:"protocol_version,omitempty"`
}

// APIError 封装服务端 HTTP 错误与业务错误码。
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
func New(baseURL string, httpClient *http.Client) *Client {
	return NewWithConfig(baseURL, httpClient, Config{})
}

// NewWithConfig 使用给定配置创建客户端并启动本地 TTL 扫描协程。
func NewWithConfig(baseURL string, httpClient *http.Client, cfg Config) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
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

	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    httpClient,
		cfg:     cfg,
		handles: make(map[string]*LockHandle),
	}

	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	c.sweepStop = sweepCancel
	c.sweepWG.Add(1)
	go c.localExpiryLoop(sweepCtx)
	return c
}

// Lock 获取二级锁。
func (c *Client) Lock(ctx context.Context, p1, p2 string, opt LockOption) (*LockHandle, error) {
	sessionID, err := c.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	acquireReqID := c.nextRequestID("acq")
	resp, err := c.acquire(ctx, lockRequest{
		Scope:     ScopeL2,
		P1:        p1,
		P2:        p2,
		TimeoutMS: durationMS(opt.Timeout),
		SessionID: sessionID,
		RequestID: acquireReqID,
	})
	if err != nil {
		c.handleAPIError(sessionID, err)
		return nil, err
	}

	c.observeServer(resp.ServerID)
	h := c.newHandle(resp, opt)

	c.mu.Lock()
	c.handles[h.Token] = h
	c.mu.Unlock()
	return h, nil
}

// LockL1 获取一级锁。
func (c *Client) LockL1(ctx context.Context, p1 string, opt LockOption) (*LockHandle, error) {
	sessionID, err := c.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	acquireReqID := c.nextRequestID("acq")
	resp, err := c.acquire(ctx, lockRequest{
		Scope:     ScopeL1,
		P1:        p1,
		TimeoutMS: durationMS(opt.Timeout),
		SessionID: sessionID,
		RequestID: acquireReqID,
	})
	if err != nil {
		c.handleAPIError(sessionID, err)
		return nil, err
	}

	c.observeServer(resp.ServerID)
	h := c.newHandle(resp, opt)

	c.mu.Lock()
	c.handles[h.Token] = h
	c.mu.Unlock()
	return h, nil
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
	q := url.Values{}
	q.Set("session_id", h.sessionID)
	q.Set("request_id", releaseReqID)

	u := c.baseURL + "/v1/locks/" + h.Token + "?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	c.decorateRequest(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	c.observeServer(resp.Header.Get(headerServerID))
	if resp.StatusCode != http.StatusNoContent {
		err = decodeHTTPError(resp)
		c.handleAPIError(h.sessionID, err)
		return err
	}

	c.mu.Lock()
	delete(c.handles, h.Token)
	c.mu.Unlock()

	h.fail(LockEvent{Type: EventUnlocked, Message: "lock unlocked", SessionID: h.sessionID, Token: h.Token, OccurredAt: time.Now()})
	return nil
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
	sweepStop := c.sweepStop
	c.sessionID = ""
	c.heartbeatStop = nil
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
	if sweepStop != nil {
		sweepStop()
	}
	if err := waitGroupWithContext(ctx, &c.heartbeatWG); err != nil {
		return err
	}
	if err := waitGroupWithContext(ctx, &c.sweepWG); err != nil {
		return err
	}
	if sessionID == "" {
		return nil
	}

	return c.closeSessionRemote(ctx, sessionID)
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
			c.mu.Unlock()

			resp, err := c.createSession(ctx)

			c.mu.Lock()
			c.sessionInitInFlight = false
			close(done)
			c.sessionInitDone = nil
			if err != nil {
				c.mu.Unlock()
				return "", err
			}
			if c.sessionID == "" {
				c.sessionID = resp.SessionID
				if resp.ServerID != "" {
					c.serverID = resp.ServerID
				}
				hbCtx, cancel := context.WithCancel(context.Background())
				c.heartbeatStop = cancel
				c.heartbeatWG.Add(1)
				go c.heartbeatLoop(hbCtx, resp.SessionID)
			}
			id := c.sessionID
			c.mu.Unlock()
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
	c.sessionID = ""
	c.heartbeatStop = nil
}

// heartbeatLoop 定期续约会话。
// 连续失败达到阈值后触发 fail-closed。
func (c *Client) heartbeatLoop(ctx context.Context, sessionID string) {
	defer c.heartbeatWG.Done()

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
	defer c.sweepWG.Done()

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

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.closeSessionRemote(ctx, sessionID)
	}()
}

// closeSessionRemote 通知服务端关闭指定 session。
func (c *Client) closeSessionRemote(ctx context.Context, sessionID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/v1/sessions/"+sessionID, nil)
	if err != nil {
		return err
	}
	c.decorateRequest(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c.observeServer(resp.Header.Get(headerServerID))

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return nil
	}
	err = decodeHTTPError(resp)
	c.handleAPIError(sessionID, err)
	return err
}

// createSession 调用服务端创建会话接口。
func (c *Client) createSession(ctx context.Context) (sessionResponse, error) {
	lease := c.effectiveServerLease()
	body, err := json.Marshal(createSessionRequest{LeaseMS: durationMS(lease)})
	if err != nil {
		return sessionResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/sessions", bytes.NewReader(body))
	if err != nil {
		return sessionResponse{}, err
	}
	c.decorateRequest(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return sessionResponse{}, err
	}
	defer resp.Body.Close()

	c.observeServer(resp.Header.Get(headerServerID))

	if resp.StatusCode != http.StatusOK {
		err := decodeHTTPError(resp)
		c.handleAPIError("", err)
		return sessionResponse{}, err
	}

	var out sessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return sessionResponse{}, err
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/sessions/"+sessionID+"/heartbeat", nil)
	if err != nil {
		return err
	}
	c.decorateRequest(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c.observeServer(resp.Header.Get(headerServerID))
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return decodeHTTPError(resp)
}

// acquire 发起加锁 HTTP 请求。
func (c *Client) acquire(ctx context.Context, reqBody lockRequest) (lockResponse, error) {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return lockResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/locks", bytes.NewReader(body))
	if err != nil {
		return lockResponse{}, err
	}
	c.decorateRequest(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return lockResponse{}, err
	}
	defer resp.Body.Close()

	c.observeServer(resp.Header.Get(headerServerID))

	if resp.StatusCode != http.StatusOK {
		return lockResponse{}, decodeHTTPError(resp)
	}

	var out lockResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return lockResponse{}, err
	}
	if out.Token == "" {
		return lockResponse{}, errors.New("lock service returned empty token")
	}
	return out, nil
}

// decorateRequest 注入协议头与期望 server id。
func (c *Client) decorateRequest(req *http.Request) {
	req.Header.Set(headerProtocol, protocolVersion)
	c.mu.Lock()
	serverID := c.serverID
	c.mu.Unlock()
	if serverID != "" {
		req.Header.Set(headerServerID, serverID)
	}
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
	c.sessionID = ""
	c.heartbeatStop = nil
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

// decodeHTTPError 解析服务端错误响应体并转换为 APIError。
func decodeHTTPError(resp *http.Response) error {
	data, _ := io.ReadAll(resp.Body)
	if len(data) == 0 {
		return &APIError{Status: resp.StatusCode, Message: "empty error response", ServerID: resp.Header.Get(headerServerID), ProtocolVersion: resp.Header.Get(headerProtocol)}
	}
	var er errorResponse
	if err := json.Unmarshal(data, &er); err == nil {
		msg := er.Error
		if msg == "" {
			msg = strings.TrimSpace(string(data))
		}
		sid := er.ServerID
		if sid == "" {
			sid = resp.Header.Get(headerServerID)
		}
		pv := er.ProtocolVersion
		if pv == "" {
			pv = resp.Header.Get(headerProtocol)
		}
		return &APIError{Status: resp.StatusCode, Code: er.Code, Message: msg, ServerID: sid, ProtocolVersion: pv}
	}
	return &APIError{Status: resp.StatusCode, Message: strings.TrimSpace(string(data)), ServerID: resp.Header.Get(headerServerID), ProtocolVersion: resp.Header.Get(headerProtocol)}
}

// durationMS 将 duration 转为毫秒数；<=0 返回 0。
func durationMS(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}

// waitGroupWithContext 在 context 约束下等待 WaitGroup 完成。
func waitGroupWithContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
