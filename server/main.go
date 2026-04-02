package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puper/klock/pkg/hierlock"
)

// lockScope 表示锁粒度：L1（前缀级）或 L2（资源级）。
type lockScope string

const (
	// scopeL1 表示锁定一级键。
	scopeL1 lockScope = "l1"
	// scopeL2 表示锁定二级键。
	scopeL2 lockScope = "l2"

	protocolVersion      = "1"
	headerServerID       = "X-Lock-Server-ID"
	headerProtocol       = "X-Lock-Protocol-Version"
	errorCodeSessionGone = "SESSION_GONE"

	maxJSONBodyBytes      = 1 << 20
	maxIdempotencyEntries = 4096
	expiredRetention      = 10 * time.Minute
	maxExpiredSessions    = 10000
)

// createSessionRequest 是创建会话的请求体。
type createSessionRequest struct {
	LeaseMS int64 `json:"lease_ms,omitempty"`
}

// sessionResponse 是会话接口返回体。
type sessionResponse struct {
	SessionID       string `json:"session_id"`
	LeaseMS         int64  `json:"lease_ms"`
	ExpiresAt       string `json:"expires_at"`
	ServerID        string `json:"server_id"`
	ProtocolVersion string `json:"protocol_version"`
}

// lockRequest 是加锁请求体。
type lockRequest struct {
	Scope     lockScope `json:"scope"`
	P1        string    `json:"p1"`
	P2        string    `json:"p2,omitempty"`
	TimeoutMS int64     `json:"timeout_ms,omitempty"`
	SessionID string    `json:"session_id"`
	RequestID string    `json:"request_id"`
}

// lockResponse 是加锁成功返回体。
type lockResponse struct {
	Token           string `json:"token"`
	Fence           uint64 `json:"fence"`
	Scope           string `json:"scope"`
	P1              string `json:"p1"`
	P2              string `json:"p2,omitempty"`
	SessionID       string `json:"session_id"`
	ServerID        string `json:"server_id"`
	ProtocolVersion string `json:"protocol_version"`
}

// errorResponse 是统一错误返回结构。
type errorResponse struct {
	Error           string `json:"error"`
	Code            string `json:"code,omitempty"`
	ServerID        string `json:"server_id"`
	ProtocolVersion string `json:"protocol_version"`
}

// apiError 用于服务内部的结构化错误表达。
type apiError struct {
	status int
	code   string
	msg    string
}

func (e *apiError) Error() string { return e.msg }

// lockHandle 保存 token 对应的解锁函数。
// once 用于保证多路径释放时只执行一次 unlock。
type lockHandle struct {
	token  string
	unlock func()
	once   sync.Once
}

// expiredInfo 记录会话过期原因及过期时间。
type expiredInfo struct {
	reason string
	at     time.Time
}

// sessionState 是单个 session 的内存状态。
//
// 关键说明：
// 1. timer + timerSeq 用于租约超时；timerSeq 防止旧回调误操作新租约。
// 2. acquire/release 的 request_id 缓存用于幂等保障。
// 3. *_Order 用于最旧淘汰，限制幂等缓存大小。
type sessionState struct {
	id string

	lease     time.Duration
	expiresAt time.Time
	timer     *time.Timer
	timerSeq  uint64

	acquireByRequest map[string]lockResponse
	acquireOrder     []string
	releaseByRequest map[string]string
	releaseOrder     []string
	locksByToken     map[string]*lockHandle
}

// lockService 是 HTTP 接口背后的核心状态机。
type lockService struct {
	locker       *hierlock.HierarchicalLocker
	defaultLease time.Duration
	maxLease     time.Duration
	serverID     string

	mu              sync.Mutex
	nextSess        uint64
	nextLock        uint64
	nextFence       uint64
	sessions        map[string]*sessionState
	tokens          map[string]*lockHandle
	expiredSessions map[string]expiredInfo

	maxIdempotencyEntries int
}

// newLockService 构造服务实例。
func newLockService(locker *hierlock.HierarchicalLocker, defaultLease, maxLease time.Duration, serverID string) *lockService {
	return &lockService{
		locker:          locker,
		defaultLease:    defaultLease,
		maxLease:        maxLease,
		serverID:        serverID,
		sessions:        make(map[string]*sessionState),
		tokens:          make(map[string]*lockHandle),
		expiredSessions: make(map[string]expiredInfo),
		maxIdempotencyEntries: maxIdempotencyEntries,
	}
}

// createSession 创建会话并初始化 lease 定时器。
func (s *lockService) createSession(req createSessionRequest) (sessionResponse, error) {
	lease := s.defaultLease
	if req.LeaseMS > 0 {
		lease = time.Duration(req.LeaseMS) * time.Millisecond
	}
	if lease <= 0 {
		return sessionResponse{}, errors.New("lease must be > 0")
	}
	if s.maxLease > 0 && lease > s.maxLease {
		return sessionResponse{}, fmt.Errorf("lease exceeds max lease (%s)", s.maxLease)
	}

	sessionID := s.nextSessionID()
	now := time.Now()
	st := &sessionState{
		id:               sessionID,
		lease:            lease,
		expiresAt:        now.Add(lease),
		acquireByRequest: make(map[string]lockResponse),
		acquireOrder:     make([]string, 0, 64),
		releaseByRequest: make(map[string]string),
		releaseOrder:     make([]string, 0, 64),
		locksByToken:     make(map[string]*lockHandle),
	}

	s.mu.Lock()
	s.sessions[sessionID] = st
	// Install timer only after session is visible in map to avoid missing expiry
	// when very short leases fire before registration.
	s.armSessionTimerLocked(st)
	s.pruneExpiredLocked(now)
	s.mu.Unlock()

	return sessionResponse{
		SessionID:       sessionID,
		LeaseMS:         lease.Milliseconds(),
		ExpiresAt:       st.expiresAt.UTC().Format(time.RFC3339Nano),
		ServerID:        s.serverID,
		ProtocolVersion: protocolVersion,
	}, nil
}

// heartbeat 为指定 session 续约。
func (s *lockService) heartbeat(sessionID string) (sessionResponse, error) {
	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if !ok {
		reason := s.expiredReasonLocked(sessionID)
		s.mu.Unlock()
		if reason != "" {
			return sessionResponse{}, &apiError{status: http.StatusGone, code: errorCodeSessionGone, msg: "session expired: " + reason}
		}
		return sessionResponse{}, &apiError{status: http.StatusNotFound, code: "SESSION_NOT_FOUND", msg: "session not found"}
	}
	st.expiresAt = time.Now().Add(st.lease)
	s.armSessionTimerLocked(st)
	resp := sessionResponse{
		SessionID:       st.id,
		LeaseMS:         st.lease.Milliseconds(),
		ExpiresAt:       st.expiresAt.UTC().Format(time.RFC3339Nano),
		ServerID:        s.serverID,
		ProtocolVersion: protocolVersion,
	}
	s.mu.Unlock()
	return resp, nil
}

// closeSession 执行客户端主动关闭会话流程。
func (s *lockService) closeSession(sessionID string) bool {
	return s.expireSession(sessionID, "client_close")
}

// acquire 执行完整加锁流程：校验 -> 幂等 -> 真正加锁 -> 状态登记。
func (s *lockService) acquire(ctx context.Context, req lockRequest) (lockResponse, error) {
	if req.SessionID == "" {
		return lockResponse{}, errors.New("session_id is required")
	}
	if req.RequestID == "" {
		return lockResponse{}, errors.New("request_id is required")
	}
	if req.P1 == "" {
		return lockResponse{}, errors.New("p1 is required")
	}

	s.mu.Lock()
	st, ok := s.sessions[req.SessionID]
	if !ok {
		reason := s.expiredReasonLocked(req.SessionID)
		s.mu.Unlock()
		if reason != "" {
			return lockResponse{}, &apiError{status: http.StatusGone, code: errorCodeSessionGone, msg: "session expired: " + reason}
		}
		return lockResponse{}, &apiError{status: http.StatusNotFound, code: "SESSION_NOT_FOUND", msg: "session not found"}
	}
	if cached, ok := st.acquireByRequest[req.RequestID]; ok {
		s.mu.Unlock()
		return cached, nil
	}
	expiresAt := st.expiresAt
	s.mu.Unlock()

	if req.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}
	if dl := time.Until(expiresAt); dl > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dl)
		defer cancel()
	}

	var (
		unlockFn func()
		err      error
	)
	switch req.Scope {
	case scopeL1:
		unlockFn, err = s.locker.LockL1(ctx, req.P1)
	case scopeL2:
		if req.P2 == "" {
			return lockResponse{}, errors.New("p2 is required for scope l2")
		}
		unlockFn, err = s.locker.Lock(ctx, req.P1, req.P2)
	default:
		return lockResponse{}, errors.New("scope must be one of: l1, l2")
	}
	if err != nil {
		return lockResponse{}, err
	}

	h := &lockHandle{token: s.nextLockToken(), unlock: unlockFn}
	resp := lockResponse{
		Token:           h.token,
		Fence:           atomic.AddUint64(&s.nextFence, 1),
		Scope:           string(req.Scope),
		P1:              req.P1,
		P2:              req.P2,
		SessionID:       req.SessionID,
		ServerID:        s.serverID,
		ProtocolVersion: protocolVersion,
	}

	s.mu.Lock()
	st, ok = s.sessions[req.SessionID]
	if !ok {
		reason := s.expiredReasonLocked(req.SessionID)
		s.mu.Unlock()
		h.safeUnlock()
		if reason != "" {
			return lockResponse{}, &apiError{status: http.StatusGone, code: errorCodeSessionGone, msg: "session expired: " + reason}
		}
		return lockResponse{}, &apiError{status: http.StatusNotFound, code: "SESSION_NOT_FOUND", msg: "session not found"}
	}
	if cached, ok := st.acquireByRequest[req.RequestID]; ok {
		s.mu.Unlock()
		h.safeUnlock()
		return cached, nil
	}
	s.putAcquireIdempotentLocked(st, req.RequestID, resp)
	st.locksByToken[h.token] = h
	s.tokens[h.token] = h
	s.mu.Unlock()

	return resp, nil
}

// release 释放 token 对应锁，支持 request_id 幂等语义。
func (s *lockService) release(sessionID, token, requestID string) (bool, error) {
	if sessionID == "" {
		return false, errors.New("session_id is required")
	}
	if requestID == "" {
		return false, errors.New("request_id is required")
	}

	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if !ok {
		reason := s.expiredReasonLocked(sessionID)
		s.mu.Unlock()
		if reason != "" {
			return false, &apiError{status: http.StatusGone, code: errorCodeSessionGone, msg: "session expired: " + reason}
		}
		return false, &apiError{status: http.StatusNotFound, code: "SESSION_NOT_FOUND", msg: "session not found"}
	}
	if prevToken, ok := st.releaseByRequest[requestID]; ok {
		s.mu.Unlock()
		if prevToken == token {
			return true, nil
		}
		return false, &apiError{status: http.StatusConflict, code: "IDEMPOTENCY_CONFLICT", msg: "request_id already used for a different token"}
	}
	h, ok := st.locksByToken[token]
	if !ok {
		s.mu.Unlock()
		return false, &apiError{status: http.StatusNotFound, code: "LOCK_NOT_FOUND", msg: "lock token not found"}
	}
	delete(st.locksByToken, token)
	delete(s.tokens, token)
	s.putReleaseIdempotentLocked(st, requestID, token)
	s.mu.Unlock()

	h.safeUnlock()
	return true, nil
}

// expireSession 使 session 失效并回收该 session 下的全部锁。
func (s *lockService) expireSession(sessionID, reason string) bool {
	var handles []*lockHandle

	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.sessions, sessionID)
	s.expiredSessions[sessionID] = expiredInfo{reason: reason, at: time.Now()}
	s.pruneExpiredLocked(time.Now())
	if st.timer != nil {
		st.timer.Stop()
	}
	for token, h := range st.locksByToken {
		handles = append(handles, h)
		delete(s.tokens, token)
	}
	s.mu.Unlock()

	for _, h := range handles {
		h.safeUnlock()
	}
	return true
}

// expiredReasonLocked 查询会话是否在“最近过期记录”中。
func (s *lockService) expiredReasonLocked(sessionID string) string {
	info, ok := s.expiredSessions[sessionID]
	if !ok {
		return ""
	}
	if time.Since(info.at) > expiredRetention {
		delete(s.expiredSessions, sessionID)
		return ""
	}
	return info.reason
}

// pruneExpiredLocked 清理过期历史，避免 expiredSessions 无界增长。
func (s *lockService) pruneExpiredLocked(now time.Time) {
	for id, info := range s.expiredSessions {
		if now.Sub(info.at) > expiredRetention {
			delete(s.expiredSessions, id)
		}
	}
	for len(s.expiredSessions) > maxExpiredSessions {
		for id := range s.expiredSessions {
			delete(s.expiredSessions, id)
			break
		}
	}
}

// safeUnlock 保证 unlock 只执行一次。
func (h *lockHandle) safeUnlock() {
	h.once.Do(func() { h.unlock() })
}

// armSessionTimerLocked 布置（或重置）session 超时计时器。
func (s *lockService) armSessionTimerLocked(st *sessionState) {
	st.timerSeq++
	seq := st.timerSeq
	sessionID := st.id
	d := time.Until(st.expiresAt)
	if d < 0 {
		d = 0
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	st.timer = time.AfterFunc(d, func() {
		s.onSessionTimer(sessionID, seq)
	})
}

// onSessionTimer 是 session 定时器回调入口。
// seq 用于确认当前回调是否仍属于最新 lease 代次。
func (s *lockService) onSessionTimer(sessionID string, seq uint64) {
	s.mu.Lock()
	st, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return
	}
	if st.timerSeq != seq {
		s.mu.Unlock()
		return
	}
	// If expiresAt moved forward due to heartbeat, re-arm and do not expire now.
	if time.Now().Before(st.expiresAt) {
		s.armSessionTimerLocked(st)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.expireSession(sessionID, "lease_expired")
}

// putAcquireIdempotentLocked 写入 acquire 幂等缓存并执行有界淘汰。
func (s *lockService) putAcquireIdempotentLocked(st *sessionState, requestID string, resp lockResponse) {
	if _, exists := st.acquireByRequest[requestID]; exists {
		return
	}
	maxEntries := s.maxIdempotencyEntries
	if maxEntries <= 0 {
		maxEntries = maxIdempotencyEntries
	}
	if len(st.acquireByRequest) >= maxEntries {
		s.evictOldestAcquireLocked(st)
	}
	st.acquireByRequest[requestID] = resp
	st.acquireOrder = append(st.acquireOrder, requestID)
}

// putReleaseIdempotentLocked 写入 release 幂等缓存并执行有界淘汰。
func (s *lockService) putReleaseIdempotentLocked(st *sessionState, requestID, token string) {
	if _, exists := st.releaseByRequest[requestID]; exists {
		return
	}
	maxEntries := s.maxIdempotencyEntries
	if maxEntries <= 0 {
		maxEntries = maxIdempotencyEntries
	}
	if len(st.releaseByRequest) >= maxEntries {
		s.evictOldestReleaseLocked(st)
	}
	st.releaseByRequest[requestID] = token
	st.releaseOrder = append(st.releaseOrder, requestID)
}

// evictOldestAcquireLocked 淘汰最旧 acquire request_id。
func (s *lockService) evictOldestAcquireLocked(st *sessionState) {
	for len(st.acquireOrder) > 0 {
		oldest := st.acquireOrder[0]
		st.acquireOrder = st.acquireOrder[1:]
		if _, ok := st.acquireByRequest[oldest]; ok {
			delete(st.acquireByRequest, oldest)
			return
		}
	}
}

// evictOldestReleaseLocked 淘汰最旧 release request_id。
func (s *lockService) evictOldestReleaseLocked(st *sessionState) {
	for len(st.releaseOrder) > 0 {
		oldest := st.releaseOrder[0]
		st.releaseOrder = st.releaseOrder[1:]
		if _, ok := st.releaseByRequest[oldest]; ok {
			delete(st.releaseByRequest, oldest)
			return
		}
	}
}

// nextSessionID 生成 session 标识。
func (s *lockService) nextSessionID() string {
	n := atomic.AddUint64(&s.nextSess, 1)
	return fmt.Sprintf("sess_%d_%d", time.Now().UnixNano(), n)
}

// nextLockToken 生成锁 token。
// 默认使用强随机，随机源异常时回退到时间戳+计数器以保证服务可用。
func (s *lockService) nextLockToken() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// Fallback keeps service available if entropy source is temporarily unavailable.
		n := atomic.AddUint64(&s.nextLock, 1)
		return fmt.Sprintf("lk_%d_%d", time.Now().UnixNano(), n)
	}
	return "lk_" + hex.EncodeToString(buf)
}

// newHandler 组装 HTTP 路由。
func newHandler(svc *lockService) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerServerID, svc.serverID)
		w.Header().Set(headerProtocol, protocolVersion)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		if err := checkProtocol(r); err != nil {
			writeAPIError(w, svc, err)
			return
		}
		switch r.Method {
		case http.MethodPost:
			var req createSessionRequest
			if r.Body != nil {
				if err := json.NewDecoder(io.LimitReader(r.Body, maxJSONBodyBytes)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
					writeError(w, svc, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body")
					return
				}
			}
			resp, err := svc.createSession(req)
			if err != nil {
				writeError(w, svc, http.StatusBadRequest, "BAD_REQUEST", err.Error())
				return
			}
			writeJSON(w, svc, http.StatusOK, resp)
		default:
			writeError(w, svc, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	})

	mux.HandleFunc("/v1/sessions/", func(w http.ResponseWriter, r *http.Request) {
		if err := checkProtocol(r); err != nil {
			writeAPIError(w, svc, err)
			return
		}
		if err := checkServerID(r, svc.serverID); err != nil {
			writeAPIError(w, svc, err)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/v1/sessions/")
		if path == "" {
			writeError(w, svc, http.StatusBadRequest, "BAD_REQUEST", "session id is required")
			return
		}
		if strings.HasSuffix(path, "/heartbeat") {
			sessionID := strings.TrimSuffix(path, "/heartbeat")
			sessionID = strings.TrimSuffix(sessionID, "/")
			if r.Method != http.MethodPost {
				writeError(w, svc, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
				return
			}
			resp, err := svc.heartbeat(sessionID)
			if err != nil {
				writeAPIError(w, svc, err)
				return
			}
			writeJSON(w, svc, http.StatusOK, resp)
			return
		}
		if r.Method != http.MethodDelete {
			writeError(w, svc, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		if !svc.closeSession(path) {
			writeError(w, svc, http.StatusNotFound, "SESSION_NOT_FOUND", "session not found")
			return
		}
		w.Header().Set(headerServerID, svc.serverID)
		w.Header().Set(headerProtocol, protocolVersion)
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/v1/locks", func(w http.ResponseWriter, r *http.Request) {
		if err := checkProtocol(r); err != nil {
			writeAPIError(w, svc, err)
			return
		}
		if err := checkServerID(r, svc.serverID); err != nil {
			writeAPIError(w, svc, err)
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, svc, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}

		var req lockRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, maxJSONBodyBytes)).Decode(&req); err != nil {
			writeError(w, svc, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON body")
			return
		}

		resp, err := svc.acquire(r.Context(), req)
		if err != nil {
			writeAPIError(w, svc, err)
			return
		}
		writeJSON(w, svc, http.StatusOK, resp)
	})

	mux.HandleFunc("/v1/locks/", func(w http.ResponseWriter, r *http.Request) {
		if err := checkProtocol(r); err != nil {
			writeAPIError(w, svc, err)
			return
		}
		if err := checkServerID(r, svc.serverID); err != nil {
			writeAPIError(w, svc, err)
			return
		}
		if r.Method != http.MethodDelete {
			writeError(w, svc, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		token := strings.TrimPrefix(r.URL.Path, "/v1/locks/")
		if token == "" {
			writeError(w, svc, http.StatusBadRequest, "BAD_REQUEST", "token is required")
			return
		}
		sessionID := r.URL.Query().Get("session_id")
		requestID := r.URL.Query().Get("request_id")
		ok, err := svc.release(sessionID, token, requestID)
		if err != nil {
			writeAPIError(w, svc, err)
			return
		}
		if !ok {
			writeError(w, svc, http.StatusNotFound, "LOCK_NOT_FOUND", "lock token not found")
			return
		}
		w.Header().Set(headerServerID, svc.serverID)
		w.Header().Set(headerProtocol, protocolVersion)
		w.WriteHeader(http.StatusNoContent)
	})

	return mux
}

// checkProtocol 校验协议版本。
func checkProtocol(r *http.Request) error {
	v := r.Header.Get(headerProtocol)
	if v == "" || v == protocolVersion {
		return nil
	}
	return &apiError{status: http.StatusConflict, code: "PROTOCOL_MISMATCH", msg: "protocol version mismatch"}
}

// checkServerID 校验客户端期望的服务实例 ID。
func checkServerID(r *http.Request, current string) error {
	expected := r.Header.Get(headerServerID)
	if expected == "" || expected == current {
		return nil
	}
	return &apiError{status: http.StatusConflict, code: "SERVER_INSTANCE_MISMATCH", msg: "server instance changed"}
}

// writeJSON 写入 JSON 响应并补充协议头。
func writeJSON(w http.ResponseWriter, svc *lockService, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(headerServerID, svc.serverID)
	w.Header().Set(headerProtocol, protocolVersion)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError 写结构化错误响应。
func writeError(w http.ResponseWriter, svc *lockService, status int, code, msg string) {
	writeJSON(w, svc, status, errorResponse{Error: msg, Code: code, ServerID: svc.serverID, ProtocolVersion: protocolVersion})
}

// writeAPIError 将内部错误映射为对外可消费的 API 错误。
func writeAPIError(w http.ResponseWriter, svc *lockService, err error) {
	var ae *apiError
	if errors.As(err, &ae) {
		writeError(w, svc, ae.status, ae.code, ae.msg)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		writeError(w, svc, http.StatusConflict, "LOCK_TIMEOUT", "lock wait timeout or canceled")
		return
	}
	writeError(w, svc, http.StatusBadRequest, "BAD_REQUEST", err.Error())
}

// newServerID 生成服务实例 ID，用于客户端实例粘性校验。
func newServerID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("srv_%d", time.Now().UnixNano())
	}
	return "srv_" + hex.EncodeToString(buf)
}

// main 启动 HTTP 锁服务。
func main() {
	addr := envOrDefault("LOCK_SERVER_ADDR", ":8080")
	shardCount := envAsInt("LOCK_SERVER_SHARDS", 1024)
	defaultLeaseMS := envAsInt("LOCK_SERVER_DEFAULT_LEASE_MS", 8000)
	maxLeaseMS := envAsInt("LOCK_SERVER_MAX_LEASE_MS", 60000)

	locker, err := hierlock.New(shardCount)
	if err != nil {
		log.Fatalf("create locker: %v", err)
	}

	serverID := newServerID()
	svc := newLockService(locker, time.Duration(defaultLeaseMS)*time.Millisecond, time.Duration(maxLeaseMS)*time.Millisecond, serverID)
	server := &http.Server{
		Addr:              addr,
		Handler:           newHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	log.Printf("lock server listening on %s server_id=%s protocol=%s", addr, serverID, protocolVersion)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// envOrDefault 读取字符串环境变量并回退默认值。
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envAsInt 读取整型环境变量并回退默认值。
func envAsInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
