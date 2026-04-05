package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/puper/klock/pkg/hierlock"
	"github.com/puper/klock/pkg/lockrpcpb"
	"github.com/puper/klock/pkg/logger"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// lockScope 表示锁粒度：L1（前缀级）或 L2（资源级）。
type lockScope string

const (
	// scopeL1 表示锁定一级键。
	scopeL1 lockScope = "l1"
	// scopeL2 表示锁定二级键。
	scopeL2 lockScope = "l2"

	protocolVersion      = "1"
	errorCodeSessionGone = "SESSION_GONE"

	maxIdempotencyEntries = 4096
	defaultIdempotencyTTL = time.Hour
	expiredRetention      = 10 * time.Minute
	maxExpiredSessions    = 10000

	authHeaderKey   = "authorization"
	authTokenPrefix = "Bearer "
)

var errAcquireSessionInvalidated = errors.New("session invalidated while waiting for acquire")

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

type acquireFingerprint struct {
	scope lockScope
	p1    string
	p2    string
}

type acquireIdempotentEntry struct {
	resp        lockResponse
	fingerprint acquireFingerprint
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

	lease       time.Duration
	expiresAt   time.Time
	timer       *time.Timer
	timerSeq    uint64
	invalidated chan struct{}

	acquireByRequest map[string]acquireIdempotentEntry
	acquireAt        map[string]time.Time
	acquireOrder     []string
	acquireHead      int
	releaseByRequest map[string]string
	releaseAt        map[string]time.Time
	releaseOrder     []string
	releaseHead      int
	locksByToken     map[string]*lockHandle
	watchers         map[uint64]chan *lockrpcpb.ServerEvent
}

// lockService 是 HTTP 接口背后的核心状态机。
type lockService struct {
	locker       *hierlock.HierarchicalLocker
	defaultLease time.Duration
	maxLease     time.Duration
	serverID     string

	mu              sync.Mutex
	nextSess        atomic.Uint64
	nextLock        atomic.Uint64
	nextFence       atomic.Uint64
	sessions        map[string]*sessionState
	tokens          map[string]*lockHandle
	expiredSessions map[string]expiredInfo

	maxIdempotencyEntries int
	idempotencyTTL        time.Duration
	nextWatcherID         atomic.Uint64
	draining              bool
	metrics               *serverMetrics
}

type serverMetrics struct {
	unaryCalls          atomic.Int64
	unaryFailures       atomic.Int64
	streamCalls         atomic.Int64
	streamFailures      atomic.Int64
	activeWatchers      atomic.Int64
	sessionInvalidation atomic.Int64
	authFailures        atomic.Int64
	rateLimited         atomic.Int64
}

type rateEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type rateLimiter struct {
	mu          sync.Mutex
	limitRPS    rate.Limit
	burst       int
	entries     map[string]*rateEntry
	lastCleanup time.Time
}

func newRateLimiter(limitRPS, burst int) *rateLimiter {
	if limitRPS <= 0 {
		limitRPS = 200
	}
	if burst <= 0 {
		burst = limitRPS * 2
	}
	return &rateLimiter{
		limitRPS:    rate.Limit(limitRPS),
		burst:       burst,
		entries:     make(map[string]*rateEntry),
		lastCleanup: time.Now(),
	}
}

func (r *rateLimiter) allow(key string, now time.Time) bool {
	r.mu.Lock()
	entry, ok := r.entries[key]
	if !ok {
		entry = &rateEntry{
			limiter:  rate.NewLimiter(r.limitRPS, r.burst),
			lastSeen: now,
		}
		r.entries[key] = entry
	} else {
		entry.lastSeen = now
	}
	shouldCleanup := now.Sub(r.lastCleanup) > time.Minute
	if shouldCleanup {
		r.lastCleanup = now
	}
	r.mu.Unlock()

	allowed := entry.limiter.AllowN(now, 1)

	if shouldCleanup {
		r.cleanup(now)
	}
	return allowed
}

func (r *rateLimiter) cleanup(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, entry := range r.entries {
		if now.Sub(entry.lastSeen) > 2*time.Minute {
			delete(r.entries, key)
		}
	}
}

func (r *rateLimiter) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// newLockService 构造服务实例。
func newLockService(locker *hierlock.HierarchicalLocker, defaultLease, maxLease time.Duration, serverID string) *lockService {
	return &lockService{
		locker:                locker,
		defaultLease:          defaultLease,
		maxLease:              maxLease,
		serverID:              serverID,
		sessions:              make(map[string]*sessionState),
		tokens:                make(map[string]*lockHandle),
		expiredSessions:       make(map[string]expiredInfo),
		maxIdempotencyEntries: maxIdempotencyEntries,
		idempotencyTTL:        defaultIdempotencyTTL,
		metrics:               &serverMetrics{},
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
		invalidated:      make(chan struct{}),
		acquireByRequest: make(map[string]acquireIdempotentEntry),
		acquireAt:        make(map[string]time.Time),
		acquireOrder:     make([]string, 0, 64),
		releaseByRequest: make(map[string]string),
		releaseAt:        make(map[string]time.Time),
		releaseOrder:     make([]string, 0, 64),
		locksByToken:     make(map[string]*lockHandle),
		watchers:         make(map[uint64]chan *lockrpcpb.ServerEvent),
	}

	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return sessionResponse{}, &apiError{status: http.StatusServiceUnavailable, code: "SERVER_DRAINING", msg: "server is draining for restart"}
	}
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

func acquireContextWithSessionInvalidation(parent context.Context, invalidated <-chan struct{}) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	if invalidated == nil {
		return ctx, cancel
	}
	go func() {
		select {
		case <-invalidated:
			cancel(errAcquireSessionInvalidated)
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (s *lockService) sessionLookupError(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reason := s.expiredReasonLocked(sessionID); reason != "" {
		return &apiError{status: http.StatusGone, code: errorCodeSessionGone, msg: "session expired: " + reason}
	}
	if _, ok := s.sessions[sessionID]; ok {
		return context.Canceled
	}
	return &apiError{status: http.StatusNotFound, code: "SESSION_NOT_FOUND", msg: "session not found"}
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
	if s.draining {
		s.mu.Unlock()
		return lockResponse{}, &apiError{status: http.StatusServiceUnavailable, code: "SERVER_DRAINING", msg: "server is draining for restart"}
	}
	st, ok := s.sessions[req.SessionID]
	if !ok {
		reason := s.expiredReasonLocked(req.SessionID)
		s.mu.Unlock()
		if reason != "" {
			return lockResponse{}, &apiError{status: http.StatusGone, code: errorCodeSessionGone, msg: "session expired: " + reason}
		}
		return lockResponse{}, &apiError{status: http.StatusNotFound, code: "SESSION_NOT_FOUND", msg: "session not found"}
	}
	if cached, ok, err := s.getAcquireIdempotentLocked(st, req, time.Now()); err != nil {
		s.mu.Unlock()
		return lockResponse{}, err
	} else if ok {
		s.mu.Unlock()
		return cached, nil
	}
	invalidated := st.invalidated
	s.mu.Unlock()

	if req.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}
	ctx, cancelAcquireWait := acquireContextWithSessionInvalidation(ctx, invalidated)
	defer cancelAcquireWait(nil)

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
		if errors.Is(context.Cause(ctx), errAcquireSessionInvalidated) {
			return lockResponse{}, s.sessionLookupError(req.SessionID)
		}
		return lockResponse{}, err
	}

	h := &lockHandle{token: s.nextLockToken(), unlock: unlockFn}
	resp := lockResponse{
		Token:           h.token,
		Fence:           s.nextFence.Add(1),
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
	if cached, ok, err := s.getAcquireIdempotentLocked(st, req, time.Now()); err != nil {
		s.mu.Unlock()
		h.safeUnlock()
		return lockResponse{}, err
	} else if ok {
		s.mu.Unlock()
		h.safeUnlock()
		return cached, nil
	}
	s.putAcquireIdempotentLocked(st, req, resp)
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
	if prevToken, ok := s.getReleaseIdempotentLocked(st, requestID, time.Now()); ok {
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
	var watcherChans []chan *lockrpcpb.ServerEvent

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
	if st.invalidated != nil {
		close(st.invalidated)
		st.invalidated = nil
	}
	for token, h := range st.locksByToken {
		handles = append(handles, h)
		delete(s.tokens, token)
	}
	for id, ch := range st.watchers {
		watcherChans = append(watcherChans, ch)
		delete(st.watchers, id)
		if s.metrics != nil {
			s.metrics.activeWatchers.Add(-1)
		}
	}
	s.mu.Unlock()

	for _, h := range handles {
		h.safeUnlock()
	}
	ev := lockrpcpb.ServerEvent{
		Type:            "SESSION_INVALIDATED",
		Message:         "session invalidated: " + reason,
		Code:            "SESSION_GONE",
		SessionId:       sessionID,
		ServerId:        s.serverID,
		ProtocolVersion: protocolVersion,
	}
	if s.metrics != nil {
		s.metrics.sessionInvalidation.Add(1)
	}
	for _, ch := range watcherChans {
		select {
		case ch <- &ev:
		default:
		}
		close(ch)
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

func (s *lockService) registerWatcher(sessionID string) (<-chan *lockrpcpb.ServerEvent, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return nil, 0, &apiError{status: http.StatusServiceUnavailable, code: "SERVER_DRAINING", msg: "server is draining for restart"}
	}
	st, ok := s.sessions[sessionID]
	if !ok {
		reason := s.expiredReasonLocked(sessionID)
		if reason != "" {
			return nil, 0, &apiError{status: http.StatusGone, code: errorCodeSessionGone, msg: "session expired: " + reason}
		}
		return nil, 0, &apiError{status: http.StatusNotFound, code: "SESSION_NOT_FOUND", msg: "session not found"}
	}
	id := s.nextWatcherID.Add(1)
	ch := make(chan *lockrpcpb.ServerEvent, 16)
	st.watchers[id] = ch
	if s.metrics != nil {
		s.metrics.activeWatchers.Add(1)
	}
	return ch, id, nil
}

func (s *lockService) unregisterWatcher(sessionID string, watcherID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.sessions[sessionID]
	if !ok {
		return
	}
	ch, ok := st.watchers[watcherID]
	if !ok {
		return
	}
	delete(st.watchers, watcherID)
	if s.metrics != nil {
		s.metrics.activeWatchers.Add(-1)
	}
	close(ch)
}

func (s *lockService) drainForRestart() {
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return
	}
	s.draining = true
	sessionIDs := make([]string, 0, len(s.sessions))
	for sid := range s.sessions {
		sessionIDs = append(sessionIDs, sid)
	}
	s.mu.Unlock()
	for _, sid := range sessionIDs {
		s.expireSession(sid, "server_restarting")
	}
}

// putAcquireIdempotentLocked 写入 acquire 幂等缓存并执行有界淘汰。
func (s *lockService) putAcquireIdempotentLocked(st *sessionState, req lockRequest, resp lockResponse) {
	requestID := req.RequestID
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
	st.acquireByRequest[requestID] = acquireIdempotentEntry{
		resp:        resp,
		fingerprint: fingerprintAcquireRequest(req),
	}
	st.acquireAt[requestID] = time.Now()
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
	st.releaseAt[requestID] = time.Now()
	st.releaseOrder = append(st.releaseOrder, requestID)
}

func (s *lockService) getAcquireIdempotentLocked(st *sessionState, req lockRequest, now time.Time) (lockResponse, bool, error) {
	requestID := req.RequestID
	entry, ok := st.acquireByRequest[requestID]
	if !ok {
		return lockResponse{}, false, nil
	}
	if s.idempotencyTTL > 0 {
		at := st.acquireAt[requestID]
		if at.IsZero() || now.Sub(at) > s.idempotencyTTL {
			delete(st.acquireByRequest, requestID)
			delete(st.acquireAt, requestID)
			return lockResponse{}, false, nil
		}
	}
	if entry.fingerprint != fingerprintAcquireRequest(req) {
		return lockResponse{}, false, &apiError{
			status: http.StatusConflict,
			code:   "IDEMPOTENCY_CONFLICT",
			msg:    "request_id already used for a different acquire request",
		}
	}
	return entry.resp, true, nil
}

func fingerprintAcquireRequest(req lockRequest) acquireFingerprint {
	p2 := req.P2
	if req.Scope == scopeL1 {
		// L1 锁语义只依赖 p1，忽略 p2 以避免无意义的幂等冲突。
		p2 = ""
	}
	return acquireFingerprint{
		scope: req.Scope,
		p1:    req.P1,
		p2:    p2,
	}
}

func (s *lockService) getReleaseIdempotentLocked(st *sessionState, requestID string, now time.Time) (string, bool) {
	token, ok := st.releaseByRequest[requestID]
	if !ok {
		return "", false
	}
	if s.idempotencyTTL > 0 {
		at := st.releaseAt[requestID]
		if at.IsZero() || now.Sub(at) > s.idempotencyTTL {
			delete(st.releaseByRequest, requestID)
			delete(st.releaseAt, requestID)
			return "", false
		}
	}
	return token, true
}

func popOrderQueue(order []string, head int) (string, []string, int) {
	if head >= len(order) {
		return "", nil, 0
	}
	oldest := order[head]
	order[head] = ""
	head++
	if head >= len(order) {
		return oldest, nil, 0
	}
	if head >= 64 && head*2 >= len(order) {
		compacted := append([]string(nil), order[head:]...)
		return oldest, compacted, 0
	}
	return oldest, order, head
}

// evictOldestAcquireLocked 淘汰最旧 acquire request_id。
func (s *lockService) evictOldestAcquireLocked(st *sessionState) {
	for st.acquireHead < len(st.acquireOrder) {
		var oldest string
		oldest, st.acquireOrder, st.acquireHead = popOrderQueue(st.acquireOrder, st.acquireHead)
		if _, ok := st.acquireByRequest[oldest]; ok {
			delete(st.acquireByRequest, oldest)
			delete(st.acquireAt, oldest)
			return
		}
	}
	st.acquireOrder = nil
	st.acquireHead = 0
}

// evictOldestReleaseLocked 淘汰最旧 release request_id。
func (s *lockService) evictOldestReleaseLocked(st *sessionState) {
	for st.releaseHead < len(st.releaseOrder) {
		var oldest string
		oldest, st.releaseOrder, st.releaseHead = popOrderQueue(st.releaseOrder, st.releaseHead)
		if _, ok := st.releaseByRequest[oldest]; ok {
			delete(st.releaseByRequest, oldest)
			delete(st.releaseAt, oldest)
			return
		}
	}
	st.releaseOrder = nil
	st.releaseHead = 0
}

// nextSessionID 生成 session 标识。
func (s *lockService) nextSessionID() string {
	n := s.nextSess.Add(1)
	return fmt.Sprintf("sess_%d_%d", time.Now().UnixNano(), n)
}

// nextLockToken 生成锁 token。
// 默认使用强随机，随机源异常时回退到时间戳+计数器以保证服务可用。
func (s *lockService) nextLockToken() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// Fallback keeps service available if entropy source is temporarily unavailable.
		n := s.nextLock.Add(1)
		return fmt.Sprintf("lk_%d_%d", time.Now().UnixNano(), n)
	}
	return "lk_" + hex.EncodeToString(buf)
}

type grpcServer struct {
	lockrpcpb.UnimplementedLockServiceServer
	svc *lockService
}

func newGRPCServer(svc *lockService) *grpcServer {
	return &grpcServer{svc: svc}
}

func (g *grpcServer) rpcErr(status int, code, msg string) *lockrpcpb.ErrorStatus {
	return &lockrpcpb.ErrorStatus{
		Status:          int32(status),
		Code:            code,
		Message:         msg,
		ServerId:        g.svc.serverID,
		ProtocolVersion: protocolVersion,
	}
}

func (g *grpcServer) rpcFromErr(err error) *lockrpcpb.ErrorStatus {
	if err == nil {
		return nil
	}
	var ae *apiError
	if errors.As(err, &ae) {
		return g.rpcErr(ae.status, ae.code, ae.msg)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return g.rpcErr(http.StatusConflict, "LOCK_TIMEOUT", "lock wait timeout or canceled")
	}
	return g.rpcErr(http.StatusBadRequest, "BAD_REQUEST", err.Error())
}

func checkProtocolValue(v string) error {
	if v == protocolVersion {
		return nil
	}
	return &apiError{status: http.StatusConflict, code: "PROTOCOL_MISMATCH", msg: "protocol version mismatch"}
}

func checkServerValue(expected, current string) error {
	if expected == "" || expected == current {
		return nil
	}
	return &apiError{status: http.StatusConflict, code: "SERVER_INSTANCE_MISMATCH", msg: "server instance changed"}
}

func (g *grpcServer) CreateSession(_ context.Context, req *lockrpcpb.CreateSessionRequest) (*lockrpcpb.SessionResponse, error) {
	if err := checkProtocolValue(req.ProtocolVersion); err != nil {
		return &lockrpcpb.SessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	if err := checkServerValue(req.ExpectedServer, g.svc.serverID); err != nil {
		return &lockrpcpb.SessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	resp, err := g.svc.createSession(createSessionRequest{LeaseMS: req.LeaseMs})
	if err != nil {
		return &lockrpcpb.SessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	return &lockrpcpb.SessionResponse{
		SessionId:       resp.SessionID,
		LeaseMs:         resp.LeaseMS,
		ExpiresAt:       resp.ExpiresAt,
		ServerId:        resp.ServerID,
		ProtocolVersion: resp.ProtocolVersion,
	}, nil
}

func (g *grpcServer) Heartbeat(_ context.Context, req *lockrpcpb.HeartbeatRequest) (*lockrpcpb.SessionResponse, error) {
	if err := checkProtocolValue(req.ProtocolVersion); err != nil {
		return &lockrpcpb.SessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	if err := checkServerValue(req.ExpectedServer, g.svc.serverID); err != nil {
		return &lockrpcpb.SessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	resp, err := g.svc.heartbeat(req.SessionId)
	if err != nil {
		return &lockrpcpb.SessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	return &lockrpcpb.SessionResponse{
		SessionId:       resp.SessionID,
		LeaseMs:         resp.LeaseMS,
		ExpiresAt:       resp.ExpiresAt,
		ServerId:        resp.ServerID,
		ProtocolVersion: resp.ProtocolVersion,
	}, nil
}

func (g *grpcServer) Acquire(ctx context.Context, req *lockrpcpb.LockRequest) (*lockrpcpb.LockResponse, error) {
	if err := checkProtocolValue(req.ProtocolVersion); err != nil {
		return &lockrpcpb.LockResponse{Error: g.rpcFromErr(err)}, nil
	}
	if err := checkServerValue(req.ExpectedServer, g.svc.serverID); err != nil {
		return &lockrpcpb.LockResponse{Error: g.rpcFromErr(err)}, nil
	}
	resp, err := g.svc.acquire(ctx, lockRequest{
		Scope:     lockScope(req.Scope),
		P1:        req.P1,
		P2:        req.P2,
		TimeoutMS: req.TimeoutMs,
		SessionID: req.SessionId,
		RequestID: req.RequestId,
	})
	if err != nil {
		return &lockrpcpb.LockResponse{Error: g.rpcFromErr(err)}, nil
	}
	return &lockrpcpb.LockResponse{
		Token:           resp.Token,
		Fence:           resp.Fence,
		Scope:           resp.Scope,
		P1:              resp.P1,
		P2:              resp.P2,
		SessionId:       resp.SessionID,
		ServerId:        resp.ServerID,
		ProtocolVersion: resp.ProtocolVersion,
	}, nil
}

func (g *grpcServer) Release(_ context.Context, req *lockrpcpb.ReleaseRequest) (*lockrpcpb.ReleaseResponse, error) {
	if err := checkProtocolValue(req.ProtocolVersion); err != nil {
		return &lockrpcpb.ReleaseResponse{Error: g.rpcFromErr(err)}, nil
	}
	if err := checkServerValue(req.ExpectedServer, g.svc.serverID); err != nil {
		return &lockrpcpb.ReleaseResponse{Error: g.rpcFromErr(err)}, nil
	}
	ok, err := g.svc.release(req.SessionId, req.Token, req.RequestId)
	if err != nil {
		return &lockrpcpb.ReleaseResponse{Error: g.rpcFromErr(err)}, nil
	}
	return &lockrpcpb.ReleaseResponse{
		Ok:              ok,
		ServerId:        g.svc.serverID,
		ProtocolVersion: protocolVersion,
	}, nil
}

func (g *grpcServer) CloseSession(_ context.Context, req *lockrpcpb.CloseSessionRequest) (*lockrpcpb.CloseSessionResponse, error) {
	if err := checkProtocolValue(req.ProtocolVersion); err != nil {
		return &lockrpcpb.CloseSessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	if err := checkServerValue(req.ExpectedServer, g.svc.serverID); err != nil {
		return &lockrpcpb.CloseSessionResponse{Error: g.rpcFromErr(err)}, nil
	}
	closed := g.svc.closeSession(req.SessionId)
	if !closed {
		return &lockrpcpb.CloseSessionResponse{
			Closed:          false,
			ServerId:        g.svc.serverID,
			ProtocolVersion: protocolVersion,
			Error:           g.rpcErr(http.StatusNotFound, "SESSION_NOT_FOUND", "session not found"),
		}, nil
	}
	return &lockrpcpb.CloseSessionResponse{
		Closed:          true,
		ServerId:        g.svc.serverID,
		ProtocolVersion: protocolVersion,
	}, nil
}

func (g *grpcServer) WatchSession(stream lockrpcpb.LockService_WatchSessionServer) error {
	first, err := stream.Recv()
	if err != nil {
		return nil
	}
	if err := checkProtocolValue(first.ProtocolVersion); err != nil {
		ev := g.rpcFromErr(err)
		_ = stream.Send(&lockrpcpb.ServerEvent{
			Type:            "PROTOCOL_MISMATCH",
			Message:         ev.Message,
			Code:            ev.Code,
			ServerId:        g.svc.serverID,
			ProtocolVersion: protocolVersion,
		})
		return nil
	}
	sessionID := first.SessionId
	wch, watcherID, err := g.svc.registerWatcher(sessionID)
	if err != nil {
		ev := g.rpcFromErr(err)
		_ = stream.Send(&lockrpcpb.ServerEvent{
			Type:            "SESSION_INVALIDATED",
			Message:         ev.Message,
			Code:            ev.Code,
			SessionId:       sessionID,
			ServerId:        g.svc.serverID,
			ProtocolVersion: protocolVersion,
		})
		return nil
	}
	defer g.svc.unregisterWatcher(sessionID, watcherID)

	if err := stream.Send(&lockrpcpb.ServerEvent{
		Type:            "WATCH_ESTABLISHED",
		Message:         "watch established",
		SessionId:       sessionID,
		ServerId:        g.svc.serverID,
		ProtocolVersion: protocolVersion,
	}); err != nil {
		return nil
	}

	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-recvDone:
			return nil
		case ev, ok := <-wch:
			if !ok {
				return nil
			}
			if err := stream.Send(ev); err != nil {
				return nil
			}
		}
	}
}

func authTokenFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(authHeaderKey)
	if len(values) == 0 {
		return ""
	}
	v := strings.TrimSpace(values[0])
	if strings.HasPrefix(strings.ToLower(v), strings.ToLower(authTokenPrefix)) {
		return strings.TrimSpace(v[len(authTokenPrefix):])
	}
	return v
}

func peerKeyFromContext(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil || p.Addr == nil {
		return "unknown"
	}
	addr := p.Addr.String()
	host, _, err := net.SplitHostPort(addr)
	if err == nil && host != "" {
		return host
	}
	return addr
}

func grpcServerAuthUnaryInterceptor(expectedToken string, m *serverMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if expectedToken == "" {
			return handler(ctx, req)
		}
		got := authTokenFromContext(ctx)
		if subtleConstantTimeMatch(got, expectedToken) {
			return handler(ctx, req)
		}
		m.authFailures.Add(1)
		return nil, status.Error(codes.Unauthenticated, "missing or invalid auth token")
	}
}

func grpcServerAuthStreamInterceptor(expectedToken string, m *serverMetrics) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if expectedToken == "" {
			return handler(srv, ss)
		}
		got := authTokenFromContext(ss.Context())
		if subtleConstantTimeMatch(got, expectedToken) {
			return handler(srv, ss)
		}
		m.authFailures.Add(1)
		return status.Error(codes.Unauthenticated, "missing or invalid auth token")
	}
}

func subtleConstantTimeMatch(a, b string) bool {
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	var diff byte = byte(len(a) ^ len(b))
	for i := 0; i < maxLen; i++ {
		var av, bv byte
		if i < len(a) {
			av = a[i]
		}
		if i < len(b) {
			bv = b[i]
		}
		diff |= av ^ bv
	}
	return subtle.ConstantTimeByteEq(diff, 0) == 1
}

func grpcServerRateUnaryInterceptor(limiter *rateLimiter, m *serverMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		key := info.FullMethod + "|" + peerKeyFromContext(ctx)
		if limiter.allow(key, time.Now()) {
			return handler(ctx, req)
		}
		m.rateLimited.Add(1)
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}
}

func grpcServerRateStreamInterceptor(limiter *rateLimiter, m *serverMetrics) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		key := info.FullMethod + "|" + peerKeyFromContext(ss.Context())
		if limiter.allow(key, time.Now()) {
			return handler(srv, ss)
		}
		m.rateLimited.Add(1)
		return status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}
}

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

// newServerID 生成服务实例 ID，用于客户端实例粘性校验。
func newServerID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("srv_%d", time.Now().UnixNano())
	}
	return "srv_" + hex.EncodeToString(buf)
}

// main 启动 gRPC 锁服务。
func main() {
	// Initialize logger
	logConfig := logger.LoadConfigFromEnv()
	zapLogger, err := logger.NewLogger(logConfig)
	if err != nil {
		panic(fmt.Sprintf("initialize logger: %v", err))
	}
	defer zapLogger.Sync()

	// Replace global logger
	zap.ReplaceGlobals(zapLogger)

	addr := envOrDefault("LOCK_SERVER_ADDR", ":8080")
	shardCount := envAsInt("LOCK_SERVER_SHARDS", 1024)
	defaultLeaseMS := envAsInt("LOCK_SERVER_DEFAULT_LEASE_MS", 8000)
	maxLeaseMS := envAsInt("LOCK_SERVER_MAX_LEASE_MS", 60000)
	idempotencyEntries := envAsInt("LOCK_SERVER_IDEMPOTENCY_ENTRIES", 65536)
	idempotencyTTLMS := envAsInt("LOCK_SERVER_IDEMPOTENCY_TTL_MS", int(defaultIdempotencyTTL/time.Millisecond))
	authToken := os.Getenv("LOCK_SERVER_AUTH_TOKEN")
	rateLimitRPS := envAsInt("LOCK_SERVER_RATE_LIMIT_RPS", 200)
	rateLimitBurst := envAsInt("LOCK_SERVER_RATE_LIMIT_BURST", 400)

	locker, err := hierlock.New(shardCount)
	if err != nil {
		zap.L().Fatal("failed to create locker", zap.Error(err))
	}

	serverID := newServerID()
	svc := newLockService(locker, time.Duration(defaultLeaseMS)*time.Millisecond, time.Duration(maxLeaseMS)*time.Millisecond, serverID)
	svc.maxIdempotencyEntries = idempotencyEntries
	svc.idempotencyTTL = time.Duration(idempotencyTTLMS) * time.Millisecond
	metrics := &serverMetrics{}
	svc.metrics = metrics
	limiter := newRateLimiter(rateLimitRPS, rateLimitBurst)
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcServerAuthUnaryInterceptor(authToken, metrics),
			grpcServerRateUnaryInterceptor(limiter, metrics),
			grpcServerUnaryMetricsInterceptor(metrics),
		),
		grpc.ChainStreamInterceptor(
			grpcServerAuthStreamInterceptor(authToken, metrics),
			grpcServerRateStreamInterceptor(limiter, metrics),
			grpcServerStreamMetricsInterceptor(metrics),
		),
	)
	lockrpcpb.RegisterLockServiceServer(grpcServer, newGRPCServer(svc))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		zap.L().Fatal("failed to listen", zap.Error(err), zap.String("address", addr))
	}
	defer lis.Close()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(lis)
	}()

	zap.L().Info("lock grpc server started",
		zap.String("address", addr),
		zap.String("server_id", serverID),
		zap.String("protocol", protocolVersion),
		zap.Bool("auth_enabled", authToken != ""),
		zap.Int("rate_rps", rateLimitRPS),
		zap.Int("rate_burst", rateLimitBurst),
		zap.Int("idempotency_entries", idempotencyEntries),
		zap.Int("idempotency_ttl_ms", idempotencyTTLMS),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go startMetricsReporter(ctx, metrics, limiter)

	select {
	case <-ctx.Done():
		zap.L().Info("shutdown signal received; draining sessions")
		svc.drainForRestart()
		time.Sleep(200 * time.Millisecond)
		grpcServer.GracefulStop()
	case err := <-serveErr:
		if err != nil {
			zap.L().Fatal("server error", zap.Error(err))
		}
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
