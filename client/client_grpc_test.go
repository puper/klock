package client

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/puper/klock/pkg/lockrpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeLockRPCServer struct {
	lockrpcpb.UnimplementedLockServiceServer

	mu                         sync.Mutex
	watchers                   []chan *lockrpcpb.ServerEvent
	closeSessionCalls          int
	authToken                  string
	releaseFailFor             int
	releaseCalls               int
	releaseRequestID           []string
	releaseTokens              []string
	failAcquireP2              string
	failAcquireCode            codes.Code
	failAcquireAPIErrorP2      string
	failAcquireAPIError        *lockrpcpb.ErrorStatus
	acquireTimeoutMS           []int64
	acquireDeadlines           []time.Duration
	acquireDelay               time.Duration
	acquireStarted             chan struct{}
	acquireStartSent           bool
	acquireBlock               chan struct{}
	createSessionCalls         int
	createSessionFailFor       int
	createSessionStarted       chan struct{}
	createSessionStartSent     bool
	createSessionBlock         chan struct{}
	createSessionDelay         time.Duration
	closeSessionDelay          time.Duration
	watchBreakAfterEstablished bool
	acquireServerID            string
}

func (f *fakeLockRPCServer) requireAuth(ctx context.Context) error {
	if strings.TrimSpace(f.authToken) == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	v := ""
	values := md.Get("authorization")
	if len(values) > 0 {
		v = strings.TrimSpace(values[0])
	}
	if v != "Bearer "+f.authToken {
		return status.Error(codes.Unauthenticated, "missing or invalid auth token")
	}
	return nil
}

func (f *fakeLockRPCServer) CreateSession(ctx context.Context, _ *lockrpcpb.CreateSessionRequest) (*lockrpcpb.SessionResponse, error) {
	if err := f.requireAuth(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.createSessionCalls++
	call := f.createSessionCalls
	delay := f.createSessionDelay
	started := f.createSessionStarted
	if started != nil && !f.createSessionStartSent {
		close(started)
		f.createSessionStartSent = true
	}
	block := f.createSessionBlock
	failFor := f.createSessionFailFor
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	if call <= failFor {
		return nil, status.Error(codes.Unavailable, "simulated create session failure")
	}
	return &lockrpcpb.SessionResponse{
		SessionId:       "sess-1",
		LeaseMs:         2000,
		ExpiresAt:       time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339Nano),
		ServerId:        "srv-a",
		ProtocolVersion: protocolVersion,
	}, nil
}

func (f *fakeLockRPCServer) Heartbeat(ctx context.Context, _ *lockrpcpb.HeartbeatRequest) (*lockrpcpb.SessionResponse, error) {
	if err := f.requireAuth(ctx); err != nil {
		return nil, err
	}
	return &lockrpcpb.SessionResponse{
		SessionId:       "sess-1",
		LeaseMs:         2000,
		ExpiresAt:       time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339Nano),
		ServerId:        "srv-a",
		ProtocolVersion: protocolVersion,
	}, nil
}

func (f *fakeLockRPCServer) Acquire(ctx context.Context, req *lockrpcpb.LockRequest) (*lockrpcpb.LockResponse, error) {
	if err := f.requireAuth(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	failP2 := f.failAcquireP2
	failCode := f.failAcquireCode
	failAPIErrorP2 := f.failAcquireAPIErrorP2
	failAPIError := f.failAcquireAPIError
	acquireServerID := f.acquireServerID
	f.acquireTimeoutMS = append(f.acquireTimeoutMS, req.TimeoutMs)
	if dl, ok := ctx.Deadline(); ok {
		f.acquireDeadlines = append(f.acquireDeadlines, time.Until(dl))
	}
	delay := f.acquireDelay
	started := f.acquireStarted
	if started != nil && !f.acquireStartSent {
		close(started)
		f.acquireStartSent = true
	}
	block := f.acquireBlock
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	if failP2 != "" && req.P2 == failP2 {
		return nil, status.Error(failCode, "simulated acquire failure")
	}
	if failAPIErrorP2 != "" && req.P2 == failAPIErrorP2 {
		return &lockrpcpb.LockResponse{
			Error: failAPIError,
		}, nil
	}
	if acquireServerID == "" {
		acquireServerID = "srv-a"
	}
	return &lockrpcpb.LockResponse{
		Token:           "lk-1",
		Fence:           1,
		SessionId:       "sess-1",
		ServerId:        acquireServerID,
		ProtocolVersion: protocolVersion,
	}, nil
}

func TestLockAttemptTimeoutAppliedPerAcquire(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{authToken: "test-token"}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h, err := c.Lock(ctx, "tenant-1", "res-attempt", LockOption{
		Timeout:        time.Second,
		AttemptTimeout: 120 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}
	if err := h.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}
	<-h.Done()

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.acquireTimeoutMS) == 0 {
		t.Fatal("expected acquire timeout_ms recorded")
	}
	gotTimeout := fake.acquireTimeoutMS[0]
	if gotTimeout <= 0 || gotTimeout > 120 {
		t.Fatalf("expected acquire timeout_ms in (0,120], got %d", gotTimeout)
	}
	if len(fake.acquireDeadlines) == 0 {
		t.Fatal("expected acquire context deadline captured")
	}
	gotDeadline := fake.acquireDeadlines[0]
	if gotDeadline <= 0 || gotDeadline > 220*time.Millisecond {
		t.Fatalf("expected per-attempt deadline <=220ms, got %s", gotDeadline)
	}
}

func (f *fakeLockRPCServer) Release(ctx context.Context, req *lockrpcpb.ReleaseRequest) (*lockrpcpb.ReleaseResponse, error) {
	if err := f.requireAuth(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.releaseCalls++
	call := f.releaseCalls
	f.releaseRequestID = append(f.releaseRequestID, req.RequestId)
	f.releaseTokens = append(f.releaseTokens, req.Token)
	failFor := f.releaseFailFor
	f.mu.Unlock()
	if call <= failFor {
		return nil, status.Error(codes.Unavailable, "temporary release failure")
	}
	return &lockrpcpb.ReleaseResponse{
		Ok:              true,
		ServerId:        "srv-a",
		ProtocolVersion: protocolVersion,
	}, nil
}

func (f *fakeLockRPCServer) CloseSession(ctx context.Context, _ *lockrpcpb.CloseSessionRequest) (*lockrpcpb.CloseSessionResponse, error) {
	if err := f.requireAuth(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.closeSessionCalls++
	delay := f.closeSessionDelay
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return &lockrpcpb.CloseSessionResponse{
		Closed:          true,
		ServerId:        "srv-a",
		ProtocolVersion: protocolVersion,
	}, nil
}

func (f *fakeLockRPCServer) WatchSession(stream lockrpcpb.LockService_WatchSessionServer) error {
	if err := f.requireAuth(stream.Context()); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return nil
	}
	_ = first
	ch := make(chan *lockrpcpb.ServerEvent, 4)
	f.mu.Lock()
	f.watchers = append(f.watchers, ch)
	f.mu.Unlock()
	defer close(ch)

	if err := stream.Send(&lockrpcpb.ServerEvent{
		Type:            "WATCH_ESTABLISHED",
		SessionId:       "sess-1",
		ServerId:        "srv-a",
		ProtocolVersion: protocolVersion,
	}); err != nil {
		return nil
	}
	f.mu.Lock()
	breakAfterEstablished := f.watchBreakAfterEstablished
	f.mu.Unlock()
	if breakAfterEstablished {
		return nil
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev := <-ch:
			if err := stream.Send(ev); err != nil {
				return nil
			}
		}
	}
}

func (f *fakeLockRPCServer) invalidate(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.watchers {
		ch <- &lockrpcpb.ServerEvent{
			Type:            "SESSION_INVALIDATED",
			Message:         "session invalidated: " + reason,
			Code:            "SESSION_GONE",
			SessionId:       "sess-1",
			ServerId:        "srv-a",
			ProtocolVersion: protocolVersion,
		}
	}
}

func TestGRPCWatchInvalidatesLockOnServerRestart(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{authToken: "test-token"}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	h, err := c.Lock(context.Background(), "tenant-1", "res-1", LockOption{})
	if err != nil {
		t.Fatalf("grpc lock failed: %v", err)
	}

	time.Sleep(80 * time.Millisecond)
	fake.invalidate("server_restarting")

	select {
	case ev := <-h.Done():
		if ev.Type != EventSessionGone {
			t.Fatalf("expected session_gone, got %s", ev.Type)
		}
		if ev.ErrorCode != "SESSION_GONE" {
			t.Fatalf("expected SESSION_GONE, got %s", ev.ErrorCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected lock invalidation event from watch stream")
	}
}

func TestUnlockWithRetryUsesSameRequestIDAndEventuallySucceeds(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:      "test-token",
		releaseFailFor: 2,
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	h, err := c.Lock(context.Background(), "tenant-1", "res-retry", LockOption{})
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}
	if err := h.UnlockWithRetry(context.Background(), 4, 10*time.Millisecond); err != nil {
		t.Fatalf("UnlockWithRetry failed: %v", err)
	}

	ev := <-h.Done()
	if ev.Type != EventUnlocked {
		t.Fatalf("expected unlocked event, got %s", ev.Type)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.releaseCalls < 3 {
		t.Fatalf("expected at least 3 release attempts, got %d", fake.releaseCalls)
	}
	if len(fake.releaseRequestID) == 0 {
		t.Fatal("expected release request ids recorded")
	}
	first := fake.releaseRequestID[0]
	for i, rid := range fake.releaseRequestID {
		if rid != first {
			t.Fatalf("expected same request_id across retries, idx=%d got=%q want=%q", i, rid, first)
		}
	}
}

func TestLockUncertainFailureTriggersFailClosedSession(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:       "test-token",
		failAcquireP2:   "res-uncertain",
		failAcquireCode: codes.Unavailable,
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	h1, err := c.Lock(context.Background(), "tenant-1", "res-hold", LockOption{})
	if err != nil {
		t.Fatalf("first lock failed: %v", err)
	}

	_, err = c.Lock(context.Background(), "tenant-1", "res-uncertain", LockOption{Timeout: 300 * time.Millisecond})
	if err == nil {
		t.Fatal("expected uncertain lock failure")
	}

	select {
	case ev := <-h1.Done():
		if ev.Type != EventSessionGone {
			t.Fatalf("expected session_gone after uncertain lock failure, got %s", ev.Type)
		}
		if ev.ErrorCode != "LOCK_UNCERTAIN_RESULT" {
			t.Fatalf("expected LOCK_UNCERTAIN_RESULT, got %s", ev.ErrorCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected held lock to be fail-closed after uncertain lock failure")
	}
}

func TestLockIdempotencyConflictDoesNotFailCloseSession(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:             "test-token",
		failAcquireAPIErrorP2: "res-idempotency-conflict",
		failAcquireAPIError: &lockrpcpb.ErrorStatus{
			Status:          409,
			Code:            "IDEMPOTENCY_CONFLICT",
			Message:         "request_id already used for a different acquire request",
			ServerId:        "srv-a",
			ProtocolVersion: protocolVersion,
		},
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	h1, err := c.Lock(context.Background(), "tenant-1", "res-hold", LockOption{})
	if err != nil {
		t.Fatalf("first lock failed: %v", err)
	}
	defer func() {
		_ = h1.Unlock(context.Background())
		<-h1.Done()
	}()

	_, err = c.Lock(context.Background(), "tenant-1", "res-idempotency-conflict", LockOption{Timeout: 300 * time.Millisecond})
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected IDEMPOTENCY_CONFLICT, got: %v", err)
	}

	select {
	case ev := <-h1.Done():
		t.Fatalf("expected held lock to remain valid on idempotency conflict, got event type=%s code=%s", ev.Type, ev.ErrorCode)
	case <-time.After(300 * time.Millisecond):
	}

	c.mu.Lock()
	sessionID := c.sessionID
	c.mu.Unlock()
	if sessionID == "" {
		t.Fatal("expected session to remain valid after idempotency conflict")
	}
}

func TestWatchDisconnectTriggersBestEffortCloseSession(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:                  "test-token",
		watchBreakAfterEstablished: true,
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      15 * time.Second,
		HeartbeatInterval: 30 * time.Second,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          15 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	h, err := c.Lock(context.Background(), "tenant-1", "res-watch-break", LockOption{})
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}

	select {
	case ev := <-h.Done():
		if ev.Type != EventSessionGone {
			t.Fatalf("expected session_gone, got %s", ev.Type)
		}
		if ev.ErrorCode != "WATCH_STREAM_DISCONNECTED" {
			t.Fatalf("expected WATCH_STREAM_DISCONNECTED, got %s", ev.ErrorCode)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("expected watch disconnect to invalidate lock")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		calls := fake.closeSessionCalls
		fake.mu.Unlock()
		if calls >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected best-effort CloseSession after watch disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRemoteCloseDeduplicatedPerSession(t *testing.T) {
	c := &Client{
		remoteCloseInFlight: make(map[string]chan struct{}),
	}

	ch1, started1 := c.beginManagedRemoteClose("sess-1")
	if !started1 {
		t.Fatal("expected first close attempt to start")
	}
	ch2, started2 := c.beginManagedRemoteClose("sess-1")
	if started2 {
		t.Fatal("expected second close attempt to be deduplicated")
	}
	if ch1 != ch2 {
		t.Fatal("expected deduplicated close attempts to share the same completion channel")
	}
	c.finishManagedRemoteClose("sess-1", ch1)
	select {
	case <-ch2:
	case <-time.After(time.Second):
		t.Fatal("expected deduplicated close waiter to be released")
	}
}

func TestUncertainFailureStormDeduplicatesRemoteClose(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{authToken: "test-token"}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.failClosedOnUncertainLockFailure("sess-1", status.Error(codes.Internal, "simulated uncertain failure"))
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		calls := fake.closeSessionCalls
		fake.mu.Unlock()
		if calls == 1 {
			break
		}
		if time.Now().After(deadline) {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			t.Fatalf("expected failure storm to produce exactly one CloseSession call, got %d", fake.closeSessionCalls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.closeSessionCalls != 1 {
		t.Fatalf("expected deduplicated remote close count to remain 1, got %d", fake.closeSessionCalls)
	}
}

func TestCloseWaitsForInFlightCreateSessionCleanup(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:          "test-token",
		createSessionDelay: 150 * time.Millisecond,
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})

	lockDone := make(chan error, 1)
	go func() {
		_, err := c.Lock(context.Background(), "tenant-1", "res-close-race", LockOption{})
		lockDone <- err
	}()

	time.Sleep(30 * time.Millisecond)
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(closeCtx); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err = <-lockDone
	if err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("expected lock to observe closed client, got %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.closeSessionCalls != 1 {
		t.Fatalf("expected exactly one CloseSession call after in-flight createSession cleanup, got %d", fake.closeSessionCalls)
	}
}

func TestEnsureSessionWaitersUnblockAfterCreateFailure(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:            "test-token",
		createSessionFailFor: 1,
		createSessionStarted: make(chan struct{}),
		createSessionBlock:   make(chan struct{}),
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	initErrCh := make(chan error, 1)
	go func() {
		_, err := c.ensureSession(context.Background())
		initErrCh <- err
	}()

	select {
	case <-fake.createSessionStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial CreateSession start")
	}

	const waiters = 8
	waiterDone := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := c.ensureSession(ctx)
			waiterDone <- err
		}()
	}

	time.Sleep(20 * time.Millisecond)
	close(fake.createSessionBlock)

	select {
	case err := <-initErrCh:
		if err == nil {
			t.Fatal("expected first ensureSession to fail")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initial ensureSession did not return after create failure")
	}

	for i := 0; i < waiters; i++ {
		select {
		case err := <-waiterDone:
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("waiter %d timed out; possible unblock regression: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d did not return; possible unblock regression", i)
		}
	}

	if _, err := c.ensureSession(context.Background()); err != nil {
		t.Fatalf("expected ensureSession recovery after transient create failure, got %v", err)
	}
}

func TestCloseDuringInFlightAcquireReleasesLateHandle(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:         "test-token",
		acquireStarted:    make(chan struct{}),
		acquireBlock:      make(chan struct{}),
		closeSessionDelay: 200 * time.Millisecond,
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})

	if _, err := c.ensureSession(context.Background()); err != nil {
		t.Fatalf("ensure session failed: %v", err)
	}

	lockDone := make(chan error, 1)
	go func() {
		_, err := c.Lock(context.Background(), "tenant-1", "res-close-after-acquire", LockOption{})
		lockDone <- err
	}()

	<-fake.acquireStarted
	closeDone := make(chan error, 1)
	go func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		closeDone <- c.Close(closeCtx)
	}()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for client closed flag")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(fake.acquireBlock)

	if err := <-closeDone; err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err = <-lockDone
	if err == nil || !strings.Contains(err.Error(), "client is closed") {
		t.Fatalf("expected lock to fail with closed client, got %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.releaseCalls != 1 {
		t.Fatalf("expected close-race lock cleanup to issue one release, got %d", fake.releaseCalls)
	}
	if len(fake.releaseTokens) != 1 || fake.releaseTokens[0] != "lk-1" {
		t.Fatalf("unexpected release token cleanup payload: %+v", fake.releaseTokens)
	}
}

func TestLockRejectsHandleWhenSessionInvalidatedBeforeRegistration(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:       "test-token",
		acquireServerID: "srv-b",
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	h, err := c.Lock(ctx, "tenant-1", "res-server-switch", LockOption{})
	if err == nil {
		t.Fatalf("expected lock failure under repeated session invalidation, got handle: %+v", h)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	c.mu.Lock()
	handles := len(c.handles)
	c.mu.Unlock()
	if handles != 0 {
		t.Fatalf("expected no registered handles after invalidation race, got %d", handles)
	}

	fake.mu.Lock()
	releaseCalls := fake.releaseCalls
	fake.mu.Unlock()
	if releaseCalls == 0 {
		t.Fatal("expected best-effort release to run when registration is rejected")
	}
}

func TestConcurrentObserveServerDuringAcquireDoesNotCreateOrphanHandle(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer()
	fake := &fakeLockRPCServer{
		authToken:      "test-token",
		acquireStarted: make(chan struct{}),
		acquireBlock:   make(chan struct{}),
	}
	lockrpcpb.RegisterLockServiceServer(grpcSrv, fake)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()

	c := NewWithConfig("grpc://"+lis.Addr().String(), Config{
		SessionLease:      2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  500 * time.Millisecond,
		LocalTTL:          2 * time.Second,
		AuthToken:         "test-token",
	})
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(closeCtx)
	}()

	if _, err := c.ensureSession(context.Background()); err != nil {
		t.Fatalf("ensure session failed: %v", err)
	}

	lockErr := make(chan error, 1)
	lockHandle := make(chan *LockHandle, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		defer cancel()
		h, err := c.Lock(ctx, "tenant-1", "res-concurrent-server-switch", LockOption{})
		lockHandle <- h
		lockErr <- err
	}()

	select {
	case <-fake.acquireStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire to start")
	}

	c.observeServer("srv-b")
	close(fake.acquireBlock)

	h := <-lockHandle
	err = <-lockErr
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected success or deadline exceeded after concurrent invalidation, got %v", err)
	}

	if err == nil {
		if h == nil {
			t.Fatal("expected non-nil handle on successful lock")
		}
		c.mu.Lock()
		currentSessionID := c.sessionID
		c.mu.Unlock()
		if currentSessionID == "" || h.sessionID != currentSessionID {
			t.Fatalf("expected handle bound to current session, handle session=%q current=%q", h.sessionID, currentSessionID)
		}
		if unlockErr := h.Unlock(context.Background()); unlockErr != nil {
			t.Fatalf("unlock successful handle failed: %v", unlockErr)
		}
		<-h.Done()
	}

	c.mu.Lock()
	currentSessionID := c.sessionID
	for token, handle := range c.handles {
		if handle.sessionID != currentSessionID {
			c.mu.Unlock()
			t.Fatalf("found orphan handle token=%s handleSession=%q currentSession=%q", token, handle.sessionID, currentSessionID)
		}
	}
	handles := len(c.handles)
	c.mu.Unlock()
	if err != nil && handles != 0 {
		t.Fatalf("expected no registered handles after failed lock attempt, got %d", handles)
	}

	fake.mu.Lock()
	releaseCalls := fake.releaseCalls
	fake.mu.Unlock()
	if releaseCalls == 0 {
		t.Fatal("expected release cleanup after rejecting stale handle")
	}
}
