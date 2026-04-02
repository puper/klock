package client

import (
	"context"
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
	failAcquireP2              string
	failAcquireCode            codes.Code
	acquireTimeoutMS           []int64
	acquireDeadlines           []time.Duration
	acquireDelay               time.Duration
	createSessionDelay         time.Duration
	watchBreakAfterEstablished bool
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
	delay := f.createSessionDelay
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
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
	f.acquireTimeoutMS = append(f.acquireTimeoutMS, req.TimeoutMs)
	if dl, ok := ctx.Deadline(); ok {
		f.acquireDeadlines = append(f.acquireDeadlines, time.Until(dl))
	}
	delay := f.acquireDelay
	f.mu.Unlock()
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
	return &lockrpcpb.LockResponse{
		Token:           "lk-1",
		Fence:           1,
		SessionId:       "sess-1",
		ServerId:        "srv-a",
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
	f.mu.Unlock()
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
