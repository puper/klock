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

	mu               sync.Mutex
	watchers         []chan *lockrpcpb.ServerEvent
	authToken        string
	releaseFailFor   int
	releaseCalls     int
	releaseRequestID []string
	failAcquireP2    string
	failAcquireCode  codes.Code
	acquireTimeoutMS []int64
	acquireDeadlines []time.Duration
	acquireDelay     time.Duration
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
