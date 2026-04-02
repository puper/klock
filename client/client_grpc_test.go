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

	mu        sync.Mutex
	watchers  []chan *lockrpcpb.ServerEvent
	authToken string
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

func (f *fakeLockRPCServer) Acquire(ctx context.Context, _ *lockrpcpb.LockRequest) (*lockrpcpb.LockResponse, error) {
	if err := f.requireAuth(ctx); err != nil {
		return nil, err
	}
	return &lockrpcpb.LockResponse{
		Token:           "lk-1",
		Fence:           1,
		SessionId:       "sess-1",
		ServerId:        "srv-a",
		ProtocolVersion: protocolVersion,
	}, nil
}

func (f *fakeLockRPCServer) Release(ctx context.Context, _ *lockrpcpb.ReleaseRequest) (*lockrpcpb.ReleaseResponse, error) {
	if err := f.requireAuth(ctx); err != nil {
		return nil, err
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
