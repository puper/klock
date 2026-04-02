package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/puper/klock/pkg/hierlock"
	"github.com/puper/klock/pkg/lockrpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func newTestGRPCClient(t *testing.T, authToken string, limiter *rateLimiter) (lockrpcpb.LockServiceClient, func()) {
	t.Helper()

	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	metrics := &serverMetrics{}
	svc.metrics = metrics

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcServerAuthUnaryInterceptor(authToken, metrics),
			grpcServerRateUnaryInterceptor(limiter, metrics),
		),
		grpc.ChainStreamInterceptor(
			grpcServerAuthStreamInterceptor(authToken, metrics),
			grpcServerRateStreamInterceptor(limiter, metrics),
		),
	)
	lockrpcpb.RegisterLockServiceServer(s, newGRPCServer(svc))
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	client := lockrpcpb.NewLockServiceClient(conn)
	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}
	return client, cleanup
}

func TestGRPCAuthInterceptorRejectsMissingToken(t *testing.T) {
	client, cleanup := newTestGRPCClient(t, "secret-token", newRateLimiter(100, 100))
	defer cleanup()

	_, err := client.CreateSession(context.Background(), &lockrpcpb.CreateSessionRequest{
		LeaseMs:         1000,
		ProtocolVersion: protocolVersion,
	})
	if err == nil {
		t.Fatal("expected unauthenticated error")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got: %v (%s)", err, status.Code(err))
	}
}

func TestGRPCRateLimiterInterceptor(t *testing.T) {
	client, cleanup := newTestGRPCClient(t, "secret-token", newRateLimiter(1, 1))
	defer cleanup()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer secret-token")
	if _, err := client.CreateSession(ctx, &lockrpcpb.CreateSessionRequest{
		LeaseMs:         1000,
		ProtocolVersion: protocolVersion,
	}); err != nil {
		t.Fatalf("first CreateSession should pass, got: %v", err)
	}

	_, err := client.CreateSession(ctx, &lockrpcpb.CreateSessionRequest{
		LeaseMs:         1000,
		ProtocolVersion: protocolVersion,
	})
	if err == nil {
		t.Fatal("expected rate limit error")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got: %v (%s)", err, status.Code(err))
	}
}
