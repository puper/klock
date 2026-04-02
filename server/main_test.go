package main

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/puper/klock/pkg/hierlock"
	"google.golang.org/grpc/metadata"
)

func TestIdempotentAcquireByRequestID(t *testing.T) {
	// 验证：同一 request_id 的 acquire 返回同一 token/fence（幂等）。
	svc := newLockService(hierlock.MustNew(16), 200*time.Millisecond, time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	req := lockRequest{Scope: scopeL2, P1: "tenant-1", P2: "res-1", SessionID: sess.SessionID, RequestID: "req-1"}
	r1, err := svc.acquire(context.Background(), req)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	r2, err := svc.acquire(context.Background(), req)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if r1.Token != r2.Token || r1.Fence != r2.Fence {
		t.Fatalf("expected idempotent acquire, got token/fence (%s,%d) vs (%s,%d)", r1.Token, r1.Fence, r2.Token, r2.Fence)
	}
}

func TestFencingTokenMonotonic(t *testing.T) {
	// 验证：fence 单调递增。
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	r1, err := svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "t", P2: "a", SessionID: sess.SessionID, RequestID: "req-a"})
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if _, err := svc.release(sess.SessionID, r1.Token, "rel-a"); err != nil {
		t.Fatalf("release a: %v", err)
	}

	r2, err := svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "t", P2: "b", SessionID: sess.SessionID, RequestID: "req-b"})
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if r2.Fence <= r1.Fence {
		t.Fatalf("expected monotonic fencing token, got %d then %d", r1.Fence, r2.Fence)
	}
}

func TestSessionExpiryReleasesLocks(t *testing.T) {
	// 验证：session 过期后，关联锁会被回收。
	svc := newLockService(hierlock.MustNew(16), 40*time.Millisecond, time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{LeaseMS: 40})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	_, err = svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "tenant", P2: "res", SessionID: sess.SessionID, RequestID: "req-hold"})
	if err != nil {
		t.Fatalf("acquire hold: %v", err)
	}

	time.Sleep(90 * time.Millisecond)

	_, err = svc.heartbeat(sess.SessionID)
	var ae *apiError
	if !errors.As(err, &ae) || ae.code != errorCodeSessionGone {
		t.Fatalf("expected session gone after expiry, got: %v", err)
	}

	newSess, err := svc.createSession(createSessionRequest{LeaseMS: 100})
	if err != nil {
		t.Fatalf("create second session: %v", err)
	}
	_, err = svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "tenant", P2: "res", SessionID: newSess.SessionID, RequestID: "req-new"})
	if err != nil {
		t.Fatalf("expected lock released after session expiry, got: %v", err)
	}
}

func TestHeartbeatKeepsSessionAlive(t *testing.T) {
	// 验证：heartbeat 成功可延长会话存活。
	svc := newLockService(hierlock.MustNew(16), 50*time.Millisecond, time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{LeaseMS: 50})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	_, err = svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "tenant", P2: "hb", SessionID: sess.SessionID, RequestID: "req-hold"})
	if err != nil {
		t.Fatalf("acquire hold: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	if _, err := svc.heartbeat(sess.SessionID); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	time.Sleep(35 * time.Millisecond)
	_, err = svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "tenant", P2: "other", SessionID: sess.SessionID, RequestID: "req-2"})
	if err != nil {
		t.Fatalf("expected session alive after heartbeat, got: %v", err)
	}
}

func TestReleaseIdempotencyConflictsOnDifferentToken(t *testing.T) {
	// 验证：同 request_id 释放不同 token 会触发幂等冲突。
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	r1, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "a",
		SessionID: sess.SessionID,
		RequestID: "acq-a",
	})
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	r2, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "b",
		SessionID: sess.SessionID,
		RequestID: "acq-b",
	})
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}

	if _, err := svc.release(sess.SessionID, r1.Token, "rel-1"); err != nil {
		t.Fatalf("first release: %v", err)
	}
	_, err = svc.release(sess.SessionID, r2.Token, "rel-1")
	var ae *apiError
	if !errors.As(err, &ae) || ae.code != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected idempotency conflict, got: %v", err)
	}
}

func TestVeryShortLeaseSessionStillExpires(t *testing.T) {
	// 验证：极短 lease 仍能正确过期（覆盖定时器竞态回归）。
	svc := newLockService(hierlock.MustNew(16), 2*time.Millisecond, time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{LeaseMS: 2})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	time.Sleep(25 * time.Millisecond)

	_, err = svc.heartbeat(sess.SessionID)
	var ae *apiError
	if !errors.As(err, &ae) || ae.code != errorCodeSessionGone {
		t.Fatalf("expected short-lease session to expire, got: %v", err)
	}
}

func TestHeartbeatExtendsLeaseWithoutSpuriousExpiry(t *testing.T) {
	// 验证：续约后不会被旧 timer 回调误过期。
	svc := newLockService(hierlock.MustNew(16), 20*time.Millisecond, time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{LeaseMS: 20})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if _, err := svc.heartbeat(sess.SessionID); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	// Wait past the original lease boundary but before the refreshed lease expires.
	time.Sleep(15 * time.Millisecond)
	if _, err := svc.heartbeat(sess.SessionID); err != nil {
		t.Fatalf("session should still be alive after heartbeat extension, got: %v", err)
	}
}

func TestAcquireIdempotencyUsesBoundedEviction(t *testing.T) {
	// 验证：acquire 幂等缓存为有界最旧淘汰。
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	svc.maxIdempotencyEntries = 2
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	r1, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "r1",
		SessionID: sess.SessionID,
		RequestID: "acq-1",
	})
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if _, err := svc.release(sess.SessionID, r1.Token, "rel-1"); err != nil {
		t.Fatalf("release 1: %v", err)
	}

	r2, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "r2",
		SessionID: sess.SessionID,
		RequestID: "acq-2",
	})
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	r3, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "r3",
		SessionID: sess.SessionID,
		RequestID: "acq-3",
	})
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if r2.Token == "" || r3.Token == "" {
		t.Fatal("expected valid tokens")
	}

	// acq-1 should be evicted; reacquire with same request ID should no longer hit cache.
	r1b, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "r1",
		SessionID: sess.SessionID,
		RequestID: "acq-1",
	})
	if err != nil {
		t.Fatalf("reacquire 1: %v", err)
	}
	if r1b.Token == r1.Token {
		t.Fatalf("expected oldest acquire idempotency entry to be evicted")
	}
}

func TestReleaseIdempotencyUsesBoundedEviction(t *testing.T) {
	// 验证：release 幂等缓存为有界最旧淘汰。
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	svc.maxIdempotencyEntries = 2
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	r1, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "a",
		SessionID: sess.SessionID,
		RequestID: "acq-a",
	})
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	r2, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "b",
		SessionID: sess.SessionID,
		RequestID: "acq-b",
	})
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	r3, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant",
		P2:        "c",
		SessionID: sess.SessionID,
		RequestID: "acq-c",
	})
	if err != nil {
		t.Fatalf("acquire c: %v", err)
	}

	if _, err := svc.release(sess.SessionID, r1.Token, "rel-1"); err != nil {
		t.Fatalf("release 1: %v", err)
	}
	if _, err := svc.release(sess.SessionID, r2.Token, "rel-2"); err != nil {
		t.Fatalf("release 2: %v", err)
	}
	if _, err := svc.release(sess.SessionID, r3.Token, "rel-3"); err != nil {
		t.Fatalf("release 3: %v", err)
	}

	// rel-1 should have been evicted; reuse should not be treated idempotent anymore.
	_, err = svc.release(sess.SessionID, r2.Token, "rel-1")
	var ae *apiError
	if !errors.As(err, &ae) || ae.code != "LOCK_NOT_FOUND" {
		t.Fatalf("expected evicted idempotency key to behave as fresh request, got: %v", err)
	}
}

func TestNextLockTokenUsesRandomHexFormat(t *testing.T) {
	// 验证：锁 token 使用随机十六进制格式，且相邻 token 不重复。
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")

	tok1 := svc.nextLockToken()
	tok2 := svc.nextLockToken()
	if tok1 == tok2 {
		t.Fatalf("expected distinct lock tokens, got duplicated token %q", tok1)
	}
	re := regexp.MustCompile(`^lk_[0-9a-f]{32}$`)
	if !re.MatchString(tok1) || !re.MatchString(tok2) {
		t.Fatalf("unexpected token format: tok1=%q tok2=%q", tok1, tok2)
	}
}

func TestDrainForRestartNotifiesWatchers(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	ch, watcherID, err := svc.registerWatcher(sess.SessionID)
	if err != nil {
		t.Fatalf("register watcher: %v", err)
	}
	defer svc.unregisterWatcher(sess.SessionID, watcherID)

	svc.drainForRestart()

	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watcher channel closed without invalidation event")
		}
		if ev.Type != "SESSION_INVALIDATED" {
			t.Fatalf("unexpected event type: %s", ev.Type)
		}
		if ev.Code != "SESSION_GONE" {
			t.Fatalf("unexpected event code: %s", ev.Code)
		}
		if !strings.Contains(ev.Message, "server_restarting") {
			t.Fatalf("unexpected event message: %s", ev.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for restart invalidation event")
	}
}

func TestCreateSessionRejectedWhileDraining(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	svc.drainForRestart()

	_, err := svc.createSession(createSessionRequest{})
	var ae *apiError
	if !errors.As(err, &ae) {
		t.Fatalf("expected apiError, got: %v", err)
	}
	if ae.code != "SERVER_DRAINING" {
		t.Fatalf("expected SERVER_DRAINING, got: %s", ae.code)
	}
}

func TestRegisterWatcherRejectedWhileDraining(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	svc.drainForRestart()

	_, _, err = svc.registerWatcher(sess.SessionID)
	var ae *apiError
	if !errors.As(err, &ae) {
		t.Fatalf("expected apiError, got: %v", err)
	}
	if ae.code != "SERVER_DRAINING" {
		t.Fatalf("expected SERVER_DRAINING, got: %s", ae.code)
	}
}

func TestAuthTokenFromContextBearer(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token-123"))
	got := authTokenFromContext(ctx)
	if got != "token-123" {
		t.Fatalf("unexpected token: %q", got)
	}
}

func TestAuthTokenFromContextRaw(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "token-raw"))
	got := authTokenFromContext(ctx)
	if got != "token-raw" {
		t.Fatalf("unexpected token: %q", got)
	}
}

func TestSubtleConstantTimeMatch(t *testing.T) {
	if !subtleConstantTimeMatch("abc", "abc") {
		t.Fatal("expected equal tokens to match")
	}
	if subtleConstantTimeMatch("abc", "abd") {
		t.Fatal("expected different tokens not to match")
	}
}

func TestRateLimiterAllowAndRefill(t *testing.T) {
	rl := newRateLimiter(1, 1)
	now := time.Now()
	key := "/klock.v1.LockService/Acquire|127.0.0.1:1"
	if !rl.allow(key, now) {
		t.Fatal("expected first request allowed")
	}
	if rl.allow(key, now) {
		t.Fatal("expected second request denied due to burst limit")
	}
	if !rl.allow(key, now.Add(1100*time.Millisecond)) {
		t.Fatal("expected token refill after 1s")
	}
}
