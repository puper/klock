package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/puper/klock/pkg/hierlock"
	"github.com/puper/klock/pkg/lockrpcpb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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

func TestAcquireIdempotencyConflictOnDifferentRequestFingerprint(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), 200*time.Millisecond, time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	_, err = svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant-1",
		P2:        "res-1",
		SessionID: sess.SessionID,
		RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	_, err = svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL2,
		P1:        "tenant-1",
		P2:        "res-2",
		SessionID: sess.SessionID,
		RequestID: "req-1",
	})
	var ae *apiError
	if !errors.As(err, &ae) || ae.code != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected idempotency conflict, got: %v", err)
	}
}

func TestAcquireIdempotencyL1IgnoresP2InFingerprint(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), 200*time.Millisecond, time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	r1, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL1,
		P1:        "tenant-1",
		P2:        "ignored-a",
		SessionID: sess.SessionID,
		RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	r2, err := svc.acquire(context.Background(), lockRequest{
		Scope:     scopeL1,
		P1:        "tenant-1",
		P2:        "ignored-b",
		SessionID: sess.SessionID,
		RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if r1.Token != r2.Token || r1.Fence != r2.Fence {
		t.Fatalf("expected idempotent acquire for l1 with varying p2, got token/fence (%s,%d) vs (%s,%d)", r1.Token, r1.Fence, r2.Token, r2.Fence)
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

func TestAcquireWaitTracksHeartbeatExtendedSessionLifetime(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), 50*time.Millisecond, time.Second, "srv-test")
	holder, err := svc.createSession(createSessionRequest{LeaseMS: 200})
	if err != nil {
		t.Fatalf("create holder session: %v", err)
	}
	waiter, err := svc.createSession(createSessionRequest{LeaseMS: 50})
	if err != nil {
		t.Fatalf("create waiter session: %v", err)
	}

	holdResp, err := svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "tenant", P2: "res", SessionID: holder.SessionID, RequestID: "hold-1"})
	if err != nil {
		t.Fatalf("holder acquire: %v", err)
	}

	resultCh := make(chan error, 1)
	go func() {
		resp, err := svc.acquire(context.Background(), lockRequest{Scope: scopeL2, P1: "tenant", P2: "res", SessionID: waiter.SessionID, RequestID: "wait-1"})
		if err == nil {
			_, err = svc.release(waiter.SessionID, resp.Token, "wait-rel-1")
		}
		resultCh <- err
	}()

	time.Sleep(30 * time.Millisecond)
	if _, err := svc.heartbeat(waiter.SessionID); err != nil {
		t.Fatalf("waiter heartbeat: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := svc.release(holder.SessionID, holdResp.Token, "hold-rel-1"); err != nil {
		t.Fatalf("holder release: %v", err)
	}

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("expected waiting acquire to survive heartbeat-based lease extension, got: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for acquire after heartbeat-based lease extension")
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

func TestEvictOldestAcquireCompactsOrderQueue(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	st := &sessionState{
		acquireByRequest: make(map[string]acquireIdempotentEntry),
		acquireAt:        make(map[string]time.Time),
		acquireOrder:     make([]string, 0, 128),
	}
	for i := 0; i < 96; i++ {
		id := fmt.Sprintf("acq-%d", i)
		st.acquireByRequest[id] = acquireIdempotentEntry{resp: lockResponse{Token: id}}
		st.acquireAt[id] = time.Now()
		st.acquireOrder = append(st.acquireOrder, id)
	}
	for i := 0; i < 64; i++ {
		svc.evictOldestAcquireLocked(st)
	}
	if st.acquireHead != 0 || len(st.acquireOrder) != 32 {
		t.Fatalf("expected acquire order queue compaction after repeated evictions, head=%d len=%d", st.acquireHead, len(st.acquireOrder))
	}
}

func TestEvictOldestReleaseCompactsOrderQueue(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	st := &sessionState{
		releaseByRequest: make(map[string]string),
		releaseAt:        make(map[string]time.Time),
		releaseOrder:     make([]string, 0, 128),
	}
	for i := 0; i < 96; i++ {
		id := fmt.Sprintf("rel-%d", i)
		st.releaseByRequest[id] = id
		st.releaseAt[id] = time.Now()
		st.releaseOrder = append(st.releaseOrder, id)
	}
	for i := 0; i < 64; i++ {
		svc.evictOldestReleaseLocked(st)
	}
	if st.releaseHead != 0 || len(st.releaseOrder) != 32 {
		t.Fatalf("expected release order queue compaction after repeated evictions, head=%d len=%d", st.releaseHead, len(st.releaseOrder))
	}
}

func TestAcquireHotspotSingleKeyProgresses(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), 2*time.Second, 5*time.Second, "srv-test")
	const contenders = 12
	sessions := make([]string, 0, contenders)
	for i := 0; i < contenders; i++ {
		sess, err := svc.createSession(createSessionRequest{LeaseMS: 2000})
		if err != nil {
			t.Fatalf("create session %d: %v", i, err)
		}
		sessions = append(sessions, sess.SessionID)
	}

	start := make(chan struct{})
	errCh := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		sessionID := sessions[i]
		requestID := fmt.Sprintf("hot-acq-%d", i)
		releaseID := fmt.Sprintf("hot-rel-%d", i)
		go func(sessionID, requestID, releaseID string) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := svc.acquire(ctx, lockRequest{Scope: scopeL2, P1: "tenant-hot", P2: "res-hot", SessionID: sessionID, RequestID: requestID})
			if err != nil {
				errCh <- err
				return
			}
			time.Sleep(5 * time.Millisecond)
			_, err = svc.release(sessionID, resp.Token, releaseID)
			errCh <- err
		}(sessionID, requestID, releaseID)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("expected hotspot contenders to serialize successfully, got: %v", err)
		}
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

func TestExpireSessionRecordsWatcherEventDropWhenChannelFull(t *testing.T) {
	svc := newLockService(hierlock.MustNew(16), time.Second, 5*time.Second, "srv-test")
	sess, err := svc.createSession(createSessionRequest{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	_, watcherID, err := svc.registerWatcher(sess.SessionID)
	if err != nil {
		t.Fatalf("register watcher: %v", err)
	}

	svc.mu.Lock()
	st := svc.sessions[sess.SessionID]
	ch := st.watchers[watcherID]
	for i := 0; i < cap(ch); i++ {
		ch <- &lockrpcpb.ServerEvent{Type: "FILL"}
	}
	svc.mu.Unlock()

	if !svc.expireSession(sess.SessionID, "test_drop") {
		t.Fatal("expected session to expire")
	}
	if got := svc.metrics.watcherEventDrops.Load(); got != 1 {
		t.Fatalf("expected watcher event drop metric to be 1, got %d", got)
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
	if subtleConstantTimeMatch("abc", "abcd") {
		t.Fatal("expected different-length tokens not to match")
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

func TestPeerKeyFromContextUsesHostOnly(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 43210},
	})
	got := peerKeyFromContext(ctx)
	if got != "127.0.0.1" {
		t.Fatalf("expected host-only key, got: %q", got)
	}
}
