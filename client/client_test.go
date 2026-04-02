package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestLockHandleUnlockAndDone(t *testing.T) {
	// 验证：解锁成功后 Done 会收到 unlocked 事件。
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerServerID, "srv-a")
		w.Header().Set(headerProtocol, protocolVersion)

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-1",
				"lease_ms":         3000,
				"expires_at":       time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339Nano),
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions/sess-1/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-1",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/locks":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":            "lk-1",
				"fence":            1,
				"session_id":       "sess-1",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/locks/lk-1":
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer s.Close()

	c := NewWithConfig(s.URL, s.Client(), Config{
		SessionLease:      300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
	})

	h, err := c.Lock(context.Background(), "tenant-1", "res-1", LockOption{})
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}
	if h.Token == "" || h.Fence == 0 {
		t.Fatalf("invalid handle token/fence: %q/%d", h.Token, h.Fence)
	}

	if err := h.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}

	select {
	case ev, ok := <-h.Done():
		if !ok {
			t.Fatal("done channel closed without event")
		}
		if ev.Type != EventUnlocked {
			t.Fatalf("expected unlocked event, got %s", ev.Type)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected done event")
	}
}

func TestPassiveSessionGoneNotifiesHandle(t *testing.T) {
	// 验证：服务端返回 SESSION_GONE 时，句柄会收到被动失效事件。
	var (
		mu      sync.Mutex
		expired bool
	)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerServerID, "srv-a")
		w.Header().Set(headerProtocol, protocolVersion)

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-1",
				"lease_ms":         3000,
				"expires_at":       time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339Nano),
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions/sess-1/heartbeat":
			mu.Lock()
			isExpired := expired
			mu.Unlock()
			if isExpired {
				w.WriteHeader(http.StatusGone)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":            "session expired: lease_expired",
					"code":             "SESSION_GONE",
					"server_id":        "srv-a",
					"protocol_version": protocolVersion,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-1",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/locks":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":            "lk-1",
				"fence":            1,
				"session_id":       "sess-1",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer s.Close()

	c := NewWithConfig(s.URL, s.Client(), Config{
		SessionLease:      300 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond,
	})

	h, err := c.Lock(context.Background(), "tenant-1", "res-1", LockOption{})
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}

	mu.Lock()
	expired = true
	mu.Unlock()

	select {
	case ev := <-h.Done():
		if ev.Type != EventSessionGone {
			t.Fatalf("expected session gone event, got %s", ev.Type)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("expected passive close notification")
	}
}

func TestHeartbeatTwoFailuresInvalidateLocks(t *testing.T) {
	// 验证：heartbeat 连续失败达到阈值后触发 fail-closed。
	var (
		mu           sync.Mutex
		heartbeatHit int
	)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerServerID, "srv-a")
		w.Header().Set(headerProtocol, protocolVersion)

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-1",
				"lease_ms":         3000,
				"expires_at":       time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339Nano),
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions/sess-1/heartbeat":
			mu.Lock()
			heartbeatHit++
			count := heartbeatHit
			mu.Unlock()
			if count >= 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":            "temporary heartbeat failure",
					"code":             "TEMP_HEARTBEAT_ERROR",
					"server_id":        "srv-a",
					"protocol_version": protocolVersion,
				})
				return
			}
		case r.Method == http.MethodPost && r.URL.Path == "/v1/locks":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":            "lk-2",
				"fence":            2,
				"session_id":       "sess-1",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer s.Close()

	c := NewWithConfig(s.URL, s.Client(), Config{
		SessionLease:      300 * time.Millisecond,
		HeartbeatInterval: 30 * time.Millisecond,
	})

	h, err := c.Lock(context.Background(), "tenant-1", "res-2", LockOption{})
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}

	select {
	case ev := <-h.Done():
		if ev.Type != EventSessionGone {
			t.Fatalf("expected session gone event, got %s", ev.Type)
		}
		if ev.ErrorCode != "HEARTBEAT_CONSECUTIVE_FAILURE" {
			t.Fatalf("expected heartbeat failure code, got %s", ev.ErrorCode)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected lock invalidation after two heartbeat failures")
	}
}

func TestAutoRenewLocalTTLOnHeartbeatSuccess(t *testing.T) {
	// 验证：自动续约句柄会在 heartbeat 成功后刷新本地 TTL。
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerServerID, "srv-a")
		w.Header().Set(headerProtocol, protocolVersion)

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-1",
				"lease_ms":         5000,
				"expires_at":       time.Now().Add(5 * time.Second).UTC().Format(time.RFC3339Nano),
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions/sess-1/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-1",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/locks":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":            "lk-auto",
				"fence":            3,
				"session_id":       "sess-1",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/locks/lk-auto":
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer s.Close()

	c := NewWithConfig(s.URL, s.Client(), Config{
		HeartbeatInterval: 25 * time.Millisecond,
		HeartbeatTimeout:  2 * time.Second,
		LocalTTL:          40 * time.Millisecond,
	})

	h, err := c.Lock(context.Background(), "tenant-1", "res-auto", LockOption{})
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}

	// Without auto-renew, this would have expired after ~40ms.
	select {
	case ev := <-h.Done():
		t.Fatalf("unexpected early done event: %v", ev)
	case <-time.After(120 * time.Millisecond):
	}

	if err := h.Unlock(context.Background()); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}
}

func TestHeartbeatFailureDoesNotStopLocalExpiryLoopAfterSessionReinit(t *testing.T) {
	// 验证：heartbeat 失败导致 session 失效后，重建 session 仍保持本地 TTL 扫描可用。
	var (
		mu             sync.Mutex
		sessionCreates int
	)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerServerID, "srv-a")
		w.Header().Set(headerProtocol, protocolVersion)

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions":
			mu.Lock()
			sessionCreates++
			n := sessionCreates
			mu.Unlock()
			sessionID := "sess-1"
			if n >= 2 {
				sessionID = "sess-2"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       sessionID,
				"lease_ms":         3000,
				"expires_at":       time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339Nano),
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions/sess-1/heartbeat":
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":            "temporary heartbeat failure",
				"code":             "TEMP_HEARTBEAT_ERROR",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions/sess-2/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"session_id":       "sess-2",
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodDelete && (r.URL.Path == "/v1/sessions/sess-1" || r.URL.Path == "/v1/sessions/sess-2"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/locks":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			sid, _ := req["session_id"].(string)
			token := "lk-1"
			if sid == "sess-2" {
				token = "lk-2"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"token":            token,
				"fence":            1,
				"session_id":       sid,
				"server_id":        "srv-a",
				"protocol_version": protocolVersion,
			})
		case r.Method == http.MethodDelete && (r.URL.Path == "/v1/locks/lk-1" || r.URL.Path == "/v1/locks/lk-2"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer s.Close()

	c := NewWithConfig(s.URL, s.Client(), Config{
		SessionLease:       300 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
		LocalSweepInterval: 10 * time.Millisecond,
	})

	h1, err := c.Lock(context.Background(), "tenant-1", "res-1", LockOption{LocalTTL: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("first lock failed: %v", err)
	}

	select {
	case ev := <-h1.Done():
		if ev.Type != EventSessionGone {
			t.Fatalf("expected session gone after heartbeat failures, got %s", ev.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected first handle to fail-closed after heartbeat failures")
	}

	h2, err := c.Lock(context.Background(), "tenant-1", "res-2", LockOption{LocalTTL: 60 * time.Millisecond})
	if err != nil {
		t.Fatalf("second lock failed: %v", err)
	}

	select {
	case ev := <-h2.Done():
		if ev.Type != EventLocalExpired {
			t.Fatalf("expected local TTL expiry on second handle, got %s", ev.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected second handle to expire locally")
	}
}

func TestClosePreventsNewLockAcquisition(t *testing.T) {
	// 验证：Close 后客户端不可复用，新加锁请求会被拒绝。
	c := NewWithConfig("http://127.0.0.1:65535", nil, Config{
		HeartbeatInterval: 20 * time.Millisecond,
	})
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	_, err := c.Lock(context.Background(), "tenant-1", "res-1", LockOption{})
	if err == nil || err.Error() != "client is closed" {
		t.Fatalf("expected closed client error, got: %v", err)
	}
}
