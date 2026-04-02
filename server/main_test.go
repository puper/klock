package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"klock/pkg/hierlock"
)

func TestIdempotentAcquireByRequestID(t *testing.T) {
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
