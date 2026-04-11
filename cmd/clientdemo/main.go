package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/puper/klock/client"
)

func main() {
	addr := flag.String("addr", "grpcs://klock.xxxx.com:443", "klock grpc address")
	token := flag.String("token", "", "auth token")
	runBadToken := flag.Bool("run-bad-token", true, "run auth failure scenario with a wrong token")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("client demo start addr=%s", *addr)

	c, err := newClient(*addr, *token)
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer closeClient(c)

	must(runBasicLockUnlock(c))
	must(runContentionTimeout(c))
	must(runRetryAfterRelease(c))
	must(runLocalTTLExpiry(c))
	if *runBadToken {
		must(runAuthFailureScenario(*addr, *token))
	}

	log.Printf("client demo done")
}

func newClient(addr, token string) (*client.Client, error) {
	c := client.NewWithConfig(addr, client.Config{
		SessionLease:      8 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		HeartbeatTimeout:  2 * time.Second,
		LocalTTL:          3 * time.Second,
		ServerLeaseBuffer: 4 * time.Second,
		AuthToken:         token,
	})

	const maxAttempts = 4
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		h, err := c.Lock(ctx, "demo-health", "ping", client.LockOption{
			Timeout:        1500 * time.Millisecond,
			AttemptTimeout: 500 * time.Millisecond,
		})
		cancel()
		if err == nil {
			_ = h.Unlock(context.Background())
			<-h.Done()
			// best effort cleanup through Close
			return c, nil
		}

		lastErr = err
		if attempt < maxAttempts {
			backoff := time.Duration(attempt) * 300 * time.Millisecond
			log.Printf("health check attempt %d/%d failed: %v (retry in %s)", attempt, maxAttempts, err, backoff)
			time.Sleep(backoff)
		}
	}

	return nil, fmt.Errorf("health check failed after %d attempts: %w", maxAttempts, lastErr)
}

func closeClient(c *client.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		log.Printf("close client warning: %v", err)
	}
}

func must(err error) {
	if err != nil {
		log.Printf("demo failed: %v", err)
		os.Exit(1)
	}
}

func runBasicLockUnlock(c *client.Client) error {
	log.Printf("[S1] basic lock/unlock start")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h, err := c.Lock(ctx, "tenant-demo", "res-basic", client.LockOption{
		Timeout:        3 * time.Second,
		AttemptTimeout: 800 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("S1 lock failed: %w", err)
	}
	log.Printf("[S1] lock acquired token=%s fence=%d", h.Token, h.Fence)
	time.Sleep(time.Second * 5)
	if err := h.Unlock(context.Background()); err != nil {
		return fmt.Errorf("S1 unlock failed: %w", err)
	}
	ev := <-h.Done()
	log.Printf("[S1] done event type=%s code=%s msg=%s", ev.Type, ev.ErrorCode, ev.Message)
	return nil
}

func runContentionTimeout(c *client.Client) error {
	log.Printf("[S2] contention timeout start")
	h1, err := c.Lock(context.Background(), "tenant-demo", "res-hot", client.LockOption{
		Timeout:        1500 * time.Millisecond,
		AttemptTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("S2 first lock failed: %w", err)
	}
	log.Printf("[S2] first holder token=%s", h1.Token)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel2()
	_, err = c.Lock(ctx2, "tenant-demo", "res-hot", client.LockOption{
		Timeout:        500 * time.Millisecond,
		AttemptTimeout: 200 * time.Millisecond,
	})
	if err == nil {
		_ = h1.Unlock(context.Background())
		return fmt.Errorf("S2 expected timeout under contention, got success")
	}
	log.Printf("[S2] second lock expected failure: %v", err)

	if err := h1.Unlock(context.Background()); err != nil {
		if !isSessionGone(err) {
			return fmt.Errorf("S2 release first holder failed: %w", err)
		}
		log.Printf("[S2] first holder already invalidated by fail-closed path: %v", err)
	}
	select {
	case ev := <-h1.Done():
		log.Printf("[S2] first holder done event type=%s code=%s msg=%s", ev.Type, ev.ErrorCode, ev.Message)
	case <-time.After(2 * time.Second):
		return fmt.Errorf("S2 timeout waiting first holder done event")
	}
	return nil
}

func runRetryAfterRelease(c *client.Client) error {
	log.Printf("[S3] retry after release start")
	h, err := c.Lock(context.Background(), "tenant-demo", "res-retry", client.LockOption{
		Timeout:        1200 * time.Millisecond,
		AttemptTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("S3 first lock failed: %w", err)
	}
	log.Printf("[S3] lock acquired token=%s", h.Token)

	if err := h.Unlock(context.Background()); err != nil {
		return fmt.Errorf("S3 unlock failed: %w", err)
	}
	<-h.Done()

	h2, err := c.Lock(context.Background(), "tenant-demo", "res-retry", client.LockOption{
		Timeout:        1200 * time.Millisecond,
		AttemptTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("S3 reacquire failed: %w", err)
	}
	log.Printf("[S3] reacquired token=%s fence=%d", h2.Token, h2.Fence)
	_ = h2.Unlock(context.Background())
	<-h2.Done()
	return nil
}

func runLocalTTLExpiry(c *client.Client) error {
	log.Printf("[S4] local ttl expiry start")
	h, err := c.Lock(context.Background(), "tenant-demo", "res-ttl", client.LockOption{
		Timeout:        1200 * time.Millisecond,
		AttemptTimeout: 500 * time.Millisecond,
		LocalTTL:       300 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("S4 lock failed: %w", err)
	}
	log.Printf("[S4] lock acquired token=%s; waiting for local ttl expire", h.Token)

	select {
	case ev := <-h.Done():
		log.Printf("[S4] done event type=%s code=%s msg=%s", ev.Type, ev.ErrorCode, ev.Message)
		if ev.Type != client.EventLocalExpired {
			return fmt.Errorf("S4 expected local_expired, got %s", ev.Type)
		}
	case <-time.After(2 * time.Second):
		return fmt.Errorf("S4 timeout waiting local ttl event")
	}
	return nil
}

func runAuthFailureScenario(addr, token string) error {
	log.Printf("[S5] auth failure scenario start")
	if token == "" {
		log.Printf("[S5] skip auth failure scenario because -token is empty (server may not enforce auth)")
		return nil
	}

	bad := client.NewWithConfig(addr, client.Config{
		HeartbeatInterval: 500 * time.Millisecond,
		HeartbeatTimeout:  time.Second,
		LocalTTL:          time.Second,
		AuthToken:         token + "-wrong",
	})
	defer closeClient(bad)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_, err := bad.Lock(ctx, "tenant-demo", "res-auth", client.LockOption{
		Timeout:        1200 * time.Millisecond,
		AttemptTimeout: 500 * time.Millisecond,
	})
	if err == nil {
		return fmt.Errorf("S5 expected auth failure, got success")
	}
	log.Printf("[S5] expected auth failure: %v", err)
	return nil
}

func isSessionGone(err error) bool {
	var ae *client.APIError
	if !errors.As(err, &ae) {
		return false
	}
	return ae.Code == "SESSION_GONE" || ae.Code == "SESSION_NOT_FOUND"
}
