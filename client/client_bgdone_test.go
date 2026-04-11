package client

import (
	"context"
	"testing"
	"time"
)

func chanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestBGDoneGenerationLifecycle(t *testing.T) {
	c := &Client{
		bgDone:              make(chan struct{}),
		handles:             make(map[string]*LockHandle),
		remoteCloseInFlight: make(map[string]chan struct{}),
	}
	close(c.bgDone) // keep same initial state as NewWithConfig

	c.markBGStart()
	first := c.bgDone
	if chanClosed(first) {
		t.Fatal("expected first bgDone generation to be open")
	}

	c.markBGStart()
	if c.bgDone != first {
		t.Fatal("expected same bgDone while workers remain active")
	}

	c.markBGDone()
	if chanClosed(first) {
		t.Fatal("expected bgDone to stay open until bgActive reaches zero")
	}

	c.markBGDone()
	if !chanClosed(first) {
		t.Fatal("expected first bgDone generation to be closed")
	}

	c.markBGStart()
	second := c.bgDone
	if second == first {
		t.Fatal("expected a new bgDone channel for next generation")
	}
	if chanClosed(second) {
		t.Fatal("expected second bgDone generation to be open")
	}
	if !chanClosed(first) {
		t.Fatal("expected previous generation to stay closed")
	}

	c.markBGDone()
	if !chanClosed(second) {
		t.Fatal("expected second bgDone generation to be closed when worker exits")
	}
}

func TestBGDoneOldWaiterReleasedAfterGenerationSwitch(t *testing.T) {
	c := &Client{
		bgDone:              make(chan struct{}),
		handles:             make(map[string]*LockHandle),
		remoteCloseInFlight: make(map[string]chan struct{}),
	}
	close(c.bgDone)

	c.markBGStart()
	first := c.bgDone
	waitDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		waitDone <- waitChannelWithContext(ctx, first)
	}()

	c.markBGDone()
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("expected waiter to be released by first generation close, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first generation waiter")
	}

	c.markBGStart()
	second := c.bgDone
	if second == first {
		t.Fatal("expected new bgDone generation")
	}
	if !chanClosed(first) {
		t.Fatal("expected first generation channel to remain closed")
	}
	if chanClosed(second) {
		t.Fatal("expected second generation channel to remain open")
	}
	c.markBGDone()
}

func TestCloseWaitsForActiveBackgroundWorkers(t *testing.T) {
	c := &Client{
		bgDone:              make(chan struct{}),
		handles:             make(map[string]*LockHandle),
		remoteCloseInFlight: make(map[string]chan struct{}),
	}
	close(c.bgDone)
	c.markBGStart()

	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		closeDone <- c.Close(ctx)
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("expected Close to wait for active background workers, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	c.markBGDone()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("expected close to succeed once background workers finish, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Close to return")
	}
}
