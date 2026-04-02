package hierlock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLockPrefixBlocksLockKey(t *testing.T) {
	// 验证：持有 L1 锁时，同前缀下 L2 申请会被阻塞直到超时。
	l := MustNew(16)
	ctx := context.Background()

	unlockPrefix, err := l.LockL1(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("lock prefix failed: %v", err)
	}
	defer unlockPrefix()

	childCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()

	_, err = l.Lock(childCtx, "tenant-1", "res-1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline, got %v", err)
	}
}

func TestLockKeyMutualExclusionOnSameKey(t *testing.T) {
	// 验证：同一 (p1,p2) 必须互斥。
	l := MustNew(16)
	ctx := context.Background()

	unlockFirst, err := l.Lock(ctx, "tenant-1", "res-1")
	if err != nil {
		t.Fatalf("first lock failed: %v", err)
	}

	blocked := make(chan struct{})
	released := make(chan struct{})
	var secondUnlock func()

	go func() {
		defer close(released)
		u, e := l.Lock(ctx, "tenant-1", "res-1")
		if e != nil {
			t.Errorf("second lock failed: %v", e)
			return
		}
		secondUnlock = u
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("second lock should be blocked on same key")
	case <-time.After(30 * time.Millisecond):
	}

	unlockFirst()

	select {
	case <-blocked:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second lock did not acquire after unlock")
	}

	if secondUnlock != nil {
		secondUnlock()
	}
	<-released
}

func TestLockKeyDifferentKeysCanProceed(t *testing.T) {
	// 验证：同一 p1 下不同 p2 可以并行。
	l := MustNew(16)
	ctx := context.Background()

	unlockFirst, err := l.Lock(ctx, "tenant-1", "res-1")
	if err != nil {
		t.Fatalf("first lock failed: %v", err)
	}
	defer unlockFirst()

	done := make(chan struct{})
	go func() {
		defer close(done)
		u, e := l.Lock(ctx, "tenant-1", "res-2")
		if e != nil {
			t.Errorf("second lock failed: %v", e)
			return
		}
		u()
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("different key under same prefix should not block")
	}
}

func TestTimeoutRollbackAndReclaim(t *testing.T) {
	// 验证：超时失败路径会正确回滚并回收节点，不残留引用。
	l := MustNew(16)
	ctx := context.Background()

	unlockPrefix, err := l.LockL1(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("lock prefix failed: %v", err)
	}

	childCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()

	_, err = l.Lock(childCtx, "tenant-1", "res-1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline, got %v", err)
	}

	unlockPrefix()

	// Wait for deferred cleanups to complete under contention.
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		l1, l2 := l.stats()
		if l1 == 0 && l2 == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected zero nodes after cleanup, got l1=%d l2=%d", l1, l2)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUnlockIsIdempotent(t *testing.T) {
	// 验证：unlock 幂等，多次调用只生效一次。
	l := MustNew(1)
	ctx := context.Background()

	unlock, err := l.Lock(ctx, "tenant-1", "res-1")
	if err != nil {
		t.Fatalf("lock failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		unlock()
	}()
	go func() {
		defer wg.Done()
		unlock()
	}()
	wg.Wait()

	l1, l2 := l.stats()
	if l1 != 0 || l2 != 0 {
		t.Fatalf("expected zero nodes after unlock, got l1=%d l2=%d", l1, l2)
	}
}

func TestPendingWriterBlocksNewReaders(t *testing.T) {
	// 验证：有 L1 写者等待时，新 L2 读者会被门控阻塞。
	l := MustNew(16)
	ctx := context.Background()

	// Hold one L2 lock so a subsequent L1 writer has to wait.
	holdUnlock, err := l.Lock(ctx, "tenant-1", "res-hold")
	if err != nil {
		t.Fatalf("hold lock failed: %v", err)
	}
	defer holdUnlock()

	writerStarted := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		close(writerStarted)
		wctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		u, e := l.LockL1(wctx, "tenant-1")
		if e == nil {
			u()
		}
		writerDone <- e
	}()

	<-writerStarted
	time.Sleep(10 * time.Millisecond)

	// While writer is pending, new readers should be gated.
	readerCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = l.Lock(readerCtx, "tenant-1", "res-new")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected new reader to be blocked by pending writer, got %v", err)
	}

	holdUnlock()

	if err := <-writerDone; err != nil {
		t.Fatalf("writer should acquire after existing reader unlocks, got %v", err)
	}
}
