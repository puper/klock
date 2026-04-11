package hierlock

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	benchSpinActiveSpins = 32
	benchSpinMaxBackoff  = time.Millisecond
)

type lockerCtor struct {
	name string
	new  func(shards int) Locker
}

func benchmarkLockers() []lockerCtor {
	return []lockerCtor{
		{
			name: "blocking",
			new: func(shards int) Locker {
				return MustNew(shards)
			},
		},
		{
			name: "spin",
			new: func(shards int) Locker {
				return mustNewSpin(shards)
			},
		},
	}
}

func BenchmarkLocker_L2HotKey(b *testing.B) {
	ctors := benchmarkLockers()
	ctx := context.Background()
	for _, c := range ctors {
		c := c
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			l := c.new(16)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u, err := l.Lock(ctx, "tenant-hot", "res-hot")
					if err != nil {
						b.Fatalf("lock failed: %v", err)
					}
					u()
				}
			})
		})
	}
}

func BenchmarkLocker_L2Keyspace1024(b *testing.B) {
	ctors := benchmarkLockers()
	ctx := context.Background()
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	for _, c := range ctors {
		c := c
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			l := c.new(16)
			var seq uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := atomic.AddUint64(&seq, 1)
					u, err := l.Lock(ctx, "tenant-hot", keys[idx%uint64(len(keys))])
					if err != nil {
						b.Fatalf("lock failed: %v", err)
					}
					u()
				}
			})
		})
	}
}

func BenchmarkLocker_MixedL1WriterL2Readers_W5(b *testing.B) {
	benchmarkLockerMixed(b, 5, 1024)
}

func BenchmarkLocker_MixedL1WriterL2Readers_W20(b *testing.B) {
	benchmarkLockerMixed(b, 20, 1024)
}

func benchmarkLockerMixed(b *testing.B, writerPercent int, readerKeyspace int) {
	ctors := benchmarkLockers()
	ctx := context.Background()
	keys := make([]string, readerKeyspace)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}

	for _, c := range ctors {
		c := c
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			l := c.new(16)
			var seq uint64

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := atomic.AddUint64(&seq, 1)
					if int(n%100) < writerPercent {
						u, err := l.LockL1(ctx, "tenant-mixed")
						if err != nil {
							b.Fatalf("lock l1 failed: %v", err)
						}
						u()
						continue
					}

					u, err := l.Lock(ctx, "tenant-mixed", keys[n%uint64(len(keys))])
					if err != nil {
						b.Fatalf("lock l2 failed: %v", err)
					}
					u()
				}
			})
		})
	}
}

type spinNode struct {
	mu             sync.RWMutex
	refCount       int32
	pendingWriters int32
}

type spinShard struct {
	mu sync.Mutex
	l1 map[string]*spinNode
	l2 map[key2]*spinNode
}

type spinHierarchicalLocker struct {
	shards []spinShard
}

func newSpin(shardCount int) (*spinHierarchicalLocker, error) {
	if shardCount == 0 {
		shardCount = defaultShardCount
	}
	if shardCount < 0 {
		return nil, errInvalidShardCount
	}
	h := &spinHierarchicalLocker{shards: make([]spinShard, shardCount)}
	for i := range h.shards {
		h.shards[i].l1 = make(map[string]*spinNode)
		h.shards[i].l2 = make(map[key2]*spinNode)
	}
	return h, nil
}

func mustNewSpin(shardCount int) *spinHierarchicalLocker {
	h, err := newSpin(shardCount)
	if err != nil {
		panic(err)
	}
	return h
}

func (h *spinHierarchicalLocker) LockL1(ctx context.Context, p1 string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := h.shardFor(p1)
	l1 := s.retainL1(p1)
	atomic.AddInt32(&l1.pendingWriters, 1)
	if err := spinWaitUntilLocked(ctx, l1.mu.TryLock); err != nil {
		atomic.AddInt32(&l1.pendingWriters, -1)
		s.releaseL1(p1, l1)
		return nil, err
	}
	var once sync.Once
	unlock := func() {
		once.Do(func() {
			l1.mu.Unlock()
			atomic.AddInt32(&l1.pendingWriters, -1)
			s.releaseL1(p1, l1)
		})
	}
	return unlock, nil
}

func (h *spinHierarchicalLocker) Lock(ctx context.Context, p1, p2 string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := h.shardFor(p1)
	l1 := s.retainL1(p1)
	if err := spinWaitUntilLockedWithGate(ctx, func() bool {
		return atomic.LoadInt32(&l1.pendingWriters) == 0
	}, l1.mu.TryRLock); err != nil {
		s.releaseL1(p1, l1)
		return nil, err
	}

	k := key2{p1: p1, p2: p2}
	l2 := s.retainL2(k)
	if err := spinWaitUntilLocked(ctx, l2.mu.TryLock); err != nil {
		s.releaseL2(k, l2)
		l1.mu.RUnlock()
		s.releaseL1(p1, l1)
		return nil, err
	}

	var once sync.Once
	unlock := func() {
		once.Do(func() {
			l2.mu.Unlock()
			s.releaseL2(k, l2)
			l1.mu.RUnlock()
			s.releaseL1(p1, l1)
		})
	}
	return unlock, nil
}

func (h *spinHierarchicalLocker) shardFor(p1 string) *spinShard {
	if len(h.shards) == 1 {
		return &h.shards[0]
	}
	hash := hashString(p1)
	return &h.shards[int(hash%uint32(len(h.shards)))]
}

func (s *spinShard) retainL1(p1 string) *spinNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.l1[p1]
	if !ok {
		n = &spinNode{}
		s.l1[p1] = n
	}
	atomic.AddInt32(&n.refCount, 1)
	return n
}

func (s *spinShard) retainL2(k key2) *spinNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.l2[k]
	if !ok {
		n = &spinNode{}
		s.l2[k] = n
	}
	atomic.AddInt32(&n.refCount, 1)
	return n
}

func (s *spinShard) releaseL1(p1 string, n *spinNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if atomic.AddInt32(&n.refCount, -1) == 0 {
		if cur := s.l1[p1]; cur == n {
			delete(s.l1, p1)
		}
	}
}

func (s *spinShard) releaseL2(k key2, n *spinNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if atomic.AddInt32(&n.refCount, -1) == 0 {
		if cur := s.l2[k]; cur == n {
			delete(s.l2, k)
		}
	}
}

func spinWaitUntilLocked(ctx context.Context, tryLock func() bool) error {
	return spinWaitUntilLockedWithGate(ctx, nil, tryLock)
}

func spinWaitUntilLockedWithGate(ctx context.Context, canTry func() bool, tryLock func() bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	backoff := time.Microsecond
	gateBlocked := false
	for attempts := 0; ; attempts++ {
		if canTry != nil && !canTry() {
			gateBlocked = true
			if err := ctx.Err(); err != nil {
				return err
			}
			if attempts < benchSpinActiveSpins {
				runtime.Gosched()
				continue
			}
			if !benchSleepOrDone(ctx, backoff) {
				return ctx.Err()
			}
			if backoff < benchSpinMaxBackoff {
				backoff *= 2
				if backoff > benchSpinMaxBackoff {
					backoff = benchSpinMaxBackoff
				}
			}
			continue
		}
		if gateBlocked {
			attempts = 0
			backoff = time.Microsecond
			gateBlocked = false
		}

		if tryLock() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		if attempts < benchSpinActiveSpins {
			runtime.Gosched()
			continue
		}
		if !benchSleepOrDone(ctx, backoff) {
			return ctx.Err()
		}
		if backoff < benchSpinMaxBackoff {
			backoff *= 2
			if backoff > benchSpinMaxBackoff {
				backoff = benchSpinMaxBackoff
			}
		}
	}
}

func benchSleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
