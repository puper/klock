package hierlock

import (
	"context"
	"errors"
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultShardCount = 1024
	activeSpins       = 32
	maxBackoff        = time.Millisecond
)

var errInvalidShardCount = errors.New("hierlock: shard count must be > 0")

type key2 struct {
	p1 string
	p2 string
}

type lockNode struct {
	mu       sync.RWMutex
	refCount int32
	// pendingWriters gates new readers when level-1 writers are waiting.
	pendingWriters int32
}

type lockShard struct {
	mu sync.Mutex
	l1 map[string]*lockNode
	l2 map[key2]*lockNode
}

type Locker interface {
	// LockL1 locks a level-1 key directly.
	// This can be used as a normal key lock when only level-1 is involved.
	LockL1(ctx context.Context, p1 string) (unlock func(), err error)
	// Lock locks a level-2 key under a level-1 key.
	Lock(ctx context.Context, p1, p2 string) (unlock func(), err error)
}

type HierarchicalLocker struct {
	shards []lockShard
}

func New(shardCount int) (*HierarchicalLocker, error) {
	if shardCount == 0 {
		shardCount = defaultShardCount
	}
	if shardCount < 0 {
		return nil, errInvalidShardCount
	}

	h := &HierarchicalLocker{shards: make([]lockShard, shardCount)}
	for i := range h.shards {
		h.shards[i].l1 = make(map[string]*lockNode)
		h.shards[i].l2 = make(map[key2]*lockNode)
	}
	return h, nil
}

func MustNew(shardCount int) *HierarchicalLocker {
	h, err := New(shardCount)
	if err != nil {
		panic(err)
	}
	return h
}

func (h *HierarchicalLocker) LockL1(ctx context.Context, p1 string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := h.shardFor(p1)
	l1 := s.retainL1(p1)
	atomic.AddInt32(&l1.pendingWriters, 1)

	if err := spinUntilLocked(ctx, l1.mu.TryLock); err != nil {
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

func (h *HierarchicalLocker) Lock(ctx context.Context, p1, p2 string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := h.shardFor(p1)
	l1 := s.retainL1(p1)
	if err := spinUntilLockedWithGate(ctx, func() bool {
		return atomic.LoadInt32(&l1.pendingWriters) == 0
	}, l1.mu.TryRLock); err != nil {
		s.releaseL1(p1, l1)
		return nil, err
	}

	k := key2{p1: p1, p2: p2}
	l2 := s.retainL2(k)
	if err := spinUntilLocked(ctx, l2.mu.TryLock); err != nil {
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

// LockPrefix is kept for backward compatibility. Prefer LockL1.
func (h *HierarchicalLocker) LockPrefix(ctx context.Context, p1 string) (func(), error) {
	return h.LockL1(ctx, p1)
}

// LockKey is kept for backward compatibility. Prefer Lock.
func (h *HierarchicalLocker) LockKey(ctx context.Context, p1, p2 string) (func(), error) {
	return h.Lock(ctx, p1, p2)
}

func (h *HierarchicalLocker) shardFor(p1 string) *lockShard {
	if len(h.shards) == 1 {
		return &h.shards[0]
	}
	hash := hashString(p1)
	return &h.shards[int(hash%uint32(len(h.shards)))]
}

func (s *lockShard) retainL1(p1 string) *lockNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.l1[p1]
	if !ok {
		n = &lockNode{}
		s.l1[p1] = n
	}
	atomic.AddInt32(&n.refCount, 1)
	return n
}

func (s *lockShard) retainL2(k key2) *lockNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.l2[k]
	if !ok {
		n = &lockNode{}
		s.l2[k] = n
	}
	atomic.AddInt32(&n.refCount, 1)
	return n
}

func (s *lockShard) releaseL1(p1 string, n *lockNode) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.AddInt32(&n.refCount, -1) == 0 {
		if cur := s.l1[p1]; cur == n {
			delete(s.l1, p1)
		}
	}
}

func (s *lockShard) releaseL2(k key2, n *lockNode) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.AddInt32(&n.refCount, -1) == 0 {
		if cur := s.l2[k]; cur == n {
			delete(s.l2, k)
		}
	}
}

func spinUntilLocked(ctx context.Context, tryLock func() bool) error {
	return spinUntilLockedWithGate(ctx, nil, tryLock)
}

func spinUntilLockedWithGate(ctx context.Context, canTry func() bool, tryLock func() bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	backoff := time.Microsecond
	for attempts := 0; ; attempts++ {
		if canTry != nil && !canTry() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if attempts < activeSpins {
				runtime.Gosched()
				continue
			}
			if !sleepOrDone(ctx, backoff) {
				return ctx.Err()
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		if tryLock() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		if attempts < activeSpins {
			runtime.Gosched()
			continue
		}

		if !sleepOrDone(ctx, backoff) {
			return ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// stats returns the current shard-local node counts (for testing).
func (h *HierarchicalLocker) stats() (l1Count int, l2Count int) {
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.Lock()
		l1Count += len(s.l1)
		l2Count += len(s.l2)
		s.mu.Unlock()
	}
	return
}
