package hierlock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

const (
	// defaultShardCount 是默认分片数。分片越多，热点 key 在 map 层面的锁竞争通常越小，
	// 但会增加一些内存占用。
	defaultShardCount = 1024
)

// errInvalidShardCount 表示分片数非法。
var errInvalidShardCount = errors.New("hierlock: shard count must be > 0")

// key2 表示二级锁键 (p1, p2)。
type key2 struct {
	p1 string
	p2 string
}

// lockNode 是实际的锁节点。
//
// 设计说明：
//  1. stateMu 保护节点内部可重入状态；通过等待队列实现 park/unpark。
//  2. refCount 用于生命周期管理；当引用归零后从 shard map 中回收节点。
//  3. waitQ 按到达顺序排队；唤醒策略为“写优先 + 连续读者批量放行”。
type lockNode struct {
	stateMu sync.Mutex

	readers int
	writer  bool
	waitQ   []*nodeWaiter

	refCount int32
}

type waiterKind uint8

const (
	waiterReader waiterKind = iota + 1
	waiterWriter
)

type nodeWaiter struct {
	kind waiterKind
	ch   chan struct{}

	granted bool
}

// lockShard 是分片容器，承载一组 L1/L2 节点 map。
// 每个 shard 拥有独立互斥锁，以降低全局 map 竞争。
type lockShard struct {
	mu sync.Mutex
	l1 map[string]*lockNode
	l2 map[key2]*lockNode
}

// Locker 定义层级锁能力。
type Locker interface {
	// LockL1 locks a level-1 key directly.
	// This can be used as a normal key lock when only level-1 is involved.
	LockL1(ctx context.Context, p1 string) (unlock func(), err error)
	// Lock locks a level-2 key under a level-1 key.
	Lock(ctx context.Context, p1, p2 string) (unlock func(), err error)
}

// HierarchicalLocker 是层级锁实现，内部按 p1 做分片。
type HierarchicalLocker struct {
	shards []lockShard
}

// New 创建层级锁。
//
// shardCount:
// 1. =0 时使用默认分片数。
// 2. <0 返回错误。
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

// MustNew 是 New 的 panic 版本，适合在初始化阶段使用。
func MustNew(shardCount int) *HierarchicalLocker {
	h, err := New(shardCount)
	if err != nil {
		panic(err)
	}
	return h
}

// LockL1 获取一级键的独占锁。
//
// 语义：
// 1. 持有期间，同 p1 下新的 L2 进入会被阻塞。
// 2. 返回的 unlock 幂等，多次调用仅首个生效。
func (h *HierarchicalLocker) LockL1(ctx context.Context, p1 string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := h.shardFor(p1)
	l1 := s.retainL1(p1)
	if err := l1.lockWriter(ctx); err != nil {
		s.releaseL1(p1, l1)
		return nil, err
	}

	var once sync.Once
	unlock := func() {
		once.Do(func() {
			l1.unlockWriter()
			s.releaseL1(p1, l1)
		})
	}
	return unlock, nil
}

// Lock 获取二级键锁。
//
// 获取顺序：
// 1. 先获取对应 L1 的读锁（允许多个不同 L2 并行）。
// 2. 再获取具体 (p1,p2) 的写锁（同二级键互斥）。
//
// 这样保证：
// 1. 同一 L2 互斥；
// 2. 同一 L1 下不同 L2 可并行；
// 3. 与 L1 写锁互斥。
func (h *HierarchicalLocker) Lock(ctx context.Context, p1, p2 string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := h.shardFor(p1)
	l1 := s.retainL1(p1)
	if err := l1.lockReader(ctx); err != nil {
		s.releaseL1(p1, l1)
		return nil, err
	}

	k := key2{p1: p1, p2: p2}
	l2 := s.retainL2(k)
	if err := l2.lockWriter(ctx); err != nil {
		s.releaseL2(k, l2)
		l1.unlockReader()
		s.releaseL1(p1, l1)
		return nil, err
	}

	var once sync.Once
	unlock := func() {
		once.Do(func() {
			l2.unlockWriter()
			s.releaseL2(k, l2)

			l1.unlockReader()
			s.releaseL1(p1, l1)
		})
	}
	return unlock, nil
}

// shardFor 根据 p1 计算分片，保证同一 p1 总是落在同一 shard。
func (h *HierarchicalLocker) shardFor(p1 string) *lockShard {
	if len(h.shards) == 1 {
		return &h.shards[0]
	}
	hash := hashString(p1)
	return &h.shards[int(hash%uint32(len(h.shards)))]
}

// retainL1 获取或创建 L1 节点，并增加引用计数。
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

// retainL2 获取或创建 L2 节点，并增加引用计数。
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

// releaseL1 释放一次 L1 引用；引用归零时从 map 移除。
func (s *lockShard) releaseL1(p1 string, n *lockNode) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.AddInt32(&n.refCount, -1) == 0 {
		if cur := s.l1[p1]; cur == n {
			delete(s.l1, p1)
		}
	}
}

// releaseL2 释放一次 L2 引用；引用归零时从 map 移除。
func (s *lockShard) releaseL2(k key2, n *lockNode) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.AddInt32(&n.refCount, -1) == 0 {
		if cur := s.l2[k]; cur == n {
			delete(s.l2, k)
		}
	}
}

func (n *lockNode) lockReader(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.stateMu.Lock()
	if err := ctx.Err(); err != nil {
		n.stateMu.Unlock()
		return err
	}
	if !n.writer && len(n.waitQ) == 0 {
		n.readers++
		n.stateMu.Unlock()
		return nil
	}
	w := &nodeWaiter{kind: waiterReader, ch: make(chan struct{})}
	n.waitQ = append(n.waitQ, w)
	n.stateMu.Unlock()

	return n.waitForGrant(ctx, w)
}

func (n *lockNode) unlockReader() {
	n.stateMu.Lock()
	n.readers--
	if n.readers == 0 {
		n.wakeWaitersLocked()
	}
	n.stateMu.Unlock()
}

func (n *lockNode) lockWriter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.stateMu.Lock()
	if err := ctx.Err(); err != nil {
		n.stateMu.Unlock()
		return err
	}
	if !n.writer && n.readers == 0 && len(n.waitQ) == 0 {
		n.writer = true
		n.stateMu.Unlock()
		return nil
	}
	w := &nodeWaiter{kind: waiterWriter, ch: make(chan struct{})}
	n.waitQ = append(n.waitQ, w)
	n.stateMu.Unlock()

	return n.waitForGrant(ctx, w)
}

func (n *lockNode) unlockWriter() {
	n.stateMu.Lock()
	n.writer = false
	n.wakeWaitersLocked()
	n.stateMu.Unlock()
}

func (n *lockNode) waitForGrant(ctx context.Context, w *nodeWaiter) error {
	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		n.stateMu.Lock()
		if w.granted {
			n.stateMu.Unlock()
			return nil
		}
		if n.removeWaiterLocked(w) {
			n.wakeWaitersLocked()
		}
		n.stateMu.Unlock()
		return ctx.Err()
	}
}

func (n *lockNode) removeWaiterLocked(target *nodeWaiter) bool {
	for i := range n.waitQ {
		if n.waitQ[i] == target {
			copy(n.waitQ[i:], n.waitQ[i+1:])
			n.waitQ[len(n.waitQ)-1] = nil
			n.waitQ = n.waitQ[:len(n.waitQ)-1]
			return true
		}
	}
	return false
}

func (n *lockNode) wakeWaitersLocked() {
	if n.writer || n.readers > 0 || len(n.waitQ) == 0 {
		return
	}

	first := n.waitQ[0]
	if first.kind == waiterWriter {
		n.waitQ[0] = nil
		n.waitQ = n.waitQ[1:]
		first.granted = true
		n.writer = true
		close(first.ch)
		return
	}

	idx := 0
	for idx < len(n.waitQ) && n.waitQ[idx].kind == waiterReader {
		r := n.waitQ[idx]
		r.granted = true
		n.readers++
		close(r.ch)
		idx++
	}
	for i := 0; i < idx; i++ {
		n.waitQ[i] = nil
	}
	n.waitQ = n.waitQ[idx:]
}

// hashString 使用 FNV-1a 计算字符串哈希，用于分片定位。
func hashString(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
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
