package pool

// T12：可插拔 idle 容器的测试。
//
//   - 容器级：Add/Pop/Remove/Len、FIFO/LIFO 顺序、任意位置 O(1) 删除、
//     删除/弹出后不残留引用、零分配；
//   - 池级：默认容器是 FIFO；stack/fifo 两种实现接入真实 Pool 后行为一致
//     （任务恰好执行一次、idle 数量正确、关闭正常）。

import (
	"context"
	"sync"
	"testing"
	"time"
)

// containerWorkers 返回容器当前持有的 worker（按容器顺序）。
// 对 stack 只返回有效区间；对 fifo 沿链表遍历。
func containerWorkers(c idleContainer) []*worker {
	switch v := c.(type) {
	case *stackIdle:
		out := make([]*worker, len(v.items))
		copy(out, v.items)
		return out
	case *fifoIdle:
		var out []*worker
		seen := make(map[*worker]bool, v.n)
		for w := v.head; w != nil; w = w.idleNext {
			if seen[w] {
				return out // 链表成环；由 Len 断言暴露
			}
			seen[w] = true
			out = append(out, w)
		}
		return out
	default:
		return nil
	}
}

func assertNoWorkerRef(t *testing.T, c idleContainer, w *worker) {
	t.Helper()
	for i, got := range containerWorkers(c) {
		if got == w {
			t.Fatalf("container still references removed/popped worker at index %d", i)
		}
	}
	if _, ok := c.(*fifoIdle); ok {
		if w.idleLinked || w.idlePrev != nil || w.idleNext != nil {
			t.Fatalf("removed worker still has idle links (linked=%v prev=%p next=%p)",
				w.idleLinked, w.idlePrev, w.idleNext)
		}
	}
}

func assertContainerEmpty(t *testing.T, c idleContainer) {
	t.Helper()
	if got := c.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
	switch v := c.(type) {
	case *stackIdle:
		for i, slot := range v.items[:cap(v.items)] {
			if slot != nil {
				t.Fatalf("stack slot %d = %p, want nil after drain", i, slot)
			}
		}
	case *fifoIdle:
		if v.head != nil || v.tail != nil {
			t.Fatalf("fifo head/tail = %p/%p, want nil after drain", v.head, v.tail)
		}
		if len(containerWorkers(c)) != 0 {
			t.Fatal("fifo still has linked workers after drain")
		}
	}
}

// drainOrder 依次 Pop 出全部元素；测试用，调用后容器为空。
func drainOrder(c idleContainer) []*worker {
	var out []*worker
	for {
		w := c.Pop()
		if w == nil {
			return out
		}
		out = append(out, w)
	}
}

func newIdleContainer(name string) idleContainer {
	if name == "stack" {
		return newStackIdle()
	}
	return newFIFOIdle()
}

func TestIdleContainerAddPopOrder(t *testing.T) {
	want := map[string][]int{
		"stack": {3, 2, 1}, // LIFO：最近归还的先复用
		"fifo":  {1, 2, 3}, // FIFO：最先空闲的先复用
	}
	for _, name := range []string{"stack", "fifo"} {
		t.Run(name, func(t *testing.T) {
			c := newIdleContainer(name)
			ws := []*worker{{}, {}, {}}
			for _, w := range ws {
				c.Add(w)
			}
			if got := c.Len(); got != 3 {
				t.Fatalf("Len() = %d, want 3", got)
			}

			for i, id := range want[name] {
				got := c.Pop()
				if got != ws[id-1] {
					t.Fatalf("Pop #%d = %p, want ws[%d]", i, got, id-1)
				}
				assertNoWorkerRef(t, c, got)
				if wantLen := 3 - i - 1; c.Len() != wantLen {
					t.Fatalf("Len() after Pop #%d = %d, want %d", i, c.Len(), wantLen)
				}
			}
			if got := c.Pop(); got != nil {
				t.Fatalf("Pop on empty = %p, want nil", got)
			}
			assertContainerEmpty(t, c)
		})
	}
}

func TestIdleContainerRemove(t *testing.T) {
	for _, name := range []string{"stack", "fifo"} {
		t.Run(name, func(t *testing.T) {
			c := newIdleContainer(name)
			ws := []*worker{{}, {}, {}, {}}
			for _, w := range ws {
				c.Add(w)
			}

			if !c.Remove(ws[1]) {
				t.Fatal("Remove(middle) = false, want true")
			}
			if c.Remove(ws[1]) {
				t.Fatal("Remove(removed) = true, want false")
			}
			if c.Remove(&worker{}) {
				t.Fatal("Remove(unknown) = true, want false")
			}
			assertNoWorkerRef(t, c, ws[1])
			if got := c.Len(); got != 3 {
				t.Fatalf("Len() after removing middle = %d, want 3", got)
			}

			if !c.Remove(ws[3]) { // 尾部
				t.Fatal("Remove(tail) = false, want true")
			}
			if !c.Remove(ws[0]) { // 头部
				t.Fatal("Remove(head) = false, want true")
			}
			assertNoWorkerRef(t, c, ws[0])
			assertNoWorkerRef(t, c, ws[3])

			order := drainOrder(c)
			if len(order) != 1 || order[0] != ws[2] {
				t.Fatalf("drain order = %v, want [ws[2]]", order)
			}
			assertContainerEmpty(t, c)
		})
	}
}

// TestFIFOIdleRemoveMiddlePreservesOrder 覆盖任意位置删除：链表实现不依赖
// 删除顺序，剩余元素仍保持 FIFO 顺序。
func TestFIFOIdleRemoveMiddlePreservesOrder(t *testing.T) {
	c := newFIFOIdle()
	ws := make([]*worker, 10)
	for i := range ws {
		ws[i] = &worker{}
		c.Add(ws[i])
	}

	for _, i := range []int{4, 0, 9} {
		if !c.Remove(ws[i]) {
			t.Fatalf("Remove(ws[%d]) = false, want true", i)
		}
		assertNoWorkerRef(t, c, ws[i])
	}
	if got := c.Len(); got != 7 {
		t.Fatalf("Len() = %d, want 7", got)
	}

	order := drainOrder(c)
	want := []int{1, 2, 3, 5, 6, 7, 8}
	if len(order) != len(want) {
		t.Fatalf("drained %d workers, want %d", len(order), len(want))
	}
	for i, id := range want {
		if order[i] != ws[id] {
			t.Fatalf("order[%d] = %p, want ws[%d]", i, order[i], id)
		}
	}
	assertContainerEmpty(t, c)
}

// TestFIFOIdleNoAllocs 固定链表实现的零分配特性：节点内嵌在 worker 上，
// Add/Pop/Remove 都不分配内存。
func TestFIFOIdleNoAllocs(t *testing.T) {
	c := newFIFOIdle()
	ws := make([]*worker, 128)
	for i := range ws {
		ws[i] = &worker{}
	}

	allocs := testing.AllocsPerRun(100, func() {
		for _, w := range ws {
			c.Add(w)
		}
		// 逆序删除，避免依赖队首快路径。
		for i := len(ws) - 1; i >= 0; i-- {
			if !c.Remove(ws[i]) {
				t.Fatal("Remove failed")
			}
		}
		for _, w := range ws {
			c.Add(w)
		}
		for c.Len() > 0 {
			c.Pop()
		}
	})
	if allocs != 0 {
		t.Fatalf("AllocsPerRun = %v, want 0", allocs)
	}
}

// TestFIFOIdleNoRetainedRefs 确认排空后链表节点字段与 head/tail 都不再引用 worker。
func TestFIFOIdleNoRetainedRefs(t *testing.T) {
	c := newFIFOIdle()
	ws := make([]*worker, 16)
	for i := range ws {
		ws[i] = &worker{}
		c.Add(ws[i])
	}
	for c.Len() > 0 {
		c.Pop()
	}
	assertContainerEmpty(t, c)
	for i, w := range ws {
		if w.idleLinked || w.idlePrev != nil || w.idleNext != nil {
			t.Fatalf("worker %d still linked after drain (linked=%v)", i, w.idleLinked)
		}
	}
}

// TestNewUsesFIFOIdle 固定生产默认容器：New 使用 FIFO，而不是 T8 的 LIFO 栈。
func TestNewUsesFIFOIdle(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	if _, ok := p.idle.(*fifoIdle); !ok {
		t.Fatalf("default idle container = %T, want *fifoIdle", p.idle)
	}
}

// TestIdleContainerEquivalence 用同一个场景验证 stack/fifo 两种容器接入 Pool
// 后语义一致：每个任务恰好执行一次、idle 数量正确、关闭能正常收尾。
func TestIdleContainerEquivalence(t *testing.T) {
	const capacity = 8

	for _, name := range []string{"stack", "fifo"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := normalizeConfig(Config{
				Capacity:    capacity,
				IdleTimeout: time.Second,
				MaxChunkLen: 4,
				MaxBuffered: 32,
				Mode:        BLOCK,
			})
			if err != nil {
				t.Fatalf("normalizeConfig: %v", err)
			}
			p := newPool(cfg, newIdleContainer(name))
			defer closeAndWait(t, p)

			var first sync.WaitGroup
			first.Add(capacity)
			started := make(chan struct{}, capacity)
			release := make(chan struct{})
			for i := 0; i < capacity; i++ {
				if err := p.Submit(func(context.Context) error {
					started <- struct{}{}
					<-release
					first.Done()
					return nil
				}); err != nil {
					t.Fatalf("Submit blocker #%d: %v", i, err)
				}
			}
			for i := 0; i < capacity; i++ {
				<-started
			}
			waitUntil(t, 2*time.Second, func() bool { return p.Running() == capacity })
			close(release)
			waitGroup(t, &first, 5*time.Second)
			waitUntil(t, 2*time.Second, func() bool { return p.Idle() == capacity })

			done := make(chan struct{}, capacity)
			for i := 0; i < capacity; i++ {
				if err := p.Submit(func(context.Context) error {
					done <- struct{}{}
					return nil
				}); err != nil {
					t.Fatalf("Submit batch #%d: %v", i, err)
				}
			}
			for i := 0; i < capacity; i++ {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("second batch task did not run")
				}
			}
			waitUntil(t, 2*time.Second, func() bool { return p.Idle() == capacity })
		})
	}
}
