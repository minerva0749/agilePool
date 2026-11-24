package pool

// T12：idle worker 容器的内部抽象。
//
// 仅包内可插拔：Pool 持有一个 idleContainer，构造期选定；不新增公开 API。
// 容器本身不加锁——所有方法都必须在 p.mu 持有期间调用。
//
// 契约：
//   - Add：加入一个空闲 worker；同一个 worker 在容器内最多出现一次。
//   - Pop：取走“下一个复用”的 worker；空容器返回 nil。取走后容器不得再
//     持有该 worker 的引用（实现必须清空槽位/链接）。
//   - Remove：移除指定 worker；返回 false 表示它已被 Pop 取走（或从未加入）。
//     Remove 必须清空被移出的引用。
//   - Len：当前空闲 worker 数；调用方仍持有 p.mu。
//
// 为什么没有 RemoveExpired：v5 的 idle timeout 由每个 worker 自己的 timer
// 驱动，worker 到点后调用 Remove(wk)；容器不保存 idle 起始时间，也不负责
// 关闭 worker。把过期收归容器要同时改 worker 退出协议（quit + 池级 reaper），
// T12 没有采用（理由见 实现方案与接口.md 6.4 / T12 记录）。
type idleContainer interface {
	Add(*worker)
	Pop() *worker
	Remove(*worker) bool
	Len() int
}

// stackIdle 是 T8 的 LIFO 栈实现，保留作为可插拔实现的对照。
// Pop 取栈顶（最近归还的 worker），Remove 保序删除并清空尾部槽位。
type stackIdle struct {
	items []*worker
}

func newStackIdle() *stackIdle { return &stackIdle{} }

func (s *stackIdle) Add(w *worker) {
	s.items = append(s.items, w)
}

func (s *stackIdle) Pop() *worker {
	n := len(s.items) - 1
	if n < 0 {
		return nil
	}
	w := s.items[n]
	s.items[n] = nil
	s.items = s.items[:n]
	return w
}

func (s *stackIdle) Remove(w *worker) bool {
	for i, item := range s.items {
		if item == w {
			copy(s.items[i:], s.items[i+1:])
			last := len(s.items) - 1
			s.items[last] = nil
			s.items = s.items[:last]
			return true
		}
	}
	return false
}

func (s *stackIdle) Len() int { return len(s.items) }

// fifoIdle 是默认的 FIFO 容器：按空闲开始顺序排队，先空闲的 worker 先复用、
// 也先超时。用双向链表 + 内嵌在 worker 上的节点实现，Add/Pop/Remove 都是 O(1)。
//
// 为什么不用环形缓冲：环形的“队首快路径”依赖 Remove 顺序与队列顺序一致。
// 实测 Capacity=10000 同时 idle timeout 时，pushBack 与 timer Reset 之间存在
// 调度窗口，超时顺序会与队列顺序错位，ring 的 Remove 频繁退化为 O(n) 搬移，
// 端到端反而明显慢于旧 LIFO 栈（见文档 9.12）。双向链表不依赖顺序：每次
// Remove 直接按节点前后指针摘除，批量超时严格 O(n)。
//
// 节点字段在 worker 上，容器本身不分配内存；FIFO 顺序由链表维护。
type fifoIdle struct {
	head *worker
	tail *worker
	n    int
}

func newFIFOIdle() *fifoIdle { return &fifoIdle{} }

func (q *fifoIdle) Len() int { return q.n }

func (q *fifoIdle) Add(w *worker) {
	w.idlePrev = q.tail
	w.idleNext = nil
	w.idleLinked = true
	if q.tail != nil {
		q.tail.idleNext = w
	} else {
		q.head = w
	}
	q.tail = w
	q.n++
}

func (q *fifoIdle) Pop() *worker {
	w := q.head
	if w == nil {
		return nil
	}
	q.unlink(w)
	return w
}

func (q *fifoIdle) Remove(w *worker) bool {
	if w == nil || !w.idleLinked {
		return false
	}
	q.unlink(w)
	return true
}

// unlink 把 w 从链表摘除并清空节点字段；调用方保证 w 在链表中。
func (q *fifoIdle) unlink(w *worker) {
	if w.idlePrev != nil {
		w.idlePrev.idleNext = w.idleNext
	} else {
		q.head = w.idleNext
	}
	if w.idleNext != nil {
		w.idleNext.idlePrev = w.idlePrev
	} else {
		q.tail = w.idlePrev
	}
	w.idlePrev = nil
	w.idleNext = nil
	w.idleLinked = false
	q.n--
}
