package pool

import (
	"context"
	"sync"
	"time"
)

// chunk 是分块链表里的一个定长块。
//
// 字段只在 taskQueue.mu 持有期间访问：
//   - tasks 的长度固定为 chunkLen；
//   - read 是下一个出队下标；
//   - count 是当前块内任务数，push 写在 (read+count)%chunkLen。
//
// 用 count 而不是“读写指针相等”判断空/满，避免环形下标回绕后
// 无法区分空和满。
type chunk struct {
	tasks []task
	next  *chunk
	read  int
	count int
}

func newChunk(size int) *chunk {
	return &chunk{tasks: make([]task, size)}
}

// maxRecycledSlots 限制每个队列的空闲块缓存最多保留多少个任务槽位。
// 用槽位数而不是块数做上限，避免 MaxChunkLen 很大时缓存占用失控；
// 超出上限的块直接交给 GC。
const maxRecycledSlots = 4096

// taskQueue 是 v5 的缓冲队列：固定大小 chunk 串成的 FIFO 链表。
//
// 不变量：
//  1. head 指向最早未消费完的 chunk，tail 指向最新写入的 chunk；
//  2. head 到 tail 之间的非 tail 块都是满的；
//  3. taskNum == 所有 chunk.count 之和，且 taskNum 只在 mu 内修改；
//  4. 生产只改 tail，消费只改 head，但二者共用 mu；
//  5. notEmpty/notFull 只是“可能可读/可写”的提示，真正条件必须在锁内复查。
//
// v5 是“多生产者 + 单消费者（dispatcher）”模型，因此队列只需要一把锁：
// 生产者不会和另一个消费者争抢 head/tail 指针。
type taskQueue struct {
	mu         sync.Mutex
	head       *chunk
	tail       *chunk
	taskNum    int
	maxTaskNum int // 最大任务数；-1 表示不设限
	chunkLen   int

	// recycled 是已完全消费、等待复用的块（T9）。只在 mu 内访问：
	// popLocked 把消费完的块放入，pushLocked 优先从这里取，取不到才分配。
	// 这样“队列满 → 消费释放块 → 继续入队”的稳态不再每 chunkLen 个任务
	// 分配一次 ~chunkLen*sizeof(task) 的数组，也省掉对应的清零与 GC。
	recycled []*chunk

	closed   bool
	closedCh chan struct{} // Close 时关闭，唤醒所有等待者

	notEmpty chan struct{} // 队列非空提示，容量 1，允许合并
	notFull  chan struct{} // 队列有空位提示，容量 1

	// pushers 是已被判定为“队列满”、正在 notFull 上等待的 BLOCK 提交者数量。
	// 由 mu 保护。pop 只在 pushers > 0 时发送 notFull；否则一次非阻塞发送
	// 只是白白碰一次容量为 1 的 channel，在高吞吐下会放大 pop 的成本。
	//
	// 登记点必须在“锁内判定为满”的同一临界区：如果先解锁再登记，pop 可能
	// 在中间释放空位并看到 pushers == 0 而跳过信号，导致提交者永久睡眠。
	pushers int

	// onAccept 在 pushLocked 写入任务后、仍持有 q.mu 时调用。
	// Pool 用它把“接受任务”和“pending +1”放进同一个临界区，
	// 保证 dispatcher 不可能在计数增加之前 pop 到该任务。
	//
	// 只允许在回调里获取 pending 的锁；不能反向获取 q.mu，否则会
	// 与 finishTask（pending.mu）和 pop（q.mu）形成锁环。队列单测
	// 和纯队列基准中它为 nil。
	onAccept func()
}

func newTaskQueue(chunkLen, max int) *taskQueue {
	return &taskQueue{
		chunkLen:   chunkLen,
		maxTaskNum: max,
		closedCh:   make(chan struct{}),
		notEmpty:   make(chan struct{}, 1),
		notFull:    make(chan struct{}, 1),
	}
}

func (q *taskQueue) signalNotEmpty() {
	select {
	case q.notEmpty <- struct{}{}:
	default:
	}
}

func (q *taskQueue) signalNotFull() {
	select {
	case q.notFull <- struct{}{}:
	default:
	}
}

// size 返回当前缓冲任务数（用于观察和测试）。
func (q *taskQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.taskNum
}

// push 入队。
//
// 错误优先级（T2 固定）：
//  1. 池已关闭 → ErrPoolClosed；
//  2. ctx 已取消 → ctx.Err()；
//  3. 任务 deadline 已过 → context.DeadlineExceeded；
//  4. NONBLOCK 且队列满 → ErrPoolFull。
//
// 关闭优先于 ctx/deadline：Close 之后所有提交都稳定返回 ErrPoolClosed，
// 不会因为 ctx 恰好也取消而返回两种不同的错误。
//
// BLOCK 模式下，队列满时等待 notFull/closed/ctx/deadline；每次醒来都在
// 锁内重新检查真实条件，因为 notFull 只是“可能有过空位”的提示。
//
// 入队成功只表示任务已经进入缓冲队列，不表示已经执行。
func (q *taskQueue) push(t task, ctx context.Context, deadline time.Time, mode BlockMode) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// T9：deadline timer 延迟到真正需要阻塞等待时才创建。队列立即可写时
	// （SubmitBefore 的常见路径）不再为一次成功的入队分配 time.Timer。
	var (
		timer     *time.Timer
		deadlineC <-chan time.Time
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	// woke 表示本次 select 消费了 notFull token。若随后因取消/超期返回，
	// 要把这个“空位提示”转交给其他阻塞提交者，否则它会被吞掉。
	woke := false
	for {
		// 在锁外读取 ctx.Err()：自定义 Context 的 Err 方法属于用户代码，
		// 不应在 queue.mu 持有期间调用。若 ctx 在读取后、加锁前被取消，
		// 任务可能仍然入队，随后由 dispatcher/worker 的 abortErr 跳过；
		// 这与入队后取消的既有语义一致。
		ctxErr := ctx.Err()

		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrPoolClosed
		}
		if ctxErr != nil {
			q.mu.Unlock()
			if woke {
				q.forwardNotFull()
			}
			return ctxErr
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			q.mu.Unlock()
			if woke {
				q.forwardNotFull()
			}
			return context.DeadlineExceeded
		}
		if q.maxTaskNum < 0 || q.taskNum < q.maxTaskNum {
			wasEmpty := q.taskNum == 0
			q.pushLocked(t)
			// 一次 pop 释放多个空位时，notFull 的容量 1 会合并通知。
			// 如果入队后还有空位，把通知继续传给下一个等待者。
			moreSpace := q.maxTaskNum >= 0 && q.taskNum < q.maxTaskNum
			waiters := q.pushers
			q.mu.Unlock()

			// T9：只有“空 → 非空”才会让一个正在等待的 pop 错过任务；
			// 队列原本非空时，pop 已经在处理或会在下一次循环里看到。
			if wasEmpty {
				q.signalNotEmpty()
			}
			// 只有仍存在阻塞提交者时，空位提示才有接收者。
			if moreSpace && waiters > 0 {
				q.signalNotFull()
			}
			return nil
		}

		if mode == NONBLOCK {
			q.mu.Unlock()
			return ErrPoolFull
		}

		// 在“判定为满”的同一临界区登记等待者：pop 只有在锁内看到
		// pushers > 0 才会发 notFull，因此无等待者时可以安全省略信号。
		q.pushers++
		q.mu.Unlock()

		if timer == nil && !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			deadlineC = timer.C
		}

		woke = false
		select {
		case <-q.notFull:
			woke = true
		case <-q.closedCh:
			// 回到循环顶部，由 closed 检查决定返回值。
		case <-ctx.Done():
		case <-deadlineC:
		}

		q.mu.Lock()
		q.pushers--
		q.mu.Unlock()
	}
}

// forwardNotFull 把当前 goroutine 消费到的空位提示转交给其他阻塞提交者。
// 只在锁外调用；没有其他等待者时不需要发送（token 只会变成陈旧提示）。
func (q *taskQueue) forwardNotFull() {
	q.mu.Lock()
	waiters := q.pushers
	q.mu.Unlock()
	if waiters > 0 {
		q.signalNotFull()
	}
}

// pushLocked 在 q.mu 内追加任务。
// 队列空时复用最后一个 chunk；tail 满时新建（或从 recycled 复用）chunk 并链接。
func (q *taskQueue) pushLocked(t task) {
	if q.tail == nil {
		c := q.getChunkLocked()
		q.head = c
		q.tail = c
	}
	if q.tail.count == q.chunkLen {
		c := q.getChunkLocked()
		q.tail.next = c
		q.tail = c
	}

	idx := (q.tail.read + q.tail.count) % q.chunkLen
	q.tail.tasks[idx] = t
	q.tail.count++
	q.taskNum++
	if q.onAccept != nil {
		q.onAccept()
	}
}

// getChunkLocked 优先复用已消费完的块；没有可复用的块时才分配。
// 必须在 q.mu 持有期间调用。
func (q *taskQueue) getChunkLocked() *chunk {
	n := len(q.recycled) - 1
	if n < 0 {
		return newChunk(q.chunkLen)
	}
	c := q.recycled[n]
	q.recycled[n] = nil
	q.recycled = q.recycled[:n]
	return c
}

// recycleChunkLocked 把已完全消费的块放入复用缓存。
//
// 只接受 count == 0 的块：popLocked 每弹出一个任务就把对应槽位置零，
// 因此块内不会残留 task（及其 ctx/Fn 引用）。缓存上限按槽位数计算，
// 防止无界队列排空后长期保留大量块。超出上限时直接交给 GC。
//
// 必须在 q.mu 持有期间调用。
func (q *taskQueue) recycleChunkLocked(c *chunk) {
	if c == nil || q.closed {
		// 关闭后不会再有入队，复用缓存没有意义；直接交给 GC。
		return
	}
	if (len(q.recycled)+1)*q.chunkLen > maxRecycledSlots {
		return
	}
	c.next = nil
	c.read = 0
	c.count = 0
	q.recycled = append(q.recycled, c)
}

// pop 取队头任务。
//
// 关闭后仍会把队列剩余任务取完；只有“已关闭且为空”才返回 false。
// 这是 dispatcher 排空缓冲区的依据。
func (q *taskQueue) pop() (task, bool) {
	for {
		q.mu.Lock()
		if q.taskNum > 0 {
			t := q.popLocked()
			waiters := q.pushers
			q.mu.Unlock()
			// T9：没有阻塞提交者时，这次 pop 释放的空位无人等待，
			// 不需要碰 notFull；有等待者时仍需逐个释放提示。
			if waiters > 0 {
				q.signalNotFull()
			}
			return t, true
		}
		if q.closed {
			q.mu.Unlock()
			return task{}, false
		}
		q.mu.Unlock()

		select {
		case <-q.notEmpty:
		case <-q.closedCh:
		}
	}
}

// popLocked 在 q.mu 内消费一个任务。
func (q *taskQueue) popLocked() task {
	// 正常情况下 head 有任务；若为空则说明前面的块已经消费完，
	// 前移到下一个块。length > 0 时不应该遇到 head.next == nil。
	for q.head.count == 0 {
		if q.head.next == nil {
			return task{}
		}
		old := q.head
		q.head = q.head.next
		old.next = nil
		q.recycleChunkLocked(old) // 已消费完的块进入复用缓存
	}

	idx := q.head.read
	t := q.head.tasks[idx]
	q.head.tasks[idx] = task{} // 释放对 ctx/Fn 的引用，避免长生命周期的块保留任务
	q.head.read = (q.head.read + 1) % q.chunkLen
	q.head.count--
	q.taskNum--

	// 当前块消费完后立即前移，避免下一次 pop 再判断一次。
	if q.head.count == 0 && q.head.next != nil {
		old := q.head
		q.head = q.head.next
		old.next = nil
		q.recycleChunkLocked(old)
	}
	return t
}

// close 停止接收新任务，并唤醒所有阻塞在 push/pop 上的 goroutine。
// 已入队的任务不会被丢弃；dispatcher 会继续 pop，直到队列为空。
func (q *taskQueue) close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	// 关闭后不会再有入队：复用缓存已无意义，释放它对已消费块的引用。
	q.recycled = nil
	q.mu.Unlock()
	close(q.closedCh)
}
