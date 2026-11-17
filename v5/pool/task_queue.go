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

	closed   bool
	closedCh chan struct{} // Close 时关闭，唤醒所有等待者

	notEmpty chan struct{} // 队列非空提示，容量 1，允许合并
	notFull  chan struct{} // 队列有空位提示，容量 1
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
// BLOCK：队列满时等待 notFull/closed/ctx/deadline；
// NONBLOCK：队列满时立即返回 ErrPoolFull。
//
// 入队成功只表示任务已经进入缓冲队列，不表示已经执行。
func (q *taskQueue) push(t task, ctx context.Context, deadline time.Time, mode BlockMode) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}

	var deadlineC <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		deadlineC = timer.C
	}

	// woke 表示上一次循环消费了 notFull token。若醒来后任务又被取消，
	// 要把这个“空位提示”还回去，否则其他阻塞的提交者可能永远等不到唤醒。
	woke := false
	for {
		if err := ctx.Err(); err != nil {
			if woke {
				q.signalNotFull()
			}
			return err
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			if woke {
				q.signalNotFull()
			}
			return context.DeadlineExceeded
		}

		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrPoolClosed
		}
		if q.maxTaskNum < 0 || q.taskNum < q.maxTaskNum {
			q.pushLocked(t)
			// 一次 pop 释放多个空位时，notFull 的容量 1 会合并通知。
			// 如果入队后还有空位，把通知继续传给下一个等待者，
			// 避免它明明有空间却一直睡到下一次 pop。
			moreSpace := q.maxTaskNum >= 0 && q.taskNum < q.maxTaskNum
			q.mu.Unlock()
			q.signalNotEmpty()
			if moreSpace {
				q.signalNotFull()
			}
			return nil
		}
		q.mu.Unlock()

		if mode == NONBLOCK {
			return ErrPoolFull
		}

		select {
		case <-q.notFull:
			woke = true
		case <-q.closedCh:
			return ErrPoolClosed
		case <-ctx.Done():
			return ctx.Err()
		case <-deadlineC:
			return context.DeadlineExceeded
		}
	}
}

// pushLocked 在 q.mu 内追加任务。
// 队列空时复用最后一个 chunk；tail 满时新建 chunk 并链接。
func (q *taskQueue) pushLocked(t task) {
	if q.tail == nil {
		c := newChunk(q.chunkLen)
		q.head = c
		q.tail = c
	}
	if q.tail.count == q.chunkLen {
		c := newChunk(q.chunkLen)
		q.tail.next = c
		q.tail = c
	}

	idx := (q.tail.read + q.tail.count) % q.chunkLen
	q.tail.tasks[idx] = t
	q.tail.count++
	q.taskNum++
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
			q.mu.Unlock()
			q.signalNotFull()
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
		old.next = nil // 断开旧块，让 GC 回收
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
	q.mu.Unlock()
	close(q.closedCh)
}
