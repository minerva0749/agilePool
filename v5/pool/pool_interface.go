package pool

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

var (
	ErrPoolClosed = errors.New("pool: closed")
	ErrPoolFull   = errors.New("pool: task buffer full")

	// ErrTaskBufferFull 是历史命名，保留为 ErrPoolFull 的别名，
	// 这样 errors.Is(err, ErrPoolFull) 和旧代码都能工作。
	ErrTaskBufferFull = ErrPoolFull
)

// BlockMode 控制缓冲队列满时提交方的行为。
type BlockMode int

const (
	BLOCK    BlockMode = iota // 缓冲满时阻塞提交方（默认）
	NONBLOCK                  // 缓冲满时立即返回 ErrPoolFull
)

type TaskFunc func(ctx context.Context) error

type task struct {
	Fn       TaskFunc
	ctx      context.Context
	deadline time.Time
}

type worker struct {
	tasks chan task
}

type Pool struct {
	cfg Config

	mu      sync.Mutex // 保护 idle / running
	idle    []*worker  // 空闲 worker 集合（slice 当栈用）
	running int        // 当前存活 worker 数（锁内维护）

	wg        sync.WaitGroup // Wait 等 dispatcher + 所有 worker
	onceClose sync.Once

	queue      *taskQueue
	workerFree chan struct{} // 有 worker 空闲或容量槽释放的提示
	drained    chan struct{} // dispatcher 排空并退出后关闭
}

type Config struct {
	Capacity    int           // worker 数量上限，必须 > 0
	IdleTimeout time.Duration // 空闲多久自动退出；<=0 时使用默认值
	MaxChunkLen int           // 每个缓冲块的容量，例如 64；<=0 时使用默认值 64
	MaxBuffered int           // 缓冲任务总数上限；-1 不设限，0 使用默认值（一个 chunk）；不必是 MaxChunkLen 的倍数
	Mode        BlockMode     // BLOCK / NONBLOCK
}

// New 创建并启动一个 worker pool。
//
// 队列和 dispatcher 在 New 里就绪；dispatcher 会一直运行到 Close
// 把队列关闭并且排空为止。
func New(cfg Config) (*Pool, error) {
	if cfg.Capacity <= 0 {
		return nil, errors.New("pool: capacity must be > 0")
	}
	if cfg.Mode != BLOCK && cfg.Mode != NONBLOCK {
		return nil, errors.New("pool: invalid block mode")
	}

	// 零值配置尽量可用：
	// - MaxChunkLen 未设置时用 64；
	// - MaxBuffered 未设置时使用一个 chunk 的容量，而不是 0（0 会让
	//   BLOCK 永远等不到空间）；显式不设限请用 -1。
	if cfg.MaxChunkLen <= 0 {
		cfg.MaxChunkLen = 64
	}
	if cfg.MaxBuffered == 0 {
		cfg.MaxBuffered = cfg.MaxChunkLen
	}
	if cfg.MaxBuffered < -1 {
		return nil, errors.New("pool: max buffered must be -1 or > 0")
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = time.Second
	}

	p := &Pool{
		cfg:        cfg,
		idle:       make([]*worker, 0),
		queue:      newTaskQueue(cfg.MaxChunkLen, cfg.MaxBuffered),
		workerFree: make(chan struct{}, 1),
		drained:    make(chan struct{}),
	}

	// dispatcher 也计入 wg：Wait 必须等它把最后一个任务派发出去，
	// 不能只等 worker。
	p.wg.Add(1)
	go p.dispatcher()
	return p, nil
}

// dispatcher 是队列的唯一消费者。
//
// 生命周期：
//  1. 队列为空且未关闭时阻塞；
//  2. Close 后先把队列剩余任务全部派发出去；
//  3. 队列关闭且为空时退出，并 close(drained) 通知 worker 收尾。
//
// dispatcher 绝不能在 Close 后立刻返回，否则缓冲区里的任务会丢。
func (p *Pool) dispatcher() {
	defer p.wg.Done()
	defer close(p.drained)

	for {
		t, ok := p.queue.pop()
		if !ok {
			// 只有“队列已关闭且为空”才会返回 false。
			return
		}

		// 在占用 worker 之前丢掉已取消/已超期的任务，
		// 避免为了一个不会执行的任务消耗一个 worker。
		if abortErr(t) != nil {
			continue
		}
		p.dispatchTask(t)
	}
}

// dispatchTask 把 t 交给一个 worker。
//
// 这是 v5 的关键交接点，必须守住不变量：
//   - 从 idle 取走一个 worker 后，必须向它的 tasks 发送恰好一个任务；
//   - 新建 worker 时，任务作为 firstTask 传入，不能再额外发送一次。
//
// 否则 worker 会在 <-wk.tasks 上永久阻塞，Close 的最后一棒也会失效。
func (p *Pool) dispatchTask(t task) {
	for {
		// 在等待 worker 期间任务可能已经取消/超期，此时直接丢弃，
		// 不占用 worker（队列长度已在 pop 时减掉）。
		if abortErr(t) != nil {
			return
		}

		p.mu.Lock()
		if len(p.idle) > 0 {
			wk := p.idle[0]
			p.idle = p.idle[1:]
			p.mu.Unlock()
			wk.tasks <- t // 一旦 pop，就一定要发
			return
		}
		if p.running < p.cfg.Capacity {
			p.makeWorker(t) // 新 worker 以 firstTask 启动，不再额外 send
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		// 没有空闲 worker 且达到容量上限：等 pushBack 或 worker 退出。
		// worker 退出时 decRunning 也会发信号，避免容量槽释放后丢失唤醒。
		<-p.workerFree
	}
}

// makeWorker 创建一个 worker，并让 firstTask 成为它的第一个任务。
// 必须在 p.mu 持有期间调用（只由 dispatcher 调用）。
func (p *Pool) makeWorker(firstTask task) {
	wk := &worker{tasks: make(chan task, 1)}
	p.running++
	p.wg.Add(1)

	go func(wk *worker, p *Pool) {
		defer p.wg.Done()
		defer p.decRunning()

		// 每个 worker 只创建一个 timer，之后每次空闲等待前 Reset。
		// 空任务密集时，每任务 NewTimer 会把大量时间花在 timer 堆和
		// 分配上；复用后只保留一次创建，Reset 仍然会重新定时，但不再
		// 产生新的 timer 对象。
		timeout := time.NewTimer(p.cfg.IdleTimeout)
		defer timeout.Stop()

		t := firstTask
		for {
			p.runTask(t)
			p.pushBack(wk)

			// 上一轮执行期间 timer 可能已经触发（尤其是慢任务），
			// 先 Stop 并清掉可能残留的 channel 值，再 Reset 到完整的
			// IdleTimeout，避免带着过期信号进入 select。
			resetTimer(timeout, p.cfg.IdleTimeout)

			/*
				timeout 不放进外层循环重建，是因为 select 选中 timeout 后
				worker 可能恰好被 dispatcher 认领：此时 removeIdle 失败，
				说明任务马上会从 tasks 发过来，必须回到 select 继续等，
				不能直接退出。
			*/
		inner:
			for {
				select {
				case t = <-wk.tasks:
					// 不在这里 Stop：外层 resetTimer 会统一 Stop/drain/Reset。
					// 这样任务执行期间即使 timer 到期，也不会留下旧信号。
					break inner
				case <-timeout.C:
					if p.removeIdle(wk) {
						return
					}
					// 被 dispatcher 认领了；继续等它把任务发进来。
				case <-p.drained:
					timeout.Stop()
					if p.removeIdle(wk) {
						return
					}
					// 被认领的 worker 一定会在 dispatcher 退出前收到任务：
					// dispatcher 先完成 send，最后才 close(drained)。
					t = <-wk.tasks
					p.runTask(t)
					return
				}
			}
		}
	}(wk, p)
}

// resetTimer 复用 time.Timer：先停止并清空可能已触发的 channel，
// 再重新设置 IdleTimeout。
//
// time.Timer.Reset 要求旧 timer 已停止或已过期，且 channel 已 drain；
// 这里统一处理，避免“任务和 timeout 同时就绪”时把旧信号带到下一轮。
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

// pushBack 把执行完任务的 worker 放回 idle。
func (p *Pool) pushBack(wk *worker) {
	p.mu.Lock()
	p.idle = append(p.idle, wk)
	p.mu.Unlock()

	// 先改状态，再发信号；否则等待者可能看到旧状态又睡回去。
	p.signalWorkerFree()
}

// removeIdle 从 idle 中移除 worker；返回 false 表示它已被 dispatcher 认领。
// 不要持 p.mu 调用。
func (p *Pool) removeIdle(wk *worker) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, w := range p.idle {
		if w == wk {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			return true
		}
	}
	return false
}

// decRunning 在 worker 退出时减少计数。
//
// 注意：worker 因 idle timeout 退出也会释放一个 Capacity 槽。如果这里
// 不发信号，dispatcher 可能刚好在“running == Capacity、idle 为空”时
// 永久等待一个不会到来的 worker，所以退出也必须唤醒等待者。
func (p *Pool) decRunning() {
	p.mu.Lock()
	p.running--
	p.mu.Unlock()

	p.signalWorkerFree()
}

func (p *Pool) signalWorkerFree() {
	select {
	case p.workerFree <- struct{}{}:
	default:
	}
}

// Submit 把任务放入缓冲队列。
//
// 返回 nil 只表示“已入队”，不表示“已执行”；任务之后仍可能因为
// ctx 取消或 deadline 过期被 dispatcher/worker 跳过。
func (p *Pool) Submit(fn TaskFunc) error {
	return p.enqueue(task{Fn: fn, ctx: context.Background()})
}

// SubmitCtx 带 context 提交。等待缓冲空间期间可以被 ctx 取消。
func (p *Pool) SubmitCtx(ctx context.Context, fn TaskFunc) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return p.enqueue(task{Fn: fn, ctx: ctx})
}

// SubmitBefore 给任务一个“开始执行”的期限，而不是执行时长限制。
//
// 如果在等待缓冲空间时到期，返回 DeadlineExceeded 且不入队；
// 如果已经入队但排队到 deadline 之后，dispatcher/worker 会跳过它。
func (p *Pool) SubmitBefore(fn TaskFunc, timeout time.Duration) error {
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	return p.enqueue(task{
		Fn:       fn,
		ctx:      context.Background(),
		deadline: time.Now().Add(timeout),
	})
}

func (p *Pool) enqueue(t task) error {
	return p.queue.push(t, t.ctx, t.deadline, p.cfg.Mode)
}

// Close 停止接收新任务，并触发 dispatcher 排空缓冲区。
//
// Close 本身不阻塞；需要等所有已入队任务执行完，请调用 Wait。
// 典型顺序：Close() -> Wait()。
func (p *Pool) Close() {
	p.onceClose.Do(func() {
		p.queue.close()
	})
}

// Wait 等 dispatcher 和所有 worker 退出。
//
// 必须在 Close 之后调用：不 Close 时 dispatcher 会一直等队列，
// Wait 永远不会返回。
func (p *Pool) Wait() {
	p.wg.Wait()
}

// Running 返回当前存活的 worker 数（不包含 dispatcher）。
func (p *Pool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// runTask 是任务执行的唯一入口：panic 隔离 + ctx/deadline 检查。
// 跳过执行不等于跳过清理；worker 的归还和退出由调用方继续负责。
func (p *Pool) runTask(t task) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("task panic: %v", r)
		}
	}()

	if abortErr(t) == nil {
		ctx := t.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		_ = t.Fn(ctx)
	}
}

// abortErr 检查任务是否已经取消或超过开始执行期限。
func abortErr(t task) error {
	if t.ctx != nil {
		select {
		case <-t.ctx.Done():
			return t.ctx.Err()
		default:
		}
	}
	if !t.deadline.IsZero() && !time.Now().Before(t.deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
