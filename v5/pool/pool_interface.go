package pool

import (
	"context"
	"errors"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

var (
	ErrPoolClosed = errors.New("pool: closed")
	ErrPoolFull   = errors.New("pool: task buffer full")

	// ErrNilTask 表示提交的 TaskFunc 为 nil。
	// 这是调用方参数错误，先于池状态和 ctx 状态返回。
	ErrNilTask = errors.New("pool: nil task")

	// ErrTaskBufferFull 是历史命名，保留为 ErrPoolFull 的别名，
	// 这样 errors.Is(err, ErrPoolFull) 和旧代码都能工作。
	ErrTaskBufferFull = ErrPoolFull
)

const (
	defaultMaxChunkLen = 64
	defaultIdleTimeout = time.Second
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

	// 以下字段只被 fifoIdle 使用，作为 idle 双向链表的节点；只在持有 p.mu
	// 且 worker 位于 idle 容器时有效。stackIdle 实现不使用它们（T12）。
	idlePrev   *worker
	idleNext   *worker
	idleLinked bool
}

type Pool struct {
	cfg Config

	mu      sync.Mutex    // 保护 idle / running / created
	idle    idleContainer // 空闲 worker 容器（默认 FIFO；T12 包内可插拔）
	running int           // 当前存活 worker 数（锁内维护）
	created int64         // 自 New 以来创建过的 worker 总数（锁内维护，只增不减）

	wg        sync.WaitGroup // WaitPool 等 dispatcher + 所有 worker
	onceClose sync.Once

	queue      *taskQueue
	workerFree chan struct{} // 有 worker 空闲或容量槽释放的提示
	drained    chan struct{} // dispatcher 排空并退出后关闭

	// pending 记录“已接受（已入队/已派发）且尚未到达终态”的任务数。
	// 仍阻塞在 Submit 里等待缓冲空间的提交中任务不计入（T0 第 1 条）。
	// 计数点与所有权规则见 task_state.go 的 finishTask 和
	// taskQueue.pushLocked 里的 onAccept 回调。
	pending *pendingTracker

	// 以下诊断与生命周期回调全部在 New 时固定，运行期间只读：
	// 不需要原子操作或锁，也不会与 worker 竞争。
	logger       Logger
	errorHandler ErrorHandler
	panicHandler PanicHandler
	hooks        Hooks
}

type Config struct {
	Capacity    int           // worker 数量上限，必须 > 0
	IdleTimeout time.Duration // 空闲多久自动退出；<=0 时使用默认值
	MaxChunkLen int           // 每个缓冲块的容量，例如 64；<=0 时使用默认值 64
	MaxBuffered int           // 缓冲任务总数上限；-1 不设限，0 使用默认值（一个 chunk）；不必是 MaxChunkLen 的倍数
	Mode        BlockMode     // BLOCK / NONBLOCK

	// 以下是可选的诊断与生命周期回调，全部在 New 时固定，运行期间不再修改。
	// 零值表示不启用；不影响队列、worker 数量和任务语义。

	// Logger 用于池的诊断输出；nil 表示 log.Default()。
	Logger Logger
	// ErrorHandler 接收任务返回的非 nil error；nil 表示静默丢弃。
	ErrorHandler ErrorHandler
	// PanicHandler 接收任务 panic；nil 表示通过 Logger 输出默认报告。
	PanicHandler PanicHandler
	// Hooks 是任务与池的生命周期回调；零值表示不监听。
	Hooks Hooks
}

// New 创建并启动一个 worker pool。
//
// 配置的默认值与校验集中在 normalizeConfig：
//   - Capacity 必须 > 0；
//   - Mode 必须是 BLOCK 或 NONBLOCK；
//   - MaxChunkLen <= 0 时用 64；
//   - MaxBuffered == 0 时用一个 chunk（MaxChunkLen）；< -1 返回错误；
//   - IdleTimeout <= 0 时用 1s。
//
// 队列和 dispatcher 在 New 里就绪；dispatcher 会一直运行到 Close
// 把队列关闭并且排空为止。
func New(cfg Config) (*Pool, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newPool(cfg, newFIFOIdle()), nil
}

// newPool 用已 normalize 的配置和指定的 idle 容器构造池。
// 仅供包内测试替换容器实现（T12）；生产入口只用 New。
func newPool(cfg Config, idle idleContainer) *Pool {
	p := &Pool{
		cfg:          cfg,
		idle:         idle,
		queue:        newTaskQueue(cfg.MaxChunkLen, cfg.MaxBuffered),
		workerFree:   make(chan struct{}, 1),
		drained:      make(chan struct{}),
		pending:      newPendingTracker(),
		logger:       cfg.Logger,
		errorHandler: cfg.ErrorHandler,
		panicHandler: cfg.PanicHandler,
		hooks:        cfg.Hooks,
	}
	if p.logger == nil {
		p.logger = log.Default()
	}

	// 队列接受任务的同一临界区里先增加 pending，保证计数先于任务对
	// dispatcher 可见，避免“pop + 扣账”抢在“接受 + 加账”之前发生。
	p.queue.onAccept = p.pending.add

	// dispatcher 也计入 wg：Wait 必须等它把最后一个任务派发出去，
	// 不能只等 worker。
	p.wg.Add(1)
	go p.dispatcher()
	return p
}

// normalizeConfig 集中处理 Config 的默认值与合法性校验。
//
// 零值配置尽量可用：MaxChunkLen/MaxBuffered/IdleTimeout 都有默认值，
// 只有 Capacity 和 Mode 必须由调用方明确给出。
func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Capacity <= 0 {
		return Config{}, errors.New("pool: capacity must be > 0")
	}
	if cfg.Mode != BLOCK && cfg.Mode != NONBLOCK {
		return Config{}, errors.New("pool: invalid block mode")
	}

	if cfg.MaxChunkLen <= 0 {
		cfg.MaxChunkLen = defaultMaxChunkLen
	}
	// MaxBuffered == 0 表示“一个 chunk”，而不是零缓冲：
	// 零缓冲会让 BLOCK 模式永远等不到空间。
	if cfg.MaxBuffered == 0 {
		cfg.MaxBuffered = cfg.MaxChunkLen
	}
	if cfg.MaxBuffered < -1 {
		return Config{}, errors.New("pool: max buffered must be -1 or > 0")
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}
	return cfg, nil
}

// dispatcher 是队列的唯一消费者。
//
// 生命周期：
//  1. 队列为空且未关闭时阻塞；
//  2. Close 后先把队列剩余任务全部派发出去；
//  3. 队列关闭且为空时退出，并 close(drained) 通知 worker 收尾。
//
// dispatcher 不会在 Close 后立刻返回，否则缓冲区里的任务会丢。
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
		// 丢弃同样是一次终态，必须走统一记账入口，否则 pending 会泄漏。
		if err := abortErr(t); err != nil {
			p.finishTask(t, outcomeForAbort(err))
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
//
// 达到容量上限时，等待 worker 的这段时间也必须能被任务自己的取消/deadline
// 打断（T5）：被取消或超期的任务不会执行，如果它还占着 dispatcher 等一个
// worker，队列里后续任务的派发就会被一起拖住。
func (p *Pool) dispatchTask(t task) {
	// deadline timer 只在真正进入“等 worker”分支时才创建：能被立即派发
	// 的任务不该为一次成功的派发付出 timer 分配。整个等待期间最多一个
	// timer；deadline 是绝对时间，不需要 Reset。
	var deadlineTimer *time.Timer
	var deadlineC <-chan time.Time
	defer func() {
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
	}()

	for {
		// 在等待 worker 期间任务可能已经取消/超期，此时直接丢弃，
		// 不占用 worker（队列长度已在 pop 时减掉）。这条路径和
		// dispatcher() 里的丢弃共用 outcomeForAbort + finishTask。
		if err := abortErr(t); err != nil {
			p.finishTask(t, outcomeForAbort(err))
			return
		}

		p.mu.Lock()
		if wk := p.popIdleLocked(); wk != nil {
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

		if deadlineTimer == nil && !t.deadline.IsZero() {
			deadlineTimer = time.NewTimer(time.Until(t.deadline))
			deadlineC = deadlineTimer.C
		}

		// 没有空闲 worker 且达到容量上限：等以下任一事件：
		//   - worker 归还（pushBack）或容量槽释放（decRunning）；
		//   - 任务 ctx 被取消（或 ctx 自身到期）；
		//   - SubmitBefore 的 deadline 到期。
		//
		// workerFree 只是“可能变了”的提示，可能已过期（被其他等待者消费），
		// 因此醒来后必须回到循环顶部，在锁内重新检查真实条件。
		// 取消/超期分支直接走统一终态入口，不再认领 worker，dispatcher 随后
		// 继续 pop 队列里的下一个任务。
		select {
		case <-p.workerFree:
			// 不在这里下结论：提示可能已过期，回到循环顶部在 p.mu 内复查。
		case <-taskDone(t.ctx):
			// ctx 取消（或 ctx 自身到期）：不再认领 worker。
			// 终态统一由 abortErr 判定，避免 ctx 与 deadline 同时成立时
			// 依赖 select 的随机选择；与 runTask 入口的检查顺序一致。
			p.finishTask(t, outcomeForAbort(abortErr(t)))
			return
		case <-deadlineC:
			// SubmitBefore 的“开始执行”期限已过：同样走统一终态入口。
			// 若此时 ctx 也已取消，abortErr 仍会给出 ctx 的终态。
			p.finishTask(t, outcomeForAbort(abortErr(t)))
			return
		}
	}
}

// makeWorker 创建一个 worker，并让 firstTask 成为它的第一个任务。
// 必须在 p.mu 持有期间调用（只由 dispatcher 调用）。
func (p *Pool) makeWorker(firstTask task) {
	wk := &worker{tasks: make(chan task, 1)}
	p.running++
	p.created++
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
			// runTask 只负责产生结果；finishTask 负责恰好一次扣账。
			// 即使 observeOutcome 里的日志/回调出错，扣账也已经完成。
			p.finishTask(t, p.runTask(t))
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
					p.finishTask(t, p.runTask(t))
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

// pushBack 把执行完任务的 worker 放回 idle 栈。
func (p *Pool) pushBack(wk *worker) {
	p.mu.Lock()
	p.pushIdleLocked(wk)
	p.mu.Unlock()

	// 先改状态，再发信号；否则等待者可能看到旧状态又睡回去。
	p.signalWorkerFree()
}

// idle 容器的三个操作都要求调用方持有 p.mu；容器本身不加锁。
//
// 不变量（由 idleContainer 实现共同保证）：
//  1. 每个“存活且空闲”的 worker 在容器中恰好出现一次；
//  2. Pop/Remove 之后容器不得再持有该 worker 的引用（实现负责清空槽位），
//     否则会延长已退出/已认领 worker 及其 tasks channel 的生命周期；
//  3. 容器不负责 idle timeout：worker 自己的 timer 到点后调用 removeIdle；
//     容器不知道 idle 起始时间，也不关闭 worker。
//
// 默认实现是 FIFO 双向链表（fifoIdle，T12）：先空闲的 worker 先复用、也先超时，
// Add/Pop/Remove 都是 O(1)，批量超时严格 O(n)；stackIdle 是 T8 的 LIFO 对照实现。
func (p *Pool) pushIdleLocked(wk *worker) {
	p.idle.Add(wk)
}

// popIdleLocked 取走下一个可复用的 worker；容器空时返回 nil。
func (p *Pool) popIdleLocked() *worker {
	return p.idle.Pop()
}

// removeIdleLocked 从容器移除指定 worker（worker 自身超时或池排空时使用）；
// 返回 false 表示它已被 dispatcher 认领。
func (p *Pool) removeIdleLocked(wk *worker) bool {
	return p.idle.Remove(wk)
}

// removeIdle 从 idle 容器移除 worker；返回 false 表示它已被 dispatcher 认领。
// 不要持 p.mu 调用。
func (p *Pool) removeIdle(wk *worker) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.removeIdleLocked(wk)
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
// fn 为 nil 时返回 ErrNilTask，任务不会入队，也不会启动 worker。
// 返回 nil 只表示“已入队”，不表示“已执行”；任务之后仍可能因为
// ctx 取消或 deadline 过期被 dispatcher/worker 跳过。
func (p *Pool) Submit(fn TaskFunc) error {
	if fn == nil {
		return ErrNilTask
	}
	return p.enqueue(task{Fn: fn, ctx: context.Background()})
}

// SubmitCtx 带 context 提交。等待缓冲空间期间可以被 ctx 取消。
//
// fn 为 nil 时返回 ErrNilTask；ctx 为 nil 时按 context.Background() 处理。
func (p *Pool) SubmitCtx(ctx context.Context, fn TaskFunc) error {
	if fn == nil {
		return ErrNilTask
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return p.enqueue(task{Fn: fn, ctx: ctx})
}

// SubmitBefore 给任务一个“开始执行”的期限，而不是执行时长限制。
//
// 错误优先级：
//  1. fn == nil → ErrNilTask；
//  2. timeout <= 0 → context.DeadlineExceeded（参数错误，不进入队列等待）；
//  3. 池已关闭 → ErrPoolClosed；
//  4. 等待缓冲空间期间 deadline 到期 → context.DeadlineExceeded。
//
// 如果在等待缓冲空间时到期，任务不入队；如果已经入队但排队到 deadline
// 之后，dispatcher/worker 会跳过它。
func (p *Pool) SubmitBefore(fn TaskFunc, timeout time.Duration) error {
	if fn == nil {
		return ErrNilTask
	}
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
	// submitting 只用于 WaitIdle：提交者阻塞在满队列上时，WaitIdle 不能
	// 提前返回。pending 指标本身不含提交中任务（T0 第 1 条），被接受的
	// 任务由 queue.pushLocked 在锁内调用 p.pending.add。
	p.pending.beginSubmit()

	// Submitted 在参数校验之后、入队尝试之前触发：被拒绝的提交也会到达
	// 这里，因此“尝试”与“接受”可以分开观测（Enqueued 只在成功时触发）。
	p.hookSubmitted(t.ctx)

	err := p.queue.push(t, t.ctx, t.deadline, p.cfg.Mode)
	p.pending.endSubmit(err == nil)
	if err != nil {
		return err
	}

	// push 返回时 queue.mu 已释放；Enqueued 不允许在锁内触发用户回调。
	// 代价是：任务可能已经被 dispatcher 取走甚至执行完，因此 Enqueued
	// 与 Started/Completed 之间的顺序不保证（见 Hooks 文档）。
	p.hookEnqueued(t.ctx)
	return nil
}

// Close 停止接收新任务，并触发 dispatcher 排空缓冲区。
//
// Close 不等待已接受任务执行完；需要等它们完成，请调用 WaitPool。
// Close 会同步执行 PoolClosed hook，因此阻塞的 hook 会延长 Close 的返回时间。
// 典型顺序：Close() -> WaitPool()。
func (p *Pool) Close() {
	p.onceClose.Do(func() {
		p.queue.close()
		// PoolClosed 在队列关闭之后触发：回调里再 Submit 会稳定拿到
		// ErrPoolClosed，而不是与关闭过程竞争。此时任务可能仍在执行。
		p.hookPoolClosed()
	})
}

// WaitPool 等 dispatcher 和所有 worker 退出。
//
// 必须在 Close 之后调用：不 Close 时 dispatcher 会一直等队列，
// WaitPool 永远不会返回。
func (p *Pool) WaitPool() {
	p.wg.Wait()
}

// WaitIdle 等待“已接受以及正在提交”的任务全部到达终态，但不关闭池；
// 返回后仍可继续提交，也可以再次等待下一批。
//
// 语义：
//   - 等待范围包含已入队/已派发的任务，以及已经进入 Submit、仍阻塞在满
//     队列等待空间的提交；已完成的任务不再计入；
//   - 空池立即返回；不需要先 Close；
//   - 多个 goroutine 可以同时等待同一批；
//   - Close 之后仍可调用，用于等待已接受任务排空；池退出等待仍用
//     Close + WaitPool；
//   - 这是“批次快照”语义：等待期间新提交的任务属于下一批，不保证被本次
//     WaitIdle 覆盖。
//
// 不要从任务函数内部调用 WaitIdle：该任务自己也是 pending，会等待自己，
// 造成永久阻塞。
func (p *Pool) WaitIdle() {
	p.pending.wait()
}

// Running 返回当前存活的 worker 数（不包含 dispatcher）。
func (p *Pool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// runTask 是任务执行的唯一入口：结果分类 + panic 隔离 + ctx/deadline 检查
// + Started hook。它不负责记账；调用方拿到 taskOutcome 后必须交给 finishTask。
//
// panic 的 goroutine 栈必须在 recover 点捕获：一旦 recover 完成、栈展开结束，
// 之后（如 finishTask 里的 PanicHandler）再调用 debug.Stack() 就只能看到
// 回调自身的栈，看不到 panic 发生位置。
func (p *Pool) runTask(t task) (out taskOutcome) {
	defer func() {
		if r := recover(); r != nil {
			out = taskOutcome{result: resultPanic, panicVal: r, stack: debug.Stack()}
		}
	}()

	if err := abortErr(t); err != nil {
		return outcomeForAbort(err)
	}

	ctx := contextOrBackground(t.ctx)

	// Started 只在任务确实会执行时触发：被跳过（取消/超期）的任务不应让
	// 观察者误以为它开始过。hook 自身的 panic 由 safeCallback 隔离，
	// 不会在这里被误判成任务 panic。
	p.hookStarted(ctx)

	if err := t.Fn(ctx); err != nil {
		return taskOutcome{result: resultError, err: err}
	}
	return taskOutcome{result: resultCompleted}
}

// taskDone 返回任务 ctx 的 Done channel；ctx 为 nil 时返回 nil，
// 让 select 的这一路永远不就绪（Submit 系列入口都会填非 nil ctx，
// 这里只做防御）。
func taskDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
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
