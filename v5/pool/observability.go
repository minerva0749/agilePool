package pool

import (
	"context"
	"log"
	"runtime/debug"
)

// Logger 是池自身诊断日志的最小接口。
//
// 标准库 *log.Logger 直接满足该接口；传 nil 表示使用 log.Default()。
// Logger 只承载池的诊断输出（任务 panic 的默认报告、hook/handler 自身 panic），
// 不用于任务业务日志。
//
// Logger 在 New 时固定，之后不可替换：读取路径不需要原子操作或锁，也不会
// 与运行中的 worker 产生数据竞争。Logger 自身 panic 会被池隔离，不会传播给
// worker 或提交者。
type Logger interface {
	Printf(format string, args ...any)
}

// ErrorHandler 接收任务函数返回的非 nil error。
//
// 调用时机：任务到达 TaskFailed 终态、pending 已扣减之后，在 worker goroutine
// 上同步调用。普通 error 只做回调与（后续 T7 的）计数，不触发重试；重试只属于
// T10 的 TaskWithRetry。ErrorHandler 为 nil 时 error 被安静地丢弃。
//
// ctx 是提交任务时使用的 context；Submit/SubmitBefore 传入的等价于
// context.Background()。ErrorHandler 自身 panic 会被隔离并通过 Logger 报告。
type ErrorHandler func(ctx context.Context, err error)

// PanicHandler 接收被 worker 隔离的任务 panic。
//
// recovered 是 recover() 的返回值；stack 是在 recover 点捕获的 goroutine 栈
// （debug.Stack() 的格式）。配置 PanicHandler 后，池不再向 Logger 输出默认的
// panic 日志，由 handler 决定如何上报；handler 自身 panic 会被隔离并通过
// Logger 报告。ctx 与 ErrorHandler 相同。
type PanicHandler func(ctx context.Context, recovered any, stack []byte)

// TaskResult 是任务终态的原因分类。它与规划中的五类终态一一对应。
type TaskResult int

const (
	TaskCompleted       TaskResult = iota // Fn 正常返回 nil
	TaskFailed                            // Fn 返回非 nil error
	TaskPanicked                          // Fn panic，已被隔离
	TaskCanceled                          // 执行前 ctx 已取消，任务被跳过
	TaskDeadlineSkipped                   // 执行前已超过 SubmitBefore 的开始期限
)

// String 只用于测试与调试输出。
func (r TaskResult) String() string {
	switch r {
	case TaskCompleted:
		return "completed"
	case TaskFailed:
		return "error"
	case TaskPanicked:
		return "panic"
	case TaskCanceled:
		return "canceled"
	case TaskDeadlineSkipped:
		return "deadline-skipped"
	default:
		return "unknown"
	}
}

// TaskOutcome 是传给 Completed/Skipped hook 的只读终态快照。
//
// Err 只在 Result == TaskFailed 时非 nil；Panic 只在 Result == TaskPanicked
// 时非 nil（其值为 recover() 的返回值）。
type TaskOutcome struct {
	Result TaskResult
	Err    error
	Panic  any
}

// Hooks 是池的生命周期回调集合。零值表示不监听任何事件。
//
// 所有回调都在 New 时固定，不支持运行中注册或替换，因此池内部不需要为 hook
// 加锁或做原子读写。
//
// 调用语义：
//   - 每个回调都在触发事件的 goroutine 上同步执行，不另起 goroutine；
//     Submitted/Enqueued 在提交者 goroutine，Started/Completed 在 worker
//     goroutine，Skipped 在 dispatcher 或 worker goroutine，PoolClosed 在
//     Close 调用者 goroutine；
//   - 回调不在 p.mu / queue.mu 持有期间执行，因此可以在回调里调用
//     Running()/Pending()/Queued() 等只读指标（T7）；
//   - 回调必须自行保证并发安全：不同任务的回调可能在不同 goroutine 上并发执行；
//   - 回调 panic 会被隔离并通过 Logger 报告，不会中断任务记账、worker 存活或
//     关闭流程；但回调不应长时间阻塞，否则会拖慢提交、worker 回收或 Close；
//   - 不要在回调里调用 Submit、WaitIdle、WaitPool 或 Close：Submit 会递归
//     触发 Submitted；Submitted/Enqueued 回调里的 WaitIdle 会等待当前这次
//     提交自身；WaitPool 会等待回调所在的 worker/dispatcher 退出；从
//     PoolClosed 回调再调用 Close 会重入 sync.Once 而死锁。
//   - 所有 hook 都不在 WaitIdle 的等待条件内；Completed/Skipped 在 pending
//     扣减之后执行，因此 WaitIdle 可能先于回调返回；
//   - 同一任务的 Started 保证早于 Completed；Completed 与 Skipped 互斥。
//     Submitted 早于 Enqueued（同一提交者 goroutine）。Enqueued 与 Started
//     之间不保证顺序：任务入队后可能立即被 dispatcher 取走并开始执行，而
//     Enqueued 必须在 queue.mu 释放后才能安全触发。
//   - 重试（T10）按逻辑任务处理：Submitted/Enqueued/Started/
//     Completed/Skipped 仍各触发一次，重试尝试不会重复触发这些 hook。
type Hooks struct {
	// Submitted 在提交参数合法（fn != nil）之后、尝试入队之前触发。
	// 被拒绝的提交（池已关闭、队列满、入队期间 ctx 取消/超期）同样会触发
	// Submitted，但不会触发 Enqueued，也不会触发 Completed/Skipped。
	Submitted func(ctx context.Context)

	// Enqueued 在任务成功进入缓冲队列、queue.mu 已释放后触发，每个被接受
	// 的任务恰好一次。
	Enqueued func(ctx context.Context)

	// Started 在任务通过执行前检查（ctx 未取消、deadline 未过）之后、
	// 调用 TaskFunc 之前触发。被跳过的任务不会触发 Started。
	Started func(ctx context.Context)

	// Completed 在任务执行完成（正常返回、返回 error 或 panic 被隔离）且
	// pending 已扣减之后触发。被取消/超期跳过的任务不会触发 Completed。
	Completed func(ctx context.Context, outcome TaskOutcome)

	// Skipped 在任务因 ctx 取消或 SubmitBefore 期限已过而未执行时触发，
	// 与 Completed 互斥。outcome.Result 只会是 TaskCanceled 或
	// TaskDeadlineSkipped。
	Skipped func(ctx context.Context, outcome TaskOutcome)

	// PoolClosed 在 Close 关闭队列之后、Close 返回之前触发，只会触发一次。
	// 此时新提交已经稳定返回 ErrPoolClosed，但已接受任务可能仍在执行。
	PoolClosed func()
}

// hookSubmitted 触发 Submitted；调用方必须已经完成参数校验。
func (p *Pool) hookSubmitted(ctx context.Context) {
	if fn := p.hooks.Submitted; fn != nil {
		c := contextOrBackground(ctx)
		p.safeCallback("Submitted", func() { fn(c) })
	}
}

// hookEnqueued 触发 Enqueued；调用方必须已经成功入队并释放 queue.mu。
func (p *Pool) hookEnqueued(ctx context.Context) {
	if fn := p.hooks.Enqueued; fn != nil {
		c := contextOrBackground(ctx)
		p.safeCallback("Enqueued", func() { fn(c) })
	}
}

// hookStarted 触发 Started；调用方必须已经通过 abortErr 检查。
func (p *Pool) hookStarted(ctx context.Context) {
	if fn := p.hooks.Started; fn != nil {
		c := contextOrBackground(ctx)
		p.safeCallback("Started", func() { fn(c) })
	}
}

// hookCompleted 触发 Completed；调用方必须已经完成 pending 扣减。
func (p *Pool) hookCompleted(ctx context.Context, out taskOutcome) {
	if fn := p.hooks.Completed; fn != nil {
		c := contextOrBackground(ctx)
		exported := out.export()
		p.safeCallback("Completed", func() { fn(c, exported) })
	}
}

// hookSkipped 触发 Skipped；调用方必须已经完成 pending 扣减。
func (p *Pool) hookSkipped(ctx context.Context, out taskOutcome) {
	if fn := p.hooks.Skipped; fn != nil {
		c := contextOrBackground(ctx)
		exported := out.export()
		p.safeCallback("Skipped", func() { fn(c, exported) })
	}
}

// hookPoolClosed 触发 PoolClosed；调用方必须已经关闭队列。
func (p *Pool) hookPoolClosed() {
	if fn := p.hooks.PoolClosed; fn != nil {
		p.safeCallback("PoolClosed", fn)
	}
}

// reportTaskError 把普通 error 交给 ErrorHandler（未配置时静默）。
func (p *Pool) reportTaskError(ctx context.Context, out taskOutcome) {
	if h := p.errorHandler; h != nil {
		c := contextOrBackground(ctx)
		p.safeCallback("ErrorHandler", func() { h(c, out.err) })
	}
}

// reportTaskPanic 把任务 panic 交给 PanicHandler；未配置时使用默认日志。
// 默认日志同样包含 recover 点捕获的 stack trace。
func (p *Pool) reportTaskPanic(ctx context.Context, out taskOutcome) {
	if h := p.panicHandler; h != nil {
		c := contextOrBackground(ctx)
		p.safeCallback("PanicHandler", func() { h(c, out.panicVal, out.stack) })
		return
	}
	p.logf("task panic: %v\n%s", out.panicVal, out.stack)
}

// observeOutcome 在 pending 已扣减之后观察终态。
//
// 顺序固定为：error/panic 诊断（ErrorHandler / PanicHandler / 默认日志）→
// 生命周期 hook（Completed 或 Skipped）。Completed 与 Skipped 互斥：
// canceled / deadline-skipped 只进入 Skipped，其余终态只进入 Completed。
func (p *Pool) observeOutcome(ctx context.Context, out taskOutcome) {
	switch out.result {
	case resultError:
		p.reportTaskError(ctx, out)
	case resultPanic:
		p.reportTaskPanic(ctx, out)
	}

	if out.isSkipped() {
		p.hookSkipped(ctx, out)
		return
	}
	p.hookCompleted(ctx, out)
}

// safeCallback 同步执行一个用户回调，并隔离其 panic。
//
// 回调 panic 只影响本次回调；调用方（记账、worker 主循环、Close）继续执行。
func (p *Pool) safeCallback(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			p.logCallbackPanic(name, r, debug.Stack())
		}
	}()
	fn()
}

// logCallbackPanic 通过 Logger 报告回调 panic。
// Logger 自身 panic 也必须被吞掉，否则会把诊断路径变成新的崩溃源。
func (p *Pool) logCallbackPanic(name string, r any, stack []byte) {
	p.logf("%s callback panicked: %v\n%s", name, r, stack)
}

// logf 是池内唯一的日志出口：Logger 为 nil 时回退到 log.Default()，
// 且 Logger 自身 panic 被隔离。
func (p *Pool) logf(format string, args ...any) {
	defer func() { _ = recover() }()

	l := p.logger
	if l == nil {
		l = log.Default()
	}
	l.Printf(format, args...)
}

// contextOrBackground 保证交给用户回调的 ctx 非 nil。
func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
