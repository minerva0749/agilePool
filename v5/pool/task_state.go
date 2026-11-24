package pool

import (
	"context"
	"errors"
	"sync"
)

// 五类终态统一使用公开的 TaskResult（observability.go），这里保留短名作为
// 内部别名，避免 runTask / outcomeForAbort / 测试里出现两份枚举定义。
const (
	resultCompleted       = TaskCompleted
	resultError           = TaskFailed
	resultPanic           = TaskPanicked
	resultCanceled        = TaskCanceled
	resultDeadlineSkipped = TaskDeadlineSkipped
)

// taskOutcome 是一次任务终态的内部记录。
// err 只在 resultError 时非 nil；panicVal 只在 resultPanic 时非 nil；
// stack 只在 resultPanic 时非 nil，保存 recover 点捕获的 goroutine 栈。
// 对外通过 export() 转成只读的 TaskOutcome。
type taskOutcome struct {
	result   TaskResult
	err      error
	panicVal any
	stack    []byte
}

// export 把内部 outcome 转成传给 hook 的公开只读快照。
// Err / Panic 严格按终态填充，保证 TaskOutcome 的字段契约与文档一致。
func (o taskOutcome) export() TaskOutcome {
	switch o.result {
	case resultError:
		return TaskOutcome{Result: o.result, Err: o.err}
	case resultPanic:
		return TaskOutcome{Result: o.result, Panic: o.panicVal}
	default:
		return TaskOutcome{Result: o.result}
	}
}

// isSkipped 报告任务是否因取消/超期而没有执行。
func (o taskOutcome) isSkipped() bool {
	return o.result == resultCanceled || o.result == resultDeadlineSkipped
}

// outcomeForAbort 把 abortErr 的返回值映射到“被跳过”的终态。
func outcomeForAbort(err error) taskOutcome {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return taskOutcome{result: resultDeadlineSkipped}
	case errors.Is(err, context.Canceled):
		return taskOutcome{result: resultCanceled}
	default:
		// abortErr 只会返回上面两类；保留兜底避免新增检查时漏映射。
		return taskOutcome{result: resultCanceled, err: err}
	}
}

// pendingTracker 维护两组实时计数、五个累计计数，并为任务级等待提供可重建的
// drain channel：
//   - pending：已接受（已入队/已派发）、尚未到达终态的任务数；这是公开
//     Pending 指标（T7）的语义，不含提交中的任务；
//   - submitting：已进入 Submit、尚未返回的任务数，包含阻塞在满队列上
//     等待空间的提交。它只参与 WaitIdle 的等待条件，不计入 Pending 指标；
//   - submitted/rejected/completed/dropped/panicked：T7 的累计计数。它们与
//     上面两个计数的增减共用同一把 mu，因此不需要在提交/完成热路径新增锁。
//
// 等待条件是两组实时计数之和为 0：
//   - 任一组使总数从 0 变为正数时，换一个新的 idle channel；
//   - 总数从正数减到 0 时，关闭当前 channel，唤醒这一批的等待者。
//
// 这样同一个 tracker 可以服务多批“提交 → 等待 → 再提交”，不需要在
// 关闭后重建整个池。所有计数和 channel 都由 mu 保护。
type pendingTracker struct {
	mu         sync.Mutex
	pending    int
	submitting int
	idle       chan struct{}

	// 累计计数（单调递增）：
	// submitted：进入 enqueue 的合法提交尝试数（与 Submitted hook 对齐）；
	// rejected：submitted 中最终未入队的数量；
	// completed：到达 completed/error/panic 终态的任务数（不含 skipped）；
	// dropped：已接受但因取消/超期被跳过的任务数；
	// panicked：completed 中 panic 的数量。
	submitted int64
	rejected  int64
	completed int64
	dropped   int64
	panicked  int64
}

func newPendingTracker() *pendingTracker {
	idle := make(chan struct{})
	close(idle) // 总数为 0 的初始状态：wait 立即返回
	return &pendingTracker{idle: idle}
}

// totalLocked 返回等待条件使用的总数；必须在 mu 持有期间调用。
func (t *pendingTracker) totalLocked() int {
	return t.pending + t.submitting
}

// beginLocked 在计数增加前调用：总数 0→正数时开启新一批等待。
func (t *pendingTracker) beginLocked() {
	if t.totalLocked() == 0 {
		t.idle = make(chan struct{})
	}
}

// endLocked 在计数减少后调用：总数减到 0 时唤醒这一批等待者。
func (t *pendingTracker) endLocked() {
	if t.totalLocked() == 0 {
		close(t.idle)
	}
}

// add 记录一个被 Submit 接受的任务。调用方必须保证任务对 dispatcher
// 还不可见：当前由 taskQueue.pushLocked 在 q.mu 内回调，和写入 chunk
// 处于同一个临界区。
func (t *pendingTracker) add() {
	t.mu.Lock()
	t.beginLocked()
	t.pending++
	t.mu.Unlock()
}

// done 记录一个任务到达终态。每个 add 对应的任务只能 done 一次；
// 重复 done 会把计数减成负数，测试会以 pending != 0 暴露该协议错误。
//
// result 用于更新 T7 的累计计数：skipped 计入 dropped，其余计入 completed，
// panic 额外计入 panicked。计数更新与 pending-- 在同一把锁内完成。
func (t *pendingTracker) done(result TaskResult) {
	t.mu.Lock()
	t.pending--
	switch result {
	case resultCanceled, resultDeadlineSkipped:
		t.dropped++
	default:
		t.completed++
		if result == resultPanic {
			t.panicked++
		}
	}
	t.endLocked()
	t.mu.Unlock()
}

// beginSubmit 记录一次进入 Submit 的调用（在 push 之前）。
// WaitIdle 需要覆盖“正在提交”的任务，避免在提交者仍阻塞时提前返回；
// 同时累计 submitted（T7），与 Submitted hook 的覆盖范围一致。
func (t *pendingTracker) beginSubmit() {
	t.mu.Lock()
	t.beginLocked()
	t.submitting++
	t.submitted++
	t.mu.Unlock()
}

// endSubmit 与 beginSubmit 配对，在 push 返回后调用（无论成功还是拒绝）。
// accepted=false 时累计 rejected（T7）。
func (t *pendingTracker) endSubmit(accepted bool) {
	t.mu.Lock()
	t.submitting--
	if !accepted {
		t.rejected++
	}
	t.endLocked()
	t.mu.Unlock()
}

// wait 等待调用时刻已经存在的“已接受 + 提交中”任务全部结束。
// 总数为 0 时立即返回。
func (t *pendingTracker) wait() {
	t.mu.Lock()
	ch, total := t.idle, t.totalLocked()
	t.mu.Unlock()
	if total <= 0 {
		return
	}
	<-ch
}

// snapshot 返回已接受且未到终态的任务数（Pending 指标语义）。
func (t *pendingTracker) snapshot() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending
}

// submittingSnapshot 返回正在 Submit 中的任务数，仅用于测试与后续断言。
func (t *pendingTracker) submittingSnapshot() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.submitting
}

// counterSnapshot 是一次累计计数快照。
type counterSnapshot struct {
	submitted int64
	rejected  int64
	completed int64
	dropped   int64
	panicked  int64
}

// counters 在同一把锁内读取五个累计计数，保证它们来自同一时刻。
func (t *pendingTracker) counters() counterSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return counterSnapshot{
		submitted: t.submitted,
		rejected:  t.rejected,
		completed: t.completed,
		dropped:   t.dropped,
		panicked:  t.panicked,
	}
}

// finishTask 是任务到达终态的唯一入口，负责恰好一次 pending 扣减。
//
// 顺序很重要：先记账，再做 error/panic 诊断与 hook。T6 的 Logger/handler/
// hook 都在这里扩展，但即使它们 panic，也必须已经完成记账；因此 WaitIdle
// 可能先于 Completed/Skipped 回调返回（见 7.1）。
func (p *Pool) finishTask(t task, out taskOutcome) {
	p.pending.done(out.result)
	p.observeOutcome(t.ctx, out)
}
