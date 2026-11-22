package pool

import (
	"context"
	"errors"
	"log"
	"sync"
)

// taskResult 描述任务到达终态的原因。
//
// 五类终态与规划 T3 一一对应：
//   - resultCompleted：任务函数正常返回 nil；
//   - resultError：任务函数返回非 nil error；
//   - resultPanic：任务函数 panic，已被隔离；
//   - resultCanceled：执行前 ctx 已取消，任务被跳过；
//   - resultDeadlineSkipped：执行前已超过开始执行期限，任务被跳过。
type taskResult uint8

const (
	resultCompleted taskResult = iota
	resultError
	resultPanic
	resultCanceled
	resultDeadlineSkipped
)

// String 只用于测试与调试输出。
func (r taskResult) String() string {
	switch r {
	case resultCompleted:
		return "completed"
	case resultError:
		return "error"
	case resultPanic:
		return "panic"
	case resultCanceled:
		return "canceled"
	case resultDeadlineSkipped:
		return "deadline-skipped"
	default:
		return "unknown"
	}
}

// taskOutcome 是一次任务终态的完整记录。
// err 只在 resultError 时非 nil；panicVal 只在 resultPanic 时非 nil。
type taskOutcome struct {
	result   taskResult
	err      error
	panicVal any
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

// pendingTracker 维护两组计数，并为任务级等待提供可重建的 drain channel：
//   - pending：已接受（已入队/已派发）、尚未到达终态的任务数；这是公开
//     Pending 指标（T7）的语义，不含提交中的任务；
//   - submitting：已进入 Submit、尚未返回的任务数，包含阻塞在满队列上
//     等待空间的提交。它只参与 WaitIdle 的等待条件，不计入 Pending 指标。
//
// 等待条件是两组计数之和为 0：
//   - 任一组使总数从 0 变为正数时，换一个新的 idle channel；
//   - 总数从正数减到 0 时，关闭当前 channel，唤醒这一批的等待者。
//
// 这样同一个 tracker 可以服务多批“提交 → 等待 → 再提交”，不需要在
// 关闭后重建整个池。两个计数和 channel 都由 mu 保护。
type pendingTracker struct {
	mu         sync.Mutex
	pending    int
	submitting int
	idle       chan struct{}
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
func (t *pendingTracker) done() {
	t.mu.Lock()
	t.pending--
	t.endLocked()
	t.mu.Unlock()
}

// beginSubmit 记录一次进入 Submit 的调用（在 push 之前）。
// WaitIdle 需要覆盖“正在提交”的任务，避免在提交者仍阻塞时提前返回。
func (t *pendingTracker) beginSubmit() {
	t.mu.Lock()
	t.beginLocked()
	t.submitting++
	t.mu.Unlock()
}

// endSubmit 与 beginSubmit 配对，在 push 返回后调用（无论成功还是拒绝）。
func (t *pendingTracker) endSubmit() {
	t.mu.Lock()
	t.submitting--
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

// finishTask 是任务到达终态的唯一入口，负责恰好一次 pending 扣减。
//
// 顺序很重要：先记账，再做 panic 日志等副作用。T6 的 hook/Logger 会
// 在这里扩展，但即使它们 panic，也必须已经完成记账。
func (p *Pool) finishTask(out taskOutcome) {
	p.pending.done()
	p.observeOutcome(out)
}

// observeOutcome 在记账之后观察终态。
// 当前只保留 panic 日志；T6 会把它扩展为可配置的 Logger/PanicHandler。
// 这里的 recover 保证副作用异常不会打断 worker 主循环。
func (p *Pool) observeOutcome(out taskOutcome) {
	defer func() { _ = recover() }()

	if out.result == resultPanic {
		log.Printf("task panic: %v", out.panicVal)
	}
}
