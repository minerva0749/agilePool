package pool

// 本文件提供 T7 的运行指标。
//
// 约定：
//   - 每个方法都是一次独立瞬时快照，只读取一个来源；不同方法之间不保证来自同一
//     时刻，也不保证跨方法恒等式（唯一例外是 Busy 内部对同一把 p.mu 快照计算
//     running - len(idle)）；
//   - 不新增锁：Pending 与累计计数复用 pendingTracker.mu，Idle/Busy/
//     WorkerCreateCount 复用 p.mu，Queued 复用 queue.mu；
//   - 可以在任意 goroutine 调用，包括 T6 的 hook/ErrorHandler/PanicHandler；
//     不要在池内部已经持有 p.mu / queue.mu / pending.mu 的路径调用（包内已遵守）。
//
// 关闭池并 WaitPool 之后：Pending/Queued/Running/Idle/Busy 都回到 0；
// Capacity 与累计计数不会回落（WorkerCreateCount 与五个计数器都是单调值）。

// Pending 返回“已接受（已入队/已派发）且尚未到达终态”的任务数。
//
// 不包括仍在 Submit、尚未入队的任务（T0 第 1 条）；这部分由 WaitIdle 内部
// 的 submitting 计数覆盖，但不出现在 Pending 指标里。
func (p *Pool) Pending() int {
	return p.pending.snapshot()
}

// Queued 返回缓冲队列中的任务数。
//
// 只统计 taskQueue 里尚未被 pop 的任务；dispatcher 已经 pop、正在等待 worker
// 的任务与执行中的任务都不计入，因此 Queued <= Pending。该值是瞬时快照，
// 可能在读取后立即被 pop 改变。
func (p *Pool) Queued() int {
	return p.queue.size()
}

// Idle 返回当前在 idle 容器中的 worker 数。
//
// 这些 worker 存活且正在等待任务；包含刚 pushBack、尚未进入 select 的 worker，
// 也可能短暂包含已触发 idle timeout、但还没完成 removeIdle 的 worker。
// 该值只反映数量，不反映容器顺序（复用顺序见 6.4）。
func (p *Pool) Idle() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.idle.Len()
}

// Busy 返回已被 dispatcher 认领（正在交接）或执行中的 worker 数。
//
// 定义为同一次 p.mu 快照下的 running - len(idle)，因此在该快照内
// Idle()+Busy()==Running()。由于是瞬时值，它可能短暂包含：
//   - 刚执行完任务、尚未 pushBack 的 worker；
//   - 已从 idle 移除、尚未完成 running-- 的退出 worker。
//
// 这两个窗口都很短；需要严格“纯执行数”时应结合 WorkerCreateCount 与自己的
// 任务计数判断，而不是依赖 Busy 的瞬时值。
func (p *Pool) Busy() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running - p.idle.Len()
}

// Capacity 返回创建池时配置的 worker 数量上限。
//
// 该值在 New 时固定，不随 Running/Idle 变化；Close 之后仍返回同一值。
func (p *Pool) Capacity() int {
	return p.cfg.Capacity
}

// WorkerCreateCount 返回自 New 以来创建过的 worker 总数（单调递增）。
//
// worker 因 idle timeout 退出不会减少该值；它回答“池一共创建过多少 worker”，
// 而不是“当前有多少 worker”（后者用 Running）。
func (p *Pool) WorkerCreateCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.created
}

// Submitted 返回进入入队流程的合法提交尝试数。
//
// 与 T6 的 Submitted hook 对齐：fn != nil 并进入 enqueue 的尝试都计数，
// 包括随后因池关闭、队列满、ctx 取消或 deadline 过期而被拒绝的尝试。
// 参数错误（nil task、SubmitBefore 的非正 timeout）不会进入 enqueue，因此
// 不计数也不触发 hook。
func (p *Pool) Submitted() int64 {
	return p.pending.counters().submitted
}

// Rejected 返回 Submitted 中最终未能入队的提交数。
//
// 拒绝原因包括 ErrPoolClosed、ErrPoolFull、ctx 取消与 deadline 过期；
// 参数错误（nil task、非正 timeout）不计入。
func (p *Pool) Rejected() int64 {
	return p.pending.counters().rejected
}

// Completed 返回已到达 completed / error / panic 终态的任务数。
//
// 不包含 canceled / deadline-skipped（它们计入 Dropped）；panic 任务同时计入
// Completed 与 Panicked。
func (p *Pool) Completed() int64 {
	return p.pending.counters().completed
}

// Dropped 返回已接受但由于 ctx 取消或 SubmitBefore 期限已过而跳过执行的任务数。
//
// 与 T6 的 Skipped hook 对齐；与 Completed 互斥。
func (p *Pool) Dropped() int64 {
	return p.pending.counters().dropped
}

// Panicked 返回任务 panic 的次数（已隔离），是 Completed 的子集。
func (p *Pool) Panicked() int64 {
	return p.pending.counters().panicked
}
