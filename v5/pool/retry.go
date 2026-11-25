package pool

import (
	"context"
	"time"
)

const (
	// defaultRetryMinBackOff 是 RetryPolicy 未指定 MinBackOff 时的首次退避。
	defaultRetryMinBackOff = 100 * time.Millisecond
	// defaultRetryMaxBackOff 是 RetryPolicy 未指定 MaxBackOff 时的退避上限。
	defaultRetryMaxBackOff = time.Second
)

// RetryPolicy 描述 TaskWithRetry 的重试与退避策略。
//
// 字段语义：
//   - RetryNum 是**额外重试次数**，总执行次数为 RetryNum+1；<=0 表示只执行一次。
//   - 第 k 次重试（k 从 1 开始）执行前等待 MinBackOff*2^(k-1)，并封顶 MaxBackOff。
//   - MinBackOff / MaxBackOff <= 0 时使用默认值（100ms / 1s）；
//     MaxBackOff < MinBackOff 时按 MinBackOff 处理。
//   - BackOffStrategy 非 nil 时完全接管退避计算，签名为
//     func(attempt int, min, max time.Duration) time.Duration；attempt 从 1 开始，
//     min/max 是已经归一化过的值。返回值 <=0 表示不等待；池不对自定义结果再封顶。
//     策略在 worker goroutine 上同步执行，应快速返回；策略自身 panic 会被当作任务
//     panic 隔离（终态 TaskPanicked）。
//
// RetryPolicy 是纯值类型：可以复用同一份策略创建多个 TaskFunc；运行期间修改原变量
// 不会影响已经创建的包装函数（TaskWithRetry 内部保存的是副本）。
type RetryPolicy struct {
	RetryNum        int
	MinBackOff      time.Duration
	MaxBackOff      time.Duration
	BackOffStrategy func(attempt int, min, max time.Duration) time.Duration
}

// TaskWithRetry 把 fn 包装成一个“失败后按 policy 重试”的任务函数。
//
// 用法：
//
//	p.Submit(pool.TaskWithRetry(fn, pool.RetryPolicy{RetryNum: 3}))
//
// 重试属于同一个逻辑任务：包装后的函数仍然只提交一次，因此 pending 只增加一次、
// 只扣减一次；Submitted/Enqueued/Started/Completed 各触发一次；ErrorHandler 只在
// 所有尝试结束后收到最终 error，中间尝试的 error 不单独上报（需要逐次观测时，
// 可以在 fn 内部自行记录）。
//
// 失败与取消：
//   - 只有 fn 返回非 nil error 才会重试。panic 不重试，直接交给池的 recover，
//     最终终态为 TaskPanicked，剩余重试不会执行。
//   - 退避等待用 timer + select ctx.Done()，可被 ctx 取消或 ctx 自身的 deadline
//     立即打断；此时返回 ctx.Err()，不再发起新的尝试。按 T0 的 D5，任务已经开始
//     执行，所以终态是 TaskFailed（ErrorHandler 会收到 ctx.Err()），而不是
//     TaskCanceled。
//   - SubmitBefore 的 deadline 仍只约束“开始执行”：任务一旦开始，重试过程只受
//     ctx 约束，不受 SubmitBefore 的 deadline 影响（见文档 §4.2）。
//
// 资源语义：退避发生在执行 fn 的 worker goroutine 内，一次重试任务在成功或耗尽
// 之前会一直占用一个 worker。Capacity 较小时，请把重试次数与退避上限控制在合理
// 范围内，避免少数重试任务长时间占满池子。
//
// fn 为 nil 时返回 nil，交给 Submit 系列入口返回 ErrNilTask。
func TaskWithRetry(fn TaskFunc, policy RetryPolicy) TaskFunc {
	if fn == nil {
		return nil
	}
	policy = policy.normalized()

	return func(ctx context.Context) error {
		if ctx == nil {
			ctx = context.Background()
		}

		var lastErr error
		for attempt := 0; attempt <= policy.RetryNum; attempt++ {
			if attempt > 0 {
				if err := waitBackOff(ctx, policy.backOffDelay(attempt)); err != nil {
					return err
				}
			}

			// 退避可能刚好与取消同时就绪（select 随机选择）；在调用 fn 前
			// 再检查一次，避免“已经取消却仍执行下一次尝试”。
			if err := ctx.Err(); err != nil {
				return err
			}

			err := fn(ctx)
			if err == nil {
				return nil
			}
			lastErr = err
		}
		return lastErr
	}
}

// normalized 返回一份填好默认值、修正非法组合的副本。
//
// 约定：
//   - 负的 RetryNum 视为 0（调用方错误，不 panic）；
//   - 非正的退避参数使用默认值；MaxBackOff < MinBackOff 时提升到 MinBackOff，
//     避免默认策略出现“等待时间超过上限”的自相矛盾。
func (p RetryPolicy) normalized() RetryPolicy {
	if p.RetryNum < 0 {
		p.RetryNum = 0
	}
	if p.MinBackOff <= 0 {
		p.MinBackOff = defaultRetryMinBackOff
	}
	if p.MaxBackOff <= 0 {
		p.MaxBackOff = defaultRetryMaxBackOff
	}
	if p.MaxBackOff < p.MinBackOff {
		p.MaxBackOff = p.MinBackOff
	}
	return p
}

// backOffDelay 返回第 attempt 次重试前的等待时间（attempt 从 1 开始）。
//
// 默认是指数退避：min、2*min、4*min……并在 max 处封顶。用比较代替乘法，
// 避免重试次数很大时 time.Duration 溢出。
func (p RetryPolicy) backOffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	if p.BackOffStrategy != nil {
		d := p.BackOffStrategy(attempt, p.MinBackOff, p.MaxBackOff)
		if d < 0 {
			return 0
		}
		return d
	}

	d := p.MinBackOff
	for i := 1; i < attempt; i++ {
		if d > p.MaxBackOff/2 {
			return p.MaxBackOff
		}
		d *= 2
	}
	if d > p.MaxBackOff {
		return p.MaxBackOff
	}
	return d
}

// waitBackOff 等待 d，或直到 ctx 取消。d <= 0 时只做一次 ctx 检查后立即返回。
//
// 不使用裸 time.Sleep：重试的退避必须能被调用方的取消/超时打断，否则一个
// 已经取消的任务还要白白占着 worker 睡到退避结束。
func waitBackOff(ctx context.Context, d time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
