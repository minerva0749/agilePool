package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const retryTestTimeout = 5 * time.Second

func newRetryTestPool(t *testing.T, mutate func(*Config)) *Pool {
	t.Helper()
	cfg := Config{
		Capacity:    4,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 64,
		Mode:        BLOCK,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// instantRetryPolicy 让退避为 0，避免测试依赖真实等待时间；退避本身另有
// TestTaskWithRetryWaitsBetweenAttempts 与 TestRetryPolicyBackOffScheduleAndDefaults 覆盖。
func instantRetryPolicy(retryNum int) RetryPolicy {
	return RetryPolicy{
		RetryNum: retryNum,
		BackOffStrategy: func(int, time.Duration, time.Duration) time.Duration {
			return 0
		},
	}
}

func waitForSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(retryTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestTaskWithRetrySucceedsFirstAttempt(t *testing.T) {
	var calls, errCalls atomic.Int64
	completed := make(chan TaskOutcome, 1)

	p := newRetryTestPool(t, func(c *Config) {
		c.Hooks.Completed = func(_ context.Context, out TaskOutcome) { completed <- out }
		c.ErrorHandler = func(context.Context, error) { errCalls.Add(1) }
	})

	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		calls.Add(1)
		return nil
	}, instantRetryPolicy(3))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()

	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (success must not retry)", got)
	}
	if got := errCalls.Load(); got != 0 {
		t.Fatalf("ErrorHandler calls = %d, want 0", got)
	}
	if got := p.Pending(); got != 0 {
		t.Fatalf("Pending = %d, want 0", got)
	}
	if got := p.Completed(); got != 1 {
		t.Fatalf("Completed() = %d, want 1 logical task", got)
	}

	select {
	case out := <-completed:
		if out.Result != TaskCompleted {
			t.Fatalf("Completed hook result = %v, want TaskCompleted", out.Result)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("Completed hook was not called")
	}

	closeAndWait(t, p)
}

func TestTaskWithRetrySucceedsAfterRetries(t *testing.T) {
	var calls atomic.Int64
	completed := make(chan TaskOutcome, 1)

	p := newRetryTestPool(t, func(c *Config) {
		c.Hooks.Completed = func(_ context.Context, out TaskOutcome) { completed <- out }
	})

	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}, instantRetryPolicy(3))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()

	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3 (two failures + one success)", got)
	}
	if got := p.Completed(); got != 1 {
		t.Fatalf("Completed() = %d, want 1 logical task", got)
	}

	select {
	case out := <-completed:
		if out.Result != TaskCompleted {
			t.Fatalf("Completed hook result = %v, want TaskCompleted", out.Result)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("Completed hook was not called")
	}

	closeAndWait(t, p)
}

// 所有尝试都失败：执行次数 = RetryNum+1，ErrorHandler 只收到最终 error，
// Completed hook 只触发一次且结果为 TaskFailed。
func TestTaskWithRetryExhaustsRetries(t *testing.T) {
	var calls, errCalls atomic.Int64
	errCh := make(chan error, 1)
	completed := make(chan TaskOutcome, 1)

	p := newRetryTestPool(t, func(c *Config) {
		c.ErrorHandler = func(_ context.Context, err error) {
			errCalls.Add(1)
			errCh <- err
		}
		c.Hooks.Completed = func(_ context.Context, out TaskOutcome) { completed <- out }
	})

	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		return fmt.Errorf("attempt %d failed", calls.Add(1))
	}, instantRetryPolicy(2))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()

	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3 (RetryNum=2 means 1+2 attempts)", got)
	}
	if got := p.Pending(); got != 0 {
		t.Fatalf("Pending = %d, want 0", got)
	}
	if got := p.Completed(); got != 1 {
		t.Fatalf("Completed() = %d, want 1 logical task", got)
	}
	if got := p.Rejected(); got != 0 {
		t.Fatalf("Rejected() = %d, want 0", got)
	}

	// WaitIdle 只等待 pending 记账，不等待 ErrorHandler 返回；先收到事件，
	// 再断言次数，避免把文档允许的回调窗口误判为重试实现错误。
	select {
	case err := <-errCh:
		if err == nil || err.Error() != "attempt 3 failed" {
			t.Fatalf("ErrorHandler err = %v, want last attempt error", err)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("ErrorHandler was not called")
	}
	if got := errCalls.Load(); got != 1 {
		t.Fatalf("ErrorHandler calls = %d, want exactly 1 (final error only)", got)
	}

	select {
	case out := <-completed:
		if out.Result != TaskFailed {
			t.Fatalf("Completed hook result = %v, want TaskFailed", out.Result)
		}
		if out.Err == nil || out.Err.Error() != "attempt 3 failed" {
			t.Fatalf("Completed outcome err = %v, want last attempt error", out.Err)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("Completed hook was not called")
	}

	closeAndWait(t, p)
}

func TestTaskWithRetryZeroOrNegativeRetryNum(t *testing.T) {
	p := newRetryTestPool(t, nil)
	sentinel := errors.New("fail")

	for _, retryNum := range []int{0, -3} {
		var calls atomic.Int64
		if err := p.Submit(TaskWithRetry(func(context.Context) error {
			calls.Add(1)
			return sentinel
		}, instantRetryPolicy(retryNum))); err != nil {
			t.Fatalf("Submit(RetryNum=%d): %v", retryNum, err)
		}
		p.WaitIdle()

		if got := calls.Load(); got != 1 {
			t.Fatalf("RetryNum=%d: calls = %d, want 1", retryNum, got)
		}
	}

	closeAndWait(t, p)
}

// ctx 在退避期间取消：不再发起新尝试，返回 ctx.Err()；任务已经开始执行，
// 按 D5 终态是 TaskFailed 而不是 TaskCanceled。
func TestTaskWithRetryCancelDuringBackOff(t *testing.T) {
	var calls atomic.Int64
	errCh := make(chan error, 1)
	completed := make(chan TaskOutcome, 1)

	p := newRetryTestPool(t, func(c *Config) {
		c.ErrorHandler = func(_ context.Context, err error) { errCh <- err }
		c.Hooks.Completed = func(_ context.Context, out TaskOutcome) { completed <- out }
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})

	fn := func(context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		return errors.New("retry me")
	}
	policy := RetryPolicy{RetryNum: 5, MinBackOff: 10 * time.Second, MaxBackOff: 10 * time.Second}

	if err := p.SubmitCtx(ctx, TaskWithRetry(fn, policy)); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}

	waitForSignal(t, started, "first attempt")
	start := time.Now()
	cancel()

	var gotErr error
	select {
	case gotErr = <-errCh:
	case <-time.After(retryTestTimeout):
		t.Fatal("ErrorHandler was not called after cancel")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("ErrorHandler err = %v, want context.Canceled", gotErr)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("cancel took %v, want backoff to be interrupted promptly", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (no retry after cancel)", got)
	}

	select {
	case out := <-completed:
		if out.Result != TaskFailed || !errors.Is(out.Err, context.Canceled) {
			t.Fatalf("Completed outcome = {%v, %v}, want TaskFailed with context.Canceled", out.Result, out.Err)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("Completed hook was not called")
	}

	p.WaitIdle()
	closeAndWait(t, p)
}

// ctx 自身的 deadline 在退避期间到期：停止重试，返回 DeadlineExceeded。
func TestTaskWithRetryCtxDeadlineStopsRetries(t *testing.T) {
	var calls atomic.Int64
	errCh := make(chan error, 1)

	p := newRetryTestPool(t, func(c *Config) {
		c.ErrorHandler = func(_ context.Context, err error) { errCh <- err }
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := p.SubmitCtx(ctx, TaskWithRetry(func(context.Context) error {
		calls.Add(1)
		return errors.New("retry me")
	}, RetryPolicy{RetryNum: 5, MinBackOff: 10 * time.Second, MaxBackOff: 10 * time.Second})); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}

	start := time.Now()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ErrorHandler err = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("ErrorHandler was not called after ctx deadline")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("deadline handling took %v, want it bounded by ctx deadline", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (no retry after ctx deadline)", got)
	}

	p.WaitIdle()
	closeAndWait(t, p)
}

// panic 默认不重试：只执行一次，交给池的 recover，终态 TaskPanicked。
func TestTaskWithRetryPanicIsNotRetried(t *testing.T) {
	var calls, errCalls atomic.Int64
	panicCh := make(chan any, 1)
	completed := make(chan TaskOutcome, 1)

	p := newRetryTestPool(t, func(c *Config) {
		c.PanicHandler = func(_ context.Context, recovered any, _ []byte) { panicCh <- recovered }
		c.ErrorHandler = func(context.Context, error) { errCalls.Add(1) }
		c.Hooks.Completed = func(_ context.Context, out TaskOutcome) { completed <- out }
	})

	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		calls.Add(1)
		panic("boom")
	}, instantRetryPolicy(3))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()

	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (panic must not retry)", got)
	}
	if got := errCalls.Load(); got != 0 {
		t.Fatalf("ErrorHandler calls = %d, want 0 for panic", got)
	}
	if got := p.Completed(); got != 1 {
		t.Fatalf("Completed() = %d, want 1", got)
	}
	if got := p.Panicked(); got != 1 {
		t.Fatalf("Panicked() = %d, want 1", got)
	}

	select {
	case recovered := <-panicCh:
		if recovered != "boom" {
			t.Fatalf("PanicHandler recovered = %v, want boom", recovered)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("PanicHandler was not called")
	}

	select {
	case out := <-completed:
		if out.Result != TaskPanicked {
			t.Fatalf("Completed hook result = %v, want TaskPanicked", out.Result)
		}
	case <-time.After(retryTestTimeout):
		t.Fatal("Completed hook was not called")
	}

	closeAndWait(t, p)
}

func TestTaskWithRetryNilFunc(t *testing.T) {
	if got := TaskWithRetry(nil, RetryPolicy{RetryNum: 3}); got != nil {
		t.Fatal("TaskWithRetry(nil) must return nil so Submit reports ErrNilTask")
	}

	p := newRetryTestPool(t, nil)
	if err := p.Submit(TaskWithRetry(nil, instantRetryPolicy(2))); !errors.Is(err, ErrNilTask) {
		t.Fatalf("Submit(TaskWithRetry(nil)) = %v, want ErrNilTask", err)
	}
	if got := p.Pending(); got != 0 {
		t.Fatalf("Pending = %d, want 0 after nil task rejection", got)
	}
	closeAndWait(t, p)
}

// 自定义策略完全接管退避：接收 1 起始的 attempt 与已归一化的 min/max。
func TestTaskWithRetryCustomBackOffStrategy(t *testing.T) {
	var (
		calls atomic.Int64
		mu    sync.Mutex
		seen  []int
		mins  []time.Duration
		maxs  []time.Duration
	)

	p := newRetryTestPool(t, nil)
	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("retry")
		}
		return nil
	}, RetryPolicy{
		RetryNum: 5,
		BackOffStrategy: func(attempt int, min, max time.Duration) time.Duration {
			mu.Lock()
			seen = append(seen, attempt)
			mins = append(mins, min)
			maxs = append(maxs, max)
			mu.Unlock()
			return 0
		},
	})); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()

	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 2 {
		t.Fatalf("strategy attempts = %v, want [1 2]", seen)
	}
	for i := range mins {
		if mins[i] != defaultRetryMinBackOff || maxs[i] != defaultRetryMaxBackOff {
			t.Fatalf("strategy bounds[%d] = (%v, %v), want defaults (%v, %v)",
				i, mins[i], maxs[i], defaultRetryMinBackOff, defaultRetryMaxBackOff)
		}
	}

	closeAndWait(t, p)
}

func TestRetryPolicyBackOffScheduleAndDefaults(t *testing.T) {
	p := (RetryPolicy{RetryNum: 5}).normalized()
	if p.MinBackOff != defaultRetryMinBackOff || p.MaxBackOff != defaultRetryMaxBackOff {
		t.Fatalf("defaults = (%v, %v), want (%v, %v)",
			p.MinBackOff, p.MaxBackOff, defaultRetryMinBackOff, defaultRetryMaxBackOff)
	}

	wantDefaults := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second,
		time.Second,
	}
	for i, want := range wantDefaults {
		if got := p.backOffDelay(i + 1); got != want {
			t.Fatalf("default backOffDelay(%d) = %v, want %v", i+1, got, want)
		}
	}

	small := (RetryPolicy{RetryNum: 3, MinBackOff: 10 * time.Millisecond, MaxBackOff: 25 * time.Millisecond}).normalized()
	wantSmall := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	for i, want := range wantSmall {
		if got := small.backOffDelay(i + 1); got != want {
			t.Fatalf("small backOffDelay(%d) = %v, want %v", i+1, got, want)
		}
	}

	// MaxBackOff < MinBackOff 时提升到 MinBackOff，负 RetryNum 归零。
	clamped := (RetryPolicy{RetryNum: -1, MinBackOff: 50 * time.Millisecond, MaxBackOff: time.Millisecond}).normalized()
	if clamped.RetryNum != 0 {
		t.Fatalf("negative RetryNum normalized = %d, want 0", clamped.RetryNum)
	}
	if clamped.MaxBackOff != 50*time.Millisecond {
		t.Fatalf("MaxBackOff normalized = %v, want 50ms", clamped.MaxBackOff)
	}

	// 自定义策略的返回值直接生效；负值按 0 处理。
	custom := RetryPolicy{
		RetryNum: 1,
		BackOffStrategy: func(attempt int, min, max time.Duration) time.Duration {
			if attempt != 1 || min != 7*time.Millisecond || max != 9*time.Millisecond {
				t.Fatalf("strategy args = (%d, %v, %v), want (1, 7ms, 9ms)", attempt, min, max)
			}
			return 3 * time.Millisecond
		},
		MinBackOff: 7 * time.Millisecond,
		MaxBackOff: 9 * time.Millisecond,
	}.normalized()
	if got := custom.backOffDelay(1); got != 3*time.Millisecond {
		t.Fatalf("custom backOffDelay = %v, want 3ms", got)
	}

	negative := RetryPolicy{
		RetryNum:        1,
		BackOffStrategy: func(int, time.Duration, time.Duration) time.Duration { return -time.Second },
	}.normalized()
	if got := negative.backOffDelay(1); got != 0 {
		t.Fatalf("negative strategy delay = %v, want 0", got)
	}
}

// 一个逻辑任务的 5 个 hook 各触发一次：重试尝试不会重复触发生命周期事件。
func TestTaskWithRetryHooksFireOncePerLogicalTask(t *testing.T) {
	var submitted, enqueued, started, completed, skipped, calls atomic.Int64
	completedCh := make(chan struct{}, 1)

	p := newRetryTestPool(t, func(c *Config) {
		c.Hooks.Submitted = func(context.Context) { submitted.Add(1) }
		c.Hooks.Enqueued = func(context.Context) { enqueued.Add(1) }
		c.Hooks.Started = func(context.Context) { started.Add(1) }
		c.Hooks.Completed = func(_ context.Context, out TaskOutcome) {
			completed.Add(1)
			if out.Result == TaskCompleted {
				select {
				case completedCh <- struct{}{}:
				default:
				}
			}
		}
		c.Hooks.Skipped = func(context.Context, TaskOutcome) { skipped.Add(1) }
	})

	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("retry")
		}
		return nil
	}, instantRetryPolicy(3))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()
	waitForSignal(t, completedCh, "Completed hook")

	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
	if got := submitted.Load(); got != 1 {
		t.Fatalf("Submitted hook calls = %d, want 1", got)
	}
	if got := enqueued.Load(); got != 1 {
		t.Fatalf("Enqueued hook calls = %d, want 1", got)
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("Started hook calls = %d, want 1", got)
	}
	if got := completed.Load(); got != 1 {
		t.Fatalf("Completed hook calls = %d, want 1", got)
	}
	if got := skipped.Load(); got != 0 {
		t.Fatalf("Skipped hook calls = %d, want 0", got)
	}

	closeAndWait(t, p)
}

// 重试期间 pending 始终只计 1（一个逻辑任务），不会随尝试次数增长。
func TestTaskWithRetryKeepsPendingOnceAcrossAttempts(t *testing.T) {
	p := newRetryTestPool(t, func(c *Config) {
		c.Capacity = 1
	})

	var calls, firstPending, secondPending atomic.Int64
	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		switch calls.Add(1) {
		case 1:
			firstPending.Store(int64(p.Pending()))
			return errors.New("retry")
		default:
			secondPending.Store(int64(p.Pending()))
			return nil
		}
	}, instantRetryPolicy(5))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()

	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
	if got := firstPending.Load(); got != 1 {
		t.Fatalf("Pending during first attempt = %d, want 1", got)
	}
	if got := secondPending.Load(); got != 1 {
		t.Fatalf("Pending during second attempt = %d, want 1", got)
	}
	if got := p.Pending(); got != 0 {
		t.Fatalf("Pending after WaitIdle = %d, want 0", got)
	}
	if got := p.Completed(); got != 1 {
		t.Fatalf("Completed() = %d, want 1 logical task", got)
	}

	closeAndWait(t, p)
}

func TestTaskWithRetryConcurrentLogicalTasks(t *testing.T) {
	const tasks = 20
	p := newRetryTestPool(t, func(c *Config) {
		c.Capacity = 4
	})

	var calls atomic.Int64
	for i := 0; i < tasks; i++ {
		var attempts atomic.Int64
		if err := p.Submit(TaskWithRetry(func(context.Context) error {
			calls.Add(1)
			if attempts.Add(1) < 3 {
				return errors.New("retry")
			}
			return nil
		}, instantRetryPolicy(3))); err != nil {
			t.Fatalf("Submit(%d): %v", i, err)
		}
	}
	p.WaitIdle()

	if got := calls.Load(); got != tasks*3 {
		t.Fatalf("calls = %d, want %d", got, tasks*3)
	}
	if got := p.Completed(); got != tasks {
		t.Fatalf("Completed() = %d, want %d logical tasks", got, tasks)
	}
	if got := p.Pending(); got != 0 {
		t.Fatalf("Pending = %d, want 0", got)
	}

	closeAndWait(t, p)
}

// SubmitBefore 的 deadline 只约束“开始执行”：任务开始后即使 deadline 已过，
// 重试仍会继续（只受 ctx 约束）。
func TestTaskWithRetryIgnoresSubmitBeforeDeadlineAfterStart(t *testing.T) {
	p := newRetryTestPool(t, func(c *Config) {
		c.Capacity = 1
	})

	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})

	fn := func(context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return errors.New("first attempt failed")
		}
		return nil
	}

	if err := p.SubmitBefore(TaskWithRetry(fn, instantRetryPolicy(1)), 500*time.Millisecond); err != nil {
		t.Fatalf("SubmitBefore: %v", err)
	}

	waitForSignal(t, started, "first attempt")
	// 让 SubmitBefore 的期限在任务执行期间过去；任务已经开始，期限不再生效。
	time.Sleep(600 * time.Millisecond)
	close(release)
	p.WaitIdle()

	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2 (retry must continue after start deadline)", got)
	}

	closeAndWait(t, p)
}

// 包装函数自身在 ctx 已取消时不应调用 fn（池的正常路径会在 Started 前跳过，
// 这里直接单测包装函数的前置检查）。
func TestTaskWithRetryCanceledContextSkipsExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls atomic.Int64
	wrapped := TaskWithRetry(func(context.Context) error {
		calls.Add(1)
		return nil
	}, instantRetryPolicy(2))

	if err := wrapped(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wrapped(canceled ctx) = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("calls = %d, want 0", got)
	}
}

// 默认退避确实会在两次尝试之间等待，而不是立即重试。
func TestTaskWithRetryWaitsBetweenAttempts(t *testing.T) {
	p := newRetryTestPool(t, nil)

	var calls atomic.Int64
	start := time.Now()
	if err := p.Submit(TaskWithRetry(func(context.Context) error {
		if calls.Add(1) == 1 {
			return errors.New("retry")
		}
		return nil
	}, RetryPolicy{
		RetryNum:   1,
		MinBackOff: 60 * time.Millisecond,
		MaxBackOff: 60 * time.Millisecond,
	})); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	p.WaitIdle()

	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("retry completed in %v, want at least the 60ms backoff", elapsed)
	}

	closeAndWait(t, p)
}

func TestWaitBackOff(t *testing.T) {
	if err := waitBackOff(context.Background(), 0); err != nil {
		t.Fatalf("waitBackOff(0) = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitBackOff(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitBackOff(canceled, 1h) = %v, want context.Canceled", err)
	}
}
