package pool

// T7：运行指标的回归测试。
//
// 覆盖点：
//   - 空池、积压、执行、idle、关闭后五种场景下的 Pending/Queued/Running/
//     Idle/Busy/Capacity/WorkerCreateCount；
//   - 五个累计计数（Submitted/Rejected/Completed/Dropped/Panicked）的定义；
//   - 计数与 T6 hook 的一致性、关闭后的恒等式；
//   - 指标可从 hook 内读取，且测试覆盖并发读取。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func assertGauges(t *testing.T, p *Pool, pending, queued, running, idle, busy int) {
	t.Helper()
	if got := p.Pending(); got != pending {
		t.Fatalf("Pending() = %d, want %d", got, pending)
	}
	if got := p.Queued(); got != queued {
		t.Fatalf("Queued() = %d, want %d", got, queued)
	}
	if got := p.Running(); got != running {
		t.Fatalf("Running() = %d, want %d", got, running)
	}
	if got := p.Idle(); got != idle {
		t.Fatalf("Idle() = %d, want %d", got, idle)
	}
	if got := p.Busy(); got != busy {
		t.Fatalf("Busy() = %d, want %d", got, busy)
	}
}

func assertCounters(t *testing.T, p *Pool, submitted, rejected, completed, dropped, panicked int64) {
	t.Helper()
	if got := p.Submitted(); got != submitted {
		t.Fatalf("Submitted() = %d, want %d", got, submitted)
	}
	if got := p.Rejected(); got != rejected {
		t.Fatalf("Rejected() = %d, want %d", got, rejected)
	}
	if got := p.Completed(); got != completed {
		t.Fatalf("Completed() = %d, want %d", got, completed)
	}
	if got := p.Dropped(); got != dropped {
		t.Fatalf("Dropped() = %d, want %d", got, dropped)
	}
	if got := p.Panicked(); got != panicked {
		t.Fatalf("Panicked() = %d, want %d", got, panicked)
	}
}

// 空池：所有瞬时指标为 0，Capacity 来自配置，累计计数为 0。
func TestMetricsEmptyPool(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	assertGauges(t, p, 0, 0, 0, 0, 0)
	if got := p.Capacity(); got != 4 {
		t.Fatalf("Capacity() = %d, want 4", got)
	}
	if got := p.WorkerCreateCount(); got != 0 {
		t.Fatalf("WorkerCreateCount() = %d, want 0", got)
	}
	assertCounters(t, p, 0, 0, 0, 0, 0)

	closeAndWait(t, p)

	// Close/Wait 后仍然全 0；Capacity 与累计计数不回落。
	assertGauges(t, p, 0, 0, 0, 0, 0)
	if got := p.Capacity(); got != 4 {
		t.Fatalf("Capacity() after Close = %d, want 4", got)
	}
	assertCounters(t, p, 0, 0, 0, 0, 0)
}

// 积压场景：2 个 worker 被占用、dispatcher 持有一个任务、队列里还有 2 个。
// Pending 包含 dispatcher 持有与执行中的任务，Queued 只算队列里的。
func TestMetricsBacklogAndBusy(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	for i := 0; i < 2; i++ {
		if err := p.Submit(func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}); err != nil {
			t.Fatalf("Submit blocker: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("blocker did not start")
		}
	}

	// 再提交 3 个：dispatcher 取走 1 个等 worker，另外 2 个留在队列。
	for i := 0; i < 3; i++ {
		if err := p.Submit(noopTask); err != nil {
			t.Fatalf("Submit backlog task: %v", err)
		}
	}
	waitUntil(t, 2*time.Second, func() bool { return p.Queued() == 2 })

	assertGauges(t, p, 5, 2, 2, 0, 2)
	if got := p.Capacity(); got != 2 {
		t.Fatalf("Capacity() = %d, want 2", got)
	}
	if got := p.WorkerCreateCount(); got != 2 {
		t.Fatalf("WorkerCreateCount() = %d, want 2", got)
	}

	close(release)
	p.WaitIdle()

	// 任务全部完成后、worker 仍存活：Busy=0，两个 worker 回到 idle。
	waitUntil(t, 2*time.Second, func() bool {
		return p.Pending() == 0 && p.Queued() == 0 && p.Busy() == 0 && p.Running() == 2
	})
	if got := p.Idle(); got != 2 {
		t.Fatalf("Idle() = %d after drain, want 2", got)
	}

	closeAndWait(t, p)
	assertGauges(t, p, 0, 0, 0, 0, 0)
	assertCounters(t, p, 5, 0, 5, 0, 0)
}

// 执行中：唯一 worker 正在跑任务；完成后回到 idle。
func TestMetricsBusyWhileExecuting(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
	})

	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-started

	assertGauges(t, p, 1, 0, 1, 0, 1)

	close(release)
	waitPendingZero(t, p, 2*time.Second)
	waitUntil(t, 2*time.Second, func() bool {
		return p.Idle() == 1 && p.Busy() == 0
	})
	assertGauges(t, p, 0, 0, 1, 1, 0)

	closeAndWait(t, p)
}

// WorkerCreateCount 是单调累计值：worker 因 idle timeout 退出后不回落，
// 下一次提交会创建新的 worker 并继续累加。
func TestWorkerCreateCountMonotonic(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
	})

	firstDone := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		firstDone <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit first: %v", err)
	}
	<-firstDone
	waitUntil(t, 2*time.Second, func() bool { return p.Running() == 0 })

	if got := p.WorkerCreateCount(); got != 1 {
		t.Fatalf("WorkerCreateCount() = %d after the first worker exited, want 1", got)
	}
	assertGauges(t, p, 0, 0, 0, 0, 0)

	secondDone := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		secondDone <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit second: %v", err)
	}
	<-secondDone

	waitUntil(t, 2*time.Second, func() bool { return p.WorkerCreateCount() == 2 })
	closeAndWait(t, p)

	if got := p.WorkerCreateCount(); got != 2 {
		t.Fatalf("WorkerCreateCount() = %d after Close, want 2", got)
	}
}

// completed / error / panic 都计入 Completed；panic 同时计入 Panicked。
func TestCountersCompletedAndPanicked(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    3,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
		Logger:      &captureLogger{},
	})

	done := make(chan struct{}, 3)
	for _, fn := range []TaskFunc{
		func(context.Context) error { done <- struct{}{}; return nil },
		func(context.Context) error { done <- struct{}{}; return errors.New("task failed") },
		func(context.Context) error { done <- struct{}{}; panic("task panic") },
	} {
		if err := p.Submit(fn); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("a task did not run")
		}
	}
	waitPendingZero(t, p, 2*time.Second)

	assertCounters(t, p, 3, 0, 3, 0, 1)

	closeAndWait(t, p)
	assertCounters(t, p, 3, 0, 3, 0, 1)
	assertGauges(t, p, 0, 0, 0, 0, 0)
}

// 已接受但取消/超期的任务只计入 Dropped，不计入 Completed。
func TestCountersDropped(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.SubmitCtx(ctx, noopTask); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}
	cancel()
	if err := p.SubmitBefore(noopTask, 20*time.Millisecond); err != nil {
		t.Fatalf("SubmitBefore: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool { return p.Dropped() == 2 })
	assertCounters(t, p, 3, 0, 0, 2, 0)

	close(release)
	waitPendingZero(t, p, 2*time.Second)
	assertCounters(t, p, 3, 0, 1, 2, 0)

	closeAndWait(t, p)
	assertCounters(t, p, 3, 0, 1, 2, 0)
	assertGauges(t, p, 0, 0, 0, 0, 0)
}

// 被拒绝的提交：nil task 与非正 timeout 是参数错误，不计数；
// 池关闭、队列满、ctx 已取消等入队失败计入 Rejected。
func TestCountersRejected(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 1,
		MaxBuffered: 1,
		Mode:        NONBLOCK,
	})

	if err := p.Submit(nil); !errors.Is(err, ErrNilTask) {
		t.Fatalf("Submit(nil) = %v, want ErrNilTask", err)
	}
	if err := p.SubmitBefore(noopTask, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SubmitBefore(0) = %v, want DeadlineExceeded", err)
	}
	assertCounters(t, p, 0, 0, 0, 0, 0)

	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	<-started

	// A 被 dispatcher 持有，B 填满 MaxBuffered=1 的缓冲。
	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("Submit A: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.Queued() == 0 })
	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("Submit B: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.Queued() == 1 })

	if err := p.Submit(noopTask); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("Submit C = %v, want ErrPoolFull", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.SubmitCtx(ctx, noopTask); !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitCtx(canceled) = %v, want context.Canceled", err)
	}

	p.Close()
	if err := p.Submit(noopTask); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit after Close = %v, want ErrPoolClosed", err)
	}

	// blocker + A + B 被接受；C、canceled、after-close 被拒绝。
	assertCounters(t, p, 6, 3, 0, 0, 0)

	close(release)
	p.WaitPool()

	assertCounters(t, p, 6, 3, 3, 0, 0)
	assertGauges(t, p, 0, 0, 0, 0, 0)
}

// 混合终态 + Close/Wait 后所有瞬时指标归零，累计计数满足
// Submitted = Rejected + Completed + Dropped。
func TestMetricsZeroAfterCloseAndDrain(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 16,
		Mode:        BLOCK,
		Logger:      &captureLogger{},
	})

	const total = 10
	var ran atomic.Int64
	for i := 0; i < total; i++ {
		var err error
		switch i % 3 {
		case 0:
			err = p.Submit(func(context.Context) error { ran.Add(1); return nil })
		case 1:
			err = p.Submit(func(context.Context) error { ran.Add(1); return errors.New("failed") })
		case 2:
			err = p.Submit(func(context.Context) error { ran.Add(1); panic("panicked") })
		}
		if err != nil {
			t.Fatalf("Submit[%d]: %v", i, err)
		}
	}

	p.Close()
	p.WaitPool()

	if got := ran.Load(); got != total {
		t.Fatalf("executed = %d, want %d", got, total)
	}
	assertGauges(t, p, 0, 0, 0, 0, 0)
	if got := p.WorkerCreateCount(); got < 1 {
		t.Fatalf("WorkerCreateCount() = %d, want >= 1", got)
	}

	submitted, rejected := p.Submitted(), p.Rejected()
	completed, dropped := p.Completed(), p.Dropped()
	if submitted != rejected+completed+dropped {
		t.Fatalf("Submitted(%d) != Rejected(%d) + Completed(%d) + Dropped(%d)",
			submitted, rejected, completed, dropped)
	}
	if completed != total {
		t.Fatalf("Completed = %d, want %d", completed, total)
	}
	if got := p.Panicked(); got != 3 {
		t.Fatalf("Panicked = %d, want 3", got)
	}
}

// worker 因 idle timeout 退出不会改变累计计数。
func TestCountersSurviveIdleTimeout(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
	})

	done := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		done <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-done
	waitUntil(t, 2*time.Second, func() bool { return p.Running() == 0 })

	assertCounters(t, p, 1, 0, 1, 0, 0)
	assertGauges(t, p, 0, 0, 0, 0, 0)
	if got := p.WorkerCreateCount(); got != 1 {
		t.Fatalf("WorkerCreateCount() = %d, want 1", got)
	}

	closeAndWait(t, p)
}

// 指标可以从 T6 的 hook 内安全读取：若指标路径与 hook 触发路径发生锁重入，
// 这个测试会超时。
func TestMetricsCallableFromHooks(t *testing.T) {
	var calls atomic.Int64
	errCh := make(chan error, 1)

	go func() {
		var p *Pool
		hooks := Hooks{
			Submitted: func(context.Context) {
				calls.Add(1)
				_ = p.Pending()
				_ = p.Busy()
			},
			Enqueued: func(context.Context) {
				calls.Add(1)
				_ = p.Queued()
				_ = p.Submitted()
			},
			Started: func(context.Context) {
				calls.Add(1)
				_ = p.Pending()
				_ = p.Idle()
			},
			Completed: func(context.Context, TaskOutcome) {
				calls.Add(1)
				_ = p.Pending()
				_ = p.Completed()
				_ = p.WorkerCreateCount()
			},
			PoolClosed: func() {
				calls.Add(1)
				_ = p.Pending()
				_ = p.Queued()
				_ = p.Running()
				_ = p.Capacity()
			},
		}

		var err error
		p, err = New(Config{
			Capacity:    1,
			IdleTimeout: time.Second,
			MaxChunkLen: 4,
			MaxBuffered: 4,
			Mode:        BLOCK,
			Hooks:       hooks,
		})
		if err != nil {
			errCh <- err
			return
		}
		if err := p.Submit(noopTask); err != nil {
			errCh <- err
			return
		}
		p.WaitIdle()
		p.Close()
		p.WaitPool()
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("pool failed while hooks read metrics: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metrics read from a hook deadlocked")
	}
	if got := calls.Load(); got < 5 {
		t.Fatalf("hook calls = %d, want at least 5", got)
	}
}

// 并发读取指标与并发提交/完成共存，不 panic、不阻塞；结束后指标归零。
func TestMetricsConcurrentReads(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: time.Second,
		MaxChunkLen: 8,
		MaxBuffered: 64,
		Mode:        BLOCK,
		Logger:      &captureLogger{},
	})

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = p.Pending()
				_ = p.Queued()
				_ = p.Running()
				_ = p.Idle()
				_ = p.Busy()
				_ = p.Capacity()
				_ = p.WorkerCreateCount()
				_ = p.Submitted()
				_ = p.Rejected()
				_ = p.Completed()
				_ = p.Dropped()
				_ = p.Panicked()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	const total = 200
	for i := 0; i < total; i++ {
		var fn TaskFunc
		switch i % 5 {
		case 0:
			fn = func(context.Context) error { return errors.New("failed") }
		case 1:
			fn = func(context.Context) error { panic("panicked") }
		default:
			fn = noopTask
		}
		if err := p.Submit(fn); err != nil {
			t.Fatalf("Submit[%d]: %v", i, err)
		}
	}
	p.WaitIdle()
	p.Close()
	p.WaitPool()
	close(stop)
	readers.Wait()

	assertGauges(t, p, 0, 0, 0, 0, 0)
	if p.Submitted() != p.Rejected()+p.Completed()+p.Dropped() {
		t.Fatalf("counter identity broken: submitted=%d rejected=%d completed=%d dropped=%d",
			p.Submitted(), p.Rejected(), p.Completed(), p.Dropped())
	}
	if got := p.Completed(); got != total {
		t.Fatalf("Completed = %d, want %d", got, total)
	}
	if got := p.Panicked(); got != 40 {
		t.Fatalf("Panicked = %d, want 40", got)
	}
}
