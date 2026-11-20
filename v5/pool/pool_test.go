package pool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestPool(t *testing.T, cfg Config) *Pool {
	t.Helper()
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func waitGroup(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("tasks did not finish before timeout")
	}
}

func closeAndWait(t *testing.T, p *Pool) {
	t.Helper()
	closeAndWaitWithin(t, p, 5*time.Second)
}

func closeAndWaitWithin(t *testing.T, p *Pool, timeout time.Duration) {
	t.Helper()
	p.Close()

	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("Wait blocked after %v; possible lost worker wakeup or leaked dispatcher", timeout)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition was not met before timeout")
	}
}

func TestSubmitExecutesAll(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 16,
		Mode:        BLOCK,
	})

	const total = 200
	var (
		executed atomic.Int64
		wg       sync.WaitGroup
	)
	wg.Add(total)

	for i := 0; i < total; i++ {
		err := p.Submit(func(context.Context) error {
			executed.Add(1)
			wg.Done()
			return nil
		})
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	waitGroup(t, &wg, 5*time.Second)
	closeAndWait(t, p)

	if got := executed.Load(); got != total {
		t.Fatalf("executed = %d, want %d", got, total)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after Wait, want 0", got)
	}
}

// NONBLOCK：满时立即返回 ErrPoolFull；已被接受的任务仍应全部执行。
func TestNonBlockFull(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 1,
		MaxBuffered: 1,
		Mode:        NONBLOCK,
	})

	started := make(chan struct{})
	release := make(chan struct{})
	var executed atomic.Int64

	if err := p.Submit(func(context.Context) error {
		close(started)
		<-release
		executed.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Submit blocking task: %v", err)
	}
	<-started

	accepted := 0
	full := false
	for i := 0; i < 10; i++ {
		err := p.Submit(func(context.Context) error {
			executed.Add(1)
			return nil
		})
		if err == nil {
			accepted++
			continue
		}
		if errors.Is(err, ErrPoolFull) {
			full = true
			break
		}
		t.Fatalf("unexpected Submit error: %v", err)
	}
	if !full {
		t.Fatal("expected ErrPoolFull while the only worker was busy")
	}

	close(release)
	closeAndWait(t, p)

	if got := executed.Load(); got != int64(accepted)+1 {
		t.Fatalf("executed = %d, want %d", got, accepted+1)
	}
}

// Close 后 dispatcher 必须把缓冲区里的任务全部派发/执行完。
func TestCloseDrainsQueued(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 100,
		Mode:        BLOCK,
	})

	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocking task: %v", err)
	}
	<-started

	const queued = 50
	var executed atomic.Int64
	for i := 0; i < queued; i++ {
		if err := p.Submit(func(context.Context) error {
			executed.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("Submit queued task: %v", err)
		}
	}

	p.Close()
	if err := p.Submit(func(context.Context) error { return nil }); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit after Close = %v, want ErrPoolClosed", err)
	}

	close(release)
	closeAndWait(t, p)

	if got := executed.Load(); got != queued {
		t.Fatalf("executed = %d, want %d; Close must drain the buffer", got, queued)
	}
}

func TestSubmitCtxCanceledBeforeSubmit(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var executed atomic.Int64
	err := p.SubmitCtx(ctx, func(context.Context) error {
		executed.Add(1)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitCtx = %v, want context.Canceled", err)
	}

	closeAndWait(t, p)
	if got := executed.Load(); got != 0 {
		t.Fatalf("executed = %d, want 0", got)
	}
}

// deadline 是“开始执行”的期限：排队超过 deadline 的任务不能执行。
func TestSubmitBeforeExpiredIsSkipped(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 100 * time.Millisecond,
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
		t.Fatalf("Submit blocking task: %v", err)
	}
	<-started

	var expiredRan atomic.Int64
	if err := p.SubmitBefore(func(context.Context) error {
		expiredRan.Add(1)
		return nil
	}, 20*time.Millisecond); err != nil {
		t.Fatalf("SubmitBefore: %v", err)
	}

	// 等 deadline 过去，再放行 worker。任务此时要么还在队列里被
	// dispatcher 丢弃，要么已经派发但在 runTask 里被跳过。
	time.Sleep(50 * time.Millisecond)
	p.Close()
	close(release)
	closeAndWait(t, p)

	if got := expiredRan.Load(); got != 0 {
		t.Fatalf("expired task executed %d times, want 0", got)
	}
}

func TestSubmitBeforeNonPositiveTimeout(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	for _, timeout := range []time.Duration{0, -time.Millisecond} {
		err := p.SubmitBefore(func(context.Context) error { return nil }, timeout)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("SubmitBefore(%v) = %v, want DeadlineExceeded", timeout, err)
		}
	}
}

// worker 空闲超时退出后，后续 Submit 仍能创建新 worker。
// 这条测试覆盖“容量槽释放没有唤醒”的 liveness 窗口。
func TestIdleTimeoutThenSubmit(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Millisecond,
		MaxChunkLen: 1,
		MaxBuffered: 1,
		Mode:        BLOCK,
	})

	for i := 0; i < 20; i++ {
		done := make(chan struct{}, 1)
		if err := p.Submit(func(context.Context) error {
			done <- struct{}{}
			return nil
		}); err != nil {
			t.Fatalf("round %d Submit: %v", i, err)
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d task did not run", i)
		}
		waitUntil(t, 2*time.Second, func() bool {
			return p.Running() == 0
		})
	}

	closeAndWait(t, p)
}

func TestPanicIsolation(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	if err := p.Submit(func(context.Context) error {
		panic("boom")
	}); err != nil {
		t.Fatalf("Submit panic task: %v", err)
	}

	ran := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		ran <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit follow-up task: %v", err)
	}

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not survive a panicking task")
	}

	closeAndWait(t, p)
}

// SubmitCtx 在缓冲满、等待空间期间被取消：必须返回 ctx.Err，
// 且这个任务不能入队、不能执行。
func TestSubmitCtxCancelWhileBufferFull(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 1,
		MaxBuffered: 1,
		Mode:        BLOCK,
	})

	// 第一个任务占住唯一的 worker。
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocking task: %v", err)
	}
	<-started

	// 第二个任务会被 dispatcher 从队列取走，并卡在等唯一 worker 上。
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit held task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return p.queue.size() == 0
	})

	// 第三个任务填满 MaxBuffered=1 的缓冲；dispatcher 还拿着第二个任务，
	// 不会再来 pop，所以此时缓冲确实是满的。
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit filling task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return p.queue.size() == 1
	})

	var canceledRan atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.SubmitCtx(ctx, func(context.Context) error {
			canceledRan.Add(1)
			return nil
		})
	}()

	// 给它时间进入 push 的 BLOCK 等待分支；此时不能提前返回。
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("SubmitCtx returned before cancel: %v", err)
	default:
	}
	if got := p.queue.size(); got != 1 {
		t.Fatalf("buffer size before cancel = %d, want 1", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SubmitCtx = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitCtx did not return after cancel")
	}

	close(release)
	closeAndWaitWithin(t, p, 10*time.Second)

	if got := canceledRan.Load(); got != 0 {
		t.Fatalf("canceled task executed %d times, want 0", got)
	}
}

// TestStress100kTasks 覆盖指南要求的 10 万任务 + 突发写入：
//   - 100 个 producer 在同一 barrier 后同时提交，每个 1000 个任务；
//   - 统计实际执行数，必须恰好 100000；
//   - Close/Wait 后 Running()==0；
//   - 用 NumGoroutine 基线检查 dispatcher/worker 没有泄漏。
func TestStress100kTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("100k stress skipped in -short")
	}

	const (
		producers   = 100
		perProducer = 1000
		total       = producers * perProducer
	)

	baseline := runtime.NumGoroutine()

	p := newTestPool(t, Config{
		Capacity:    256,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 64,
		MaxBuffered: 4096,
		Mode:        BLOCK,
	})

	var (
		executed atomic.Int64
		tasksWG  sync.WaitGroup
		submitWG sync.WaitGroup
		errCh    = make(chan error, producers)
	)
	tasksWG.Add(total)

	// start 让所有 producer 同时开跑，模拟突发写入而不是串行灌任务。
	start := make(chan struct{})
	for i := 0; i < producers; i++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			<-start
			for j := 0; j < perProducer; j++ {
				if err := p.Submit(func(context.Context) error {
					executed.Add(1)
					tasksWG.Done()
					return nil
				}); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}
	close(start)

	submitWG.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("Submit: %v", err)
	default:
	}

	waitGroup(t, &tasksWG, 30*time.Second)
	closeAndWaitWithin(t, p, 30*time.Second)

	if got := executed.Load(); got != total {
		t.Fatalf("executed = %d, want exactly %d", got, total)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after Close/Wait, want 0", got)
	}

	// Wait 返回说明 dispatcher 和 worker 的逻辑退出路径已经走完；
	// 这里再用 NumGoroutine 做一次基线检查，容差留给 testing/GC 的辅助 goroutine。
	runtime.GC()
	waitUntil(t, 5*time.Second, func() bool {
		return runtime.NumGoroutine() <= baseline+10
	})
	t.Logf("goroutines: baseline=%d after=%d", baseline, runtime.NumGoroutine())
}
