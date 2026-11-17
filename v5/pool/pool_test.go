package pool

import (
	"context"
	"errors"
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
	p.Close()

	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait blocked; possible lost worker wakeup or leaked dispatcher")
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
