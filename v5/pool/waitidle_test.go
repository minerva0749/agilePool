package pool

// T4：任务级 WaitIdle 的回归测试。
//
// 语义（2026-11-22 确认）：
//   - 等待"已接受"以及"正在提交"（已进入 Submit、仍阻塞在满队列上）的任务
//     全部到达终态；
//   - 不关闭池，返回后仍可继续提交；
//   - 一批任务完成后可以再次等待下一批；
//   - 多个 goroutine 可以同时等待同一批；
//   - Close 之后 WaitIdle 仍可等待已接受任务归零；WaitPool 仍要求先 Close。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func startWaitIdle(p *Pool) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		p.WaitIdle()
		close(done)
	}()
	return done
}

func expectWaitIdleDone(t *testing.T, done <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

func expectWaitIdleBlocked(t *testing.T, done <-chan struct{}, window time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(msg)
	case <-time.After(window):
	}
}

// 空池调用立即返回，不需要先 Close。
func TestWaitIdleReturnsImmediatelyOnEmptyPool(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	expectWaitIdleDone(t, startWaitIdle(p), 2*time.Second,
		"WaitIdle blocked on an empty pool")
}

// 不 Close 也能等到当前批次结束；返回后池仍可继续提交，下一批可以再次等待。
func TestWaitIdleWaitsForBatchWithoutClosing(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: time.Second,
		MaxChunkLen: 8,
		MaxBuffered: 32,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	release := make(chan struct{})
	started := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		if err := p.Submit(func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("task did not start")
		}
	}

	done := startWaitIdle(p)
	expectWaitIdleBlocked(t, done, 30*time.Millisecond,
		"WaitIdle returned while tasks were still running")

	close(release)
	expectWaitIdleDone(t, done, 5*time.Second,
		"WaitIdle did not return after the batch finished")
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitIdle, want 0", got)
	}

	// 池没有被关闭：第二批提交与等待都必须正常。
	ran := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(ran)
		return nil
	}); err != nil {
		t.Fatalf("Submit after WaitIdle: %v", err)
	}
	expectWaitIdleDone(t, startWaitIdle(p), 5*time.Second,
		"WaitIdle did not wait for the second batch")
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("second batch task did not run")
	}
}

// 同一批任务可以被多个 goroutine 同时等待，全部都会被唤醒。
func TestWaitIdleMultipleWaiters(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 16,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	release := make(chan struct{})
	started := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		if err := p.Submit(func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		<-started
	}

	const waiters = 8
	done := make([]<-chan struct{}, waiters)
	for i := range done {
		done[i] = startWaitIdle(p)
	}
	expectWaitIdleBlocked(t, done[0], 30*time.Millisecond,
		"a waiter returned while tasks were still running")

	close(release)
	for _, ch := range done {
		expectWaitIdleDone(t, ch, 5*time.Second,
			"waiter did not return after the batch finished")
	}
}

// WaitIdle 必须覆盖"正在提交"：producer 阻塞在满队列上时不能提前返回。
func TestWaitIdleWaitsForInflightSubmit(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 1,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

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

	// A 被 dispatcher 持有，B 填满 MaxBuffered=1，C 阻塞在 Submit 里。
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit held task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit filling task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 1 })

	submitStarted := make(chan struct{})
	submitErr := make(chan error, 1)
	go func() {
		close(submitStarted)
		submitErr <- p.Submit(func(context.Context) error { return nil })
	}()
	<-submitStarted
	select {
	case err := <-submitErr:
		t.Fatalf("Submit on a full BLOCK queue returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	done := startWaitIdle(p)
	expectWaitIdleBlocked(t, done, 30*time.Millisecond,
		"WaitIdle returned while a Submit was still blocked on the full queue")

	// 释放 worker 后，C 会被接受，所有任务执行完 WaitIdle 才返回。
	close(release)
	select {
	case err := <-submitErr:
		if err != nil {
			t.Fatalf("blocked Submit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Submit did not return after space was freed")
	}
	expectWaitIdleDone(t, done, 5*time.Second,
		"WaitIdle did not return after the inflight Submit and its tasks finished")
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitIdle, want 0", got)
	}
}

// Close 后正在提交的任务会被拒绝；WaitIdle 不能因此永久阻塞。
func TestWaitIdleAfterCloseWithBlockedSubmit(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 1,
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

	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit held task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit filling task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 1 })

	submitStarted := make(chan struct{})
	submitErr := make(chan error, 1)
	go func() {
		close(submitStarted)
		submitErr <- p.Submit(func(context.Context) error { return nil })
	}()
	<-submitStarted
	select {
	case err := <-submitErr:
		t.Fatalf("Submit on a full BLOCK queue returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	done := startWaitIdle(p)
	p.Close()

	select {
	case err := <-submitErr:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("blocked Submit after Close = %v, want ErrPoolClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Submit did not return after Close")
	}

	// Close 只是停止接收，已接受任务仍要执行完 WaitIdle 才返回。
	expectWaitIdleBlocked(t, done, 30*time.Millisecond,
		"WaitIdle returned while accepted tasks were still pending after Close")

	close(release)
	expectWaitIdleDone(t, done, 5*time.Second,
		"WaitIdle did not return after Close drained the accepted tasks")

	p.WaitPool()
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitPool, want 0", got)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after WaitPool, want 0", got)
	}
}

// Close → WaitIdle → WaitPool 的顺序可用；WaitPool 后重复 WaitIdle 立即返回。
func TestCloseWaitIdleWaitPool(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: time.Second,
		MaxChunkLen: 8,
		MaxBuffered: 32,
		Mode:        BLOCK,
	})

	const total = 20
	var ran atomic.Int64
	for i := 0; i < total; i++ {
		if err := p.Submit(func(context.Context) error {
			ran.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	p.Close()
	expectWaitIdleDone(t, startWaitIdle(p), 5*time.Second,
		"WaitIdle did not return after Close drained the accepted tasks")
	if got := ran.Load(); got != total {
		t.Fatalf("executed = %d, want %d", got, total)
	}
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitIdle, want 0", got)
	}

	p.WaitPool()
	expectWaitIdleDone(t, startWaitIdle(p), 2*time.Second,
		"WaitIdle blocked after WaitPool")
}

// dispatcher 持有已取消任务时，WaitIdle 要等它真正进入终态（T5 之后会更及时）。
func TestWaitIdleWithCanceledHeldTask(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

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

	ctx, cancel := context.WithCancel(context.Background())
	var canceledRan atomic.Int64
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		canceledRan.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })

	done := startWaitIdle(p)
	expectWaitIdleBlocked(t, done, 20*time.Millisecond,
		"WaitIdle returned while a held task was still pending")

	cancel()
	close(release)
	expectWaitIdleDone(t, done, 5*time.Second,
		"WaitIdle did not return after the canceled task reached a terminal state")
	if got := canceledRan.Load(); got != 0 {
		t.Fatalf("canceled task executed %d times, want 0", got)
	}
}

// panic 任务到达终态后 WaitIdle 返回，worker 仍然存活。
func TestWaitIdleWithPanicTask(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	panicked := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(panicked)
		panic("waitidle panic")
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-panicked

	expectWaitIdleDone(t, startWaitIdle(p), 5*time.Second,
		"WaitIdle did not return after the panicking task finished")

	ran := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(ran)
		return nil
	}); err != nil {
		t.Fatalf("Submit after panic: %v", err)
	}
	expectWaitIdleDone(t, startWaitIdle(p), 5*time.Second,
		"WaitIdle did not return for the follow-up batch")
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not survive the panicking task")
	}
}

// 并发 producer 与反复 WaitIdle 并行：不能死锁，全部提交完成后最终归零。
func TestWaitIdleWithConcurrentProducers(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    8,
		IdleTimeout: time.Second,
		MaxChunkLen: 8,
		MaxBuffered: 64,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	const producers, perProducer = 4, 50
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				if err := p.Submit(func(context.Context) error { return nil }); err != nil {
					return
				}
			}
		}()
	}

	producersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(producersDone)
	}()

	for finished := false; !finished; {
		expectWaitIdleDone(t, startWaitIdle(p), 5*time.Second,
			"WaitIdle deadlocked while producers were submitting")
		select {
		case <-producersDone:
			finished = true
		default:
			time.Sleep(time.Millisecond)
		}
	}

	expectWaitIdleDone(t, startWaitIdle(p), 5*time.Second,
		"final WaitIdle did not return after producers finished")
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after final WaitIdle, want 0", got)
	}
}

// pendingTracker 的 submitting 计数：WaitIdle 等它，pending 指标不含它。
func TestPendingTrackerWaitsForSubmitting(t *testing.T) {
	tr := newPendingTracker()

	tr.beginSubmit()
	if got := tr.snapshot(); got != 0 {
		t.Fatalf("snapshot() = %d while only a Submit is inflight, want 0", got)
	}
	if got := tr.submittingSnapshot(); got != 1 {
		t.Fatalf("submittingSnapshot() = %d, want 1", got)
	}

	done := make(chan struct{})
	go func() {
		tr.wait()
		close(done)
	}()
	expectWaitIdleBlocked(t, done, 20*time.Millisecond,
		"wait returned while a Submit was still inflight")

	tr.endSubmit()
	expectWaitIdleDone(t, done, 2*time.Second,
		"wait did not return after the inflight Submit ended")
}

// 一次 endSubmit 与一次 done 都在等待对方：只有两个计数同时为 0 才唤醒。
func TestPendingTrackerWaitsForBothCounts(t *testing.T) {
	tr := newPendingTracker()
	tr.add()
	tr.beginSubmit()

	done := make(chan struct{})
	go func() {
		tr.wait()
		close(done)
	}()

	tr.endSubmit()
	expectWaitIdleBlocked(t, done, 20*time.Millisecond,
		"wait returned while an accepted task was still pending")

	tr.done()
	expectWaitIdleDone(t, done, 2*time.Second,
		"wait did not return after both counts reached zero")
}
