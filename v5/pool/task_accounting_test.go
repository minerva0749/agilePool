package pool

// T3：任务所有权、执行结果与 pending 记账的回归测试。
//
// 这些测试只通过包内可见的 pendingTracker 观察计数：公开的 Pending()
// 指标属于 T7，公开的 WaitIdle() 属于 T4，本阶段都不提前实现。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func pendingOf(p *Pool) int { return p.pending.snapshot() }

func waitPendingZero(t *testing.T, p *Pool, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, func() bool { return pendingOf(p) == 0 })
}

// 已接受的任务在开始执行后必须仍然计入 pending，执行完成后归零。
func TestPendingTracksAcceptedTask(t *testing.T) {
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
		t.Fatalf("Submit: %v", err)
	}
	<-started

	if got := pendingOf(p); got != 1 {
		t.Fatalf("pending = %d while the only task is running, want 1", got)
	}

	close(release)
	waitPendingZero(t, p, 2*time.Second)
	closeAndWait(t, p)

	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitPool, want 0", got)
	}
}

// 正常完成、返回 error、panic 三种 worker 侧终态都必须恰好扣一次 pending。
func TestPendingDrainsOnErrorAndPanic(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	taskDone := make(chan struct{}, 3)
	var completed, failed, panicked atomic.Int64

	mustSubmit := func(fn TaskFunc) {
		t.Helper()
		if err := p.Submit(fn); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	mustSubmit(func(context.Context) error {
		completed.Add(1)
		taskDone <- struct{}{}
		return nil
	})
	mustSubmit(func(context.Context) error {
		failed.Add(1)
		taskDone <- struct{}{}
		return errors.New("task failed")
	})
	mustSubmit(func(context.Context) error {
		panicked.Add(1)
		taskDone <- struct{}{}
		panic("task panic")
	})

	for i := 0; i < 3; i++ {
		select {
		case <-taskDone:
		case <-time.After(2 * time.Second):
			t.Fatal("a task did not run")
		}
	}
	waitPendingZero(t, p, 2*time.Second)

	if completed.Load() != 1 || failed.Load() != 1 || panicked.Load() != 1 {
		t.Fatalf("executed completed=%d failed=%d panicked=%d, want 1/1/1",
			completed.Load(), failed.Load(), panicked.Load())
	}

	closeAndWait(t, p)
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitPool, want 0", got)
	}
}

// SubmitBefore 已超期、最终被 dispatcher 跳过的任务也必须恰好在跳过时扣账。
func TestPendingDrainsOnDeadlineSkipped(t *testing.T) {
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

	// 让 deadline 真正过去；这不是用 sleep 代替状态同步，而是把
	// “期限已过”这个时间条件显式等待出来。
	deadlinePassed := time.Now().Add(60 * time.Millisecond)
	waitUntil(t, 2*time.Second, func() bool { return time.Now().After(deadlinePassed) })

	if got := pendingOf(p); got != 2 {
		t.Fatalf("pending = %d (blocker + expired task), want 2", got)
	}

	close(release)
	waitPendingZero(t, p, 2*time.Second)

	if got := expiredRan.Load(); got != 0 {
		t.Fatalf("expired task executed %d times, want 0", got)
	}

	closeAndWait(t, p)
}

// dispatcher 已经 pop 并持有一个任务、正在等 worker 时任务被取消：
// 取消后释放 worker，dispatcher 必须走统一终态入口，pending 归零。
func TestPendingDrainsOnCanceledHeldTask(t *testing.T) {
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

	// 队列变空说明 dispatcher 已经 pop 走了任务（worker 被 blocker 占满）。
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })
	if got := pendingOf(p); got != 2 {
		t.Fatalf("pending = %d (blocker + held task), want 2", got)
	}

	cancel()
	close(release)
	waitPendingZero(t, p, 2*time.Second)

	if got := canceledRan.Load(); got != 0 {
		t.Fatalf("canceled held task executed %d times, want 0", got)
	}

	closeAndWait(t, p)
}

// 已取消的任务排在队列里，而 dispatcher 正持有前一个任务等 worker：
// dispatcher 之后 pop 到它时走 pop 后的统一终态入口，pending 归零。
func TestPendingDrainsOnCanceledQueuedTask(t *testing.T) {
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
		t.Fatalf("Submit blocking task: %v", err)
	}
	<-started

	// A 被 dispatcher 持有，占住唯一的“派发位”。
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit held task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })

	// B 只能留在队列里；随后取消它。
	ctx, cancel := context.WithCancel(context.Background())
	var queuedRan atomic.Int64
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		queuedRan.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 1 })
	cancel()

	close(release)
	waitPendingZero(t, p, 2*time.Second)

	if got := queuedRan.Load(); got != 0 {
		t.Fatalf("canceled queued task executed %d times, want 0", got)
	}

	closeAndWait(t, p)
}

// 提交被拒绝（NONBLOCK 满、ctx 已取消）不得留下 pending 计数。
func TestPendingNotLeakedByRejectedSubmit(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 1,
		Mode:        NONBLOCK,
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

	// A 被 dispatcher 持有，B 填满 MaxBuffered=1 的缓冲。
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit held task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit filling task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 1 })

	if got := pendingOf(p); got != 3 {
		t.Fatalf("pending = %d (blocker + held + queued), want 3", got)
	}

	if err := p.Submit(func(context.Context) error { return nil }); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("Submit on full buffer = %v, want ErrPoolFull", err)
	}
	if got := pendingOf(p); got != 3 {
		t.Fatalf("pending = %d after ErrPoolFull, want 3", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.SubmitCtx(ctx, func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitCtx with canceled ctx = %v, want context.Canceled", err)
	}
	if got := pendingOf(p); got != 3 {
		t.Fatalf("pending = %d after canceled SubmitCtx, want 3", got)
	}

	close(release)
	closeAndWait(t, p)
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitPool, want 0", got)
	}
}

// 提交中（阻塞在满队列上的 Submit）尚未被接受，不计入 pending（T0 第 1 条）。
func TestPendingExcludesBlockedSubmit(t *testing.T) {
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

	// A 被 dispatcher 持有，B 填满 MaxBuffered=1 的缓冲，pending = 3。
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit held task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })
	if err := p.Submit(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Submit filling task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 1 })
	if got := pendingOf(p); got != 3 {
		t.Fatalf("pending = %d (blocker + held + queued), want 3", got)
	}

	// C 阻塞在满队列上：仍未入队，不得增加 pending。
	submitStarted := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		close(submitStarted)
		errCh <- p.Submit(func(context.Context) error { return nil })
	}()
	<-submitStarted
	select {
	case err := <-errCh:
		t.Fatalf("Submit on a full BLOCK queue returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if got := pendingOf(p); got != 3 {
		t.Fatalf("pending = %d while a Submit is blocked on the full queue, want 3 (submitting is not accepted)", got)
	}

	// 释放 worker，让 C 真正入队并返回 nil，再关闭池。
	close(release)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("blocked Submit after space was freed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Submit did not return after space was freed")
	}

	closeAndWait(t, p)
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitPool, want 0", got)
	}
}

// Close + WaitPool 返回时，所有已接受任务都必须已到达终态。
func TestPendingZeroAfterCloseAndWait(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: time.Second,
		MaxChunkLen: 8,
		MaxBuffered: 64,
		Mode:        BLOCK,
	})

	const total = 50
	var ran atomic.Int64
	for i := 0; i < total; i++ {
		var err error
		switch i % 4 {
		case 0:
			err = p.Submit(func(context.Context) error {
				ran.Add(1)
				return nil
			})
		case 1:
			err = p.Submit(func(context.Context) error {
				ran.Add(1)
				return errors.New("task error")
			})
		case 2:
			err = p.Submit(func(context.Context) error {
				ran.Add(1)
				panic("task panic")
			})
		case 3:
			err = p.SubmitBefore(func(context.Context) error {
				ran.Add(1)
				return nil
			}, 5*time.Millisecond)
		}
		if err != nil {
			t.Fatalf("Submit[%d]: %v", i, err)
		}
	}

	p.Close()
	done := make(chan struct{})
	go func() {
		p.WaitPool()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("WaitPool did not return after Close")
	}

	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after WaitPool, want 0", got)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after WaitPool, want 0", got)
	}
	if got := ran.Load(); got == 0 {
		t.Fatal("no task executed; test setup is broken")
	}
}

// runTask 是结果分类的唯一入口：五类终态必须映射正确，且取消/超期
// 不得调用任务函数。
func TestRunTaskClassifiesOutcomes(t *testing.T) {
	p := &Pool{}
	sentinel := errors.New("boom")

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name      string
		task      task
		want      taskResult
		wantErr   error
		wantPanic bool
	}{
		{
			name: "completed",
			task: task{Fn: func(context.Context) error { return nil }},
			want: resultCompleted,
		},
		{
			name:    "error",
			task:    task{Fn: func(context.Context) error { return sentinel }},
			want:    resultError,
			wantErr: sentinel,
		},
		{
			name:      "panic",
			task:      task{Fn: func(context.Context) error { panic("kaboom") }},
			want:      resultPanic,
			wantPanic: true,
		},
		{
			name: "canceled before run",
			task: task{
				ctx: canceledCtx,
				Fn:  func(context.Context) error { t.Error("canceled task must not run"); return nil },
			},
			want: resultCanceled,
		},
		{
			name: "deadline skipped",
			task: task{
				deadline: time.Now().Add(-time.Second),
				Fn:       func(context.Context) error { t.Error("expired task must not run"); return nil },
			},
			want: resultDeadlineSkipped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := p.runTask(tt.task)
			if out.result != tt.want {
				t.Fatalf("result = %v, want %v", out.result, tt.want)
			}
			if tt.wantErr != nil && !errors.Is(out.err, tt.wantErr) {
				t.Fatalf("outcome err = %v, want %v", out.err, tt.wantErr)
			}
			if tt.wantPanic && out.panicVal == nil {
				t.Fatal("panic outcome did not record the recovered value")
			}
		})
	}
}

// pendingTracker 是 T4 WaitIdle 的基础：空池立即返回，有任务时阻塞，
// 归零时唤醒。
func TestPendingTrackerWait(t *testing.T) {
	tr := newPendingTracker()

	immediate := make(chan struct{})
	go func() {
		tr.wait()
		close(immediate)
	}()
	select {
	case <-immediate:
	case <-time.After(2 * time.Second):
		t.Fatal("wait blocked on an empty tracker")
	}

	tr.add()
	done := make(chan struct{})
	go func() {
		tr.wait()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("wait returned while a task was still pending")
	case <-time.After(20 * time.Millisecond):
	}

	tr.done()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wait was not woken when pending reached zero")
	}
}
