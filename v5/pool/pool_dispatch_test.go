package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockOnlyWorker 用一个长任务占住 Capacity=1 的池，返回“已开始”信号和
// 幂等的 release 函数。必须调用 release，否则 worker 会一直阻塞在任务里。
func blockOnlyWorker(t *testing.T, p *Pool) (<-chan struct{}, func()) {
	t.Helper()

	started := make(chan struct{})
	releaseCh := make(chan struct{})
	var once sync.Once

	if err := p.Submit(func(context.Context) error {
		close(started)
		<-releaseCh
		return nil
	}); err != nil {
		t.Fatalf("Submit blocking task: %v", err)
	}

	return started, func() { once.Do(func() { close(releaseCh) }) }
}

// T5：dispatcher 在等 worker 时，如果手里的任务被取消，必须立刻放弃它并
// 回到队列继续消费，不能一直等到 worker 归还。
//
// 观测方式：Capacity=1 时 worker 被长任务占住，dispatcher 已经 pop 走一个
// 可取消任务（queue.size()==0）。取消后立刻 Submit 一个 follow-up：
//   - T5 之前：dispatcher 卡在 <-p.workerFree 上，follow-up 留在队列里；
//   - T5 之后：取消任务被丢弃，dispatcher 取走 follow-up 继续等 worker。
//
// 随后 Close 并释放 worker：follow-up 必须执行，被取消的任务必须不执行。
func TestDispatcherCancelReleasesWorkerWait(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Minute, // 排除 idle timeout 回收 worker 的干扰
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	started, release := blockOnlyWorker(t, p)
	defer release()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	var canceledRan atomic.Int64
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		canceledRan.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}

	// 等 dispatcher 把这个任务取走：队列空了，但它拿不到 worker。
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })

	cancel()

	ran := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Submit follow-up: %v", err)
	}

	// 只有 dispatcher 放弃被取消的任务后，follow-up 才会离开队列。
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })

	p.Close()
	release()

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("follow-up task did not run after the worker was released")
	}

	closeAndWait(t, p)
	if got := canceledRan.Load(); got != 0 {
		t.Fatalf("canceled task executed %d times, want 0", got)
	}
}

// T5 在等待路径上新增了取消/超期两个 finishTask 出口。这里用批量取消 +
// 批量超期制造 churn：每个被跳过任务只能扣一次 pending，最终只剩两个占住
// worker 的 blocker，池仍能正常关闭。
func TestDispatchSkipChurnKeepsPendingConsistent(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 8,
		MaxBuffered: 128,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()

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
		<-started
	}

	var ran atomic.Int64
	const n = 100
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		if err := p.SubmitCtx(ctx, func(context.Context) error {
			ran.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("SubmitCtx: %v", err)
		}
		cancel()
	}
	for i := 0; i < n; i++ {
		if err := p.SubmitBefore(func(context.Context) error {
			ran.Add(1)
			return nil
		}, 10*time.Millisecond); err != nil {
			t.Fatalf("SubmitBefore: %v", err)
		}
	}

	// 所有被取消/超期的任务都到达终态后，pending 只应剩两个 blocker；
	// 少一个说明重复扣账，多一个说明有路径漏记。
	waitUntil(t, 5*time.Second, func() bool { return pendingOf(p) == 2 })

	unblock()
	waitPendingZero(t, p, 5*time.Second)
	if got := ran.Load(); got != 0 {
		t.Fatalf("skipped tasks executed %d times, want 0", got)
	}
}

// T5：dispatcher 等 worker 时，SubmitBefore 的 deadline 到期也必须立刻放弃
// 任务并继续消费后续任务，而不是等 worker 归还。
func TestDispatcherDeadlineReleasesWorkerWait(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Minute,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	started, release := blockOnlyWorker(t, p)
	defer release()
	<-started

	var expiredRan atomic.Int64
	if err := p.SubmitBefore(func(context.Context) error {
		expiredRan.Add(1)
		return nil
	}, 50*time.Millisecond); err != nil {
		t.Fatalf("SubmitBefore: %v", err)
	}

	// dispatcher 已取走这个任务，正在等 worker。
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })

	ran := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("Submit follow-up: %v", err)
	}

	// deadline 到期后 dispatcher 必须取走 follow-up，留足 Windows 时钟余量。
	waitUntil(t, 3*time.Second, func() bool { return p.queue.size() == 0 })

	release()
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("follow-up task did not run after the worker was released")
	}

	closeAndWait(t, p)
	if got := expiredRan.Load(); got != 0 {
		t.Fatalf("expired task executed %d times, want 0", got)
	}
}
