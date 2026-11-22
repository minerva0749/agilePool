package pool

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func noopTask(context.Context) error { return nil }

// TestTaskQueuePartialCancelWake 固定队列的一条关键行为：
// 多个 producer 阻塞在满队列上时，取消其中一部分，被取消者必须归还
// notFull 唤醒提示，剩下的人必须仍能依次拿到空位，不能永久阻塞。
//
// 这条测试直接对应 taskQueue.push 里的 woke 记账，是后续 T9 调整信号
// 发送策略时必须保住的语义。
func TestTaskQueuePartialCancelWake(t *testing.T) {
	q := newTaskQueue(1, 1)
	defer q.close()

	mustPush(t, q, queueTask(0)) // 先占满唯一的槽位

	const producers = 6
	canceled := map[int]bool{1: true, 3: true, 5: true}

	errChs := make([]chan error, producers)
	cancels := make([]context.CancelFunc, producers)
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels[i] = cancel
		errChs[i] = make(chan error, 1)
		wg.Add(1)
		go func(i int, ctx context.Context, ch chan error) {
			defer wg.Done()
			ch <- q.push(queueTask(100+i), ctx, time.Time{}, BLOCK)
		}(i, ctx, errChs[i])
	}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	// 等所有 producer 进入阻塞分支。队列始终是满的，它们不可能成功；
	// 之后取消其中一半，确保取消发生在 select 等待中，而不是入队窗口内。
	time.Sleep(50 * time.Millisecond)
	for i := range cancels {
		if canceled[i] {
			cancels[i]()
		}
	}

	prodDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(prodDone)
	}()

	// 作为唯一消费者不断 pop，为等待者释放空位；每 pop 一次只应唤醒
	// 一个 producer。带超时守卫，防止 notFull 提示丢失导致永久阻塞。
	var popped []int
	deadline := time.Now().Add(5 * time.Second)
waitLoop:
	for {
		select {
		case <-prodDone:
			break waitLoop
		default:
		}
		if q.size() > 0 {
			popped = append(popped, queueTaskID(mustPop(t, q)))
			continue
		}
		if time.Now().After(deadline) {
			t.Fatal("producers did not finish: possible lost notFull wakeup")
		}
		time.Sleep(time.Millisecond)
	}
	for q.size() > 0 {
		popped = append(popped, queueTaskID(mustPop(t, q)))
	}

	for i, ch := range errChs {
		var err error
		select {
		case err = <-ch:
		case <-time.After(time.Second):
			t.Fatalf("producer %d did not return", i)
		}
		if canceled[i] {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("producer %d = %v, want context.Canceled", i, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("producer %d (not canceled) = %v, want nil", i, err)
		}
	}

	want := map[int]bool{0: true}
	for i := 0; i < producers; i++ {
		if !canceled[i] {
			want[100+i] = true
		}
	}
	if len(popped) != len(want) {
		t.Fatalf("popped %d tasks, want %d: %v", len(popped), len(want), popped)
	}
	for _, id := range popped {
		if !want[id] {
			t.Fatalf("popped unexpected task id %d (canceled producer or duplicate)", id)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Fatalf("tasks missing from queue: %v", want)
	}
	if got := q.size(); got != 0 {
		t.Fatalf("queue size = %d after drain, want 0", got)
	}
}

// TestConcurrentSubmitAndClose 覆盖“多 producer 与 Close 并发”：
// 每个 Submit 都必须返回（nil 或 ErrPoolClosed），Close 之后 dispatcher
// 必须把已接受任务全部排空并执行，不能出现“已接受但丢失”。
func TestConcurrentSubmitAndClose(t *testing.T) {
	const (
		producers   = 8
		perProducer = 50
	)
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 1,
		MaxBuffered: 1,
		Mode:        BLOCK,
	})

	var (
		accepted atomic.Int64
		executed atomic.Int64
		submitWG sync.WaitGroup
	)
	unexpected := make(chan error, producers)
	start := make(chan struct{})
	for i := 0; i < producers; i++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			<-start
			for j := 0; j < perProducer; j++ {
				err := p.Submit(func(context.Context) error {
					// 用一个短阻塞任务制造背压。Windows 上 200µs 的 Sleep
					// 实测约 0.6ms，但这里只关心“worker 暂时没空”，不断言时间。
					time.Sleep(200 * time.Microsecond)
					executed.Add(1)
					return nil
				})
				if err == nil {
					accepted.Add(1)
					continue
				}
				if errors.Is(err, ErrPoolClosed) {
					return
				}
				select {
				case unexpected <- err:
				default:
				}
				return
			}
		}()
	}
	close(start)

	// 让 producer 进入提交/阻塞状态后再关闭，尽量覆盖“阻塞在 push 上
	// 被 Close 唤醒”的分支。是否真的全部阻塞不影响本测试的断言。
	time.Sleep(2 * time.Millisecond)
	p.Close()

	waitGroup(t, &submitWG, 10*time.Second)
	select {
	case err := <-unexpected:
		t.Fatalf("Submit returned unexpected error: %v", err)
	default:
	}
	closeAndWaitWithin(t, p, 20*time.Second)

	if got, want := executed.Load(), accepted.Load(); got != want {
		t.Fatalf("executed = %d, accepted = %d; Close must drain every accepted task", got, want)
	}
	if got := p.queue.size(); got != 0 {
		t.Fatalf("queue size = %d after drain, want 0", got)
	}
	if err := p.Submit(noopTask); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit after Close/Wait = %v, want ErrPoolClosed", err)
	}
}

// TestCloseIdempotentAndMultipleWaiters 覆盖三件事：
//  1. Close 可以并发重复调用，不 panic；
//  2. 多个 goroutine 可以同时调用 WaitPool，全部返回；
//  3. WaitPool 一旦返回，再次调用应立即返回。
func TestCloseIdempotentAndMultipleWaiters(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 8,
		MaxBuffered: 64,
		Mode:        BLOCK,
	})
	for i := 0; i < 200; i++ {
		if err := p.Submit(noopTask); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	p.Close()

	var waiters sync.WaitGroup
	for i := 0; i < 8; i++ {
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			p.WaitPool()
		}()
	}
	waitGroup(t, &waiters, 10*time.Second)

	var closers sync.WaitGroup
	for i := 0; i < 16; i++ {
		closers.Add(1)
		go func() {
			defer closers.Done()
			p.Close()
		}()
	}
	waitGroup(t, &closers, 5*time.Second)

	done := make(chan struct{})
	go func() {
		p.WaitPool()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("repeated WaitPool blocked after it had already returned")
	}

	if err := p.Submit(noopTask); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit after Close = %v, want ErrPoolClosed", err)
	}
}

// SubmitCtx(nil, fn) 当前把 nil ctx 当成 context.Background()。
func TestSubmitCtxWithNilContext(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	got := make(chan context.Context, 1)
	if err := p.SubmitCtx(nil, func(ctx context.Context) error {
		got <- ctx
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx(nil, fn): %v", err)
	}
	select {
	case ctx := <-got:
		if ctx == nil {
			t.Fatal("task received nil context")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task with nil ctx did not run")
	}
	closeAndWait(t, p)
}

// TestDispatcherHeldCanceledTaskIsDropped 覆盖“dispatcher 已经 pop 到任务、
// 但还在等 worker 时任务被取消”的路径：
//   - 被取消的任务不能执行；
//   - dispatcher 不能因此卡死，后续任务必须继续被执行。
func TestDispatcherHeldCanceledTaskIsDropped(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 1,
		MaxBuffered: 1,
		Mode:        BLOCK,
	})

	release := make(chan struct{})
	blockerStarted := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(blockerStarted)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	<-blockerStarted

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var heldRan atomic.Int64
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		heldRan.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx held task: %v", err)
	}

	// dispatcher 是唯一消费者：队列变空说明它已经把该任务拿在手里，
	// 此时唯一的 worker 还被 blocker 占着，它只能等 workerFree。
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })
	time.Sleep(10 * time.Millisecond)
	cancel()

	nextRan := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		nextRan <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit next task: %v", err)
	}

	close(release)
	select {
	case <-nextRan:
	case <-time.After(5 * time.Second):
		t.Fatal("task after the canceled held task did not run")
	}
	if got := heldRan.Load(); got != 0 {
		t.Fatalf("canceled held task executed %d times, want 0", got)
	}
	closeAndWait(t, p)
}

// TestManyWorkersIdleTimeoutThenSubmit 覆盖“大量 worker 同时 idle timeout，
// 再提交新任务”的容量槽 liveness：worker 退出时必须唤醒 dispatcher，
// 否则 running==Capacity 且 idle 为空时 dispatcher 会永久等待。
func TestManyWorkersIdleTimeoutThenSubmit(t *testing.T) {
	const capacity = 32
	p := newTestPool(t, Config{
		Capacity:    capacity,
		IdleTimeout: 20 * time.Millisecond,
		MaxChunkLen: 8,
		MaxBuffered: 128,
		Mode:        BLOCK,
	})

	release := make(chan struct{})
	var started atomic.Int64
	for i := 0; i < capacity; i++ {
		if err := p.Submit(func(context.Context) error {
			started.Add(1)
			<-release
			return nil
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	waitUntil(t, 5*time.Second, func() bool { return started.Load() == capacity })
	if got := p.Running(); got != capacity {
		t.Fatalf("Running() = %d while all workers busy, want %d", got, capacity)
	}

	close(release)
	waitUntil(t, 10*time.Second, func() bool { return p.Running() == 0 })

	ran := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		ran <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit after idle timeout: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not create a new worker after all workers timed out")
	}
	closeAndWait(t, p)
}

// TestCapacityOnePreservesOrder 在 Capacity=1 时断言执行顺序。
// Capacity>1 时只应断言“每个任务恰好执行一次”，不要断言完成顺序。
func TestCapacityOnePreservesOrder(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 8,
		MaxBuffered: 64,
		Mode:        BLOCK,
	})

	const n = 64
	order := make([]int, 0, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		if err := p.Submit(func(context.Context) error {
			defer wg.Done()
			order = append(order, i)
			return nil
		}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	waitGroup(t, &wg, 10*time.Second)
	closeAndWait(t, p)

	if len(order) != n {
		t.Fatalf("executed %d tasks, want %d", len(order), n)
	}
	for i := 0; i < n; i++ {
		if order[i] != i {
			t.Fatalf("order[%d] = %d, want %d (single worker must be FIFO)", i, order[i], i)
		}
	}
}

// TestMultipleWorkersRunEachTaskOnce 在 Capacity>1 时只断言每任务恰好一次，
// 不断言完成顺序。
func TestMultipleWorkersRunEachTaskOnce(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    4,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 8,
		MaxBuffered: 32,
		Mode:        BLOCK,
	})

	const n = 200
	counts := make([]atomic.Int64, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		if err := p.Submit(func(context.Context) error {
			defer wg.Done()
			counts[i].Add(1)
			return nil
		}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	waitGroup(t, &wg, 10*time.Second)
	closeAndWait(t, p)

	for i := 0; i < n; i++ {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("task %d executed %d times, want 1", i, got)
		}
	}
}

// TestTerminalPathsPoolStaysConsistent 覆盖 panic、error、deadline skipped、
// canceled skipped 混在一起后，池仍然关闭干净：
//   - 正常/panic/error 任务各执行一次；
//   - deadline/取消任务一次都不执行；
//   - 队列排空、Running 归零、WaitPool 可重复调用。
//
// 当前实现还没有 pending 计数，这里用“执行次数 + queue.size + Running”
// 作为一致性代理；T3/T7 引入 pending 后应补充 pending==0 的断言。
func TestTerminalPathsPoolStaysConsistent(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 2,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	old := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(old)

	release := make(chan struct{})
	blockerStarted := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(blockerStarted)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	<-blockerStarted

	var panicRuns, errRuns atomic.Int64
	panicDone := make(chan struct{}, 1)
	errDone := make(chan struct{}, 1)

	if err := p.Submit(func(context.Context) error {
		panicRuns.Add(1)
		defer func() { panicDone <- struct{}{} }()
		panic("boom")
	}); err != nil {
		t.Fatalf("Submit panic task: %v", err)
	}
	if err := p.Submit(func(context.Context) error {
		errRuns.Add(1)
		defer func() { errDone <- struct{}{} }()
		return errors.New("task error")
	}); err != nil {
		t.Fatalf("Submit error task: %v", err)
	}

	var expiredRuns atomic.Int64
	if err := p.SubmitBefore(func(context.Context) error {
		expiredRuns.Add(1)
		return nil
	}, 20*time.Millisecond); err != nil {
		t.Fatalf("SubmitBefore: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var canceledRuns atomic.Int64
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		canceledRuns.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}

	// 让 deadline 过期，再取消排队中的任务，最后放行 blocker。
	time.Sleep(40 * time.Millisecond)
	cancel()
	close(release)

	select {
	case <-panicDone:
	case <-time.After(5 * time.Second):
		t.Fatal("panic task did not run")
	}
	select {
	case <-errDone:
	case <-time.After(5 * time.Second):
		t.Fatal("error task did not run")
	}

	closeAndWait(t, p)

	if got := panicRuns.Load(); got != 1 {
		t.Fatalf("panic task ran %d times, want 1", got)
	}
	if got := errRuns.Load(); got != 1 {
		t.Fatalf("error task ran %d times, want 1", got)
	}
	if got := expiredRuns.Load(); got != 0 {
		t.Fatalf("expired task ran %d times, want 0", got)
	}
	if got := canceledRuns.Load(); got != 0 {
		t.Fatalf("canceled task ran %d times, want 0", got)
	}
	if got := p.queue.size(); got != 0 {
		t.Fatalf("queue size = %d after drain, want 0", got)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after WaitPool, want 0", got)
	}

	done := make(chan struct{})
	go func() {
		p.WaitPool()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("repeated WaitPool blocked after it had already returned")
	}
}
