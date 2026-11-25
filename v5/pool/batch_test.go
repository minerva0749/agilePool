package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultBatchSize(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	if defaultBatchSize != 16 {
		t.Fatalf("defaultBatchSize = %d, want 16", defaultBatchSize)
	}
	if p.batchSize != defaultBatchSize {
		t.Fatalf("pool batchSize = %d, want %d", p.batchSize, defaultBatchSize)
	}
}

func TestTaskQueuePopUpToFIFOAcrossChunks(t *testing.T) {
	q := newTaskQueue(3, -1)
	defer q.close()

	for i := 0; i < 10; i++ {
		mustPush(t, q, queueTask(i))
	}
	if got := queueTaskID(mustPop(t, q)); got != 0 {
		t.Fatalf("first pop = %d, want 0", got)
	}

	for _, step := range []struct {
		max  int
		want []int
	}{
		{max: 5, want: []int{1, 2, 3, 4, 5}},
		{max: 2, want: []int{6, 7}},
		{max: 10, want: []int{8, 9}},
		{max: 1, want: nil},
	} {
		got := q.popUpTo(step.max)
		if len(got) != len(step.want) {
			t.Fatalf("popUpTo(%d) returned %d tasks, want %d", step.max, len(got), len(step.want))
		}
		for i, tk := range got {
			if id := queueTaskID(tk); id != step.want[i] {
				t.Fatalf("popUpTo(%d)[%d] = %d, want %d", step.max, i, id, step.want[i])
			}
		}
	}
	if got := q.popUpTo(0); got != nil {
		t.Fatalf("popUpTo(0) = %v, want nil", got)
	}
	if got := q.popUpTo(-1); got != nil {
		t.Fatalf("popUpTo(-1) = %v, want nil", got)
	}
	if got := q.size(); got != 0 {
		t.Fatalf("queue size after batch drain = %d, want 0", got)
	}
}

func TestTaskQueuePopUpToWakesBlockedPush(t *testing.T) {
	q := newTaskQueue(2, 2)
	defer q.close()

	mustPush(t, q, queueTask(0))
	mustPush(t, q, queueTask(1))
	done := make(chan error, 1)
	go func() {
		done <- q.push(queueTask(2), context.Background(), time.Time{}, BLOCK)
	}()
	waitUntil(t, 2*time.Second, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.pushers == 1
	})

	got := q.popUpTo(2)
	if len(got) != 2 || queueTaskID(got[0]) != 0 || queueTaskID(got[1]) != 1 {
		t.Fatalf("popUpTo(2) returned unexpected tasks: %v", got)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked push after popUpTo: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("popUpTo did not wake a blocked producer")
	}
	if got := q.size(); got != 1 {
		t.Fatalf("queue size = %d, want 1 after the blocked producer was accepted", got)
	}
	if got := queueTaskID(mustPop(t, q)); got != 2 {
		t.Fatalf("remaining task = %d, want 2", got)
	}
}

func TestBatchLimitFairShare(t *testing.T) {
	for _, tc := range []struct {
		name      string
		batchSize int
		queued    int
		idle      int
		want      int
	}{
		{name: "disabled", batchSize: 1, queued: 64, idle: 4, want: 1},
		{name: "empty queue", batchSize: 16, queued: 0, idle: 4, want: 1},
		{name: "fewer tasks than workers", batchSize: 16, queued: 3, idle: 4, want: 1},
		{name: "fair share", batchSize: 16, queued: 31, idle: 4, want: 7},
		{name: "batch cap", batchSize: 16, queued: 100, idle: 4, want: 16},
		{name: "one remaining idle worker", batchSize: 16, queued: 64, idle: 1, want: 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Pool{
				batchSize: tc.batchSize,
				idle:      newFIFOIdle(),
				queue:     newTaskQueue(8, -1),
			}
			defer p.queue.close()
			for i := 0; i < tc.idle; i++ {
				p.idle.Add(&worker{})
			}
			for i := 0; i < tc.queued; i++ {
				mustPush(t, p.queue, queueTask(i))
			}
			if got := p.batchLimit(); got != tc.want {
				t.Fatalf("batchLimit() = %d, want %d (queued=%d idle=%d batch=%d)",
					got, tc.want, tc.queued, tc.idle, tc.batchSize)
			}
		})
	}
}

func TestBatchFairnessAcrossIdleWorkers(t *testing.T) {
	const capacity = 32
	p := newTestPool(t, Config{
		Capacity:    capacity,
		IdleTimeout: time.Second,
		MaxChunkLen: 64,
		MaxBuffered: 64,
		Mode:        BLOCK,
	})
	defer closeAndWait(t, p)

	// 新建 worker 的路径必须保持一对一，先让 Capacity 个 worker 全部启动，
	// 再释放为 idle，隔离验证后续的 idle-worker 批量路径。
	warmStarted := make(chan struct{}, capacity)
	warmRelease := make(chan struct{})
	var warmOnce sync.Once
	releaseWarm := func() { warmOnce.Do(func() { close(warmRelease) }) }
	defer releaseWarm()
	for i := 0; i < capacity; i++ {
		if err := p.Submit(func(context.Context) error {
			warmStarted <- struct{}{}
			<-warmRelease
			return nil
		}); err != nil {
			t.Fatalf("warm-up Submit %d: %v", i, err)
		}
	}
	for i := 0; i < capacity; i++ {
		select {
		case <-warmStarted:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d warm-up tasks started", i, capacity)
		}
	}
	releaseWarm()
	p.WaitIdle()
	waitUntil(t, 2*time.Second, func() bool { return p.Idle() == capacity })

	const blockers = 8
	started := make(chan struct{}, blockers)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	for i := 0; i < blockers; i++ {
		if err := p.Submit(func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}); err != nil {
			t.Fatalf("Submit blocker %d: %v", i, err)
		}
	}

	// 没有公平上限时，一个 idle worker 会拿走全部 8 个任务，只有第一个
	// 能开始；公平分摊必须让 8 个 worker 各启动一个。
	for i := 0; i < blockers; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d blockers started; batch scheduling is not fair", i, blockers)
		}
	}
	unblock()
}

func TestBatchCloseDrainsInFlightBatch(t *testing.T) {
	var submitted, enqueued, startedHook, completed, skipped, poolClosed atomic.Int64
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 3,
		MaxBuffered: 64,
		Mode:        BLOCK,
		Hooks: Hooks{
			Submitted:  func(context.Context) { submitted.Add(1) },
			Enqueued:   func(context.Context) { enqueued.Add(1) },
			Started:    func(context.Context) { startedHook.Add(1) },
			Completed:  func(context.Context, TaskOutcome) { completed.Add(1) },
			Skipped:    func(context.Context, TaskOutcome) { skipped.Add(1) },
			PoolClosed: func() { poolClosed.Add(1) },
		},
	})
	defer closeAndWait(t, p)

	// 预热一个 worker；下面的计数断言把这一个任务也计算在内。
	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("warm-up Submit: %v", err)
	}
	p.WaitIdle()
	waitUntil(t, 2*time.Second, func() bool { return p.Idle() == 1 })

	const total = 32
	order := make([]int, 0, total)
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()

	// 暂停 dispatcher 的认领步骤，但允许它先 pop 队头。这样提交期间可以
	// 确定地积累 backlog，Close 时第一批必然已经组成并正在 worker 中执行。
	p.mu.Lock()
	locked := true
	defer func() {
		if locked {
			p.mu.Unlock()
		}
	}()
	for i := 0; i < total; i++ {
		i := i
		if err := p.Submit(func(context.Context) error {
			if i == 0 {
				close(firstStarted)
				<-release
			}
			order = append(order, i)
			return nil
		}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	waitUntil(t, 2*time.Second, func() bool { return p.Queued() == total-1 })
	p.Close()
	p.mu.Unlock()
	locked = false

	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the first task in the batch did not start after Close")
	}
	// 第一个任务仍阻塞，因此所有已接受任务都还在 Pending。第一批 16 个
	// 已离开队列；dispatcher 可能已开始持有下一批队头，所以 Queued 为 15
	// 或 16，而不是原单任务实现里的 30 或 31。
	if got := p.Pending(); got != total {
		t.Fatalf("Pending() with an in-flight batch = %d, want %d", got, total)
	}
	if got := p.Queued(); got > total-defaultBatchSize {
		t.Fatalf("Queued() with an in-flight batch = %d, want <= %d", got, total-defaultBatchSize)
	}
	unblock()
	closeAndWait(t, p)

	if len(order) != total {
		t.Fatalf("executed %d batched tasks, want %d", len(order), total)
	}
	for i, got := range order {
		if got != i {
			t.Fatalf("batch order[%d] = %d, want %d", i, got, i)
		}
	}
	want := int64(total + 1) // 包含预热任务
	for name, got := range map[string]int64{
		"Submitted":  submitted.Load(),
		"Enqueued":   enqueued.Load(),
		"Started":    startedHook.Load(),
		"Completed":  completed.Load(),
		"PoolClosed": poolClosed.Load(),
	} {
		wantForHook := want
		if name == "PoolClosed" {
			wantForHook = 1
		}
		if got != wantForHook {
			t.Fatalf("%s hook fired %d times, want %d", name, got, wantForHook)
		}
	}
	if got := skipped.Load(); got != 0 {
		t.Fatalf("Skipped hook fired %d times, want 0", got)
	}
	assertCounters(t, p, want, 0, want, 0, 0)
	assertGauges(t, p, 0, 0, 0, 0, 0)
}

func TestBatchSkipsCanceledAndExpiredExtras(t *testing.T) {
	var started, completed atomic.Int64
	skipped := make(chan TaskResult, 2)
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 16,
		Mode:        BLOCK,
		Hooks: Hooks{
			Started:   func(context.Context) { started.Add(1) },
			Completed: func(context.Context, TaskOutcome) { completed.Add(1) },
			Skipped: func(_ context.Context, out TaskOutcome) {
				skipped <- out.Result
			},
		},
	})
	defer closeAndWait(t, p)

	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("warm-up Submit: %v", err)
	}
	p.WaitIdle()
	waitUntil(t, 2*time.Second, func() bool { return p.Idle() == 1 })

	var validRuns, canceledRuns, expiredRuns atomic.Int64
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	valid := func(context.Context) error {
		validRuns.Add(1)
		return nil
	}

	p.mu.Lock()
	locked := true
	defer func() {
		if locked {
			p.mu.Unlock()
		}
	}()
	if err := p.Submit(func(context.Context) error {
		validRuns.Add(1)
		close(firstStarted)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit first: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		canceledRuns.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx extra: %v", err)
	}
	if err := p.SubmitBefore(func(context.Context) error {
		expiredRuns.Add(1)
		return nil
	}, 20*time.Millisecond); err != nil {
		t.Fatalf("SubmitBefore extra: %v", err)
	}
	// 两个正常任务放在被取消/超期任务之后：前者保证批内跳过后仍能继续，
	// 最后一个同时覆盖 dispatcher 处理批次之后剩余队头的路径。
	for i := 0; i < 2; i++ {
		if err := p.Submit(valid); err != nil {
			t.Fatalf("Submit valid extra %d: %v", i, err)
		}
	}
	waitUntil(t, 2*time.Second, func() bool { return p.Queued() == 4 })
	cancel()
	time.Sleep(40 * time.Millisecond)
	p.mu.Unlock()
	locked = false

	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the first valid task did not start")
	}
	// 这两个 Skipped 在组批时、第一个任务结束前就由 dispatcher 发出。
	gotSkipped := map[TaskResult]int{}
	for i := 0; i < 2; i++ {
		select {
		case result := <-skipped:
			gotSkipped[result]++
		case <-time.After(2 * time.Second):
			t.Fatal("canceled/expired batch extras did not reach Skipped")
		}
	}
	if gotSkipped[TaskCanceled] != 1 || gotSkipped[TaskDeadlineSkipped] != 1 {
		t.Fatalf("Skipped results = %v, want one canceled and one deadline-skipped", gotSkipped)
	}
	unblock()
	closeAndWait(t, p)

	if got := validRuns.Load(); got != 3 {
		t.Fatalf("valid tasks executed %d times, want 3", got)
	}
	if got := canceledRuns.Load(); got != 0 {
		t.Fatalf("canceled extra executed %d times, want 0", got)
	}
	if got := expiredRuns.Load(); got != 0 {
		t.Fatalf("expired extra executed %d times, want 0", got)
	}
	if got := started.Load(); got != 4 { // 3 个有效任务 + 预热任务
		t.Fatalf("Started hook fired %d times, want 4", got)
	}
	if got := completed.Load(); got != 4 {
		t.Fatalf("Completed hook fired %d times, want 4", got)
	}
	assertCounters(t, p, 6, 0, 4, 2, 0)
	assertGauges(t, p, 0, 0, 0, 0, 0)
}

func TestBatchRejectsSubmitAfterClose(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 16,
		Mode:        NONBLOCK,
	})
	p.Close()
	p.WaitPool()
	if err := p.Submit(noopTask); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit after Close = %v, want ErrPoolClosed", err)
	}
}
