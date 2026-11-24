package pool

// T6：错误处理、panic 处理与生命周期 hook 的回归测试。
//
// 覆盖点：
//   - 五类 hook 的触发时机、次数、goroutine 归属与 per-task 顺序；
//   - 拒绝路径（nil / 关闭 / 满队列）只有 Submitted，没有 Enqueued/终态 hook；
//   - canceled / deadline-skipped 只进 Skipped，Started/Completed 都不触发；
//   - ErrorHandler / PanicHandler 的参数、默认 panic 日志与 stack trace；
//   - hook/handler 自身 panic 的隔离，以及 pending / Close 协议不被破坏；
//   - 回调不在 p.mu / queue.mu 内执行；
//   - pending 先扣减、hook 后执行（WaitIdle 不等待 hook）。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// captureLogger 记录池的诊断输出，替代默认 log.Default()。
type captureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *captureLogger) Printf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.lines = append(l.lines, msg)
	l.mu.Unlock()
}

func (l *captureLogger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

func (l *captureLogger) contains(substr string) bool {
	for _, line := range l.snapshot() {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

// eventRecorder 记录 hook 事件名；hook 可能来自不同 goroutine，必须加锁。
type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{}
}

func (r *eventRecorder) add(name string) {
	r.mu.Lock()
	r.events = append(r.events, name)
	r.mu.Unlock()
}

func (r *eventRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *eventRecorder) count(name string) int {
	n := 0
	for _, e := range r.snapshot() {
		if e == name {
			n++
		}
	}
	return n
}

func (r *eventRecorder) index(name string) int {
	for i, e := range r.snapshot() {
		if e == name {
			return i
		}
	}
	return -1
}

// hooks 返回一个只记录事件名的完整 hook 集合，供顺序/次数断言使用。
func (r *eventRecorder) hooks() Hooks {
	return Hooks{
		Submitted:  func(context.Context) { r.add("Submitted") },
		Enqueued:   func(context.Context) { r.add("Enqueued") },
		Started:    func(context.Context) { r.add("Started") },
		Completed:  func(context.Context, TaskOutcome) { r.add("Completed") },
		Skipped:    func(_ context.Context, out TaskOutcome) { r.add("Skipped:" + out.Result.String()) },
		PoolClosed: func() { r.add("PoolClosed") },
	}
}

func waitLogContains(t *testing.T, l *captureLogger, substr string, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, func() bool { return l.contains(substr) })
}

// 正常路径：五个 hook 各触发一次，且 Submitted→Enqueued、Started→Completed
// 的顺序成立。Enqueued 与 Started 之间不保证顺序，这里不做断言。
func TestHooksLifecycleOrder(t *testing.T) {
	rec := newEventRecorder()
	completed := make(chan TaskOutcome, 1)

	hooks := rec.hooks()
	hooks.Completed = func(_ context.Context, out TaskOutcome) {
		rec.add("Completed")
		completed <- out
	}

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
		Hooks:       hooks,
	})

	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case out := <-completed:
		if out.Result != TaskCompleted {
			t.Fatalf("Completed outcome = %v, want %v", out.Result, TaskCompleted)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Completed hook did not fire")
	}

	p.Close()
	p.WaitPool()

	events := rec.snapshot()
	for _, name := range []string{"Submitted", "Enqueued", "Started", "Completed", "PoolClosed"} {
		if got := rec.count(name); got != 1 {
			t.Fatalf("%s fired %d times, want exactly 1; events = %v", name, got, events)
		}
	}
	if rec.index("Submitted") > rec.index("Enqueued") {
		t.Fatalf("Submitted must precede Enqueued; events = %v", events)
	}
	if rec.index("Started") > rec.index("Completed") {
		t.Fatalf("Started must precede Completed; events = %v", events)
	}
}

// 参数错误（nil task、非正 timeout）在进入 enqueue 前就被拒绝，不触发任何 hook；
// 池关闭导致的拒绝是“合法尝试”，只触发 Submitted，不触发 Enqueued。
func TestSubmittedOnlyForValidAttempts(t *testing.T) {
	var submitted, enqueued atomic.Int64
	hooks := Hooks{
		Submitted: func(context.Context) { submitted.Add(1) },
		Enqueued:  func(context.Context) { enqueued.Add(1) },
	}

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
		Hooks:       hooks,
	})

	if err := p.Submit(nil); !errors.Is(err, ErrNilTask) {
		t.Fatalf("Submit(nil) = %v, want ErrNilTask", err)
	}
	if err := p.SubmitBefore(noopTask, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SubmitBefore(timeout=0) = %v, want DeadlineExceeded", err)
	}
	if got := submitted.Load(); got != 0 {
		t.Fatalf("Submitted fired %d times for argument errors, want 0", got)
	}

	// ctx 已取消仍是一次“合法尝试”：Submitted 触发，但 Enqueued 不触发。
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.SubmitCtx(canceledCtx, noopTask); !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitCtx(canceled) = %v, want context.Canceled", err)
	}
	if got := submitted.Load(); got != 1 {
		t.Fatalf("Submitted fired %d times for the canceled-ctx attempt, want 1", got)
	}
	if got := enqueued.Load(); got != 0 {
		t.Fatalf("Enqueued fired %d times for the canceled-ctx attempt, want 0", got)
	}

	p.Close()
	if err := p.Submit(noopTask); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit after Close = %v, want ErrPoolClosed", err)
	}
	if got := submitted.Load(); got != 2 {
		t.Fatalf("Submitted fired %d times for the rejected Submit, want 2 total", got)
	}
	if got := enqueued.Load(); got != 0 {
		t.Fatalf("Enqueued fired %d times for rejected Submits, want 0", got)
	}

	p.WaitPool()
}

// NONBLOCK 满队列：每个合法提交都触发 Submitted，只有被接受的任务触发 Enqueued。
func TestEnqueuedOnlyForAcceptedTasks(t *testing.T) {
	var submitted, enqueued atomic.Int64
	hooks := Hooks{
		Submitted: func(context.Context) { submitted.Add(1) },
		Enqueued:  func(context.Context) { enqueued.Add(1) },
	}

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 1,
		MaxBuffered: 1,
		Mode:        NONBLOCK,
		Hooks:       hooks,
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

	// A 被 dispatcher 持有，B 填满 MaxBuffered=1 的缓冲，C 被拒绝。
	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("Submit held task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 0 })
	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("Submit filling task: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return p.queue.size() == 1 })

	if err := p.Submit(noopTask); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("Submit on full NONBLOCK buffer = %v, want ErrPoolFull", err)
	}

	if got := submitted.Load(); got != 4 {
		t.Fatalf("Submitted = %d (blocker + 2 accepted + 1 rejected), want 4", got)
	}
	if got := enqueued.Load(); got != 3 {
		t.Fatalf("Enqueued = %d, want 3", got)
	}

	close(release)
	closeAndWait(t, p)
}

// canceled 与 deadline-skipped 只触发 Skipped：没有 Started/Completed，
// 任务函数不执行，结果分类与 ctx 值都正确。
func TestSkippedHookForCanceledAndDeadline(t *testing.T) {
	type ctxKey struct{}
	const ctxValue = "task-ctx"

	var (
		mu       sync.Mutex
		skipped  []TaskOutcome
		started  atomic.Int64
		complete atomic.Int64
		ran      atomic.Int64
	)

	hooks := Hooks{
		Started:   func(context.Context) { started.Add(1) },
		Completed: func(context.Context, TaskOutcome) { complete.Add(1) },
		Skipped: func(ctx context.Context, out TaskOutcome) {
			mu.Lock()
			skipped = append(skipped, out)
			mu.Unlock()
			if out.Result == TaskCanceled {
				if got, _ := ctx.Value(ctxKey{}).(string); got != ctxValue {
					t.Errorf("Skipped(canceled) ctx value = %q, want %q", got, ctxValue)
				}
			}
		},
	}

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
		Hooks:       hooks,
	})

	startedCh := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(func(context.Context) error {
		close(startedCh)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	<-startedCh
	// blocker 自己也是一次正常执行，会触发一次 Started；记录基线，后面只
	// 断言两个被跳过的任务没有增加 Started。
	startedBefore := started.Load()

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, ctxValue))
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		ran.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}
	cancel()

	if err := p.SubmitBefore(func(context.Context) error {
		ran.Add(1)
		return nil
	}, 20*time.Millisecond); err != nil {
		t.Fatalf("SubmitBefore: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(skipped) == 2
	})

	mu.Lock()
	gotResults := map[TaskResult]bool{}
	for _, out := range skipped {
		gotResults[out.Result] = true
	}
	mu.Unlock()
	if !gotResults[TaskCanceled] || !gotResults[TaskDeadlineSkipped] {
		t.Fatalf("skipped results = %v, want canceled and deadline-skipped", gotResults)
	}
	if got := started.Load(); got != startedBefore {
		t.Fatalf("Started fired %d more times for skipped tasks, want 0", got-startedBefore)
	}
	if got := complete.Load(); got != 0 {
		t.Fatalf("Completed fired %d times for skipped tasks, want 0", got)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("skipped tasks executed %d times, want 0", got)
	}

	close(release)
	waitPendingZero(t, p, 2*time.Second)
	closeAndWait(t, p)
}

// Completed 覆盖 completed/error/panic 三类执行结果，并携带正确的
// Err / Panic；PanicHandler 拿到 recover 值与 stack，且替代默认日志。
func TestCompletedOutcomesAndHandlers(t *testing.T) {
	sentinel := errors.New("task failed")
	logger := &captureLogger{}

	var (
		mu        sync.Mutex
		outcomes  []TaskOutcome
		errSeen   error
		panicSeen any
		stackSeen []byte
	)
	completed := make(chan TaskOutcome, 3)

	p := newTestPool(t, Config{
		Capacity:    3,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 16,
		Mode:        BLOCK,
		Logger:      logger,
		ErrorHandler: func(_ context.Context, err error) {
			mu.Lock()
			errSeen = err
			mu.Unlock()
		},
		PanicHandler: func(_ context.Context, recovered any, stack []byte) {
			mu.Lock()
			panicSeen = recovered
			stackSeen = append([]byte(nil), stack...)
			mu.Unlock()
		},
		Hooks: Hooks{Completed: func(_ context.Context, out TaskOutcome) {
			mu.Lock()
			outcomes = append(outcomes, out)
			mu.Unlock()
			completed <- out
		}},
	})

	tasks := []TaskFunc{
		func(context.Context) error { return nil },
		func(context.Context) error { return sentinel },
		func(context.Context) error { panic("kaboom") },
	}
	for i, fn := range tasks {
		if err := p.Submit(fn); err != nil {
			t.Fatalf("Submit[%d]: %v", i, err)
		}
	}

	for i := 0; i < len(tasks); i++ {
		select {
		case <-completed:
		case <-time.After(2 * time.Second):
			t.Fatal("Completed hook did not fire for every task")
		}
	}
	waitPendingZero(t, p, 2*time.Second)
	closeAndWait(t, p)

	mu.Lock()
	byResult := make(map[TaskResult]TaskOutcome, len(outcomes))
	for _, out := range outcomes {
		byResult[out.Result] = out
	}
	gotErr, gotPanic := errSeen, panicSeen
	gotStack := append([]byte(nil), stackSeen...)
	mu.Unlock()

	if len(byResult) != 3 {
		t.Fatalf("Completed outcomes = %v, want one for each of 3 results", byResult)
	}
	if !errors.Is(byResult[TaskFailed].Err, sentinel) {
		t.Fatalf("TaskFailed outcome Err = %v, want %v", byResult[TaskFailed].Err, sentinel)
	}
	if byResult[TaskPanicked].Panic != "kaboom" {
		t.Fatalf("TaskPanicked outcome Panic = %v, want kaboom", byResult[TaskPanicked].Panic)
	}
	if !errors.Is(gotErr, sentinel) {
		t.Fatalf("ErrorHandler err = %v, want %v", gotErr, sentinel)
	}
	if gotPanic != "kaboom" {
		t.Fatalf("PanicHandler recovered = %v, want kaboom", gotPanic)
	}
	if len(gotStack) == 0 || !strings.Contains(string(gotStack), "panic") {
		t.Fatalf("PanicHandler stack does not look like a stack trace: %q", gotStack)
	}
	if logger.contains("task panic") {
		t.Fatal("PanicHandler should replace the default panic logging")
	}
}

// 未配置 PanicHandler 时，默认日志必须同时包含 panic 值和 stack trace。
func TestDefaultPanicLoggingIncludesStack(t *testing.T) {
	logger := &captureLogger{}
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
		Logger:      logger,
	})

	if err := p.Submit(func(context.Context) error { panic("kaboom") }); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	waitLogContains(t, logger, "task panic", 2*time.Second)
	waitLogContains(t, logger, "kaboom", 2*time.Second)
	waitLogContains(t, logger, "goroutine", 2*time.Second)

	closeAndWait(t, p)
}

// 每个 hook 自身 panic 都必须被隔离：任务照常执行、记账归零、Close 不被卡住。
func TestHookPanicIsolation(t *testing.T) {
	logger := &captureLogger{}
	ran := make(chan struct{}, 1)

	hooks := Hooks{
		Submitted:  func(context.Context) { panic("boom-submitted") },
		Enqueued:   func(context.Context) { panic("boom-enqueued") },
		Started:    func(context.Context) { panic("boom-started") },
		Completed:  func(context.Context, TaskOutcome) { panic("boom-completed") },
		PoolClosed: func() { panic("boom-closed") },
	}

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
		Logger:      logger,
		Hooks:       hooks,
	})

	if err := p.Submit(func(context.Context) error {
		ran <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit must succeed even if the Submitted hook panics: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not run after hook panics")
	}

	closeAndWait(t, p)

	for _, name := range []string{"Submitted", "Enqueued", "Started", "Completed", "PoolClosed"} {
		if !logger.contains(name + " callback panicked") {
			t.Fatalf("missing panic report for %s hook; log = %v", name, logger.snapshot())
		}
	}
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after hook panics, want 0", got)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after Close/Wait, want 0", got)
	}
}

// ErrorHandler / PanicHandler 自身 panic 同样被隔离，worker 继续处理后续任务。
func TestHandlerPanicIsolation(t *testing.T) {
	logger := &captureLogger{}
	ran := make(chan struct{}, 3)

	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
		Logger:      logger,
		ErrorHandler: func(context.Context, error) {
			panic("boom-error-handler")
		},
		PanicHandler: func(context.Context, any, []byte) {
			panic("boom-panic-handler")
		},
	})

	for _, fn := range []TaskFunc{
		func(context.Context) error { ran <- struct{}{}; return errors.New("task failed") },
		func(context.Context) error { ran <- struct{}{}; panic("task panic") },
		func(context.Context) error { ran <- struct{}{}; return nil },
	} {
		if err := p.Submit(fn); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		select {
		case <-ran:
		case <-time.After(2 * time.Second):
			t.Fatal("a task did not run after handler panics")
		}
	}
	closeAndWait(t, p)

	waitLogContains(t, logger, "ErrorHandler callback panicked", 2*time.Second)
	waitLogContains(t, logger, "PanicHandler callback panicked", 2*time.Second)
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after handler panics, want 0", got)
	}
}

// PoolClosed 在并发 Close 下只触发一次，且触发时队列已关闭（Submit 稳定返回
// ErrPoolClosed），不会出现“hook 里还能入队”的窗口。
func TestPoolClosedHookOnceAndAfterQueueClosed(t *testing.T) {
	var (
		p         *Pool
		calls     atomic.Int64
		submitErr atomic.Value
	)
	hooks := Hooks{PoolClosed: func() {
		calls.Add(1)
		submitErr.Store(p.Submit(noopTask))
	}}

	p = newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
		Hooks:       hooks,
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Close()
		}()
	}
	wg.Wait()
	p.WaitPool()

	if got := calls.Load(); got != 1 {
		t.Fatalf("PoolClosed fired %d times under concurrent Close, want 1", got)
	}
	err, _ := submitErr.Load().(error)
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit inside PoolClosed = %v, want ErrPoolClosed", err)
	}
}

// 回调不得在 p.mu / queue.mu 内执行：hook 里访问这两个锁保护的接口，
// 若池仍持锁就会死锁。用超时守卫避免测试挂死。
func TestHooksRunOutsideInternalLocks(t *testing.T) {
	errCh := make(chan error, 1)
	go func() {
		var p *Pool
		hooks := Hooks{
			Submitted:  func(context.Context) { _ = p.queue.size() },
			Enqueued:   func(context.Context) { _ = p.queue.size() },
			Started:    func(context.Context) { _ = p.Running() },
			Completed:  func(context.Context, TaskOutcome) { _ = p.Running() },
			PoolClosed: func() { _ = p.queue.size() },
		}

		var err error
		p, err = New(Config{
			Capacity:    2,
			IdleTimeout: time.Second,
			MaxChunkLen: 4,
			MaxBuffered: 8,
			Mode:        BLOCK,
			Hooks:       hooks,
		})
		if err != nil {
			errCh <- err
			return
		}

		for i := 0; i < 4; i++ {
			if err := p.Submit(noopTask); err != nil {
				errCh <- err
				return
			}
		}
		p.WaitIdle()
		p.Close()
		p.WaitPool()
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("pool failed while hooks accessed lock-protected methods: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hook deadlocked on an internal lock (p.mu or queue.mu held during callback)")
	}
}

// dispatcher goroutine 上的 Skipped hook panic 同样被隔离：不会让 dispatcher
// 退出，也不会阻止后续任务被派发。
func TestSkippedHookPanicIsolation(t *testing.T) {
	logger := &captureLogger{}
	blockerStarted := make(chan struct{})
	release := make(chan struct{})

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
		Logger:      logger,
		Hooks: Hooks{Skipped: func(context.Context, TaskOutcome) {
			panic("boom-skipped")
		}},
	})

	if err := p.Submit(func(context.Context) error {
		close(blockerStarted)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	<-blockerStarted

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.SubmitCtx(ctx, noopTask); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}
	cancel()
	waitLogContains(t, logger, "Skipped callback panicked", 2*time.Second)

	ran := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		ran <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit after Skipped hook panic: %v", err)
	}

	close(release)
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher/worker did not continue after the Skipped hook panicked")
	}
	closeAndWait(t, p)

	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d after Close/Wait, want 0", got)
	}
}

// pending 扣减先于 Completed hook：hook 阻塞时 WaitIdle 仍应立即返回。
func TestWaitIdleDoesNotWaitForCompletionHooks(t *testing.T) {
	hookEntered := make(chan struct{})
	release := make(chan struct{})

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
		Hooks: Hooks{Completed: func(context.Context, TaskOutcome) {
			close(hookEntered)
			<-release
		}},
	})

	if err := p.Submit(noopTask); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case <-hookEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("Completed hook did not fire")
	}

	// hook 仍阻塞，但任务已经记账完成：pending 必须已是 0。
	if got := pendingOf(p); got != 0 {
		t.Fatalf("pending = %d while Completed hook is blocked, want 0", got)
	}

	waitIdleDone := make(chan struct{})
	go func() {
		p.WaitIdle()
		close(waitIdleDone)
	}()
	select {
	case <-waitIdleDone:
	case <-time.After(time.Second):
		t.Fatal("WaitIdle waited for the Completed hook")
	}

	close(release)
	closeAndWait(t, p)
}

// hook 收到的 ctx 与提交任务时使用的 ctx 一致（用于关联 SubmitCtx 的元数据）。
func TestHooksReceiveTaskContext(t *testing.T) {
	type ctxKey struct{}
	const value = "task-a"

	values := make(chan string, 4)
	record := func(ctx context.Context) {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			values <- v
			return
		}
		values <- "<missing>"
	}

	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: time.Second,
		MaxChunkLen: 4,
		MaxBuffered: 4,
		Mode:        BLOCK,
		Hooks: Hooks{
			Submitted: func(ctx context.Context) { record(ctx) },
			Enqueued:  func(ctx context.Context) { record(ctx) },
			Started:   func(ctx context.Context) { record(ctx) },
			Completed: func(ctx context.Context, _ TaskOutcome) { record(ctx) },
		},
	})

	ctx := context.WithValue(context.Background(), ctxKey{}, value)
	if err := p.SubmitCtx(ctx, noopTask); err != nil {
		t.Fatalf("SubmitCtx: %v", err)
	}

	for i := 0; i < 4; i++ {
		select {
		case got := <-values:
			if got != value {
				t.Fatalf("hook ctx value = %q, want %q", got, value)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 4 hooks received a ctx", i)
		}
	}

	closeAndWait(t, p)
}
