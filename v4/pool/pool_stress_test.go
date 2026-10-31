package pool

// v4 压测 / 并发回归测试。
//
// 在 my_practice/v4/source 下运行：
//
//	go test ./pool -run TestStressV4 -v -count=1
//	go test ./pool -race -run TestStressV4 -v -count=1
//	go test ./pool -bench BenchmarkV4 -benchmem -run '^$'
//
// -short 会跳过所有 TestStressV4*，方便只跑功能测试。
//
// 注意：go test ./... 还会编译 main 包；如果 main.go 还在用 func() 提交，
// 先把它改成 pool.TaskFunc，或者只跑 go test ./pool。

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stressErrBox 只记录第一个非预期错误，避免在压测 goroutine 里调用 t.Fatal。
type stressErrBox struct {
	once sync.Once
	err  error
}

func (b *stressErrBox) set(err error) {
	if err == nil {
		return
	}
	b.once.Do(func() { b.err = err })
}

func (b *stressErrBox) get() error { return b.err }

// stressWaitUntil 轮询等待条件成立，避免用固定 Sleep 造成 flaky。
func stressWaitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// stressWaitGroup 等一组提交者结束；超时就判定为“唤醒丢失/卡死”，
// 而不是让 go test 一直挂到全局 timeout。
func stressWaitGroup(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("%s: still blocked after %v (possible lost wakeup)", what, timeout)
	}
}

// stressNewPool 负责测试结束时的兜底清理：即使 t.Fatal 提前返回，
// Close 也会唤醒还卡在 Submit 里的 goroutine，Wait 再把 worker 收干净。
func stressNewPool(t *testing.T, cfg Config) *Pool {
	t.Helper()
	p := New(cfg)
	t.Cleanup(func() {
		p.Close()
		p.Wait()
	})
	return p
}

// ---------------------------------------------------------------------
// 1. 大量 Submit：容量充足，检查“每个任务恰好执行一次”。
// ---------------------------------------------------------------------
func TestStressV4SubmitAllTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short")
	}

	const (
		capacity    = 128
		producers   = 300
		perProducer = 200
		total       = producers * perProducer
	)

	p := stressNewPool(t, Config{Capacity: capacity, IdleTimeout: 5 * time.Millisecond})

	var (
		executed atomic.Int64
		box      stressErrBox
		wg       sync.WaitGroup
	)

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				err := p.Submit(func(context.Context) error {
					executed.Add(1)
					return nil
				})
				if err != nil {
					box.set(err)
					return
				}
			}
		}()
	}

	stressWaitGroup(t, &wg, 30*time.Second, "producers")
	if err := box.get(); err != nil {
		t.Fatalf("Submit failed before Close: %v", err)
	}
	if !stressWaitUntil(t, 15*time.Second, func() bool { return executed.Load() == total }) {
		t.Fatalf("executed %d/%d tasks before Close", executed.Load(), total)
	}

	p.Close()
	p.Wait()

	if got := executed.Load(); got != total {
		t.Fatalf("after Close/Wait executed %d, want exactly %d", got, total)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after Close/Wait, want 0", got)
	}
}

// ---------------------------------------------------------------------
// 2. 极小池 + 大量提交者：疯狂走“容量满 -> 等 token -> 复用 idle”路径。
// ---------------------------------------------------------------------
func TestStressV4TinyPoolBackpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short")
	}

	const (
		capacity    = 2
		producers   = 200
		perProducer = 25
		total       = producers * perProducer
	)

	p := stressNewPool(t, Config{Capacity: capacity, IdleTimeout: 2 * time.Millisecond})

	var (
		executed atomic.Int64
		box      stressErrBox
		wg       sync.WaitGroup
	)

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				err := p.Submit(func(context.Context) error {
					executed.Add(1)
					return nil
				})
				if err != nil {
					box.set(err)
					return
				}
			}
		}()
	}

	stressWaitGroup(t, &wg, 30*time.Second, "producers")
	if err := box.get(); err != nil {
		t.Fatalf("Submit failed before Close: %v", err)
	}
	if !stressWaitUntil(t, 15*time.Second, func() bool { return executed.Load() == total }) {
		t.Fatalf("executed %d/%d tasks", executed.Load(), total)
	}

	p.Close()
	p.Wait()

	if got := executed.Load(); got != total {
		t.Fatalf("executed %d, want exactly %d", got, total)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after Close/Wait, want 0", got)
	}
}

// ---------------------------------------------------------------------
//  3. 取消风暴：预先取消、等 token 时取消、派发后取消混在一起。
//     只要求：不 panic、提交都有明确结果、池子最后还能干活、不超执行上限。
//
// ---------------------------------------------------------------------
func TestStressV4CancelStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short")
	}

	const (
		producers   = 120
		perProducer = 40
		total       = producers * perProducer
	)

	p := stressNewPool(t, Config{Capacity: 64, IdleTimeout: 5 * time.Millisecond})

	var (
		accepted atomic.Int64
		rejected atomic.Int64
		executed atomic.Int64
		box      stressErrBox
		wg       sync.WaitGroup
	)

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				mode := (seed + j) % 4

				var (
					ctx    context.Context
					cancel context.CancelFunc
				)
				switch mode {
				case 0:
					// 提交前就已经取消。
					ctx, cancel = context.WithCancel(context.Background())
					cancel()
				case 1:
					// 很可能在等 token 时到期。
					ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
				case 2:
					// 可能在派发前后到期。
					ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
				default:
					// 不取消，保证最终有任务真正执行。
					ctx, cancel = context.Background(), func() {}
				}

				err := p.SubmitCtx(ctx, func(ctx context.Context) error {
					if ctx.Err() == nil {
						executed.Add(1)
					}
					return ctx.Err()
				})

				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
					rejected.Add(1)
				default:
					box.set(fmt.Errorf("unexpected SubmitCtx error: %w", err))
				}

				cancel()
			}
		}(i)
	}

	stressWaitGroup(t, &wg, 30*time.Second, "cancel-storm producers")
	if err := box.get(); err != nil {
		t.Fatalf("%v", err)
	}
	if got := accepted.Load() + rejected.Load(); got != total {
		t.Fatalf("accounted %d/%d submissions", got, total)
	}
	if executed.Load() > accepted.Load() {
		t.Fatalf("executed %d > accepted %d", executed.Load(), accepted.Load())
	}

	// 风暴过后池子必须还能接受新任务；5s 是给“丢失唤醒”留的失败上限，
	// 正常情况下它应该立刻执行。
	ran := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.SubmitCtx(ctx, func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		t.Fatalf("pool unusable after cancel storm: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("sentinel task was not executed after cancel storm")
	}

	// 排干已接受的任务，再统计实际执行数。
	p.Close()
	p.Wait()

	if executed.Load() == 0 {
		t.Fatal("no task executed during cancel storm")
	}
	if got := executed.Load(); got > accepted.Load() {
		t.Fatalf("after drain executed %d > accepted %d", got, accepted.Load())
	}
}

// ---------------------------------------------------------------------
//  4. SubmitBefore：timeout=0 必须立刻 DeadlineExceeded 且绝不执行；
//     1µs/5ms/50ms 只作为负载组，不要求精确的执行数——Windows 的
//     时钟粒度会让 1ns 这种断言不稳定，确定性留给专门的矩阵测试。
//
// ---------------------------------------------------------------------
func TestStressV4SubmitBefore(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short")
	}

	const (
		capacity    = 32
		producers   = 100
		perProducer = 40
		total       = producers * perProducer
	)

	p := stressNewPool(t, Config{Capacity: capacity, IdleTimeout: 5 * time.Millisecond})

	var (
		expiredRan atomic.Int64
		executed   atomic.Int64
		accepted   atomic.Int64
		rejected   atomic.Int64
		box        stressErrBox
		wg         sync.WaitGroup
	)

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				mode := (seed + j) % 4

				var (
					timeout    time.Duration
					mustNotRun bool // 只有 timeout=0 做确定性断言
					mustErr    bool
				)
				switch mode {
				case 0:
					timeout, mustNotRun, mustErr = 0, true, true
				case 1:
					timeout = time.Microsecond
				case 2:
					timeout = 5 * time.Millisecond
				default:
					timeout = 50 * time.Millisecond
				}

				err := p.SubmitBefore(func(context.Context) error {
					if mustNotRun {
						expiredRan.Add(1)
					}
					executed.Add(1)
					return nil
				}, timeout)

				switch {
				case err == nil:
					accepted.Add(1)
					if mustErr {
						box.set(fmt.Errorf("SubmitBefore(%v) returned nil, want DeadlineExceeded", timeout))
					}
				case errors.Is(err, context.DeadlineExceeded):
					rejected.Add(1)
				default:
					box.set(fmt.Errorf("unexpected SubmitBefore error: %w", err))
				}
			}
		}(i)
	}

	stressWaitGroup(t, &wg, 30*time.Second, "SubmitBefore producers")
	if err := box.get(); err != nil {
		t.Fatalf("%v", err)
	}
	if got := accepted.Load() + rejected.Load(); got != total {
		t.Fatalf("accounted %d/%d submissions", got, total)
	}
	if executed.Load() > accepted.Load() {
		t.Fatalf("executed %d > accepted %d", executed.Load(), accepted.Load())
	}

	// 再提交一个 1s 的任务，确保“至少有一个任务真正跑过”，
	// 不依赖 5ms/50ms 组在负载下的运气。
	ran := make(chan struct{}, 1)
	if err := p.SubmitBefore(func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	}, time.Second); err != nil {
		t.Fatalf("sentinel SubmitBefore failed: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("sentinel SubmitBefore task was not executed")
	}

	p.Close()
	p.Wait()

	if got := expiredRan.Load(); got != 0 {
		t.Fatalf("tasks with timeout=0 executed %d times, want 0", got)
	}
	if executed.Load() == 0 {
		t.Fatal("no SubmitBefore task executed")
	}
}

// ---------------------------------------------------------------------
//  5. 语义守卫：SubmitBefore 的 deadline 是“开始执行”的期限，
//     不是“执行时长”的超时；任务开始后 ctx 不能被 deadline 取消。
//
// ---------------------------------------------------------------------
func TestStressV4SubmitBeforeIsStartCutoff(t *testing.T) {
	p := stressNewPool(t, Config{Capacity: 1, IdleTimeout: time.Second})

	done := make(chan error, 1)
	begin := time.Now()
	err := p.SubmitBefore(func(ctx context.Context) error {
		time.Sleep(80 * time.Millisecond)
		done <- ctx.Err()
		return nil
	}, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("SubmitBefore failed: %v", err)
	}

	select {
	case ctxErr := <-done:
		if ctxErr != nil {
			t.Fatalf("task ctx was canceled after it started: %v; SubmitBefore must not impose an execution timeout", ctxErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task did not finish")
	}

	if elapsed := time.Since(begin); elapsed < 80*time.Millisecond {
		t.Fatalf("task returned after %v, want at least 80ms", elapsed)
	}
}

// ---------------------------------------------------------------------
// 6. 生命周期：反复建池/关池，检查 worker 数量和执行数不泄漏。
// ---------------------------------------------------------------------
func TestStressV4GoroutineLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short")
	}

	baseline := runtime.NumGoroutine()

	for round := 0; round < 20; round++ {
		p := stressNewPool(t, Config{Capacity: 16, IdleTimeout: time.Millisecond})

		const (
			producers   = 20
			perProducer = 25
			total       = producers * perProducer
		)
		var (
			executed atomic.Int64
			wg       sync.WaitGroup
		)
		for i := 0; i < producers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < perProducer; j++ {
					_ = p.Submit(func(context.Context) error {
						executed.Add(1)
						return nil
					})
				}
			}()
		}

		stressWaitGroup(t, &wg, 10*time.Second, "lifecycle producers")
		p.Close()
		p.Wait()

		if got := executed.Load(); got != total {
			t.Fatalf("round %d executed %d/%d", round, got, total)
		}
		if got := p.Running(); got != 0 {
			t.Fatalf("round %d Running() = %d, want 0", round, got)
		}
	}

	runtime.GC()
	if !stressWaitUntil(t, 5*time.Second, func() bool {
		return runtime.NumGoroutine() <= baseline+10
	}) {
		t.Logf("goroutines did not fully settle: baseline %d, now %d", baseline, runtime.NumGoroutine())
	}
}

// ---------------------------------------------------------------------
// 7. Submit 与 Close 并发：提交要么成功、要么 ErrPoolClosed，不能 panic。
// ---------------------------------------------------------------------
func TestStressV4CloseWhileSubmitting(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short")
	}

	p := stressNewPool(t, Config{Capacity: 8, IdleTimeout: 5 * time.Millisecond})

	var (
		calls atomic.Int64
		box   stressErrBox
		wg    sync.WaitGroup
		stop  = make(chan struct{})
	)

	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := p.Submit(func(context.Context) error { return nil })
				calls.Add(1)
				if err != nil && !errors.Is(err, ErrPoolClosed) {
					box.set(err)
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	p.Close()
	time.Sleep(20 * time.Millisecond)
	close(stop)

	stressWaitGroup(t, &wg, 10*time.Second, "close-while-submitting producers")
	p.Wait()

	if err := box.get(); err != nil {
		t.Fatalf("unexpected Submit error: %v", err)
	}
	if calls.Load() == 0 {
		t.Fatal("no Submit was attempted")
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Running() = %d after Close/Wait, want 0", got)
	}
}

// ---------------------------------------------------------------------
// Benchmark：只测提交/调度开销，任务体都是空操作。
// ---------------------------------------------------------------------

func BenchmarkV4Submit(b *testing.B) {
	p := New(Config{Capacity: 256, IdleTimeout: time.Second})
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.Submit(func(context.Context) error { return nil }); err != nil {
				b.Errorf("Submit: %v", err)
				return
			}
		}
	})

	b.StopTimer()
	p.Close()
	p.Wait()
}

func BenchmarkV4SubmitCtx(b *testing.B) {
	p := New(Config{Capacity: 256, IdleTimeout: time.Second})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.SubmitCtx(ctx, func(context.Context) error { return nil }); err != nil {
				b.Errorf("SubmitCtx: %v", err)
				return
			}
		}
	})

	b.StopTimer()
	p.Close()
	p.Wait()
}

func BenchmarkV4SubmitBefore(b *testing.B) {
	p := New(Config{Capacity: 256, IdleTimeout: time.Second})
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.SubmitBefore(func(context.Context) error { return nil }, time.Second); err != nil {
				b.Errorf("SubmitBefore: %v", err)
				return
			}
		}
	})

	b.StopTimer()
	p.Close()
	p.Wait()
}

// BenchmarkV4Execution 把任务执行也计入：提交 b.N 个任务并等它们全部完成。
func BenchmarkV4Execution(b *testing.B) {
	p := New(Config{Capacity: 256, IdleTimeout: time.Second})
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		if err := p.Submit(func(context.Context) error {
			wg.Done()
			return nil
		}); err != nil {
			b.Fatalf("Submit: %v", err)
		}
	}
	wg.Wait()

	b.StopTimer()
	p.Close()
	p.Wait()
}
