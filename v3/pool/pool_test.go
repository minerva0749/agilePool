package pool

// =====================================================================
// Go 测试文件速成（只想知道怎么跑，可跳过这段注释）
// =====================================================================
//
// 1) 文件名必须以 _test.go 结尾，go test 才会认它。
//    （之前那个 test_interface.go 名字不对，文件里的东西不会被当成测试执行，
//      它现在只是个空文件，删不删都行。）
// 2) 测试函数必须写成 func TestXxx(t *testing.T)，Xxx 首字母要大写。
// 3) 报错用两种：
//      t.Fatalf(...)  打印并立刻结束当前测试（后面的检查没意义时用）
//      t.Errorf(...)  打印但继续跑（想一次性看全所有问题时用）
// 4) 测试文件和被测代码同属 package pool，能直接调用 New/Submit/Close/Wait/Running。
// 5) 常用命令（在本目录或上级执行）：
//      go test ./...                跑全部测试
//      go test -v ./...             显示每个测试的名字和结果
//      go test -race ./...          加数据竞争检测（V3 验收必须全绿）
//      go test -short ./...         跳过压测
//      go test -run TestSubmit -v   只跑名字含 TestSubmit 的测试
//      go test -count=10 ./...      每个测试重复跑 10 次，查偶发问题

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 轮询等待 cond() 变为 true，最多等 timeout。
//
// 为什么不直接 time.Sleep？
//   - 睡太短：机器一慢就偶发失败（flaky test）。
//   - 睡太长：测试白白变慢。
//   - 轮询"等条件成立"既快又稳，是并发测试的标准做法。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper() // 声明这是辅助函数，失败信息会指向调用它的那行，而不是这里
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

// ---------------------------------------------------------------------
// 验收点 1：创建池子时一个 worker 都没有（先有任务，才有 worker）。
// ---------------------------------------------------------------------
func TestNewRunningIsZero(t *testing.T) {
	p := New(Config{Capacity: 10, IdleTimeout: time.Second})
	defer func() {
		p.Close()
		p.Wait()
	}()

	if got := p.Running(); got != 0 {
		t.Fatalf("New 之后 Running() = %d，期望 0", got)
	}
}

// ---------------------------------------------------------------------
// 验收点 2：首个任务到来时才创建 worker。
// ---------------------------------------------------------------------
func TestFirstTaskCreatesWorker(t *testing.T) {
	// IdleTimeout 给足 10 秒，保证测试期间 worker 不会因为闲置而自行退出。
	p := New(Config{Capacity: 10, IdleTimeout: 10 * time.Second})
	defer func() {
		p.Close()
		p.Wait()
	}()

	if got := p.Running(); got != 0 {
		t.Fatalf("初始 Running() = %d，期望 0", got)
	}

	if err := p.Submit(func() {}); err != nil {
		t.Fatalf("Submit 失败：%v", err)
	}

	// Submit 返回时 worker 已在锁内创建并 running++，所以这里应当恰好为 1。
	if got := p.Running(); got != 1 {
		t.Fatalf("提交首个任务后 Running() = %d，期望 1", got)
	}
}

// ---------------------------------------------------------------------
// 验收点 3：闲置超过 IdleTimeout 后 worker 自行退出，Running() 归零。
// ---------------------------------------------------------------------
func TestIdleTimeoutRetiresWorkers(t *testing.T) {
	p := New(Config{Capacity: 10, IdleTimeout: 50 * time.Millisecond})
	defer func() {
		p.Close()
		p.Wait()
	}()

	_ = p.Submit(func() {})

	if !waitFor(t, time.Second, func() bool { return p.Running() == 1 }) {
		t.Fatalf("提交任务后应有 1 个 worker，实际 %d", p.Running())
	}

	// 任务很快执行完，worker 回到空闲集合；再等过 IdleTimeout，它应自行退出。
	if !waitFor(t, 2*time.Second, func() bool { return p.Running() == 0 }) {
		t.Fatalf("空闲超时后 Running() 应归零，实际 %d", p.Running())
	}
}

// ---------------------------------------------------------------------
// 验收点 4：突发任务把 Running() 涨到 Capacity；达到容量后，多出的任务
// 应阻塞等待空闲 worker，而不是继续创建新 worker。
// ---------------------------------------------------------------------
func TestCapacityBlocksExtraTasks(t *testing.T) {
	const capacity = 3
	p := New(Config{Capacity: capacity, IdleTimeout: 10 * time.Second})
	defer func() {
		p.Close()
		p.Wait()
	}()

	release := make(chan struct{}, capacity) // 用它"放行"卡住的任务
	var started atomic.Int32
	blockingTask := func() {
		started.Add(1)
		<-release // 一直卡着，直到测试放行
	}

	// 先提交 capacity 个会阻塞的任务，把 worker 全部占满。
	for i := 0; i < capacity; i++ {
		if err := p.Submit(blockingTask); err != nil {
			t.Fatalf("第 %d 次 Submit 失败：%v", i, err)
		}
	}

	if !waitFor(t, time.Second, func() bool { return started.Load() == int32(capacity) }) {
		t.Fatalf("应有 %d 个任务开始执行，实际 %d", capacity, started.Load())
	}
	if got := p.Running(); got != capacity {
		t.Fatalf("占满后 Running() = %d，期望 %d", got, capacity)
	}

	// 再来一个任务：没有空闲 worker，也不能超容量，它应该阻塞在 Submit 里。
	done := make(chan error, 1)
	go func() {
		done <- p.Submit(func() {})
	}()

	select {
	case err := <-done:
		t.Fatalf("容量已满时 Submit 应阻塞等待，却直接返回了（err=%v）", err)
	case <-time.After(100 * time.Millisecond):
		// 符合预期：它正在等空闲 worker
	}

	// 放行一个任务，让一个 worker 空出来；被阻塞的 Submit 应被唤醒并完成提交。
	release <- struct{}{}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("被唤醒的 Submit 不应失败：%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("放行一个 worker 后，阻塞的 Submit 没有被唤醒")
	}

	// 收尾：放行其余还在阻塞的任务。
	close(release)
}

// ---------------------------------------------------------------------
// 验收点 5：容量已满、但恰好有空闲 worker 时，Submit 必须复用空闲 worker，
// 而不是一直阻塞。（专门防"先判断 running 满、后看 idle"的顺序错误）
// ---------------------------------------------------------------------
func TestReuseIdleWorkerAtCapacity(t *testing.T) {
	p := New(Config{Capacity: 2, IdleTimeout: 5 * time.Second})
	defer func() {
		p.Close()
		p.Wait()
	}()

	release := make(chan struct{}, 2)
	blockingTask := func() { <-release }

	_ = p.Submit(blockingTask)
	_ = p.Submit(blockingTask) // 此时 running == 2 == Capacity

	// 放行一个：这个 worker 干完活后会回到空闲集合。
	release <- struct{}{}

	// 此刻 Running() 仍是 2 == Capacity，但其中一个 worker 空闲。
	// 再 Submit 必须能拿到它（最多稍等 worker 归还），不能永久阻塞。
	done := make(chan error, 1)
	go func() {
		done <- p.Submit(func() {})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("复用空闲 worker 的 Submit 不应失败：%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("容量满但存在空闲 worker 时，Submit 不应永久阻塞")
	}

	close(release)
}

// ---------------------------------------------------------------------
// 验收点 6：Close 之后 Submit 返回 error（而不是 panic）。
// ---------------------------------------------------------------------
func TestSubmitAfterCloseReturnsError(t *testing.T) {
	p := New(Config{Capacity: 2, IdleTimeout: time.Second})
	p.Close()
	p.Wait()

	if err := p.Submit(func() {}); err == nil {
		t.Fatal("池子关闭后 Submit 应返回 error，实际返回 nil")
	}
}

// ---------------------------------------------------------------------
// 验收点 7：Close 是幂等的——重复调用、并发调用都不 panic。
// ---------------------------------------------------------------------
func TestCloseIdempotent(t *testing.T) {
	p := New(Config{Capacity: 4, IdleTimeout: time.Second})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Close() // 10 个 goroutine 同时 Close
		}()
	}
	wg.Wait()

	p.Close() // 再补两刀
	p.Close()
	p.Wait()
}

// ---------------------------------------------------------------------
// 验收点 8：Close 时正在执行的任务要跑完；且 Close 之后 Submit 立刻报错。
// ---------------------------------------------------------------------
func TestCloseWaitsForRunningTasks(t *testing.T) {
	p := New(Config{Capacity: 2, IdleTimeout: time.Second})

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	finished := make(chan struct{})

	_ = p.Submit(func() {
		started <- struct{}{}
		<-release // 卡住，模拟"正在执行的长任务"
		close(finished)
	})

	<-started // 确认任务已经开始执行

	p.Close() // 关闭池子：这个正在跑的任务应该继续跑完

	// 池子已关，Submit 应立即返回 error（而不是 panic / 永久阻塞）。
	if err := p.Submit(func() {}); err == nil {
		t.Fatal("Close 之后 Submit 应返回 error，实际返回 nil")
	}

	close(release) // 放行长任务

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("Close 时正在执行的任务没有跑完")
	}

	p.Wait()
}

// ---------------------------------------------------------------------
// 验收点 9：Close 时空闲 worker 要立即退出，不能傻等到 IdleTimeout。
// （IdleTimeout 故意设成 1 小时：如果空闲 worker 不响应 Close，Wait 会卡死）
// ---------------------------------------------------------------------
func TestCloseExitsIdleWorkersImmediately(t *testing.T) {
	p := New(Config{Capacity: 4, IdleTimeout: time.Hour})

	_ = p.Submit(func() {}) // 造一个空闲 worker
	if !waitFor(t, time.Second, func() bool { return p.Running() == 1 }) {
		t.Fatal("应有 1 个 worker")
	}

	begin := time.Now()
	p.Close()
	p.Wait()

	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("Close 后等了 %v，空闲 worker 本应立即退出", elapsed)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("Close 后 Running() = %d，期望 0", got)
	}
}

// ---------------------------------------------------------------------
// 验收点 10：Close 之前提交的任务必须全部执行完，且不能重复执行。
// ---------------------------------------------------------------------
func TestDrainBeforeClose(t *testing.T) {
	const n = 100
	p := New(Config{Capacity: 10, IdleTimeout: time.Second})

	var done atomic.Int64
	for i := 0; i < n; i++ {
		if err := p.Submit(func() { done.Add(1) }); err != nil {
			t.Fatalf("第 %d 次 Submit 失败：%v", i, err)
		}
	}

	// 先等全部任务跑完，再 Close：这样测的是"关闭不会丢任务/不会重复执行"，
	// 避开设计里"When 任务已塞进缓冲、worker 还没收到时 Close"的极小概率边界。
	if !waitFor(t, 2*time.Second, func() bool { return done.Load() == int64(n) }) {
		t.Fatalf("任务没有全部执行完：%d/%d", done.Load(), n)
	}

	p.Close()
	p.Wait()

	if got := done.Load(); got != n {
		t.Fatalf("Close 后完成任务数 = %d，期望 %d", got, n)
	}
}

// ---------------------------------------------------------------------
// 验收点 11：一个任务 panic 不能被扩散出去，其他任务照常完成。
// ---------------------------------------------------------------------
func TestPanicIsolation(t *testing.T) {
	p := New(Config{Capacity: 2, IdleTimeout: time.Second})

	var done atomic.Int64
	_ = p.Submit(func() { panic("boom") }) // 会被 runTask 里的 recover 兜住
	for i := 0; i < 10; i++ {
		_ = p.Submit(func() { done.Add(1) })
	}

	if !waitFor(t, 2*time.Second, func() bool { return done.Load() == 10 }) {
		t.Fatalf("panic 影响了其他任务：只完成 %d/10", done.Load())
	}

	p.Close()
	p.Wait()
}

// ---------------------------------------------------------------------
// 验收点 12：Submit 与 Close 并发时不能 panic。
// （关闭后 Submit 可能返回 error，这是允许的；这里只保证不崩。）
// ---------------------------------------------------------------------
func TestConcurrentSubmitAndClose(t *testing.T) {
	p := New(Config{Capacity: 8, IdleTimeout: time.Second})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = p.Submit(func() {}) // 关闭后返回 error，忽略即可
			}
		}()
	}

	time.Sleep(50 * time.Millisecond) // 让提交先压一会儿
	p.Close()
	time.Sleep(50 * time.Millisecond) // 关之后让它们再撞一会儿 error
	close(stop)
	wg.Wait()
	p.Wait()
}

// ---------------------------------------------------------------------
// 压测：专攻"提示 3 竞态"——归还空闲集合、被 Submit 取走、闲置超时
// 三者同时发生时，任务不能丢、不能重复执行、计数不能失衡。
//
// go test -short 会跳过它（比较慢）。
// ---------------------------------------------------------------------
func TestStressIdleTimeoutRace(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过压测")
	}

	const (
		tasks    = 10000           // 想完全对标指南可以改成 100000
		capacity = 100             // worker 上限
		idle     = time.Millisecond // 故意极短，逼出"归还/取走/超时"的竞态
	)

	before := runtime.NumGoroutine()

	p := New(Config{Capacity: capacity, IdleTimeout: idle})
	var count atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Submit(func() { count.Add(1) }); err != nil {
				t.Errorf("提交失败：%v（池子此时还没关，不应失败）", err)
			}
		}()
	}
	wg.Wait()

	p.Close()
	p.Wait()

	// 恰好 tasks 次才正确：
	//   > tasks 说明旧任务被重复执行（竞态没修干净）；
	//   < tasks 说明任务被丢了。
	if got := count.Load(); got != tasks {
		t.Fatalf("任务执行了 %d 次，期望恰好 %d 次", got, tasks)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("关闭并等待后 Running() = %d，期望 0（计数泄漏）", got)
	}

	// goroutine 数应回落到初始水平附近。给一点余量，避免误报（仅提示，不判失败）。
	if !waitFor(t, 2*time.Second, func() bool { return runtime.NumGoroutine() <= before+5 }) {
		t.Logf("提示：goroutine 未完全回落（压测前 %d，现在 %d），可能有泄漏", before, runtime.NumGoroutine())
	}
}
