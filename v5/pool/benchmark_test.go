package pool

import (
	"context"
	"testing"
	"time"
)

// BenchmarkSubmitParallel 测的是“多个 producer 同时 Submit 空任务”的吞吐，
// 用来观察 taskQueue 单 mutex 在并发提交下的表现。
//
// 注意：
//   - MaxBuffered=-1 是为了避免队列满时阻塞，让 benchmark 尽量测 Submit 路径，
//     而不是测背压；任务空转，worker 会持续消费。
//   - 用 -cpu=1,2,4,8 可以改变 producer 数量（RunParallel 默认按 GOMAXPROCS 起
//     goroutine），观察 ns/op 是否随并发上升。
//   - 这个 benchmark 包含 worker 消费和调度开销，不是纯队列锁的微基准；
//     纯队列版本见 BenchmarkTaskQueuePushParallel。
//   - 默认 -benchtime=1s 会生成上百万个空任务，StopTimer 后 p.Wait() 排空
//     可能让整条命令多跑几十秒；建议先用 -benchtime=200ms 观察趋势，
//     需要更稳定的数字时再逐步加大。
func BenchmarkSubmitParallel(b *testing.B) {
	p, err := New(Config{
		// Capacity 固定，避免 -cpu 同时改变 worker 数，干扰 producer 数的对比。
		Capacity:    32,
		IdleTimeout: time.Second,
		MaxChunkLen: 64,
		MaxBuffered: -1, // 不触发 BLOCK 背压，专心测提交路径
		Mode:        BLOCK,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	// 复用同一个 TaskFunc，避免每个任务额外分配一个闭包。
	noop := TaskFunc(func(context.Context) error { return nil })

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.Submit(noop); err != nil {
				b.Errorf("Submit: %v", err)
				return
			}
		}
	})

	b.StopTimer()
	p.Close()
	p.WaitPool()
}

// BenchmarkTaskQueuePushParallel 只测 taskQueue 的 push/pop，不经过
// dispatcher、worker 和任务执行，用来把“队列锁”从 Submit 的总开销里剥离出来。
//
// 一个消费者持续 pop，多个 producer 通过 b.RunParallel 并发 push；生产者
// 和消费者共用 taskQueue.mu，因此能直接观察这把锁在并发下的扩展性。
// 与 BenchmarkSubmitParallel 对比，两者的差值主要就是 dispatcher/worker/
// timer/任务执行的开销。
func BenchmarkTaskQueuePushParallel(b *testing.B) {
	q := newTaskQueue(64, -1)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			if _, ok := q.pop(); !ok {
				return
			}
		}
	}()

	// 队列只负责存取，不执行任务；用零值 task 避免额外闭包/ctx 干扰。
	var zero task

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := q.push(zero, context.Background(), time.Time{}, BLOCK); err != nil {
				b.Errorf("push: %v", err)
				return
			}
		}
	})

	b.StopTimer()
	q.close()
	select {
	case <-consumerDone:
	case <-time.After(5 * time.Second):
		b.Fatal("queue consumer did not exit")
	}
}
