package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 端到端 benchmark 的公共配置。
//
// 说明：
//   - Capacity 固定为 32，避免 -cpu 同时改变 worker 数；
//   - MaxChunkLen 固定为 64；
//   - IdleTimeout 取 1s，避免基准过程中 worker 因空闲被回收，
//     否则 ns/op 会混入“重建 worker”的开销；
//   - 每个 benchmark 自己决定 MaxBuffered：有界队列用于观察背压，
//     -1 用于观察无背压时的执行吞吐。
const (
	benchCapacity = 32
	benchChunkLen = 64
	benchBuffer   = 4096
)

// benchShape 在计时区间内低频率采样队列长度和 worker 数。
//
// 采样是近似的，只用于记录“负载形状”（峰值队列、峰值 worker），
// 不作为精确断言。500µs~1ms 一次的采样对 ns/op 的影响远小于锁竞争，
// 但如果后续需要更精确的数字，可以改成队列内部计数。
type benchShape struct {
	maxQueue   atomic.Int64
	maxWorkers atomic.Int64
	stop       chan struct{}
	done       chan struct{}
}

func sampleShape(p *Pool) *benchShape {
	s := &benchShape{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		defer close(s.done)
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				if q := int64(p.queue.size()); q > s.maxQueue.Load() {
					s.maxQueue.Store(q)
				}
				if w := int64(p.Running()); w > s.maxWorkers.Load() {
					s.maxWorkers.Store(w)
				}
			}
		}
	}()
	return s
}

func (s *benchShape) stopAndReport(b *testing.B, cfg Config) {
	close(s.stop)
	<-s.done
	b.ReportMetric(float64(s.maxQueue.Load()), "peak-queue")
	b.ReportMetric(float64(s.maxWorkers.Load()), "peak-workers")
	b.ReportMetric(float64(cfg.Capacity), "capacity")
	b.ReportMetric(float64(cfg.MaxBuffered), "max-buffered")
}

func newBenchPool(b *testing.B, cfg Config) *Pool {
	b.Helper()
	p, err := New(cfg)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	return p
}

// benchmarkBatch 提交恰好 b.N 个任务，计时范围为：
// 提交 + 全部任务执行 + Close/WaitPool 排空。
//
// 这样 ns/op 是“一个任务从提交到池完成排空的摊薄成本”，而不是单纯的
// Submit 成本；单纯的提交路径基准见 BenchmarkSubmitParallel。
//
// 计时开始前先预热 Capacity 个任务，让所有 worker 完成惰性创建。
// 否则 producer 在启动阶段会一次性把有界队列灌满（实测 peak-queue
// 恒等于 MaxBuffered），ns/op 和峰值队列都失去代表性。
func benchmarkBatch(b *testing.B, cfg Config, work TaskFunc) {
	p := newBenchPool(b, cfg)

	var warm sync.WaitGroup
	warm.Add(cfg.Capacity)
	warmTask := func(ctx context.Context) error {
		defer warm.Done()
		return work(ctx)
	}
	for i := 0; i < cfg.Capacity; i++ {
		if err := p.Submit(warmTask); err != nil {
			b.Fatalf("warm-up Submit: %v", err)
		}
	}
	warm.Wait()

	shape := sampleShape(p)

	var wg sync.WaitGroup
	wg.Add(b.N)
	task := func(ctx context.Context) error {
		defer wg.Done()
		return work(ctx)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := p.Submit(task); err != nil {
			b.Fatalf("Submit: %v", err)
		}
	}
	wg.Wait()
	p.Close()
	p.WaitPool()

	b.StopTimer()
	shape.stopAndReport(b, cfg)
}

// BenchmarkEndToEndNoop：空任务，观察调度与记账开销的下限。
func BenchmarkEndToEndNoop(b *testing.B) {
	benchmarkBatch(b, Config{
		Capacity:    benchCapacity,
		IdleTimeout: time.Second,
		MaxChunkLen: benchChunkLen,
		MaxBuffered: benchBuffer,
		Mode:        BLOCK,
	}, noopTask)
}

// benchSink 防止短任务里的计算被编译器整体优化掉。
var benchSink atomic.Uint64

func shortTask(context.Context) error {
	var x uint64
	for i := uint64(1); i <= 32; i++ {
		x += i * i
	}
	benchSink.Add(x)
	return nil
}

// BenchmarkEndToEndShort：短 CPU 任务，观察执行开销与调度开销的比例。
func BenchmarkEndToEndShort(b *testing.B) {
	benchmarkBatch(b, Config{
		Capacity:    benchCapacity,
		IdleTimeout: time.Second,
		MaxChunkLen: benchChunkLen,
		MaxBuffered: benchBuffer,
		Mode:        BLOCK,
	}, shortTask)
}

// BenchmarkEndToEndLong：长任务 + 无界队列，观察“worker 被占满、队列持续增长”
// 时的执行吞吐（没有背压，ns/op 主要由任务本身耗时决定）。
//
// 任务用 Sleep 而不是 CPU 忙等：忙等不释放 P，在低 GOMAXPROCS 下会被
// 调度粒度放大（本机实测 200µs 忙等约 2.1ms），不能代表长任务。
func BenchmarkEndToEndLong(b *testing.B) {
	benchmarkBatch(b, Config{
		Capacity:    benchCapacity,
		IdleTimeout: time.Second,
		MaxChunkLen: benchChunkLen,
		MaxBuffered: -1,
		Mode:        BLOCK,
	}, func(context.Context) error {
		time.Sleep(200 * time.Microsecond)
		return nil
	})
}

// BenchmarkEndToEndBurst：突发提交 + 小缓冲，观察队列被打满后的背压。
// peak-queue 应接近 max-buffered（256）。
func BenchmarkEndToEndBurst(b *testing.B) {
	benchmarkBatch(b, Config{
		Capacity:    benchCapacity,
		IdleTimeout: time.Second,
		MaxChunkLen: benchChunkLen,
		MaxBuffered: 256,
		Mode:        BLOCK,
	}, func(context.Context) error {
		time.Sleep(100 * time.Microsecond)
		return nil
	})
}

// BenchmarkSustainedEndToEnd：多个 producer 持续提交空任务，计时范围包含
// 提交、执行和最终排空；观察并发提交下的吞吐。
func BenchmarkSustainedEndToEnd(b *testing.B) {
	cfg := Config{
		Capacity:    benchCapacity,
		IdleTimeout: time.Second,
		MaxChunkLen: benchChunkLen,
		MaxBuffered: benchBuffer,
		Mode:        BLOCK,
	}
	p := newBenchPool(b, cfg)

	var warm sync.WaitGroup
	warm.Add(cfg.Capacity)
	warmTask := func(context.Context) error {
		defer warm.Done()
		return nil
	}
	for i := 0; i < cfg.Capacity; i++ {
		if err := p.Submit(warmTask); err != nil {
			b.Fatalf("warm-up Submit: %v", err)
		}
	}
	warm.Wait()

	shape := sampleShape(p)

	done := make(chan struct{}, b.N)
	task := func(context.Context) error {
		done <- struct{}{}
		return nil
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.Submit(task); err != nil {
				b.Errorf("Submit: %v", err)
				return
			}
		}
	})
	for i := 0; i < b.N; i++ {
		<-done
	}
	p.Close()
	p.WaitPool()

	b.StopTimer()
	shape.stopAndReport(b, cfg)
}
