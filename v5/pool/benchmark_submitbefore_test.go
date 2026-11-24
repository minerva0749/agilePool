package pool

import (
	"context"
	"testing"
	"time"
)

// BenchmarkSubmitBeforeImmediate 衡量“队列立即可写”时的 SubmitBefore 提交路径。
//
// T9 之前 push 在进入循环前就为 deadline 创建 timer，因此每个成功的
// SubmitBefore 都会分配一个 time.Timer；T9 改成只在真正阻塞时创建。
// MaxBuffered=-1 保证提交不会因队列满而等待，从而只测提交路径的固定开销。
func BenchmarkSubmitBeforeImmediate(b *testing.B) {
	p, err := New(Config{
		Capacity:    32,
		IdleTimeout: time.Second,
		MaxChunkLen: 64,
		MaxBuffered: -1,
		Mode:        BLOCK,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	noop := TaskFunc(func(context.Context) error { return nil })

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.SubmitBefore(noop, time.Minute); err != nil {
				b.Errorf("SubmitBefore: %v", err)
				return
			}
		}
	})

	b.StopTimer()
	p.Close()
	p.WaitPool()
}
