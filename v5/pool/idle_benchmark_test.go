package pool

import (
	"fmt"
	"testing"
)

// BenchmarkIdleContainerRemoveAll 模拟“N 个 worker 同时 idle timeout”：
// 按空闲顺序逐个 Remove。
//
//   - stack：T8 的 LIFO 栈，最早空闲的 worker 在栈底却最先超时，
//     每次删除都要扫描 + 搬移，总成本 O(n²)；
//   - fifo：T12 的默认实现，双向链表节点内嵌在 worker 上，Remove 任意位置
//     都是 O(1)，总成本 O(n)。
//
// 只测容器本身，不启动 worker/goroutine。
func BenchmarkIdleContainerRemoveAll(b *testing.B) {
	for _, size := range []int{1000, 5000, 10000} {
		for _, impl := range []string{"stack", "fifo"} {
			b.Run(fmt.Sprintf("%s/n=%d", impl, size), func(b *testing.B) {
				ws := make([]*worker, size)
				for i := range ws {
					ws[i] = &worker{}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					c := newIdleContainer(impl)
					for _, w := range ws {
						c.Add(w)
					}
					for _, w := range ws {
						if !c.Remove(w) {
							b.Fatal("Remove failed")
						}
					}
				}
			})
		}
	}
}

// BenchmarkIdleContainerPopAll 是 O(n) 的对照：两种实现的 Pop 都是 O(1)，
// 总成本只与元素个数成正比。
func BenchmarkIdleContainerPopAll(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		for _, impl := range []string{"stack", "fifo"} {
			b.Run(fmt.Sprintf("%s/n=%d", impl, size), func(b *testing.B) {
				ws := make([]*worker, size)
				for i := range ws {
					ws[i] = &worker{}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					c := newIdleContainer(impl)
					for _, w := range ws {
						c.Add(w)
					}
					for j := 0; j < size; j++ {
						if c.Pop() == nil {
							b.Fatal("Pop returned nil")
						}
					}
				}
			})
		}
	}
}
