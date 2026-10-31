package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"workerpool/pool"
)

func main() {
	p := pool.New(pool.Config{
		Capacity:    256,
		IdleTimeout: 100 * time.Millisecond,
	})

	var (
		subWG sync.WaitGroup
		done  atomic.Int64
	)

	// TaskFunc 是命名函数类型：func(context.Context) error。
	// 函数字面量的签名匹配，就可以直接赋值，不需要显式转换。
	//
	// 等价写法：
	//   fn := func(ctx context.Context) error { ... }                // 类型推断
	//   fn := pool.TaskFunc(func(ctx context.Context) error { ... }) // 显式转换
	var fn pool.TaskFunc = func(ctx context.Context) error {
		done.Add(1)
		return nil
	}

	for i := 0; i < 1000; i++ {
		subWG.Add(1)
		go func() {
			defer subWG.Done()
			for j := 0; j < 100; j++ {
				if err := p.Submit(fn); err != nil {
					fmt.Println("submit:", err)
					return
				}
			}
		}()
	}

	subWG.Wait() // 等所有提交结束
	p.Close()    // 停止接收新任务
	p.Wait()     // 等已提交任务和 worker 全部退出

	fmt.Println("done:", done.Load()) // 期望 100000
}
