package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"workerpool/pool"
)

func main() {
	p := pool.New(pool.Config{Capacity: 1000, IdleTimeout: time.Millisecond * 10})
	var subWG sync.WaitGroup
	var done atomic.Int64
	for range 1000 {
		subWG.Add(1)
		go func() {
			defer subWG.Done()
			for range 100 {
				_ = p.Submit(func() { done.Add(1) })
			}
		}()
		fmt.Println("running worker:", p.Running())
	}
	subWG.Wait()                      // 等所有提交结束
	p.Close()                         // 停止接收
	p.Wait()                          // 等所有 worker 退出（已提交任务跑完）
	fmt.Println("done:", done.Load()) // 应该是 100000
	p.Close()
}
