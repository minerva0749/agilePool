package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"workerpool/pool"
)

func main() {
	p, err := pool.New(pool.Config{
		Capacity:    64,
		IdleTimeout: 100 * time.Millisecond,
		MaxChunkLen: 64,
		MaxBuffered: 1024,
		Mode:        pool.BLOCK,
	})
	if err != nil {
		log.Fatal(err)
	}

	const total = 1000
	var wg sync.WaitGroup
	wg.Add(total)

	for i := 0; i < total; i++ {
		err := p.Submit(func(context.Context) error {
			defer wg.Done()
			return nil
		})
		if err != nil {
			// 这里只用于演示；生产代码应根据错误决定是否重试。
			wg.Done()
			log.Fatalf("submit: %v", err)
		}
	}

	wg.Wait() // 等任务真正执行完
	p.Close() // 停止接收新任务，dispatcher 排空剩余缓冲
	p.Wait()  // 等 dispatcher 和所有 worker 退出
	fmt.Println("done:", total)
}
