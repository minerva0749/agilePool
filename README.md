# agilePool — Go 协程池练习

用纯 Go 标准库从零实现一个协程池，按版本递进：固定池 → 关闭/panic 安全 → 按需创建 + 空闲回收。

## 版本

| 版本 | 目录 | 核心能力 | 状态 |
|---|---|---|---|
| v3 | [`v3/`](v3/) | 按需创建 worker、空闲超时自动回收、容量上限背压、panic 隔离 | ✅ 当前 |
| v4 | `v4/` | 计划中 | 🚧 |
| v5 | `v5/` | 计划中 | 📝 |

## v3 设计要点

- **按需创建**：池子初始为空，`Submit` 时优先复用空闲 worker，没有则新建，直到 `Capacity` 上限。
- **空闲回收**：worker 空闲超过 `IdleTimeout` 自行退出，池子能自然缩容到 0。
- **背压**：达到容量上限时，`Submit` 阻塞等待空闲 worker（或池关闭），而不是无限堆积。
- **安全关闭**：`Close` 幂等；已提交任务会被执行完；`Wait` 等所有 worker 退出。
- **panic 隔离**：单个任务 panic 只记日志，不会带走 worker。

## 快速开始

```go
p := pool.New(pool.Config{
    Capacity:    1000,
    IdleTimeout: 10 * time.Millisecond,
})

_ = p.Submit(func() {
    // 你的任务
})

p.Close() // 停止接收新任务
p.Wait()  // 等已提交任务跑完
```

## 运行与测试

```bash
cd v3
go run .        # 1000 并发提交 × 100 任务，最终输出 done: 100000
go test ./...   # 13 个测试：生命周期、容量、复用、关闭、panic 隔离、并发压测
go test -race ./...
```

## 目录

```
v3/
├─ go.mod
├─ main.go
├─ 学习指南.md
└─ pool/
   ├─ pool_interface.go
   └─ pool_test.go
```

## License

[MIT](LICENSE)
