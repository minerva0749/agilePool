# agilePool — Go 协程池练习

用纯 Go 标准库从零实现一个协程池。当前仓库只保留 **v5**：在 v4 的 context/超时语义之上，加入分块链表缓冲队列、背压和 BLOCK / NONBLOCK 提交模式，并补上可观测性回调、运行指标和可插拔的 idle 容器。

## v5 能力

- **分块链表 FIFO 缓冲**：固定大小的 chunk 串成链表，写满追加一块，消费完的块交给 GC 回收。
- **背压**：`MaxBuffered` 控制缓冲任务总数上限，`MaxChunkLen` 控制每个 chunk 的容量。
- **BLOCK**：缓冲满时阻塞提交方；等待空间期间可以被 `ctx` 或 deadline 取消。
- **NONBLOCK**：缓冲满时立即返回 `ErrPoolFull`，把决策权交给调用方。
- **dispatcher**：单消费者从缓冲队列取任务，再交给 worker，提交路径与执行路径解耦。
- **按需创建 worker**：优先复用空闲 worker，没有则新建，直到 `Capacity` 上限。
- **空闲回收**：worker 空闲超过 `IdleTimeout` 自动退出，池子可缩容到 0。
- **取消与超时**：`SubmitCtx` / `SubmitBefore`；已取消或已过期的任务会在派发/执行前被跳过。
- **任务级等待**：`WaitIdle` 等待“已接受 + 正在提交”的任务全部到达终态，不需要先关闭池，返回后仍可继续提交。
- **安全关闭**：`Close` 停止接收新任务，dispatcher 排空缓冲区后退出；`WaitPool` 等 dispatcher 和所有 worker 结束。
- **panic 隔离**：单个任务 panic 只记日志，不会带走 worker。
- **参数校验**：`fn == nil` 返回 `ErrNilTask`；配置默认值与校验集中在 `normalizeConfig`。
- **可观测性**：`Logger` / `ErrorHandler` / `PanicHandler` 与生命周期 `Hooks`（Submitted、Enqueued、Started、Completed、Skipped）都在 `New` 时固定；回调自身的 panic 会被隔离，不会污染 worker 或提交方。
- **运行指标**：`Pending` / `Queued` / `Idle` / `Busy` / `Running` / `Capacity` 的瞬时快照，以及 `Submitted` / `Rejected` / `Completed` / `Dropped` / `Panicked` / `WorkerCreateCount` 的累计计数。
- **可插拔 idle 容器**：包内 `idleContainer` 接口，默认 LIFO 栈（复用最近归还的 worker），另提供 FIFO 实现作为对照；不新增公开 API。

完整的状态机、提交错误优先级、并发不变量和阶段验收记录见
[v5/实现方案与接口.md](v5/实现方案与接口.md)。

## 快速开始

```go
package main

import (
    "context"
    "log"
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

    if err := p.Submit(func(ctx context.Context) error {
        // 在这里执行任务
        return nil
    }); err != nil {
        log.Printf("submit: %v", err)
    }

    p.WaitIdle() // 等当前批次任务全部结束；池仍可继续提交
    p.Close()    // 停止接收新任务，并触发缓冲区排空
    p.WaitPool() // 等 dispatcher 和所有 worker 退出
}
```

`Submit` 返回 `nil` 只表示任务**已经入队**，不表示已经执行；任务之后仍可能因为 `ctx` 取消或 deadline 过期被跳过。

`WaitIdle` 是批次等待，不是关闭：它等待调用时已接受的任务以及正在提交的任务归零。不要从任务函数内部调用它（任务会等待自己）。

### 配置零值

| 字段 | 说明 | 零值/非法值 |
|---|---|---|
| `Capacity` | worker 数量上限 | 必须 `> 0`，否则 `New` 返回错误 |
| `IdleTimeout` | worker 空闲多久后退出 | `<= 0` 时使用默认值 `1s` |
| `MaxChunkLen` | 每个 chunk 的容量 | `<= 0` 时使用默认值 `64` |
| `MaxBuffered` | 缓冲任务总数上限 | `0` 时默认一个 chunk；`-1` 不设限；`< -1` 报错 |
| `Mode` | 缓冲满时的提交行为 | 默认 `BLOCK`；非法值 `New` 返回错误 |
| `Logger` | 池诊断输出（panic 默认报告等） | `nil` 时用 `log.Default()` |
| `ErrorHandler` | 接收任务返回的非 nil error | `nil` 时静默丢弃 |
| `PanicHandler` | 接收被隔离的任务 panic | `nil` 时经 `Logger` 输出默认报告 |
| `Hooks` | 任务/池生命周期回调 | 零值表示不监听 |

### 错误

- `pool.ErrPoolClosed`：池已关闭，不再接受新任务。
- `pool.ErrPoolFull`：`NONBLOCK` 模式下缓冲已满。
- `pool.ErrNilTask`：提交的 `TaskFunc` 为 `nil`。
- `context.Canceled` / `context.DeadlineExceeded`：提交等待期间被取消或到期。

提交错误按固定优先级返回：`ErrNilTask` → `SubmitBefore` 的非正 timeout →
`ErrPoolClosed` → `ctx.Err()` → `DeadlineExceeded` → `ErrPoolFull`。
`Close` 之后即使任务 ctx 已取消，也稳定返回 `ErrPoolClosed`，便于 producer 判断停止。

### 运行指标

```go
p.Pending()    // 已接受（入队/已派发）且尚未到终态的任务数；不含正在 Submit 的
p.Queued()     // 仍在缓冲队列里、还没被 dispatcher pop 的任务数（Queued <= Pending）
p.Running()    // 正在执行 TaskFunc 的 worker 数
p.Idle()       // idle 容器中的 worker 数
p.Busy()       // 已被 dispatcher 认领或正在执行的 worker 数
p.Capacity()   // 配置的 worker 上限

p.Submitted()  // 参数合法的提交次数（含被拒绝的）
p.Rejected()   // 被拒绝的提交次数
p.Completed()  // 到达终态的任务数
p.Dropped()    // 因 ctx 取消/超期被跳过的任务数
p.Panicked()   // panic 被隔离的任务数
p.WorkerCreateCount() // 累计创建的 worker 数（单调递增）
```

每个方法都是一次独立的瞬时快照，不保证跨方法来自同一时刻；`Busy` 是唯一在内部对同一次
`p.mu` 快照计算 `running - len(idle)` 的方法。`Close` + `WaitPool` 之后，`Pending` /
`Queued` / `Running` / `Idle` / `Busy` 回到 0，累计计数与 `Capacity` 不回退。

### 可观测性

```go
p, _ := pool.New(pool.Config{
    Capacity: 64,
    Logger:   log.Default(),
    ErrorHandler: func(ctx context.Context, err error) {
        // 任务返回的非 nil error，在 worker goroutine 上同步调用
    },
    PanicHandler: func(ctx context.Context, recovered any, stack []byte) {
        // 任务 panic 已被隔离，这里决定如何上报
    },
    Hooks: pool.Hooks{
        Submitted: func(ctx context.Context) {},
        Enqueued:  func(ctx context.Context) {},
        Started:   func(ctx context.Context) {},
        Completed: func(ctx context.Context, out pool.TaskOutcome) {},
        Skipped:   func(ctx context.Context, out pool.TaskOutcome) {},
    },
})
```

- `TaskOutcome.Result` 取 `TaskCompleted` / `TaskFailed` / `TaskPanicked` / `TaskCanceled` /
  `TaskDeadlineSkipped`，与五种终态一一对应。
- `Completed` 与 `Skipped` 互斥：被取消或超期跳过的任务只触发 `Skipped`。
- 所有回调都在 `New` 时固定，运行期不可替换，因此读取路径无需加锁。
- 不要在池内部已持锁的路径上调用这些回调（包内实现已遵守）；也不要在任务函数里调 `WaitIdle`。

## 运行与测试

```bash
cd v5
go run .                 # 提交 1000 个任务，输出 done: 1000
go test ./...            # 94 个测试：队列、背压、关闭排空、取消/超时、pending、WaitIdle、panic、指标、hook、idle 容器、压力
go test -race ./...
go test ./pool -run 'TestTaskQueue|TestWaitIdle' -v
go test ./pool -bench . -benchmem -run '^$'
```

## 目录

```text
v5/
├─ go.mod
├─ main.go
├─ 实现方案与接口.md
└─ pool/
   ├─ pool_interface.go      # Config/New/Pool、提交与派发、worker 生命周期
   ├─ task_queue.go          # 分块链表 FIFO 缓冲队列与背压
   ├─ task_state.go          # 五种终态、pendingTracker 与累计计数
   ├─ idle_container.go      # idleContainer 接口 + LIFO/FIFO 实现
   ├─ metrics.go             # 运行指标读取 API
   ├─ observability.go       # Logger / ErrorHandler / PanicHandler / Hooks
   └─ *_test.go              # 单元、回归、压测与 benchmark
```

## 设计要点

```text
提交方 --Submit--> [chunk queue] <--消费-- [dispatcher] --派发--> worker
```

- 多个提交者只操作队列的生产端，dispatcher 是唯一的消费者，因此队列只用一把锁即可。
- `BLOCK` 的等待由 `notFull` 提示、`closed` 信号、`ctx.Done()` 和 deadline 共同驱动；被唤醒后必须在锁内重新检查条件。
- dispatcher 从队列取出任务后，先检查 `ctx`/deadline，再取 worker，避免为不会执行的任务占用 worker。
- `Close` 不会丢弃已经入队的任务：dispatcher 会一直排空到队列关闭且为空，最后关闭 `drained`，worker 再收尾退出。
- 所有终态（完成/error/panic/取消跳过/超期跳过）统一经过 `finishTask`，保证每个被接受的任务恰好扣一次 pending。
- `pendingTracker` 分开记录 `pending`（已接受）与 `submitting`（提交中）；`WaitIdle` 等两者归零，但 `pending` 仍只统计已接受任务。
- 需要等池退出时使用 `Close → WaitIdle → WaitPool`：`WaitIdle` 等任务终态，`WaitPool` 等 goroutine 收尾。
- idle 容器只负责“空闲 worker 的存放顺序”，自身不加锁，所有方法都必须在持有 `p.mu` 期间调用；worker 的 idle 超时由各自的 timer 驱动并回调 `Remove`，容器不保存 idle 起始时间。
- 指标读取不新增锁：`Pending`/累计计数复用 `pendingTracker.mu`，`Idle`/`Busy`/`WorkerCreateCount` 复用 `p.mu`，`Queued` 复用 `queue.mu`。

## License

[MIT](LICENSE)
