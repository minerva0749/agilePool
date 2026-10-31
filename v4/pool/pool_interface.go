package pool

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

var ErrPoolClosed = errors.New("pool: closed")

type TaskFunc func(ctx context.Context) error

type task struct {
	Fn       TaskFunc
	ctx      context.Context
	deadline time.Time
}

type worker struct {
	tasks chan task
}

type Pool struct {
	cfg          Config
	mu           sync.Mutex     // 保护 idle 和 running 两样共享状态
	idle         []*worker      // 空闲 worker 集合（slice 当栈用）
	running      int            // 当前存活 worker 数（锁内维护，Running() 读它）
	closed       chan struct{}  // Close 时 close 一次，广播给 Submit 和所有 worker
	wg           sync.WaitGroup // Wait() 等所有 worker 退出（V2 你用过的老搭档）
	onceClose    sync.Once
	isFreeWorker chan struct{}
	taskQueue    chan func() // provide buffer for task submitting
}

type Config struct {
	Capacity      int           // worker 数量上限
	IdleTimeout   time.Duration // 空闲多久自动退出
	taskQueueSize int
	nonBlocking   bool //if taskQueue full:true means block and wait;
	// false means return ErrPoolOverload instantly
}

func New(cfg Config) *Pool {
	p := new(Pool)
	p.cfg = cfg
	p.idle = make([]*worker, 0)
	p.running = 0
	p.closed = make(chan struct{})
	p.isFreeWorker = make(chan struct{}, cfg.Capacity)
	p.taskQueue = make(chan func(), cfg.taskQueueSize)
	return p
}

// makeWorker will make a new worker goroutine with a task spread in.
// Must be used inter Mutex
func (p *Pool) makeWorker(firstTask task) {
	wk := &worker{tasks: make(chan task, 1)}
	p.running++
	p.wg.Add(1)

	go func(wk *worker, p *Pool) {
		defer p.wg.Done()
		defer p.decRunning()
		t := firstTask
		for {
			p.runTask(t)
			p.pushBack(wk)
			timeout := time.NewTimer(p.cfg.IdleTimeout)
			/*
				timeout重置不放在内循环是因为select在选择timeout后worker恰好被
				submit弹出仍能处理然后回到内循环，此时timeout.C内信号已被读取，再
				select一次只会选择<-wk.tasks(就算被close了也能正常处理)
			*/
		inner:
			for {
				select {
				case t = <-wk.tasks:
					timeout.Stop()
					break inner
				case <-timeout.C:
					if p.removeIdle(wk) {
						return
					}
				case <-p.closed:
					timeout.Stop()
					if p.removeIdle(wk) {
						return
					}
					t = <-wk.tasks
					p.runTask(t)
					return
				}
			}
		}
	}(wk, p)
}

// submit
// return nil only means that the task have been accepted by worker,if worker
// is popped and task is cancel by ctx.Done or deadline, task would still be
// accepted and submit would method return nil.
func (p *Pool) submit(t task) error {
	if t.ctx == nil {
		t.ctx = context.Background()
	}

	//deadlineC用于SubmitBefore
	var deadlineC <-chan time.Time
	if !t.deadline.IsZero() {
		timer := time.NewTimer(time.Until(t.deadline))
		defer timer.Stop()
		deadlineC = timer.C
	}

	//若等待中被isWorkerFree唤醒后该任务又被取消，则非阻塞地往isWorkerFree补一个token
	//尽管补token容易漏返回路径，但是做成广播会导致惊群，在worker数量大时会产生O(n)负担
	woke := false
	giveBack := func() {
		if !woke {
			return
		}
		select {
		case p.isFreeWorker <- struct{}{}:
		default:
		}
		woke = false
	}

	for {
		select {
		case <-p.closed:
			return ErrPoolClosed
		case <-t.ctx.Done():
			giveBack()
			return t.ctx.Err()
		case <-deadlineC:
			giveBack()
			return context.DeadlineExceeded
		default:
		}
		if !t.deadline.IsZero() && !time.Now().Before(t.deadline) {
			giveBack()
			return context.DeadlineExceeded
		}
		p.mu.Lock()
		if len(p.idle) > 0 {
			if err := abortErr(t); err != nil { // 只读检查，没改状态
				p.mu.Unlock()
				giveBack()
				return err
			}
			wk := p.idle[0]
			p.idle = p.idle[1:]
			p.mu.Unlock()
			wk.tasks <- t // 一旦 pop，就一定要发
			return nil
		}
		if p.running < p.cfg.Capacity {
			if err := abortErr(t); err != nil {
				p.mu.Unlock()
				giveBack()
				return err
			}
			p.makeWorker(t)
			p.mu.Unlock()
			return nil
		}
		p.mu.Unlock()
		woke = false
		select {
		case <-p.isFreeWorker:
			woke = true
			continue
		case <-p.closed:
			return ErrPoolClosed
		case <-t.ctx.Done():
			return t.ctx.Err()
		case <-deadlineC:
			return context.DeadlineExceeded
		}
	}
}

// Submit task without context and deadline
func (p *Pool) Submit(fn TaskFunc) error {
	t := task{Fn: fn, ctx: context.Background()}
	return p.submit(t)
}

// SubmitCtx submit task with context, able to cut off the task with this context
func (p *Pool) SubmitCtx(ctx context.Context, fn TaskFunc) error {
	t := task{Fn: fn, ctx: ctx}
	return p.submit(t)
}

// SubmitBefore submit a TaskFunc with deadline,if deadline exceeded would not be carried out
func (p *Pool) SubmitBefore(fn TaskFunc, timeout time.Duration) error {
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	/*不直接构造context.WithDeadline 复用 SubmitCtx，是因为的 TaskFunc 接 ctx。
	如果 SubmitBefore 把 deadline ctx 填进 Task.ctx，
	一个在 deadline 前 1ms 开始、要跑 10s 的任务
	会在 deadline 被 cancel——语义就从“开始期限”变成了“执行超时”*/
	return p.submit(task{Fn: fn,
		ctx:      context.Background(),
		deadline: time.Now().Add(timeout)})
}

func (p *Pool) Close() {
	p.onceClose.Do(func() {
		close(p.closed)
	})
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

// Running return the number of worker living now
func (p *Pool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// runTask recover task once it panic,and log.Printf
func (p *Pool) runTask(t task) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("task panic: %v", r)
		}
	}()
	if abortErr(t) == nil {
		_ = t.Fn(t.ctx)
	}
}

// clear timed out worker , should not be used inside mutex
func (p *Pool) removeIdle(wk *worker) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, w := range p.idle {
		if w == wk {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			return true
		}
	}
	return false
}

// push worker back to pool
func (p *Pool) pushBack(wk *worker) {
	p.mu.Lock()
	p.idle = append(p.idle, wk)
	p.mu.Unlock()
	select {
	case p.isFreeWorker <- struct{}{}:
	default:
	}
}

// do -1 operation to running
func (p *Pool) decRunning() {
	p.mu.Lock()
	p.running--
	p.mu.Unlock()
}

// abortErr is a function to check a task's ctx.Done and deadline
func abortErr(t task) error {
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	default:
		if !t.deadline.IsZero() && !time.Now().Before(t.deadline) {
			return context.DeadlineExceeded
		}
	}
	return nil
}
