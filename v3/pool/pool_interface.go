package pool

import (
	"errors"
	"log"
	"sync"
	"time"
)

type worker struct {
	tasks chan func()
}
type Pool struct {
	cfg          Config         // Capacity + IdleTimeout（可内嵌，直接 p.Capacity 用）
	mu           sync.Mutex     // 保护 idle 和 running 两样共享状态
	idle         []*worker      // 空闲 worker 集合（slice 当栈用）
	running      int            // 当前存活 worker 数（锁内维护，Running() 读它）
	closed       chan struct{}  // Close 时 close 一次，广播给 Submit 和所有 worker
	wg           sync.WaitGroup // Wait() 等所有 worker 退出（V2 你用过的老搭档）
	onceClose    sync.Once
	isFreeWorker chan struct{}
}
type Config struct {
	Capacity    int           // worker 数量上限
	IdleTimeout time.Duration // 空闲多久自动退出
}

func New(cfg Config) *Pool {
	p := new(Pool)
	p.cfg = cfg
	p.idle = make([]*worker, 0)
	p.running = 0
	p.closed = make(chan struct{})
	p.isFreeWorker = make(chan struct{}, cfg.Capacity)
	return p
}

// 必须在持锁调用
func (p *Pool) makeWorker(firstTask func()) {
	wk := &worker{tasks: make(chan func(), 1)}
	p.running++
	p.wg.Add(1)

	go func(wk *worker, p *Pool) {
		defer p.wg.Done()
		defer p.decRunning()
		task := firstTask

		for {
			p.runTask(task)
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
				case task = <-wk.tasks:
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
					task = <-wk.tasks
					p.runTask(task)
					return
				}
			}
		}
	}(wk, p)
}

func (p *Pool) Submit(task func()) error {
	for {
		select {
		case <-p.closed:
			return errors.New("pool closed")
		default:
		}
		p.mu.Lock()
		if len(p.idle) > 0 {
			wk := p.idle[0]
			p.idle = p.idle[1:]
			p.mu.Unlock()
			wk.tasks <- task
			return nil
		}
		if p.running < p.cfg.Capacity {
			p.makeWorker(task)
			p.mu.Unlock()
			return nil
		}
		p.mu.Unlock()
		select {
		case <-p.isFreeWorker:
			continue
		case <-p.closed:
			return errors.New("pool closed")
		}
	}
}

func (p *Pool) Close() {
	p.onceClose.Do(func() {
		close(p.closed)
	})
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

// Running 返回当前存活的 worker 数（用于测试和观察）
func (p *Pool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

func (p *Pool) runTask(task func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("task panic: %v", r)
		}
	}()
	task()
}

// 用于清除超时worker，无需持锁调用
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

// 执行完的worker放回协程池
func (p *Pool) pushBack(wk *worker) {
	p.mu.Lock()
	p.idle = append(p.idle, wk)
	p.mu.Unlock()
	select {
	case p.isFreeWorker <- struct{}{}:
	default:
	}
}

// 对running-1操作
func (p *Pool) decRunning() {
	p.mu.Lock()
	p.running--
	p.mu.Unlock()
}
