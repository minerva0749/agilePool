package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// T2 固定的提交错误语义：
//
//	nil task > 非正 timeout（参数） > 池已关闭 > ctx 取消 > 排队 deadline > 队列满
//
// 参数错误（nil task、非正 timeout）先于池状态，属于调用方程序错误；
// 进入队列后，池生命周期状态（closed）优先于任务级运行时条件（ctx/deadline）。

func TestSubmitNilTaskRejected(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    2,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	calls := []struct {
		name string
		call func() error
	}{
		{"Submit", func() error { return p.Submit(nil) }},
		{"SubmitCtx", func() error { return p.SubmitCtx(context.Background(), nil) }},
		{"SubmitBefore", func() error { return p.SubmitBefore(nil, time.Second) }},
	}
	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, ErrNilTask) {
				t.Fatalf("%s(nil) = %v, want ErrNilTask", tc.name, err)
			}
		})
	}

	if got := p.queue.size(); got != 0 {
		t.Fatalf("nil task must not be enqueued, queue size = %d", got)
	}

	// nil 拒绝不能影响后续任务。
	ran := make(chan struct{}, 1)
	if err := p.Submit(func(context.Context) error {
		ran <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit after nil task: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not accept a task after nil-task rejection")
	}
	closeAndWait(t, p)
}

// nil task 是参数错误，优先于 ctx 取消和池关闭。
func TestNilTaskPriority(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.SubmitCtx(ctx, nil); !errors.Is(err, ErrNilTask) {
		t.Fatalf("SubmitCtx(canceled, nil) = %v, want ErrNilTask", err)
	}

	p.Close()
	if err := p.Submit(nil); !errors.Is(err, ErrNilTask) {
		t.Fatalf("Submit(nil) on closed pool = %v, want ErrNilTask", err)
	}
	closeAndWait(t, p)
}

// 池关闭是终态，优先于任务 ctx 取消：所有提交在 Close 后都应稳定报告
// ErrPoolClosed，方便 producer 循环无歧义地终止。
func TestClosedPoolBeatsCanceledContext(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})
	p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var executed atomic.Int64
	err := p.SubmitCtx(ctx, func(context.Context) error {
		executed.Add(1)
		return nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("SubmitCtx on closed pool with canceled ctx = %v, want ErrPoolClosed", err)
	}
	if got := executed.Load(); got != 0 {
		t.Fatalf("task executed %d times, want 0", got)
	}
	closeAndWait(t, p)
}

// 队列层的错误优先级：
//   - closed 优先于 ctx/deadline（即使两者同时成立）；
//   - ctx 优先于 deadline；
//   - 开放式队列的过期 deadline 仍返回 DeadlineExceeded。
func TestPushErrorPriority(t *testing.T) {
	pastDeadline := time.Now().Add(-time.Second)

	t.Run("closed beats canceled ctx", func(t *testing.T) {
		q := newTaskQueue(1, 1)
		q.close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := q.push(task{}, ctx, pastDeadline, BLOCK)
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("push = %v, want ErrPoolClosed", err)
		}
	})

	t.Run("closed beats expired deadline", func(t *testing.T) {
		q := newTaskQueue(1, 1)
		q.close()
		err := q.push(task{}, context.Background(), pastDeadline, BLOCK)
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("push = %v, want ErrPoolClosed", err)
		}
	})

	t.Run("canceled ctx beats expired deadline", func(t *testing.T) {
		q := newTaskQueue(1, 1)
		defer q.close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := q.push(task{}, ctx, pastDeadline, BLOCK)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("push = %v, want context.Canceled", err)
		}
	})

	t.Run("open queue expired deadline", func(t *testing.T) {
		q := newTaskQueue(1, 1)
		defer q.close()
		err := q.push(task{}, context.Background(), pastDeadline, BLOCK)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("push = %v, want DeadlineExceeded", err)
		}
		if got := q.size(); got != 0 {
			t.Fatalf("queue size = %d, want 0", got)
		}
	})
}

// 非正 timeout 是参数错误：在进入队列前直接拒绝，不等待，也不报告池状态。
func TestNonPositiveTimeoutIsArgumentError(t *testing.T) {
	p := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: 50 * time.Millisecond,
		MaxChunkLen: 4,
		MaxBuffered: 8,
		Mode:        BLOCK,
	})

	for _, d := range []time.Duration{0, -time.Second} {
		if err := p.SubmitBefore(noopTask, d); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("SubmitBefore(timeout=%v) = %v, want DeadlineExceeded", d, err)
		}
	}

	p.Close()
	// 关闭后仍按参数错误优先返回，与文档的优先级说明一致。
	if err := p.SubmitBefore(noopTask, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SubmitBefore(timeout=0) on closed pool = %v, want DeadlineExceeded", err)
	}
	closeAndWait(t, p)
}

func TestConfigDefaultsApplied(t *testing.T) {
	base := newTestPool(t, Config{Capacity: 1})
	if got := base.cfg.MaxChunkLen; got != defaultMaxChunkLen {
		t.Fatalf("MaxChunkLen = %d, want %d", got, defaultMaxChunkLen)
	}
	if got := base.cfg.MaxBuffered; got != defaultMaxChunkLen {
		t.Fatalf("MaxBuffered = %d, want %d (one chunk)", got, defaultMaxChunkLen)
	}
	if got := base.cfg.IdleTimeout; got != defaultIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", got, defaultIdleTimeout)
	}
	closeAndWait(t, base)

	withChunk := newTestPool(t, Config{Capacity: 1, MaxChunkLen: 8})
	if got := withChunk.cfg.MaxBuffered; got != 8 {
		t.Fatalf("MaxBuffered = %d, want 8 (MaxChunkLen)", got)
	}
	closeAndWait(t, withChunk)

	unbounded := newTestPool(t, Config{Capacity: 1, MaxBuffered: -1})
	if got := unbounded.cfg.MaxBuffered; got != -1 {
		t.Fatalf("MaxBuffered = %d, want -1 (unbounded)", got)
	}
	closeAndWait(t, unbounded)

	negative := newTestPool(t, Config{
		Capacity:    1,
		IdleTimeout: -time.Second,
		MaxChunkLen: -5,
	})
	if got := negative.cfg.MaxChunkLen; got != defaultMaxChunkLen {
		t.Fatalf("MaxChunkLen = %d, want default %d", got, defaultMaxChunkLen)
	}
	if got := negative.cfg.IdleTimeout; got != defaultIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want default %v", got, defaultIdleTimeout)
	}
	closeAndWait(t, negative)
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"zero capacity", Config{}},
		{"negative capacity", Config{Capacity: -1}},
		{"invalid mode", Config{Capacity: 1, Mode: BlockMode(2)}},
		{"max buffered below -1", Config{Capacity: 1, MaxBuffered: -2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(tc.cfg)
			if err == nil {
				if p != nil {
					p.Close()
					p.WaitPool()
				}
				t.Fatal("New returned nil error for invalid config")
			}
			if p != nil {
				t.Fatal("New returned a non-nil pool together with an error")
			}
		})
	}
}

func TestNewAcceptsBoundaryConfig(t *testing.T) {
	cases := []Config{
		{Capacity: 1},
		{Capacity: 1, MaxBuffered: -1},
		{Capacity: 1, MaxChunkLen: 1, MaxBuffered: 1},
		{Capacity: 1, MaxChunkLen: 2, MaxBuffered: 5},
	}
	for i, cfg := range cases {
		p, err := New(cfg)
		if err != nil {
			t.Fatalf("case %d: New(%+v) = %v, want nil error", i, cfg, err)
		}
		closeAndWait(t, p)
	}
}
