package pool

import (
	"context"
	"errors"
	"testing"
	"time"
)

// queueTaskKey 只用于队列单测里给任务编号，不参与生产逻辑。
type queueTaskKey struct{}

func queueTask(id int) task {
	return task{
		ctx: context.WithValue(context.Background(), queueTaskKey{}, id),
		Fn:  func(context.Context) error { return nil },
	}
}

func queueTaskID(t task) int {
	return t.ctx.Value(queueTaskKey{}).(int)
}

func mustPush(t *testing.T, q *taskQueue, tk task) {
	t.Helper()
	if err := q.push(tk, context.Background(), time.Time{}, BLOCK); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func mustPop(t *testing.T, q *taskQueue) task {
	t.Helper()
	tk, ok := q.pop()
	if !ok {
		t.Fatal("pop returned false, want a task")
	}
	return tk
}

func TestTaskQueueFIFOAcrossChunks(t *testing.T) {
	q := newTaskQueue(4, -1)
	defer q.close()

	for i := 0; i < 10; i++ {
		mustPush(t, q, queueTask(i))
	}
	if got := q.size(); got != 10 {
		t.Fatalf("size = %d, want 10", got)
	}

	for i := 0; i < 10; i++ {
		if got := queueTaskID(mustPop(t, q)); got != i {
			t.Fatalf("pop[%d] = %d, want %d", i, got, i)
		}
	}
	if got := q.size(); got != 0 {
		t.Fatalf("size after drain = %d, want 0", got)
	}
}

// 覆盖“消费一半又继续生产”的交错场景：读位置和写位置不在同一个块内。
func TestTaskQueueInterleaved(t *testing.T) {
	q := newTaskQueue(4, -1)
	defer q.close()

	mustPush(t, q, queueTask(0))
	mustPush(t, q, queueTask(1))
	mustPush(t, q, queueTask(2))

	if got := queueTaskID(mustPop(t, q)); got != 0 {
		t.Fatalf("pop = %d, want 0", got)
	}
	if got := queueTaskID(mustPop(t, q)); got != 1 {
		t.Fatalf("pop = %d, want 1", got)
	}

	for i := 3; i < 8; i++ {
		mustPush(t, q, queueTask(i))
	}
	for i := 2; i < 8; i++ {
		if got := queueTaskID(mustPop(t, q)); got != i {
			t.Fatalf("pop = %d, want %d", got, i)
		}
	}
}

// 队列为空后复用最后一个 chunk，不能 panic，也不能把旧任务读出来。
func TestTaskQueueChunkReuse(t *testing.T) {
	q := newTaskQueue(2, -1)
	defer q.close()

	mustPush(t, q, queueTask(0))
	mustPush(t, q, queueTask(1))
	for i := 0; i < 2; i++ {
		if got := queueTaskID(mustPop(t, q)); got != i {
			t.Fatalf("pop = %d, want %d", got, i)
		}
	}

	mustPush(t, q, queueTask(2))
	mustPush(t, q, queueTask(3))
	for i := 2; i < 4; i++ {
		if got := queueTaskID(mustPop(t, q)); got != i {
			t.Fatalf("pop = %d, want %d", got, i)
		}
	}
}

func TestTaskQueueNonBlockFull(t *testing.T) {
	q := newTaskQueue(2, 2)
	defer q.close()

	mustPush(t, q, queueTask(0))
	mustPush(t, q, queueTask(1))

	err := q.push(queueTask(2), context.Background(), time.Time{}, NONBLOCK)
	if !errors.Is(err, ErrPoolFull) {
		t.Fatalf("push = %v, want ErrPoolFull", err)
	}
	if got := q.size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}

	for i := 0; i < 2; i++ {
		if got := queueTaskID(mustPop(t, q)); got != i {
			t.Fatalf("pop = %d, want %d", got, i)
		}
	}
}

// BLOCK：队列满时阻塞；ctx 取消后返回 context.Canceled，任务不入队。
func TestTaskQueueBlockingPushCancel(t *testing.T) {
	q := newTaskQueue(2, 2)
	defer q.close()

	mustPush(t, q, queueTask(0))
	mustPush(t, q, queueTask(1))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- q.push(queueTask(2), ctx, time.Time{}, BLOCK)
	}()

	select {
	case err := <-done:
		t.Fatalf("push returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("push = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("push did not return after cancel")
	}
	if got := q.size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
}

// BLOCK：队列满时阻塞；pop 释放空间后，等待者被唤醒并入队。
func TestTaskQueueBlockingPushWake(t *testing.T) {
	q := newTaskQueue(2, 2)
	defer q.close()

	mustPush(t, q, queueTask(0))
	mustPush(t, q, queueTask(1))

	done := make(chan error, 1)
	go func() {
		done <- q.push(queueTask(2), context.Background(), time.Time{}, BLOCK)
	}()

	time.Sleep(20 * time.Millisecond)
	if got := queueTaskID(mustPop(t, q)); got != 0 {
		t.Fatalf("pop = %d, want 0", got)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("push after wake: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked push was not woken by pop")
	}

	if got := q.size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
	for _, want := range []int{1, 2} {
		if got := queueTaskID(mustPop(t, q)); got != want {
			t.Fatalf("pop = %d, want %d", got, want)
		}
	}
}

// BLOCK + SubmitBefore：等待空间期间 deadline 到期，返回 DeadlineExceeded。
func TestTaskQueueBlockingPushDeadline(t *testing.T) {
	q := newTaskQueue(1, 1)
	defer q.close()

	mustPush(t, q, queueTask(0))

	start := time.Now()
	err := q.push(queueTask(1), context.Background(), time.Now().Add(20*time.Millisecond), BLOCK)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("push = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Fatalf("push returned too early: %v", elapsed)
	}
	if got := q.size(); got != 1 {
		t.Fatalf("size = %d, want 1", got)
	}
}

func TestTaskQueueCloseWakesPush(t *testing.T) {
	q := newTaskQueue(1, 1)
	mustPush(t, q, queueTask(0))

	done := make(chan error, 1)
	go func() {
		done <- q.push(queueTask(1), context.Background(), time.Time{}, BLOCK)
	}()

	time.Sleep(20 * time.Millisecond)
	q.close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("push = %v, want ErrPoolClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked push was not woken by close")
	}
}

func TestTaskQueueCloseWakesPop(t *testing.T) {
	q := newTaskQueue(1, 1)

	type popResult struct {
		tk task
		ok bool
	}
	done := make(chan popResult, 1)
	go func() {
		tk, ok := q.pop()
		done <- popResult{tk: tk, ok: ok}
	}()

	time.Sleep(20 * time.Millisecond)
	q.close()

	select {
	case got := <-done:
		if got.ok {
			t.Fatalf("pop returned ok = true, want false after close")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked pop was not woken by close")
	}
}

func TestTaskQueueCanceledBeforePush(t *testing.T) {
	q := newTaskQueue(1, 1)
	defer q.close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := q.push(queueTask(0), ctx, time.Time{}, BLOCK)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("push = %v, want context.Canceled", err)
	}
	if got := q.size(); got != 0 {
		t.Fatalf("size = %d, want 0", got)
	}
}
