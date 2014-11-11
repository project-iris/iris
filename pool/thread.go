// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
//
// Community license: for open source projects and services, Iris is free to use,
// redistribute and/or modify under the terms of the GNU Affero General Public
// License as published by the Free Software Foundation, either version 3, or (at
// your option) any later version.
//
// Evaluation license: you are free to privately evaluate Iris without adhering
// to either of the community or commercial licenses for as long as you like,
// however you are not permitted to publicly release any software or service
// built on top of it without a valid license.
//
// Commercial license: for commercial and/or closed source projects and services,
// the Iris cloud messaging system may be used in accordance with the terms and
// conditions contained in an individually negotiated signed written agreement
// between you and the author(s).

// This file contains a thread pool implementation that allows tasks to be
// scheduled and executes them concurrently, but making sure that at all times a
// limited number of threads exist.

package pool

import (
	"errors"
	"sync"

	"github.com/project-iris/iris/container/queue"
)

var ErrTerminating = errors.New("pool terminating")

// A task function meant to be started as a go routine.
type Task func()

// A thread pool to place a hard limit on the number of go-routines doing some
// type of (possibly too consuming) work.
type ThreadPool struct {
	tasks *queue.Queue // List of pending tasks

	idle  int // Number of idle workers (i.e. not running)
	total int // Maximum pool worker capacity

	start bool // Whether the pool was already started
	quit  bool // Whether the pool was already terminated

	mutex sync.Mutex
	done  *sync.Cond
}

// Creates a thread pool with the given concurrent thread capacity.
func NewThreadPool(cap int) *ThreadPool {
	t := &ThreadPool{
		tasks: queue.New(),
		idle:  cap,
		total: cap,
	}
	t.done = sync.NewCond(&t.mutex)
	return t
}

// Starts the thread pool and workers.
func (t *ThreadPool) Start() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.start {
		for i := 0; i < t.total && !t.tasks.Empty(); i++ {
			t.idle--
			go t.runner(t.tasks.Pop().(Task))
		}
		t.start = true
	}
}

// Waits for all threads to finish, terminating the whole pool afterwards. No
// new tasks are accepted in the meanwhile.
func (t *ThreadPool) Terminate(clear bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.quit = true
	if clear {
		t.tasks.Reset()
	}

	for t.idle < t.total {
		t.done.Wait()
	}
	// Zero out the task queue, which could have reached a significant size
	t.tasks = nil
}

// Schedules a new task into the thread pool.
func (t *ThreadPool) Schedule(task Task) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// If terminating, return so
	if t.quit {
		return ErrTerminating
	}

	if t.start && t.idle > 0 {
		t.idle--
		go t.runner(task)
	} else {
		t.tasks.Push(task)
	}
	return nil
}

// Dumps the waiting tasks from the pool.
func (t *ThreadPool) Clear() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.tasks.Reset()
}

// Runs an initial task, fetching new ones until available.
func (t *ThreadPool) runner(task Task) {
	// Make sure the idle count is incremented back even if we panic
	defer func() {
		t.mutex.Lock()
		// Without this respawn hack there's a race condition where a task
		// may be scheduled after a runner has exited its loop but before it's
		// gotten here to be marked as idle. Do one last check for that case
		// while we have the lock.
		if t.tasks.Empty() {
			t.idle++
		} else {
			go t.runner(t.tasks.Pop().(Task))
		}
		t.mutex.Unlock()
		t.done.Broadcast()
	}()
	// Execute all tasks that are available
	for ; task != nil; task = t.next() {
		task()
	}
}

// Fetches the next task from the queue.
func (t *ThreadPool) next() Task {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.tasks.Empty() { // Note, tasks is reset on termination
		return nil
	}
	return t.tasks.Pop().(Task)
}
