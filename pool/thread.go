// Iris - Decentralized Messaging Framework
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// Iris is dual licensed: you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation, either version 3 of the License, or (at your option) any later
// version.
//
// The framework is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
// more details.
//
// Alternatively, the Iris framework may be used in accordance with the terms
// and conditions contained in a signed written agreement between you and the
// author(s).
//
// Author: peterke@gmail.com (Peter Szilagyi)

// This file contains a thread pool implementation that allows tasks to be
// scheduled and executes them concurrently, but making sure that at all times a
// limited number of threads exist.

package pool

import (
	"errors"
	"sync"

	"github.com/karalabe/iris/container/queue"
)

var ErrTerminating = errors.New("pool terminating")

// A task function meant to be started as a go routine.
type Task func()

// A thread pool to place a hard limit on the number of go-routines doing some
// type of (possibly too consuming) work.
type ThreadPool struct {
	mutex sync.Mutex
	tasks *queue.Queue

	start bool
	idle  int
	total int

	quit bool
	done *sync.Cond
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
func (t *ThreadPool) Terminate() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.quit = true
	t.tasks.Reset()

	for t.idle < t.total {
		t.done.Wait()
	}
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

		t.done.Signal()
	}()

	// Execute all tasks that are available
	for ; task != nil; task = t.next() {
		task()
	}
}

func (t *ThreadPool) next() Task {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.tasks.Empty() { // tasks reset on termination
		return nil
	}

	return t.tasks.Pop().(Task)
}
