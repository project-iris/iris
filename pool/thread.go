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
	"fmt"
	"github.com/karalabe/iris/container/queue"
	"sync"
)

// A task function meant to be started as a go routine.
type Task func()

// A thread pool to place a hard limit on the number of go-routines doing some
// type of (possibly too consuming) work.
type ThreadPool struct {
	mutex sync.Mutex
	tasks *queue.Queue

	idle  int
	total int

	quit chan struct{}
}

// Creates a thread pool with the given concurrent thread capacity.
func NewThreadPool(cap int) *ThreadPool {
	return &ThreadPool{
		tasks: queue.New(),
		idle:  0,
		total: cap,
		quit:  make(chan struct{}),
	}
}

// Starts the thread pool and workers.
func (t *ThreadPool) Start() {
	// Although we could check to start min(tasks, total), but this way is simpler
	for i := 0; i < t.total; i++ {
		go t.runner()
	}
}

// Waits for all threads to finish, terminating the whole pool afterwards. No
// new tasks are accepted in the meanwhile.
func (t *ThreadPool) Terminate() {
	close(t.quit)
}

// Schedules a new task into the thread pool.
func (t *ThreadPool) Schedule(task Task) error {
	// If terminating, return so
	select {
	case <-t.quit:
		return fmt.Errorf("pool terminating")
	default:
		// Ok, schedule
	}
	// Schedule the task and start execution if threads available
	t.mutex.Lock()
	t.tasks.Push(task)
	if t.idle > 0 {
		t.idle--
		go t.runner()
	}
	t.mutex.Unlock()

	return nil
}

// Dumps the waiting tasks from the pool.
func (t *ThreadPool) Clear() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.tasks.Reset()
}

func (t *ThreadPool) runner() {
	// Make sure the idle count is incremented bask even if we die
	defer func() {
		t.mutex.Lock()
		t.idle++
		t.mutex.Unlock()
	}()
	// Execute jobs until all's done
	var task Task
	for done := false; !done; {
		// Check for termination
		select {
		case <-t.quit:
			return
		default:
		}
		// Fetch a new task or terminate if all's done
		t.mutex.Lock()
		if t.tasks.Empty() {
			done = true
		} else {
			task = t.tasks.Pop().(Task)
		}
		t.mutex.Unlock()

		// Execute the task if any was fetched
		if !done {
			task()
		}
	}
}
