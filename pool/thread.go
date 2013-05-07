// Iris - Distributed Messaging Framework
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
	"github.com/karalabe/cookiejar/queue"
	"sync"
)

// A task function meant to be started as a go routine.
type Task func()

// A thread pool to place a hard limit on the number of go-routines doing some
// type of (possibly too consuming) work.
type ThreadPool struct {
	mutex sync.Mutex
	tasks *queue.Queue

	done chan struct{}
	idle chan struct{}
	wake chan struct{}
	quit chan struct{}
}

// Creates a thread pool with the given concurrent thread capacity.
func NewThreadPool(cap int) *ThreadPool {
	t := &ThreadPool{
		tasks: queue.New(),
		done:  make(chan struct{}, cap),
		idle:  make(chan struct{}, cap),
		wake:  make(chan struct{}, 1),
		quit:  make(chan struct{}),
	}
	for i := 0; i < cap; i++ {
		t.idle <- struct{}{}
	}
	return t
}

// Starts the thread pool and workers.
func (t *ThreadPool) Start() {
	go t.runner()
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
	// Schedule the task for future execution
	t.mutex.Lock()
	t.tasks.Push(task)
	t.mutex.Unlock()

	// Signal the runner to start threads if needed
	select {
	case t.wake <- struct{}{}:
		// Ok, pool notified
	default:
		// Signal is already pending, no need for multiple
	}
	return nil
}

// Dumps the waiting tasks from the pool.
func (t *ThreadPool) Clear() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.tasks.Reset()
}

// Waits for new tasks to be scheduled and executes them.
func (t *ThreadPool) runner() {
	for {
		select {
		case <-t.quit:
			// Wait till pending workers complete and terminate
			for i := 0; i < cap(t.idle); i++ {
				select {
				case <-t.done:
					// Stop a recently finished thread
				case <-t.idle:
					// Stop an idling thread
				}
			}
			return
		case <-t.done:
			// Worker done, give out new task
			t.mutex.Lock()
			if !t.tasks.Empty() {
				t.execute(t.tasks.Pop().(Task))
			} else {
				// No tasks available, mark a thread idle
				t.idle <- struct{}{}
			}
			t.mutex.Unlock()
		case <-t.wake:
			// New tasks were scheduled, execute as many as possible
			t.mutex.Lock()
			for available := true; available; {
				select {
				case <-t.idle:
					if !t.tasks.Empty() {
						t.execute(t.tasks.Pop().(Task))
					} else {
						// Nothing to execute, place thread back into idle pool
						available = false
						t.idle <- struct{}{}
					}
				default:
					available = false
				}
			}
			t.mutex.Unlock()
		}
	}
}

// Executes a given task, signaling the pool on completion.
func (t *ThreadPool) execute(task Task) {
	go func() {
		defer func() { t.done <- struct{}{} }()
		task()
	}()
}
