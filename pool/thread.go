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
	"sync"

	"github.com/karalabe/iris/container/queue"
)

// A task function meant to be started as a go routine.
type Task func()

// A thread pool to place a hard limit on the number of go-routines doing some
// type of (possibly too consuming) work.
type ThreadPool struct {
	mutex  sync.Mutex
	seed   bool
	task   chan Task
	worker chan struct{}
	clear  chan struct{}
	quit   chan struct{}
}

// Creates a thread pool with the given concurrent thread capacity.
func NewThreadPool(capacity int) *ThreadPool {
	p := &ThreadPool{
		task:   make(chan Task),
		worker: make(chan struct{}, capacity),
		clear:  make(chan struct{}),
		quit:   make(chan struct{}),
	}
	go p.loop()
	return p
}

func (t *ThreadPool) loop() {
	var next Task
	tasks := queue.New()
	worker := t.worker
	// wait for work or signals
	for {
		// get the next task if we need one and it's available
		if next == nil && !tasks.Empty() {
			next = tasks.Pop().(Task)
		}
		if next == nil {
			worker = nil // disable receiving a worker since we don't have a task
		} else {
			worker = t.worker // enable receiving a worker since we do have a task
		}
		select {
		case task := <-t.task:
			tasks.Push(task)
		case <-worker:
			task := next
			go func() {
				task()
				t.worker <- struct{}{}
			}()
			next = nil
		case <-t.clear:
			tasks.Reset()
			next = nil
		case <-t.quit:
			return
		}
	}
}

// Starts the thread pool and workers.
func (t *ThreadPool) Start() {
	t.mutex.Lock()
	if !t.seed {
		// seed with workers
		for i := 0; i < cap(t.worker); i++ {
			t.worker <- struct{}{}
		}
		t.seed = true
	}
	t.mutex.Unlock()
}

// Waits for all threads to finish, terminating the whole pool afterwards. No
// new tasks are accepted in the meanwhile.
func (t *ThreadPool) Terminate() {
	t.mutex.Lock()
	select {
	case <-t.quit:
		// closed
	default:
		// close and wait for all work to complete
		close(t.quit)
		if t.seed {
			for i := 0; i < cap(t.worker); i++ {
				<-t.worker
			}
		}
	}
	t.mutex.Unlock()
}

// Schedules a new task into the thread pool.
func (t *ThreadPool) Schedule(task Task) error {
	select {
	case t.task <- task:
		return nil
	case <-t.quit:
		return fmt.Errorf("pool terminating")
	}
}

// Dumps the waiting tasks from the pool.
func (t *ThreadPool) Clear() {
	select {
	case t.clear <- struct{}{}:
		// cleared
	case <-t.quit:
		// closed
	}
}
