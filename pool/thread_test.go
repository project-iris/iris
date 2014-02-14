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

package pool

import (
	"sync"
	"testing"
	"time"
)

func TestThreadPool(t *testing.T) {
	t.Parallel()
	// Create a simple counter task for the pool to execute repeatedly
	var mutex sync.Mutex
	count := 0
	task := func() {
		time.Sleep(50 * time.Millisecond)

		mutex.Lock()
		count++
		mutex.Unlock()
	}
	// Create the thread pool
	pool := NewThreadPool(3)

	// Schedule some tasks and make sure they don't run before the pool's started
	for i := 0; i < 9; i++ {
		if err := pool.Schedule(task); err != nil {
			t.Errorf("failed to schedule task: %v.", err)
		}
	}
	if size := pool.tasks.Size(); size != 9 {
		t.Errorf("task count mismatch: have %v, want %v.", size, 9)
	}
	time.Sleep(100 * time.Millisecond)
	if count > 0 {
		t.Errorf("non-started pool executed tasks.")
	}
	// Start the pool and make sure tasks finish in batches
	pool.Start()
	time.Sleep(75 * time.Millisecond)
	if count != 3 {
		t.Errorf("unexpected finished tasks: have %v, want %v.", count, 3)
	}
	time.Sleep(50 * time.Millisecond)
	if count != 6 {
		t.Errorf("unexpected finished tasks: have %v, want %v.", count, 6)
	}
	time.Sleep(50 * time.Millisecond)
	if count != 9 {
		t.Errorf("unexpected finished tasks: have %v, want %v.", count, 9)
	}
	// Verify that pool starts new threads when needed
	for c := 1; c <= 3; c++ {
		count = 0
		for i := 0; i < c; i++ {
			if err := pool.Schedule(task); err != nil {
				t.Errorf("failed to schedule task: %v.", err)
			}
		}
		time.Sleep(75 * time.Millisecond)
		if count != c {
			t.Errorf("unexpected finished tasks: have %v, want %v.", count, c)
		}
	}
	// Verify that clearing the pool removes all pending tasks
	count = 0
	for i := 0; i < 6; i++ {
		if err := pool.Schedule(task); err != nil {
			t.Errorf("failed to schedule task: %v.", err)
		}
	}
	time.Sleep(25 * time.Millisecond)
	pool.Clear()
	time.Sleep(100 * time.Millisecond)
	if count != 3 {
		t.Errorf("unexpected finished tasks: have %v, want %v.", count, 3)
	}
	// Verify that termination waits for running threads and discards rest
	count = 0
	for i := 0; i < 4; i++ {
		if err := pool.Schedule(task); err != nil {
			t.Errorf("failed to schedule task: %v.", err)
		}
	}
	time.Sleep(10 * time.Millisecond)
	pool.Terminate()
	time.Sleep(150 * time.Millisecond)
	if count != 3 {
		t.Errorf("unexpected finished tasks: have %v, want %v.", count, 3)
	}
	// Check that no more tasks can be scheduled
	if err := pool.Schedule(task); err == nil {
		t.Errorf("task scheduling succeeded, shouldn't have.")
	}
}

func TestTasks(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	pool := NewThreadPool(3)
	tasks := 10
	pool.Start()
	wg.Add(tasks)
	for i := 0; i < tasks; i++ {
		if err := pool.Schedule(func() { wg.Done() }); err != nil {
			t.Errorf("failed to schedule task, err: %v", err)
		}
	}
	wg.Wait() // deadlock if any task doesn't complete
}

func TestOrder(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	pool := NewThreadPool(1) // one task at a time
	tasks := 10
	next := 0
	pool.Start()
	wg.Add(tasks)
	for i := 0; i < tasks; i++ {
		n := i
		err := pool.Schedule(func() {
			if next != n {
				t.Errorf("expected task %d, got task %d", next, n)
			}
			next = n + 1
			wg.Done()
		})
		if err != nil {
			t.Errorf("failed to schedule task, err: %v", err)
		}
	}
	wg.Wait() // deadlock if any task doesn't complete
}

func TestClear(t *testing.T) {
	t.Parallel()
	var mutex sync.Mutex
	started := 0
	workers := 2
	pool := NewThreadPool(workers)
	for i := 0; i < workers*2; i++ {
		err := pool.Schedule(func() {
			mutex.Lock()
			started++
			mutex.Unlock()
			time.Sleep(10 * time.Millisecond)
		})
		if err != nil {
			t.Errorf("failed to schedule task, err: %v", err)
		}
	}
	pool.Start() // should start initial tasks
	pool.Clear() // before clear removes them
	// check that no more tasks can be scheduled
	if err := pool.Schedule(func() {}); err != nil {
		t.Errorf("failed to schedule task after clear, err: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if started != workers {
		t.Errorf("unexpected tasks started: have %d, want %d", started, workers)
	}
}

func TestTerminate(t *testing.T) {
	t.Parallel()
	var mutex sync.Mutex
	started := 0
	workers := 2
	pool := NewThreadPool(workers)
	for i := 0; i < workers*2; i++ {
		err := pool.Schedule(func() {
			mutex.Lock()
			started++
			mutex.Unlock()
			time.Sleep(10 * time.Millisecond)
		})
		if err != nil {
			t.Errorf("failed to schedule task, err: %v", err)
		}
	}
	pool.Start()     // should start initial tasks
	pool.Terminate() // before terminate removes them
	// should block until current workers have finished
	if started != workers {
		t.Errorf("unexpected tasks started: have %d, want %d", started, workers)
		t.FailNow()
	}
	// check that no more tasks can be scheduled
	if err := pool.Schedule(func() {}); err == nil {
		t.Errorf("task scheduling succeeded, shouldn't have.")
	}
	time.Sleep(20 * time.Millisecond)
	// no extra tasts should finish
	if started != workers {
		t.Errorf("unexpected tasks started: have %d, want %d", started, workers)
	}
}
