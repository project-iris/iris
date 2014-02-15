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

// A complex test suite (too complex).
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
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	if size := pool.tasks.Size(); size != 9 {
		t.Fatalf("task count mismatch: have %v, want %v.", size, 9)
	}
	time.Sleep(100 * time.Millisecond)
	if count > 0 {
		t.Fatalf("non-started pool executed tasks.")
	}
	// Start the pool and make sure tasks finish in batches
	pool.Start()
	time.Sleep(75 * time.Millisecond)
	if count != 3 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", count, 3)
	}
	time.Sleep(50 * time.Millisecond)
	if count != 6 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", count, 6)
	}
	time.Sleep(50 * time.Millisecond)
	if count != 9 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", count, 9)
	}
	// Verify that pool starts new threads when needed
	for c := 1; c <= 3; c++ {
		count = 0
		for i := 0; i < c; i++ {
			if err := pool.Schedule(task); err != nil {
				t.Fatalf("failed to schedule task: %v.", err)
			}
		}
		time.Sleep(75 * time.Millisecond)
		if count != c {
			t.Fatalf("unexpected finished tasks: have %v, want %v.", count, c)
		}
	}
	// Verify that clearing the pool removes all pending tasks
	count = 0
	for i := 0; i < 6; i++ {
		if err := pool.Schedule(task); err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	time.Sleep(25 * time.Millisecond)
	pool.Clear()
	time.Sleep(100 * time.Millisecond)
	if count != 3 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", count, 3)
	}
	// Verify that termination waits for running threads and discards rest
	count = 0
	for i := 0; i < 4; i++ {
		if err := pool.Schedule(task); err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	time.Sleep(10 * time.Millisecond)
	pool.Terminate()
	time.Sleep(150 * time.Millisecond)
	if count != 3 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", count, 3)
	}
	// Check that no more tasks can be scheduled
	if err := pool.Schedule(task); err == nil {
		t.Fatalf("task scheduling succeeded, shouldn't have.")
	}
}

// Tests that tasks can be scheduled and run successfully.
func TestTasks(t *testing.T) {
	t.Parallel()

	tasks := 1000
	wg := new(sync.WaitGroup)
	wg.Add(tasks)

	// Create and start a new thread pool
	pool := NewThreadPool(32)
	pool.Start()

	// Schedule all the tasks
	for i := 0; i < tasks; i++ {
		if err := pool.Schedule(func() { wg.Done() }); err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	wg.Wait() // deadlock if any task doesn't complete
}

// Tests that tasks are executed in scheduled order.
func TestOrder(t *testing.T) {
	t.Parallel()

	tasks := 1000
	wg := new(sync.WaitGroup)
	wg.Add(tasks)

	// Create and start a new thread pool (one task at a time)
	pool := NewThreadPool(1)
	pool.Start()

	// Schedule tasks to verify their own execution order
	next := 0
	for i := 0; i < tasks; i++ {
		n := i
		err := pool.Schedule(func() {
			if next != n {
				t.Fatalf("unexpected task number: have %d, want %d.", next, n)
			}
			next = n + 1
			wg.Done()
		})
		if err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	wg.Wait() // deadlock if any task doesn't complete
}

// Tests that task dumping is possible, and pool keeps operating afterwards.
func TestClear(t *testing.T) {
	t.Parallel()

	mutex := new(sync.Mutex)
	started := 0
	workers := 32

	// Create the pool and schedule more work than workers
	pool := NewThreadPool(workers)
	for i := 0; i < workers*8; i++ {
		err := pool.Schedule(func() {
			mutex.Lock()
			started++
			mutex.Unlock()

			time.Sleep(10 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	pool.Start() // should start initial tasks
	pool.Clear() // before clear removes them

	// Ensure that further tasks can be scheduled
	if err := pool.Schedule(func() {}); err != nil {
		t.Fatalf("failed to schedule task after clear: %v.", err)
	}
	// Check that only the first batch got processed
	time.Sleep(20 * time.Millisecond)
	if started != workers {
		t.Fatalf("unexpected tasks started: have %d, want %d.", started, workers)
	}
}

// Tests that pool termination correctly dumps pending tasks and that it waits
// for already running ones to finish.
func TestTerminate(t *testing.T) {
	t.Parallel()

	mutex := new(sync.Mutex)
	started := 0
	workers := 32

	// Create the pool and schedule more work than workers
	pool := NewThreadPool(workers)
	for i := 0; i < workers*8; i++ {
		err := pool.Schedule(func() {
			mutex.Lock()
			started++
			mutex.Unlock()

			time.Sleep(10 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	pool.Start()     // should start initial tasks
	pool.Terminate() // before terminate removes them

	// Ensure terminate blocked until current workers have finished
	if started != workers {
		t.Fatalf("unexpected tasks started: have %d, want %d.", started, workers)
	}
	// Ensure that no more tasks can be scheduled
	if err := pool.Schedule(func() {}); err == nil {
		t.Fatalf("task scheduling succeeded, shouldn't have.")
	}
	time.Sleep(20 * time.Millisecond)

	// No extra tasks should finish
	if started != workers {
		t.Fatalf("unexpected tasks started: have %d, want %d.", started, workers)
	}
}
