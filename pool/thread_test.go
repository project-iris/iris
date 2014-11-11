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

package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A complex test suite (too complex).
func TestThreadPool(t *testing.T) {
	t.Parallel()

	// Create a simple counter task for the pool to execute repeatedly
	count := int32(0)
	task := func() {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&count, 1)
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
	if atomic.LoadInt32(&count) > 0 {
		t.Fatalf("non-started pool executed tasks.")
	}
	// Start the pool and make sure tasks finish in batches
	pool.Start()
	time.Sleep(75 * time.Millisecond)
	if cnt := int(atomic.LoadInt32(&count)); cnt != 3 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", cnt, 3)
	}
	time.Sleep(50 * time.Millisecond)
	if cnt := int(atomic.LoadInt32(&count)); cnt != 6 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", cnt, 6)
	}
	time.Sleep(50 * time.Millisecond)
	if cnt := int(atomic.LoadInt32(&count)); cnt != 9 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", cnt, 9)
	}
	// Verify that pool starts new threads when needed
	for c := 1; c <= 3; c++ {
		atomic.StoreInt32(&count, 0)
		for i := 0; i < c; i++ {
			if err := pool.Schedule(task); err != nil {
				t.Fatalf("failed to schedule task: %v.", err)
			}
		}
		time.Sleep(75 * time.Millisecond)
		if cnt := int(atomic.LoadInt32(&count)); cnt != c {
			t.Fatalf("unexpected finished tasks: have %v, want %v.", cnt, c)
		}
	}
	// Verify that clearing the pool removes all pending tasks
	atomic.StoreInt32(&count, 0)
	for i := 0; i < 6; i++ {
		if err := pool.Schedule(task); err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	time.Sleep(25 * time.Millisecond)
	pool.Clear()
	time.Sleep(100 * time.Millisecond)
	if cnt := int(atomic.LoadInt32(&count)); cnt != 3 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", cnt, 3)
	}
	// Verify that termination waits for running threads and discards rest
	atomic.StoreInt32(&count, 0)
	for i := 0; i < 4; i++ {
		if err := pool.Schedule(task); err != nil {
			t.Fatalf("failed to schedule task: %v.", err)
		}
	}
	time.Sleep(10 * time.Millisecond)
	pool.Terminate(true)
	time.Sleep(150 * time.Millisecond)
	if cnt := int(atomic.LoadInt32(&count)); cnt != 3 {
		t.Fatalf("unexpected finished tasks: have %v, want %v.", cnt, 3)
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

	started := int32(0)
	workers := 32

	// Create the pool and schedule more work than workers
	pool := NewThreadPool(workers)
	for i := 0; i < workers*8; i++ {
		err := pool.Schedule(func() {
			atomic.AddInt32(&started, 1)
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
	if start := int(atomic.LoadInt32(&started)); start != workers {
		t.Fatalf("unexpected tasks started: have %d, want %d.", start, workers)
	}
}

// Tests that pool termination correctly dumps pending tasks and that it waits
// for already running ones to finish.
func TestTerminate(t *testing.T) {
	t.Parallel()

	// Test termination with both clear flags
	for _, clear := range []bool{false, true} {
		started := int32(0)
		workers := 32

		// Create the pool and schedule more work than workers
		pool := NewThreadPool(workers)
		for i := 0; i < workers*8; i++ {
			err := pool.Schedule(func() {
				atomic.AddInt32(&started, 1)
				time.Sleep(10 * time.Millisecond)
			})
			if err != nil {
				t.Fatalf("failed to schedule task: %v.", err)
			}
		}
		pool.Start()

		// Launch a number of terminations to ensure correct blocking and no deadlocks (issue #7)
		for i := 0; i < 16; i++ {
			go pool.Terminate(clear)
		}
		pool.Terminate(clear) // main terminator

		// Ensure terminate blocked until current workers have finished
		if start := int(atomic.LoadInt32(&started)); clear && start != workers {
			t.Fatalf("unexpected tasks started: have %d, want %d.", start, workers)
		} else if !clear && start != workers*8 {
			t.Fatalf("task completion mismatch: have %d, want %d.", start, workers*8)
		}
		// Ensure that no more tasks can be scheduled
		if err := pool.Schedule(func() {}); err == nil {
			t.Fatalf("task scheduling succeeded, shouldn't have.")
		}
		// Verify whether the pool was cleared or not before termination
		time.Sleep(20 * time.Millisecond)
		if start := int(atomic.LoadInt32(&started)); clear && start != workers {
			t.Fatalf("unexpected tasks started: have %d, want %d.", start, workers)
		} else if !clear && start != workers*8 {
			t.Fatalf("task completion mismatch: have %d, want %d.", start, workers*8)
		}
	}
}
