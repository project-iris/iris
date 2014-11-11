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

package heart

import (
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Simple heartbeat callback to gather the events
type testCallback struct {
	beat int32
	dead []*big.Int
	lock sync.RWMutex
}

func (cb *testCallback) Beat() {
	atomic.AddInt32(&cb.beat, 1)
}

func (cb *testCallback) Dead(id *big.Int) {
	cb.lock.Lock()
	defer cb.lock.Unlock()

	cb.dead = append(cb.dead, id)
}

// Checks synchronously if the dead count matches k.
func (cb *testCallback) assertDead(t *testing.T, k int) {
	cb.lock.RLock()
	defer cb.lock.RUnlock()

	if n := len(cb.dead); n != k {
		t.Fatalf("dead event count mismatch: have %v, want %v", n, k)
	}
}

func TestHeart(t *testing.T) {
	// Some predefined ids
	alice := big.NewInt(314)
	bob := big.NewInt(241)

	// Heartbeat parameters
	beat := time.Duration(25 * time.Millisecond)
	kill := 3
	call := &testCallback{dead: []*big.Int{}}

	// Create the heartbeat mechanism and monitor some entities
	heart := New(beat, kill, call)
	if err := heart.Monitor(alice); err != nil {
		t.Fatalf("failed to monitor alice: %v.", err)
	}
	// Make sure no beat requests are issued before starting
	for i := 0; i < kill+1; i++ {
		time.Sleep(beat)
	}
	if call.beat > 0 || len(call.dead) > 0 {
		t.Fatalf("events received before starting beater: %v", call)
	}
	// Start the beater and check for beat events
	heart.Start()
	time.Sleep(10 * time.Millisecond) // Go out of sync with beater

	time.Sleep(beat)
	if n := int(atomic.LoadInt32(&call.beat)); n != 1 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 1)
	}
	call.assertDead(t, 0)

	// Insert another entity, check the beats again
	if err := heart.Monitor(bob); err != nil {
		t.Fatalf("failed to monitor bob: %v.", err)
	}
	time.Sleep(beat)
	if n := int(atomic.LoadInt32(&call.beat)); n != 2 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 2)
	}
	call.assertDead(t, 0)

	// Wait another beat, check beats and dead reports
	time.Sleep(beat)
	if n := int(atomic.LoadInt32(&call.beat)); n != 3 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 3)
	}
	call.assertDead(t, 1)

	// Remove dead guy, ping live one, make sure bob doesn't die now
	if err := heart.Unmonitor(alice); err != nil {
		t.Fatalf("failed to unmonitor alice: %v.", err)
	}
	if err := heart.Ping(bob); err != nil {
		t.Fatalf("failed to ping bob: %v.", err)
	}
	time.Sleep(beat)
	if n := int(atomic.LoadInt32(&call.beat)); n != 4 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 4)
	}
	call.assertDead(t, 1)

	// Terminate beater and ensure no more events are fired
	if err := heart.Terminate(); err != nil {
		t.Fatalf("failed to terminate beater: %v.", err)
	}
	time.Sleep(beat)
	if n := int(atomic.LoadInt32(&call.beat)); n != 4 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 4)
	}
	call.assertDead(t, 1)
}
