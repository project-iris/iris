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

package heart

import (
	"math/big"
	"testing"
	"time"
)

// Simple heartbeat callback to gather the events
type testCallback struct {
	beat int
	dead []*big.Int
}

func (cb *testCallback) Beat() {
	cb.beat++
}

func (cb *testCallback) Dead(id *big.Int) {
	cb.dead = append(cb.dead, id)
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
	if n := call.beat; n != 1 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 1)
	}
	if n := len(call.dead); n != 0 {
		t.Fatalf("dead event count mismatch: have %v, want %v", n, 0)
	}
	// Insert another entity, check the beats again
	if err := heart.Monitor(bob); err != nil {
		t.Fatalf("failed to monitor bob: %v.", err)
	}
	time.Sleep(beat)
	if n := call.beat; n != 2 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 2)
	}
	if n := len(call.dead); n != 0 {
		t.Fatalf("dead event count mismatch: have %v, want %v", n, 0)
	}
	// Wait another beat, check beats and dead reports
	time.Sleep(beat)
	if n := call.beat; n != 3 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 3)
	}
	if n := len(call.dead); n != 1 {
		t.Fatalf("dead event count mismatch: have %v, want %v", n, 1)
	}
	// Remove dead guy, ping live one, make sure bob doesn't die now
	if err := heart.Unmonitor(alice); err != nil {
		t.Fatalf("failed to unmonitor alice: %v.", err)
	}
	if err := heart.Ping(bob); err != nil {
		t.Fatalf("failed to ping bob: %v.", err)
	}
	time.Sleep(beat)
	if n := call.beat; n != 4 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 4)
	}
	if n := len(call.dead); n != 1 {
		t.Fatalf("dead event count mismatch: have %v, want %v", n, 1)
	}
	// Terminate beater and ensure no more events are fired
	if err := heart.Terminate(); err != nil {
		t.Fatalf("failed to terminate beater: %v.", err)
	}
	time.Sleep(beat)
	if n := call.beat; n != 4 {
		t.Fatalf("beat event count mismatch: have %v, want %v", n, 4)
	}
	if n := len(call.dead); n != 1 {
		t.Fatalf("dead event count mismatch: have %v, want %v", n, 1)
	}
}
