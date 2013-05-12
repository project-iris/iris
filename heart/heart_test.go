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

package heart

import (
	"math/big"
	"testing"
	"time"
)

// Simple heartbeat callback to gather the events
type testCallback struct {
	ping []*big.Int
	dead []*big.Int
}

func (cb *testCallback) Ping(id *big.Int) {
	cb.ping = append(cb.ping, id)
}

func (cb *testCallback) Dead(id *big.Int) {
	cb.dead = append(cb.dead, id)
}

func TestHeart(t *testing.T) {
	// Some predefined ids
	alice := big.NewInt(314)
	bob := big.NewInt(241)

	// Heartbeat parameters
	beat := time.Duration(50 * time.Millisecond)
	kill := 3
	call := &testCallback{[]*big.Int{}, []*big.Int{}}

	// Create the heartbeat mechanism and monitor some entities
	heart := New(beat, kill, call)
	heart.Monitor(alice)

	// Make sure no ping requests are issued before starting
	for i := 0; i < kill+1; i++ {
		time.Sleep(beat)
	}
	if len(call.ping) > 0 || len(call.dead) > 0 {
		t.Errorf("events received before starting beater: %v", call)
	}
	// Start the beater and check for ping events
	heart.Start()
	time.Sleep(10 * time.Millisecond) // Go out of sync with beater

	time.Sleep(beat)
	if n := len(call.ping); n != 1 {
		t.Errorf("ping event count mismatch: have %v, want %v", n, 1)
	}
	if n := len(call.dead); n != 0 {
		t.Errorf("dead event count mismatch: have %v, want %v", n, 0)
	}
	// Insert another entity, check the pings again
	heart.Monitor(bob)
	time.Sleep(beat)
	if n := len(call.ping); n != 3 {
		t.Errorf("ping event count mismatch: have %v, want %v", n, 3)
	}
	if n := len(call.dead); n != 0 {
		t.Errorf("dead event count mismatch: have %v, want %v", n, 0)
	}
	// Wait another beat, check pings and dead reports
	time.Sleep(beat)
	if n := len(call.ping); n != 4 {
		t.Errorf("ping event count mismatch: have %v, want %v", n, 4)
	}
	if n := len(call.dead); n != 1 {
		t.Errorf("dead event count mismatch: have %v, want %v", n, 1)
	}
	// Remove dead guy, ping live one, make sure bob doesn't die now
	heart.Unmonitor(alice)
	heart.Ping(bob)

	time.Sleep(beat)
	if n := len(call.ping); n != 5 {
		t.Errorf("ping event count mismatch: have %v, want %v", n, 5)
	}
	if n := len(call.dead); n != 1 {
		t.Errorf("dead event count mismatch: have %v, want %v", n, 1)
	}
	// Terminate beater and ensure no more events are fired
	heart.Terminate()
	time.Sleep(beat)
	if n := len(call.ping); n != 5 {
		t.Errorf("ping event count mismatch: have %v, want %v", n, 5)
	}
	if n := len(call.dead); n != 1 {
		t.Errorf("dead event count mismatch: have %v, want %v", n, 1)
	}
}
