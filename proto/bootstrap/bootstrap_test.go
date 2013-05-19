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
package bootstrap

import (
	"github.com/karalabe/iris/config"
	"net"
	"testing"
	"time"
)

func TestPortSelection(t *testing.T) {
	defer time.Sleep(time.Second)

	// Make sure bootstrappers can select unused ports
	for i := 0; i < len(config.BootPorts); i++ {
		if _, quit, err := Boot(net.IPv4(127, 0, 0, 1), []byte("magic"), 11111); err != nil {
			t.Errorf("failed to start a bootstrapper for each allowed port: %v.", err)
		} else {
			defer close(quit)
		}
	}
	// Ensure failure after all ports are used
	if _, _, err := Boot(net.IPv4(127, 0, 0, 1), []byte("magic"), 11111); err == nil {
		t.Errorf("bootstrapper started even though no ports were available.")
	}
}

func TestScan(t *testing.T) {
	defer time.Sleep(time.Second)

	// Define some local constants
	over1, _ := net.ResolveTCPAddr("tcp", "127.0.0.3:33333")
	over2, _ := net.ResolveTCPAddr("tcp", "127.0.0.5:55555")

	// Start up two bootstrappers
	evs1, quit, err := Boot(over1.IP, []byte("magic"), over1.Port)
	if err != nil {
		t.Errorf("failed to start first booter: %v.", err)
	}
	defer close(quit)

	evs2, quit, err := Boot(over2.IP, []byte("magic"), over2.Port)
	if err != nil {
		t.Errorf("failed to start second booter: %v.", err)
	}
	defer close(quit)

	// Wait and make sure they found each other and not themselves
	e1, e2 := <-evs1, <-evs2
	if !e1.Addr.IP.Equal(over2.IP) || e1.Addr.Port != over2.Port {
		t.Errorf("invalid address on first booter: have %v, want %v.", e1.Addr, over2)
	}
	if !e2.Addr.IP.Equal(over1.IP) || e2.Addr.Port != over1.Port {
		t.Errorf("invalid address on first booter: have %v, want %v.", e2.Addr, over1)
	}

	// Each should report twice (foreign request + foreign response to local request)
	e1, e2 = <-evs1, <-evs2
	if !e1.Addr.IP.Equal(over2.IP) || e1.Addr.Port != over2.Port {
		t.Errorf("invalid address on first booter: have %v, want %v.", e1.Addr, over2)
	}
	if !e2.Addr.IP.Equal(over1.IP) || e2.Addr.Port != over1.Port {
		t.Errorf("invalid address on first booter: have %v, want %v.", e2.Addr, over1)
	}

	// Further beats shouldn't arrive (unless the probing catches us, should be rare)
	timeout := time.Tick(250 * time.Millisecond)
	select {
	case <-timeout:
		// Do nothing
	case a := <-evs1:
		t.Errorf("extra address on first booter: %v.", a)
	case a := <-evs2:
		t.Errorf("extra address on second booter: %v.", a)
	}
}

func TestMagic(t *testing.T) {
	defer time.Sleep(time.Second)

	// Define some local constants
	over1, _ := net.ResolveTCPAddr("tcp", "127.0.0.3:33333")
	over2, _ := net.ResolveTCPAddr("tcp", "127.0.0.5:55555")

	// Start up two bootstrappers
	evs1, quit, err := Boot(over1.IP, []byte("magic1"), over1.Port)
	if err != nil {
		t.Errorf("failed to start first booter: %v.", err)
	}
	defer close(quit)

	evs2, quit, err := Boot(over2.IP, []byte("magic2"), over2.Port)
	if err != nil {
		t.Errorf("failed to start second booter: %v.", err)
	}
	defer close(quit)

	// No beats should arrive since magic does not match
	timeout := time.Tick(500 * time.Millisecond)
	select {
	case <-timeout:
		// Do nothing
	case a := <-evs1:
		t.Errorf("extra address on first booter: %v.", a)
	case a := <-evs2:
		t.Errorf("extra address on second booter: %v.", a)
	}
}

// Missing test for probing. A bit complicated as a small subnet is needed with
// scanning disabled. Delay for now.
