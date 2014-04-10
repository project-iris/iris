// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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

package bootstrap

import (
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/project-iris/iris/config"
)

func TestPortSelection(t *testing.T) {
	// Create the localhost IP net
	ipnet := &net.IPNet{
		IP:   net.IPv4(127, 0, 0, 1),
		Mask: net.IPv4Mask(0xff, 0, 0, 0),
	}
	// Make sure bootstrappers can select unused ports
	for i := 0; i < len(config.BootPorts); i++ {
		if bs, _, err := New(ipnet, []byte("magic"), big.NewInt(int64(i)), 11111); err != nil {
			t.Fatalf("failed to create bootstrapper: %v.", err)
		} else {
			if err := bs.Boot(); err != nil {
				t.Fatalf("failed to boot bootstrapper: %v.", err)
			}
			defer bs.Terminate()
		}
	}
	// Ensure failure after all ports are used
	if _, _, err := New(ipnet, []byte("magic"), big.NewInt(333), 11111); err == nil {
		t.Errorf("bootstrapper created even though no ports were available.")
	}
}

func TestScan(t *testing.T) {
	// Define some local constants
	over1, _ := net.ResolveTCPAddr("tcp", "127.0.0.3:33333")
	over2, _ := net.ResolveTCPAddr("tcp", "127.0.0.5:55555")
	ipnet1 := &net.IPNet{
		IP:   over1.IP,
		Mask: over1.IP.DefaultMask(),
	}
	ipnet2 := &net.IPNet{
		IP:   over2.IP,
		Mask: over2.IP.DefaultMask(),
	}
	// Start up two bootstrappers
	bs1, evs1, err := New(ipnet1, []byte("magic"), big.NewInt(1), over1.Port)
	if err != nil {
		t.Fatalf("failed to create first booter: %v.", err)
	}
	if err := bs1.Boot(); err != nil {
		t.Fatalf("failed to boot first booter: %v.", err)
	}
	defer bs1.Terminate()

	bs2, evs2, err := New(ipnet2, []byte("magic"), big.NewInt(2), over2.Port)
	if err != nil {
		t.Fatalf("failed to create second booter: %v.", err)
	}
	if err := bs2.Boot(); err != nil {
		t.Fatalf("failed to boot second booter: %v.", err)
	}
	defer bs2.Terminate()

	// Wait and make sure they found each other and not themselves
	e1, e2 := <-evs1, <-evs2
	if !e1.Addr.IP.Equal(over2.IP) || e1.Addr.Port != over2.Port {
		t.Fatalf("invalid address on first booter: have %v, want %v.", e1.Addr, over2)
	}
	if !e2.Addr.IP.Equal(over1.IP) || e2.Addr.Port != over1.Port {
		t.Fatalf("invalid address on second booter: have %v, want %v.", e2.Addr, over1)
	}

	// Each should report twice (foreign request + foreign response to local request)
	e1, e2 = <-evs1, <-evs2
	if !e1.Addr.IP.Equal(over2.IP) || e1.Addr.Port != over2.Port {
		t.Fatalf("invalid address on first booter: have %v, want %v.", e1.Addr, over2)
	}
	if !e2.Addr.IP.Equal(over1.IP) || e2.Addr.Port != over1.Port {
		t.Fatalf("invalid address on second booter: have %v, want %v.", e2.Addr, over1)
	}

	// Further beats shouldn't arrive (unless the probing catches us, should be rare)
	timeout := time.Tick(250 * time.Millisecond)
	select {
	case <-timeout:
		// Do nothing
	case a := <-evs1:
		t.Fatalf("extra address on first booter: %v.", a)
	case a := <-evs2:
		t.Fatalf("extra address on second booter: %v.", a)
	}
}

func TestMagic(t *testing.T) {
	// Define some local constants
	over1, _ := net.ResolveTCPAddr("tcp", "127.0.0.3:33333")
	over2, _ := net.ResolveTCPAddr("tcp", "127.0.0.5:55555")
	ipnet1 := &net.IPNet{
		IP:   over1.IP,
		Mask: over1.IP.DefaultMask(),
	}
	ipnet2 := &net.IPNet{
		IP:   over2.IP,
		Mask: over2.IP.DefaultMask(),
	}
	// Start up two bootstrappers
	bs1, evs1, err := New(ipnet1, []byte("magic1"), big.NewInt(1), over1.Port)
	if err != nil {
		t.Fatalf("failed to create first booter: %v.", err)
	}
	if err := bs1.Boot(); err != nil {
		t.Fatalf("failed to boot first booter: %v.", err)
	}
	defer bs1.Terminate()

	bs2, evs2, err := New(ipnet2, []byte("magic2"), big.NewInt(2), over2.Port)
	if err != nil {
		t.Fatalf("failed to create second booter: %v.", err)
	}
	if err := bs2.Boot(); err != nil {
		t.Fatalf("failed to boot second booter: %v.", err)
	}
	defer bs2.Terminate()

	// No beats should arrive since magic does not match
	timeout := time.Tick(500 * time.Millisecond)
	select {
	case <-timeout:
		// Do nothing
	case a := <-evs1:
		t.Fatalf("extra address on first booter: %v.", a)
	case a := <-evs2:
		t.Fatalf("extra address on second booter: %v.", a)
	}
}

// Missing test for probing. A bit complicated as a small subnet is needed with
// scanning disabled. Delay for now.
