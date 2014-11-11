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

package bootstrap

import (
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/project-iris/iris/config"
)

// Tests that ports are automatically selected from a range if conflicting.
func TestPortSelection(t *testing.T) {
	// Create the localhost IP net
	ipnet := &net.IPNet{
		IP:   net.IPv4(127, 0, 0, 1),
		Mask: net.IPv4Mask(0xff, 0, 0, 0),
	}
	// Make sure bootstrappers can select unused ports
	for i := 0; i < len(config.BootPorts); i++ {
		if booter, _, err := New(ipnet, []byte("magic"), big.NewInt(int64(i)), 11111); err != nil {
			t.Fatalf("failed to create bootstrapper: %v.", err)
		} else {
			if err := booter.Boot(); err != nil {
				t.Fatalf("failed to boot bootstrapper: %v.", err)
			}
			defer booter.Terminate()
		}
	}
	// Ensure failure after all ports are used
	if _, _, err := New(ipnet, []byte("magic"), big.NewInt(333), 11111); err == nil {
		t.Errorf("bootstrapper created even though no ports were available.")
	}
}

// Tests that correctly configured bootstrappers can indeed find each other.
func TestBootstrap(t *testing.T) {
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
	select {
	case <-time.After(100 * time.Millisecond):
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
	select {
	case <-time.After(250 * time.Millisecond):
		// Do nothing
	case a := <-evs1:
		t.Fatalf("extra address on first booter: %v.", a)
	case a := <-evs2:
		t.Fatalf("extra address on second booter: %v.", a)
	}
}
