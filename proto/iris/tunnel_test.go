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

package iris

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"github.com/karalabe/iris/proto/carrier"
	"testing"
	"time"
)

// Boots a random carrier for testing purposes
func startCarrier() (carrier.Carrier, error) {
	// Generate a new temporary key for the carrier
	key, _ := rsa.GenerateKey(rand.Reader, 512)

	// Create and boot a new carrier
	car := carrier.New("iris", key)
	if _, err := car.Boot(); err != nil {
		return nil, err
	}
	return car, nil
}

// Very simple test handler to stream inbound tunnel messages into a sink channel.
type tunnelHandler struct {
	sink chan []byte
}

func (h *tunnelHandler) HandleBroadcast(msg []byte) {
	panic("Not implemented!")
}
func (h *tunnelHandler) HandleRequest(req []byte, timeout time.Duration) []byte {
	panic("Not implemented!")
}
func (h *tunnelHandler) HandleTunnel(tun Tunnel) {
	defer tun.Close()
	for {
		if msg, err := tun.Recv(time.Second); err == nil {
			select {
			case h.sink <- msg:
				// Ok
			case <-time.After(time.Second):
				panic("Sink full!")
			}
		} else {
			break
		}
	}
}

// Synchronous tunnel data transfer tests
func TestTunnelSync(t *testing.T) {
	// Boot the carrier
	car, err := startCarrier()
	if err != nil {
		t.Fatalf("failed to boot carrier: %v.", err)
	}
	defer car.Shutdown()

	// Connect to the carrier
	app := "tunnel-sync-test"
	handler := &tunnelHandler{
		sink: make(chan []byte),
	}
	conn := Connect(car, app, handler)
	defer conn.Close()

	// Open a new self-tunnel
	tun, err := conn.Tunnel(app, time.Second)
	if err != nil {
		t.Fatalf("failed to create tunnel: %v.", err)
	}
	defer tun.Close()

	// Send a load of messages one-by-one, waiting for remote arrival
	for i := 0; i < 100000; i++ {
		out := []byte(fmt.Sprintf("%d", i))
		if err := tun.Send(out); err != nil {
			t.Fatalf("failed to send message %d: %v.", i, err)
		}
		select {
		case msg := <-handler.sink:
			if bytes.Compare(out, msg) != 0 {
				t.Fatalf("message %d mismatch: have %v, want %v.", i, string(msg), string(out))
			}
		case <-time.After(time.Second):
			t.Fatalf("transfer %d timeout.", i)
		}
	}
}

// Asynchronous tunnel data transfer tests
func TestTunnelAsync(t *testing.T) {
	// Boot the carrier
	car, err := startCarrier()
	if err != nil {
		t.Fatalf("failed to boot carrier: %v.", err)
	}
	defer car.Shutdown()

	// Connect to the carrier
	app := "tunnel-async-test"
	handler := &tunnelHandler{
		sink: make(chan []byte),
	}
	conn := Connect(car, app, handler)
	defer conn.Close()

	// Open a new self-tunnel
	tun, err := conn.Tunnel(app, time.Second)
	if err != nil {
		t.Fatalf("failed to create tunnel: %v.", err)
	}
	defer tun.Close()

	// Send a load of messages async, reading whilst sending
	messages := 100000

	go func() {
		for i := 0; i < messages; i++ {
			out := []byte(fmt.Sprintf("%d", i))
			if err := tun.Send(out); err != nil {
				t.Fatalf("failed to send message %d: %v.", i, err)
			}
		}
	}()

	for i := 0; i < messages; i++ {
		out := []byte(fmt.Sprintf("%d", i))
		select {
		case msg := <-handler.sink:
			if bytes.Compare(out, msg) != 0 {
				t.Fatalf("message %d mismatch: have %v, want %v.", i, string(msg), string(out))
			}
		case <-time.After(time.Second):
			t.Fatalf("transfer %d timeout.", i)
		}
	}
}
