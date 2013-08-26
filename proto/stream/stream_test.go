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
package stream

import (
	"net"
	"testing"
	"time"
)

func TestListen(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Errorf("failed to resolve local address: %v.", err)
	}
	sink, quit, err := Listen(addr)
	if err != nil {
		t.Errorf("failed to listen for incomming streams: %v", err)
	}
	for c := 0; c < 3; c++ {
		sock, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			t.Errorf("test %d: failed to connect to stream listener: %v", c, err)
		}
		timeout := time.Tick(time.Second)
		select {
		case <-sink:
			continue
		case <-timeout:
			t.Errorf("test %d: listener didn't return incoming stream", c)
		}
		sock.Close()
	}
	close(quit)
}

func TestDial(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Errorf("failed to resolve local address: %v.", err)
	}
	sock, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Errorf("failed to listen for incoming TCP connections: %v", err)
	}
	for c := 0; c < 3; c++ {
		strm, err := Dial("localhost", sock.Addr().(*net.TCPAddr).Port)
		if err != nil {
			t.Errorf("test %d: failed to connect to TCP listener: %v", c, err)
		}
		strm.Close()
	}
	sock.Close()
}

func TestSendRecv(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Errorf("failed to resolve local address: %v.", err)
	}
	sink, quit, err := Listen(addr)
	if err != nil {
		t.Errorf("failed to listen for incomming streams: %v", err)
	}
	c2s, err := Dial("localhost", addr.Port)
	if err != nil {
		t.Errorf("failed to connect to stream listener: %v", err)
	}
	s2c := <-sink

	var send1 = struct{ A, B int }{3, 14}
	var recv1 = struct{ A, B int }{}

	err = c2s.Send(send1)
	if err != nil {
		t.Errorf("failed to send client -> server: %v", err)
	}
	err = c2s.Flush()
	if err != nil {
		t.Errorf("failed to flush client -> server: %v", err)
	}
	err = s2c.Recv(&recv1)
	if err != nil {
		t.Errorf("failed to recieve server -> client: %v", err)
	}
	if send1.A != recv1.A || send1.B != recv1.B {
		t.Errorf("sent/received mismatch: have %v, want %v", recv1, send1)
	}

	var send2 = struct{ A, B, C int }{3, 1, 4}
	var recv2 = struct{ A, C int }{}

	err = s2c.Send(send2)
	if err != nil {
		t.Errorf("failed to send server -> client: %v", err)
	}
	err = s2c.Flush()
	if err != nil {
		t.Errorf("failed to flush server -> client: %v", err)
	}
	err = c2s.Recv(&recv2)
	if err != nil {
		t.Errorf("failed to recieve client -> server: %v", err)
	}
	if send2.A != recv2.A || send2.C != recv2.C {
		t.Errorf("sent/received mismatch: have %v, want %v", recv1, send1)
	}

	c2s.Close()
	s2c.Close()
	close(quit)
}
