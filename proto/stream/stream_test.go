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

package stream

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

// Tests whether the stream listener can be set up and torn down correctly.
func TestListen(t *testing.T) {
	t.Parallel()

	// Resolve a random local port and listen on it
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to resolve local address: %v.", err)
	}
	sock, err := Listen(addr)
	if err != nil {
		t.Fatalf("failed to listen for incoming streams: %v.", err)
	}
	sock.Accept(10 * time.Millisecond)

	// Establish a few connections and ensure each gets reported
	for i := 0; i < 3; i++ {
		conn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			t.Fatalf("test %d: failed to connect to stream listener: %v.", i, err)
		}
		select {
		case stream := <-sock.Sink:
			stream.Close()
			continue
		case <-time.After(10 * time.Millisecond):
			t.Fatalf("test %d: listener didn't return incoming stream.", i)
		}
		conn.Close()
	}
	// Establish a few connections and ensure they get dropped if not handled
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stderr)

	for i := 0; i < 3; i++ {
		conn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			t.Fatalf("test %d: failed to connect to stream listener: %v.", i, err)
		}
		// Wait for handler to time out
		time.Sleep(15 * time.Millisecond)

		select {
		case stream := <-sock.Sink:
			t.Fatalf("test %d: listener returned timed out stream.", i)
			stream.Close()
		default:
			// Ok
		}
		conn.Close()
	}
	// Close the stream listener and ensure it terminates correctly
	if err := sock.Close(); err != nil {
		t.Fatalf("failed to close listener: %v.", err)
	}
}

// Tests whether a stream connection can be made to an open TCP port and closed.
func TestDial(t *testing.T) {
	t.Parallel()

	// Resolve a random local port and listen on it
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to resolve local address: %v.", err)
	}
	sock, err := net.ListenTCP(addr.Network(), addr)
	if err != nil {
		t.Fatalf("failed to listen for incoming TCP connections: %v.", err)
	}
	defer sock.Close()

	// Establish a few connections and ensure each succeeds
	for i := 0; i < 3; i++ {
		host := fmt.Sprintf("%s:%d", "localhost", sock.Addr().(*net.TCPAddr).Port)
		strm, err := Dial(host, time.Millisecond)
		if err != nil {
			t.Fatalf("test %d: failed to connect to TCP listener: %v.", i, err)
		}
		if err := strm.Close(); err != nil {
			t.Fatalf("test %d: failed to close dialed stream: %v.", i, err)
		}
	}
}

// Tests whether data can be correctly sent and received through a stream connection.
func TestSendRecv(t *testing.T) {
	t.Parallel()

	// Resolve a random local port and listen on it
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to resolve local address: %v.", err)
	}
	sock, err := Listen(addr)
	if err != nil {
		t.Fatalf("failed to listen for incoming streams: %v.", err)
	}
	sock.Accept(10 * time.Millisecond)

	// Establish a stream connection to the listener
	host := fmt.Sprintf("%s:%d", "localhost", addr.Port)
	client, err := Dial(host, time.Millisecond)
	if err != nil {
		t.Fatalf("failed to connect to stream listener: %v.", err)
	}
	server := <-sock.Sink

	// Swap some data a few times to ensure all works
	for i := 0; i < 1000; i++ {
		// Execute a data transfer from client to server
		send1 := struct{ A, B int }{3, 14}
		recv1 := struct{ A, B int }{}
		if err = client.Send(send1); err != nil {
			t.Fatalf("failed to send through client: %v.", err)
		}
		if err = client.Flush(); err != nil {
			t.Fatalf("failed to flush client: %v.", err)
		}
		if err = server.Recv(&recv1); err != nil {
			t.Fatalf("failed to receive through server: %v.", err)
		}
		if send1.A != recv1.A || send1.B != recv1.B {
			t.Fatalf("send/recv mismatch: have %v, want %v.", recv1, send1)
		}
		// Execute a data transfer from server to client
		send2 := struct{ A, B, C int }{3, 1, 4}
		recv2 := struct{ A, C int }{}
		if err = server.Send(send2); err != nil {
			t.Fatalf("failed to send through server: %v", err)
		}
		if err = server.Flush(); err != nil {
			t.Fatalf("failed to flush server: %v", err)
		}
		if err = client.Recv(&recv2); err != nil {
			t.Fatalf("failed to receive through client: %v", err)
		}
		if send2.A != recv2.A || send2.C != recv2.C {
			t.Fatalf("send/recv mismatch: have %v, want %v", recv1, send1)
		}
	}
	// Close the active connections
	if err = client.Close(); err != nil {
		t.Fatalf("failed to close client stream: %v.", err)
	}
	if err = server.Close(); err != nil {
		t.Fatalf("failed to close server stream: %v.", err)
	}
	// Close the stream listener and ensure it terminates correctly
	if err := sock.Close(); err != nil {
		t.Fatalf("failed to close listener: %v.", err)
	}
}
