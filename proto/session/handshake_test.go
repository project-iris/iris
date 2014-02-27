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

package session

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"net"
	"testing"
	"time"
)

// Tests whether the session handshake works.
func TestHandshake(t *testing.T) {
	t.Parallel()

	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	// Start the server
	sock, err := Listen(addr, key)
	if err != nil {
		t.Fatalf("failed to start the session listener: %v.", err)
	}
	sock.Accept(10 * time.Millisecond)

	// Connect with a few clients, verifying the crypto primitives
	for i := 0; i < 3; i++ {
		client, err := Dial("localhost", addr.Port, key)
		if err != nil {
			t.Fatalf("test %d: failed to connect to the server: %v.", i, err)
		}
		// Make sure the server also gets back a live session
		select {
		case server := <-sock.Sink:
			// Ensure server and client side crypto primitives match
			clientData := make([]byte, 4096)
			serverData := make([]byte, 4096)

			client.inCipher.XORKeyStream(clientData, clientData)
			server.outCipher.XORKeyStream(serverData, serverData)
			if !bytes.Equal(clientData, serverData) {
				t.Fatalf("test %d: cipher mismatch on the session endpoints", i)
			}
			client.outCipher.XORKeyStream(clientData, clientData)
			server.inCipher.XORKeyStream(serverData, serverData)
			if !bytes.Equal(clientData, serverData) {
				t.Fatalf("test %d: cipher mismatch on the session endpoints", i)
			}
			client.inMacer.Write(clientData)
			server.outMacer.Write(serverData)
			clientData = client.inMacer.Sum(nil)
			serverData = server.outMacer.Sum(nil)
			if !bytes.Equal(clientData, serverData) {
				t.Fatalf("test %d: macer mismatch on the session endpoints", i)
			}
			client.outMacer.Write(clientData)
			server.inMacer.Write(serverData)
			clientData = client.outMacer.Sum(nil)
			serverData = server.inMacer.Sum(nil)
			if !bytes.Equal(clientData, serverData) {
				t.Fatalf("test %d: macer mismatch on the session endpoints", i)
			}
		case <-time.After(10 * time.Millisecond):
			t.Fatalf("test %d: server-side handshake timed out.", i)
		}
	}
	// Ensure the listener can be torn down correctly
	if err := sock.Close(); err != nil {
		t.Fatalf("failed to terminate session listener: %v.", err)
	}
}

// Benchmarks the session setup performance.
func BenchmarkHandshake(b *testing.B) {
	b.StopTimer()

	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	sock, err := Listen(addr, key)
	if err != nil {
		b.Fatalf("failed to start the session listener: %v.", err)
	}
	sock.Accept(10 * time.Millisecond)

	sink := make(chan *Session)

	// Execute the handshake benchmarks
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// Start a dialer on a new thread
		go func() {
			ses, err := Dial("localhost", addr.Port, key)
			if err != nil {
				b.Fatalf("failed to connect to the server: %v.", err)
				close(sink)
			} else {
				sink <- ses
			}
		}()
		// Wait for the negotiated session from both client and server side
		_, ok := <-sink
		if !ok {
			b.Fatalf("client negotiation failed.")
		}
		select {
		case <-sock.Sink:
			// Ok
		case <-time.After(10 * time.Millisecond):
			b.Fatalf("server-side handshake timed out.")
		}
	}
	b.StopTimer()

	// Tear down the listener
	if err := sock.Close(); err != nil {
		b.Fatalf("failed to terminate session listener: %v.", err)
	}
}
