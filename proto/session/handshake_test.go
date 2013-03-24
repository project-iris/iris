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
package session

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"net"
	"testing"
	"time"
)

func TestHandshake(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")

	serverKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	clientKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	// Start the server and connect with a client
	sink, quit, err := Listen(addr, serverKey, store)
	if err != nil {
		t.Errorf("failed to start the session listener: %v.", err)
	}
	c2sSes, err := Dial("localhost", addr.Port, "client", clientKey, &serverKey.PublicKey)
	if err != nil {
		t.Errorf("failed to connect to the server: %v.", err)
	}
	// Make sure the server also gets back a live session
	timeout := time.Tick(time.Second)
	select {
	case s2cSes := <-sink:
		// Ensure server and client side crypto primitives match
		c2sData := make([]byte, 4096)
		s2cData := make([]byte, 4096)

		c2sSes.inCipher.XORKeyStream(c2sData, c2sData)
		s2cSes.outCipher.XORKeyStream(s2cData, s2cData)
		if !bytes.Equal(c2sData, s2cData) {
			t.Errorf("cipher mismatch on the session endpoints")
		}
		c2sSes.outCipher.XORKeyStream(c2sData, c2sData)
		s2cSes.inCipher.XORKeyStream(s2cData, s2cData)
		if !bytes.Equal(c2sData, s2cData) {
			t.Errorf("cipher mismatch on the session endpoints")
		}
		c2sSes.inMacer.Write(c2sData)
		s2cSes.outMacer.Write(s2cData)
		c2sData = c2sSes.inMacer.Sum(nil)
		s2cData = s2cSes.outMacer.Sum(nil)
		if !bytes.Equal(c2sData, s2cData) {
			t.Errorf("macer mismatch on the session endpoints")
		}
		c2sSes.outMacer.Write(c2sData)
		s2cSes.inMacer.Write(s2cData)
		c2sData = c2sSes.outMacer.Sum(nil)
		s2cData = s2cSes.inMacer.Sum(nil)
		if !bytes.Equal(c2sData, s2cData) {
			t.Errorf("macer mismatch on the session endpoints")
		}
	case <-timeout:
		t.Errorf("server-side handshake timed out.")
	}
	close(quit)
}

func BenchmarkHandshake(b *testing.B) {
	b.StopTimer()
	// Setup the benchmark: public keys, stores and open TCP socket
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")

	serverKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	clientKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	sink, quit, err := Listen(addr, serverKey, store)
	if err != nil {
		b.Errorf("failed to start the session listener: %v.", err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// Start a dialler on a new thread
		ch := make(chan *Session)
		go func() {
			ses, err := Dial("localhost", addr.Port, "client", clientKey, &serverKey.PublicKey)
			if err != nil {
				b.Errorf("failed to connect to the server: %v.", err)
				close(ch)
			} else {
				ch <- ses
			}
		}()
		// Wait for the negotiated session from both client and server side
		_, ok := <-ch
		if !ok {
			b.Errorf("client negotiation failed.")
		}
		timeout := time.Tick(time.Second)
		select {
		case <-sink:
			// All ok, continue
		case <-timeout:
			b.Errorf("server-side handshake timed out.")
		}
	}
	close(quit)
}
