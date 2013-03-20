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
package proto

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"
)

func TestHandshake(t *testing.T) {
	tcpPort := 31420

	// Generate the key-pairs
	serverKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Errorf("failed to generate server key: %v,", err)
	}
	clientKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Errorf("failed to generate client key: %v.", err)
	}
	// Create the server key-store
	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	// Start the server and connect with a client
	sink, quit, err := Listen(tcpPort, serverKey, store)
	if err != nil {
		t.Errorf("failed to start the session listener: %v.", err)
	}
	_, err = Dial("localhost", tcpPort, "client", clientKey, &serverKey.PublicKey)
	if err != nil {
		t.Errorf("failed to connect to the server: %v.", err)
	}
	// Make sure the server also gets back a live session
	timeout := time.Tick(time.Second)
	select {
	case <-sink:
		// Ok, do nothing
	case <-timeout:
		t.Errorf("server-side handshake timed out.")
	}
	close(quit)
}
