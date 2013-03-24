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
	"io"
	"net"
	"testing"
	"time"
)

func TestForwarding(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")

	serverKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	clientKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	sink, quit, _ := Listen(addr, serverKey, store)
	cliSes, _ := Dial("localhost", addr.Port, "client", clientKey, &serverKey.PublicKey)
	srvSes := <-sink

	// Create the sender and receiver channels for both session sides
	cliAppChan := make(chan *Message)
	srvAppChan := make(chan *Message)

	cliNetChan := cliSes.Communicate(cliAppChan, quit) // Hack: reuse prev live quit channel
	srvNetChan := srvSes.Communicate(srvAppChan, quit) // Hack: reuse prev live quit channel

	// Send a message in both directions
	head := Header{"client", "server", []byte{0x00, 0x01}, []byte{0x02, 0x03}, nil}
	pack := Message{&head, []byte{0x04, 0x05}}

	cliNetChan <- &pack
	timeout1 := time.Tick(time.Second)
	select {
	case <-timeout1:
		t.Errorf("server receive timed out.")
	case recv, ok := <-srvAppChan:
		if !ok || bytes.Compare(pack.Data, recv.Data) != 0 || bytes.Compare(head.Key, recv.Head.Key) != 0 ||
			bytes.Compare(head.Iv, recv.Head.Iv) != 0 || bytes.Compare(head.Mac, recv.Head.Mac) != 0 ||
			head.Origin != recv.Head.Origin || head.Target != recv.Head.Target {
			t.Errorf("send/receive mismatch: have %v, want %v.", recv, pack)
		}
	}

	head = Header{"server", "client", []byte{0x10, 0x11}, []byte{0x12, 0x13}, nil}
	pack = Message{&head, []byte{0x14, 0x15}}

	srvNetChan <- &pack
	timeout2 := time.Tick(time.Second)
	select {
	case <-timeout2:
		t.Errorf("server receive timed out.")
	case recv, ok := <-cliAppChan:
		if !ok || bytes.Compare(pack.Data, recv.Data) != 0 || bytes.Compare(head.Key, recv.Head.Key) != 0 ||
			bytes.Compare(head.Iv, recv.Head.Iv) != 0 || bytes.Compare(head.Mac, recv.Head.Mac) != 0 ||
			head.Origin != recv.Head.Origin || head.Target != recv.Head.Target {
			t.Errorf("send/receive mismatch: have %v, want %v.", recv, pack)
		}
	}
	close(quit)
}

func BenchmarkForwarding(b *testing.B) {
	b.StopTimer()
	// Setup the benchmark: public keys, stores and sessions
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")

	serverKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	clientKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	sink, quit, _ := Listen(addr, serverKey, store)
	cliSes, _ := Dial("localhost", addr.Port, "client", clientKey, &serverKey.PublicKey)
	srvSes := <-sink

	// Create the sender and receiver channels for both session sides
	cliApp := make(chan *Message)
	srvApp := make(chan *Message)

	cliNet := cliSes.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	srvSes.Communicate(srvApp, quit)           // Hack: reuse prev live quit channel

	head := Header{"client", "server", []byte{0x00, 0x01}, []byte{0x02, 0x03}, nil}

	// Generate a large batch of random data to forward
	block := 8192
	payloads := make([]byte, block*b.N)
	io.ReadFull(rand.Reader, payloads)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// Create the message
		msg := Message{&head, payloads[i*block : (i+1)*block]}

		// Send from the client to the server
		cliNet <- &msg
		<-srvApp
	}
	close(quit)
}
