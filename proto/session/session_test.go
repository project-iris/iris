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
	cliApp := make(chan *Message, 2)
	srvApp := make(chan *Message, 2)

	cliNet := cliSes.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	srvNet := srvSes.Communicate(srvApp, quit) // Hack: reuse prev live quit channel

	// Generate the messages to transmit
	msgs := make([]Message, 10)
	for i := 0; i < len(msgs); i++ {
		key := make([]byte, 20)
		iv := make([]byte, 20)
		data := make([]byte, 20)

		io.ReadFull(rand.Reader, key)
		io.ReadFull(rand.Reader, iv)
		io.ReadFull(rand.Reader, data)
		msgs[i] = Message{Header{[]byte("meta"), key, iv, nil}, data}
	}
	// Send from client to server
	go func() {
		for i := 0; i < len(msgs); i++ {
			cliNet <- &msgs[i]
		}
	}()
	recvs := make([]Message, 10)
	for i := 0; i < len(msgs); i++ {
		timeout := time.Tick(250 * time.Millisecond)
		select {
		case msg := <-srvApp:
			recvs[i] = *msg
		case <-timeout:
			t.Errorf("receive timed out")
			break
		}
	}
	for i := 0; i < 10; i++ {
		if bytes.Compare(msgs[i].Data, recvs[i].Data) != 0 || bytes.Compare(msgs[i].Head.Key, recvs[i].Head.Key) != 0 ||
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Mac, recvs[i].Head.Mac) != 0 ||
			bytes.Compare(msgs[i].Head.Meta, []byte("meta")) != 0 {
			t.Errorf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	// Send from server to client
	go func() {
		for i := 0; i < len(msgs); i++ {
			srvNet <- &msgs[i]
		}
	}()
	recvs = make([]Message, 10)
	for i := 0; i < len(msgs); i++ {
		timeout := time.Tick(250 * time.Millisecond)
		select {
		case msg := <-cliApp:
			recvs[i] = *msg
		case <-timeout:
			t.Errorf("receive timed out")
			break
		}
	}
	for i := 0; i < 10; i++ {
		if bytes.Compare(msgs[i].Data, recvs[i].Data) != 0 || bytes.Compare(msgs[i].Head.Key, recvs[i].Head.Key) != 0 ||
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Mac, recvs[i].Head.Mac) != 0 ||
			bytes.Compare(msgs[i].Head.Meta, []byte("meta")) != 0 {
			t.Errorf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	close(quit)
}

func BenchmarkForwarding1Byte(b *testing.B) {
	benchmarkForwarding(b, 1)
}

func BenchmarkForwarding16Byte(b *testing.B) {
	benchmarkForwarding(b, 16)
}

func BenchmarkForwarding256Byte(b *testing.B) {
	benchmarkForwarding(b, 256)
}

func BenchmarkForwarding1KByte(b *testing.B) {
	benchmarkForwarding(b, 1024)
}

func BenchmarkForwarding4KByte(b *testing.B) {
	benchmarkForwarding(b, 4096)
}

func BenchmarkForwarding16KByte(b *testing.B) {
	benchmarkForwarding(b, 16384)
}

func BenchmarkForwarding64KByte(b *testing.B) {
	benchmarkForwarding(b, 65536)
}

func BenchmarkForwarding256KByte(b *testing.B) {
	benchmarkForwarding(b, 262144)
}

func BenchmarkForwarding1MByte(b *testing.B) {
	benchmarkForwarding(b, 1048576)
}

func benchmarkForwarding(b *testing.B, block int) {
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
	cliApp := make(chan *Message, 64)
	srvApp := make(chan *Message, 64)

	cliNet := cliSes.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	srvSes.Communicate(srvApp, quit)           // Hack: reuse prev live quit channel

	head := Header{[]byte{0x99, 0x98, 0x97, 0x96}, []byte{0x00, 0x01}, []byte{0x02, 0x03}, nil}

	// Generate a large batch of random data to forward
	b.SetBytes(int64(block))
	msgs := make([]Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create the client and server runner routines
	cliDone := make(chan bool)
	srvDone := make(chan bool)

	cliRun := func() {
		for i := 0; i < b.N; i++ {
			cliNet <- &msgs[i]
		}
		cliDone <- true
	}
	srvRun := func() {
		for i := 0; i < b.N; i++ {
			<-srvApp
		}
		srvDone <- true
	}
	// Send 1 message through to ensure internal caches are up
	cliNet <- &msgs[0]
	<-srvApp

	// Execute the client and server runners, wait till termination and exit
	b.ResetTimer()
	go cliRun()
	go srvRun()
	<-cliDone
	<-srvDone

	b.StopTimer()
	close(quit)
}
