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
	"io"
	"net"
	"testing"
	"time"

	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/proto"
)

func TestForward(t *testing.T) {
	t.Parallel()

	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	// Start the server and connect with a client
	sock, err := Listen(addr, key)
	if err != nil {
		t.Fatalf("failed to start the session listener: %v.", err)
	}
	sock.Accept(10 * time.Millisecond)

	client, err := Dial("localhost", addr.Port, key)
	if err != nil {
		t.Fatalf("failed to connect to the server: %v.", err)
	}
	server := <-sock.Sink

	quit := make(chan struct{})

	// Create the sender and receiver channels for both session sides
	cliApp := make(chan *proto.Message, 2)
	srvApp := make(chan *proto.Message, 2)

	cliNet := client.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	srvNet := server.Communicate(srvApp, quit) // Hack: reuse prev live quit channel

	// Generate the messages to transmit
	msgs := make([]proto.Message, 10)
	for i := 0; i < len(msgs); i++ {
		key := make([]byte, 20)
		iv := make([]byte, 20)
		data := make([]byte, 20)

		io.ReadFull(rand.Reader, key)
		io.ReadFull(rand.Reader, iv)
		io.ReadFull(rand.Reader, data)
		msgs[i] = proto.Message{proto.Header{[]byte("meta"), key, iv}, data}
	}
	// Send from client to server
	go func() {
		for i := 0; i < len(msgs); i++ {
			cliNet <- &msgs[i]
		}
	}()
	recvs := make([]proto.Message, 10)
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
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Meta.([]byte), []byte("meta")) != 0 {
			t.Errorf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	// Send from server to client
	go func() {
		for i := 0; i < len(msgs); i++ {
			srvNet <- &msgs[i]
		}
	}()
	recvs = make([]proto.Message, 10)
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
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Meta.([]byte), []byte("meta")) != 0 {
			t.Errorf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	// Tear down the listener
	if err := sock.Close(); err != nil {
		t.Fatalf("failed to terminate session listener: %v.", err)
	}
	close(quit)
}

func BenchmarkLatency1Byte(b *testing.B) {
	benchmarkLatency(b, 1)
}

func BenchmarkLatency4Byte(b *testing.B) {
	benchmarkLatency(b, 4)
}

func BenchmarkLatency16Byte(b *testing.B) {
	benchmarkLatency(b, 16)
}

func BenchmarkLatency64Byte(b *testing.B) {
	benchmarkLatency(b, 64)
}

func BenchmarkLatency256Byte(b *testing.B) {
	benchmarkLatency(b, 256)
}

func BenchmarkLatency1KByte(b *testing.B) {
	benchmarkLatency(b, 1024)
}

func BenchmarkLatency4KByte(b *testing.B) {
	benchmarkLatency(b, 4096)
}

func BenchmarkLatency16KByte(b *testing.B) {
	benchmarkLatency(b, 16384)
}

func BenchmarkLatency64KByte(b *testing.B) {
	benchmarkLatency(b, 65536)
}

func BenchmarkLatency256KByte(b *testing.B) {
	benchmarkLatency(b, 262144)
}

func BenchmarkLatency1MByte(b *testing.B) {
	benchmarkLatency(b, 1048576)
}

func benchmarkLatency(b *testing.B, block int) {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	// Start the server
	sock, err := Listen(addr, key)
	if err != nil {
		b.Fatalf("failed to start the session listener: %v.", err)
	}
	sock.Accept(10 * time.Millisecond)

	client, err := Dial("localhost", addr.Port, key)
	if err != nil {
		b.Fatalf("failed to connect to the server: %v.", err)
	}
	server := <-sock.Sink

	quit := make(chan struct{})

	// Create the sender and receiver channels for both session sides
	cliApp := make(chan *proto.Message, 64)
	srvApp := make(chan *proto.Message, 64)

	cliNet := client.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	server.Communicate(srvApp, quit)           // Hack: reuse prev live quit channel

	// Create a header of the right size
	msgKey := make([]byte, config.PacketCipherBits/8)
	io.ReadFull(rand.Reader, msgKey)
	cipher, _ := config.PacketCipher(msgKey)

	iv := make([]byte, cipher.BlockSize())
	io.ReadFull(rand.Reader, iv)

	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, msgKey, iv}

	// Generate a large batch of random data to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create the client and server runner routines with a sync channel
	sync := make(chan struct{})
	done := make(chan struct{}, 2)

	cliRun := func() {
		for i := 0; i < b.N; i++ {
			cliNet <- &msgs[i]
			<-sync
		}
		done <- struct{}{}
	}
	srvRun := func() {
		for i := 0; i < b.N; i++ {
			<-srvApp
			sync <- struct{}{}
		}
		done <- struct{}{}
	}
	// Send 1 message through to ensure internal caches are up
	cliNet <- &msgs[0]
	<-srvApp

	// Execute the client and server runners, wait till termination and exit
	b.ResetTimer()
	go cliRun()
	go srvRun()

	<-done
	<-done

	b.StopTimer()

	// Tear down the listener
	if err := sock.Close(); err != nil {
		b.Fatalf("failed to terminate session listener: %v.", err)
	}
	close(quit)
}

func BenchmarkThroughput1Byte(b *testing.B) {
	benchmarkThroughput(b, 1)
}

func BenchmarkThroughput4Byte(b *testing.B) {
	benchmarkThroughput(b, 4)
}

func BenchmarkThroughput16Byte(b *testing.B) {
	benchmarkThroughput(b, 16)
}

func BenchmarkThroughput64Byte(b *testing.B) {
	benchmarkThroughput(b, 64)
}

func BenchmarkThroughput256Byte(b *testing.B) {
	benchmarkThroughput(b, 256)
}

func BenchmarkThroughput1KByte(b *testing.B) {
	benchmarkThroughput(b, 1024)
}

func BenchmarkThroughput4KByte(b *testing.B) {
	benchmarkThroughput(b, 4096)
}

func BenchmarkThroughput16KByte(b *testing.B) {
	benchmarkThroughput(b, 16384)
}

func BenchmarkThroughput64KByte(b *testing.B) {
	benchmarkThroughput(b, 65536)
}

func BenchmarkThroughput256KByte(b *testing.B) {
	benchmarkThroughput(b, 262144)
}

func BenchmarkThroughput1MByte(b *testing.B) {
	benchmarkThroughput(b, 1048576)
}

func benchmarkThroughput(b *testing.B, block int) {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	// Start the server
	sock, err := Listen(addr, key)
	if err != nil {
		b.Fatalf("failed to start the session listener: %v.", err)
	}
	sock.Accept(10 * time.Millisecond)

	client, err := Dial("localhost", addr.Port, key)
	if err != nil {
		b.Fatalf("failed to connect to the server: %v.", err)
	}
	server := <-sock.Sink

	quit := make(chan struct{})

	// Create the sender and receiver channels for both session sides
	cliApp := make(chan *proto.Message, 64)
	srvApp := make(chan *proto.Message, 64)

	cliNet := client.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	server.Communicate(srvApp, quit)           // Hack: reuse prev live quit channel

	// Create a header of the right size
	msgKey := make([]byte, config.PacketCipherBits/8)
	io.ReadFull(rand.Reader, msgKey)
	cipher, _ := config.PacketCipher(msgKey)

	iv := make([]byte, cipher.BlockSize())
	io.ReadFull(rand.Reader, iv)

	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, msgKey, iv}

	// Generate a large batch of random data to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create the client and server runner routines
	done := make(chan struct{}, 2)

	cliRun := func() {
		for i := 0; i < b.N; i++ {
			cliNet <- &msgs[i]
		}
		done <- struct{}{}
	}
	srvRun := func() {
		for i := 0; i < b.N; i++ {
			<-srvApp
		}
		done <- struct{}{}
	}
	// Send 1 message through to ensure internal caches are up
	cliNet <- &msgs[0]
	<-srvApp

	// Execute the client and server runners, wait till termination and exit
	b.ResetTimer()
	go cliRun()
	go srvRun()

	<-done
	<-done

	b.StopTimer()

	// Tear down the listener
	if err := sock.Close(); err != nil {
		b.Fatalf("failed to terminate session listener: %v.", err)
	}
	close(quit)
}
