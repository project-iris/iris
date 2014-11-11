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

package session

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/project-iris/iris/proto"
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
	sock.Accept(100 * time.Millisecond)

	client, err := Dial("localhost", addr.Port, key)
	if err != nil {
		t.Fatalf("failed to connect to the server: %v.", err)
	}
	server := <-sock.Sink

	// Initiate the message transfers
	client.Start(2)
	server.Start(2)

	// Generate the messages to transmit
	msgs := make([]proto.Message, 1000)
	for i := 0; i < len(msgs); i++ {
		msgs[i] = proto.Message{
			Head: proto.Header{
				Meta: []byte("meta"),
			},
			Data: make([]byte, 256),
		}
		io.ReadFull(rand.Reader, msgs[i].Data)
		msgs[i].Encrypt()
	}
	// Send from client to server
	go func() {
		for i := 0; i < len(msgs); i++ {
			client.CtrlLink.Send <- &msgs[i]
		}
	}()
	recvs := make([]proto.Message, len(msgs))
	for i := 0; i < len(msgs); i++ {
		select {
		case msg := <-server.CtrlLink.Recv:
			recvs[i] = *msg
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("receive timed out")
			break
		}
	}
	for i := 0; i < len(msgs); i++ {
		if bytes.Compare(msgs[i].Data, recvs[i].Data) != 0 || bytes.Compare(msgs[i].Head.Key, recvs[i].Head.Key) != 0 ||
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Meta.([]byte), []byte("meta")) != 0 {
			t.Fatalf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	// Send from server to client
	go func() {
		for i := 0; i < len(msgs); i++ {
			server.CtrlLink.Send <- &msgs[i]
		}
	}()
	recvs = make([]proto.Message, len(msgs))
	for i := 0; i < len(msgs); i++ {
		select {
		case msg := <-client.CtrlLink.Recv:
			recvs[i] = *msg
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("receive timed out")
			break
		}
	}
	for i := 0; i < len(msgs); i++ {
		if bytes.Compare(msgs[i].Data, recvs[i].Data) != 0 || bytes.Compare(msgs[i].Head.Key, recvs[i].Head.Key) != 0 ||
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Meta.([]byte), []byte("meta")) != 0 {
			t.Fatalf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	// Close the client and server sessions (concurrently, as they depend on each other)
	errc := make(chan error)
	go func() { errc <- client.Close() }()
	go func() { errc <- server.Close() }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("failed to close a session: %v.", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("session tear-down timeout.")
		}
	}
	// Tear down the listener
	if err := sock.Close(); err != nil {
		t.Fatalf("failed to terminate session listener: %v.", err)
	}
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
	sock.Accept(100 * time.Millisecond)

	client, err := Dial("localhost", addr.Port, key)
	if err != nil {
		b.Fatalf("failed to connect to the server: %v.", err)
	}
	server := <-sock.Sink

	// Initiate the message transfers
	client.Start(64)
	server.Start(64)

	// Generate a large batch of random data to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i] = proto.Message{
			Head: proto.Header{
				Meta: []byte{0x99, 0x98, 0x97, 0x96},
			},
			Data: make([]byte, block),
		}
		io.ReadFull(rand.Reader, msgs[i].Data)
		msgs[i].Encrypt()
	}
	// Create the client and server runner routines with a sync channel
	syncer := make(chan struct{})
	var run sync.WaitGroup

	cliRun := func() {
		for i := 0; i < b.N; i++ {
			client.CtrlLink.Send <- &msgs[i]
			<-syncer
		}
		run.Done()
	}
	srvRun := func() {
		for i := 0; i < b.N; i++ {
			<-server.CtrlLink.Recv
			syncer <- struct{}{}
		}
		run.Done()
	}
	// Send 1 message through to ensure internal caches are up
	client.CtrlLink.Send <- &msgs[0]
	<-server.CtrlLink.Recv

	// Execute the client and server runners, wait till termination and exit
	b.ResetTimer()
	run.Add(2)
	go cliRun()
	go srvRun()
	run.Wait()
	b.StopTimer()

	// Close the client and server sessions (concurrently, as they depend on each other)
	errc := make(chan error)
	go func() { errc <- client.Close() }()
	go func() { errc <- server.Close() }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				b.Fatalf("failed to close a session: %v.", err)
			}
		case <-time.After(100 * time.Millisecond):
			b.Fatalf("session tear-down timeout.")
		}
	}
	// Tear down the listener
	if err := sock.Close(); err != nil {
		b.Fatalf("failed to terminate session listener: %v.", err)
	}
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
	sock.Accept(100 * time.Millisecond)

	client, err := Dial("localhost", addr.Port, key)
	if err != nil {
		b.Fatalf("failed to connect to the server: %v.", err)
	}
	server := <-sock.Sink

	// Initiate the message transfers
	client.Start(64)
	server.Start(64)

	// Generate a large batch of random data to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i] = proto.Message{
			Head: proto.Header{
				Meta: []byte{0x99, 0x98, 0x97, 0x96},
			},
			Data: make([]byte, block),
		}
		io.ReadFull(rand.Reader, msgs[i].Data)
		msgs[i].Encrypt()
	}
	// Create the client and server runner routines
	var run sync.WaitGroup

	cliRun := func() {
		for i := 0; i < b.N; i++ {
			client.CtrlLink.Send <- &msgs[i]
		}
		run.Done()
	}
	srvRun := func() {
		for i := 0; i < b.N; i++ {
			<-server.CtrlLink.Recv
		}
		run.Done()
	}
	// Send 1 message through to ensure internal caches are up
	client.CtrlLink.Send <- &msgs[0]
	<-server.CtrlLink.Recv

	// Execute the client and server runners, wait till termination and exit
	b.ResetTimer()
	run.Add(2)
	go cliRun()
	go srvRun()
	run.Wait()
	b.StopTimer()

	// Close the client and server sessions (concurrently, as they depend on each other)
	errc := make(chan error)
	go func() { errc <- client.Close() }()
	go func() { errc <- server.Close() }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				b.Fatalf("failed to close a session: %v.", err)
			}
		case <-time.After(100 * time.Millisecond):
			b.Fatalf("session tear-down timeout.")
		}
	}
	// Tear down the listener
	if err := sock.Close(); err != nil {
		b.Fatalf("failed to terminate session listener: %v.", err)
	}
}
