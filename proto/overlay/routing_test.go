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

package overlay

import (
	"bytes"
	"crypto/rand"
	"crypto/x509"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/proto"
)

type collector struct {
	delivs []*proto.Message
}

func (c *collector) Deliver(msg *proto.Message, key *big.Int) {
	c.delivs = append(c.delivs, msg)
}

func (c *collector) Forward(msg *proto.Message, key *big.Int) bool {
	return true
}

func TestRouting(t *testing.T) {
	// Override the boot and convergence times
	swapConvLimits()
	defer swapConvLimits()

	// Make sure there are enough ports to use
	peers := 4
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()
	for i := 0; i < peers; i++ {
		config.BootPorts = append(config.BootPorts, 65520+i)
	}
	// Parse encryption key
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Create the callbacks to listen on incoming messages
	apps := []*collector{}
	for i := 0; i < peers; i++ {
		apps = append(apps, &collector{[]*proto.Message{}})
	}
	// Start handful of nodes and ensure valid routing state
	nodes := []*Overlay{}
	for i := 0; i < peers; i++ {
		nodes = append(nodes, New(appId, key, apps[i]))
		if _, err := nodes[i].Boot(); err != nil {
			t.Fatalf("failed to boot nodes: %v.", err)
		}
		defer nodes[i].Shutdown()
	}

	// Create the messages to pass around
	meta := []byte{0x99, 0x98, 0x97, 0x96}
	head := proto.Header{make([]byte, len(meta)), []byte{0x00, 0x01}, []byte{0x02, 0x03}}
	copy(head.Meta.([]byte), meta)

	msgs := make([][]proto.Message, peers)
	for i := 0; i < peers; i++ {
		msgs[i] = make([]proto.Message, peers)
		for j := 0; j < peers; j++ {
			msgs[i][j].Head = head
			msgs[i][j].Data = []byte(nodes[i].nodeId.String() + nodes[j].nodeId.String())
		}
	}
	// Check that each node can route to everybody
	for i, src := range nodes {
		for j, dst := range nodes {
			src.Send(dst.nodeId, &msgs[i][j])
			time.Sleep(250 * time.Millisecond) // Makes the deliver order verifiable
		}
	}
	// Sleep a bit and verify
	time.Sleep(time.Second)
	for i := 0; i < peers; i++ {
		if len(apps[i].delivs) != peers {
			t.Fatalf("app #%v: message count mismatch: have %v, want %v.", i, len(apps[i].delivs), peers)
		} else {
			for j := 0; j < peers; j++ {
				// Check contents (a bit reduced, not every field was verified below)
				if bytes.Compare(meta, apps[j].delivs[i].Head.Meta.([]byte)) != 0 {
					t.Fatalf("send/receive meta mismatch: have %v, want %v.", apps[i].delivs[j].Head.Meta, meta)
				}
				if bytes.Compare(msgs[i][j].Data, apps[j].delivs[i].Data) != 0 {
					t.Fatalf("send/receive data mismatch: have %v, want %v.", apps[i].delivs[j].Data, msgs[j][i].Data)
				}
			}
		}
	}
}

func BenchmarkLatency1Byte(b *testing.B) {
	benchmarkLatency(b, 1)
}

func BenchmarkLatency16Byte(b *testing.B) {
	benchmarkLatency(b, 16)
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

// Overlay callback app which will send one message at a time, waiting for delivery
type sequencer struct {
	over *Overlay
	dest *big.Int
	msgs []proto.Message
	left int
	quit chan struct{}
}

func (s *sequencer) Deliver(msg *proto.Message, key *big.Int) {
	if s.left--; s.left < 0 {
		close(s.quit)
	} else {
		s.over.Send(s.dest, &s.msgs[s.left])
	}
}

func (s *sequencer) Forward(msg *proto.Message, key *big.Int) bool {
	return true
}

func benchmarkLatency(b *testing.B, block int) {
	// Override the boot and convergence times
	swapConvLimits()
	defer swapConvLimits()

	b.SetBytes(int64(block))
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Generate a batch of messages to send around
	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, []byte{0x00, 0x01}, []byte{0x02, 0x03}}
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create the sender node
	send := New(appId, key, new(nopCallback))
	send.Boot()
	defer send.Shutdown()

	// Create the receiver app to sequence messages and the associated overlay node
	recvApp := &sequencer{send, nil, msgs, b.N, make(chan struct{})}
	recv := New(appId, key, recvApp)
	recvApp.dest = recv.nodeId
	recv.Boot()
	defer recv.Shutdown()

	// Reset timer and start message passing
	b.ResetTimer()
	recvApp.Deliver(nil, nil)
	<-recvApp.quit
	b.StopTimer()
}

func BenchmarkThroughput1Byte(b *testing.B) {
	benchmarkThroughput(b, 1)
}

func BenchmarkThroughput16Byte(b *testing.B) {
	benchmarkThroughput(b, 16)
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

// Overlay pllication callback to wait for a number of messages and signal afterwards.
type waiter struct {
	left int
	quit chan struct{}
}

func (w *waiter) Deliver(msg *proto.Message, key *big.Int) {
	if w.left--; w.left <= 0 {
		close(w.quit)
	}
}

func (w *waiter) Forward(msg *proto.Message, key *big.Int) bool {
	return true
}

func benchmarkThroughput(b *testing.B, block int) {
	// Override the boot and convergence times
	swapConvLimits()
	defer swapConvLimits()

	b.SetBytes(int64(block))
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Generate a bach of messages to send around
	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, []byte{0x00, 0x01}, []byte{0x02, 0x03}}
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create two overlay nodes to communicate
	send := New(appId, key, new(nopCallback))
	send.Boot()
	defer send.Shutdown()

	wait := &waiter{b.N, make(chan struct{})}
	recv := New(appId, key, wait)
	recv.Boot()
	defer recv.Shutdown()

	// Create the sender to push the messages
	sender := func() {
		for i := 0; i < len(msgs); i++ {
			send.Send(recv.nodeId, &msgs[i])
		}
	}

	// Reset timer and start message passing
	b.ResetTimer()
	go sender()
	<-wait.quit
	b.StopTimer()
}
