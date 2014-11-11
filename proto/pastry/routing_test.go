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

package pastry

import (
	"bytes"
	"crypto/rand"
	"crypto/x509"
	"io"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/proto"
)

type collector struct {
	delivs []*proto.Message
	lock   sync.RWMutex
}

func (c *collector) Deliver(msg *proto.Message, key *big.Int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.delivs = append(c.delivs, msg)
}

func (c *collector) Forward(msg *proto.Message, key *big.Int) bool {
	return true
}

func TestRouting(t *testing.T) {
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	originals := 4
	additions := 1

	// Make sure there are enough ports to use
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()
	for i := 0; i < originals+additions; i++ {
		config.BootPorts = append(config.BootPorts, 65500+i)
	}
	// Parse encryption key
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Create the callbacks to listen on incoming messages
	apps := []*collector{}
	for i := 0; i < originals; i++ {
		apps = append(apps, &collector{delivs: []*proto.Message{}})
	}
	// Start handful of nodes and ensure valid routing state
	nodes := []*Overlay{}
	for i := 0; i < originals; i++ {
		nodes = append(nodes, New(appId, key, apps[i]))
		if _, err := nodes[i].Boot(); err != nil {
			t.Fatalf("failed to boot original node: %v.", err)
		}
		defer nodes[i].Shutdown()
	}
	time.Sleep(time.Second)

	// Create the messages to pass around
	meta := []byte{0x99, 0x98, 0x97, 0x96}
	msgs := make([][]proto.Message, originals)
	for i := 0; i < originals; i++ {
		msgs[i] = make([]proto.Message, originals)
		for j := 0; j < originals; j++ {
			msgs[i][j] = proto.Message{
				Head: proto.Header{
					Meta: meta,
				},
				Data: []byte(nodes[i].nodeId.String() + nodes[j].nodeId.String()),
			}
			msgs[i][j].Encrypt()
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
	for i := 0; i < originals; i++ {
		apps[i].lock.RLock()
		if len(apps[i].delivs) != originals {
			t.Fatalf("app #%v: message count mismatch: have %v, want %v.", i, len(apps[i].delivs), originals)
		} else {
			for j := 0; j < originals; j++ {
				apps[j].lock.RLock()
				// Check contents (a bit reduced, not every field was verified below)
				if bytes.Compare(meta, apps[j].delivs[i].Head.Meta.([]byte)) != 0 {
					t.Fatalf("send/receive meta mismatch: have %v, want %v.", apps[j].delivs[i].Head.Meta, meta)
				}
				if bytes.Compare(msgs[i][j].Data, apps[j].delivs[i].Data) != 0 {
					t.Fatalf("send/receive data mismatch: have %v, want %v.", apps[j].delivs[i].Data, msgs[i][j].Data)
				}
				apps[j].lock.RUnlock()
			}
		}
		apps[i].lock.RUnlock()
	}
	// Clear out all the collectors
	for i := 0; i < len(apps); i++ {
		apps[i].lock.Lock()
		apps[i].delivs = apps[i].delivs[:0]
		apps[i].lock.Unlock()
	}
	// Start a load of parallel transfers
	sent := make([][]int, originals)
	quit := make([]chan chan struct{}, originals)
	for i := 0; i < originals; i++ {
		sent[i] = make([]int, originals)
		quit[i] = make(chan chan struct{})
	}
	for i := 0; i < originals; i++ {
		go func(idx int, quit chan chan struct{}) {
			for {
				// Send a message to all original nodes
				for j, dst := range nodes[0:originals] {
					// Create the message to pass around
					msg := &proto.Message{
						Head: proto.Header{
							Meta: meta,
						},
						Data: []byte(nodes[idx].nodeId.String() + dst.nodeId.String()),
					}
					msg.Encrypt()

					// Send the message and increment the counter
					nodes[idx].Send(dst.nodeId, msg)
					sent[idx][j]++
					select {
					case q := <-quit:
						q <- struct{}{}
						return
					default:
						// Repeat
					}
				}
			}
		}(i, quit[i])
	}
	// Boot a few more nodes and shut them down afterwards
	pend := new(sync.WaitGroup)
	for i := 0; i < additions; i++ {
		pend.Add(1)
		go func() {
			defer pend.Done()

			temp := New(appId, key, new(nopCallback))
			if _, err := temp.Boot(); err != nil {
				t.Fatalf("failed to boot additional node: %v.", err)
			}
			if err := temp.Shutdown(); err != nil {
				t.Fatalf("failed to terminate additional node: %v.", err)
			}
		}()
	}
	pend.Wait()

	// Terminate all the senders
	for i := 0; i < len(quit); i++ {
		q := make(chan struct{})
		quit[i] <- q
		<-q
	}
	time.Sleep(time.Second)

	// Verify send/receive count
	for i := 0; i < originals; i++ {
		count := 0
		for j := 0; j < originals; j++ {
			count += sent[j][i]
		}
		apps[i].lock.RLock()
		if len(apps[i].delivs) != count {
			t.Fatalf("send/receive count mismatch: have %v, want %v.", len(apps[i].delivs), count)
		}
		apps[i].lock.RUnlock()
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
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	b.SetBytes(int64(block))
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Generate a batch of messages to send around
	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, []byte{0x00, 0x01}, []byte{0x02, 0x03}}
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
		msgs[i].Encrypt()
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

// Overlay application callback to wait for a number of messages and signal afterwards.
type waiter struct {
	left int32
	quit chan struct{}
}

func (w *waiter) Deliver(msg *proto.Message, key *big.Int) {
	if atomic.AddInt32(&w.left, -1) == 0 {
		close(w.quit)
	}
}

func (w *waiter) Forward(msg *proto.Message, key *big.Int) bool {
	return true
}

func benchmarkThroughput(b *testing.B, block int) {
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	b.SetBytes(int64(block))
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Generate a bach of messages to send around
	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, []byte{0x00, 0x01}, []byte{0x02, 0x03}}
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
		msgs[i].Encrypt()
	}
	// Create two overlay nodes to communicate
	send := New(appId, key, new(nopCallback))
	send.Boot()
	defer send.Shutdown()

	wait := &waiter{
		left: int32(b.N),
		quit: make(chan struct{}),
	}
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
