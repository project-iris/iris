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

package overlay

import (
	"config"
	"crypto/rand"
	"crypto/x509"
	"io"
	"math/big"
	"proto/session"
	"testing"
	"time"
)

type collector struct {
	delivs []*session.Message
}

func (c *collector) Deliver(msg *session.Message) {
	c.delivs = append(c.delivs, msg)
}

func TestRouting(t *testing.T) {
	// Make sure cleanups terminate before returning
	defer time.Sleep(3 * time.Second)

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
		apps = append(apps, &collector{[]*session.Message{}})
	}
	// Start handful of nodes and ensure valid routing state
	nodes := []*Overlay{}
	for i := 0; i < peers; i++ {
		nodes = append(nodes, New(appId, key, apps[i]))
		if err := nodes[i].Boot(); err != nil {
			t.Errorf("failed to boot nodes: %v.", err)
		}
		defer nodes[i].Shutdown()
	}
	// Wait a while for the handshakes to complete
	time.Sleep(3 * time.Second)

	// Check that each node can route to everybody
	for _, src := range nodes {
		for _, dst := range nodes {
			src.Send(dst.nodeId, new(session.Message))
		}
	}
	// Sleep a bit and verify
	time.Sleep(time.Second)
	for i := 0; i < peers; i++ {
		if len(apps[i].delivs) != peers {
			t.Errorf("app #%v: message count mismatch: %v.", i, apps[i].delivs)
		}
	}
}

type repeater struct {
	over *Overlay
	dest *big.Int
	msgs []session.Message
	left int
	quit chan struct{}
}

func (r *repeater) Deliver(msg *session.Message) {
	if r.left--; r.left < 0 {
		close(r.quit)
	} else {
		r.over.Send(r.dest, &r.msgs[r.left])
	}
}

func BenchmarkPassing(b *testing.B) {
	// Make sure all previously started tests and benchmarks terminate fully
	time.Sleep(time.Second)

	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Generate a bach of messages to send around
	head := session.Header{[]byte{0x99, 0x98, 0x97, 0x96}, []byte{0x00, 0x01}, []byte{0x02, 0x03}, nil}

	block := 8192
	b.SetBytes(int64(block))
	msgs := make([]session.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create two overlay nodes to communicate
	send := New(appId, key, nil)
	send.Boot()
	defer send.Shutdown()

	recvApp := &repeater{send, nil, msgs, b.N, make(chan struct{})}
	recv := New(appId, key, recvApp)
	recvApp.dest = recv.nodeId
	recv.Boot()
	defer recv.Shutdown()

	// Wait a while for booting to finish
	time.Sleep(time.Second)

	// Reset timer and start message passing
	b.ResetTimer()
	recvApp.Deliver(nil)
	<-recvApp.quit
}

type waiter struct {
	left int
	quit chan struct{}
}

func (w *waiter) Deliver(msg *session.Message) {
	if w.left--; w.left <= 0 {
		close(w.quit)
	}
}

func BenchmarkThroughput(b *testing.B) {
	// Make sure all previously started tests and benchmarks terminate fully
	time.Sleep(time.Second)

	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Generate a bach of messages to send around
	head := session.Header{[]byte{0x99, 0x98, 0x97, 0x96}, []byte{0x00, 0x01}, []byte{0x02, 0x03}, nil}

	block := 8192
	b.SetBytes(int64(block))
	msgs := make([]session.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create two overlay nodes to communicate
	send := New(appId, key, nil)
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
	// Wait a while for booting to finish
	time.Sleep(time.Second)

	// Reset timer and start message passing
	b.ResetTimer()
	go sender()
	<-wait.quit
}
