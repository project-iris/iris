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

package iris

import (
	"fmt"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/proto/carrier"
	"sync"
	"time"
)

type tunnel struct {
	parent *connection
	id     uint64

	peerAddr *carrier.Address
	peerId   uint64

	// Control flow fields
	outNext   uint64        // Next message sequence number to place in the send window
	outAcked  uint64        // Id of the first gap in the acks
	outGrant  uint64        // Message limit the remote endpoint is accepting (excluded)
	outWindow [][]byte      // Send window for repeating lost messages
	outSize   uint64        // Size of the input window
	outSlots  chan struct{} // Channel for the available window slots
	outLock   sync.Mutex    // Synchronizer to the output state

	inNext   uint64        // Next sequence id to be read from the receive window
	inReady  uint64        // Id of the first gap in the window
	inWindow [][]byte      // Receive window for ordering messages
	inSize   uint64        // Size of the input window
	inSlots  chan struct{} // Channel for the slots ready for recv
	inLock   sync.Mutex    // Synchronizer to the input state

	// Bookkeeping fields
	init chan struct{} // Initialization channel for outbound tunnels
	term chan struct{} // Channel to signal termination to blocked go-routines
}

// Initiates an outgoing tunnel to a remote app. A new tunnel is created and set
// as pending until either the tunneling response arrives or the request times
// out.
func (c *connection) initiateTunnel(app string, timeout time.Duration) (Tunnel, error) {
	// Create a potential tunnel
	c.tunLock.Lock()
	tunId := c.tunIdx
	tun := &tunnel{
		parent: c,
		id:     tunId,

		outWindow: make([][]byte, config.IrisTunnelWindow),
		outSize:   uint64(config.IrisTunnelWindow),
		outGrant:  uint64(config.IrisTunnelWindow),
		outSlots:  make(chan struct{}, config.IrisTunnelWindow),
		inWindow:  make([][]byte, config.IrisTunnelWindow),
		inSize:    uint64(config.IrisTunnelWindow),
		inSlots:   make(chan struct{}, config.IrisTunnelWindow),

		init: make(chan struct{}, 1),
		term: make(chan struct{}),
	}
	c.tunIdx++
	c.tunLive[tunId] = tun
	c.tunLock.Unlock()

	// Send the tunneling request
	c.conn.Balance(appPrefix+app, assembleTunnelRequest(tunId))

	// Retrieve the results, time out or terminate
	var err error
	select {
	case <-c.term:
		err = permError(fmt.Errorf("connection terminating"))
	case <-time.After(timeout):
		err = timeError(fmt.Errorf("tunneling timeout"))
	case <-tun.init:
		return tun, nil
	}
	// Cleanup and report failure
	c.tunLock.Lock()
	delete(c.tunLive, tunId)
	c.tunLock.Unlock()
	return nil, err
}

// Accepts an incoming tunneling request from a remote, initializes and stores
// the new tunnel into the connection state.
func (c *connection) acceptTunnel(peerAddr *carrier.Address, peerId uint64) (Tunnel, error) {
	// Create the local tunnel endpoint
	c.tunLock.Lock()
	tunId := c.tunIdx
	tun := &tunnel{
		parent:   c,
		id:       tunId,
		peerAddr: peerAddr,
		peerId:   peerId,

		outWindow: make([][]byte, config.IrisTunnelWindow),
		outSize:   uint64(config.IrisTunnelWindow),
		outGrant:  uint64(config.IrisTunnelWindow),
		outSlots:  make(chan struct{}, config.IrisTunnelWindow),
		inWindow:  make([][]byte, config.IrisTunnelWindow),
		inSize:    uint64(config.IrisTunnelWindow),
		inSlots:   make(chan struct{}, config.IrisTunnelWindow),

		term: make(chan struct{}),
	}
	c.tunIdx++
	c.tunLive[tunId] = tun
	c.tunLock.Unlock()

	// Return a successful tunnel setup to the initiator
	c.conn.Direct(peerAddr, assembleTunnelReply(peerId, tunId))

	// Return the accepted tunnel
	return tun, nil
}

// Implements iris.Tunnel.Send.
//
// The method is not reentrant. It is meant to be used in a single thread, with
// concurrent access leading to undefined behavior!
func (t *tunnel) Send(msg []byte) error {
	// Wait till an output slot frees up
	select {
	case <-t.term:
		return permError(fmt.Errorf("tunnel closed"))
	case t.outSlots <- struct{}{}:
		t.outLock.Lock()
		defer t.outLock.Unlock()

		// Store the message into the send window
		t.outWindow[t.outNext%t.outSize] = msg

		// Send the message if allowed
		if t.outNext < t.outGrant {
			go t.parent.conn.Direct(t.peerAddr, assembleTunnelData(t.peerId, t.outNext, msg))
		}
		// Update state and return
		t.outNext++
	}
	return nil
}

func (t *tunnel) handleAck(seqId uint64) {
	t.outLock.Lock()
	defer t.outLock.Unlock()

	// Acknowledge a message if inside the window and not yet marked as ready for reuse
	if t.outAcked <= seqId {
		t.outWindow[seqId%t.outSize] = nil
	}
	// Grant send window slots if any became available
	for t.outWindow[t.outAcked%t.outSize] == nil && t.outAcked < t.outNext {
		<-t.outSlots
		t.outAcked++
	}
}

func (t *tunnel) handleGrant(seqId uint64) {
	t.outLock.Lock()
	defer t.outLock.Unlock()

	// If reordered grant message, discard
	if t.outGrant >= seqId {
		return
	}
	// Send all pending messages between the last grant and the new
	for t.outGrant < seqId && t.outGrant < t.outNext {
		msg := t.outWindow[t.outGrant%t.outSize]
		go t.parent.conn.Direct(t.peerAddr, assembleTunnelData(t.peerId, t.outGrant, msg))
		t.outGrant++
	}
	// Update grant entry and return
	t.outGrant = seqId
}

// The method is not reentrant. It is meant to be used in a single thread, with
// concurrent access leading to undefined behavior!
func (t *tunnel) Recv(timeout time.Duration) ([]byte, error) {
	// Wait till an input slot is granted
	select {
	case <-t.term:
		return nil, permError(fmt.Errorf("tunnel closed"))
	case <-time.After(timeout):
		return nil, timeError(fmt.Errorf("tunnel recv timeout"))
	case <-t.inSlots:
		t.inLock.Lock()
		defer t.inLock.Unlock()

		// Retrieve the message from the window
		msg := t.inWindow[t.inNext%t.inSize]
		t.inWindow[t.inNext%t.inSize] = nil
		t.inNext++

		// Signal a window shift to the remote endpoint
		go t.parent.conn.Direct(t.peerAddr, assembleTunnelGrant(t.peerId, t.inNext+t.inSize))

		// Return the retrieved message
		return msg, nil
	}
}

// Handles the arrival
func (t *tunnel) handleData(seqId uint64, msg []byte) {
	// Sync the input state
	t.inLock.Lock()

	// Store the message if inside the window and not yet marked as ready for receive
	if t.inReady <= seqId {
		t.inWindow[seqId%t.inSize] = msg
	}
	// Grant receive slots if message is new (or filled a hole)
	for t.inWindow[t.inReady%t.inSize] != nil && t.inReady < t.inNext+t.inSize {
		t.inSlots <- struct{}{}
		t.inReady++
	}
	// Release lock
	t.inLock.Unlock()

	// Always ack a message (in case a previous got lost)
	t.parent.conn.Direct(t.peerAddr, assembleTunnelAck(t.peerId, seqId))
}

// Implements iris.Tunnel.Close. Signals the parent connection of the tunnel
// closure for clean-up purposes and notifies the remote endpoint too.
func (t *tunnel) Close() {
	t.parent.handleTunnelClose(t.id)
	t.parent.conn.Direct(t.peerAddr, assembleTunnelClose(t.peerId))
}
