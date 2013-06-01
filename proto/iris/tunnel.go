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
	"github.com/karalabe/iris/proto/carrier"
	"time"
)

type tunnel struct {
	parent *connection
	id     uint64

	peerAddr *carrier.Address
	peerId   uint64

	data chan []byte

	inBuf  chan []byte
	outBuf chan []byte

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
		data:   make(chan []byte, 64),

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
		data:     make(chan []byte, 64),
		term:     make(chan struct{}),
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
func (t *tunnel) Send(msg []byte) error {
	select {
	case <-t.term:
		return permError(fmt.Errorf("tunnel closed"))
	default:
		go t.parent.conn.Direct(t.peerAddr, assembleTunnelData(t.peerId, msg))
	}
	return nil
}

func (t *tunnel) Recv(timeout time.Duration) ([]byte, error) {
	select {
	case msg := <-t.data:
		return msg, nil
	case <-t.term:
		return nil, permError(fmt.Errorf("tunnel closed"))
	case <-time.After(timeout):
		return nil, timeError(fmt.Errorf("tunnel recv timeout"))
	}
}

// Implements iris.Tunnel.Close. Signals the parent connection of the tunnel
// closure for clean-up purposes and notifies the remote endpoint too.
func (t *tunnel) Close() {
	t.parent.handleTunnelClose(t.id)
	t.parent.conn.Direct(t.peerAddr, assembleTunnelClose(t.peerId))
}

func (t *tunnel) handleData(msg []byte) {
	t.data <- msg
}
