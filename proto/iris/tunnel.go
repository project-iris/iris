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
	"sync"
	"time"
)

type tunnel struct {
	relay *carrier.Connection

	peerAddr *carrier.Address
	peerId   uint64

	init chan uint64
	data chan []byte

	inBuf  chan []byte
	outBuf chan []byte

	lock sync.Mutex
	term chan struct{}
}

// Implements iris.Connection.Tunnel.
func (c *connection) Tunnel(app string, timeout time.Duration) (Tunnel, error) {
	return c.initiateTunnel(app, timeout)
}

func (c *connection) initiateTunnel(app string, timeout time.Duration) (Tunnel, error) {
	// Create a potential tunnel
	c.lock.Lock()
	tunId := c.tunIdx
	tun := &tunnel{
		relay: c.relay,
		init:  make(chan uint64, 1),
		data:  make(chan []byte, 64),
		term:  make(chan struct{}),
	}
	c.tunIdx++
	c.tuns[tunId] = tun
	c.lock.Unlock()

	// Send the tunneling request
	c.relay.Balance(appPrefix+app, assembleTunnelRequest(tunId))

	// Retrieve the results or time out
	tick := time.Tick(time.Duration(timeout) * time.Millisecond)
	select {
	case peer := <-tun.init:
		// Remote id arrived, save and return
		tun.peerId = peer
		return tun, nil
	case <-tick:
		// Timeout, remove the tunnel leftover and error out
		c.lock.Lock()
		delete(c.tuns, tunId)
		c.lock.Unlock()

		return nil, fmt.Errorf("iris: couldn't tunnel within %d ms", timeout)
	}
}

// Accepts an incoming tunneling request from a remote app.
func (c *connection) acceptTunnel(src *carrier.Address, peerId uint64) (Tunnel, error) {
	// Create the local tunnel endpoint
	c.lock.Lock()
	tun := &tunnel{
		relay:    c.relay,
		peerAddr: src,
		peerId:   peerId,
		data:     make(chan []byte, 64),
		term:     make(chan struct{}),
	}
	tunId := c.tunIdx
	c.tunIdx++
	c.tuns[tunId] = tun
	c.lock.Unlock()

	// Reply with a successful tunnel setup message
	go c.relay.Direct(src, assembleTunnelReply(peerId, tunId))

	// Return the accepted tunnel
	return tun, nil
}

// Implements iris.Tunnel.Send.
func (t *tunnel) Send(msg []byte) error {
	select {
	case <-t.term:
		return permError(fmt.Errorf("tunnel closed"))
	default:
		go t.relay.Direct(t.peerAddr, assembleTunnelData(t.peerId, msg))
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

// Implements iris.Tunnel.Close.
func (t *tunnel) Close() {
	close(t.term)

	go t.relay.Direct(t.peerAddr, assembleTunnelClose(t.peerId))
}

func (t *tunnel) handleData(msg []byte) {
	t.data <- msg
}
