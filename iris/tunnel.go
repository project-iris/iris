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
	"proto/carrier"
	"sync"
	"time"
)

type tunnel struct {
	relay *carrier.Connection

	peerAddr  *carrier.Address
	peerTunId uint64

	init chan struct{}

	inBuf  chan []byte
	outBuf chan []byte

	lock sync.Mutex
}

// Implements iris.Connection.Tunnel.
func (c *connection) Tunnel(app string, timeout time.Duration) (Tunnel, error) {
	// Create an uninitialized tunnel
	c.lock.Lock()
	tun := &tunnel{
		relay: c.relay,
		init:  make(chan struct{}, 1),
		//buff:  make(chan Message, 4096),
	}
	tunId := c.tunIdx
	c.tuns[tunId] = tun
	c.tunIdx++
	c.lock.Unlock()

	// Send the tunnel request
	c.relay.Balance(appPrefix+app, assembleTunnelRequest(tunId))

	// Wait till tunneling completes or times out
	tick := time.Tick(timeout)
	select {
	case <-tick:
		// Timeout, remove tunnel and fail
		c.lock.Lock()
		defer c.lock.Unlock()
		delete(c.tuns, tunId)
		return nil, fmt.Errorf("tunneling timed out")
	case <-tun.init:
		return tun, nil
	}
}

// Implements iris.Tunnel.Send.
func (t *tunnel) Send(msg []byte) error {
	/*select {
	case t.buff <- msg:
		return nil
	default:
		return fmt.Errorf("buffer full")
	}*/
	return nil
}

func (t *tunnel) Recv(timeout time.Duration) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

// Implements iris.Tunnel.Close.
func (t *tunnel) Close() {

}
