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

// Package relay implements the message relay between the Iris node and locally
// attached applications.
package relay

import (
	"bufio"
	"github.com/karalabe/iris/proto/iris"
	"net"
	"sync"
)

// Message relay between the local carrier and an attached client app.
type relay struct {
	// Application layer fields
	iris iris.Connection // Interface into the distributed carrier

	reqIdx  uint64                 // Index to assign the next request
	reqPend map[uint64]chan []byte // Active requests waiting for a reply
	reqLock sync.RWMutex           // Mutex to protect the request map

	tunIdx  uint64
	tunPend map[uint64]chan uint64 // Tunnels pending id assignment
	tunLive map[uint64]iris.Tunnel
	tunLock sync.RWMutex

	// Network layer fields
	sock     net.Conn          // Network connection to the attached client
	sockBuf  *bufio.ReadWriter // Buffered access to the network socket
	sockLock sync.Mutex        // Mutex to atomise message sending

	// Bookkeeping fields
	done chan *relay     // Channel on which to signal termination
	quit chan chan error // Quit channe to synchronize relay termination
	term chan struct{}   // Channel to signal termination to blocked go-routines
}

// Accepts an inbound relay connection, executing the initialization procedure.
func (r *Relay) acceptRelay(sock net.Conn) (*relay, error) {
	// Create the relay object
	rel := &relay{
		reqPend: make(map[uint64]chan []byte),
		tunPend: make(map[uint64]chan uint64),
		tunLive: make(map[uint64]iris.Tunnel),

		// Network layer
		sock:    sock,
		sockBuf: bufio.NewReadWriter(bufio.NewReader(sock), bufio.NewWriter(sock)),

		// Misc
		done: r.done,
		quit: make(chan chan error),
		term: make(chan struct{}),
	}
	// Initialize the relay
	app, err := rel.procInit()
	if err != nil {
		return nil, err
	}
	// Report the connection accepted
	if err := rel.sendInit(); err != nil {
		return nil, err
	}
	// Connect to the Iris network and start accepting messages
	rel.iris = iris.Connect(r.carrier, app, rel)
	go rel.process()
	return rel, nil
}

// Fetches the closure report from the relay.
func (r *relay) report() error {
	errc := make(chan error, 1)
	r.quit <- errc
	return <-errc
}
