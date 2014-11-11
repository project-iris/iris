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

// Package relay implements the message relay between the Iris node and locally
// attached applications.
package relay

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/pool"
	"github.com/project-iris/iris/proto/iris"
)

// Message relay between the local carrier and an attached binding.
type relay struct {
	// Application layer fields
	iris *iris.Connection // Interface into the iris overlay

	reqIdx  uint64                 // Index to assign the next request
	reqReps map[uint64]chan []byte // Reply channels for active requests
	reqErrs map[uint64]chan error  // Error channels for active requests
	reqLock sync.RWMutex           // Mutex to protect the result channel maps

	tunIdx  uint64                   // Temporary index to assign the next inbound tunnel
	tunPend map[uint64]*iris.Tunnel  // Tunnels pending binding confirmation
	tunInit map[uint64]chan struct{} // Confirmation channels for the pending tunnels
	tunLive map[uint64]*tunnel       // Active tunnels
	tunLock sync.RWMutex             // Mutex to protect the tunnel maps

	// Network layer fields
	sock     net.Conn          // Network connection to the attached client
	sockBuf  *bufio.ReadWriter // Buffered access to the network socket
	sockLock sync.Mutex        // Mutex to atomize message sending
	sockWait int32             // Counter for the pending writes (batch before flush)

	// Quality of service fields
	workers *pool.ThreadPool // Concurrent threads handling the connection

	// Bookkeeping fields
	done chan *relay     // Channel on which to signal termination
	quit chan chan error // Quit channel to synchronize relay termination
	term chan struct{}   // Channel to signal termination to blocked go-routines
}

// Accepts an inbound relay connection, executing the initialization procedure.
func (r *Relay) acceptRelay(sock net.Conn) (*relay, error) {
	// Create the relay object
	rel := &relay{
		reqReps: make(map[uint64]chan []byte),
		reqErrs: make(map[uint64]chan error),
		tunPend: make(map[uint64]*iris.Tunnel),
		tunInit: make(map[uint64]chan struct{}),
		tunLive: make(map[uint64]*tunnel),

		// Network layer
		sock:    sock,
		sockBuf: bufio.NewReadWriter(bufio.NewReader(sock), bufio.NewWriter(sock)),

		// Quality of service
		workers: pool.NewThreadPool(config.RelayHandlerThreads),

		// Misc
		done: r.done,
		quit: make(chan chan error),
		term: make(chan struct{}),
	}
	// Lock the socket to ensure no writes pass during init
	rel.sockLock.Lock()
	defer rel.sockLock.Unlock()

	// Initialize the relay
	version, cluster, err := rel.procInit()
	if err != nil {
		rel.drop()
		return nil, err
	}
	// Make sure the protocol version is compatible
	if version != protoVersion {
		// Drop the connection in either error branch
		defer rel.drop()

		reason := fmt.Sprintf("Unsupported protocol. Client: %s. Iris: %s.", version, protoVersion)
		if err := rel.sendDeny(reason); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("relay: unsupported client protocol version: have %v, want %v", version, protoVersion)
	}
	// Connect to the Iris network either as a service or as a client
	var handler iris.ConnectionHandler
	if cluster != "" {
		handler = rel
	}
	conn, err := r.iris.Connect(cluster, handler)
	if err != nil {
		rel.drop()
		return nil, err
	}
	rel.iris = conn

	// Report the connection accepted
	if err := rel.sendInit(); err != nil {
		rel.drop()
		return nil, err
	}
	// Start accepting messages and return
	rel.workers.Start()
	go rel.process()
	return rel, nil
}

// Forcefully drops the relay connection. Used during irrecoverable errors.
func (r *relay) drop() {
	r.sock.Close()
}

// Fetches the closure report from the relay.
func (r *relay) report() error {
	errc := make(chan error, 1)
	r.quit <- errc
	return <-errc
}
