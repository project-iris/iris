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
	"bytes"
	"config"
	"crypto/rand"
	"crypto/rsa"
	"encoding/gob"
	"fmt"
	"io"
	"math/big"
	"net"
	"proto/session"
	"sync"
)

// Different status types in which the node can be.
type status uint8

const (
	none status = iota
	join
	done
)

// Internal structure for the overlay state information.
type overlay struct {
	// Local and remote keys to authorize
	lkey  *rsa.PrivateKey
	rkeys map[string]*rsa.PublicKey

	// Global overlay id, local peer id and local listener addresses
	overId string
	nodeId *big.Int
	addrs  []string

	// The active connection pool, ip to id translations and routing table with modification timestamp
	pool  map[string]*peer
	trans map[string]*big.Int

	routes *table
	time   uint64
	stat   status

	// Fan-in sinks for bootstrap, network and state update events + quit channel
	bootSink chan *net.TCPAddr
	sesSink  chan *session.Session
	upSink   chan *state
	quit     chan struct{}

	// Syncer for state mods after booting and wait group for dials in progress.
	lock sync.RWMutex
	pend sync.WaitGroup
}

// Routing table
type table struct {
	leaves []*big.Int
	routes [][]*big.Int
	nears  []*big.Int
}

// Peer state information.
type peer struct {
	// Virtual id and reachable addresses
	self  *big.Int
	addrs []string

	// Connection details
	laddr string
	raddr string

	// In/out-bound transport channels and quit channel
	out    chan *message
	netIn  chan *session.Message
	netOut chan *session.Message
	quit   chan struct{}

	// Buffers and gob coders for the overlay specific meta-headers
	inBuf  bytes.Buffer
	outBuf bytes.Buffer

	dec *gob.Decoder
	enc *gob.Encoder

	// Pastry state infos
	time uint64
}

// Boots the iris network on each IPv4 interface present.
func New(self string, key *rsa.PrivateKey) *overlay {
	o := new(overlay)

	o.lkey = key
	o.rkeys = make(map[string]*rsa.PublicKey)
	o.rkeys[self] = &key.PublicKey

	id := make([]byte, config.PastrySpace/8)
	if n, err := io.ReadFull(rand.Reader, id); n < len(id) || err != nil {
		panic(fmt.Sprintf("failed to generate node id: %v", err))
	}
	o.nodeId = new(big.Int).SetBytes(id)
	o.overId = self
	o.addrs = []string{}

	o.pool = make(map[string]*peer)
	o.trans = make(map[string]*big.Int)

	o.routes = new(table)
	o.routes.leaves = make([]*big.Int, 1, config.PastryLeaves)
	o.routes.leaves[0] = o.nodeId
	o.routes.routes = make([][]*big.Int, config.PastrySpace/config.PastryBase)
	for i := 0; i < len(o.routes.routes); i++ {
		o.routes.routes[i] = make([]*big.Int, 1<<uint(config.PastryBase))
	}
	o.routes.nears = make([]*big.Int, 0, config.PastryNeighbors)
	o.time = 1

	o.bootSink = make(chan *net.TCPAddr)
	o.sesSink = make(chan *session.Session)
	o.upSink = make(chan *state)
	o.quit = make(chan struct{})

	return o
}

// Boots the overlay network: it starts up boostrappers and connection acceptors
// on all local IPv4 interfaces, after which the pastry overlay management is
// booted.
func (o *overlay) Boot() error {
	// Start the individual acceptors
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {
			if !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				go o.acceptor(ipnet.IP)
			}
		}
	}
	// Start the message receiver and pastry manager
	go o.merger()
	go o.shaker()
	return nil
}

// Sends a termination signal to all the go routines part of the overlay.
func (o *overlay) Shutdown() {
	fmt.Println(o.nodeId, "terminating...")
	close(o.quit)
}
