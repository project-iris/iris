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

// Package overlay contains the peer-to-peer virtual transport network. It is
// currently based on a simplified version of Pastry, where proximity is not
// taken into consideration (i.e. no neighbor set).
package overlay

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/pool"
	"github.com/karalabe/iris/proto"
	"io"
	"math/big"
	"net"
	"sync"
)

// Different status types in which the node can be.
type status uint8

const (
	none status = iota
	join
	done
)

// Callback for events leaving the overlay network.
type Callback interface {
	Deliver(msg *proto.Message, key *big.Int)
	Forward(msg *proto.Message, key *big.Int) bool
}

// Internal structure for the overlay state information.
type Overlay struct {
	app Callback

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

	// Fan-in sinks for state update and connection drop events + quit channel
	upSink   chan *state
	dropSink chan *peer
	quit     chan struct{}

	// Thread pool to limit thread proliferation
	auther *pool.ThreadPool

	// Syncer for state mods after booting
	lock sync.RWMutex
}

// Peer state information.
type peer struct {
	// Virtual id and reachable addresses
	nodeId *big.Int
	addrs  []string

	// Connection details
	laddr string
	raddr string

	// In/out-bound transport channels and quit channel
	out    chan *proto.Message
	netIn  chan *proto.Message
	netOut chan *proto.Message
	quit   chan struct{}

	// Overlay state infos
	time    uint64
	passive bool
	killed  bool
}

// Creates a new overlay structure with all internal state initialized, ready to
// be booted. Self is used as the id used for discovering similar peers, and key
// for the security.
func New(self string, key *rsa.PrivateKey, app Callback) *Overlay {
	o := new(Overlay)
	o.app = app

	o.lkey = key
	o.rkeys = make(map[string]*rsa.PublicKey)
	o.rkeys[self] = &key.PublicKey

	id := make([]byte, config.OverlaySpace/8)
	if n, err := io.ReadFull(rand.Reader, id); n < len(id) || err != nil {
		panic(fmt.Sprintf("failed to generate node id: %v", err))
	}
	o.nodeId = new(big.Int).SetBytes(id)
	o.overId = self
	o.addrs = []string{}

	o.pool = make(map[string]*peer)
	o.trans = make(map[string]*big.Int)

	o.routes = newTable(o.nodeId)
	o.time = 1

	o.upSink = make(chan *state)
	o.dropSink = make(chan *peer)
	o.quit = make(chan struct{})

	o.auther = pool.NewThreadPool(config.OverlayAuthThreads)

	return o
}

// Boots the overlay network: it starts up boostrappers and connection acceptors
// on all local IPv4 interfaces, after which the overlay management is booted.
func (o *Overlay) Boot() error {
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
	// Start the overlay processes
	go o.manager()
	go o.beater()

	o.auther.Start()
	return nil
}

// Sends a termination signal to all the go routines part of the overlay.
func (o *Overlay) Shutdown() {
	close(o.quit)
	o.auther.Terminate()
}

// Returns the overlay node's identifier.
func (o *Overlay) Self() *big.Int {
	return o.nodeId
}

// Sends a message to the closest node to the given destination.
func (o *Overlay) Send(dest *big.Int, msg *proto.Message) {
	// Package into overlay envelope
	head := &header{
		Meta: msg.Head.Meta,
		Dest: dest,
	}
	msg.Head.Meta = head

	// Assemble and send an internal message with overlay state included
	o.route(nil, msg)
}
