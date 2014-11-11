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

// Package pastry contains a simplified version of Pastry, where proximity is
// not taken into consideration (i.e. no neighbor set).
package pastry

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"sync"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/pool"
	"github.com/project-iris/iris/proto"
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
	app Callback // Upstream application callback

	authId  string          // Iris network id
	authKey *rsa.PrivateKey // Iris authentication key

	nodeId *big.Int // Pastry peer id
	addrs  []string // Listener addresses

	livePeers map[string]*peer // Active connection pool
	heart     *heartbeat       // Beater for the active peers

	routes *table
	time   uint64
	stat   status

	acceptQuit []chan chan error // Quit sync channels for the acceptors
	maintQuit  chan chan error   // Quit sync channel for the maintenance routine

	authInit   *pool.ThreadPool // Locally initiated authentication pool
	authAccept *pool.ThreadPool // Remotely initiated authentication pool
	stateExch  *pool.ThreadPool // Pool for limiting active state exchanges

	exchSet map[*peer]*state   // State exchanges pending merging
	dropSet map[*peer]struct{} // Peers pending dropping

	eventLock   sync.Mutex    // Lock protecting overlay events
	eventNotify chan struct{} // Notifier for event changes

	stable sync.WaitGroup // Syncer for reaching convergence
	lock   sync.RWMutex   // Syncer for state mods after booting
}

// Creates a new overlay structure with all internal state initialized, ready to
// be booted.
func New(id string, key *rsa.PrivateKey, app Callback) *Overlay {
	// Generate the random node id for this overlay peer
	peerId := make([]byte, config.PastrySpace/8)
	if n, err := io.ReadFull(rand.Reader, peerId); n < len(peerId) || err != nil {
		panic(fmt.Sprintf("failed to generate node id: %v", err))
	}
	nodeId := new(big.Int).SetBytes(peerId)

	// Assemble and return the overlay instance
	o := &Overlay{
		app: app,

		authId:  id,
		authKey: key,

		nodeId: nodeId,
		addrs:  []string{},

		livePeers: make(map[string]*peer),
		routes:    newRoutingTable(nodeId),
		time:      1,

		acceptQuit: []chan chan error{},
		maintQuit:  make(chan chan error),

		authInit:   pool.NewThreadPool(config.PastryAuthThreads),
		authAccept: pool.NewThreadPool(config.PastryAuthThreads),
		stateExch:  pool.NewThreadPool(config.PastryExchThreads),

		exchSet:     make(map[*peer]*state),
		dropSet:     make(map[*peer]struct{}),
		eventNotify: make(chan struct{}, 1), // Buffer one notification
	}
	o.heart = newHeart(o)
	return o
}

// Boots the overlay network: it starts up boostrappers and connection acceptors
// on all local IPv4 interfaces, after which the overlay management is booted.
// The method returns the number of remote peers after convergence is reached.
func (o *Overlay) Boot() (int, error) {
	// Start the individual acceptors
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return 0, err
	}
	for _, addr := range addrs {
		// Workaround for upstream Go issue #5395, construct an IPNet if IPAddr is returned
		var ipnet *net.IPNet
		switch addr.(type) {
		case *net.IPNet:
			ipnet = addr.(*net.IPNet)
		case *net.IPAddr:
			log.Printf("pastry: OS returned no network mask, using defaults...")
			ipnet = &net.IPNet{
				IP:   addr.(*net.IPAddr).IP,
				Mask: addr.(*net.IPAddr).IP.DefaultMask(),
			}
		default:
			log.Printf("pastry: unknown interface address type for: %v.", addr)
			continue
		}

		if !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			// Create a quit channel and start the acceptor
			quit := make(chan chan error)
			o.acceptQuit = append(o.acceptQuit, quit)
			go o.acceptor(ipnet, quit)
		}
	}
	// Start the overlay processes
	o.stable.Add(1)
	go o.manager()
	o.heart.start()

	o.authInit.Start()
	o.authAccept.Start()
	o.stateExch.Start()

	// Wait for convergence and report remote connections
	o.stable.Wait()

	o.lock.RLock()
	defer o.lock.RUnlock()

	peers := 0
	for _, p := range o.livePeers {
		if o.active(p.nodeId) {
			peers++
		}
	}
	return peers, nil
}

// Sends a termination signal to all the go routines part of the overlay.
func (o *Overlay) Shutdown() error {
	errs := []error{}
	errc := make(chan error)

	// Close the peer listeners to prevent new connections
	for _, quit := range o.acceptQuit {
		quit <- errc
	}
	for i := 0; i < len(o.acceptQuit); i++ {
		if err := <-errc; err != nil {
			errs = append(errs, err)
		}
	}
	// Wait for all pending handshakes to finish
	o.authAccept.Terminate(false)
	o.authInit.Terminate(false)

	// Terminate the heartbeat mechanism
	if err := o.heart.terminate(); err != nil {
		errs = append(errs, err)
	}
	// Wait for all state exchanges to finish
	o.stateExch.Terminate(true)

	// Terminate the maintainer and all peer connections with it
	o.maintQuit <- errc
	if err := <-errc; err != nil {
		errs = append(errs, err)
	}
	// Report the errors and return
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return fmt.Errorf("%v", errs)
	}
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
