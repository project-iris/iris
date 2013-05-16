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
	"encoding/binary"
	"fmt"
	"iris"
	"log"
	"net"
	"proto/carrier"
	"sync"
	"time"
)

// Constants for the endpoint TCP/IP layer
var acceptPollRate = time.Second

// A relay listener accepting local clients and passing messages to a carrier.
type Relay struct {
	sock *net.TCPListener
	car  carrier.Carrier

	quit chan struct{}
}

//
type relay struct {
	sock net.Conn        // Network connection to the client application
	iris iris.Connection // Interface into the distributed carrier

	outVarBuf []byte // Buffer for socket variable int encoding
	inByteBuf []byte // Buffer for socket byte decoding
	inVarBuf  []byte // Buffer for socket variable int decoding

	reqIdx uint64                 // Index to assign the next request
	reqs   map[uint64]chan []byte // Active requests waiting for a reply

	quit chan struct{}

	sockLock sync.Mutex
	reqLock  sync.Mutex
}

// Creates a new relay attached to a carrier and opens the listener socket on
// the specified local port.
func New(port int, car carrier.Carrier) (*Relay, error) {
	// Assemble the listener address
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}
	// Open the server socket
	sock, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	rel := &Relay{
		sock: sock,
		car:  car,
		quit: make(chan struct{}),
	}
	return rel, nil
}

// Starts accepting inbound connections anf relaying them to the carrier.
func (r *Relay) Boot() {
	go r.acceptor()
}

// Terminates all open connections and stops accepting new ones.
func (r *Relay) Terminate() {
	close(r.quit)
}

// Accepts inbound connections till the endpoint is terminated. For each one it
// starts a new handler and hands the socket over.
func (r *Relay) acceptor() {
	defer r.sock.Close()
	for {
		select {
		case <-r.quit:
			return
		default:
			// Accept an incoming connection but without blocking for too long
			r.sock.SetDeadline(time.Now().Add(acceptPollRate))
			if sock, err := r.sock.Accept(); err == nil {
				go r.handle(sock)
			}
		}
	}
}

func (r *Relay) handle(sock net.Conn) {
	// Create the relay object
	rel := &relay{
		sock:      sock,
		outVarBuf: make([]byte, binary.MaxVarintLen64),
		inByteBuf: make([]byte, 1),
		inVarBuf:  make([]byte, binary.MaxVarintLen64),
		reqs:      make(map[uint64]chan []byte),
	}
	// Initialize the relay
	if err := rel.procInit(r.car); err != nil {
		log.Printf("relay: failed to initialize connection: %v.", err)
		return
	}
	// Process messages till conenction is terminated
	rel.process()
}
