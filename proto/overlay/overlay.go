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
	"crypto/rsa"
	"fmt"
	"net"
	"proto/bootstrap"
	"proto/session"
)

// Internal structure for the overlay state information.
type overlay struct {
	self  string
	lkey  *rsa.PrivateKey
	rkeys map[string]*rsa.PublicKey

	state *table

	peerSink chan *net.TCPAddr
	sessSink chan *session.Session
	quit     chan struct{}
}

// Boots the iris network on each IPv4 interface present.
func New(self string, key *rsa.PrivateKey) *overlay {
	o := new(overlay)

	o.self = self
	o.lkey = key
	o.rkeys = make(map[string]*rsa.PublicKey)
	o.rkeys[self] = &key.PublicKey

	o.state = newTable()

	o.peerSink = make(chan *net.TCPAddr)
	o.sessSink = make(chan *session.Session)
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
	// Start the pastry management
	go o.manager()
	return nil
}

// Sends a termination signal to all the go routines part of the overlay.
func (o *overlay) Shutdown() {
	close(o.quit)
}

// Starts up the overlay networking on a specified interface and fans in all the
// inbound conenctions into the overlay-global channels.
func (o *overlay) acceptor(ip net.IP) {
	// Listen for incomming session on the given interface and random port.
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		panic(fmt.Sprintf("failed to resolve interface (%v): %v.", ip, err))
	}
	sessSink, quit, err := session.Listen(addr, o.lkey, o.rkeys)
	if err != nil {
		panic(fmt.Sprintf("failed to start session listener: %v.", err))
	}
	defer close(quit)

	// Start the bootstrapper on the specified interface
	peerSink, quit, err := bootstrap.Boot(ip, addr.Port)
	if err != nil {
		panic(fmt.Sprintf("failed to start bootstrapper: %v.", err))
	}
	defer close(quit)

	// Loop indefinitely, faning in the sessions and discovered peers
	for {
		select {
		case <-quit:
			return
		case peer := <-peerSink:
			o.peerSink <- peer
		case sess := <-sessSink:
			o.sessSink <- sess
		}
	}
}
