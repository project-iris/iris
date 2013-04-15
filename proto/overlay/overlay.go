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
	"crypto/rsa"
	"encoding/gob"
	"math/big"
	"net"
	"proto/session"
)

// Internal structure for the overlay state information.
type overlay struct {
	self  string
	lkey  *rsa.PrivateKey
	rkeys map[string]*rsa.PublicKey

	state *table

	bootSink chan *tagBoot
	sesSink  chan *tagSes
	peerSink chan *peer
	msgSink  chan *session.Message
	quit     chan struct{}
}

// Peer state information.
type peer struct {
	addr string
	id   *big.Int

	in   chan *session.Message
	out  chan *session.Message
	quit chan struct{}

	inBuf  bytes.Buffer
	outBuf bytes.Buffer

	dec *gob.Decoder
	enc *gob.Encoder
}

// Boots the iris network on each IPv4 interface present.
func New(self string, key *rsa.PrivateKey) *overlay {
	o := new(overlay)

	o.self = self
	o.lkey = key
	o.rkeys = make(map[string]*rsa.PublicKey)
	o.rkeys[self] = &key.PublicKey

	o.state = newTable()

	o.bootSink = make(chan *tagBoot)
	o.sesSink = make(chan *tagSes)
	o.peerSink = make(chan *peer)
	o.msgSink = make(chan *session.Message)
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
	go o.shaker()
	return nil
}

// Sends a termination signal to all the go routines part of the overlay.
func (o *overlay) Shutdown() {
	close(o.quit)
}

// Starts a receiver routine to listen on one particular session and fan in
// messages into the shared message channel.
func (o *overlay) receiver(p *peer) {
	for {
		select {
		case <-o.quit:
			return
		case <-p.quit:
			return
		case msg, ok := <-p.in:
			if ok {
				o.msgSink <- msg
			} else {
				return
			}
		}
	}
}
