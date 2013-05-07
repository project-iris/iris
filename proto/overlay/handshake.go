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

// This file contains the overlay session listener and negotiation. For every
// network interface a separate bootstrapper and session acceptor is started,
// faning in discovered peers and incoming (cryptographically already set up
// sessions). An single go routine is responsible for gathering all the data
// from the interfaces and executing the overlay handshake.

package overlay

import (
	"config"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"net"
	"proto/bootstrap"
	"proto/session"
	"sort"
	"time"
)

// The initialization packet when the connection is set up.
type initPacket struct {
	Id    *big.Int
	Addrs []string
}

// Starts up the overlay networking on a specified interface and fans in all the
// inbound connections into the overlay-global channels.
func (o *Overlay) acceptor(ip net.IP) {
	// Listen for incomming session on the given interface and random port.
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		panic(fmt.Sprintf("failed to resolve interface (%v): %v.", ip, err))
	}
	sesSink, quit, err := session.Listen(addr, o.lkey, o.rkeys)
	if err != nil {
		panic(fmt.Sprintf("failed to start session listener: %v.", err))
	}
	defer close(quit)

	// Save the new listener address into the local (sorted) address list
	o.lock.Lock()
	o.addrs = append(o.addrs, addr.String())
	sort.Strings(o.addrs)
	o.lock.Unlock()

	// Start the bootstrapper on the specified interface
	bootSink, quit, err := bootstrap.Boot(ip, []byte(o.overId), addr.Port)
	if err != nil {
		panic(fmt.Sprintf("failed to start bootstrapper: %v.", err))
	}
	defer close(quit)

	// Loop indefinitely, faning in the sessions and discovered peers
	for {
		select {
		case <-o.quit:
			return
		case boot := <-bootSink:
			o.bootSink <- boot
		case ses := <-sesSink:
			o.sesSink <- ses
		}
	}
}

// Processes the incoming connections: for bootstrap messages it makes sure that
// the node isn't already connected and if not, dials the remote node, setting
// up a secure session and executing the overlay handshake. For incoming session
// requests the handshake alone is done.
func (o *Overlay) shaker() {
	for {
		select {
		case <-o.quit:
			return
		case boot := <-o.bootSink:
			// Discard already connected nodes
			o.lock.RLock()
			_, ok := o.trans[boot.String()]
			o.lock.RUnlock()
			if !ok {
				o.auther.Schedule(func() { o.dial(boot) })
			}
		case ses := <-o.sesSink:
			go o.shake(ses)
		}
	}
}

// Asynchronously connects to a remote overlay peer and executes handshake. In
// the mean time, the overlay waitgroup is marked to signal pending connections.
func (o *Overlay) dial(addr *net.TCPAddr) {
	// Sanity check to make sure self connections are not possible (i.e. malicious bootstrapper)
	for _, a := range o.addrs {
		if addr.String() == a {
			log.Printf("self connection not allowed: %v.", o.nodeId)
			return
		}
	}
	// Dial away
	if ses, err := session.Dial(addr.IP.String(), addr.Port, o.overId, o.lkey, o.rkeys[o.overId]); err != nil {
		log.Printf("failed to dial remote peer: %v.", err)
	} else {
		o.shake(ses)
	}
}

// Executes a two way overlay handshake where both peers exchange their server
// addresses and virtual ids to enable them both to filter out multiple
// connections. To prevent resource exhaustion, a timeout is attached to the
// handshake, the violation of which results in a dropped connection.
func (o *Overlay) shake(ses *session.Session) {
	p := new(peer)

	p.laddr = ses.Raw().LocalAddr().String()
	p.raddr = ses.Raw().RemoteAddr().String()
	p.dec = gob.NewDecoder(&p.inBuf)
	p.enc = gob.NewEncoder(&p.outBuf)

	p.quit = make(chan struct{})
	p.out = make(chan *message, config.OverlayNetPreBuffer)
	p.netIn = make(chan *session.Message, config.OverlayNetBuffer)
	p.netOut = ses.Communicate(p.netIn, p.quit)

	// Send an init packet to the remote peer
	pkt := new(initPacket)
	pkt.Id = new(big.Int).Set(o.nodeId)

	o.lock.RLock()
	pkt.Addrs = make([]string, len(o.addrs))
	copy(pkt.Addrs, o.addrs)
	o.lock.RUnlock()

	if err := p.enc.Encode(pkt); err != nil {
		panic(fmt.Sprintf("failed to encode init packet: %v.", err))
	}
	msg := new(session.Message)
	msg.Head.Meta = make([]byte, p.outBuf.Len())
	copy(msg.Head.Meta, p.outBuf.Bytes())
	p.outBuf.Reset()
	p.netOut <- msg

	// Wait for an incoming init packet
	timeout := time.Tick(time.Duration(config.OverlayInitTimeout) * time.Millisecond)
	success := true
	select {
	case <-timeout:
		log.Printf("session initialization timed out.")
		success = false
		break
	case msg, ok := <-p.netIn:
		if !ok {
			log.Printf("remote closed connection before init packet.")
			success = false
			break
		}
		p.inBuf.Write(msg.Head.Meta)
		if err := p.dec.Decode(pkt); err != nil {
			log.Printf("failed to decode remote init packet: %v.", err)
			success = false
			break
		}
		p.nodeId = pkt.Id
		p.addrs = pkt.Addrs

		// Everything ok, accept connection
		o.filter(p)
	}
	// Make sure we release anything associated with a failed connection
	if !success {
		close(p.quit)
	}
}

// Filters a new peer connection to ensure there are no duplicates. In case one
// already exists, either the old or the new is dropped:
//  - If old and new have the same direction (race), keep the lower client
//  - Otherwise server (host:port) should be smaller (covers multi-instance too)
func (o *Overlay) filter(p *peer) {
	o.lock.Lock()

	// Keep only one active connection
	var old *peer
	if old, ok := o.pool[p.nodeId.String()]; ok {
		keep := true
		switch {
		case old.laddr == p.laddr:
			keep = old.raddr < p.raddr
		case old.raddr == p.raddr:
			keep = old.laddr < p.laddr
		default:
			// If we're the server
			if i := sort.SearchStrings(o.addrs, p.laddr); i < len(o.addrs) && o.addrs[i] == p.laddr {
				keep = o.addrs[0] < p.addrs[0]
			} else {
				keep = o.addrs[0] > p.addrs[0]
			}
		}
		if keep {
			close(p.quit)
			o.lock.Unlock() // There's one more release point!
			return
		}
	}
	// Connections is accepted, start the data handlers
	o.pool[p.nodeId.String()] = p
	for _, addr := range p.addrs {
		o.trans[addr] = p.nodeId
	}
	go o.sender(p)
	go o.receiver(p)

	// If we swapped, terminate the old directly
	if old != nil {
		close(old.quit)
	}
	// Decide whether to send a join request or a state exchange
	status := o.stat
	if o.stat == none {
		o.stat = join
	}
	// Release lock before proceeding with state exchanges
	o.lock.Unlock() // There's one more release point!
	if status == none {
		o.sendJoin(p)
	} else if o.stat == done {
		o.sendState(p, false)
	}
}
