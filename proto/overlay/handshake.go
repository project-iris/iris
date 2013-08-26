// Iris - Decentralized Messaging Framework
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
// each conencting nodes and executing the overlay handshake.

package overlay

import (
	"encoding/gob"
	"fmt"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/proto"
	"github.com/karalabe/iris/proto/bootstrap"
	"github.com/karalabe/iris/proto/session"
	"log"
	"math/big"
	"net"
	"sort"
	"time"
)

// The initialization packet when the connection is set up.
type initPacket struct {
	Id    *big.Int
	Addrs []string
}

// Make sure the init packet is registered with gob.
func init() {
	gob.Register(&initPacket{})
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
	booter, bootSink, err := bootstrap.New(ip, []byte(o.overId), o.nodeId, addr.Port)
	if err != nil {
		panic(fmt.Sprintf("failed to create bootstrapper: %v.", err))
	}
	if err := booter.Boot(); err != nil {
		panic(fmt.Sprintf("failed to boot bootstrapper: %v.", err))
	}
	defer booter.Terminate()

	// Processes the incoming connections
	for {
		select {
		case <-o.quit:
			return
		case boot := <-bootSink:
			// Discard bootstrap requests (prevent simultaneous double connecting)
			if !boot.Resp {
				break
			}
			// If the peer id is desirable, dial and authenticate
			if !o.filter(boot.Peer) {
				o.auther.Schedule(func() { o.dial([]*net.TCPAddr{boot.Addr}) })
			}
		case ses := <-sesSink:
			// Agree upon overlay states
			go o.shake(ses)
		}
	}
}

// Checks whether a bootstrap-located peer fits into the local routing table or
// will be just discarded anyway.
func (o *Overlay) filter(id *big.Int) bool {
	o.lock.RLock()
	defer o.lock.RUnlock()

	// Discard already connected nodes
	if _, ok := o.pool[id.String()]; ok {
		return true
	}

	table := o.routes

	// Check for empty slot in leaf set
	for i, leaf := range table.leaves {
		if leaf.Cmp(o.nodeId) == 0 {
			if delta(id, leaf).Sign() >= 0 && i < config.OverlayLeaves/2 {
				return false
			}
			if delta(leaf, id).Sign() >= 0 && len(table.leaves)-i < config.OverlayLeaves/2 {
				return false
			}
			break
		}
	}
	// Check for better leaf set
	if delta(table.leaves[0], id).Sign() >= 0 && delta(id, table.leaves[len(table.leaves)-1]).Sign() >= 0 {
		return false
	}
	// Check place in routing table
	pre, col := prefix(o.nodeId, id)
	if prev := table.routes[pre][col]; prev == nil {
		return false
	}
	// Nowhere to insert, bin it
	return true
}

// Asynchronously connects to a remote overlay peer and executes handshake.
func (o *Overlay) dial(addrs []*net.TCPAddr) {
	// Sanity check to make sure self connections are not possible (i.e. malicious bootstrapper)
	for _, ownAddr := range o.addrs {
		for _, peerAddr := range addrs {
			if peerAddr.String() == ownAddr {
				log.Printf("overlay: self connection not allowed: %v.", o.nodeId)
				return
			}
		}
	}
	// Dial away, trying interfaces one after the other until connection succeeds
	for _, addr := range addrs {
		if ses, err := session.Dial(addr.IP.String(), addr.Port, o.overId, o.lkey, o.rkeys[o.overId]); err == nil {
			o.shake(ses)
			return
		} else {
			log.Printf("overlay: failed to dial remote peer %v, at %v: %v.", o.overId, addr, err)
		}
	}
}

// Executes a two way overlay handshake where both peers exchange their server
// addresses and virtual ids to enable them both to filter out multiple
// connections. To prevent resource exhaustion, a timeout is attached to the
// handshake, the violation of which results in a dropped connection.
func (o *Overlay) shake(ses *session.Session) {
	p, err := o.newPeer(ses)
	if err != nil {
		log.Printf("overlay: failed to create peer: %v.", err)
		return
	}
	// Send an init packet to the remote peer
	pkt := new(initPacket)
	pkt.Id = new(big.Int).Set(o.nodeId)

	o.lock.RLock()
	pkt.Addrs = make([]string, len(o.addrs))
	copy(pkt.Addrs, o.addrs)
	o.lock.RUnlock()

	msg := new(proto.Message)
	msg.Head.Meta = pkt
	if err := p.send(msg); err != nil {
		log.Printf("overlay: failed to send init packet: %v.", err)
		if err := p.Close(); err != nil {
			log.Printf("overlay: failed to close peer connection: %v.", err)
		}
		return
	}
	// Wait for an incoming init packet
	success := false
	select {
	case <-time.After(time.Duration(config.OverlayInitTimeout) * time.Millisecond):
		log.Printf("overlay: session initialization timed out.")
	case msg, ok := <-p.netIn:
		if ok {
			success = true

			pkt = msg.Head.Meta.(*initPacket)
			p.nodeId = pkt.Id
			p.addrs = pkt.Addrs

			// Everything ok, accept connection
			o.dedup(p)
		}
	}
	// Make sure we release anything associated with a failed connection
	if !success {
		if err := p.Close(); err != nil {
			log.Printf("overlay: failed to close peer connection: %v.", err)
		}
	}
}

// Filters a new peer connection to ensure there are no duplicates. In case one
// already exists, either the old or the new is dropped:
//  - Same network, same direction: keep the lower client
//  - Same network, diff direction: keep the lower server
//  - Diff network:                 keep the lower network
func (o *Overlay) dedup(p *peer) {
	o.lock.Lock()

	// Keep only one active connection
	old, ok := o.pool[p.nodeId.String()]
	if ok {
		keep := true
		switch {
		// Same network, same direction
		case old.laddr == p.laddr:
			keep = old.raddr < p.raddr
		case old.raddr == p.raddr:
			keep = old.laddr < p.laddr

		// Same network, different direction
		case old.lhost == p.lhost:
			if i := sort.SearchStrings(o.addrs, p.laddr); i < len(o.addrs) && o.addrs[i] == p.laddr {
				// We're the server in 'p', remote is the server in 'old'
				keep = old.raddr < p.laddr
			} else {
				keep = old.laddr < p.raddr
			}
		// Different network
		default:
			keep = old.lhost < p.lhost
		}
		if keep {
			o.lock.Unlock() // There's one more release point!
			if err := p.Close(); err != nil {
				log.Printf("overlay: failed to close peer connection: %v.", err)
			}
			return
		}
	}
	// Connections is accepted, start the data handlers
	o.pool[p.nodeId.String()] = p
	for _, addr := range p.addrs {
		o.trans[addr] = p.nodeId
	}
	if err := p.Start(); err != nil {
		log.Printf("overlay: failed to start peer receiver: %v.", err)
	}
	// If we swapped, terminate the old directly
	if old != nil {
		if err := old.Close(); err != nil {
			log.Printf("overlay: failed to close peer connection: %v.", err)
		}
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
