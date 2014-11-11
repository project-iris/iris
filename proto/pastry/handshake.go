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

// This file contains the pastry session listener and negotiation. For every
// network interface a separate bootstrapper and session acceptor is started,
// each conencting nodes and executing the pastry handshake.

package pastry

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"net"
	"sort"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/proto"
	"github.com/project-iris/iris/proto/bootstrap"
	"github.com/project-iris/iris/proto/session"
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
func (o *Overlay) acceptor(ipnet *net.IPNet, quit chan chan error) {
	// Listen for incoming session on the given interface and random port.
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(ipnet.IP.String(), "0"))
	if err != nil {
		panic(fmt.Sprintf("failed to resolve interface (%v): %v.", ipnet.IP, err))
	}
	sock, err := session.Listen(addr, o.authKey)
	if err != nil {
		panic(fmt.Sprintf("failed to start session listener: %v.", err))
	}
	sock.Accept(config.PastryAcceptTimeout)

	// Save the new listener address into the local (sorted) address list
	o.lock.Lock()
	o.addrs = append(o.addrs, addr.String())
	sort.Strings(o.addrs)
	o.lock.Unlock()

	// Start the bootstrapper on the specified interface
	boot, discover, err := bootstrap.New(ipnet, []byte(o.authId), o.nodeId, addr.Port)
	if err != nil {
		panic(fmt.Sprintf("failed to create bootstrapper: %v.", err))
	}
	if err := boot.Boot(); err != nil {
		panic(fmt.Sprintf("failed to boot bootstrapper: %v.", err))
	}
	// Process incoming connection until termination is requested
	var errc chan error
	for errc == nil {
		select {
		case errc = <-quit:
			// Terminating, close and return
			continue
		case node := <-discover:
			// Discard bootstrap requests, and only react to responses (prevent simultaneous double connecting)
			if !node.Resp {
				continue
			}
			// If the peer id is desirable, dial and authenticate
			if !o.filter(node.Peer) {
				o.authInit.Schedule(func() { o.dial([]*net.TCPAddr{node.Addr}) })
			}
		case ses := <-sock.Sink:
			// There's a hidden panic possibility here: the listener socket can fail
			// if the system is overloaded with open connections. Alas, solving it is
			// not trivial as it would require restarting the whole listener. Figure it
			// out eventually.

			// Agree upon overlay states
			o.authAccept.Schedule(func() { o.shake(ses) })
		}
	}
	// Terminate the bootstrapper and peer listener
	errv := boot.Terminate()
	if errv != nil {
		log.Printf("pastry: failed to terminate bootstrapper: %v.", errv)
	}
	if err := sock.Close(); err != nil {
		log.Printf("pastry: failed to terminate session listener: %v.", err)
		if errv == nil {
			errv = err
		}
	}
	errc <- errv
}

// Checks whether a bootstrap-located peer fits into the local routing table or
// will be just discarded anyway.
func (o *Overlay) filter(id *big.Int) bool {
	o.lock.RLock()
	defer o.lock.RUnlock()

	// Discard already connected nodes
	if _, ok := o.livePeers[id.String()]; ok {
		return true
	}

	table := o.routes

	// Check for empty slot in leaf set
	for i, leaf := range table.leaves {
		if leaf.Cmp(o.nodeId) == 0 {
			if delta(id, leaf).Sign() >= 0 && i < config.PastryLeaves/2 {
				return false
			}
			if delta(leaf, id).Sign() >= 0 && len(table.leaves)-i < config.PastryLeaves/2 {
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
				log.Printf("pastry: self connection not allowed: %v.", o.nodeId)
				return
			}
		}
	}
	// Dial away, trying interfaces one after the other until connection succeeds
	for _, addr := range addrs {
		if ses, err := session.Dial(addr.IP.String(), addr.Port, o.authKey); err == nil {
			o.shake(ses)
			return
		} else {
			log.Printf("pastry: failed to dial remote peer at %v: %v.", addr, err)
		}
	}
}

// Executes a two way overlay handshake where both peers exchange their server
// addresses and virtual ids to enable them both to filter out multiple
// connections. To prevent resource exhaustion, a timeout is attached to the
// handshake, the violation of which results in a dropped connection.
func (o *Overlay) shake(ses *session.Session) {
	// Start the message transfers and create the peer
	ses.Start(config.PastryNetBuffer)
	p := o.newPeer(ses)

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
		log.Printf("pastry: failed to send init packet: %v.", err)
		if err := ses.Close(); err != nil {
			log.Printf("pastry: failed to close uninited session: %v.", err)
		}
		return
	}
	// Wait for an incoming init packet
	select {
	case <-time.After(config.PastryInitTimeout):
		log.Printf("pastry: session initialization timed out.")
		if err := ses.Close(); err != nil {
			log.Printf("pastry: failed to close unacked session: %v.", err)
		}
	case msg, ok := <-p.conn.CtrlLink.Recv:
		if ok {
			pkt = msg.Head.Meta.(*initPacket)
			p.nodeId = pkt.Id
			p.addrs = pkt.Addrs

			// Everything ok, accept connection
			o.dedup(p)
		} else {
			log.Printf("pastry: session closed before init arrived.")
			if err := ses.Close(); err != nil {
				log.Printf("pastry: failed to close dropped session: %v.", err)
			}
		}
	}
}

// Filters a new peer connection to ensure there are no duplicates.
//  - Same network, same direction: keep the lower client
//  - Same network, diff direction: keep the lower server
//  - Diff network:                 keep the lower network
func (o *Overlay) dedup(p *peer) {
	// Even though p might be a duplicate, parallel dedups might run in an inverse
	// order at the remote side, thus it might start routing messages before being
	// deduped too. To ensure racy messages don't get lost silently, start the peer
	// anyways and gracefully close it if not needed.
	p.Start()

	var dump *peer // The peer to dump, if any
	o.lock.Lock()

	// Keep only one active connection
	old, ok := o.livePeers[p.nodeId.String()]
	keepOld := false
	if ok {
		switch {
		// Same network, same direction
		case old.laddr == p.laddr:
			keepOld = old.raddr < p.raddr
		case old.raddr == p.raddr:
			keepOld = old.laddr < p.laddr

		// Same network, different direction
		case old.lhost == p.lhost:
			if i := sort.SearchStrings(o.addrs, p.laddr); i < len(o.addrs) && o.addrs[i] == p.laddr {
				// We're the server in 'p', remote is the server in 'old'
				keepOld = old.raddr < p.laddr
			} else {
				keepOld = old.laddr < p.raddr
			}
		// Different network
		default:
			keepOld = old.lhost < p.lhost
		}
	}
	// If the new connection is accepted, swap out old one if any
	var stat status
	if !keepOld {
		// Swap out the old peer connection
		o.livePeers[p.nodeId.String()] = p
		dump = old

		// Decide whether to send a join request or a state exchange to the new
		stat = o.stat
		if o.stat == none {
			o.stat = join
		}
	} else {
		dump = p
	}
	o.lock.Unlock()

	// Initialize the new peer if needed
	if !keepOld {
		if stat == none {
			o.sendJoin(p)
		} else if stat == done {
			o.sendState(p)
		}
		// If brand new peer, start monitoring it
		if old == nil {
			o.heart.heart.Monitor(p.nodeId)
		}
	}
	// Terminate the duplicate if any
	if dump != nil {
		o.drop(dump)
	}
}
