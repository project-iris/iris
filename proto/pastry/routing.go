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

// This file contains the routing logic in the overlay network, which currently
// is a simplified version of Pastry: the leafset and routing table is the same,
// but no proximity metric is taken into consideration.
//
// Beside the above, it also contains the system event processing logic.

package pastry

import (
	"log"
	"math/big"
	"net"

	"github.com/project-iris/iris/proto"
)

// Pastry routing algorithm.
func (o *Overlay) route(src *peer, msg *proto.Message) {
	// Sync the routing table
	o.lock.RLock() // Note, unlock is in deliver and forward!!!

	// Extract some vars for easier access
	tab := o.routes
	dest := msg.Head.Meta.(*header).Dest

	// Check the leaf set for direct delivery
	// TODO: corner cases with if only handful of nodes?
	// TODO: binary search with idSlice could be used (worthwhile?)
	if delta(tab.leaves[0], dest).Sign() >= 0 && delta(dest, tab.leaves[len(tab.leaves)-1]).Sign() >= 0 {
		best := tab.leaves[0]
		dist := Distance(best, dest)
		for _, leaf := range tab.leaves[1:] {
			if d := Distance(leaf, dest); d.Cmp(dist) < 0 {
				best, dist = leaf, d
			}
		}
		// If self, deliver, otherwise forward
		if o.nodeId.Cmp(best) == 0 {
			o.deliver(src, msg)
		} else {
			o.forward(src, msg, best)
		}
		return
	}
	// Check the routing table for indirect delivery
	pre, col := prefix(o.nodeId, dest)
	if best := tab.routes[pre][col]; best != nil {
		o.forward(src, msg, best)
		return
	}
	// Route to anybody closer than the local node
	dist := Distance(o.nodeId, dest)
	for _, peer := range tab.leaves {
		if p, _ := prefix(peer, dest); p >= pre && Distance(peer, dest).Cmp(dist) < 0 {
			o.forward(src, msg, peer)
			return
		}
	}
	for _, row := range tab.routes {
		for _, peer := range row {
			if peer != nil {
				if p, _ := prefix(peer, dest); p >= pre && Distance(peer, dest).Cmp(dist) < 0 {
					o.forward(src, msg, peer)
					return
				}
			}
		}
	}
	// Well, shit. Deliver locally and hope for the best.
	o.deliver(src, msg)
}

// Delivers a message to the application layer or processes it if a system message.
func (o *Overlay) deliver(src *peer, msg *proto.Message) {
	head := msg.Head.Meta.(*header)
	if head.Op != opNop {
		o.process(src, head)
		o.lock.RUnlock()
	} else {
		// Remove all overlay infos from the message and send upwards
		o.lock.RUnlock()
		msg.Head.Meta = head.Meta
		o.app.Deliver(msg, head.Dest)
	}
}

// Forwards a message to the node with the given id and also checks its contents
// if it's a system message.
func (o *Overlay) forward(src *peer, msg *proto.Message, id *big.Int) {
	head := msg.Head.Meta.(*header)
	if head.Op != opNop {
		// Overlay system message, process and forward
		o.process(src, head)
		p, ok := o.livePeers[id.String()]
		o.lock.RUnlock()

		if ok {
			o.send(msg, p)
		}
		return
	}
	// Upper layer message, pass up and check if forward is needed
	o.lock.RUnlock()
	msg.Head.Meta = head.Meta
	allow := o.app.Forward(msg, head.Dest)

	// Forwarding was allowed, repack headers and send
	if allow {
		o.lock.RLock()
		p, ok := o.livePeers[id.String()]
		o.lock.RUnlock()

		if ok {
			head.Meta = msg.Head.Meta
			msg.Head.Meta = head
			o.send(msg, p)
		}
	}
}

// Processes overlay system messages: for joins it simply responds with the
// local state, whilst for state updates if verifies the timestamps and merges
// if newer, also always replying if a repair request was included. Finally the
// heartbeat messages are checked and two-way idle connections dropped.
func (o *Overlay) process(src *peer, head *header) {
	// Notify the heartbeat mechanism that source is alive
	o.heart.heart.Ping(src.nodeId)

	// Extract the remote id and state
	remId, remState := head.Dest.String(), head.State

	switch head.Op {
	case opJoin:
		// Discard self joins (rare race condition during update)
		if o.nodeId.Cmp(head.Dest) == 0 {
			return
		}
		// Node joining into currents responsibility list
		if p, ok := o.livePeers[remId]; !ok {
			// Connect new peers and let the handshake do the state exchange
			peerAddrs := make([]*net.TCPAddr, 0, len(remState.Addrs[remId]))
			for _, a := range remState.Addrs[remId] {
				if addr, err := net.ResolveTCPAddr("tcp", a); err != nil {
					log.Printf("pastry: failed to resolve address %v: %v.", a, err)
				} else {
					peerAddrs = append(peerAddrs, addr)
				}
			}
			o.authInit.Schedule(func() { o.dial(peerAddrs) })
		} else {
			// Handshake should have already sent state, unless local isn't joined either
			if o.stat != done {
				o.stateExch.Schedule(func() { o.sendState(p) })
			}
		}
	case opRepair:
		// Respond to any repair requests
		o.stateExch.Schedule(func() { o.sendState(src) })

	case opActive:
		// Ensure the peer is set to an active state
		src.passive = false

	case opPassive:
		// If remote connection reported passive after being already registered as
		// such locally too, drop the connection.
		if src.passive && !o.active(src.nodeId) {
			o.lock.RUnlock()
			o.drop(src)
			o.lock.RLock()
		}
	case opExchage:
		// State update, merge into local if new
		if remState.Version > src.time {
			src.time = remState.Version

			// Make sure we don't cause a deadlock if blocked
			o.lock.RUnlock()
			o.exch(src, remState)
			o.lock.RLock()
		}
	case opClose:
		// Remote side requested a graceful close
		o.lock.RUnlock()
		o.drop(src)
		o.lock.RLock()

	default:
		log.Printf("pastry: unknown system message: %+v", head)
	}
}
