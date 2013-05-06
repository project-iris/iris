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

// This file contains the routing logic in the overlay network, which currently
// is a simplified version of Pastry: the leafset and routing table is the same,
// but no proximity metric is taken into consideration.
//
// Beside the above, it also contains the system event processing logic.

package overlay

import (
	"log"
	"math/big"
	"net"
)

// Pastry routing algorithm.
func (o *Overlay) route(src *peer, msg *message) {
	// Sync the routing table
	o.lock.RLock()
	defer o.lock.RUnlock()

	// Extract some vars for easier access
	tab := o.routes
	dst := msg.head.Dest

	// Check the leaf set for direct delivery
	// TODO: corner cases with if only handful of nodes
	// TODO: binary search with idSlice could be used (worthwhile?)
	if delta(tab.leaves[0], dst).Sign() >= 0 && delta(dst, tab.leaves[len(tab.leaves)-1]).Sign() >= 0 {
		best := tab.leaves[0]
		dist := distance(best, dst)
		for _, leaf := range tab.leaves[1:] {
			if d := distance(leaf, dst); d.Cmp(dist) < 0 {
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
	pre, col := prefix(o.nodeId, dst)
	if best := tab.routes[pre][col]; best != nil {
		o.forward(src, msg, best)
		return
	}
	// Route to anybody closer than the local node
	dist := distance(o.nodeId, dst)
	for _, peer := range tab.leaves {
		if p, _ := prefix(peer, dst); p >= pre && distance(peer, dst).Cmp(dist) < 0 {
			o.forward(src, msg, peer)
			return
		}
	}
	for _, row := range tab.routes {
		for _, peer := range row {
			if peer != nil {
				if p, _ := prefix(peer, dst); p >= pre && distance(peer, dst).Cmp(dist) < 0 {
					o.forward(src, msg, peer)
					return
				}
			}
		}
	}
	for _, peer := range tab.nears {
		if p, _ := prefix(peer, dst); p >= pre && distance(peer, dst).Cmp(dist) < 0 {
			o.forward(src, msg, peer)
			return
		}
	}
	// Well, shit. Deliver locally and hope for the best.
	o.deliver(src, msg)
}

// Delivers a message to the application layer or processes it if a system message.
func (o *Overlay) deliver(src *peer, msg *message) {
	if msg.head.State != nil {
		o.process(src, msg.head.Dest, msg.head.State)
	} else {
		// Remove all overlay infos from the message and send upwards
		m := msg.data
		m.Head.Meta = msg.head.Meta
		o.app.Deliver(m)
	}
}

// Forwards a message to the node with the given id and also checks its contents
// if it's a system message.
func (o *Overlay) forward(src *peer, msg *message, id *big.Int) {
	if msg.head.State != nil {
		o.process(src, msg.head.Dest, msg.head.State)
	}
	if p, ok := o.pool[id.String()]; ok {
		o.send(msg, p)
	}
}

// Processes overlay system messages: for joins it simply responds with the
// local state, whilst for state updates if verifies the timestamps and merges
// if newer, also always replying if a repair request was included. Finally the
// heartbeat messages are checked and two-way idle connections dropped.
func (o *Overlay) process(src *peer, dst *big.Int, s *state) {
	if s.Updated == 0 {
		// Join request, connect (if needed) and send local state
		if p, ok := o.pool[dst.String()]; !ok {
			if addr, err := net.ResolveTCPAddr("tcp", s.Addrs[dst.String()][0]); err != nil {
				log.Printf("failed to resolve address %v: %v.", s.Addrs[dst.String()][0], err)
			} else {
				o.dial(addr)
			}
		} else {
			// Handshake should have already sent state, unless local isn't joined either
			if o.stat != done {
				go o.sendState(p, false)
			}
		}
	} else {
		// State update, merge into local if new
		if s.Updated > src.time {
			src.time = s.Updated

			// Respond to any repair requests
			if s.Repair {
				go o.sendState(src, false)
			}
			// Make sure we don't cause a deadlock if blocked
			o.lock.RUnlock()
			o.upSink <- s
			o.lock.RLock()
		}
		// Connection filtering: drop after two requests and if local is idle too
		if src.passive && s.Passive && !o.active(src.nodeId) {
			o.lock.RUnlock()
			o.dropSink <- src
			o.lock.RLock()
		} else {
			// Save passive state for next beat
			src.passive = s.Passive
		}
	}
}
