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
	"fmt"
	"log"
	"math/big"
	"net"
)

// Pastry routing algorithm.
func (o *overlay) route(src *peer, msg *message) {
	// Sync the routing table
	o.lock.RLock()
	defer o.lock.RUnlock()
	r := o.routes

	// Extract the recipient
	dest := new(big.Int).Set(msg.head.Dest)

	// Check the leaf set for direct delivery
	if delta(r.leaves[0], dest).Sign() > 0 && delta(dest, r.leaves[len(r.leaves)-1]).Sign() > 0 {
		best := r.leaves[0]
		dist := distance(best, dest)
		for i := 1; i < len(r.leaves); i++ {
			curLeaf := r.leaves[i]
			curDist := distance(curLeaf, dest)
			if curDist.Cmp(dist) < 0 {
				best = curLeaf
				dist = curDist
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
	if best := r.routes[pre][col]; best != nil {
		o.forward(src, msg, best)
		return
	}
	// Route to anybody closer
	dist := distance(o.nodeId, dest)
	for _, peer := range r.leaves {
		if p, _ := prefix(peer, dest); p >= pre && distance(peer, dest).Cmp(dist) < 0 {
			o.forward(src, msg, peer)
			return
		}
	}
	for _, row := range r.routes {
		for _, peer := range row {
			if peer != nil {
				if p, _ := prefix(peer, dest); p >= pre && distance(peer, dest).Cmp(dist) < 0 {
					o.forward(src, msg, peer)
					return
				}
			}
		}
	}
	for _, peer := range r.nears {
		if p, _ := prefix(peer, dest); p >= pre && distance(peer, dest).Cmp(dist) < 0 {
			o.forward(src, msg, peer)
			return
		}
	}
	// Well, shit. Deliver locally and hope for the best.
	o.deliver(src, msg)
}

// Delivers a message to the application layer or process if sys message.
func (o *overlay) deliver(src *peer, msg *message) {
	if msg.head.State != nil {
		o.process(src, msg.head.Dest, msg.head.State)
	} else {
		fmt.Println("Deliver:", msg)
	}
}

// Forwards a message to the node with the given id and also checks its contents
// if system message.
func (o *overlay) forward(src *peer, msg *message, id *big.Int) {
	if msg.head.State != nil {
		o.process(src, msg.head.Dest, msg.head.State)
	}
	o.pool[id.String()].out <- msg
}

// Processes a pastry system messages: for joins it simply responds with the
// local state, whilst for state updates if verifies the timestamps and acts
// accordingly.
func (o *overlay) process(src *peer, dst *big.Int, s *state) {
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
				o.sendState(p)
			}
		}
	} else {
		// State update, merge into local if new
		if s.Updated > src.time {
			src.time = s.Updated

			// Make sure we don't cause a deadlock if blocked
			o.lock.RUnlock()
			o.upSink <- s
			o.lock.RLock()
		}
	}
}
