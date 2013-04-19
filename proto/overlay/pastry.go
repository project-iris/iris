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
	"config"
	"fmt"
	"github.com/karalabe/cookiejar/exts/mathext"
	"github.com/karalabe/cookiejar/exts/sortext"
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
	if r.leaves[0].Cmp(dest) <= 0 && dest.Cmp(r.leaves[len(r.leaves)-1]) <= 0 {
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
	common := prefix(o.nodeId, dest)
	column := uint(0)
	for b := 0; b < config.PastryBase; b++ {
		column |= dest.Bit(common*config.PastryBase+b) << uint(b)
	}
	if best := r.routes[common][column]; best != nil {
		o.forward(src, msg, best)
		return
	}
	// Route to anybody closer
	dist := distance(o.nodeId, dest)
	for _, peer := range r.leaves {
		if prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
			o.forward(src, msg, peer)
			return
		}
	}
	for _, row := range r.routes {
		for _, peer := range row {
			if peer != nil && prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
				o.forward(src, msg, peer)
				return
			}
		}
	}
	for _, peer := range r.nears {
		if prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
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
	fmt.Println("Forward:", id, msg)
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
		fmt.Println("join request.")
		if p, ok := o.pool[dst.String()]; !ok {
			fmt.Println("Joined node join request.")
			if addr, err := net.ResolveTCPAddr("tcp", s.Addrs[dst.String()][0]); err != nil {
				log.Printf("failed to resolve address %v: %v.", s.Addrs[dst.String()][0], err)
			} else {
				go o.dial(addr)
			}
		} else {
			// Handshake should have already sent state, unless local isn't joined either
			if o.stat != done {
				fmt.Println("Unjoined node join request.")
				o.sendState(p)
			} else {
				fmt.Println("Joined direct, skipping.")
			}
		}
	} else {
		// State update, merge into local if new
		if s.Updated > src.time {
			src.time = s.Updated
			fmt.Println("Merging new state...")
			go o.merge(s)
		} else {
			fmt.Println("Discarding old state...")
		}
	}
}

// Merges the recieved state into the local one, and if any modifications are
// made sends the new state out to the conencted peers.
func (o *overlay) merge(s *state) {
	o.lock.Lock()
	defer o.lock.Unlock()

	fmt.Println("Updating state")
	change := false

	// Merge the two leaf sets, sort and clear duplicates
	leaves := append(o.routes.leaves, s.Leaves...)
	sortext.BigInts(leaves)
	n := sortext.Unique(sortext.BigIntSlice(leaves))
	leaves = leaves[:n]

	// Assemble the new leafset
	origin := sortext.SearchBigInts(leaves, o.nodeId)
	leaves = leaves[mathext.MaxInt(0, origin-config.PastryLeaves/2):mathext.MinInt(len(leaves), origin+config.PastryLeaves/2)]

	// Diff and execute updates if needed
	rems, adds := diff(o.routes.leaves, leaves)
	if len(rems) != 0 || len(adds) != 0 {
		change = true
		o.routes.leaves = leaves
	}

	if change {
		fmt.Println("State updated since, broadcasting new:", o.routes.leaves)
		o.time++
		for _, peer := range o.pool {
			o.sendState(peer)
		}
	} else {
		fmt.Println("No state change detected")
	}
}

// Collects the removals and additions needed to turn src into dst.
func diff(src, dst []*big.Int) (rems, adds []*big.Int) {
	rems, adds = []*big.Int{}, []*big.Int{}
	is, id := 0, 0
	for is < len(src) && id < len(dst) {
		switch src[is].Cmp(dst[id]) {
		case -1:
			rems = append(rems, src[is])
			is++
		case 1:
			adds = append(adds, dst[id])
			id++
		default:
			is++
			id++
		}
	}
	rems = append(rems, src[is:]...)
	adds = append(adds, dst[id:]...)
	return
}
