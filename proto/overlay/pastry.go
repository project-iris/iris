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
	"math/big"
)

// Manages the pastry state table and connection pool by accepting new incomming
// sessions and boostrap events, deciding which ones to keep and which to drop.
func (o *overlay) manager() {

}

// Pastry routing algorithm.
func (o *overlay) route(src *peer, msg *message) {
	// Sync the routing table
	o.lock.RLock()
	defer o.lock.RUnlock()

	// Extract the recipient
	dest := new(big.Int).Set(msg.head.Dest)

	// Check the leaf set for direct delivery
	if o.leaves[0].Cmp(dest) <= 0 && dest.Cmp(o.leaves[len(o.leaves)-1]) <= 0 {
		best := o.leaves[0]
		dist := distance(best, dest)
		for i := 1; i < len(o.leaves); i++ {
			curLeaf := o.leaves[i]
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
	if best := o.routes[common][column]; best != nil {
		o.forward(src, msg, best)
		return
	}
	// Route to anybody closer
	dist := distance(o.nodeId, dest)
	for _, peer := range o.leaves {
		if prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
			o.forward(src, msg, peer)
			return
		}
	}
	for _, row := range o.routes {
		for _, peer := range row {
			if peer != nil && prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
				o.forward(src, msg, peer)
				return
			}
		}
	}
	for _, peer := range o.nears {
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
		// If the remote state was never updated, it's a join
		if msg.head.State.Updated == 0 {
			o.sendState(src)
			return
		}
		// Otherwise it's a state update (unless two are joining each other, but let them :P)
		if msg.head.State.Updated > src.time {
			fmt.Println("Incorporating.")
			src.time = msg.head.State.Updated
			if msg.head.State.Merged < o.time {
				fmt.Println("State updated since, sending new.")
				o.sendState(src)
			}
			return
		}
		fmt.Println("Nothing new, discarding...")
		return
	}
	// Application message, deliver upstream
	fmt.Println("Deliver:", msg)
}

// Forwards a message to the node with the given id and also checks its contents
// if system message.
func (o *overlay) forward(src *peer, msg *message, id *big.Int) {
	fmt.Println("Forward:", id, msg)
	// Process system join and state messages
	/*if msg.head.State != nil {
		if msg.head.State.Time == 0 {
			o.sendState(src)
		} else {

		}
	}*/
}

// Calculates the distance between two ids on the circular ID space
func distance(a, b *big.Int) *big.Int {
	// TODO: circular distance!!!!!
	dist := big.NewInt(0)
	dist.Sub(a, b)
	dist.Abs(dist)
	return dist
}

// Calculate the length of the common prefix of two ids
func prefix(a, b *big.Int) int {
	bit := config.PastrySpace - 1
	for ; bit >= 0; bit-- {
		if a.Bit(bit) != b.Bit(bit) {
			break
		}
	}
	return (config.PastrySpace - 1 - bit) / config.PastryBase
}
