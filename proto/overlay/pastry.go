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
	"log"
	"math/big"
	"proto/session"
	"sort"
	"sync"
)

// Routing state table for the pastry network.
type table struct {
	mutex sync.Mutex

	self *big.Int

	leaves []*big.Int
	routes [][]*big.Int
	nears  []*big.Int
}

// Creates a new empty pastry state table.
func newTable() (t *table) {
	t = new(table)
	t.leaves = make([]*big.Int, 0, config.PastryLeaves)
	t.routes = make([][]*big.Int, config.PastrySpace/config.PastryBase)
	for i := 0; i < len(t.routes); i++ {
		t.routes[i] = make([]*big.Int, 1<<uint(config.PastryBase))
	}
	t.nears = make([]*big.Int, config.PastryNeighbors)
	return
}

// Integrates a peer into the overlay structure.
func (o *overlay) integrate(p *peer) {
	// Make sure we're in sync
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// If we already have an active connection, keep only one:
	//  - If old and new have the same direction, keep the lower client
	//  - Otherwise server should be smaller
	if old, ok := o.pool[p.self.String()]; ok {
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
		// If it's a keeper, swap out old and close it
		if keep {
			close(p.quit)
		} else {
			o.pool[p.self.String()] = p
			close(old.quit)
			go o.receiver(p)
		}
		return
	}
	// Otherwise accept the new one
	o.pool[p.self.String()] = p
	go o.receiver(p)
}

// Manages the pastry state table and connection pool by accepting new incomming
// sessions and boostrap events, deciding which ones to keep and which to drop.
func (o *overlay) manager() {

}

// Processes messages arriving from the network either forwarding or delivering
// them up to the application layer.
func (o *overlay) router() {
	for {
		select {
		case <-o.quit:
			return
		case msg, ok := <-o.msgSink:
			if ok {
				if err := o.route(msg); err != nil {
					log.Println("failed to route message:", err)
				}
			} else {
				return
			}
		}
	}
}

// Pastry routing algorithm.
func (o *overlay) route(msg *session.Message) error {
	s := o.routes

	// Sync the routing table
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Extract the recipient
	dest := big.NewInt(0)
	//dest.SetString(msg.Head.Target, 10)

	// Check the leaf set for direct delivery
	if s.leaves[0].Cmp(dest) <= 0 && dest.Cmp(s.leaves[len(s.leaves)-1]) <= 0 {
		best := s.leaves[0]
		dist := distance(best, dest)
		for i := 1; i < len(s.leaves); i++ {
			curLeaf := s.leaves[i]
			curDist := distance(curLeaf, dest)
			if curDist.Cmp(dist) < 0 {
				best = curLeaf
				dist = curDist
			}
		}
		// If self, deliver, otherwise forward
		if s.self.Cmp(best) == 0 {
			return o.deliver(msg)
		} else {
			return o.forward(best, msg)
		}
	}
	// Check the routing table for indirect delivery
	common := prefix(s.self, dest)
	column := uint(0)
	for b := 0; b < config.PastryBase; b++ {
		column |= dest.Bit(common*config.PastryBase+b) << uint(b)
	}
	if best := s.routes[common][column]; best != nil {
		return o.forward(best, msg)
	}
	// Route to anybody closer
	dist := distance(s.self, dest)
	for _, peer := range s.leaves {
		if prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
			return o.forward(peer, msg)
		}
	}
	for _, row := range s.routes {
		for _, peer := range row {
			if prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
				return o.forward(peer, msg)
			}
		}
	}
	for _, peer := range s.nears {
		if prefix(peer, dest) >= common && distance(peer, dest).Cmp(dist) < 0 {
			return o.forward(peer, msg)
		}
	}
	// Well, shit
	return fmt.Errorf("Failed to route message to destination: %v.", dest)
}

// Delivers a message to the application layer.
func (o *overlay) deliver(msg *session.Message) error {
	fmt.Println("Deliver:", msg)
	return nil
}

// Forwards a message to the node with the given id
func (o *overlay) forward(id *big.Int, msg *session.Message) error {
	fmt.Println("Forward:", id, msg)
	return nil
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
